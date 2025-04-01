package call

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/deepgram"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/elevenlabs"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/email"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/conversation"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Service coordinates phone calls, speech recognition, AI responses, and workflows
type Service struct {
	twilioClient            *twilio.Client
	elevenLabsClient        *elevenlabs.Client
	deepgramClient          *deepgram.Client
	emailSender             *email.Sender
	conversationManager     *conversation.ConversationManager
	workflowService         *conversation.WorkflowService
	resultRepository        repository.ResultRepository
	webhookHandler          *twilio.WebhookHandler
	baseURL                 string
	activeStreamingSessions map[string]StreamingSession
	mu                      sync.RWMutex
}

// StreamingSession represents an active audio streaming session
type StreamingSession struct {
	CallSID           string
	ConversationID    string
	DeepgramStreaming bool
	IsActive          bool
	AudioBuffer       []byte
	LastActivity      time.Time
	PatientPhone      string
}

// CallResult contains the results of a completed call
type CallResult struct {
	CallSID        string
	ConversationID string
	PatientData    map[string]any
	Diagnosis      map[string]any
	Treatment      map[string]any
	WorkflowID     string
	InstanceID     uuid.UUID
	EmailSent      bool
	EmailAddress   string
	CompletedAt    time.Time
}

// NewService creates a new call service
func NewService(
	twilioClient *twilio.Client,
	elevenLabsClient *elevenlabs.Client,
	deepgramClient *deepgram.Client,
	emailSender *email.Sender,
	conversationManager *conversation.ConversationManager,
	workflowService *conversation.WorkflowService,
	resultRepository repository.ResultRepository,
	baseURL string,
) *Service {
	service := &Service{
		twilioClient:            twilioClient,
		elevenLabsClient:        elevenLabsClient,
		deepgramClient:          deepgramClient,
		emailSender:             emailSender,
		conversationManager:     conversationManager,
		workflowService:         workflowService,
		resultRepository:        resultRepository,
		baseURL:                 baseURL,
		activeStreamingSessions: make(map[string]StreamingSession),
		webhookHandler:          twilio.NewWebhookHandler(),
	}

	// Register webhook handlers
	service.registerWebhookHandlers()

	return service
}

// GetWebhookHandler returns the Twilio webhook handler
func (s *Service) GetWebhookHandler() *twilio.WebhookHandler {
	return s.webhookHandler
}

// InitiateCall starts a phone call to a patient
func (s *Service) InitiateCall(patientPhone string) (string, error) {
	// Create a callback URL for Twilio to use
	callbackURL := fmt.Sprintf("%s/api/call/webhook", s.baseURL)

	// Start the call
	callEvent, err := s.twilioClient.MakeCall(patientPhone, callbackURL)
	if err != nil {
		logger.Error("Failed to initiate call", "error", err, "phone", patientPhone)
		return "", fmt.Errorf("failed to initiate call: %w", err)
	}

	// Create a new conversation for this call
	conversation, err := s.conversationManager.StartConversation(patientPhone)
	if err != nil {
		logger.Error("Failed to create conversation", "error", err, "call_sid", callEvent.CallSID)
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

	// Create a new streaming session
	s.mu.Lock()
	s.activeStreamingSessions[callEvent.CallSID] = StreamingSession{
		CallSID:        callEvent.CallSID,
		ConversationID: conversation.ID,
		IsActive:       true,
		LastActivity:   time.Now(),
		PatientPhone:   patientPhone,
	}
	s.mu.Unlock()

	logger.Info("Call initiated",
		"call_sid", callEvent.CallSID,
		"conversation_id", conversation.ID,
		"patient_phone", patientPhone)

	return callEvent.CallSID, nil
}

// EndCall terminates an active call
func (s *Service) EndCall(callSID string) error {
	s.mu.Lock()
	session, exists := s.activeStreamingSessions[callSID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("no active session for call %s", callSID)
	}

	session.IsActive = false
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	// Try to stop the Deepgram streaming if it's active
	if session.DeepgramStreaming {
		if err := s.deepgramClient.StopStreamingSession(); err != nil {
			logger.Warn("Error stopping Deepgram streaming session", "error", err)
		}
	}

	// Mark the conversation as complete
	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err, "conversation_id", session.ConversationID)
	}

	// End the Twilio call
	if err := s.twilioClient.EndCall(callSID); err != nil {
		logger.Error("Failed to end call", "error", err, "call_sid", callSID)
		return fmt.Errorf("failed to end call: %w", err)
	}

	logger.Info("Call ended", "call_sid", callSID)
	return nil
}

// ProcessCallResults processes the results of a completed call
func (s *Service) ProcessCallResults(callSID string) (*CallResult, error) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no session found for call %s", callSID)
	}

	// Process the conversation data
	processedData, err := s.conversationManager.ProcessConversationData(session.ConversationID)
	if err != nil {
		logger.Error("Failed to process conversation data", "error", err, "conversation_id", session.ConversationID)
		return nil, fmt.Errorf("failed to process conversation data: %w", err)
	}

	// Get patient data
	patientData, ok := processedData["patient_data"].(map[string]any)
	if !ok {
		logger.Error("Invalid patient data format", "conversation_id", session.ConversationID)
		return nil, fmt.Errorf("invalid patient data format")
	}

	// Ensure the patient data has an ID
	if patientData["id"] == nil || patientData["id"] == "" {
		patientData["id"] = session.ConversationID
	}

	// Log the patient data
	logger.Info("Processed patient data",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"data_keys", getMapKeys(patientData))

	patientData = standardizePatientData(patientData)

	// Store the case in the repository first
	caseID := fmt.Sprintf("%s", patientData["id"])
	if caseID == "" {
		caseID = session.ConversationID
		patientData["id"] = caseID
	}

	var caseRepo repository.CaseRepository
	var resultRepo repository.ResultRepository

	for _, svc := range s.workflowService.GetAllServices() {
		if repo, ok := svc.(repository.CaseRepository); ok {
			caseRepo = repo
		}
		if repo, ok := svc.(repository.ResultRepository); ok {
			resultRepo = repo
		}
	}

	// Now explicitly create the case before starting workflow
	if caseRepo != nil {
		err := caseRepo.StoreCase(caseID, patientData, false)
		if err != nil {
			logger.Error("Failed to store case data", "error", err, "case_id", caseID)
			// Try with a memory repository as fallback
			memoryRepo := memory.NewCaseRepository()
			if err := memoryRepo.StoreCase(caseID, patientData, false); err != nil {
				logger.Error("Failed to store case in memory fallback", "error", err)
			} else {
				logger.Info("Successfully stored case in memory fallback", "case_id", caseID)
				caseRepo = memoryRepo
			}
		} else {
			logger.Info("Successfully stored case in database", "case_id", caseID)
		}
	} else {
		logger.Error("No case repository found, creating memory repository", "case_id", caseID)
		// Create a memory repository as fallback
		memoryRepo := memory.NewCaseRepository()
		if err := memoryRepo.StoreCase(caseID, patientData, false); err != nil {
			logger.Error("Failed to store case in memory fallback", "error", err)
		} else {
			logger.Info("Successfully stored case in memory fallback", "case_id", caseID)
			caseRepo = memoryRepo
		}
	}

	// Select the appropriate workflow
	workflowID, err := s.workflowService.SelectWorkflow(patientData)
	if err != nil {
		logger.Error("Failed to select workflow", "error", err)
		workflowID = "standard_diagnostic_workflow" // Fallback to standard workflow
	}

	// Run the workflow using management API endpoint
	logger.Info("Starting workflow with case", "workflow_id", workflowID, "case_id", caseID)

	// Create input data for the workflow
	inputData := map[string]any{
		"patient_data":    patientData,
		"case_id":         caseID,
		"conversation_id": session.ConversationID,
	}

	// Use proper endpoint format for workflow instances
	workflowURL := fmt.Sprintf("%s/api/management/workflows/%s/instances", s.baseURL, workflowID)

	// Prepare the request body
	jsonData, err := json.Marshal(inputData)
	if err != nil {
		logger.Error("Failed to marshal workflow input data", "error", err)
	}

	// Initialize UUID for the instance
	instanceID := uuid.Nil

	// Try direct API call
	req, err := http.NewRequest("POST", workflowURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error("Failed to create request", "error", err)
	} else {
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)

		if err != nil {
			logger.Error("API call failed", "error", err, "url", workflowURL)
		} else {
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				logger.Info("Successfully started workflow via API",
					"workflow_id", workflowID, "case_id", caseID)

				// Try to decode the response to get the instance ID
				var apiResponse struct {
					Message    string `json:"message"`
					InstanceID string `json:"instance_id"`
					WorkflowID string `json:"workflow_id"`
					Status     string `json:"status"`
				}

				if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err == nil && apiResponse.InstanceID != "" {
					// Store the instance ID as a string, we don't need to wait here
					instanceUUID, uuidErr := uuid.Parse(apiResponse.InstanceID)
					if uuidErr == nil {
						instanceID = instanceUUID
					}
				}
			} else {
				body, _ := io.ReadAll(resp.Body)
				logger.Error("API response error",
					"status", resp.StatusCode,
					"response", string(body),
					"url", workflowURL)
			}
		}
	}

	// Extract diagnosis and treatment data from result repository directly
	diagnosisData := make(map[string]any)
	treatmentData := make(map[string]any)

	// Try to get results if available
	if resultRepo != nil {
		results, err := resultRepo.GetResultsByCaseID(caseID)
		if err == nil && results != nil {
			logger.Info("Found existing results for case", "case_id", caseID)

			if diagnosis, ok := results["diagnosis"].(map[string]any); ok {
				diagnosisData = diagnosis
			}
			if treatment, ok := results["treatment_plan"].(map[string]any); ok {
				treatmentData = treatment
			}
		}
	}

	// Send email if we have an email address
	emailSent := false
	emailAddress := ""

	if email, ok := patientData["email"].(string); ok && email != "" && email != "unknown@example.com" {
		emailAddress = email
		patientName := "Patient"
		if name, ok := patientData["name"].(string); ok && name != "" {
			patientName = name
		}

		// Create and send the email
		emailContent := s.emailSender.CreateMedicalSummaryEmail(
			emailAddress,
			patientName,
			diagnosisData,
			treatmentData,
		)

		if err := s.emailSender.Send(emailContent); err != nil {
			logger.Error("Failed to send email", "error", err, "email", emailAddress)
		} else {
			logger.Info("Sent medical summary email", "email", emailAddress)
			emailSent = true
		}
	}

	// Record the results in the database
	// instanceID was initialized earlier and possibly set from the API response

	resultData := map[string]any{
		"call_sid":        callSID,
		"conversation_id": session.ConversationID,
		"patient_data":    patientData,
		"diagnosis":       diagnosisData,
		"treatment_plan":  treatmentData,
		"workflow_id":     workflowID,
		"instance_id":     instanceID.String(),
		"email_sent":      emailSent,
		"email_address":   emailAddress,
		"completed_at":    time.Now(),
	}

	// Store in result repository
	if resultRepo != nil {
		if err := resultRepo.StoreResults(caseID, resultData); err != nil {
			logger.Error("Failed to store call results", "error", err, "case_id", caseID)
		} else {
			logger.Info("Stored call results", "case_id", caseID)
		}
	}

	// Create the result object
	callResult := &CallResult{
		CallSID:        callSID,
		ConversationID: session.ConversationID,
		PatientData:    patientData,
		Diagnosis:      diagnosisData,
		Treatment:      treatmentData,
		WorkflowID:     workflowID,
		InstanceID:     instanceID,
		EmailSent:      emailSent,
		EmailAddress:   emailAddress,
		CompletedAt:    time.Now(),
	}

	return callResult, nil
}

func standardizePatientData(data map[string]any) map[string]any {
	// Ensure we have all the required fields with correct types
	result := make(map[string]any)

	// Copy all existing data
	for k, v := range data {
		result[k] = v
	}

	// Ensure id exists
	if result["id"] == nil || result["id"] == "" {
		if id, ok := result["patient_id"].(string); ok && id != "" {
			result["id"] = id
		} else {
			result["id"] = fmt.Sprintf("patient_%d", time.Now().UnixNano())
		}
	}

	// Ensure basic fields exist
	ensureField(result, "name", "")
	ensureField(result, "age", float64(0))
	ensureField(result, "gender", "")

	// Ensure array fields exist
	ensureArrayField(result, "symptoms")
	ensureArrayField(result, "conditions")
	ensureArrayField(result, "medications")
	ensureArrayField(result, "allergies")

	// Ensure vitals map exists
	if result["vitals"] == nil {
		result["vitals"] = map[string]any{
			"blood_pressure":    "",
			"heart_rate":        0,
			"temperature":       0,
			"oxygen_saturation": 0,
		}
	}

	return result
}

// Helper functions for standardization
func ensureField(data map[string]any, field string, defaultValue any) {
	if data[field] == nil {
		data[field] = defaultValue
	}
}

func ensureArrayField(data map[string]any, field string) {
	if data[field] == nil {
		data[field] = []string{}
	} else if _, ok := data[field].([]string); !ok {
		// Convert from []any to []string if needed
		if arr, ok := data[field].([]any); ok {
			strArr := make([]string, 0, len(arr))
			for _, item := range arr {
				if str, ok := item.(string); ok {
					strArr = append(strArr, str)
				} else {
					strArr = append(strArr, fmt.Sprintf("%v", item))
				}
			}
			data[field] = strArr
		} else {
			// If it's something else entirely, reset to empty array
			data[field] = []string{}
		}
	}
}

// registerWebhookHandlers sets up handlers for Twilio webhooks
func (s *Service) registerWebhookHandlers() {
	// Register voice webhook handler
	s.webhookHandler.RegisterCallHandler("voice", func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.handleIncomingCall(c, event)
	})

	// Register default handler
	s.webhookHandler.RegisterDefaultHandler(func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.handleIncomingCall(c, event)
	})

	// Register status handlers for all possible call statuses
	s.webhookHandler.RegisterStatusHandler("initiated", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call initiated", "call_sid", event.CallSID)
		return nil
	})

	s.webhookHandler.RegisterStatusHandler("ringing", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call ringing", "call_sid", event.CallSID)
		return nil
	})

	s.webhookHandler.RegisterStatusHandler("in-progress", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call in progress", "call_sid", event.CallSID)
		return nil
	})

	s.webhookHandler.RegisterStatusHandler("no-answer", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call not answered", "call_sid", event.CallSID)
		return nil
	})

	s.webhookHandler.RegisterStatusHandler("busy", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Line busy", "call_sid", event.CallSID)
		return nil
	})

	s.webhookHandler.RegisterStatusHandler("completed", func(c *gin.Context, event twilio.CallEvent) error {
		return s.handleCallCompleted(c, event)
	})

	s.webhookHandler.RegisterStatusHandler("failed", func(c *gin.Context, event twilio.CallEvent) error {
		return s.handleCallFailed(c, event)
	})

	s.webhookHandler.RegisterStatusHandler("canceled", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call canceled", "call_sid", event.CallSID)
		return nil
	})
}

// handleIncomingCall generates TwiML for incoming calls
func (s *Service) handleIncomingCall(c *gin.Context, event twilio.CallEvent) (string, error) {
	callSID := event.CallSID
	logger.Info("Handling incoming call",
		"call_sid", callSID,
		"from", event.From,
		"status", event.Status)

	s.mu.Lock()
	session, exists := s.activeStreamingSessions[callSID]
	if !exists {
		conversationID := fmt.Sprintf("conv_%d", time.Now().UnixNano())
		patientPhone := event.From

		logger.Info("Creating new conversation for call",
			"call_sid", callSID,
			"patient_phone", patientPhone)

		conversation, err := s.conversationManager.StartConversation(patientPhone)
		if err != nil {
			s.mu.Unlock()
			logger.Error("Failed to start conversation for incoming call", "error", err)
			return twilio.GenerateTwiML(
				twilio.SayAction("Sorry, we're experiencing technical difficulties. Please try again later.", "alice", "en-US"),
			), nil
		}

		conversationID = conversation.ID
		logger.Info("Created new conversation",
			"conversation_id", conversationID,
			"call_sid", callSID)

		session = StreamingSession{
			CallSID:        callSID,
			ConversationID: conversationID,
			IsActive:       true,
			LastActivity:   time.Now(),
			PatientPhone:   patientPhone,
		}
		s.activeStreamingSessions[callSID] = session
	}
	session.LastActivity = time.Now()
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	logger.Info("Getting AI response for conversation",
		"call_sid", callSID,
		"conversation_id", session.ConversationID)

	aiResponse, err := s.conversationManager.GetNextResponse(session.ConversationID)
	if err != nil {
		logger.Error("Failed to get AI response", "error", err)
		return twilio.GenerateTwiML(
			twilio.SayAction("Sorry, I'm having trouble processing that. Let me try again.", "alice", "en-US"),
			twilio.GatherAction("", map[string]string{
				"input":         "speech",
				"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
				"language":      "en-US",
				"speechTimeout": "auto",
			}),
		), nil
	}

	logger.Info("Got AI response",
		"call_sid", callSID,
		"response_length", len(aiResponse))

	voiceOptions := &elevenlabs.VoiceOptions{
		Stability:       0.5,
		SimilarityBoost: 0.75,
		Style:           0.0,
		SpeakerBoost:    true,
	}

	audioResponse, err := s.elevenLabsClient.GenerateAudio(aiResponse, voiceOptions)
	if err != nil {
		logger.Warn("Failed to generate audio, falling back to Twilio voice",
			"call_sid", callSID,
			"error", err)

		return twilio.GenerateTwiML(
			twilio.SayAction(aiResponse, "alice", "en-US"),
			twilio.GatherAction("", map[string]string{
				"input":         "speech",
				"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
				"language":      "en-US",
				"speechTimeout": "auto",
			}),
		), nil
	}

	logger.Info("Generated audio response",
		"call_sid", callSID,
		"audio_size", len(audioResponse.AudioBytes))

	audioURL := fmt.Sprintf("%s/api/call/audio/%s", s.baseURL, callSID)

	req, err := http.NewRequest("POST", audioURL, bytes.NewBuffer(audioResponse.AudioBytes))
	if err != nil {
		logger.Error("Failed to create request to store audio", "error", err)
		return twilio.GenerateTwiML(
			twilio.SayAction(aiResponse, "alice", "en-US"),
			twilio.GatherAction("", map[string]string{
				"input":         "speech",
				"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
				"language":      "en-US",
				"speechTimeout": "auto",
			}),
		), nil
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Failed to store audio", "error", err)
		return twilio.GenerateTwiML(
			twilio.SayAction(aiResponse, "alice", "en-US"),
			twilio.GatherAction("", map[string]string{
				"input":         "speech",
				"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
				"language":      "en-US",
				"speechTimeout": "auto",
			}),
		), nil
	}
	defer resp.Body.Close()

	logger.Info("Successfully stored audio and generating TwiML response",
		"call_sid", callSID)

	return twilio.GenerateTwiML(
		twilio.PlayAction(audioURL),
		twilio.GatherAction("", map[string]string{
			"input":         "speech",
			"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
			"language":      "en-US",
			"speechTimeout": "auto",
		}),
	), nil
}

// handleCallCompleted processes a completed call
func (s *Service) handleCallCompleted(c *gin.Context, event twilio.CallEvent) error {
	callSID := event.CallSID

	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for completed call", "call_sid", callSID)
		return nil
	}

	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	}

	go func() {
		if _, err := s.ProcessCallResults(callSID); err != nil {
			logger.Error("Failed to process call results", "error", err)
		}
	}()

	return nil
}

// handleCallFailed processes a failed call
func (s *Service) handleCallFailed(c *gin.Context, event twilio.CallEvent) error {
	callSID := event.CallSID

	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for failed call", "call_sid", callSID)
		return nil
	}

	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	}

	logger.Error("Call failed",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"error_code", event.ErrorCode,
		"error_message", event.ErrorMsg)

	return nil
}

// HandleSpeechInput processes speech input from a call
func (s *Service) HandleSpeechInput(callSID string, speechText string) (string, error) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Error("No active session for call", "call_sid", callSID)

		// Try to recover by creating a new session
		logger.Info("Attempting to recover session", "call_sid", callSID)

		// Create a fallback conversation
		conversation, err := s.conversationManager.StartConversation("unknown")
		if err != nil {
			return "", fmt.Errorf("failed to create recovery conversation: %w", err)
		}

		newSession := StreamingSession{
			CallSID:        callSID,
			ConversationID: conversation.ID,
			IsActive:       true,
			LastActivity:   time.Now(),
			PatientPhone:   "unknown",
		}

		s.mu.Lock()
		s.activeStreamingSessions[callSID] = newSession
		s.mu.Unlock()

		logger.Info("Created recovery session",
			"call_sid", callSID,
			"conversation_id", conversation.ID)

		session = newSession
	}

	logger.Info("Processing speech input",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"input", speechText)

	if err := s.conversationManager.AddMessage(session.ConversationID, "patient", speechText, 0.9); err != nil {
		logger.Error("Failed to add patient message", "error", err)
		return "", err
	}

	s.mu.Lock()
	session.LastActivity = time.Now()
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	// Try up to 3 times to get a response
	var aiResponse string
	var err error
	maxRetries := 3

	for i := 0; i < maxRetries; i++ {
		aiResponse, err = s.conversationManager.GetNextResponse(session.ConversationID)
		if err == nil {
			break
		}

		logger.Warn("Retry getting AI response",
			"call_sid", callSID,
			"conversation_id", session.ConversationID,
			"attempt", i+1,
			"error", err)

		time.Sleep(500 * time.Millisecond)
	}

	if err != nil {
		logger.Error("Failed to get AI response after retries", "error", err)
		return "I'm sorry, but I'm having trouble understanding right now. Could we try again?", nil
	}

	logger.Info("Successfully generated AI response",
		"call_sid", callSID,
		"response_length", len(aiResponse))

	return aiResponse, nil
}

// CleanupInactiveSessions removes sessions that have been inactive for too long
func (s *Service) CleanupInactiveSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	inactivityThreshold := 15 * time.Minute

	for callSID, session := range s.activeStreamingSessions {
		if now.Sub(session.LastActivity) > inactivityThreshold {
			if session.IsActive {
				if err := s.twilioClient.EndCall(callSID); err != nil {
					logger.Warn("Failed to end inactive call", "error", err, "call_sid", callSID)
				}
			}

			if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
				logger.Warn("Failed to complete conversation for inactive session", "error", err)
			}

			delete(s.activeStreamingSessions, callSID)
			logger.Info("Removed inactive session", "call_sid", callSID)
		}
	}
}

// StartCleanupScheduler starts a goroutine to periodically clean up inactive sessions
func (s *Service) StartCleanupScheduler(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			s.CleanupInactiveSessions()
		}
	}()
}

// getMapKeys is a helper function to get the keys from a map
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Add to internal/communication/call/service.go:
func (s *Service) GenerateAndStoreAudioResponse(callSID string, text string) ([]byte, error) {
	voiceOptions := &elevenlabs.VoiceOptions{
		Stability:       0.5,
		SimilarityBoost: 0.75,
		Style:           0.0,
		SpeakerBoost:    true,
	}

	// Generate audio
	audioResponse, err := s.elevenLabsClient.GenerateAudio(text, voiceOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to generate audio: %w", err)
	}

	// Store the audio data via API
	audioURL := fmt.Sprintf("%s/api/call/audio/%s", s.baseURL, callSID)

	req, err := http.NewRequest("POST", audioURL, bytes.NewBuffer(audioResponse.AudioBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to store audio: %w", err)
	}
	defer resp.Body.Close()

	return audioResponse.AudioBytes, nil
}

// Add getter for baseURL
func (s *Service) GetBaseURL() string {
	return s.baseURL
}

// StoreMessage stores a message in the conversation
func (s *Service) StoreMessage(callSID string, speaker string, text string) error {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no active session for call %s", callSID)
	}

	return s.conversationManager.AddMessage(session.ConversationID, speaker, text, 0.9)
}

// GetLastAIResponse gets the last AI response for a call
func (s *Service) GetLastAIResponse(callSID string) (string, error) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("no active session for call %s", callSID)
	}

	messages, err := s.conversationManager.GetConversationMessages(session.ConversationID)
	if err != nil {
		return "", err
	}

	// Find the last AI message
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Speaker == "ai" {
			return messages[i].Text, nil
		}
	}

	return "", fmt.Errorf("no AI messages found")
}
