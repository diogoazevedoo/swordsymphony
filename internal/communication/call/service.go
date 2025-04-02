package call

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/deepgram"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/elevenlabs"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/email"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/conversation"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
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
	LastAudioDuration float64
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

	// If call is already marked as inactive, just return
	if !session.IsActive {
		s.mu.Unlock()
		logger.Info("Call already marked as inactive", "call_sid", callSID)
		return nil
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

	// Mark the conversation as complete - don't return error if this fails
	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err, "conversation_id", session.ConversationID)
	}

	logger.Info("Ended call", "call_sid", callSID)

	// End the Twilio call - this might fail if the call is already ended
	err := s.twilioClient.EndCall(callSID)
	if err != nil {
		// Log but don't return error if it's likely the call is already ended
		if strings.Contains(err.Error(), "already completed") ||
			strings.Contains(err.Error(), "not found") {
			logger.Warn("Call may already be ended", "call_sid", callSID, "error", err)
			return nil
		}
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

	// Ensure we have all medical data before proceeding with workflow
	s.processPatientDataForWorkflow(session.ConversationID)

	// Process the conversation data
	processedData, err := s.conversationManager.ProcessConversationData(session.ConversationID)
	if err != nil {
		logger.Error("Failed to process conversation data", "error", err, "conversation_id", session.ConversationID)
		return nil, fmt.Errorf("failed to process conversation data: %w", err)
	}

	// Get patient data and standardize format
	patientData, ok := processedData["patient_data"].(map[string]any)
	if !ok {
		logger.Error("Invalid patient data format", "conversation_id", session.ConversationID)
		return nil, fmt.Errorf("invalid patient data format")
	}

	// Standardize the patient data to match demo format exactly
	patientData = standardizePatientData(patientData)

	// Create a proper case ID using the patient ID
	caseID := fmt.Sprintf("%v", patientData["id"])
	if caseID == "" {
		caseID = fmt.Sprintf("P%d", time.Now().Unix())
		patientData["id"] = caseID
	}

	// Log the patient data
	logger.Info("Processed patient data",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"case_id", caseID,
		"data_keys", getMapKeys(patientData),
		"name", patientData["name"],
		"age", patientData["age"],
		"gender", patientData["gender"],
		"symptoms_count", len(patientData["symptoms"].([]string)))

	// Store the case - make sure we have a proper repository
	var resultRepo repository.ResultRepository = s.resultRepository

	// Store the case first to ensure it exists
	if resultRepo != nil {
		if caseRepo, ok := resultRepo.(repository.CaseRepository); ok {
			err := caseRepo.StoreCase(caseID, patientData, false)
			if err != nil {
				logger.Error("Failed to store case with result repository", "error", err)
			} else {
				logger.Info("Successfully stored case with result repository", "case_id", caseID)
			}
		} else {
			logger.Warn("Result repository does not implement CaseRepository interface",
				"case_id", caseID)
		}
	} else {
		logger.Warn("No result repository available to store case", "case_id", caseID)
	}

	// Select workflow based on patient data
	workflowID := "standard_diagnostic_workflow"

	// Try to select a workflow if the service is available
	if s.workflowService != nil {
		// Multiple attempts to select workflow with retries
		for attempt := 0; attempt < 3; attempt++ {
			selectedID, err := s.workflowService.SelectWorkflow(patientData)
			if err == nil && selectedID != "" {
				workflowID = selectedID
				logger.Info("Selected workflow based on patient data",
					"workflow_id", workflowID,
					"case_id", caseID,
					"attempt", attempt+1)
				break
			}

			if attempt < 2 {
				logger.Warn("Failed to select workflow, retrying",
					"error", err,
					"attempt", attempt+1)
				time.Sleep(500 * time.Millisecond)
			} else {
				logger.Warn("Failed to select workflow after retries, using default",
					"error", err,
					"default_workflow", workflowID)
			}
		}
	} else {
		logger.Warn("No workflow service available, using default workflow",
			"default_workflow", workflowID)
	}

	// Create input data for the workflow
	inputData := map[string]any{
		"patient_data": patientData,
		"case_id":      caseID,
	}

	// Initialize UUID for the instance
	instanceID := uuid.Nil

	// Start the workflow via API with retries
	for attempt := 0; attempt < 3; attempt++ {
		workflowURL := fmt.Sprintf("%s/api/management/workflows/%s/instances", s.baseURL, workflowID)
		jsonData, err := json.Marshal(inputData)
		if err != nil {
			logger.Error("Failed to marshal workflow input data", "error", err)
			break
		}

		// Create the request with proper content type
		req, err := http.NewRequest("POST", workflowURL, bytes.NewBuffer(jsonData))
		if err != nil {
			logger.Error("Failed to create workflow request", "error", err)
			break
		}

		req.Header.Set("Content-Type", "application/json")

		// Use a client with reasonable timeout
		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)

		if err != nil {
			logger.Error("Failed to send workflow request",
				"error", err,
				"attempt", attempt+1)

			if attempt < 2 {
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}

		defer resp.Body.Close()

		// Check if the request was successful
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			logger.Info("Successfully started workflow via API",
				"workflow_id", workflowID,
				"case_id", caseID,
				"status_code", resp.StatusCode,
				"attempt", attempt+1)

			// Try to get the instance ID from the response
			var apiResponse struct {
				InstanceID string `json:"instance_id"`
			}

			if decodeErr := json.NewDecoder(resp.Body).Decode(&apiResponse); decodeErr != nil {
				logger.Warn("Failed to decode workflow response", "error", decodeErr)
			} else if apiResponse.InstanceID != "" {
				if id, parseErr := uuid.Parse(apiResponse.InstanceID); parseErr == nil {
					instanceID = id
					logger.Info("Got workflow instance ID", "instance_id", instanceID)
				} else {
					logger.Warn("Failed to parse instance ID", "error", parseErr)
				}
			} else {
				logger.Warn("No instance ID in workflow response")
			}

			// Success - break the retry loop
			break
		} else {
			// Log details about unsuccessful response
			respBody, _ := io.ReadAll(resp.Body)
			logger.Error("Failed to start workflow via API",
				"workflow_id", workflowID,
				"case_id", caseID,
				"status_code", resp.StatusCode,
				"response", string(respBody),
				"attempt", attempt+1)

			if attempt < 2 {
				time.Sleep(1 * time.Second)
				continue
			}
		}
	}

	// Initialize empty diagnosis and treatment data
	diagnosisData := make(map[string]any)
	treatmentData := make(map[string]any)

	// Try to get existing results if available
	if resultRepo != nil {
		results, err := resultRepo.GetResultsByCaseID(caseID)
		if err != nil {
			logger.Warn("Failed to get existing results", "error", err, "case_id", caseID)
		} else if results != nil {
			// Extract diagnosis data
			if diagnosis, ok := results["diagnosis"].(map[string]any); ok && len(diagnosis) > 0 {
				diagnosisData = diagnosis
				logger.Info("Found existing diagnosis data", "case_id", caseID)
			}

			// Extract treatment data
			if treatment, ok := results["treatment_plan"].(map[string]any); ok && len(treatment) > 0 {
				treatmentData = treatment
				logger.Info("Found existing treatment data", "case_id", caseID)
			}
		}
	}

	// Prepare result data structure with all available information
	resultData := map[string]any{
		"patient_data":   patientData,
		"diagnosis":      diagnosisData,
		"treatment_plan": treatmentData,
		"workflow_id":    workflowID,
		"instance_id":    instanceID.String(),
		"completed_at":   time.Now().Format(time.RFC3339),
	}

	// Store results, even if we don't have a workflow instance yet
	if resultRepo != nil {
		// Try storing results with retry
		var storeErr error
		for attempt := 0; attempt < 3; attempt++ {
			storeErr = resultRepo.StoreResults(caseID, resultData)
			if storeErr == nil {
				logger.Info("Successfully stored call results",
					"case_id", caseID,
					"attempt", attempt+1)
				break
			}

			logger.Error("Failed to store call results",
				"error", storeErr,
				"case_id", caseID,
				"attempt", attempt+1)

			if attempt < 2 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	} else {
		logger.Warn("No result repository available to store results", "case_id", caseID)
	}

	// Create and return the result object
	callResult := &CallResult{
		CallSID:        callSID,
		ConversationID: session.ConversationID,
		PatientData:    patientData,
		Diagnosis:      diagnosisData,
		Treatment:      treatmentData,
		WorkflowID:     workflowID,
		InstanceID:     instanceID,
		CompletedAt:    time.Now(),
	}

	logger.Info("Completed call result processing successfully",
		"call_sid", callSID,
		"case_id", caseID,
		"workflow_id", workflowID,
		"has_instance_id", instanceID != uuid.Nil)

	return callResult, nil
}

func standardizePatientData(data map[string]any) map[string]any {
	// Create result with exact format matching demo cases
	result := map[string]any{
		"id":          "",
		"name":        "",
		"age":         0.0,
		"gender":      "",
		"symptoms":    []string{},
		"conditions":  []string{},
		"medications": []string{},
		"allergies":   []string{},
		"vitals": map[string]any{
			"blood_pressure":    "",
			"heart_rate":        0.0,
			"temperature":       0.0,
			"oxygen_saturation": 0.0,
		},
	}

	// If we have an existing ID, use it
	if id, ok := data["id"].(string); ok && id != "" {
		result["id"] = id
	} else if id, ok := data["patient_id"].(string); ok && id != "" {
		result["id"] = id
	} else {
		// Generate a P-prefixed ID
		result["id"] = fmt.Sprintf("P%d", time.Now().Unix())
	}

	// Copy name
	if name, ok := data["name"].(string); ok && name != "" {
		result["name"] = name
	} else if name, ok := data["patient_name"].(string); ok && name != "" {
		result["name"] = name
	}

	// Copy age, ensuring correct type
	if age, ok := data["age"].(float64); ok {
		result["age"] = age
	} else if age, ok := data["age"].(int); ok {
		result["age"] = float64(age)
	} else if ageStr, ok := data["age"].(string); ok {
		if ageVal, err := strconv.ParseFloat(ageStr, 64); err == nil {
			result["age"] = ageVal
		}
	}

	// Copy gender
	if gender, ok := data["gender"].(string); ok {
		result["gender"] = gender
	}

	// Process string array fields
	copyStringArray(data, "symptoms", result)
	copyStringArray(data, "conditions", result)
	copyStringArray(data, "medications", result)
	copyStringArray(data, "allergies", result)

	// Process vitals
	vitals := result["vitals"].(map[string]any)
	if srcVitals, ok := data["vitals"].(map[string]any); ok {
		// Blood pressure
		if bp, ok := srcVitals["blood_pressure"].(string); ok && bp != "" {
			vitals["blood_pressure"] = bp
		}

		// Heart rate
		if hr, ok := srcVitals["heart_rate"].(float64); ok {
			vitals["heart_rate"] = hr
		} else if hr, ok := srcVitals["heart_rate"].(int); ok {
			vitals["heart_rate"] = float64(hr)
		} else if hrStr, ok := srcVitals["heart_rate"].(string); ok {
			if hrVal, err := strconv.ParseFloat(hrStr, 64); err == nil {
				vitals["heart_rate"] = hrVal
			}
		}

		// Temperature
		if temp, ok := srcVitals["temperature"].(float64); ok {
			vitals["temperature"] = temp
		} else if temp, ok := srcVitals["temperature"].(int); ok {
			vitals["temperature"] = float64(temp)
		} else if tempStr, ok := srcVitals["temperature"].(string); ok {
			if tempVal, err := strconv.ParseFloat(tempStr, 64); err == nil {
				vitals["temperature"] = tempVal
			}
		}

		// Oxygen saturation
		if o2, ok := srcVitals["oxygen_saturation"].(float64); ok {
			vitals["oxygen_saturation"] = o2
		} else if o2, ok := srcVitals["oxygen_saturation"].(int); ok {
			vitals["oxygen_saturation"] = float64(o2)
		} else if o2Str, ok := srcVitals["oxygen_saturation"].(string); ok {
			if o2Val, err := strconv.ParseFloat(o2Str, 64); err == nil {
				vitals["oxygen_saturation"] = o2Val
			}
		}
	}

	// Save original conversation data if needed but don't expose in main structure
	if convID, ok := data["conversation_id"].(string); ok {
		result["_conversation_id"] = convID
	}

	logger.Info("Standardized patient data",
		"original_keys", getMapKeys(data),
		"standardized_keys", getMapKeys(result))

	return result
}

// Helper functions for standardization
func ensureField(data map[string]any, field string, defaultValue any) {
	if data[field] == nil {
		data[field] = defaultValue
	}
}

func ensureStringArrayField(data map[string]any, field string) {
	if data[field] == nil {
		data[field] = []string{}
		return
	}

	// Convert from []any to []string if needed
	switch value := data[field].(type) {
	case []string:
		// Already the right type
		return
	case []interface{}:
		strArray := make([]string, 0, len(value))
		for _, item := range value {
			switch v := item.(type) {
			case string:
				strArray = append(strArray, v)
			default:
				strArray = append(strArray, fmt.Sprintf("%v", v))
			}
		}
		data[field] = strArray
	case string:
		// Single string, convert to array
		data[field] = []string{value}
	default:
		// Unrecognized type, reset to empty array
		data[field] = []string{}
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

		conversationID := conversation.ID
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

	// Ensure patient ID is set
	patientID := fmt.Sprintf("P%d", time.Now().Unix())
	s.conversationManager.SetKey(session.ConversationID, "id", patientID)
	s.conversationManager.SetKey(session.ConversationID, "patient_id", patientID)

	// Get the next response based on conversation state
	aiResponse, err := s.conversationManager.GetNextResponse(session.ConversationID)
	if err != nil {
		logger.Error("Failed to get AI response", "error", err)
		return twilio.GenerateTwiML(
			twilio.SayAction("Sorry, I'm having trouble at the moment. Please try again later.", "alice", "en-US"),
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
		"conversation_id", session.ConversationID,
		"response_length", len(aiResponse))

	// Generate audio for the response
	audioBytes, err := s.generateAndStoreResponseAudio(callSID, aiResponse)
	if err != nil {
		// If audio generation fails, fall back to Twilio's default voice
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

	// If audio generation succeeded, play the audio file
	audioURL := fmt.Sprintf("%s/api/call/audio/%s", s.baseURL, callSID)

	logger.Info("Successfully generated TwiML response with audio",
		"call_sid", callSID,
		"audio_size", len(audioBytes))

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

// generateAndStoreResponseAudio generates audio for the AI response and stores it
func (s *Service) generateAndStoreResponseAudio(callSID string, text string) ([]byte, error) {
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

	// Store audio duration for timing calculations
	s.mu.Lock()
	if session, exists := s.activeStreamingSessions[callSID]; exists {
		session.LastAudioDuration = audioResponse.Duration
		s.activeStreamingSessions[callSID] = session
	}
	s.mu.Unlock()

	// Store the audio data
	audioURL := fmt.Sprintf("%s/api/call/audio/%s", s.baseURL, callSID)
	req, err := http.NewRequest("POST", audioURL, bytes.NewBuffer(audioResponse.AudioBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request to store audio: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to store audio: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("audio storage API returned status code %d", resp.StatusCode)
	}

	logger.Info("Stored audio response",
		"call_sid", callSID,
		"audio_size", len(audioResponse.AudioBytes),
		"duration", audioResponse.Duration)

	return audioResponse.AudioBytes, nil
}

// handleCallCompleted processes a completed call
func (s *Service) handleCallCompleted(c *gin.Context, event twilio.CallEvent) error {
	callSID := event.CallSID

	logger.Info("Call completed notification received",
		"call_sid", callSID,
		"status", event.Status)

	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for completed call", "call_sid", callSID)
		return nil
	}

	// Set session to inactive
	s.mu.Lock()
	session.IsActive = false
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	// Ensure we have all patient data before completing the conversation
	s.processPatientDataForWorkflow(session.ConversationID)

	// Mark the conversation as complete
	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	} else {
		logger.Info("Marked conversation as complete",
			"conversation_id", session.ConversationID,
			"call_sid", callSID)
	}

	// Process the call results in a separate goroutine with retries
	go func() {
		// Wait a moment to ensure all data is processed
		time.Sleep(2 * time.Second)

		// Check if session still exists
		s.mu.RLock()
		_, sessionExists := s.activeStreamingSessions[callSID]
		s.mu.RUnlock()

		if !sessionExists {
			logger.Warn("Session no longer exists while processing completed call",
				"call_sid", callSID)
			return
		}

		var callResult *CallResult
		var err error

		// Try up to 3 times to process the results
		for attempt := 0; attempt < 3; attempt++ {
			callResult, err = s.ProcessCallResults(callSID)
			if err == nil && callResult != nil {
				logger.Info("Successfully processed call results",
					"call_sid", callSID,
					"patient_id", callResult.PatientData["id"],
					"workflow_id", callResult.WorkflowID,
					"has_instance_id", callResult.InstanceID != uuid.Nil)

				// If we successfully processed the results, we can remove the session
				if attempt == 2 {
					// Only remove on the final attempt to avoid race conditions
					s.mu.Lock()
					delete(s.activeStreamingSessions, callSID)
					s.mu.Unlock()
					logger.Info("Removed completed call session", "call_sid", callSID)
				}

				break
			}

			logger.Warn("Retrying call result processing",
				"call_sid", callSID,
				"attempt", attempt+1,
				"error", err)

			// Increase wait time between retries
			time.Sleep(time.Duration(2*(attempt+1)) * time.Second)
		}

		if err != nil || callResult == nil {
			logger.Error("Failed to process call results after retries",
				"error", err,
				"call_sid", callSID)
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

// HandleSpeechInput processes patient speech input and returns the next AI response
func (s *Service) HandleSpeechInput(callSID string, speechText string) (string, error) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Error("No active session for call", "call_sid", callSID)
		return "I'm sorry, I can't process your request right now.", nil
	}

	logger.Info("Handling speech input",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"input", speechText)

	// Update session activity timestamp
	s.mu.Lock()
	session.LastActivity = time.Now()
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	// Extract medical data from patient input - more robust approach
	s.extractAndStorePatientData(session.ConversationID, speechText)

	// Store the patient's message with minimal processing wait time
	// This simplified approach avoids the race conditions
	err := s.conversationManager.AddMessage(session.ConversationID, "patient", speechText, 0.9)
	if err != nil {
		logger.Error("Failed to store patient message", "error", err)
	}

	// Force the conversation to advance to the next state
	// The conversationManager.GetNextResponse method will now use message count
	// instead of conversation state to determine the next question
	aiResponse, err := s.conversationManager.GetNextResponse(session.ConversationID)
	if err != nil {
		logger.Error("Failed to get AI response", "error", err)
		return "Let's continue with the next question.", nil
	}

	logger.Info("Generated AI response",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"response_length", len(aiResponse))

	// Process conversation data after each interaction to ensure it's ready for workflow
	s.processPatientDataForWorkflow(session.ConversationID)

	// Check if conversation is complete and trigger workflow if needed
	go s.CheckConversationCompletion(callSID)

	return aiResponse, nil
}

// extractAndStorePatientData extracts medical information from patient input
// and stores it directly in the conversation data, regardless of conversation state
func (s *Service) extractAndStorePatientData(conversationID string, text string) {
	textLower := strings.ToLower(text)

	// Extract name if present (look for "my name is" or similar patterns)
	if strings.Contains(textLower, "name is") || strings.Contains(textLower, "call me") {
		words := strings.Fields(text)

		// Find the position of "name" and "is"
		nameIdx := -1
		isIdx := -1

		for i, word := range words {
			if strings.Contains(strings.ToLower(word), "name") {
				nameIdx = i
			} else if nameIdx != -1 && strings.ToLower(word) == "is" {
				isIdx = i
				break
			}
		}

		if nameIdx != -1 && isIdx != -1 && isIdx+1 < len(words) {
			// Take up to 3 words after "is" for the name
			endIdx := min(isIdx+4, len(words))
			potentialName := strings.Join(words[isIdx+1:endIdx], " ")
			potentialName = strings.Trim(potentialName, ".,!?;:")

			// Store the name in both standard fields
			s.conversationManager.SetKey(conversationID, "name", potentialName)
			s.conversationManager.SetKey(conversationID, "patient_name", potentialName)

			logger.Info("Extracted patient name",
				"name", potentialName,
				"conversation_id", conversationID)
		}
	}

	// Extract age (any number between 1-120)
	for _, word := range strings.Fields(textLower) {
		word = strings.Trim(word, ".,!?;:")
		if num, err := strconv.ParseFloat(word, 64); err == nil && num > 0 && num < 120 {
			s.conversationManager.SetKey(conversationID, "age", num)
			logger.Info("Extracted patient age",
				"age", num,
				"conversation_id", conversationID)
			break
		}
	}

	// Extract gender
	if strings.Contains(textLower, "male") && !strings.Contains(textLower, "female") {
		s.conversationManager.SetKey(conversationID, "gender", "male")
		logger.Info("Extracted patient gender", "gender", "male")
	} else if strings.Contains(textLower, "female") {
		s.conversationManager.SetKey(conversationID, "gender", "female")
		logger.Info("Extracted patient gender", "gender", "female")
	} else if strings.Contains(textLower, "non-binary") || strings.Contains(textLower, "nonbinary") ||
		strings.Contains(textLower, "other") {
		s.conversationManager.SetKey(conversationID, "gender", "other")
		logger.Info("Extracted patient gender", "gender", "other")
	}

	// Extract symptoms
	symptomKeywords := []string{
		"pain", "ache", "fever", "headache", "cough", "nausea", "vomiting",
		"dizziness", "fatigue", "tired", "exhausted", "short of breath",
		"difficulty breathing", "chest pain", "sore throat", "congestion",
		"runny nose", "rash", "itching", "swelling", "dizzy",
		"stomachache", "stomach pain", "back pain", "joint pain",
		"migraine", "seizure", "bleeding", "numbness", "tingling",
		"weakness", "chills", "sweating", "insomnia", "anxiety", "depression",
	}

	extractedSymptoms := []string{}
	for _, symptom := range symptomKeywords {
		if strings.Contains(textLower, symptom) {
			extractedSymptoms = append(extractedSymptoms, symptom)
		}
	}

	// Store extracted symptoms
	if len(extractedSymptoms) > 0 {
		// Get existing symptoms
		var symptoms []interface{}
		existingSymptoms, exists := s.conversationManager.GetKey(conversationID, "symptoms")
		if exists {
			if existingArray, ok := existingSymptoms.([]interface{}); ok {
				symptoms = existingArray
			} else {
				symptoms = []interface{}{}
			}
		} else {
			symptoms = []interface{}{}
		}

		// Add new symptoms and deduplicate
		for _, symptom := range extractedSymptoms {
			// Check if symptom already exists
			exists := false
			for _, existing := range symptoms {
				if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, symptom) {
					exists = true
					break
				}
			}

			if !exists {
				symptoms = append(symptoms, symptom)
			}
		}

		s.conversationManager.SetKey(conversationID, "symptoms", symptoms)
		logger.Info("Extracted patient symptoms",
			"symptoms", extractedSymptoms,
			"conversation_id", conversationID)
	}

	// Handle "no symptoms" response
	if (strings.Contains(textLower, "no symptoms") ||
		strings.Contains(textLower, "not experiencing") ||
		strings.Contains(textLower, "don't have any")) &&
		(strings.Contains(textLower, "symptom") || strings.Contains(textLower, "issue")) {
		s.conversationManager.SetKey(conversationID, "symptoms", []interface{}{})
		logger.Info("Patient reported no symptoms", "conversation_id", conversationID)
	}

	// Extract medical conditions
	conditionKeywords := []string{
		"diabetes", "asthma", "hypertension", "high blood pressure", "low blood pressure",
		"depression", "anxiety", "arthritis", "heart disease", "copd", "coronary",
		"cancer", "stroke", "thyroid", "high cholesterol", "gastritis", "ulcer",
		"kidney disease", "liver disease", "epilepsy", "seizure disorder",
		"multiple sclerosis", "parkinsons", "alzheimers", "crohns", "colitis",
		"fibromyalgia", "lupus", "hiv", "aids", "hepatitis", "dementia",
	}

	extractedConditions := []string{}
	for _, condition := range conditionKeywords {
		if strings.Contains(textLower, condition) {
			extractedConditions = append(extractedConditions, condition)
		}
	}

	// Store extracted conditions
	if len(extractedConditions) > 0 {
		// Get existing conditions
		var conditions []interface{}
		existingConditions, exists := s.conversationManager.GetKey(conversationID, "conditions")
		if exists {
			if existingArray, ok := existingConditions.([]interface{}); ok {
				conditions = existingArray
			} else {
				conditions = []interface{}{}
			}
		} else {
			conditions = []interface{}{}
		}

		// Add new conditions and deduplicate
		for _, condition := range extractedConditions {
			// Check if condition already exists
			exists := false
			for _, existing := range conditions {
				if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, condition) {
					exists = true
					break
				}
			}

			if !exists {
				conditions = append(conditions, condition)
			}
		}

		s.conversationManager.SetKey(conversationID, "conditions", conditions)
		logger.Info("Extracted patient conditions",
			"conditions", extractedConditions,
			"conversation_id", conversationID)
	}

	// Handle "no conditions" response
	if strings.Contains(textLower, "no conditions") ||
		strings.Contains(textLower, "no medical conditions") ||
		(strings.Contains(textLower, "don't have") && strings.Contains(textLower, "condition")) ||
		(strings.Contains(textLower, "do not have") && strings.Contains(textLower, "condition")) {
		s.conversationManager.SetKey(conversationID, "conditions", []interface{}{})
		logger.Info("Patient reported no conditions", "conversation_id", conversationID)
	}

	// Extract medications
	medicationKeywords := []string{
		"aspirin", "tylenol", "ibuprofen", "advil", "motrin", "acetaminophen",
		"metformin", "insulin", "lisinopril", "losartan", "atorvastatin",
		"lipitor", "simvastatin", "zoloft", "prozac", "lexapro", "citalopram",
		"xanax", "albuterol", "ventolin", "inhaler", "prednisone", "steroid",
		"antibiotic", "levothyroxine", "synthroid", "metoprolol", "amlodipine",
		"omeprazole", "nexium", "protonix", "furosemide", "lasix", "digoxin",
		"warfarin", "coumadin", "gabapentin", "neurontin", "hydrocodone", "oxycodone",
	}

	extractedMedications := []string{}
	for _, medication := range medicationKeywords {
		if strings.Contains(textLower, medication) {
			extractedMedications = append(extractedMedications, medication)
		}
	}

	// Store extracted medications
	if len(extractedMedications) > 0 {
		// Get existing medications
		var medications []interface{}
		existingMedications, exists := s.conversationManager.GetKey(conversationID, "medications")
		if exists {
			if existingArray, ok := existingMedications.([]interface{}); ok {
				medications = existingArray
			} else {
				medications = []interface{}{}
			}
		} else {
			medications = []interface{}{}
		}

		// Add new medications and deduplicate
		for _, medication := range extractedMedications {
			// Check if medication already exists
			exists := false
			for _, existing := range medications {
				if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, medication) {
					exists = true
					break
				}
			}

			if !exists {
				medications = append(medications, medication)
			}
		}

		s.conversationManager.SetKey(conversationID, "medications", medications)
		logger.Info("Extracted patient medications",
			"medications", extractedMedications,
			"conversation_id", conversationID)
	}

	// Handle "no medications" response
	if strings.Contains(textLower, "no medications") ||
		strings.Contains(textLower, "not taking") ||
		strings.Contains(textLower, "don't take any") ||
		strings.Contains(textLower, "do not take any") {
		s.conversationManager.SetKey(conversationID, "medications", []interface{}{})
		logger.Info("Patient reported no medications", "conversation_id", conversationID)
	}

	// Extract allergies
	allergyKeywords := []string{
		"penicillin", "peanuts", "tree nuts", "shellfish", "fish",
		"milk", "dairy", "eggs", "wheat", "gluten", "soy", "latex",
		"bee stings", "pollen", "dust", "mold", "cats", "dogs",
		"sulfa", "amoxicillin", "aspirin", "ibuprofen", "nsaids",
		"anaphylaxis", "hives", "rash", "swelling",
	}

	extractedAllergies := []string{}
	for _, allergy := range allergyKeywords {
		if strings.Contains(textLower, allergy) &&
			(strings.Contains(textLower, "allerg") || strings.Contains(textLower, "reaction")) {
			extractedAllergies = append(extractedAllergies, allergy)
		}
	}

	// Store extracted allergies
	if len(extractedAllergies) > 0 {
		// Get existing allergies
		var allergies []interface{}
		existingAllergies, exists := s.conversationManager.GetKey(conversationID, "allergies")
		if exists {
			if existingArray, ok := existingAllergies.([]interface{}); ok {
				allergies = existingArray
			} else {
				allergies = []interface{}{}
			}
		} else {
			allergies = []interface{}{}
		}

		// Add new allergies and deduplicate
		for _, allergy := range extractedAllergies {
			// Check if allergy already exists
			exists := false
			for _, existing := range allergies {
				if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, allergy) {
					exists = true
					break
				}
			}

			if !exists {
				allergies = append(allergies, allergy)
			}
		}

		s.conversationManager.SetKey(conversationID, "allergies", allergies)
		logger.Info("Extracted patient allergies",
			"allergies", extractedAllergies,
			"conversation_id", conversationID)
	}

	// Handle "no allergies" response
	if strings.Contains(textLower, "no allergies") ||
		strings.Contains(textLower, "not allergic") ||
		strings.Contains(textLower, "don't have any allergies") ||
		strings.Contains(textLower, "do not have any allergies") {
		s.conversationManager.SetKey(conversationID, "allergies", []interface{}{})
		logger.Info("Patient reported no allergies", "conversation_id", conversationID)
	}
}

// processPatientDataForWorkflow ensures necessary data is present and
// structured correctly for workflow triggering
func (s *Service) processPatientDataForWorkflow(conversationID string) {
	conversation, exists := s.conversationManager.GetConversation(conversationID)
	if !exists {
		logger.Error("Failed to get conversation for data processing",
			"conversation_id", conversationID)
		return
	}

	// Ensure all required array fields exist as arrays (even if empty)
	ensureArrayField(s.conversationManager, conversationID, "symptoms")
	ensureArrayField(s.conversationManager, conversationID, "conditions")
	ensureArrayField(s.conversationManager, conversationID, "medications")
	ensureArrayField(s.conversationManager, conversationID, "allergies")

	// Process collected data to ensure a proper structure
	// If patient ID is missing, create one
	patientID, exists := s.conversationManager.GetKey(conversationID, "id")
	if !exists || patientID == nil || patientID == "" {
		newID := fmt.Sprintf("P%d", time.Now().Unix())
		s.conversationManager.SetKey(conversationID, "id", newID)
		logger.Info("Created patient ID", "id", newID, "conversation_id", conversationID)
	}

	// Double check patient name is set (critical field)
	if conversation.PatientName == "" {
		name, exists := s.conversationManager.GetKey(conversationID, "name")
		if exists && name != nil && name != "" {
			if nameStr, ok := name.(string); ok {
				conversation.PatientName = nameStr
			}
		} else {
			name, exists := s.conversationManager.GetKey(conversationID, "patient_name")
			if exists && name != nil && name != "" {
				if nameStr, ok := name.(string); ok {
					conversation.PatientName = nameStr
					s.conversationManager.SetKey(conversationID, "name", nameStr)
				}
			}
		}
	}

	// Ensure patient name is stored in both conversation and data
	if conversation.PatientName != "" {
		s.conversationManager.SetKey(conversationID, "name", conversation.PatientName)
		s.conversationManager.SetKey(conversationID, "patient_name", conversation.PatientName)
	}

	logger.Info("Processed patient data for workflow",
		"conversation_id", conversationID,
		"patient_name", conversation.PatientName,
		"status", string(conversation.Status))
}

// ensureArrayField makes sure a field exists as an array
func ensureArrayField(manager *conversation.ConversationManager, conversationID, field string) {
	value, exists := manager.GetKey(conversationID, field)
	if !exists || value == nil {
		// Field doesn't exist, create empty array
		manager.SetKey(conversationID, field, []interface{}{})
	} else {
		// Check if it's already an array
		_, isArray := value.([]interface{})
		if !isArray {
			// Not an array, convert to empty array
			manager.SetKey(conversationID, field, []interface{}{})
		}
	}
}

// Helper function min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// GenerateAndStoreAudioResponse generates audio for an AI response and stores it
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

	// Set a simple property to track audio duration for our goodbye delay calculation
	s.mu.Lock()
	if session, exists := s.activeStreamingSessions[callSID]; exists {
		session.LastAudioDuration = audioResponse.Duration
		s.activeStreamingSessions[callSID] = session
		logger.Info("Recorded audio duration",
			"call_sid", callSID,
			"duration", audioResponse.Duration,
			"text_length", len(text))
	}
	s.mu.Unlock()

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

	// Add message without waiting for any processing
	err := s.conversationManager.AddMessage(session.ConversationID, speaker, text, 0.9)

	if err != nil {
		return err
	}

	logger.Info("Stored message with deterministic processing",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"speaker", speaker)

	return nil
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

func copyStringArray(src map[string]any, field string, dest map[string]any) {
	if src == nil || dest == nil {
		return
	}

	result := []string{}

	// Try to get the field from source
	if srcVal, ok := src[field]; ok {
		switch v := srcVal.(type) {
		case []string:
			// Direct copy if already string array
			result = make([]string, len(v))
			copy(result, v)
		case []interface{}:
			// Convert interface array to string array
			for _, item := range v {
				if str, ok := item.(string); ok {
					result = append(result, str)
				} else {
					// Convert any non-string to string
					result = append(result, fmt.Sprintf("%v", item))
				}
			}
		case string:
			// Handle single string
			if v != "" {
				result = []string{v}
			}
		}
	}

	dest[field] = result
}

// StoreMessageAndWaitForProcessing - simplified to not wait
func (s *Service) StoreMessageAndWaitForProcessing(callSID string, speaker string, text string) error {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no active session for call %s", callSID)
	}

	// First store the message as normal
	err := s.StoreMessage(callSID, speaker, text)
	if err != nil {
		return err
	}

	// No need to wait since we're forcing progression
	logger.Info("Message stored without waiting",
		"call_sid", callSID,
		"conversation_id", session.ConversationID)

	return nil
}

// CheckConversationCompletion checks if the conversation is complete and ends the call if necessary
// It also triggers workflow processing when the conversation is complete
func (s *Service) CheckConversationCompletion(callSID string) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for call", "call_sid", callSID)
		return
	}

	// Get the conversation - force retry on failure
	var conversation *conversation.Conversation
	var ok bool
	for attempt := 0; attempt < 3; attempt++ {
		conversation, ok = s.conversationManager.GetConversation(session.ConversationID)
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !ok || conversation == nil {
		logger.Error("Failed to get conversation after retries",
			"call_sid", callSID,
			"conversation_id", session.ConversationID)
		return
	}

	// Count messages to determine if we've reached the end of the conversation
	patientMsgCount := 0
	aiMsgCount := 0
	for _, msg := range conversation.Transcript {
		if msg.Speaker == "patient" {
			patientMsgCount++
		} else if msg.Speaker == "ai" {
			aiMsgCount++
		}
	}

	// If we have enough back-and-forth or the conversation state is complete/closing, process the call
	if patientMsgCount >= 7 || conversation.Status == "complete" || conversation.Status == "closing" {
		logger.Info("Conversation appears complete based on message count or status",
			"call_sid", callSID,
			"conversation_id", session.ConversationID,
			"status", conversation.Status,
			"patient_msg_count", patientMsgCount,
			"ai_msg_count", aiMsgCount)

		// If not already in complete state, mark it as such
		if conversation.Status != "complete" && conversation.Status != "closing" {
			err := s.conversationManager.CompleteConversation(session.ConversationID)
			if err != nil {
				logger.Error("Failed to mark conversation as complete",
					"error", err,
					"conversation_id", session.ConversationID)
			} else {
				logger.Info("Marked conversation as complete",
					"conversation_id", session.ConversationID)
			}
		}

		// Process the call results in a separate goroutine to avoid blocking
		go func() {
			// Wait a moment to ensure all data is synchronized
			time.Sleep(500 * time.Millisecond)

			// Check if we've already processed the results
			s.mu.RLock()
			_, stillExists := s.activeStreamingSessions[callSID]
			s.mu.RUnlock()

			if !stillExists {
				logger.Warn("Session no longer exists while processing results", "call_sid", callSID)
				return
			}

			// Process results with retries
			var callResult *CallResult
			var resultErr error
			for attempt := 0; attempt < 3; attempt++ {
				callResult, resultErr = s.ProcessCallResults(callSID)
				if resultErr == nil && callResult != nil {
					logger.Info("Successfully processed call results",
						"call_sid", callSID,
						"patient_id", callResult.PatientData["id"],
						"workflow_id", callResult.WorkflowID,
						"attempt", attempt+1)
					break
				}

				logger.Warn("Retrying call result processing",
					"call_sid", callSID,
					"attempt", attempt+1,
					"error", resultErr)
				time.Sleep(1 * time.Second)
			}

			if resultErr != nil || callResult == nil {
				logger.Error("Failed to process call results after retries",
					"error", resultErr,
					"call_sid", callSID)
			}
		}()

		// If conversation is officially in "complete" or "closing" state, plan to end the call
		if conversation.Status == "complete" || conversation.Status == "closing" {
			// Calculate appropriate delay based on the last message
			delaySeconds := 15

			// Try to check the length of the last AI message to adjust delay
			if aiMsgCount > 0 {
				lastAIMsg := ""
				for i := len(conversation.Transcript) - 1; i >= 0; i-- {
					if conversation.Transcript[i].Speaker == "ai" {
						lastAIMsg = conversation.Transcript[i].Text
						break
					}
				}

				// Adjust delay based on message length (longer messages need more time)
				msgWords := len(strings.Fields(lastAIMsg))
				if msgWords > 0 {
					// Approximately 3 words per second + 5 second buffer
					delaySeconds = (msgWords / 3) + 5
					// Cap at reasonable limits
					if delaySeconds < 10 {
						delaySeconds = 10
					} else if delaySeconds > 30 {
						delaySeconds = 30
					}
				}
			}

			logger.Info("Conversation completed, ending call automatically",
				"call_sid", callSID,
				"conversation_id", session.ConversationID,
				"status", conversation.Status,
				"delay_seconds", delaySeconds)

			// Wait for the goodbye message to be delivered
			time.Sleep(time.Duration(delaySeconds) * time.Second)

			// Double check that we're still in complete state
			conversation, stillExists := s.conversationManager.GetConversation(session.ConversationID)
			if !stillExists || (conversation.Status != "complete" && conversation.Status != "closing") {
				logger.Info("Conversation state changed during delay, not ending call",
					"call_sid", callSID)
				return
			}

			// End the call
			if err := s.EndCall(callSID); err != nil {
				logger.Error("Failed to automatically end call", "error", err, "call_sid", callSID)
			} else {
				logger.Info("Call ended automatically", "call_sid", callSID)
			}
		}
	} else {
		logger.Info("Conversation not yet complete, continuing",
			"call_sid", callSID,
			"conversation_id", session.ConversationID,
			"status", conversation.Status,
			"patient_msg_count", patientMsgCount,
			"ai_msg_count", aiMsgCount)
	}
}

// GenerateCustomResponse produces tailored responses for special cases
func (s *Service) GenerateCustomResponse(callSID string, text string, responseType string) (string, error) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("no active session for call %s", callSID)
	}

	// Get the conversation directly (not using type assertion)
	conversation, ok := s.conversationManager.GetConversation(session.ConversationID)
	if !ok {
		return "", fmt.Errorf("conversation %s not found", session.ConversationID)
	}

	switch responseType {
	case "name_correction":
		// Handle name correction responses
		if conversation.PatientName != "" {
			return fmt.Sprintf("Thank you for the correction. I've updated your name to %s. Let's continue from where we were.",
				conversation.PatientName), nil
		}
		return "I've noted your name. Let's continue with the next question.", nil

	case "repeat_question":
		// Get the most recent AI message (previous question)
		var lastQuestion string
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" {
				lastQuestion = conversation.Transcript[i].Text
				break
			}
		}

		if lastQuestion != "" {
			return "Let me repeat the question: " + lastQuestion, nil
		}
		return "Could you please answer the last question?", nil

	default:
		// No custom response for this type
		return "", fmt.Errorf("no custom response available for type: %s", responseType)
	}
}
