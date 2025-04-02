package call

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
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
	aiClient                ai.Client
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
	AllResponses      []string // Store all patient responses for final extraction
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
	aiClient ai.Client,
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
		aiClient:                aiClient,
		baseURL:                 baseURL,
		activeStreamingSessions: make(map[string]StreamingSession),
		webhookHandler:          twilio.NewWebhookHandler(),
	}

	// Register webhook handlers
	service.registerWebhookHandlers()

	// Start the cleanup scheduler to remove inactive sessions
	service.StartCleanupScheduler(5 * time.Minute)

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
		AllResponses:   []string{},
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

	// Process the results in a background goroutine
	go func() {
		time.Sleep(1 * time.Second) // Wait a moment to ensure data is saved
		_, err := s.ProcessCallResults(callSID)
		if err != nil {
			logger.Error("Failed to process call results after ending", "error", err, "call_sid", callSID)
		}
	}()

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

	// Extract all patient data at once from the combined responses
	patientData, err := s.extractFinalPatientData(session)
	if err != nil {
		logger.Error("Failed to extract patient data", "error", err, "call_sid", callSID)
		return nil, fmt.Errorf("failed to extract patient data: %w", err)
	}

	// Generate a case ID
	caseID := fmt.Sprintf("P%d", time.Now().Unix())
	if id, ok := patientData["id"].(string); ok && id != "" {
		caseID = id
	} else {
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
		"gender", patientData["gender"])

	// Store the case
	var resultRepo repository.ResultRepository = s.resultRepository
	if resultRepo != nil {
		if caseRepo, ok := resultRepo.(repository.CaseRepository); ok {
			err := caseRepo.StoreCase(caseID, patientData, false)
			if err != nil {
				logger.Error("Failed to store case", "error", err, "case_id", caseID)
			} else {
				logger.Info("Successfully stored case", "case_id", caseID)
			}
		}
	}

	// Select workflow based on patient data
	workflowID := "standard_diagnostic_workflow"
	if s.workflowService != nil {
		selectedID, err := s.workflowService.SelectWorkflow(patientData)
		if err == nil && selectedID != "" {
			workflowID = selectedID
			logger.Info("Selected workflow based on patient data", "workflow_id", workflowID, "case_id", caseID)
		}
	}

	// Create input data for the workflow
	inputData := map[string]any{
		"patient_data": patientData,
		"case_id":      caseID,
	}

	// Start the workflow
	instanceID := uuid.Nil
	workflowURL := fmt.Sprintf("%s/api/management/workflows/%s/instances", s.baseURL, workflowID)
	jsonData, err := json.Marshal(inputData)
	if err != nil {
		logger.Error("Failed to marshal workflow input data", "error", err)
	} else {
		// Create and send the request
		req, err := http.NewRequest("POST", workflowURL, bytes.NewBuffer(jsonData))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{Timeout: 15 * time.Second}
			resp, err := client.Do(req)

			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					// Try to get the instance ID from the response
					var apiResponse struct {
						InstanceID string `json:"instance_id"`
					}
					if json.NewDecoder(resp.Body).Decode(&apiResponse) == nil && apiResponse.InstanceID != "" {
						if id, parseErr := uuid.Parse(apiResponse.InstanceID); parseErr == nil {
							instanceID = id
							logger.Info("Got workflow instance ID", "instance_id", instanceID)
						}
					}
				}
			}
		}
	}

	// Initialize empty diagnosis and treatment data
	diagnosisData := make(map[string]any)
	treatmentData := make(map[string]any)

	// Try to get existing results if available
	if resultRepo != nil {
		results, err := resultRepo.GetResultsByCaseID(caseID)
		if err == nil && results != nil {
			if diagnosis, ok := results["diagnosis"].(map[string]any); ok && len(diagnosis) > 0 {
				diagnosisData = diagnosis
			}
			if treatment, ok := results["treatment_plan"].(map[string]any); ok && len(treatment) > 0 {
				treatmentData = treatment
			}
		}
	}

	// Prepare result data structure
	resultData := map[string]any{
		"patient_data":   patientData,
		"diagnosis":      diagnosisData,
		"treatment_plan": treatmentData,
		"workflow_id":    workflowID,
		"instance_id":    instanceID.String(),
		"completed_at":   time.Now().Format(time.RFC3339),
	}

	// Store results
	if resultRepo != nil {
		err := resultRepo.StoreResults(caseID, resultData)
		if err != nil {
			logger.Error("Failed to store call results", "error", err, "case_id", caseID)
		} else {
			logger.Info("Successfully stored call results", "case_id", caseID)
		}
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

// extractFinalPatientData uses all collected responses to extract comprehensive patient data
func (s *Service) extractFinalPatientData(session StreamingSession) (map[string]any, error) {
	s.mu.RLock()
	allResponses := session.AllResponses
	conversationID := session.ConversationID
	s.mu.RUnlock()

	// Combine all responses into a single text
	combinedText := strings.Join(allResponses, "\n")

	// Ensure we have some data to work with
	if len(combinedText) == 0 {
		// Try to get conversation messages
		messages, err := s.conversationManager.GetConversationMessages(conversationID)
		if err != nil {
			return nil, fmt.Errorf("no responses found and failed to get messages: %w", err)
		}

		// Extract patient messages
		var patientMessages []string
		for _, msg := range messages {
			if msg.Speaker == "patient" {
				patientMessages = append(patientMessages, msg.Text)
			}
		}

		combinedText = strings.Join(patientMessages, "\n")

		if len(combinedText) == 0 {
			// Fall back to minimal data structure
			return map[string]any{
				"id":          fmt.Sprintf("P%d", time.Now().Unix()),
				"name":        "Unknown Patient",
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
			}, nil
		}
	}

	// Use AI to extract all relevant medical data at once
	extractionPrompt := `Extract all patient information from the following conversation transcript.
I need structured data for a medical system. Include the following fields:
- name: The patient's full name
- age: The patient's age as a number
- gender: The patient's gender (male, female, or other)
- symptoms: All symptoms mentioned (as an array of strings)
- conditions: All pre-existing medical conditions mentioned (as an array of strings)
- medications: All medications the patient is taking (as an array of strings)
- allergies: All allergies mentioned (as an array of strings)

If any field is not mentioned or unclear, use empty values (empty string, 0, or empty array).
Return ONLY a valid JSON object containing all these fields, even if they're empty.

Conversation transcript:
"""
${TRANSCRIPT}
"""

Response must be valid JSON only.`

	// Replace placeholder with the actual transcript
	prompt := strings.Replace(extractionPrompt, "${TRANSCRIPT}", combinedText, 1)

	// Call AI to extract the information
	response, err := s.aiClient.GenerateCompletion(context.Background(), prompt, ai.CompletionOptions{
		MaxTokens:   1024,
		Temperature: 0.0,
		ModelName:   "gpt-4", // Use the most capable model for extraction
	})

	if err != nil {
		return nil, fmt.Errorf("failed to extract patient data: %w", err)
	}

	// Extract JSON from the response
	jsonString := extractJSON(response.Text)

	// Parse the JSON
	var extractedData map[string]any
	err = json.Unmarshal([]byte(jsonString), &extractedData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse extracted data: %w", err)
	}

	// Create standardized patient data structure
	result := map[string]any{
		"id":          fmt.Sprintf("P%d", time.Now().Unix()),
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

	// Copy extracted values to our result structure
	if name, ok := extractedData["name"].(string); ok && name != "" {
		result["name"] = name
	}

	if age, ok := extractedData["age"].(float64); ok && age > 0 {
		result["age"] = age
	} else if ageValue, ok := extractedData["age"]; ok {
		// Try to convert other formats
		switch v := ageValue.(type) {
		case int:
			result["age"] = float64(v)
		case string:
			if ageVal, err := strconv.ParseFloat(v, 64); err == nil {
				result["age"] = ageVal
			}
		}
	}

	if gender, ok := extractedData["gender"].(string); ok && gender != "" {
		result["gender"] = gender
	}

	// Handle array fields
	result["symptoms"] = convertToStringArray(extractedData["symptoms"])
	result["conditions"] = convertToStringArray(extractedData["conditions"])
	result["medications"] = convertToStringArray(extractedData["medications"])
	result["allergies"] = convertToStringArray(extractedData["allergies"])

	// Add conversation ID for reference
	result["_conversation_id"] = conversationID

	return result, nil
}

// Helper function to convert extracted array data to string arrays
func convertToStringArray(value any) []string {
	if value == nil {
		return []string{}
	}

	switch v := value.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				result = append(result, str)
			} else {
				result = append(result, fmt.Sprintf("%v", item))
			}
		}
		return result
	case string:
		if v == "" {
			return []string{}
		}
		return []string{v}
	default:
		return []string{}
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
			AllResponses:   []string{},
		}
		s.activeStreamingSessions[callSID] = session
	}
	session.LastActivity = time.Now()
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

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

	// Mark the conversation as complete
	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	} else {
		logger.Info("Marked conversation as complete",
			"conversation_id", session.ConversationID,
			"call_sid", callSID)
	}

	// Process the call results in a separate goroutine
	go func() {
		// Wait a moment to ensure all data is processed
		time.Sleep(2 * time.Second)

		// Process the results
		callResult, err := s.ProcessCallResults(callSID)
		if err != nil {
			logger.Error("Failed to process call results",
				"error", err,
				"call_sid", callSID)
			return
		}

		logger.Info("Successfully processed call results",
			"call_sid", callSID,
			"patient_id", callResult.PatientData["id"],
			"workflow_id", callResult.WorkflowID,
			"has_instance_id", callResult.InstanceID != uuid.Nil)

		// Clean up the session after processing
		s.mu.Lock()
		delete(s.activeStreamingSessions, callSID)
		s.mu.Unlock()
		logger.Info("Removed completed call session", "call_sid", callSID)
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

	// Clean up the session
	s.mu.Lock()
	delete(s.activeStreamingSessions, callSID)
	s.mu.Unlock()

	return nil
}

// HandleSpeechInput processes patient speech input and returns the next AI response
func (s *Service) HandleSpeechInput(callSID string, speechText string) (string, error) {
	s.mu.Lock()
	session, exists := s.activeStreamingSessions[callSID]
	if !exists {
		s.mu.Unlock()
		logger.Error("No active session for call", "call_sid", callSID)
		return "I'm sorry, I can't process your request right now.", nil
	}

	// Update session activity timestamp and store the response
	session.LastActivity = time.Now()
	session.AllResponses = append(session.AllResponses, speechText)
	s.activeStreamingSessions[callSID] = session
	s.mu.Unlock()

	logger.Info("Handling speech input",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"input", speechText,
		"response_count", len(session.AllResponses))

	// Store the patient's message
	err := s.conversationManager.AddMessage(session.ConversationID, "patient", speechText, 0.9)
	if err != nil {
		logger.Error("Failed to store patient message", "error", err)
	}

	// Get the next response
	aiResponse, err := s.conversationManager.GetNextResponse(session.ConversationID)
	if err != nil {
		logger.Error("Failed to get AI response", "error", err)
		return "Let's continue with the next question.", nil
	}

	logger.Info("Generated AI response",
		"call_sid", callSID,
		"conversation_id", session.ConversationID,
		"response_length", len(aiResponse))

	// Check if this is the last question (after 7 patient responses)
	s.mu.RLock()
	responseCount := len(session.AllResponses)
	s.mu.RUnlock()

	if responseCount >= 7 {
		// Trigger processing in background
		go func() {
			logger.Info("Triggering background processing after final question",
				"call_sid", callSID,
				"response_count", responseCount)

			// Wait a moment before processing
			time.Sleep(1 * time.Second)

			// Mark as closing
			if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
				logger.Warn("Error completing conversation", "error", err)
			}

			// Process results
			_, err := s.ProcessCallResults(callSID)
			if err != nil {
				logger.Error("Failed to process call results after final question",
					"error", err, "call_sid", callSID)
			}
		}()
	}

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
	s.mu.Lock()
	session, exists := s.activeStreamingSessions[callSID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("no active session for call %s", callSID)
	}

	// If this is a patient response, also add it to our collection
	if speaker == "patient" {
		session.AllResponses = append(session.AllResponses, text)
		s.activeStreamingSessions[callSID] = session
	}
	s.mu.Unlock()

	// Add message without waiting for any processing
	err := s.conversationManager.AddMessage(session.ConversationID, speaker, text, 0.9)

	if err != nil {
		return err
	}

	logger.Info("Stored message",
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

// extractJSON extracts JSON from a string
func extractJSON(text string) string {
	// First look for explicit JSON between curly braces
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		return text[jsonStart : jsonEnd+1]
	}

	// If we couldn't find JSON, return a minimal valid JSON object
	return "{}"
}

// CheckConversationCompletion checks if the conversation is complete and ends the call if necessary
func (s *Service) CheckConversationCompletion(callSID string) {
	s.mu.RLock()
	session, exists := s.activeStreamingSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for call", "call_sid", callSID)
		return
	}

	// Check response count
	s.mu.RLock()
	responseCount := len(session.AllResponses)
	s.mu.RUnlock()

	// If we have enough responses (7+), plan to end the call
	if responseCount >= 7 {
		logger.Info("Conversation appears complete based on response count",
			"call_sid", callSID,
			"conversation_id", session.ConversationID,
			"response_count", responseCount)

		// Wait a reasonable amount of time for the goodbye message to finish
		time.Sleep(10 * time.Second)

		// End the call
		if err := s.EndCall(callSID); err != nil {
			logger.Error("Failed to automatically end call", "error", err, "call_sid", callSID)
		} else {
			logger.Info("Call ended automatically", "call_sid", callSID)
		}
	}
}
