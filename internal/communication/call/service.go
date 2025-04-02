package call

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/deepgram"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/elevenlabs"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/conversation"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/processing"
	"github.com/gin-gonic/gin"
)

// CallSession represents an active phone call
type CallSession struct {
	CallSID         string
	ConversationID  string
	PatientPhone    string
	IsActive        bool
	StartTime       time.Time
	LastActivity    time.Time
	AudioData       []byte // Current audio response data
	LastAudioURL    string
	QuestionCounter int
}

// Service coordinates phone calls, speech recognition, and AI responses
type Service struct {
	twilioClient        *twilio.Client
	elevenLabsClient    *elevenlabs.Client
	deepgramClient      *deepgram.Client
	conversationManager *conversation.ConversationManager
	transcriptProcessor *processing.TranscriptProcessor
	webhookHandler      *twilio.WebhookHandler
	baseURL             string
	activeSessions      map[string]*CallSession
	audioCache          map[string][]byte // Cache for audio responses
	mu                  sync.RWMutex
}

// NewService creates a new call service
func NewService(
	twilioClient *twilio.Client,
	elevenLabsClient *elevenlabs.Client,
	deepgramClient *deepgram.Client,
	conversationManager *conversation.ConversationManager,
	transcriptProcessor *processing.TranscriptProcessor,
	baseURL string,
) *Service {
	service := &Service{
		twilioClient:        twilioClient,
		elevenLabsClient:    elevenLabsClient,
		deepgramClient:      deepgramClient,
		conversationManager: conversationManager,
		transcriptProcessor: transcriptProcessor,
		baseURL:             baseURL,
		activeSessions:      make(map[string]*CallSession),
		audioCache:          make(map[string][]byte),
		webhookHandler:      twilio.NewWebhookHandler(),
	}

	// Register webhook handlers
	service.registerWebhookHandlers()

	return service
}

// InitiateCall starts a phone call to a patient
func (s *Service) InitiateCall(patientPhone string) (string, error) {
	// Create a callback URL for Twilio
	callbackURL := fmt.Sprintf("%s/api/call/webhook", s.baseURL)

	// Start the call
	callEvent, err := s.twilioClient.MakeCall(patientPhone, callbackURL)
	if err != nil {
		logger.Error("Failed to initiate call", "error", err, "phone", patientPhone)
		return "", fmt.Errorf("failed to initiate call: %w", err)
	}

	// Create a new conversation
	conv, err := s.conversationManager.StartConversation(patientPhone)
	if err != nil {
		logger.Error("Failed to create conversation", "error", err, "call_sid", callEvent.CallSID)
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

	// Create a new session
	s.mu.Lock()
	s.activeSessions[callEvent.CallSID] = &CallSession{
		CallSID:         callEvent.CallSID,
		ConversationID:  conv.ID,
		PatientPhone:    patientPhone,
		IsActive:        true,
		StartTime:       time.Now(),
		LastActivity:    time.Now(),
		QuestionCounter: 0,
	}
	s.mu.Unlock()

	logger.Info("Call initiated successfully",
		"call_sid", callEvent.CallSID,
		"conversation_id", conv.ID,
		"patient_phone", patientPhone)

	return callEvent.CallSID, nil
}

// EndCall terminates an active call
func (s *Service) EndCall(callSID string) error {
	s.mu.Lock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("no active session for call %s", callSID)
	}

	// If already marked inactive, just return
	if !session.IsActive {
		s.mu.Unlock()
		return nil
	}

	// Mark as inactive
	session.IsActive = false
	s.activeSessions[callSID] = session
	conversationID := session.ConversationID
	s.mu.Unlock()

	// Mark the conversation as complete
	if err := s.conversationManager.CompleteConversation(conversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err, "conversation_id", conversationID)
	}

	// Attempt to end the Twilio call
	err := s.twilioClient.EndCall(callSID)
	if err != nil {
		// Log but don't return error if likely already ended
		if isCallAlreadyEndedError(err) {
			logger.Warn("Call may already be ended", "call_sid", callSID, "error", err)
		} else {
			logger.Error("Failed to end call", "error", err, "call_sid", callSID)
			return fmt.Errorf("failed to end call: %w", err)
		}
	}

	logger.Info("Call ended", "call_sid", callSID)

	// Process the call results in background
	go s.processCallResults(callSID)

	return nil
}

// ProcessSpeechInput handles patient speech input and returns the next response
func (s *Service) ProcessSpeechInput(callSID string, speechText string) (string, error) {
	logger.Info("Processing speech input",
		"call_sid", callSID,
		"input", speechText,
		"input_length", len(speechText))

	s.mu.Lock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		s.mu.Unlock()
		return "I'm sorry, we're experiencing technical difficulties.", fmt.Errorf("session not found")
	}

	// Update activity timestamp
	session.LastActivity = time.Now()
	conversationID := session.ConversationID
	s.mu.Unlock()

	// Add the patient's message to the conversation
	// This will also advance to the next question index internally
	if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypePatient, speechText); err != nil {
		logger.Error("Failed to add patient message",
			"error", err,
			"conversation_id", conversationID)
		return "I'm sorry, we're experiencing technical difficulties.", err
	}

	// Get the next question based on the updated index
	nextQuestion, err := s.conversationManager.GetNextQuestion(conversationID)
	if err != nil {
		logger.Error("Failed to get next question",
			"error", err,
			"conversation_id", conversationID)
		return "I'm sorry, we're experiencing technical difficulties.", err
	}

	// Add the AI's response to the conversation
	if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypeAI, nextQuestion); err != nil {
		logger.Error("Failed to add AI message",
			"error", err,
			"conversation_id", conversationID)
	}

	// Increment question counter
	s.mu.Lock()
	session.QuestionCounter++
	questionCount := session.QuestionCounter
	s.mu.Unlock()

	// Check if we should end the call (after the closing question)
	if questionCount >= 8 { // After the goodbye message
		go func() {
			// Wait a bit to let the goodbye message play
			time.Sleep(10 * time.Second)
			logger.Info("Auto-ending call after final question",
				"call_sid", callSID,
				"question_counter", questionCount)
			if err := s.EndCall(callSID); err != nil {
				logger.Error("Failed to automatically end call",
					"error", err,
					"call_sid", callSID)
			}
		}()
	}

	logger.Info("Generated next response",
		"call_sid", callSID,
		"response_length", len(nextQuestion),
		"question_counter", questionCount)

	return nextQuestion, nil
}

// GenerateAudioResponse generates audio for a text response
func (s *Service) GenerateAudioResponse(callSID string, text string) ([]byte, error) {
	// Use the ElevenLabs client to generate audio
	voiceResponse, err := s.elevenLabsClient.GenerateAudio(text, &elevenlabs.VoiceOptions{
		Stability:       0.5,
		SimilarityBoost: 0.75,
		Style:           0.0,
		SpeakerBoost:    true,
	})

	if err != nil {
		logger.Error("Failed to generate audio", "error", err, "call_sid", callSID)
		return nil, fmt.Errorf("failed to generate audio: %w", err)
	}

	// Cache the audio for this call
	s.mu.Lock()
	s.audioCache[callSID] = voiceResponse.AudioBytes
	s.mu.Unlock()

	logger.Info("Generated audio response",
		"call_sid", callSID,
		"audio_size", len(voiceResponse.AudioBytes),
		"duration", voiceResponse.Duration)

	return voiceResponse.AudioBytes, nil
}

// GetAudioResponse retrieves the cached audio response
func (s *Service) GetAudioResponse(callSID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	audio, exists := s.audioCache[callSID]
	return audio, exists
}

// processCallResults processes the results of a completed call
func (s *Service) processCallResults(callSID string) {
	logger.Info("Processing call results", "call_sid", callSID)

	s.mu.RLock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		s.mu.RUnlock()
		logger.Error("Failed to process results - session not found", "call_sid", callSID)
		return
	}
	conversationID := session.ConversationID
	s.mu.RUnlock()

	// Get the formatted transcript
	formattedTranscript, err := s.conversationManager.FormatTranscriptForProcessing(conversationID)
	if err != nil {
		logger.Error("Failed to get transcript", "error", err, "conversation_id", conversationID)
		return
	}

	// Process the transcript using the API-based processor
	if s.transcriptProcessor != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := s.transcriptProcessor.ProcessTranscript(ctx, conversationID, formattedTranscript)
		if err != nil {
			logger.Error("Failed to process transcript", "error", err, "conversation_id", conversationID)
		} else {
			logger.Info("Successfully processed call results",
				"call_sid", callSID,
				"case_id", result.CaseID,
				"patient_name", result.PatientData.Name,
				"workflow_id", result.WorkflowID)
		}
	}

	// Clean up the session
	s.mu.Lock()
	delete(s.activeSessions, callSID)
	delete(s.audioCache, callSID)
	s.mu.Unlock()

	logger.Info("Completed call result processing", "call_sid", callSID)
}

// GetWebhookHandler returns the Twilio webhook handler
func (s *Service) GetWebhookHandler() *twilio.WebhookHandler {
	return s.webhookHandler
}

// GetBaseURL returns the base URL for callbacks
func (s *Service) GetBaseURL() string {
	return s.baseURL
}

// registerWebhookHandlers sets up the Twilio webhook handlers
func (s *Service) registerWebhookHandlers() {
	// Register voice webhook handler
	s.webhookHandler.RegisterCallHandler("voice", func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.handleIncomingCall(c, event)
	})

	// Register default handler
	s.webhookHandler.RegisterDefaultHandler(func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.handleIncomingCall(c, event)
	})

	// Register status handlers
	s.webhookHandler.RegisterStatusHandler("completed", func(c *gin.Context, event twilio.CallEvent) error {
		return s.handleCallCompleted(c, event)
	})

	s.webhookHandler.RegisterStatusHandler("failed", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Error("Call failed", "call_sid", event.CallSID, "error_code", event.ErrorCode)
		return nil
	})
}

// handleIncomingCall processes an incoming call webhook
func (s *Service) handleIncomingCall(c *gin.Context, event twilio.CallEvent) (string, error) {
	callSID := event.CallSID
	patientPhone := event.From

	logger.Info("Handling incoming call",
		"call_sid", callSID,
		"from", patientPhone,
		"status", event.Status)

	s.mu.Lock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		// Create a new conversation
		conv, err := s.conversationManager.StartConversation(patientPhone)
		if err != nil {
			s.mu.Unlock()
			logger.Error("Failed to create conversation", "error", err)
			return twilioErrorResponse("We're experiencing technical difficulties. Please try again later."), nil
		}

		// Create a new session
		session = &CallSession{
			CallSID:        callSID,
			ConversationID: conv.ID,
			PatientPhone:   patientPhone,
			IsActive:       true,
			StartTime:      time.Now(),
			LastActivity:   time.Now(),
		}
		s.activeSessions[callSID] = session

		logger.Info("Created new session for incoming call",
			"call_sid", callSID,
			"conversation_id", conv.ID)
	} else {
		logger.Info("Using existing session for call",
			"call_sid", callSID,
			"conversation_id", session.ConversationID)
	}
	s.mu.Unlock()

	// Get the initial question (always the first one for a new call)
	questionText, err := s.conversationManager.GetNextQuestion(session.ConversationID)
	if err != nil {
		logger.Error("Failed to get question", "error", err)
		return twilioErrorResponse("We're experiencing technical difficulties. Please try again later."), nil
	}

	// Add this as an AI message in the conversation
	if err := s.conversationManager.AddMessage(session.ConversationID, conversation.MessageTypeAI, questionText); err != nil {
		logger.Error("Failed to add AI message", "error", err)
	}

	// Try to generate audio for the response
	_, err = s.GenerateAudioResponse(callSID, questionText)
	if err != nil {
		// If audio generation fails, fall back to Twilio's TTS
		logger.Warn("Failed to generate audio, using Twilio TTS", "error", err)
		return twilio.GenerateTwiML(
			twilio.SayAction(questionText, "alice", "en-US"),
			twilio.GatherAction("", map[string]string{
				"input":         "speech",
				"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID),
				"language":      "en-US",
				"speechTimeout": "auto",
			}),
		), nil
	}

	// Audio generation succeeded, use the audio URL
	audioURL := fmt.Sprintf("%s/api/call/audio/%s", s.baseURL, callSID)

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

// handleCallCompleted handles call completed webhook
func (s *Service) handleCallCompleted(c *gin.Context, event twilio.CallEvent) error {
	callSID := event.CallSID

	s.mu.RLock()
	session, exists := s.activeSessions[callSID]
	s.mu.RUnlock()

	if !exists {
		logger.Warn("No session found for completed call", "call_sid", callSID)
		return nil
	}

	// Mark session as inactive
	s.mu.Lock()
	session.IsActive = false
	s.activeSessions[callSID] = session
	s.mu.Unlock()

	// Mark conversation as complete
	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	}

	// Process the call results in background
	go s.processCallResults(callSID)

	return nil
}

// Helper functions

// isCallAlreadyEndedError checks if an error indicates the call is already ended
func isCallAlreadyEndedError(err error) bool {
	if err == nil {
		return false
	}

	errorMsg := err.Error()
	return strings.Contains(errorMsg, "already completed") ||
		strings.Contains(errorMsg, "not found")
}

// twilioErrorResponse generates a TwiML error response
func twilioErrorResponse(message string) string {
	return twilio.GenerateTwiML(
		twilio.SayAction(message, "alice", "en-US"),
	)
}
