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
	AudioData       []byte
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
	audioCache          map[string][]byte
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

	service.registerWebhookHandlers()

	return service
}

// InitiateCall starts a phone call to a patient
func (s *Service) InitiateCall(patientPhone string) (string, error) {
	callbackURL := fmt.Sprintf("%s/api/call/webhook", s.baseURL)

	callEvent, err := s.twilioClient.MakeCall(patientPhone, callbackURL)
	if err != nil {
		logger.Error("Failed to initiate call", "error", err, "phone", patientPhone)
		return "", fmt.Errorf("failed to initiate call: %w", err)
	}

	conv, err := s.conversationManager.StartConversation(patientPhone)
	if err != nil {
		logger.Error("Failed to create conversation", "error", err, "call_sid", callEvent.CallSID)
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

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

	if !session.IsActive {
		s.mu.Unlock()
		return nil
	}

	session.IsActive = false
	s.activeSessions[callSID] = session
	conversationID := session.ConversationID
	s.mu.Unlock()

	if err := s.conversationManager.CompleteConversation(conversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err, "conversation_id", conversationID)
	}

	err := s.twilioClient.EndCall(callSID)
	if err != nil {
		if isCallAlreadyEndedError(err) {
			logger.Warn("Call may already be ended", "call_sid", callSID, "error", err)
		} else {
			logger.Error("Failed to end call", "error", err, "call_sid", callSID)
			return fmt.Errorf("failed to end call: %w", err)
		}
	}

	logger.Info("Call ended", "call_sid", callSID)

	go s.processCallResults(callSID)

	return nil
}

// ProcessSpeechInput handles patient speech input and returns the next response
func (s *Service) ProcessSpeechInput(callSID string, speechText string) (string, error) {
	startTime := time.Now()
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

	session.LastActivity = time.Now()
	conversationID := session.ConversationID
	currentQuestionIndex := session.QuestionCounter

	totalQuestions := len(s.conversationManager.GetQuestions())
	s.mu.Unlock()

	refinedText := speechText
	if len(speechText) > 0 && s.deepgramClient != nil {
		if currentQuestionIndex > 0 && len(speechText) > 20 {
			options := map[string]string{
				"model":        "nova-2",
				"language":     "en-US",
				"punctuate":    "true",
				"diarize":      "false",
				"smart_format": "true",
			}

			textBytes := []byte(speechText)
			response, err := s.deepgramClient.TranscribeAudio(textBytes, "text/plain", options)

			if err == nil && response != nil && response.Confidence > 0.85 {
				refinedText = response.Transcript
				logger.Info("Refined transcription",
					"original", speechText,
					"refined", refinedText,
					"confidence", response.Confidence)
			}
		}
	}

	logger.Info("Current question state",
		"call_sid", callSID,
		"current_question_index", currentQuestionIndex,
		"total_questions", totalQuestions,
		"conversation_id", conversationID)

	if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypePatient, refinedText); err != nil {
		logger.Error("Failed to add patient message",
			"error", err,
			"conversation_id", conversationID)
		return "I'm sorry, we're experiencing technical difficulties.", err
	}

	nextQuestionIndex := currentQuestionIndex + 1
	if nextQuestionIndex >= totalQuestions {
		logger.Info("Reached last question, staying at final goodbye",
			"call_sid", callSID,
			"current_index", nextQuestionIndex,
			"max_questions", totalQuestions)
		nextQuestionIndex = totalQuestions - 1
	}

	questionsList := s.conversationManager.GetQuestions()
	nextQuestion := questionsList[nextQuestionIndex]

	if nextQuestionIndex == totalQuestions-1 {
		go func() {
			_, err := s.GenerateAudioResponse(callSID, nextQuestion)
			if err != nil {
				logger.Error("Failed to preemptively generate audio", "error", err)
			}
		}()
	}

	logger.Info("Next question content",
		"call_sid", callSID,
		"question_index", nextQuestionIndex,
		"question_text", nextQuestion)

	if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypeAI, nextQuestion); err != nil {
		logger.Error("Failed to add AI message",
			"error", err,
			"conversation_id", conversationID)
	}

	s.mu.Lock()
	session.QuestionCounter = nextQuestionIndex
	questionCount := session.QuestionCounter
	s.mu.Unlock()

	if questionCount == totalQuestions-1 {
		logger.Info("Reached final goodbye question - will end call soon",
			"call_sid", callSID,
			"question_index", questionCount,
			"total_questions", totalQuestions)

		go func() {
			estimatedDuration := float64(len(nextQuestion)) / 15.0
			waitTime := time.Duration(estimatedDuration*1000)*time.Millisecond + 1500*time.Millisecond
			time.Sleep(waitTime)

			logger.Info("Auto-ending call immediately after final message",
				"call_sid", callSID,
				"estimated_message_duration", estimatedDuration)

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
		"processing_time", time.Since(startTime),
		"question_counter", questionCount,
		"question_text", nextQuestion)

	return nextQuestion, nil
}

// GenerateAudioResponse generates audio for a text response
func (s *Service) GenerateAudioResponse(callSID string, text string) ([]byte, error) {
	startTime := time.Now()
	voiceResponse, err := s.elevenLabsClient.GenerateAudio(text, &elevenlabs.VoiceOptions{
		Stability:           0.65,
		SimilarityBoost:     0.80,
		Style:               0.35,
		SpeakerBoost:        true,
		LatencyOptimization: true,
		OutputFormat:        "mp3_44100_128",
	})

	if err != nil {
		logger.Error("Failed to generate audio", "error", err, "call_sid", callSID)
		return nil, fmt.Errorf("failed to generate audio: %w", err)
	}

	s.mu.Lock()
	s.audioCache[callSID] = voiceResponse.AudioBytes
	s.mu.Unlock()

	logger.Info("Generated audio response",
		"call_sid", callSID,
		"audio_size", len(voiceResponse.AudioBytes),
		"duration", voiceResponse.Duration,
		"generation_time", time.Since(startTime))

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

	if err := s.conversationManager.CompleteConversation(conversationID); err != nil {
		logger.Warn("Error ensuring conversation is marked complete", "error", err)
	}

	formattedTranscript, err := s.conversationManager.FormatTranscriptForProcessing(conversationID)
	if err != nil {
		logger.Error("Failed to get transcript", "error", err, "conversation_id", conversationID)
		return
	}

	if s.transcriptProcessor != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		logger.Info("Starting transcript processing",
			"conversation_id", conversationID,
			"transcript_length", len(formattedTranscript))

		result, err := s.transcriptProcessor.ProcessTranscript(ctx, conversationID, formattedTranscript)
		if err != nil {
			logger.Error("Failed to process transcript", "error", err, "conversation_id", conversationID)
		} else {
			logger.Info("Successfully processed call results",
				"call_sid", callSID,
				"conversation_id", conversationID,
				"case_id", result.CaseID,
				"patient_name", result.PatientData.Name,
				"workflow_id", result.WorkflowID,
				"patient_symptoms_count", len(result.PatientData.Symptoms),
				"patient_conditions_count", len(result.PatientData.Conditions))

			logger.Info("Extracted patient data",
				"name", result.PatientData.Name,
				"age", result.PatientData.Age,
				"gender", result.PatientData.Gender,
				"symptoms", strings.Join(result.PatientData.Symptoms, ", "),
				"conditions", strings.Join(result.PatientData.Conditions, ", "),
				"medications", strings.Join(result.PatientData.Medications, ", "),
				"allergies", strings.Join(result.PatientData.Allergies, ", "))
		}
	} else {
		logger.Warn("No transcript processor available - skipping processing", "conversation_id", conversationID)
	}

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

// GetCallState returns the current state of a call session
func (s *Service) GetCallState(callSID string) (*CallSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, exists := s.activeSessions[callSID]
	if !exists {
		return nil, fmt.Errorf("no active session for call %s", callSID)
	}

	sessionCopy := *session
	return &sessionCopy, nil
}

// GetNextQuestionForCall returns the next question for a call without advancing the state
func (s *Service) GetNextQuestionForCall(callSID string) (string, error) {
	s.mu.RLock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		s.mu.RUnlock()
		return "", fmt.Errorf("no active session for call %s", callSID)
	}

	questionIndex := session.QuestionCounter
	s.mu.RUnlock()

	questions := s.conversationManager.GetQuestions()
	if questionIndex < 0 || questionIndex >= len(questions) {
		return questions[len(questions)-1], nil
	}

	return questions[questionIndex], nil
}

// registerWebhookHandlers sets up the Twilio webhook handlers
func (s *Service) registerWebhookHandlers() {
	s.webhookHandler.RegisterCallHandler("voice", func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.HandleInboundCall(c, event)
	})

	s.webhookHandler.RegisterDefaultHandler(func(c *gin.Context, event twilio.CallEvent) (string, error) {
		return s.HandleInboundCall(c, event)
	})

	s.webhookHandler.RegisterStatusHandler("completed", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Info("Call completed", "call_sid", event.CallSID)
		return s.handleCallCompleted(c, event)
	})

	s.webhookHandler.RegisterStatusHandler("failed", func(c *gin.Context, event twilio.CallEvent) error {
		logger.Error("Call failed", "call_sid", event.CallSID, "error_code", event.ErrorCode)
		return nil
	})
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

	s.mu.Lock()
	session.IsActive = false
	s.activeSessions[callSID] = session
	s.mu.Unlock()

	if err := s.conversationManager.CompleteConversation(session.ConversationID); err != nil {
		logger.Warn("Error completing conversation", "error", err)
	}

	go s.processCallResults(callSID)

	return nil
}

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

// SimpleConversationFlow handles a complete call conversation with sequential questions
func (s *Service) SimpleConversationFlow(patientPhone string) (string, error) {
	callSID, err := s.InitiateCall(patientPhone)
	if err != nil {
		return "", fmt.Errorf("failed to initiate call: %w", err)
	}

	conv, err := s.conversationManager.StartConversation(patientPhone)
	if err != nil {
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

	conversationID := conv.ID

	questions := s.conversationManager.GetQuestions()

	s.mu.Lock()
	s.activeSessions[callSID] = &CallSession{
		CallSID:         callSID,
		ConversationID:  conversationID,
		PatientPhone:    patientPhone,
		IsActive:        true,
		StartTime:       time.Now(),
		LastActivity:    time.Now(),
		QuestionCounter: 0,
	}
	s.mu.Unlock()

	logger.Info("Started simple conversation flow",
		"call_sid", callSID,
		"conversation_id", conversationID,
		"total_questions", len(questions))

	return conversationID, nil
}

// ProcessConversationToCase processes a completed conversation into a patient case and triggers workflow
func (s *Service) ProcessConversationToCase(conversationID string) (string, error) {
	if err := s.conversationManager.CompleteConversation(conversationID); err != nil {
		logger.Warn("Error marking conversation as complete", "error", err)
	}

	formattedTranscript, err := s.conversationManager.FormatTranscriptForProcessing(conversationID)
	if err != nil {
		return "", fmt.Errorf("failed to format transcript: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := s.transcriptProcessor.ProcessTranscript(ctx, conversationID, formattedTranscript)
	if err != nil {
		return "", fmt.Errorf("failed to process transcript: %w", err)
	}

	caseID := result.CaseID

	logger.Info("Successfully processed conversation to case",
		"conversation_id", conversationID,
		"case_id", caseID,
		"patient_name", result.PatientData.Name,
		"workflow_id", result.WorkflowID)

	return caseID, nil
}

// HandleInboundCall processes calls initiated by patients to your Twilio number
func (s *Service) HandleInboundCall(c *gin.Context, event twilio.CallEvent) (string, error) {
	callSID := event.CallSID
	patientPhone := event.From

	logger.Info("Received inbound call from patient",
		"call_sid", callSID,
		"from", patientPhone)

	var conversationID string
	s.mu.Lock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		conv, err := s.conversationManager.StartConversation(patientPhone)
		if err != nil {
			s.mu.Unlock()
			logger.Error("Failed to create conversation", "error", err)
			return twilioErrorResponse("We're experiencing technical difficulties. Please try again later."), nil
		}

		conversationID = conv.ID

		session = &CallSession{
			CallSID:         callSID,
			ConversationID:  conversationID,
			PatientPhone:    patientPhone,
			IsActive:        true,
			StartTime:       time.Now(),
			LastActivity:    time.Now(),
			QuestionCounter: 0,
		}
		s.activeSessions[callSID] = session

		logger.Info("Created new conversation for inbound call",
			"call_sid", callSID,
			"conversation_id", conversationID)
	} else {
		conversationID = session.ConversationID
		logger.Info("Using existing conversation for inbound call",
			"call_sid", callSID,
			"conversation_id", conversationID)
	}
	s.mu.Unlock()

	questions := s.conversationManager.GetQuestions()

	if session.QuestionCounter >= len(questions) {
		questionText := questions[len(questions)-1]

		if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypeAI, questionText); err != nil {
			logger.Error("Failed to add goodbye message", "error", err)
		}

		goodbyeTwiML := twilio.GenerateTwiML(
			twilio.SayAction(questionText, "Polly.Matthew-Neural", "en-US"),
			"<Hangup/>",
		)

		go func() {
			s.mu.Lock()
			if session, exists := s.activeSessions[callSID]; exists {
				session.IsActive = false
				s.activeSessions[callSID] = session
			}
			s.mu.Unlock()

			time.Sleep(200 * time.Millisecond)

			go s.processCallResults(callSID)
		}()

		return goodbyeTwiML, nil
	}

	questionText := questions[session.QuestionCounter]

	if err := s.conversationManager.AddMessage(conversationID, conversation.MessageTypeAI, questionText); err != nil {
		logger.Error("Failed to add question message", "error", err)
	}

	actionURL := fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID)

	logger.Info("Sending question to patient",
		"call_sid", callSID,
		"question_index", session.QuestionCounter,
		"question_text", questionText)

	return twilio.GenerateTwiML(
		twilio.SayAction(questionText, "Polly.Matthew-Neural", "en-US"),
		twilio.GatherAction("", map[string]string{
			"input":             "speech",
			"action":            actionURL,
			"language":          "en-US",
			"speechTimeout":     "1",
			"timeout":           "8",
			"profanityFilter":   "false",
			"interdigitTimeout": "1",
			"enhanced":          "true",
			"speechModel":       "phone_call",
		}),
	), nil
}

// SimplifiedHandleIncomingCall provides a simpler implementation for call handling
func (s *Service) SimplifiedHandleIncomingCall(c *gin.Context, event twilio.CallEvent) (string, error) {
	callSID := event.CallSID
	patientPhone := event.From

	s.mu.Lock()
	session, exists := s.activeSessions[callSID]
	if !exists {
		conv, err := s.conversationManager.StartConversation(patientPhone)
		if err != nil {
			s.mu.Unlock()
			return "We're experiencing technical difficulties. Please try again later.", err
		}

		session = &CallSession{
			CallSID:         callSID,
			ConversationID:  conv.ID,
			PatientPhone:    patientPhone,
			IsActive:        true,
			StartTime:       time.Now(),
			LastActivity:    time.Now(),
			QuestionCounter: 0,
		}
		s.activeSessions[callSID] = session
	}
	s.mu.Unlock()

	questions := s.conversationManager.GetQuestions()
	if session.QuestionCounter >= len(questions) {
		questionText := questions[len(questions)-1]

		goodbyeTwiML := twilio.GenerateTwiML(
			twilio.SayAction(questionText, "Polly.Matthew-Neural", "en-US"),
			"<Hangup/>",
		)

		go func() {
			s.mu.Lock()
			if session, exists := s.activeSessions[callSID]; exists {
				session.IsActive = false
				s.activeSessions[callSID] = session
			}
			s.mu.Unlock()

			time.Sleep(200 * time.Millisecond)

			go s.processCallResults(callSID)
		}()

		return goodbyeTwiML, nil
	}

	questionText := questions[session.QuestionCounter]

	s.conversationManager.AddMessage(session.ConversationID, conversation.MessageTypeAI, questionText)

	actionURL := fmt.Sprintf("%s/api/call/speech?call_sid=%s", s.baseURL, callSID)

	return twilio.GenerateTwiML(
		twilio.SayAction(questionText, "Polly.Matthew-Neural", "en-US"),
		twilio.GatherAction("", map[string]string{
			"input":           "speech",
			"action":          actionURL,
			"language":        "en-US",
			"speechTimeout":   "1",
			"timeout":         "8",
			"profanityFilter": "false",
			"enhanced":        "true",
			"speechModel":     "phone_call",
		}),
	), nil
}

// IncrementQuestionCounter advances to the next question
func (s *Service) IncrementQuestionCounter(callSID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.activeSessions[callSID]
	if !exists {
		return fmt.Errorf("no active session for call %s", callSID)
	}

	session.QuestionCounter++
	session.LastActivity = time.Now()

	return nil
}

// GetConversationManager returns the conversation manager
func (s *Service) GetConversationManager() *conversation.ConversationManager {
	return s.conversationManager
}
