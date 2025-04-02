package conversation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// ConversationStatus represents the status of a conversation
type ConversationStatus string

const (
	StatusGreeting   ConversationStatus = "greeting"
	StatusName       ConversationStatus = "name"
	StatusAge        ConversationStatus = "age"
	StatusGender     ConversationStatus = "gender"
	StatusSymptoms   ConversationStatus = "symptoms"
	StatusConditions ConversationStatus = "conditions"
	StatusMedication ConversationStatus = "medication"
	StatusAllergies  ConversationStatus = "allergies"
	StatusClosing    ConversationStatus = "closing"
	StatusComplete   ConversationStatus = "complete"
	StatusError      ConversationStatus = "error"
)

// Conversation represents an ongoing conversation with a patient
type Conversation struct {
	ID             string                 `json:"id"`
	PatientPhone   string                 `json:"patient_phone"`
	PatientName    string                 `json:"patient_name"`
	PatientEmail   string                 `json:"patient_email"`
	StartTime      time.Time              `json:"start_time"`
	EndTime        time.Time              `json:"end_time"`
	Status         ConversationStatus     `json:"status"`
	Transcript     []MessageExchange      `json:"transcript"`
	CollectedData  map[string]interface{} `json:"collected_data"`
	Duration       int                    `json:"duration"`
	QuestionsAsked map[string]bool        `json:"questions_asked"`
}

// MessageExchange represents a single exchange in the conversation
type MessageExchange struct {
	Speaker     string    `json:"speaker"`
	Text        string    `json:"text"`
	Timestamp   time.Time `json:"timestamp"`
	Confidence  float64   `json:"confidence,omitempty"`
	IsProcessed bool      `json:"is_processed"`
}

// ConversationManager manages conversations with patients
type ConversationManager struct {
	aiClient            ai.Client
	activeConversations map[string]*Conversation
	systemPrompt        string
	mu                  sync.RWMutex
}

// ManagerOption configures a conversation manager
type ManagerOption func(*ConversationManager)

// NewConversationManager creates a new conversation manager with direct, focused prompts
func NewConversationManager(aiClient ai.Client, options ...ManagerOption) *ConversationManager {
	manager := &ConversationManager{
		aiClient:            aiClient,
		activeConversations: make(map[string]*Conversation),
		systemPrompt: `You are a medical data collection assistant for Sword Symphony.
You should be professional and polite while keeping responses concise.
Your goal is to collect patient information needed for the medical record.
Ask exactly ONE question at a time and never repeat questions.
Do not provide medical advice, just collect information efficiently.`,
	}

	for _, option := range options {
		option(manager)
	}

	return manager
}

// WithSystemPrompt sets a custom system prompt
func WithSystemPrompt(prompt string) ManagerOption {
	return func(m *ConversationManager) {
		m.systemPrompt = prompt
	}
}

// StartConversation begins a new conversation
func (m *ConversationManager) StartConversation(patientPhone string) (*Conversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, conv := range m.activeConversations {
		if conv.PatientPhone == patientPhone && conv.Status != StatusComplete && conv.Status != StatusError {
			return conv, nil
		}
	}

	conversation := &Conversation{
		ID:             fmt.Sprintf("conv_%d", time.Now().UnixNano()),
		PatientPhone:   patientPhone,
		StartTime:      time.Now(),
		Status:         StatusGreeting,
		Transcript:     make([]MessageExchange, 0),
		CollectedData:  make(map[string]interface{}),
		QuestionsAsked: make(map[string]bool),
	}

	m.activeConversations[conversation.ID] = conversation

	logger.Info("Started new conversation",
		"conversation_id", conversation.ID,
		"patient_phone", patientPhone)

	return conversation, nil
}

// GetConversation retrieves a conversation by ID
func (m *ConversationManager) GetConversation(conversationID string) (*Conversation, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	return conversation, exists
}

// CompleteConversation marks a conversation as complete
func (m *ConversationManager) CompleteConversation(conversationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return fmt.Errorf("conversation %s not found", conversationID)
	}

	conversation.Status = StatusComplete
	conversation.EndTime = time.Now()
	conversation.Duration = int(conversation.EndTime.Sub(conversation.StartTime).Seconds())

	logger.Info("Completed conversation",
		"conversation_id", conversationID,
		"duration_seconds", conversation.Duration,
		"exchanges", len(conversation.Transcript),
		"collected_keys", getMapKeys(conversation.CollectedData))

	return nil
}

// AddMessage adds a message exchange to the conversation
func (m *ConversationManager) AddMessage(conversationID string, speaker string, text string, confidence float64) error {
	m.mu.Lock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("conversation %s not found", conversationID)
	}

	// Check if this exact message already exists in recent history (prevents duplicates)
	isDuplicate := false
	for i := len(conversation.Transcript) - 1; i >= 0 && i >= len(conversation.Transcript)-3; i-- {
		if conversation.Transcript[i].Speaker == speaker &&
			conversation.Transcript[i].Text == text {
			// Message already exists, but we'll still process it to advance state
			logger.Info("Found duplicate message, but will process anyway",
				"conversation_id", conversationID,
				"speaker", speaker)
			isDuplicate = true
			break
		}
	}

	// Only add to transcript if not a duplicate (to avoid bloat)
	if !isDuplicate {
		exchange := MessageExchange{
			Speaker:     speaker,
			Text:        text,
			Timestamp:   time.Now(),
			Confidence:  confidence,
			IsProcessed: speaker != "patient",
		}

		conversation.Transcript = append(conversation.Transcript, exchange)

		logger.Info("Added message to conversation",
			"conversation_id", conversationID,
			"speaker", speaker,
			"text_length", len(text),
			"confidence", confidence)
	}

	// Process patient messages whether duplicate or not
	// This ensures state progression even with duplicate messages
	if speaker == "patient" {
		// If duplicate, we don't want to add to transcript but still want to process
		messageIndex := len(conversation.Transcript) - 1
		if isDuplicate && messageIndex >= 0 {
			// Mark the message as needing processing
			conversation.Transcript[messageIndex].IsProcessed = false
		}
		
		// If we didn't add a new message but there are no patient messages
		// (unlikely edge case), don't try to process
		if isDuplicate && messageIndex < 0 {
			m.mu.Unlock()
			return nil
		}
		
		m.mu.Unlock() // Temporarily unlock to allow other operations

		// Analyze the message
		analyzeErr := m.analyzePatientMessageSync(conversationID, messageIndex)
		if analyzeErr != nil {
			logger.Error("Error analyzing patient message", "error", analyzeErr)
		}

		return nil
	}

	m.mu.Unlock()
	return nil
}

// analyzePatientMessageSync analyzes a patient message synchronously
func (m *ConversationManager) analyzePatientMessageSync(conversationID string, messageIndex int) error {
	m.mu.RLock()
	conversation, exists := m.activeConversations[conversationID]
	if !exists || messageIndex >= len(conversation.Transcript) {
		m.mu.RUnlock()
		logger.Error("Cannot analyze message - conversation not found or message index invalid",
			"conversation_id", conversationID,
			"message_index", messageIndex)
		return fmt.Errorf("conversation not found or invalid message index")
	}

	message := conversation.Transcript[messageIndex]
	if message.Speaker != "patient" || message.IsProcessed {
		m.mu.RUnlock()
		return nil
	}

	currentStatus := conversation.Status
	inputText := message.Text
	m.mu.RUnlock()

	// First, check for special commands that might override the normal flow
	textLower := strings.ToLower(inputText)

	// If user is trying to correct their name
	if (currentStatus != StatusName && currentStatus != StatusGreeting) &&
		(strings.Contains(textLower, "my name") || strings.Contains(textLower, "name is")) {
		// Special handling for name correction
		logger.Info("Detected name correction attempt",
			"conversation_id", conversationID,
			"input", inputText)

		// Extract the name and update directly
		nameExtractPrompt := `Extract the patient's name from their response.
If you can't find a name, just return an empty string or your best guess.
Output as JSON: {"patient_name": "Full Name"}`

		fullPrompt := fmt.Sprintf(`
Extract information from: "%s"
%s
IMPORTANT: Your response MUST be valid JSON with no additional text.`, inputText, nameExtractPrompt)

		response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
			MaxTokens:   256,
			Temperature: 0.1,
			ModelName:   "gpt-3.5-turbo",
		})

		if err == nil {
			jsonText := extractJSON(response.Text)
			var extractedData map[string]interface{}
			if json.Unmarshal([]byte(jsonText), &extractedData) == nil {
				if name, ok := extractedData["patient_name"].(string); ok && name != "" {
					m.mu.Lock()
					conversation, stillExists := m.activeConversations[conversationID]
					if stillExists {
						conversation.PatientName = name
						conversation.CollectedData["patient_name"] = name
						conversation.CollectedData["name"] = name

						// Mark message as processed but DO NOT advance state
						conversation.Transcript[messageIndex].IsProcessed = true
						logger.Info("Updated patient name through correction",
							"conversation_id", conversationID,
							"name", name)
					}
					m.mu.Unlock()
					return nil
				}
			}
		}
	}

	// Normal processing based on current status
	var analysisPrompt string

	switch currentStatus {
	case StatusName, StatusGreeting:
		analysisPrompt = `Extract the patient's name from their response.
If you can't find a name, just return an empty string or your best guess.
Output as JSON: {"patient_name": "Full Name"}`

	case StatusAge:
		analysisPrompt = `Extract the patient's age from their response.
If you can't find the age, return 0.
Output as JSON: {"age": 42}`

	case StatusGender:
		analysisPrompt = `Extract the patient's gender from their response.
Output as JSON: {"gender": "male/female/other"}`

	case StatusSymptoms:
		analysisPrompt = `Extract all symptoms mentioned by the patient.
Be thorough and include ALL symptoms even if mentioned briefly.
Output as JSON: {"symptoms": ["symptom 1", "symptom 2", ...]}`

	case StatusConditions:
		analysisPrompt = `Extract all medical conditions mentioned by the patient.
Include ANY health conditions mentioned such as diabetes, hypertension, etc.
Output as JSON: {"conditions": ["condition 1", "condition 2", ...]}`

	case StatusMedication:
		analysisPrompt = `Extract all medications mentioned by the patient.
Include ANY medications mentioned even in passing.
Output as JSON: {"medications": ["med 1", "med 2", ...]}`

	case StatusAllergies:
		analysisPrompt = `Extract all allergies mentioned by the patient.
Include ANY allergies mentioned even in passing.
Output as JSON: {"allergies": ["allergy 1", "allergy 2", ...]}`

	default:
		analysisPrompt = `Extract ANY relevant health information from the text.
Look for medications, conditions, allergies, symptoms, age, or other health data.
Output as JSON with ALL the following fields (use empty arrays if none found):
{"patient_name": "", "age": 0, "gender": "", "medications": [], "conditions": [], "allergies": [], "symptoms": []}`
	}

	fullPrompt := fmt.Sprintf(`
Extract information from: "%s"
%s
IMPORTANT: Your response MUST be valid JSON with no additional text.
If certain information is missing, include empty values rather than omitting fields.`, inputText, analysisPrompt)

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:   256,
		Temperature: 0.1,
		ModelName:   "gpt-3.5-turbo",
	})

	if err != nil {
		logger.Error("Error analyzing patient message", "error", err)
		// Implement fallback extraction regardless of error
		fallbackExtraction(m, conversationID, messageIndex, inputText)

		// Even after error, we should mark the message as processed and advance the state
		m.mu.Lock()
		conversation, stillExists := m.activeConversations[conversationID]
		if stillExists && messageIndex < len(conversation.Transcript) {
			conversation.Transcript[messageIndex].IsProcessed = true
			// Only advance the state after processing a patient message if we extracted needed info
			advanceConversationState(conversation)
			logger.Info("Forced state advancement after extraction error",
				"conversation_id", conversationID,
				"new_state", string(conversation.Status))
		}
		m.mu.Unlock()

		return err
	}

	// Extract JSON from response
	jsonText := extractJSON(response.Text)

	var extractedData map[string]interface{}
	err = json.Unmarshal([]byte(jsonText), &extractedData)
	if err != nil {
		logger.Error("Error parsing JSON from analysis", "error", err, "json", jsonText)
		// Implement fallback extraction
		fallbackExtraction(m, conversationID, messageIndex, inputText)

		// Even after JSON parsing error, mark message as processed and advance the state
		m.mu.Lock()
		conversation, stillExists := m.activeConversations[conversationID]
		if stillExists && messageIndex < len(conversation.Transcript) {
			conversation.Transcript[messageIndex].IsProcessed = true
			advanceConversationState(conversation)
			logger.Info("Forced state advancement after JSON parsing error",
				"conversation_id", conversationID,
				"new_state", string(conversation.Status))
		}
		m.mu.Unlock()

		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists = m.activeConversations[conversationID]
	if !exists || messageIndex >= len(conversation.Transcript) {
		return fmt.Errorf("conversation no longer exists")
	}

	conversation.Transcript[messageIndex].IsProcessed = true

	// Variable to track whether we successfully extracted the expected information
	extractedExpectedInfo := false

	// Update collected data based on conversation state
	switch currentStatus {
	case StatusGreeting, StatusName:
		if name, ok := extractedData["patient_name"].(string); ok && name != "" {
			conversation.PatientName = name
			conversation.CollectedData["patient_name"] = name
			conversation.CollectedData["name"] = name
			logger.Info("Extracted patient name", "name", name)
			extractedExpectedInfo = true
		}

	case StatusAge:
		if age, ok := extractedData["age"].(float64); ok && age > 0 {
			conversation.CollectedData["age"] = age
			logger.Info("Extracted patient age", "age", age)
			extractedExpectedInfo = true
		} else if ageStr, ok := extractedData["age"].(string); ok {
			// Try to convert string to number
			if age, err := strconv.ParseFloat(ageStr, 64); err == nil && age > 0 {
				conversation.CollectedData["age"] = age
				logger.Info("Extracted patient age from string", "age", age)
				extractedExpectedInfo = true
			}
		}

	case StatusGender:
		if gender, ok := extractedData["gender"].(string); ok && gender != "" {
			conversation.CollectedData["gender"] = gender
			logger.Info("Extracted patient gender", "gender", gender)
			extractedExpectedInfo = true
		}

	case StatusSymptoms:
		if symptoms, ok := extractedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
			conversation.CollectedData["symptoms"] = symptoms
			logger.Info("Extracted patient symptoms", "count", len(symptoms))
			extractedExpectedInfo = true
		}

	case StatusConditions:
		if conditions, ok := extractedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
			conversation.CollectedData["conditions"] = conditions
			logger.Info("Extracted patient conditions", "count", len(conditions))
			extractedExpectedInfo = true
		} else if strings.Contains(strings.ToLower(inputText), "no") ||
			strings.Contains(strings.ToLower(inputText), "none") ||
			strings.Contains(strings.ToLower(inputText), "don't have") {
			// Handle negative responses
			conversation.CollectedData["conditions"] = []interface{}{}
			extractedExpectedInfo = true
		}

	case StatusMedication:
		if medications, ok := extractedData["medications"].([]interface{}); ok && len(medications) > 0 {
			conversation.CollectedData["medications"] = medications
			logger.Info("Extracted patient medications", "count", len(medications))
			extractedExpectedInfo = true
		} else if strings.Contains(strings.ToLower(inputText), "no") ||
			strings.Contains(strings.ToLower(inputText), "none") ||
			strings.Contains(strings.ToLower(inputText), "don't take") {
			// Handle negative responses
			conversation.CollectedData["medications"] = []interface{}{}
			extractedExpectedInfo = true
		}

	case StatusAllergies:
		if allergies, ok := extractedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
			conversation.CollectedData["allergies"] = allergies
			logger.Info("Extracted patient allergies", "count", len(allergies))
			extractedExpectedInfo = true
		} else if strings.Contains(strings.ToLower(inputText), "no") ||
			strings.Contains(strings.ToLower(inputText), "none") ||
			strings.Contains(strings.ToLower(inputText), "don't have") {
			// Handle negative responses
			conversation.CollectedData["allergies"] = []interface{}{}
			extractedExpectedInfo = true
		}
	}

	// Always check for specific health data regardless of conversation state
	scanForHealthData(extractedData, conversation.CollectedData)

	// Use more aggressive data extraction for common fields
	if !extractedExpectedInfo {
		// Try to extract name if we're in the name state
		if currentStatus == StatusName || currentStatus == StatusGreeting {
			nameFinder := `Extract ONLY the patient's name from this text.
			Output JUST the name as a string with no additional text.
			Text: "%s"`
			
			namePrompt := fmt.Sprintf(nameFinder, inputText)
			response, err := m.aiClient.GenerateCompletion(context.Background(), namePrompt, ai.CompletionOptions{
				MaxTokens:   128,
				Temperature: 0.1,
				ModelName:   "gpt-3.5-turbo",
			})
			
			if err == nil && response.Text != "" {
				potentialName := strings.TrimSpace(response.Text)
				if len(potentialName) > 0 && len(potentialName) < 50 {
					conversation.PatientName = potentialName
					conversation.CollectedData["patient_name"] = potentialName
					conversation.CollectedData["name"] = potentialName
					logger.Info("Extracted patient name with secondary method", "name", potentialName)
					extractedExpectedInfo = true
				}
			}
		}
		
		// Try to extract age if we're in the age state
		if currentStatus == StatusAge && !extractedExpectedInfo {
			textLower := strings.ToLower(inputText)
			// Look for patterns like "I am 42" or "42 years old"
			for _, word := range strings.Fields(textLower) {
				if num, err := strconv.ParseFloat(word, 64); err == nil && num > 0 && num < 120 {
					conversation.CollectedData["age"] = num
					logger.Info("Extracted patient age from direct text parsing", "age", num)
					extractedExpectedInfo = true
					break
				}
			}
		}
		
		// Try to extract gender if we're in the gender state
		if currentStatus == StatusGender && !extractedExpectedInfo {
			textLower := strings.ToLower(inputText)
			if strings.Contains(textLower, "male") || strings.Contains(textLower, "man") || strings.Contains(textLower, "boy") {
				conversation.CollectedData["gender"] = "male"
				logger.Info("Extracted patient gender as male from text")
				extractedExpectedInfo = true
			} else if strings.Contains(textLower, "female") || strings.Contains(textLower, "woman") || strings.Contains(textLower, "girl") {
				conversation.CollectedData["gender"] = "female"
				logger.Info("Extracted patient gender as female from text")
				extractedExpectedInfo = true
			} else if strings.Contains(textLower, "non-binary") || strings.Contains(textLower, "nonbinary") || 
					  strings.Contains(textLower, "other") || strings.Contains(textLower, "they") {
				conversation.CollectedData["gender"] = "other"
				logger.Info("Extracted patient gender as other from text")
				extractedExpectedInfo = true
			}
		}
	}

	// Always advance the state for a more fluid conversation, but track if we succeeded
	// in extracting the expected information
	advanceConversationState(conversation)

	// Log appropriate message based on extraction success
	if extractedExpectedInfo {
		logger.Info("Advanced conversation state after successful extraction",
			"conversation_id", conversationID,
			"new_state", string(conversation.Status),
			"extracted_info", true)
	} else {
		logger.Info("Advanced conversation state despite no extraction",
			"conversation_id", conversationID,
			"new_state", string(conversation.Status),
			"extracted_info", false)
	}

	return nil
}

func (m *ConversationManager) RepeatLastQuestion(conversationID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}

	// Find the last AI message
	for i := len(conversation.Transcript) - 1; i >= 0; i-- {
		if conversation.Transcript[i].Speaker == "ai" {
			return conversation.Transcript[i].Text, nil
		}
	}

	// Fallback if no previous AI message found
	return "I didn't understand that. Could you please clarify?", nil
}

// GetNextResponse generates the next AI response based on the conversation state
func (m *ConversationManager) GetNextResponse(conversationID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}

	// Use the conversation state instead of counting messages to determine the next question
	currentState := conversation.Status
	patientData := conversation.CollectedData
	var nextQuestion string
	var nextState ConversationStatus

	// Count messages for logging
	patientMsgCount := 0
	for _, msg := range conversation.Transcript {
		if msg.Speaker == "patient" {
			patientMsgCount++
		}
	}

	logger.Info("Generating next response",
		"conversation_id", conversationID,
		"current_state", string(currentState),
		"patient_messages", patientMsgCount,
		"collected_data_keys", getMapKeys(patientData))

	// Determine the next question based on the current state
	switch currentState {
	case StatusGreeting, StatusName:
		nextQuestion = "Hello, I'm the medical assistant from Sword Symphony. What is your name?"
		nextState = StatusName
		
	case StatusAge:
		if conversation.PatientName != "" {
			nextQuestion = fmt.Sprintf("Thank you, %s. What is your age?", conversation.PatientName)
		} else if name, ok := patientData["name"].(string); ok && name != "" {
			conversation.PatientName = name
			nextQuestion = fmt.Sprintf("Thank you, %s. What is your age?", name)
		} else {
			nextQuestion = "Thank you. What is your age?"
		}
		nextState = StatusAge
		
	case StatusGender:
		nextQuestion = "What is your gender? Male, female, or other?"
		nextState = StatusGender
		
	case StatusSymptoms:
		nextQuestion = "What symptoms are you experiencing? Please tell me any health issues you're having."
		nextState = StatusSymptoms
		
	case StatusConditions:
		nextQuestion = "Do you have any existing medical conditions like diabetes, hypertension, or asthma?"
		nextState = StatusConditions
		
	case StatusMedication:
		nextQuestion = "What medications are you currently taking? If none, please say 'none'."
		nextState = StatusMedication
		
	case StatusAllergies:
		nextQuestion = "Do you have any allergies to medications or other substances like penicillin or latex?"
		nextState = StatusAllergies
		
	case StatusClosing:
		nextQuestion = "Thank you for providing all the information. We have everything we need. Have a good day. Goodbye."
		nextState = StatusComplete
		
	case StatusComplete:
		nextQuestion = "Thank you for your time. Your information has been recorded. Goodbye."
		nextState = StatusComplete
		
	default:
		// If we somehow get an unknown state, reset to greeting
		nextQuestion = "Hello, I'm the medical assistant from Sword Symphony. How can I help you today?"
		nextState = StatusGreeting
	}

	// If we have a completed patient data set, move directly to closing
	if hasCompletedBasicInfo(patientData) && currentState != StatusClosing && currentState != StatusComplete {
		nextQuestion = "Thank you for providing all the information. We have everything we need. Have a good day. Goodbye."
		nextState = StatusClosing
	}

	// Update the conversation status - always progress to the next state
	if currentState == nextState && nextState != StatusComplete {
		// Force state advancement if we're still on the same state (except for Complete)
		advanceConversationState(conversation)
		nextState = conversation.Status
	} else {
		conversation.Status = nextState
	}

	// Add the message to the conversation
	exchange := MessageExchange{
		Speaker:     "ai",
		Text:        nextQuestion,
		Timestamp:   time.Now(),
		Confidence:  1.0,
		IsProcessed: true,
	}

	conversation.Transcript = append(conversation.Transcript, exchange)

	logger.Info("Added next question based on message count",
		"conversation_id", conversationID,
		"patient_msg_count", patientMsgCount,
		"next_state", string(nextState),
		"patient_name", conversation.PatientName,
		"response_length", len(nextQuestion))

	return nextQuestion, nil
}

// hasCompletedBasicInfo checks if we have collected enough basic patient information
func hasCompletedBasicInfo(data map[string]interface{}) bool {
	// Check if we have name, age, gender, and at least one symptom
	hasName := false
	if name, ok := data["name"].(string); ok && name != "" {
		hasName = true
	} else if name, ok := data["patient_name"].(string); ok && name != "" {
		hasName = true
	}
	
	hasAge := false
	if age, ok := data["age"].(float64); ok && age > 0 {
		hasAge = true
	} else if age, ok := data["age"].(int); ok && age > 0 {
		hasAge = true
	}
	
	hasGender := false
	if gender, ok := data["gender"].(string); ok && gender != "" {
		hasGender = true
	}
	
	hasSymptoms := false
	if symptoms, ok := data["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
		hasSymptoms = true
	}
	
	return hasName && hasAge && hasGender && hasSymptoms
}

// advanceConversationState moves the conversation to the next logical state
func advanceConversationState(conversation *Conversation) {
	// In this simplified approach, we just force the state to advance
	// regardless of whether we've extracted the data correctly
	switch conversation.Status {
	case StatusGreeting:
		conversation.Status = StatusName
	case StatusName:
		conversation.Status = StatusAge
	case StatusAge:
		conversation.Status = StatusGender
	case StatusGender:
		conversation.Status = StatusSymptoms
	case StatusSymptoms:
		conversation.Status = StatusConditions
	case StatusConditions:
		conversation.Status = StatusMedication
	case StatusMedication:
		conversation.Status = StatusAllergies
	case StatusAllergies:
		conversation.Status = StatusClosing
	case StatusClosing:
		conversation.Status = StatusComplete
	}

	// Mark the question as asked
	stateKey := string(conversation.Status)
	conversation.QuestionsAsked[stateKey] = true
}

// fallbackExtraction attempts to extract critical health information using simple pattern matching
func fallbackExtraction(m *ConversationManager, conversationID string, messageIndex int, text string) {
	textLower := strings.ToLower(text)

	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists || messageIndex >= len(conversation.Transcript) {
		return
	}

	// Basic name extraction
	if strings.Contains(textLower, "name is") || strings.Contains(textLower, "i am") || strings.Contains(textLower, "i'm") {
		parts := strings.Split(text, " ")
		if len(parts) >= 3 {
			possibleName := strings.Join(parts[len(parts)-2:], " ")
			possibleName = strings.TrimRight(possibleName, ".,!?")
			if conversation.PatientName == "" {
				conversation.PatientName = possibleName
				conversation.CollectedData["patient_name"] = possibleName
				conversation.CollectedData["name"] = possibleName
				logger.Info("Fallback extracted name", "name", possibleName)
			}
		}
	}

	// Extract medical conditions
	medicalConditions := []string{"diabetes", "hypertension", "asthma", "arthritis", "depression", "anxiety"}
	for _, condition := range medicalConditions {
		if strings.Contains(textLower, condition) {
			conditions, _ := conversation.CollectedData["conditions"].([]interface{})
			if conditions == nil {
				conditions = make([]interface{}, 0)
			}
			addItemIfNotExists(condition, &conditions)
			conversation.CollectedData["conditions"] = conditions
			logger.Info("Fallback extracted condition", "condition", condition)
		}
	}

	// Extract medications
	commonMeds := []string{"metformin", "insulin", "aspirin", "ibuprofen", "tylenol", "lisinopril"}
	for _, med := range commonMeds {
		if strings.Contains(textLower, med) {
			medications, _ := conversation.CollectedData["medications"].([]interface{})
			if medications == nil {
				medications = make([]interface{}, 0)
			}
			addItemIfNotExists(med, &medications)
			conversation.CollectedData["medications"] = medications
			logger.Info("Fallback extracted medication", "medication", med)
		}
	}

	// Extract allergies
	commonAllergies := []string{"penicillin", "sulfa", "latex", "peanut", "shellfish"}
	if strings.Contains(textLower, "allergic") || strings.Contains(textLower, "allergy") {
		for _, allergy := range commonAllergies {
			if strings.Contains(textLower, allergy) {
				allergies, _ := conversation.CollectedData["allergies"].([]interface{})
				if allergies == nil {
					allergies = make([]interface{}, 0)
				}
				addItemIfNotExists(allergy, &allergies)
				conversation.CollectedData["allergies"] = allergies
				logger.Info("Fallback extracted allergy", "allergy", allergy)
			}
		}
	}

	conversation.Transcript[messageIndex].IsProcessed = true
}

// addItemIfNotExists adds an item to a slice if it doesn't already exist
func addItemIfNotExists(item string, slice *[]interface{}) {
	for _, existing := range *slice {
		if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, item) {
			return
		}
	}
	*slice = append(*slice, item)
}

// scanForHealthData checks for health data in extracted data regardless of conversation state
func scanForHealthData(extractedData map[string]interface{}, collectedData map[string]interface{}) {
	// Always check for medications
	if medications, ok := extractedData["medications"].([]interface{}); ok && len(medications) > 0 {
		existingMeds, _ := collectedData["medications"].([]interface{})
		if existingMeds == nil {
			collectedData["medications"] = medications
		} else {
			for _, med := range medications {
				if medStr, ok := med.(string); ok && medStr != "" {
					var exists bool
					for _, existing := range existingMeds {
						if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, medStr) {
							exists = true
							break
						}
					}
					if !exists {
						existingMeds = append(existingMeds, medStr)
					}
				}
			}
			collectedData["medications"] = existingMeds
		}
	}

	// Always check for conditions
	if conditions, ok := extractedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
		existingConditions, _ := collectedData["conditions"].([]interface{})
		if existingConditions == nil {
			collectedData["conditions"] = conditions
		} else {
			for _, condition := range conditions {
				if condStr, ok := condition.(string); ok && condStr != "" {
					var exists bool
					for _, existing := range existingConditions {
						if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, condStr) {
							exists = true
							break
						}
					}
					if !exists {
						existingConditions = append(existingConditions, condStr)
					}
				}
			}
			collectedData["conditions"] = existingConditions
		}
	}

	// Always check for allergies
	if allergies, ok := extractedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
		existingAllergies, _ := collectedData["allergies"].([]interface{})
		if existingAllergies == nil {
			collectedData["allergies"] = allergies
		} else {
			for _, allergy := range allergies {
				if allergyStr, ok := allergy.(string); ok && allergyStr != "" {
					var exists bool
					for _, existing := range existingAllergies {
						if existingStr, ok := existing.(string); ok && strings.EqualFold(existingStr, allergyStr) {
							exists = true
							break
						}
					}
					if !exists {
						existingAllergies = append(existingAllergies, allergyStr)
					}
				}
			}
			collectedData["allergies"] = existingAllergies
		}
	}
}

// SynchronizedAddPatientMessage adds a patient message and waits for processing to complete
func (m *ConversationManager) SynchronizedAddPatientMessage(conversationID string, text string, confidence float64) error {
	processingDone := make(chan error, 1)

	// First, add the message - this will trigger analysis
	err := m.AddMessage(conversationID, "patient", text, confidence)
	if err != nil {
		return err
	}

	// Wait for processing to complete (with timeout)
	go func() {
		// Small wait to ensure analysis starts
		time.Sleep(100 * time.Millisecond)

		// Poll to check if message is processed
		for i := 0; i < 10; i++ {
			m.mu.RLock()
			conversation, exists := m.activeConversations[conversationID]
			if !exists {
				m.mu.RUnlock()
				processingDone <- fmt.Errorf("conversation %s not found", conversationID)
				return
			}

			// Find the last patient message
			var lastPatientMsg *MessageExchange
			for i := len(conversation.Transcript) - 1; i >= 0; i-- {
				if conversation.Transcript[i].Speaker == "patient" {
					lastPatientMsg = &conversation.Transcript[i]
					break
				}
			}

			if lastPatientMsg != nil && lastPatientMsg.IsProcessed {
				// Message is processed, state has been updated
				m.mu.RUnlock()
				processingDone <- nil
				return
			}
			m.mu.RUnlock()

			// Wait before checking again
			time.Sleep(200 * time.Millisecond)
		}

		// If we get here, processing is taking too long - force advance state
		m.mu.Lock()
		conversation, exists := m.activeConversations[conversationID]
		if exists {
			advanceConversationState(conversation)
			logger.Info("Forced state advancement after timeout",
				"conversation_id", conversationID,
				"new_state", string(conversation.Status))
		}
		m.mu.Unlock()

		processingDone <- nil
	}()

	// Wait with timeout
	select {
	case err := <-processingDone:
		return err
	case <-time.After(3 * time.Second):
		// If it takes too long, continue anyway
		return nil
	}
}

// ProcessConversationData prepares the collected data for external systems
func (m *ConversationManager) ProcessConversationData(conversationID string) (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return nil, fmt.Errorf("conversation %s not found", conversationID)
	}

	// Log current collected data for debugging
	logger.Info("Processing conversation data",
		"conversation_id", conversationID,
		"collected_data_keys", getMapKeys(conversation.CollectedData),
		"patient_name", conversation.PatientName)

	// Create a patient data structure that matches our target format exactly
	patientData := map[string]any{
		"id":          fmt.Sprintf("P%d", time.Now().Unix()),
		"name":        conversation.PatientName,
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

	// Set patient name - check multiple possible fields
	if conversation.PatientName != "" {
		patientData["name"] = conversation.PatientName
	} else if name, ok := conversation.CollectedData["name"].(string); ok && name != "" {
		patientData["name"] = name
		// Update the conversation's PatientName for consistency
		conversation.PatientName = name
	} else if name, ok := conversation.CollectedData["patient_name"].(string); ok && name != "" {
		patientData["name"] = name
		// Update the conversation's PatientName for consistency
		conversation.PatientName = name
	}

	// Try to extract data from conversation transcript if we don't have it
	if patientData["name"] == "" || patientData["age"] == 0.0 || patientData["gender"] == "" {
		for _, msg := range conversation.Transcript {
			if msg.Speaker == "patient" {
				// Try to extract missing data from patient messages
				text := strings.ToLower(msg.Text)
				
				// Extract name if missing
				if patientData["name"] == "" && (strings.Contains(text, "name is") || strings.Contains(text, "call me") || strings.Contains(text, "i am ")) {
					// This is a crude extraction just for fallback
					words := strings.Fields(msg.Text)
					if len(words) >= 3 {
						for i := 0; i < len(words)-2; i++ {
							if strings.Contains(strings.ToLower(words[i]), "name") && 
							   strings.ToLower(words[i+1]) == "is" {
								potentialName := strings.Join(words[i+2:i+4], " ")
								// Clean up any punctuation
								potentialName = strings.Trim(potentialName, ",.!?;:")
								if potentialName != "" {
									patientData["name"] = potentialName
									conversation.PatientName = potentialName
									conversation.CollectedData["name"] = potentialName
									conversation.CollectedData["patient_name"] = potentialName
									break
								}
							}
						}
					}
				}
				
				// Extract age if missing
				if patientData["age"].(float64) == 0.0 {
					for _, word := range strings.Fields(text) {
						if num, err := strconv.ParseFloat(word, 64); err == nil && num > 0 && num < 120 {
							patientData["age"] = num
							conversation.CollectedData["age"] = num
							break
						}
					}
				}
				
				// Extract gender if missing
				if patientData["gender"] == "" {
					if strings.Contains(text, "male") && !strings.Contains(text, "female") {
						patientData["gender"] = "male"
						conversation.CollectedData["gender"] = "male"
					} else if strings.Contains(text, "female") {
						patientData["gender"] = "female"
						conversation.CollectedData["gender"] = "female"
					}
				}
			}
		}
	}

	// Set default age/gender if not available
	if ageValue, ok := conversation.CollectedData["age"]; ok {
		if age, ok := ageValue.(float64); ok {
			patientData["age"] = age
		} else if age, ok := ageValue.(int); ok {
			patientData["age"] = float64(age)
		} else if ageStr, ok := ageValue.(string); ok && ageStr != "" {
			// Try to convert string to number
			if age, err := strconv.ParseFloat(ageStr, 64); err == nil {
				patientData["age"] = age
			}
		}
	}

	if genderValue, ok := conversation.CollectedData["gender"]; ok {
		if gender, ok := genderValue.(string); ok {
			patientData["gender"] = gender
		}
	}

	// Process symptoms - convert from interface slice to string slice
	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok {
		symptomStrings := make([]string, 0, len(symptoms))
		for _, s := range symptoms {
			if symptom, ok := s.(string); ok {
				symptomStrings = append(symptomStrings, symptom)
			} else {
				// Try converting to string
				symptomStrings = append(symptomStrings, fmt.Sprintf("%v", s))
			}
		}
		patientData["symptoms"] = symptomStrings
	}

	// Process conditions
	if conditions, ok := conversation.CollectedData["conditions"].([]interface{}); ok {
		conditionStrings := make([]string, 0, len(conditions))
		for _, c := range conditions {
			if condition, ok := c.(string); ok {
				conditionStrings = append(conditionStrings, condition)
			} else {
				// Try converting to string
				conditionStrings = append(conditionStrings, fmt.Sprintf("%v", c))
			}
		}
		patientData["conditions"] = conditionStrings
	}

	// Process medications
	if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok {
		medicationStrings := make([]string, 0, len(medications))
		for _, m := range medications {
			if medication, ok := m.(string); ok {
				medicationStrings = append(medicationStrings, medication)
			} else {
				// Try converting to string
				medicationStrings = append(medicationStrings, fmt.Sprintf("%v", m))
			}
		}
		patientData["medications"] = medicationStrings
	}

	// Process allergies
	if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok {
		allergyStrings := make([]string, 0, len(allergies))
		for _, a := range allergies {
			if allergy, ok := a.(string); ok {
				allergyStrings = append(allergyStrings, allergy)
			} else {
				// Try converting to string
				allergyStrings = append(allergyStrings, fmt.Sprintf("%v", a))
			}
		}
		patientData["allergies"] = allergyStrings
	}

	// Save original conversation ID for reference
	patientData["_conversation_id"] = conversationID

	logger.Info("Standardized patient data",
		"original_keys", getMapKeys(conversation.CollectedData),
		"standardized_keys", getMapKeys(patientData),
		"name", patientData["name"],
		"age", patientData["age"],
		"gender", patientData["gender"],
		"symptom_count", len(patientData["symptoms"].([]string)),
		"condition_count", len(patientData["conditions"].([]string)))

	return map[string]any{
		"patient_data": patientData,
		"conversation_data": map[string]any{
			"start_time": conversation.StartTime.Format(time.RFC3339),
			"status":     string(conversation.Status),
			"id":         conversationID,
		},
	}, nil
}

// GenerateTranscript creates a formatted transcript of the conversation
func (m *ConversationManager) GenerateTranscript(conversationID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}

	var transcript strings.Builder
	transcript.WriteString(fmt.Sprintf("Conversation Transcript - %s\n", conversation.StartTime.Format("January 2, 2006 at 3:04 PM")))
	transcript.WriteString(fmt.Sprintf("Patient: %s\n", conversation.PatientName))
	transcript.WriteString("-----------------------------------------\n\n")

	for _, exchange := range conversation.Transcript {
		speaker := exchange.Speaker
		if speaker == "ai" {
			speaker = "Doctor"
		} else {
			speaker = "Patient"
		}
		transcript.WriteString(fmt.Sprintf("[%s] %s: %s\n\n",
			exchange.Timestamp.Format("3:04:05 PM"),
			speaker,
			exchange.Text))
	}

	return transcript.String(), nil
}

// GetKey retrieves a specific piece of data from the conversation
func (m *ConversationManager) GetKey(conversationID, key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return nil, false
	}

	value, exists := conversation.CollectedData[key]
	return value, exists
}

// SetKey sets a specific piece of data in the conversation
func (m *ConversationManager) SetKey(conversationID, key string, value any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return false
	}

	conversation.CollectedData[key] = value
	return true
}

// getMapKeys is a helper function to get keys from a map
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// GetConversationMessages retrieves all messages in a conversation
func (m *ConversationManager) GetConversationMessages(conversationID string) ([]MessageExchange, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return nil, fmt.Errorf("conversation %s not found", conversationID)
	}

	// Return a copy to prevent modification
	messages := make([]MessageExchange, len(conversation.Transcript))
	copy(messages, conversation.Transcript)

	return messages, nil
}

// extractJSON extracts JSON from a string
func extractJSON(text string) string {
	// First look for explicit JSON between curly braces
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		return text[jsonStart : jsonEnd+1]
	}

	// If we couldn't find JSON, create a structured response based on the content
	result := map[string]interface{}{
		"error": "No valid JSON found in response",
	}

	// Try to extract common health information using simple pattern matching
	textLower := strings.ToLower(text)

	if strings.Contains(textLower, "diabet") {
		result["conditions"] = []string{"diabetes"}
	}

	if strings.Contains(textLower, "metformin") {
		result["medications"] = []string{"metformin"}
	}

	if strings.Contains(textLower, "penicillin") &&
		(strings.Contains(textLower, "allergic") || strings.Contains(textLower, "allergy")) {
		result["allergies"] = []string{"penicillin"}
	}

	if strings.Contains(textLower, "name is") || strings.Contains(textLower, "my name") {
		parts := strings.Split(text, " ")
		if len(parts) >= 3 {
			for i := 0; i < len(parts)-1; i++ {
				if strings.Contains(strings.ToLower(parts[i]), "name") &&
					(parts[i+1] == "is" || parts[i+1] == "was") &&
					i+2 < len(parts) {
					potentialName := strings.Join(parts[i+2:min(i+4, len(parts))], " ")
					potentialName = strings.TrimRight(potentialName, ".,!?")
					result["patient_name"] = potentialName
					break
				}
			}
		}
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "{\"error\":\"Failed to parse response\",\"raw_text\":\"" + text + "\"}"
	}

	return string(jsonBytes)
}

// Helper function min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func formatRecentMessages(messages []MessageExchange) string {
	var builder strings.Builder
	for _, msg := range messages {
		speaker := "Patient"
		if msg.Speaker == "ai" {
			speaker = "Assistant"
		}
		builder.WriteString(fmt.Sprintf("%s: %s\n", speaker, msg.Text))
	}
	return builder.String()
}

// GetLastAIResponse gets the last AI response for a call
func (m *ConversationManager) GetLastAIResponse(conversationID string) (string, error) {
	messages, err := m.GetConversationMessages(conversationID)
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
