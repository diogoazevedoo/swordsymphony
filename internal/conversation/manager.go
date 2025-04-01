package conversation

import (
	"context"
	"encoding/json"
	"fmt"
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
	StatusIdentity   ConversationStatus = "identity"
	StatusSymptoms   ConversationStatus = "symptoms"
	StatusHistory    ConversationStatus = "history"
	StatusMedication ConversationStatus = "medication"
	StatusQuestions  ConversationStatus = "questions"
	StatusClosing    ConversationStatus = "closing"
	StatusComplete   ConversationStatus = "complete"
	StatusError      ConversationStatus = "error"
)

// Conversation represents an ongoing conversation with a patient
type Conversation struct {
	ID            string                 `json:"id"`
	PatientPhone  string                 `json:"patient_phone"`
	PatientName   string                 `json:"patient_name"`
	PatientEmail  string                 `json:"patient_email"`
	StartTime     time.Time              `json:"start_time"`
	EndTime       time.Time              `json:"end_time"`
	Status        ConversationStatus     `json:"status"`
	Transcript    []MessageExchange      `json:"transcript"`
	CollectedData map[string]interface{} `json:"collected_data"`
	Duration      int                    `json:"duration"`
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
	greetingPrompt      string
	identityPrompt      string
	symptomsPrompt      string
	historyPrompt       string
	medicationPrompt    string
	questionsPrompt     string
	closingPrompt       string
	mu                  sync.RWMutex
}

// ManagerOption configures a conversation manager
type ManagerOption func(*ConversationManager)

// NewConversationManager creates a new conversation manager with direct, focused prompts
func NewConversationManager(aiClient ai.Client, options ...ManagerOption) *ConversationManager {
	manager := &ConversationManager{
		aiClient:            aiClient,
		activeConversations: make(map[string]*Conversation),
		systemPrompt: `You are a medical assistant gathering basic information for a patient record.
Be friendly but EXTREMELY concise. Your goal is ONLY to collect specific required information.
Keep each response under 20 words when possible. Ask exactly ONE question at a time.
NEVER repeat questions you've already asked.
Do not provide any diagnosis or medical advice.`,

		greetingPrompt: `Briefly say hello and ask for the patient's name only.`,

		identityPrompt: `Simply ask "What is your age and gender?" No other questions.`,

		symptomsPrompt: `Ask "What symptoms are you experiencing right now?" Nothing else.`,

		historyPrompt: `Ask "Do you have any existing medical conditions?" Keep it short.`,

		medicationPrompt: `Ask "What medications are you taking, and do you have any allergies?"`,

		questionsPrompt: `Ask "Is there anything else important about your health I should know?"`,

		closingPrompt: `Say "Thank you for the information" and end the call politely.`,
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

// WithPromptTemplate sets a custom prompt template for a specific stage
func WithPromptTemplate(stage ConversationStatus, prompt string) ManagerOption {
	return func(m *ConversationManager) {
		switch stage {
		case StatusGreeting:
			m.greetingPrompt = prompt
		case StatusIdentity:
			m.identityPrompt = prompt
		case StatusSymptoms:
			m.symptomsPrompt = prompt
		case StatusHistory:
			m.historyPrompt = prompt
		case StatusMedication:
			m.medicationPrompt = prompt
		case StatusQuestions:
			m.questionsPrompt = prompt
		case StatusClosing:
			m.closingPrompt = prompt
		}
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
		ID:            fmt.Sprintf("conv_%d", time.Now().UnixNano()),
		PatientPhone:  patientPhone,
		StartTime:     time.Now(),
		Status:        StatusGreeting,
		Transcript:    make([]MessageExchange, 0),
		CollectedData: make(map[string]interface{}),
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
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return fmt.Errorf("conversation %s not found", conversationID)
	}

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

	// Unlock before processing to prevent deadlocks but reacquire after
	m.mu.Unlock()

	// Process patient messages synchronously instead of in a goroutine
	if speaker == "patient" && !exchange.IsProcessed {
		m.analyzePatientMessage(conversationID, len(conversation.Transcript)-1)
	}

	// Reacquire the lock for the return
	m.mu.Lock()
	return nil
}

// GetNextResponse generates the next AI response with improved flow control
func (m *ConversationManager) GetNextResponse(conversationID string) (string, error) {
	m.mu.RLock()
	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		m.mu.RUnlock()
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}
	m.mu.RUnlock()

	// Check if we already have enough exchanges to determine what question to ask next
	questionAsked := make(map[string]bool)

	for _, exchange := range conversation.Transcript {
		if exchange.Speaker == "ai" {
			text := strings.ToLower(exchange.Text)
			if strings.Contains(text, "email") {
				questionAsked["email"] = true
			}
			if strings.Contains(text, "health concern") || strings.Contains(text, "symptoms") {
				questionAsked["symptoms"] = true
			}
			if strings.Contains(text, "medical history") || strings.Contains(text, "health conditions") {
				questionAsked["history"] = true
			}
			if strings.Contains(text, "medications") || strings.Contains(text, "allergies") {
				questionAsked["medications"] = true
			}
		}
	}

	var prompt string
	switch conversation.Status {
	case StatusGreeting:
		prompt = m.greetingPrompt
	case StatusIdentity:
		// Check if we need to get email
		if !questionAsked["email"] {
			prompt = strings.Replace(m.identityPrompt, "{{.PatientName}}", conversation.PatientName, -1)
		} else {
			// Force progress to symptoms if we've asked about email
			m.mu.Lock()
			conversation.Status = StatusSymptoms
			m.mu.Unlock()
			prompt = m.symptomsPrompt
		}
	case StatusSymptoms:
		// Check if we need to ask about symptoms
		if !questionAsked["symptoms"] {
			prompt = m.symptomsPrompt
		} else {
			// Force progress to history if we've asked about symptoms
			m.mu.Lock()
			conversation.Status = StatusHistory
			m.mu.Unlock()
			prompt = m.historyPrompt
		}
	case StatusHistory:
		// Check if we need to ask about medical history
		if !questionAsked["history"] {
			prompt = m.historyPrompt
		} else {
			// Force progress to medications if we've asked about history
			m.mu.Lock()
			conversation.Status = StatusMedication
			m.mu.Unlock()
			prompt = m.medicationPrompt
		}
	case StatusMedication:
		// Check if we need to ask about medications
		if !questionAsked["medications"] {
			prompt = m.medicationPrompt
		} else {
			// Force progress to questions if we've asked about medications
			m.mu.Lock()
			conversation.Status = StatusQuestions
			m.mu.Unlock()
			prompt = m.questionsPrompt
		}
	case StatusQuestions:
		prompt = m.questionsPrompt
	case StatusClosing:
		prompt = m.closingPrompt
	default:
		prompt = "Continue the conversation naturally based on the context. Ask appropriate follow-up questions."
	}

	var conversationContext strings.Builder
	conversationContext.WriteString("Here is the conversation so far:\n\n")

	for _, exchange := range conversation.Transcript {
		speakerName := exchange.Speaker
		if speakerName == "patient" {
			speakerName = "Patient"
		} else {
			speakerName = "Doctor"
		}
		conversationContext.WriteString(fmt.Sprintf("%s: %s\n", speakerName, exchange.Text))
	}

	fullPrompt := fmt.Sprintf(`
%s

Current conversation stage: %s

%s

Conversation context:
%s

Based on the conversation so far and the current stage, provide your next response as the AI doctor.
Keep your response brief and focused on asking the specific question needed for this stage.
Do not repeat questions that have already been asked.
Only ask one question at a time.
`, m.systemPrompt, conversation.Status, prompt, conversationContext.String())

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:    150,
		Temperature:  0.7,
		SystemPrompt: m.systemPrompt,
	})

	if err != nil {
		logger.Error("Error generating AI response", "error", err)
		return "", fmt.Errorf("error generating response: %w", err)
	}

	aiResponse := response.Text

	aiResponse = strings.ReplaceAll(aiResponse, "Doctor:", "")
	aiResponse = strings.ReplaceAll(aiResponse, "AI Doctor:", "")
	aiResponse = strings.TrimSpace(aiResponse)

	err = m.AddMessage(conversationID, "ai", aiResponse, 1.0)
	if err != nil {
		logger.Error("Error adding AI response to conversation", "error", err)
	}

	m.progressConversation(conversationID)

	return aiResponse, nil
}

// progressConversation moves the conversation to the next stage with improved logic
func (m *ConversationManager) progressConversation(conversationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return
	}

	// Count patient responses to determine progression
	patientResponses := 0
	hasAskedAboutName := false
	hasAskedAboutAge := false
	hasAskedAboutSymptoms := false
	hasAskedAboutConditions := false
	hasAskedAboutMedications := false
	hasAskedAboutAnythingElse := false

	for _, exchange := range conversation.Transcript {
		if exchange.Speaker == "patient" {
			patientResponses++
		}

		if exchange.Speaker == "ai" {
			text := strings.ToLower(exchange.Text)
			if strings.Contains(text, "name") {
				hasAskedAboutName = true
			} else if strings.Contains(text, "age") || strings.Contains(text, "gender") {
				hasAskedAboutAge = true
			} else if strings.Contains(text, "symptom") {
				hasAskedAboutSymptoms = true
			} else if strings.Contains(text, "condition") || strings.Contains(text, "history") {
				hasAskedAboutConditions = true
			} else if strings.Contains(text, "medication") || strings.Contains(text, "allergies") || strings.Contains(text, "taking") {
				hasAskedAboutMedications = true
			} else if strings.Contains(text, "anything else") || strings.Contains(text, "other") {
				hasAskedAboutAnythingElse = true
			}
		}
	}

	// Simple state machine based on what's been asked
	if !hasAskedAboutName || conversation.Status == StatusGreeting {
		if hasAskedAboutName && patientResponses >= 1 {
			conversation.Status = StatusIdentity
		} else {
			conversation.Status = StatusGreeting
		}
	} else if !hasAskedAboutAge || conversation.Status == StatusIdentity {
		if hasAskedAboutAge && patientResponses >= 2 {
			conversation.Status = StatusSymptoms
		} else {
			conversation.Status = StatusIdentity
		}
	} else if !hasAskedAboutSymptoms || conversation.Status == StatusSymptoms {
		if hasAskedAboutSymptoms && patientResponses >= 3 {
			conversation.Status = StatusHistory
		} else {
			conversation.Status = StatusSymptoms
		}
	} else if !hasAskedAboutConditions || conversation.Status == StatusHistory {
		if hasAskedAboutConditions && patientResponses >= 4 {
			conversation.Status = StatusMedication
		} else {
			conversation.Status = StatusHistory
		}
	} else if !hasAskedAboutMedications || conversation.Status == StatusMedication {
		if hasAskedAboutMedications && patientResponses >= 5 {
			conversation.Status = StatusQuestions
		} else {
			conversation.Status = StatusMedication
		}
	} else if !hasAskedAboutAnythingElse || conversation.Status == StatusQuestions {
		if hasAskedAboutAnythingElse && patientResponses >= 6 {
			conversation.Status = StatusClosing
		} else {
			conversation.Status = StatusQuestions
		}
	} else {
		conversation.Status = StatusClosing
	}

	logger.Info("Conversation progressed",
		"conversation_id", conversationID,
		"new_status", conversation.Status,
		"patient_responses", patientResponses)
}

// analyzePatientMessage analyzes a patient message to extract specific information
func (m *ConversationManager) analyzePatientMessage(conversationID string, messageIndex int) {
	m.mu.RLock()
	conversation, exists := m.activeConversations[conversationID]
	if !exists || messageIndex >= len(conversation.Transcript) {
		m.mu.RUnlock()
		logger.Error("Cannot analyze message - conversation not found or message index invalid",
			"conversation_id", conversationID,
			"message_index", messageIndex)
		return
	}

	message := conversation.Transcript[messageIndex]
	if message.Speaker != "patient" || message.IsProcessed {
		m.mu.RUnlock()
		return
	}

	status := conversation.Status
	m.mu.RUnlock()

	var analysisPrompt string

	switch status {
	case StatusGreeting:
		analysisPrompt = `Extract the patient's name from their response.
Output as JSON: {"patient_name": "Full Name"}`

	case StatusIdentity:
		analysisPrompt = `Extract the patient's age and gender from their response.
Output as JSON: {"age": 42, "gender": "male/female/other"}`

	case StatusSymptoms:
		analysisPrompt = `Extract all symptoms mentioned by the patient.
Output as JSON: {"symptoms": ["symptom 1", "symptom 2", ...]}`

	case StatusHistory:
		analysisPrompt = `Extract all medical conditions mentioned by the patient.
Output as JSON: {"conditions": ["condition 1", "condition 2", ...]}`

	case StatusMedication:
		analysisPrompt = `Extract all medications and allergies mentioned.
Output as JSON: {"medications": ["med 1", "med 2", ...], "allergies": ["allergy 1", "allergy 2", ...]}`

	case StatusQuestions:
		analysisPrompt = `Extract any additional health information mentioned.
Output as JSON: {"additional_info": "summary of additional information"}`

	default:
		analysisPrompt = `Extract any relevant health information.
Output as JSON: {"information": "summary of information"}`
	}

	fullPrompt := fmt.Sprintf(`
Extract ONLY the requested information from: "%s"
%s
MUST respond with valid JSON only.`, message.Text, analysisPrompt)

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:   256,
		Temperature: 0.1,
		ModelName:   "gpt-3.5-turbo",
	})

	if err != nil {
		logger.Error("Error analyzing patient message", "error", err)
		return
	}

	// Extract JSON from response
	jsonText := extractJSON(response.Text)

	var extractedData map[string]interface{}
	err = json.Unmarshal([]byte(jsonText), &extractedData)
	if err != nil {
		logger.Error("Error parsing JSON from analysis", "error", err, "json", jsonText)

		// Basic fallback for name extraction
		if status == StatusGreeting {
			m.mu.Lock()
			conversation, exists := m.activeConversations[conversationID]
			if exists && messageIndex < len(conversation.Transcript) {
				nameToUse := strings.TrimSpace(conversation.Transcript[messageIndex].Text)
				if len(nameToUse) > 30 {
					nameToUse = nameToUse[:30]
				}
				conversation.PatientName = nameToUse
				conversation.CollectedData["patient_name"] = nameToUse
				conversation.CollectedData["name"] = nameToUse
				logger.Info("Used fallback name extraction", "name", nameToUse)
			}
			m.mu.Unlock()
		}
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists = m.activeConversations[conversationID]
	if !exists || messageIndex >= len(conversation.Transcript) {
		return
	}

	conversation.Transcript[messageIndex].IsProcessed = true

	// Update collected data based on conversation state
	switch status {
	case StatusGreeting:
		if name, ok := extractedData["patient_name"].(string); ok && name != "" {
			conversation.PatientName = name
			conversation.CollectedData["patient_name"] = name
			conversation.CollectedData["name"] = name
			logger.Info("Extracted patient name", "name", name)
		}

	case StatusIdentity:
		if age, ok := extractedData["age"].(float64); ok {
			conversation.CollectedData["age"] = age
		}
		if gender, ok := extractedData["gender"].(string); ok {
			conversation.CollectedData["gender"] = gender
		}

	case StatusSymptoms:
		if symptoms, ok := extractedData["symptoms"].([]interface{}); ok {
			conversation.CollectedData["symptoms"] = symptoms
		}

	case StatusHistory:
		if conditions, ok := extractedData["conditions"].([]interface{}); ok {
			conversation.CollectedData["conditions"] = conditions
		}

	case StatusMedication:
		if medications, ok := extractedData["medications"].([]interface{}); ok {
			conversation.CollectedData["medications"] = medications
		}
		if allergies, ok := extractedData["allergies"].([]interface{}); ok {
			conversation.CollectedData["allergies"] = allergies
		}

	case StatusQuestions:
		if additionalInfo, ok := extractedData["additional_info"].(string); ok {
			conversation.CollectedData["additional_info"] = additionalInfo
		}
	}
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

// ProcessConversationData prepares the collected data for external systems
func (m *ConversationManager) ProcessConversationData(conversationID string) (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return nil, fmt.Errorf("conversation %s not found", conversationID)
	}

	// Create a patient data structure that exactly matches our target format
	patientData := map[string]any{
		"id":          "P" + conversationID[len(conversationID)-5:],
		"name":        conversation.PatientName,
		"phone":       conversation.PatientPhone,
		"symptoms":    []string{},
		"conditions":  []string{},
		"medications": []string{},
		"allergies":   []string{},
		"vitals": map[string]any{
			"blood_pressure":    "",
			"heart_rate":        0,
			"temperature":       0.0,
			"oxygen_saturation": 0,
		},
	}

	// Set default age/gender if not available
	if conversation.CollectedData["age"] == nil {
		patientData["age"] = 0
	} else {
		patientData["age"] = conversation.CollectedData["age"]
	}

	if conversation.CollectedData["gender"] == nil {
		patientData["gender"] = ""
	} else {
		patientData["gender"] = conversation.CollectedData["gender"]
	}

	// Process symptoms - convert from interface slice to string slice
	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok {
		symptomStrings := make([]string, 0, len(symptoms))
		for _, s := range symptoms {
			if symptom, ok := s.(string); ok {
				symptomStrings = append(symptomStrings, symptom)
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
			}
		}
		patientData["allergies"] = allergyStrings
	}

	// Attempt to extract vitals from the conversation
	// This could be expanded if we start to collect vitals specifically
	if bp, ok := conversation.CollectedData["blood_pressure"].(string); ok {
		vitals := patientData["vitals"].(map[string]any)
		vitals["blood_pressure"] = bp
	}

	if hr, ok := conversation.CollectedData["heart_rate"].(float64); ok {
		vitals := patientData["vitals"].(map[string]any)
		vitals["heart_rate"] = hr
	}

	if temp, ok := conversation.CollectedData["temperature"].(float64); ok {
		vitals := patientData["vitals"].(map[string]any)
		vitals["temperature"] = temp
	}

	if o2, ok := conversation.CollectedData["oxygen_saturation"].(float64); ok {
		vitals := patientData["vitals"].(map[string]any)
		vitals["oxygen_saturation"] = o2
	}

	return map[string]any{
		"patient_data": patientData,
		"conversation_data": map[string]any{
			"start_time": conversation.StartTime.Format(time.RFC3339),
			"status":     string(conversation.Status),
		},
	}, nil
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

func extractJSON(text string) string {
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		return text[jsonStart : jsonEnd+1]
	}

	// Fallback for missing brackets
	return fmt.Sprintf(`{"error": "No valid JSON found in '%s'"}`, text)
}
