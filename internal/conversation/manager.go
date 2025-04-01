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
		systemPrompt: `You are a medical data collection assistant for Sword Symphony.
You should be professional and polite while keeping responses concise.
Your goal is to collect patient information needed for the medical record.
Ask exactly ONE question at a time and never repeat questions.
Do not provide medical advice, just collect information efficiently.`,

		greetingPrompt: `Introduce yourself as "Hello, I'm the medical assistant from Sword Symphony." Then ask for the patient's name.`,

		identityPrompt: `Politely ask: "Could you please tell me your age and gender?"`,

		symptomsPrompt: `Ask: "What symptoms are you currently experiencing?"`,

		historyPrompt: `Ask: "Do you have any pre-existing medical conditions?"`,

		medicationPrompt: `Ask: "What medications are you taking, and do you have any allergies?"`,

		questionsPrompt: `Say "Is there any other important health information I should know about?"`,

		closingPrompt: `Say "Thank you for providing this information. I've recorded everything needed for your visit."`,
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

	// Collect information about what data we have
	hasName := conversation.PatientName != ""
	hasAge := false
	hasGender := false
	hasSymptoms := false
	hasConditions := false
	hasMedications := false
	hasAllergies := false

	if age, ok := conversation.CollectedData["age"].(float64); ok && age > 0 {
		hasAge = true
	}

	if gender, ok := conversation.CollectedData["gender"].(string); ok && gender != "" {
		hasGender = true
	}

	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
		hasSymptoms = true
	}

	if conditions, ok := conversation.CollectedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
		hasConditions = true
	}

	if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok && len(medications) > 0 {
		hasMedications = true
	}

	if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
		hasAllergies = true
	}

	// Make a snapshot of the current state for the prompt
	currentStatus := conversation.Status
	m.mu.RUnlock()

	// Determine the appropriate prompt based on collected information
	var prompt string
	switch currentStatus {
	case StatusGreeting:
		prompt = m.greetingPrompt

	case StatusIdentity:
		if hasName {
			if !hasAge && !hasGender {
				prompt = fmt.Sprintf("Hi %s, I need to ask about your age and gender. Could you please share those with me?", conversation.PatientName)
			} else if !hasAge {
				prompt = fmt.Sprintf("Thanks %s. Could you please tell me your age?", conversation.PatientName)
			} else if !hasGender {
				prompt = fmt.Sprintf("Thanks %s. Could you please specify your gender?", conversation.PatientName)
			} else {
				// Force progress to symptoms if we have both age and gender
				m.mu.Lock()
				conversation.Status = StatusSymptoms
				m.mu.Unlock()
				prompt = fmt.Sprintf("Thanks %s. What symptoms are you experiencing today?", conversation.PatientName)
			}
		} else {
			prompt = "Could you please tell me your name?"
		}

	case StatusSymptoms:
		if !hasSymptoms {
			prompt = fmt.Sprintf("%s, what symptoms are you experiencing today?", conversation.PatientName)
		} else {
			// Force progress to history if we already have symptoms
			m.mu.Lock()
			conversation.Status = StatusHistory
			m.mu.Unlock()
			prompt = fmt.Sprintf("Thank you. Do you have any existing medical conditions or health issues?", conversation.PatientName)
		}

	case StatusHistory:
		if !hasConditions {
			prompt = "Do you have any existing medical conditions or health issues?"
		} else {
			// Force progress to medication if we already have conditions
			m.mu.Lock()
			conversation.Status = StatusMedication
			m.mu.Unlock()
			prompt = "What medications are you currently taking, and do you have any allergies?"
		}

	case StatusMedication:
		if !hasMedications && !hasAllergies {
			prompt = "Please tell me about any medications you're taking and any allergies you have."
		} else if !hasMedications {
			prompt = "Are you currently taking any medications?"
		} else if !hasAllergies {
			prompt = "Do you have any allergies I should know about?"
		} else {
			// Force progress to questions if we have both medications and allergies
			m.mu.Lock()
			conversation.Status = StatusQuestions
			m.mu.Unlock()
			prompt = "Is there anything else important about your health that I should know?"
		}

	case StatusQuestions:
		prompt = "Is there anything else important about your health that I should know?"

	case StatusClosing:
		prompt = "Thank you for sharing your health information. This will help us provide better care for you."

	default:
		prompt = "Is there anything else you'd like to tell me about your health today?"
	}

	var conversationContext strings.Builder
	conversationContext.WriteString("Here is the conversation so far:\n\n")

	// Build context with last few exchanges
	recentMessages := getRecentMessages(m, conversationID, 4)
	for _, exchange := range recentMessages {
		speakerName := exchange.Speaker
		if speakerName == "patient" {
			speakerName = "Patient"
		} else {
			speakerName = "Doctor"
		}
		conversationContext.WriteString(fmt.Sprintf("%s: %s\n", speakerName, exchange.Text))
	}

	// Create a summary of what we know about the patient
	patientSummary := buildPatientSummary(m, conversationID)

	fullPrompt := fmt.Sprintf(`
%s

Current conversation stage: %s

PATIENT SUMMARY:
%s

YOUR TASK:
%s

Conversation context:
%s

Based on the conversation so far and the current stage, provide your next response as the AI medical assistant.
Keep your response brief and focused on getting the specific information needed.
Ask exactly one question.
Acknowledge any information the patient has provided.
DO NOT repeat questions that have already been answered.
`, m.systemPrompt, currentStatus, patientSummary, prompt, conversationContext.String())

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

	// Clean up any prefixes
	aiResponse = strings.ReplaceAll(aiResponse, "Doctor:", "")
	aiResponse = strings.ReplaceAll(aiResponse, "AI Doctor:", "")
	aiResponse = strings.ReplaceAll(aiResponse, "Medical Assistant:", "")
	aiResponse = strings.TrimSpace(aiResponse)

	err = m.AddMessage(conversationID, "ai", aiResponse, 1.0)
	if err != nil {
		logger.Error("Error adding AI response to conversation", "error", err)
	}

	m.progressConversation(conversationID)

	return aiResponse, nil
}

// Helper functions for GetNextResponse
func getRecentMessages(m *ConversationManager, conversationID string, count int) []MessageExchange {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return []MessageExchange{}
	}

	if len(conversation.Transcript) <= count {
		return conversation.Transcript
	}

	return conversation.Transcript[len(conversation.Transcript)-count:]
}

func buildPatientSummary(m *ConversationManager, conversationID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "No patient information available"
	}

	var summary strings.Builder

	summary.WriteString(fmt.Sprintf("Name: %s\n", conversation.PatientName))

	if age, ok := conversation.CollectedData["age"].(float64); ok && age > 0 {
		summary.WriteString(fmt.Sprintf("Age: %.0f\n", age))
	} else {
		summary.WriteString("Age: Unknown\n")
	}

	if gender, ok := conversation.CollectedData["gender"].(string); ok && gender != "" {
		summary.WriteString(fmt.Sprintf("Gender: %s\n", gender))
	} else {
		summary.WriteString("Gender: Unknown\n")
	}

	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
		summary.WriteString("Symptoms: ")
		for i, s := range symptoms {
			if i > 0 {
				summary.WriteString(", ")
			}
			summary.WriteString(fmt.Sprintf("%v", s))
		}
		summary.WriteString("\n")
	} else {
		summary.WriteString("Symptoms: None reported\n")
	}

	if conditions, ok := conversation.CollectedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
		summary.WriteString("Medical Conditions: ")
		for i, c := range conditions {
			if i > 0 {
				summary.WriteString(", ")
			}
			summary.WriteString(fmt.Sprintf("%v", c))
		}
		summary.WriteString("\n")
	} else {
		summary.WriteString("Medical Conditions: None reported\n")
	}

	if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok && len(medications) > 0 {
		summary.WriteString("Medications: ")
		for i, m := range medications {
			if i > 0 {
				summary.WriteString(", ")
			}
			summary.WriteString(fmt.Sprintf("%v", m))
		}
		summary.WriteString("\n")
	} else {
		summary.WriteString("Medications: None reported\n")
	}

	if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
		summary.WriteString("Allergies: ")
		for i, a := range allergies {
			if i > 0 {
				summary.WriteString(", ")
			}
			summary.WriteString(fmt.Sprintf("%v", a))
		}
		summary.WriteString("\n")
	} else {
		summary.WriteString("Allergies: None reported\n")
	}

	return summary.String()
}

// progressConversation moves the conversation to the next stage with improved logic
func (m *ConversationManager) progressConversation(conversationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return
	}

	// Check the content of patient's responses to determine what information we have
	hasProvidedName := false
	hasProvidedAge := false
	hasProvidedGender := false
	hasProvidedSymptoms := false
	hasProvidedMedHistory := false
	hasProvidedMedications := false
	hasProvidedAllergies := false
	hasProvidedAdditionalInfo := false

	// Check collected data for completeness
	if name, ok := conversation.CollectedData["name"].(string); ok && name != "" {
		hasProvidedName = true
	}

	if age, ok := conversation.CollectedData["age"].(float64); ok && age > 0 {
		hasProvidedAge = true
	}

	if gender, ok := conversation.CollectedData["gender"].(string); ok && gender != "" {
		hasProvidedGender = true
	}

	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
		hasProvidedSymptoms = true
	}

	if conditions, ok := conversation.CollectedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
		hasProvidedMedHistory = true
	}

	if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok && len(medications) > 0 {
		hasProvidedMedications = true
	}

	if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
		hasProvidedAllergies = true
	}

	if _, ok := conversation.CollectedData["additional_info"]; ok {
		hasProvidedAdditionalInfo = true
	}

	// Check for diabetes and penicillin mentions in recent messages
	for i := len(conversation.Transcript) - 1; i >= 0 && i >= len(conversation.Transcript)-3; i-- {
		if conversation.Transcript[i].Speaker == "patient" {
			text := strings.ToLower(conversation.Transcript[i].Text)
			if strings.Contains(text, "diabet") {
				// Add diabetes to conditions if not already there
				conditions, _ := conversation.CollectedData["conditions"].([]interface{})
				if conditions == nil {
					conditions = []interface{}{"diabetes"}
					conversation.CollectedData["conditions"] = conditions
					hasProvidedMedHistory = true
				} else {
					hasCondition := false
					for _, c := range conditions {
						if cStr, ok := c.(string); ok && strings.Contains(strings.ToLower(cStr), "diabet") {
							hasCondition = true
							break
						}
					}
					if !hasCondition {
						conversation.CollectedData["conditions"] = append(conditions, "diabetes")
						hasProvidedMedHistory = true
					}
				}
			}

			if strings.Contains(text, "metformin") {
				// Add metformin to medications if not already there
				medications, _ := conversation.CollectedData["medications"].([]interface{})
				if medications == nil {
					medications = []interface{}{"metformin"}
					conversation.CollectedData["medications"] = medications
					hasProvidedMedications = true
				} else {
					hasMed := false
					for _, m := range medications {
						if mStr, ok := m.(string); ok && strings.Contains(strings.ToLower(mStr), "metformin") {
							hasMed = true
							break
						}
					}
					if !hasMed {
						conversation.CollectedData["medications"] = append(medications, "metformin")
						hasProvidedMedications = true
					}
				}
			}

			if strings.Contains(text, "penicillin") && strings.Contains(text, "allerg") {
				// Add penicillin to allergies if not already there
				allergies, _ := conversation.CollectedData["allergies"].([]interface{})
				if allergies == nil {
					allergies = []interface{}{"penicillin"}
					conversation.CollectedData["allergies"] = allergies
					hasProvidedAllergies = true
				} else {
					hasAllergy := false
					for _, a := range allergies {
						if aStr, ok := a.(string); ok && strings.Contains(strings.ToLower(aStr), "penicillin") {
							hasAllergy = true
							break
						}
					}
					if !hasAllergy {
						conversation.CollectedData["allergies"] = append(allergies, "penicillin")
						hasProvidedAllergies = true
					}
				}
			}
		}
	}

	// Determine next conversation status based on what we need
	if !hasProvidedName {
		conversation.Status = StatusGreeting
		return
	}

	if !hasProvidedAge || !hasProvidedGender {
		conversation.Status = StatusIdentity
		return
	}

	if !hasProvidedSymptoms {
		conversation.Status = StatusSymptoms
		return
	}

	if !hasProvidedMedHistory {
		conversation.Status = StatusHistory
		return
	}

	if !hasProvidedMedications || !hasProvidedAllergies {
		conversation.Status = StatusMedication
		return
	}

	if !hasProvidedAdditionalInfo {
		conversation.Status = StatusQuestions
		return
	}

	conversation.Status = StatusClosing

	logger.Info("Conversation progressed",
		"conversation_id", conversationID,
		"new_status", conversation.Status,
		"data_collected", map[string]bool{
			"name":            hasProvidedName,
			"age":             hasProvidedAge,
			"gender":          hasProvidedGender,
			"symptoms":        hasProvidedSymptoms,
			"conditions":      hasProvidedMedHistory,
			"medications":     hasProvidedMedications,
			"allergies":       hasProvidedAllergies,
			"additional_info": hasProvidedAdditionalInfo,
		})
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
If you can't find a name, just return an empty string or your best guess.
Output as JSON: {"patient_name": "Full Name"}`

	case StatusIdentity:
		analysisPrompt = `Extract the patient's age and gender from their response.
If you can't find both, return what you can find.
Output as JSON: {"age": 42, "gender": "male/female/other"}`

	case StatusSymptoms:
		analysisPrompt = `Extract all symptoms mentioned by the patient.
Be thorough and include ALL symptoms even if mentioned briefly.
Output as JSON: {"symptoms": ["symptom 1", "symptom 2", ...]}`

	case StatusHistory:
		analysisPrompt = `Extract all medical conditions mentioned by the patient.
Include ANY health conditions mentioned such as diabetes, hypertension, etc.
Output as JSON: {"conditions": ["condition 1", "condition 2", ...]}`

	case StatusMedication:
		analysisPrompt = `Extract all medications and allergies mentioned by the patient.
Include ANY medications or allergies mentioned even in passing.
Output as JSON: {"medications": ["med 1", "med 2", ...], "allergies": ["allergy 1", "allergy 2", ...]}`

	case StatusQuestions:
		analysisPrompt = `Extract any additional health information mentioned by the patient.
Be comprehensive and include any health-related details.
Output as JSON: {"additional_info": "summary of additional information"}`

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
If certain information is missing, include empty values rather than omitting fields.`, message.Text, analysisPrompt)

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:   256,
		Temperature: 0.1,
		ModelName:   "gpt-3.5-turbo",
	})

	if err != nil {
		logger.Error("Error analyzing patient message", "error", err)
		// Implement fallback extraction regardless of error
		fallbackExtraction(m, conversationID, messageIndex, message.Text)
		return
	}

	// Extract JSON from response
	jsonText := extractJSON(response.Text)

	var extractedData map[string]interface{}
	err = json.Unmarshal([]byte(jsonText), &extractedData)
	if err != nil {
		logger.Error("Error parsing JSON from analysis", "error", err, "json", jsonText)
		// Implement fallback extraction
		fallbackExtraction(m, conversationID, messageIndex, message.Text)
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
			logger.Info("Extracted patient age", "age", age)
		}
		if gender, ok := extractedData["gender"].(string); ok {
			conversation.CollectedData["gender"] = gender
			logger.Info("Extracted patient gender", "gender", gender)
		}

	case StatusSymptoms:
		if symptoms, ok := extractedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
			conversation.CollectedData["symptoms"] = symptoms
			logger.Info("Extracted patient symptoms", "count", len(symptoms))
		}

	case StatusHistory:
		if conditions, ok := extractedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
			conversation.CollectedData["conditions"] = conditions
			logger.Info("Extracted patient conditions", "count", len(conditions))
		}

	case StatusMedication:
		if medications, ok := extractedData["medications"].([]interface{}); ok && len(medications) > 0 {
			conversation.CollectedData["medications"] = medications
			logger.Info("Extracted patient medications", "count", len(medications))
		}
		if allergies, ok := extractedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
			conversation.CollectedData["allergies"] = allergies
			logger.Info("Extracted patient allergies", "count", len(allergies))
		}

	case StatusQuestions:
		if additionalInfo, ok := extractedData["additional_info"].(string); ok && additionalInfo != "" {
			conversation.CollectedData["additional_info"] = additionalInfo
			logger.Info("Extracted additional info", "info_length", len(additionalInfo))
		}
	}

	// Always check for specific health data regardless of conversation state
	scanForHealthData(extractedData, conversation.CollectedData)
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

// ProcessConversationData prepares the collected data for external systems
func (m *ConversationManager) ProcessConversationData(conversationID string) (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return nil, fmt.Errorf("conversation %s not found", conversationID)
	}

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

	// Set default age/gender if not available
	if ageValue, ok := conversation.CollectedData["age"]; ok {
		if age, ok := ageValue.(float64); ok {
			patientData["age"] = age
		} else if age, ok := ageValue.(int); ok {
			patientData["age"] = float64(age)
		} else {
			patientData["age"] = 0.0
		}
	} else {
		patientData["age"] = 0.0
	}

	if genderValue, ok := conversation.CollectedData["gender"]; ok {
		if gender, ok := genderValue.(string); ok {
			patientData["gender"] = gender
		} else {
			patientData["gender"] = ""
		}
	} else {
		patientData["gender"] = ""
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

	// Process vitals
	if vitalsValue, ok := conversation.CollectedData["vitals"]; ok {
		if srcVitals, ok := vitalsValue.(map[string]interface{}); ok {
			vitals := patientData["vitals"].(map[string]any)

			// Copy values with appropriate type conversion
			if bp, ok := srcVitals["blood_pressure"].(string); ok && bp != "" {
				vitals["blood_pressure"] = bp
			}

			if hr, ok := srcVitals["heart_rate"].(float64); ok {
				vitals["heart_rate"] = hr
			} else if hr, ok := srcVitals["heart_rate"].(int); ok {
				vitals["heart_rate"] = float64(hr)
			}

			if temp, ok := srcVitals["temperature"].(float64); ok {
				vitals["temperature"] = temp
			} else if temp, ok := srcVitals["temperature"].(int); ok {
				vitals["temperature"] = float64(temp)
			}

			if o2, ok := srcVitals["oxygen_saturation"].(float64); ok {
				vitals["oxygen_saturation"] = o2
			} else if o2, ok := srcVitals["oxygen_saturation"].(int); ok {
				vitals["oxygen_saturation"] = float64(o2)
			}
		}
	}

	return map[string]any{
		"patient_data": patientData,
		"conversation_data": map[string]any{
			"start_time": conversation.StartTime.Format(time.RFC3339),
			"status":     string(conversation.Status),
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

func extractJSON(text string) string {
	// First look for explicit JSON between curly braces
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		return text[jsonStart : jsonEnd+1]
	}

	// If we couldn't find JSON, create a structured response based on the content
	textLower := strings.ToLower(text)

	result := map[string]interface{}{
		"error": "No valid JSON found in response",
	}

	// Try to extract common health information using simple pattern matching
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
