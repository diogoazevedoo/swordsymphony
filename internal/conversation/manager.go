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

	// Check if this exact message already exists in recent history (prevents duplicates)
	for i := len(conversation.Transcript) - 1; i >= 0 && i >= len(conversation.Transcript)-3; i-- {
		if conversation.Transcript[i].Speaker == speaker &&
			conversation.Transcript[i].Text == text {
			// Message already exists, don't add it again
			logger.Info("Skipping duplicate message",
				"conversation_id", conversationID,
				"speaker", speaker)
			return nil
		}
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

	// Process patient messages outside the lock
	if speaker == "patient" && !exchange.IsProcessed {
		messageIndex := len(conversation.Transcript) - 1
		go m.analyzePatientMessage(conversationID, messageIndex)
	}

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

	// For the very first message, ensure we say "Hello" properly
	if len(conversation.Transcript) <= 1 {
		m.mu.RUnlock()
		initialGreeting := "Hello, I'm the medical assistant from Sword Symphony. What is your name?"
		err := m.AddMessage(conversationID, "ai", initialGreeting, 1.0)
		if err != nil {
			logger.Error("Error adding initial greeting to conversation", "error", err)
		}
		m.progressConversation(conversationID)
		return initialGreeting, nil
	}

	// Direct prompts based on what data we're missing
	var directPrompt string

	// Check what data we have
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

	m.mu.RUnlock()

	// Very direct prompts based on missing data
	if !hasName {
		directPrompt = "Hello, I'm the medical assistant from Sword Symphony. What is your name?"
	} else if !hasAge {
		directPrompt = fmt.Sprintf("Thanks %s. What is your age?", conversation.PatientName)
	} else if !hasGender {
		directPrompt = "What is your gender?"
	} else if !hasSymptoms {
		directPrompt = "What symptoms are you experiencing?"
	} else if !hasConditions {
		directPrompt = "Do you have any existing medical conditions?"
	} else if !hasMedications {
		directPrompt = "What medications are you currently taking?"
	} else if !hasAllergies {
		directPrompt = "Do you have any allergies?"
	} else {
		directPrompt = "Thank you for providing all the information we need. Is there anything else you'd like to tell me about your health?"
	}

	// Create minimal system prompt
	systemPrompt := `You are a medical data collection assistant. Be professional but extremely concise. 
Your only goal is to collect specific information efficiently without small talk.
Ask exactly ONE clear question per response and acknowledge information provided.
Do not repeat questions that have been answered.`

	// Create a very minimal prompt
	var fullPrompt string

	recentMessages := getRecentMessages(m, conversationID, 3) // Reduced from 4 to 3

	fullPrompt = fmt.Sprintf(`
%s

YOUR NEXT QUESTION: %s

Recent conversation:
%s

Keep your response under 75 words. Be direct and focused on getting the specific information needed.
`, systemPrompt, directPrompt, formatRecentMessages(recentMessages))

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:    100, // Reduced from 150
		Temperature:  0.3, // Reduced from 0.7 for more consistency
		SystemPrompt: systemPrompt,
	})

	if err != nil {
		logger.Error("Error generating AI response", "error", err)
		return "", fmt.Errorf("error generating response: %w", err)
	}

	aiResponse := response.Text

	// Clean up response
	aiResponse = strings.ReplaceAll(aiResponse, "Doctor:", "")
	aiResponse = strings.ReplaceAll(aiResponse, "AI Doctor:", "")
	aiResponse = strings.ReplaceAll(aiResponse, "Medical Assistant:", "")
	aiResponse = strings.TrimSpace(aiResponse)

	// Add only once
	err = m.AddMessage(conversationID, "ai", aiResponse, 1.0)
	if err != nil {
		logger.Error("Error adding AI response to conversation", "error", err)
	}

	// Progress conversation state
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

// progressConversation moves the conversation to the next stage with improved logic
func (m *ConversationManager) progressConversation(conversationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return
	}

	// More decisive state changes
	if conversation.PatientName == "" {
		conversation.Status = StatusGreeting
		return
	}

	if age, ok := conversation.CollectedData["age"].(float64); !ok || age <= 0 {
		conversation.Status = StatusIdentity
		return
	}

	if gender, ok := conversation.CollectedData["gender"].(string); !ok || gender == "" {
		conversation.Status = StatusIdentity
		return
	}

	symptoms, hasSymptoms := conversation.CollectedData["symptoms"].([]interface{})
	if !hasSymptoms || len(symptoms) == 0 {
		conversation.Status = StatusSymptoms
		return
	}

	conditions, hasConditions := conversation.CollectedData["conditions"].([]interface{})
	if !hasConditions || len(conditions) == 0 {
		conversation.Status = StatusHistory
		return
	}

	medications, hasMeds := conversation.CollectedData["medications"].([]interface{})
	if !hasMeds || len(medications) == 0 {
		conversation.Status = StatusMedication
		return
	}

	allergies, hasAllergies := conversation.CollectedData["allergies"].([]interface{})
	if !hasAllergies || len(allergies) == 0 {
		conversation.Status = StatusMedication
		return
	}

	// Mark completion more clearly
	if _, hasInfo := conversation.CollectedData["additional_info"]; !hasInfo {
		conversation.Status = StatusQuestions
		return
	}

	conversation.Status = StatusClosing

	logger.Info("Conversation progressed",
		"conversation_id", conversationID,
		"new_status", conversation.Status,
		"data_collected", true)
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
