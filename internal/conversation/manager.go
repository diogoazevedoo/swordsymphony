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

// GetNextResponse generates the next AI response with a strict flow
func (m *ConversationManager) GetNextResponse(conversationID string) (string, error) {
	m.mu.RLock()
	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		m.mu.RUnlock()
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}

	// Determine the current state and what question to ask next
	// This follows a strict order: greeting -> name -> age -> gender -> symptoms -> conditions -> medication -> allergies -> closing
	var nextQuestion string
	var currentStatus = conversation.Status

	switch currentStatus {
	case StatusGreeting:
		nextQuestion = "Hello, I'm the medical assistant from Sword Symphony. What is your name?"
		conversation.Status = StatusName
		conversation.QuestionsAsked["greeting"] = true
	case StatusName:
		if conversation.PatientName != "" {
			// Name already collected, move to age
			nextQuestion = fmt.Sprintf("Thank you, %s. What is your age?", conversation.PatientName)
			conversation.Status = StatusAge
			conversation.QuestionsAsked["name"] = true
		} else {
			nextQuestion = "What is your name?"
		}
	case StatusAge:
		_, hasAge := conversation.CollectedData["age"]
		if hasAge {
			// Age collected, move to gender
			nextQuestion = "What is your gender?"
			conversation.Status = StatusGender
			conversation.QuestionsAsked["age"] = true
		} else {
			nextQuestion = "What is your age?"
		}
	case StatusGender:
		_, hasGender := conversation.CollectedData["gender"]
		if hasGender {
			// Gender collected, move to symptoms
			nextQuestion = "What symptoms are you experiencing?"
			conversation.Status = StatusSymptoms
			conversation.QuestionsAsked["gender"] = true
		} else {
			nextQuestion = "What is your gender?"
		}
	case StatusSymptoms:
		symptoms, hasSymptoms := conversation.CollectedData["symptoms"].([]interface{})
		if hasSymptoms && len(symptoms) > 0 {
			// Symptoms collected, move to conditions
			nextQuestion = "Do you have any existing medical conditions?"
			conversation.Status = StatusConditions
			conversation.QuestionsAsked["symptoms"] = true
		} else {
			nextQuestion = "What symptoms are you experiencing?"
		}
	case StatusConditions:
		conditions, hasConditions := conversation.CollectedData["conditions"].([]interface{})
		if hasConditions && len(conditions) > 0 {
			// Conditions collected, move to medications
			nextQuestion = "What medications are you currently taking?"
			conversation.Status = StatusMedication
			conversation.QuestionsAsked["conditions"] = true
		} else {
			nextQuestion = "Do you have any existing medical conditions?"
		}
	case StatusMedication:
		medications, hasMedications := conversation.CollectedData["medications"].([]interface{})
		if hasMedications && len(medications) > 0 {
			// Medications collected, move to allergies
			nextQuestion = "Do you have any allergies?"
			conversation.Status = StatusAllergies
			conversation.QuestionsAsked["medications"] = true
		} else {
			nextQuestion = "What medications are you currently taking?"
		}
	case StatusAllergies:
		allergies, hasAllergies := conversation.CollectedData["allergies"].([]interface{})
		if hasAllergies && len(allergies) > 0 {
			// Allergies collected, move to closing
			nextQuestion = "Thank you for providing all the information. We have everything we need. Have a good day. Goodbye."
			conversation.Status = StatusClosing
			conversation.QuestionsAsked["allergies"] = true
		} else {
			nextQuestion = "Do you have any allergies?"
		}
	case StatusClosing:
		// Conversation completed, prepare for call ending
		nextQuestion = "Thank you for your time. Your information has been recorded. Goodbye."
		conversation.Status = StatusComplete
		conversation.QuestionsAsked["closing"] = true
	case StatusComplete:
		// Conversation already completed
		nextQuestion = "Goodbye."
	default:
		// If state is unknown, reset to greeting
		nextQuestion = "Hello, I'm the medical assistant from Sword Symphony. What is your name?"
		conversation.Status = StatusName
	}

	m.mu.RUnlock()

	// Create simplified response
	aiResponse := nextQuestion

	// Add the AI response to the conversation
	err := m.AddMessage(conversationID, "ai", aiResponse, 1.0)
	if err != nil {
		logger.Error("Error adding AI response to conversation", "error", err)
	}

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
	case StatusGreeting, StatusName:
		if name, ok := extractedData["patient_name"].(string); ok && name != "" {
			conversation.PatientName = name
			conversation.CollectedData["patient_name"] = name
			conversation.CollectedData["name"] = name
			logger.Info("Extracted patient name", "name", name)
		}

	case StatusAge:
		if age, ok := extractedData["age"].(float64); ok {
			conversation.CollectedData["age"] = age
			logger.Info("Extracted patient age", "age", age)
		}

	case StatusGender:
		if gender, ok := extractedData["gender"].(string); ok {
			conversation.CollectedData["gender"] = gender
			logger.Info("Extracted patient gender", "gender", gender)
		}

	case StatusSymptoms:
		if symptoms, ok := extractedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
			conversation.CollectedData["symptoms"] = symptoms
			logger.Info("Extracted patient symptoms", "count", len(symptoms))
		}

	case StatusConditions:
		if conditions, ok := extractedData["conditions"].([]interface{}); ok && len(conditions) > 0 {
			conversation.CollectedData["conditions"] = conditions
			logger.Info("Extracted patient conditions", "count", len(conditions))
		}

	case StatusMedication:
		if medications, ok := extractedData["medications"].([]interface{}); ok && len(medications) > 0 {
			conversation.CollectedData["medications"] = medications
			logger.Info("Extracted patient medications", "count", len(medications))
		}

	case StatusAllergies:
		if allergies, ok := extractedData["allergies"].([]interface{}); ok && len(allergies) > 0 {
			conversation.CollectedData["allergies"] = allergies
			logger.Info("Extracted patient allergies", "count", len(allergies))
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

	// Save original conversation data if needed but don't expose in main structure
	if convID, ok := conversation.CollectedData["conversation_id"].(string); ok {
		patientData["_conversation_id"] = convID
	}

	logger.Info("Standardized patient data",
		"original_keys", getMapKeys(conversation.CollectedData),
		"standardized_keys", getMapKeys(patientData))

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
