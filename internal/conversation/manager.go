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

// NewConversationManager creates a new conversation manager
func NewConversationManager(aiClient ai.Client, options ...ManagerOption) *ConversationManager {
	manager := &ConversationManager{
		aiClient:            aiClient,
		activeConversations: make(map[string]*Conversation),
		systemPrompt: `You are an AI medical doctor conducting a phone interview with a patient. 
Be efficient and direct while remaining friendly. Your goal is to quickly gather essential medical information in a conversational way.
Keep responses under 3 sentences when possible. Ask only one question at a time. Avoid repeating questions.
Collect: name, email, chief complaint, symptom details, medical history, current medications, and allergies.`,
		greetingPrompt:   `Briefly introduce yourself as Dr. AI from SwordSymphony Medical. Say you'll ask a few questions about their health. This call is recorded to provide better care. Simply ask for their name. Keep it brief but friendly.`,
		identityPrompt:   `Thank {{.PatientName}} and ask for their email address only so you can send a summary of your conversation. Confirm what you heard.`,
		symptomsPrompt:   `Ask what health concern brings them to this call today. Don't list possible symptoms, just ask one clear question about their primary concern.`,
		historyPrompt:    `Ask if they have any ongoing health conditions or significant past illnesses that might be relevant to their current symptoms. Keep it to one direct question.`,
		medicationPrompt: `Ask about current medications and allergies in one simple question. Ask for medication names and any known allergies.`,
		questionsPrompt:  `Ask if they have any questions or if there's anything else about their health they'd like to share.`,
		closingPrompt:    `Thank them for sharing their information. Let them know you'll send this to the medical team who will analyze it and email them a detailed summary. Ask if there's anything else they need before ending the call.`,
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

// GetNextResponse generates the next AI response
func (m *ConversationManager) GetNextResponse(conversationID string) (string, error) {
	m.mu.RLock()
	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		m.mu.RUnlock()
		return "", fmt.Errorf("conversation %s not found", conversationID)
	}
	m.mu.RUnlock()

	var prompt string
	switch conversation.Status {
	case StatusGreeting:
		prompt = m.greetingPrompt
	case StatusIdentity:
		prompt = strings.Replace(m.identityPrompt, "{{.PatientName}}", conversation.PatientName, -1)
	case StatusSymptoms:
		prompt = m.symptomsPrompt
	case StatusHistory:
		prompt = m.historyPrompt
	case StatusMedication:
		prompt = m.medicationPrompt
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

Based on the conversation so far and the current stage, provide your next response as the AI doctor:
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

// analyzePatientMessage processes a patient message to extract information
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

	var contextBuilder strings.Builder
	for i := 0; i < messageIndex; i++ {
		prev := conversation.Transcript[i]
		contextBuilder.WriteString(fmt.Sprintf("%s: %s\n", prev.Speaker, prev.Text))
	}

	logger.Info("Analyzing patient message",
		"conversation_id", conversationID,
		"message_text", message.Text,
		"status", status)
	m.mu.RUnlock()

	var analysisPrompt string

	switch status {
	case StatusGreeting:
		analysisPrompt = `The patient is providing their name. Extract their name.
Output JSON with the format: {"patient_name": "Name of Patient"}`

	case StatusIdentity:
		analysisPrompt = `The patient is providing their email address. Extract the email.
Output JSON with the format: {"patient_email": "email@example.com"}`

	case StatusSymptoms:
		analysisPrompt = `Extract all symptoms mentioned. Include duration, severity or any details provided.
Output JSON with the format: {
  "symptoms": ["symptom1 with details", "symptom2 with details"],
  "duration": "duration information",
  "severity": "severity information if mentioned"
}`

	case StatusHistory:
		analysisPrompt = `Extract all medical conditions and previous surgeries mentioned.
Output JSON with the format: {
  "medical_conditions": ["condition1", "condition2"],
  "surgeries": ["surgery1", "surgery2"]
}`

	case StatusMedication:
		analysisPrompt = `Extract all medications and allergies mentioned.
Output JSON with the format: {
  "medications": ["medication1 and dosage", "medication2 and dosage"],
  "allergies": ["allergy1", "allergy2"]
}`

	case StatusQuestions:
		analysisPrompt = `Extract any questions or additional information provided.
Output JSON with the format: {
  "additional_info": "any additional health information"
}`

	default:
		analysisPrompt = `Extract any relevant medical information from the patient's message.
Output JSON with the format: {"information": "extracted information"}`
	}

	fullPrompt := fmt.Sprintf(`
Extract only the specific pieces of information from the patient's response.
Be factual and precise. Only extract information that is clearly stated by the patient.

Patient's message: "%s"

%s
`, message.Text, analysisPrompt)

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:   512,
		Temperature: 0.1,
		ModelName:   "gpt-3.5-turbo",
	})

	if err != nil {
		logger.Error("Error analyzing patient message", "error", err)
		return
	}

	// Extract JSON from response - look for everything between { and }
	jsonStart := strings.Index(response.Text, "{")
	jsonEnd := strings.LastIndex(response.Text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonText := response.Text[jsonStart : jsonEnd+1]
		logger.Info("Extracted JSON from analysis", "json", jsonText)

		var extractedData map[string]interface{}
		err = json.Unmarshal([]byte(jsonText), &extractedData)
		if err != nil {
			logger.Error("Error parsing JSON from analysis", "error", err, "json", jsonText)

			// Fallback for name extraction
			if status == StatusGreeting {
				m.mu.Lock()
				conversation, exists := m.activeConversations[conversationID]
				if exists && messageIndex < len(conversation.Transcript) {
					patientMessage := conversation.Transcript[messageIndex].Text
					// Use a cleaner name extraction
					words := strings.Fields(patientMessage)
					if len(words) > 0 {
						nameToUse := words[0]
						if len(words) > 1 {
							nameToUse = strings.Join(words[:2], " ")
						}
						if len(nameToUse) > 30 {
							nameToUse = nameToUse[:30]
						}
						conversation.PatientName = nameToUse
						conversation.CollectedData["patient_name"] = nameToUse
						logger.Info("Used fallback name extraction",
							"name", nameToUse,
							"conversation_id", conversationID)
					}
				}
				m.mu.Unlock()
			}
			return
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		conversation, exists = m.activeConversations[conversationID]
		if !exists {
			return
		}

		if messageIndex >= len(conversation.Transcript) {
			return
		}

		conversation.Transcript[messageIndex].IsProcessed = true

		if status == StatusGreeting && extractedData["patient_name"] != nil {
			patientName, ok := extractedData["patient_name"].(string)
			if ok && patientName != "" {
				conversation.PatientName = patientName
				conversation.CollectedData["patient_name"] = patientName
				logger.Info("Extracted patient name",
					"name", patientName,
					"conversation_id", conversationID)
			} else {
				// Fallback: use the message text as the name
				patientMessage := conversation.Transcript[messageIndex].Text
				if patientMessage != "" {
					nameWords := strings.Fields(patientMessage)
					nameToUse := patientMessage
					if len(nameWords) > 0 && len(nameWords) <= 3 {
						nameToUse = strings.Join(nameWords, " ")
					}
					conversation.PatientName = nameToUse
					conversation.CollectedData["patient_name"] = nameToUse
					logger.Info("Using message as patient name",
						"name", nameToUse,
						"conversation_id", conversationID)
				}
			}
		} else if status == StatusIdentity && extractedData["patient_email"] != nil {
			patientEmail, ok := extractedData["patient_email"].(string)
			if ok && patientEmail != "" {
				conversation.PatientEmail = patientEmail
				conversation.CollectedData["patient_email"] = patientEmail
				logger.Info("Extracted patient email",
					"email", patientEmail,
					"conversation_id", conversationID)
			}
		} else {
			// For other data, store it directly in CollectedData
			for key, value := range extractedData {
				if value != nil {
					conversation.CollectedData[key] = value
					logger.Info("Stored extracted data",
						"key", key,
						"conversation_id", conversationID)
				}
			}
		}

		logger.Info("Analyzed patient message",
			"conversation_id", conversationID,
			"extracted_keys", getMapKeys(extractedData),
			"patient_name", conversation.PatientName,
			"conversation_status", conversation.Status)
	} else {
		logger.Error("No JSON found in analysis response",
			"response", response.Text,
			"conversation_id", conversationID)

		// Fallback for name extraction
		if status == StatusGreeting {
			m.mu.Lock()
			conversation, exists := m.activeConversations[conversationID]
			if exists && messageIndex < len(conversation.Transcript) {
				patientMessage := conversation.Transcript[messageIndex].Text
				if patientMessage != "" {
					nameWords := strings.Fields(patientMessage)
					nameToUse := patientMessage
					if len(nameWords) > 0 && len(nameWords) <= 3 {
						nameToUse = strings.Join(nameWords, " ")
					}
					conversation.PatientName = nameToUse
					conversation.CollectedData["patient_name"] = nameToUse
					logger.Info("Used fallback name extraction after JSON parsing failure",
						"name", nameToUse,
						"conversation_id", conversationID)
				}
			}
			m.mu.Unlock()
		}
	}
}

// progressConversation moves the conversation to the next stage if appropriate
func (m *ConversationManager) progressConversation(conversationID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return
	}

	logger.Info("Checking conversation progression",
		"conversation_id", conversationID,
		"current_status", conversation.Status,
		"has_name", conversation.PatientName != "",
		"has_email", conversation.PatientEmail != "")

	switch conversation.Status {
	case StatusGreeting:
		if conversation.PatientName != "" {
			conversation.Status = StatusIdentity
			logger.Info("Conversation progressed to identity stage",
				"conversation_id", conversationID,
				"patient_name", conversation.PatientName)
		} else {
			// Force progression after just 2 exchanges
			greetingCount := 0
			for _, exchange := range conversation.Transcript {
				if exchange.Speaker == "ai" {
					greetingCount++
				}
			}
			if greetingCount >= 2 {
				// Set a placeholder name if needed
				if conversation.PatientName == "" {
					conversation.PatientName = "Patient"
					conversation.CollectedData["patient_name"] = "Patient"
				}
				conversation.Status = StatusIdentity
				logger.Info("Forced progression to identity stage",
					"conversation_id", conversationID,
					"greeting_count", greetingCount)
			}
		}

	case StatusIdentity:
		if conversation.PatientEmail != "" {
			conversation.Status = StatusSymptoms
			logger.Info("Conversation progressed to symptoms stage",
				"conversation_id", conversationID,
				"patient_email", conversation.PatientEmail)
		} else {
			// Force progression after just 2 exchanges
			identityCount := 0
			for i := len(conversation.Transcript) - 1; i >= 0; i-- {
				if conversation.Transcript[i].Speaker == "ai" &&
					conversation.Status == StatusIdentity {
					identityCount++
				}
				if identityCount >= 2 {
					// Set a placeholder email if needed
					if conversation.PatientEmail == "" {
						conversation.PatientEmail = "unknown@example.com"
						conversation.CollectedData["patient_email"] = "unknown@example.com"
					}
					conversation.Status = StatusSymptoms
					logger.Info("Forced progression to symptoms stage",
						"conversation_id", conversationID,
						"identity_count", identityCount)
					break
				}
			}
		}

	case StatusSymptoms:
		// Progress after just 2 exchanges or if we have symptoms
		symptomsCount := 0
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusSymptoms {
				symptomsCount++
			}
			if symptomsCount >= 2 {
				conversation.Status = StatusHistory
				logger.Info("Conversation progressed to history stage",
					"conversation_id", conversationID)
				break
			}
		}

	case StatusHistory:
		// Progress after just 2 exchanges
		historyCount := 0
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusHistory {
				historyCount++
			}
			if historyCount >= 2 {
				conversation.Status = StatusMedication
				logger.Info("Conversation progressed to medication stage",
					"conversation_id", conversationID)
				break
			}
		}

	case StatusMedication:
		// Progress after just 2 exchanges
		medicationCount := 0
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusMedication {
				medicationCount++
			}
			if medicationCount >= 2 {
				conversation.Status = StatusQuestions
				logger.Info("Conversation progressed to questions stage",
					"conversation_id", conversationID)
				break
			}
		}

	case StatusQuestions:
		// Progress after 1-2 exchanges
		questionCount := 0
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusQuestions {
				questionCount++
			}
			if questionCount >= 1 {
				conversation.Status = StatusClosing
				logger.Info("Conversation progressed to closing stage",
					"conversation_id", conversationID)
				break
			}
		}

	case StatusClosing:
		// Complete after one closing message and one patient reply
		closingDelivered := false
		patientReplied := false

		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if !closingDelivered && conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusClosing {
				closingDelivered = true
			} else if closingDelivered && conversation.Transcript[i].Speaker == "patient" {
				patientReplied = true
				break
			}
		}

		if closingDelivered && patientReplied {
			logger.Info("Conversation ready for completion",
				"conversation_id", conversationID)
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

	processedData := map[string]any{
		"patient_data": map[string]any{
			"id":          conversationID,
			"name":        conversation.PatientName,
			"email":       conversation.PatientEmail,
			"phone":       conversation.PatientPhone,
			"symptoms":    []string{},
			"conditions":  []string{},
			"medications": []string{},
			"allergies":   []string{},
		},
		"conversation_data": map[string]any{
			"start_time": conversation.StartTime,
			"end_time":   conversation.EndTime,
			"duration":   conversation.Duration,
			"status":     conversation.Status,
		},
	}

	patientData := processedData["patient_data"].(map[string]any)

	if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok {
		symptomsList := make([]string, 0, len(symptoms))
		for _, s := range symptoms {
			if symptom, ok := s.(string); ok {
				symptomsList = append(symptomsList, symptom)
			}
		}
		patientData["symptoms"] = symptomsList
	}

	if conditions, ok := conversation.CollectedData["medical_conditions"].([]interface{}); ok {
		conditionsList := make([]string, 0, len(conditions))
		for _, c := range conditions {
			if condition, ok := c.(string); ok {
				conditionsList = append(conditionsList, condition)
			}
		}
		patientData["conditions"] = conditionsList
	}

	if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok {
		medicationsList := make([]string, 0, len(medications))
		for _, m := range medications {
			if medication, ok := m.(string); ok {
				medicationsList = append(medicationsList, medication)
			}
		}
		patientData["medications"] = medicationsList
	}

	if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok {
		allergiesList := make([]string, 0, len(allergies))
		for _, a := range allergies {
			if allergy, ok := a.(string); ok {
				allergiesList = append(allergiesList, allergy)
			}
		}
		patientData["allergies"] = allergiesList
	}

	patientData["duration"] = conversation.CollectedData["duration"]
	patientData["severity"] = conversation.CollectedData["severity"]
	patientData["other_details"] = conversation.CollectedData["other_details"]

	if conversation.CollectedData["age"] != nil {
		patientData["age"] = 0
		if ageStr, ok := conversation.CollectedData["age"].(string); ok {
			var age float64
			fmt.Sscanf(ageStr, "%f", &age)
			patientData["age"] = age
		} else if ageFloat, ok := conversation.CollectedData["age"].(float64); ok {
			patientData["age"] = ageFloat
		}
	}

	if conversation.CollectedData["gender"] != nil {
		if gender, ok := conversation.CollectedData["gender"].(string); ok {
			patientData["gender"] = gender
		}
	}

	return processedData, nil
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
