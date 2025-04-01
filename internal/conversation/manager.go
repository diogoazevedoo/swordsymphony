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

// NewConversationManager creates a new conversation manager with improved prompts
func NewConversationManager(aiClient ai.Client, options ...ManagerOption) *ConversationManager {
	manager := &ConversationManager{
		aiClient:            aiClient,
		activeConversations: make(map[string]*Conversation),
		systemPrompt: `You are an AI medical doctor conducting a phone interview with a patient. 
Be efficient, direct, and professional while remaining warm and empathetic. 
Your goal is to quickly gather essential medical information in a conversational way.
Keep responses under 3 sentences when possible. 
Ask only ONE question at a time. 
Never repeat questions that have already been asked or answered.
Listen carefully to patient responses and adjust follow-up questions accordingly.
Always keep track of what information you've already gathered.`,

		// Clear, direct greeting that sets expectations
		greetingPrompt: `Briefly introduce yourself as Dr. AI from SwordSymphony Medical. 
Say you're going to ask a few quick questions about the patient's health. 
Mention the call is being recorded to provide better care.
Ask ONLY for their name - nothing else in this first exchange.
Keep it brief but warm.`,

		// Get email for follow-up
		identityPrompt: `Thank {{.PatientName}} for sharing their name.
Ask ONLY for their email address so you can send a summary of the conversation.
Keep it brief and only ask this single question.`,

		// Focus on primary concern
		symptomsPrompt: `Ask what specific health concern brings them to this call today.
Focus only on their primary symptoms in this question.
Don't list possible symptoms or suggest answers - just ask one clear, open-ended question.`,

		// Get medical history
		historyPrompt: `Ask if they have any ongoing medical conditions or significant past medical history.
Keep it to a single, direct question focused only on their medical history.
Do not ask about medications or allergies yet - that will be your next question.`,

		// Get medications and allergies
		medicationPrompt: `Ask about what medications they're currently taking and if they have any known drug allergies.
Ask this as a single question about both medications and allergies.
Be brief and direct.`,

		// Allow for additional information
		questionsPrompt: `Ask if there's anything else about their health concerns they'd like to share,
or if they have any questions before ending the call.
Keep it brief and open-ended.`,

		// Clear closing
		closingPrompt: `Thank them for sharing their information.
Let them know you'll send a summary to their email with an initial assessment.
Briefly explain that a medical team will review their case and follow up if needed.
End with a polite goodbye.`,
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

	logger.Info("Checking conversation progression",
		"conversation_id", conversationID,
		"current_status", conversation.Status,
		"has_name", conversation.PatientName != "",
		"has_email", conversation.PatientEmail != "")

	// Count exchanges at each stage to prevent getting stuck
	stageExchangeCount := make(map[ConversationStatus]int)

	for i := 0; i < len(conversation.Transcript); i++ {
		if conversation.Transcript[i].Speaker == "ai" {
			// Use the conversation status at the time this message was sent
			// (This is an approximation since we don't store status with messages)
			var messageStage ConversationStatus

			// Try to determine the stage based on message content
			message := strings.ToLower(conversation.Transcript[i].Text)
			if strings.Contains(message, "email") {
				messageStage = StatusIdentity
			} else if strings.Contains(message, "health concern") || strings.Contains(message, "symptoms") {
				messageStage = StatusSymptoms
			} else if strings.Contains(message, "medical history") || strings.Contains(message, "health conditions") {
				messageStage = StatusHistory
			} else if strings.Contains(message, "medications") || strings.Contains(message, "allergies") {
				messageStage = StatusMedication
			} else if i == 0 || strings.Contains(message, "name") {
				messageStage = StatusGreeting
			} else {
				messageStage = StatusQuestions
			}

			stageExchangeCount[messageStage]++
		}
	}

	// Logic for progressing the conversation
	switch conversation.Status {
	case StatusGreeting:
		if conversation.PatientName != "" || stageExchangeCount[StatusGreeting] >= 2 {
			// Set a placeholder name if needed
			if conversation.PatientName == "" {
				conversation.PatientName = "Patient"
				conversation.CollectedData["patient_name"] = "Patient"
			}
			conversation.Status = StatusIdentity
			logger.Info("Conversation progressed to identity stage",
				"conversation_id", conversationID,
				"patient_name", conversation.PatientName)
		}

	case StatusIdentity:
		if conversation.PatientEmail != "" || stageExchangeCount[StatusIdentity] >= 2 {
			// Set a placeholder email if needed
			if conversation.PatientEmail == "" {
				conversation.PatientEmail = "unknown@example.com"
				conversation.CollectedData["patient_email"] = "unknown@example.com"
			}
			conversation.Status = StatusSymptoms
			logger.Info("Conversation progressed to symptoms stage",
				"conversation_id", conversationID,
				"identity_count", stageExchangeCount[StatusIdentity])
		}

	case StatusSymptoms:
		if stageExchangeCount[StatusSymptoms] >= 1 {
			conversation.Status = StatusHistory
			logger.Info("Conversation progressed to history stage",
				"conversation_id", conversationID)
		}

	case StatusHistory:
		if stageExchangeCount[StatusHistory] >= 1 {
			conversation.Status = StatusMedication
			logger.Info("Conversation progressed to medication stage",
				"conversation_id", conversationID)
		}

	case StatusMedication:
		if stageExchangeCount[StatusMedication] >= 1 {
			conversation.Status = StatusQuestions
			logger.Info("Conversation progressed to questions stage",
				"conversation_id", conversationID)
		}

	case StatusQuestions:
		if stageExchangeCount[StatusQuestions] >= 1 {
			conversation.Status = StatusClosing
			logger.Info("Conversation progressed to closing stage",
				"conversation_id", conversationID)
		}

	case StatusClosing:
		// Complete after one closing message and one patient reply
		closingExchanges := 0
		patientReplied := false

		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				strings.Contains(strings.ToLower(conversation.Transcript[i].Text), "thank") {
				closingExchanges++
			} else if conversation.Transcript[i].Speaker == "patient" && closingExchanges > 0 {
				patientReplied = true
				break
			}
		}

		if (closingExchanges > 0 && patientReplied) || stageExchangeCount[StatusClosing] >= 2 {
			conversation.Status = StatusComplete
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
