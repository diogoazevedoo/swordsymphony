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
Your goal is to gather information in a conversational, empathetic manner while ensuring you collect all necessary medical details.
Speak naturally and conversationally like a real doctor would, not like a questionnaire. 
Respond to the patient's concerns and ask logical follow-up questions.
Try to get: chief complaint, symptom details (onset, duration, severity), medical history, current medications, allergies, and any questions the patient has.
Be empathetic, professional, and concise in your responses. Never diagnose or prescribe treatment during the call.`,
		greetingPrompt: `Begin the call by introducing yourself as an AI medical assistant. Say something like: "Hello, I'm Dr. AI from SwordSymphony Medical. I'll be asking you some questions to understand your health concerns better. This call is being recorded to provide you with the best care. May I start by asking your name?" Keep it brief but warm and professional.`,
		identityPrompt: `Now that you have the patient's name, politely ask for their email address so we can send them a summary of this conversation and any recommendations. Say something like: "Thank you, {{.PatientName}}. Could you please share your email address so we can send you a summary of our conversation afterward?" Be sure to confirm both the name and email address you hear to ensure accuracy.`,
		symptomsPrompt: `Begin asking about the patient's primary health concern or symptoms. Say something like: "What brings you to our call today? Can you tell me about any symptoms you're experiencing?" Listen carefully to their chief complaint and ask natural follow-up questions about the symptoms such as:
- When did these symptoms start?
- How severe are they on a scale of 1-10?
- Is there anything that makes them better or worse?
- Have you experienced anything like this before?
Make this feel like a natural conversation, not an interrogation. Show empathy and understanding.`,
		historyPrompt: `Now ask about the patient's relevant medical history. Say something like: "To better understand your situation, I'd like to know about your medical history. Do you have any ongoing health conditions or have you had any significant illnesses or surgeries in the past?" Listen carefully and ask appropriate follow-up questions. You should aim to learn about:
- Any chronic conditions (like diabetes, hypertension, etc.)
- Previous surgeries or hospitalizations
- Family history of relevant medical conditions
Be conversational and tie this to their current symptoms when appropriate.`,
		medicationPrompt: `Ask about medications and allergies. Say something like: "Are you currently taking any medications, including prescription medications, over-the-counter drugs, or supplements? And do you have any medication allergies I should be aware of?" Make sure to get specific names of medications and dosages if possible, as well as any allergic reactions they've experienced.`,
		questionsPrompt:  `Give the patient an opportunity to ask questions or share additional information. Say something like: "Before we wrap up, do you have any questions for me or is there anything else about your health that you think would be helpful for me to know?" Listen carefully to their questions/concerns and respond appropriately while setting proper expectations about the AI consultation process.`,
		closingPrompt:    `Conclude the call professionally. Say something like: "Thank you for sharing all this information with me today. I'm going to send this information to our medical team who will analyze it and send you a detailed summary by email. If you have any urgent concerns, please contact your primary healthcare provider. Is there anything else you need from me before we end our call?"`,
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

	if speaker == "patient" && !exchange.IsProcessed {
		go m.analyzePatientMessage(conversationID, len(conversation.Transcript)-1)
	}

	logger.Info("Added message to conversation",
		"conversation_id", conversationID,
		"speaker", speaker,
		"text_length", len(text),
		"confidence", confidence)

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
		MaxTokens:    256,
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

	ctx := contextBuilder.String()
	m.mu.RUnlock()

	var analysisPrompt string

	switch status {
	case StatusGreeting:
		analysisPrompt = `The patient is introducing themselves. Extract their name if present. 
Output JSON with the format: {"patient_name": "Name of Patient"}`

	case StatusIdentity:
		analysisPrompt = `The patient is providing their email address. Extract the email if present.
Output JSON with the format: {"patient_email": "email@example.com"}`

	case StatusSymptoms:
		analysisPrompt = `The patient is describing their symptoms. Extract all symptoms, their duration, severity, and any other relevant details.
Output JSON with the format: {
  "symptoms": ["symptom1", "symptom2"],
  "duration": "duration information",
  "severity": "severity information",
  "other_details": "any other relevant information"
}`

	case StatusHistory:
		analysisPrompt = `The patient is describing their medical history. Extract all medical conditions, previous surgeries, and family history details.
Output JSON with the format: {
  "medical_conditions": ["condition1", "condition2"],
  "surgeries": ["surgery1", "surgery2"],
  "family_history": ["relevant family history details"]
}`

	case StatusMedication:
		analysisPrompt = `The patient is describing their medications and allergies. Extract all medications, their dosages, and any allergies.
Output JSON with the format: {
  "medications": ["medication1 and dosage", "medication2 and dosage"],
  "allergies": ["allergy1", "allergy2"]
}`

	case StatusQuestions:
		analysisPrompt = `The patient is asking questions or providing additional information. Extract any questions and additional health information.
Output JSON with the format: {
  "questions": ["question1", "question2"],
  "additional_info": "any additional health information"
}`

	default:
		analysisPrompt = `Extract any relevant medical information from the patient's message.
Output JSON with the format: {
  "information": "extracted information"
}`
	}

	fullPrompt := fmt.Sprintf(`
You are an AI medical information extractor. Your task is to analyze the patient's message and extract relevant information.

Previous conversation context:
%s

Current conversation stage: %s

Patient message: "%s"

%s
`, ctx, status, message.Text, analysisPrompt)

	response, err := m.aiClient.GenerateCompletion(context.Background(), fullPrompt, ai.CompletionOptions{
		MaxTokens:   1024,
		Temperature: 0.1,
		ModelName:   "gpt-3.5-turbo",
	})

	if err != nil {
		logger.Error("Error analyzing patient message", "error", err)
		return
	}

	jsonStart := strings.Index(response.Text, "{")
	jsonEnd := strings.LastIndex(response.Text, "}")

	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonText := response.Text[jsonStart : jsonEnd+1]

		var extractedData map[string]interface{}
		err = json.Unmarshal([]byte(jsonText), &extractedData)
		if err != nil {
			logger.Error("Error parsing JSON from analysis", "error", err, "json", jsonText)
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
			}
		} else if status == StatusIdentity && extractedData["patient_email"] != nil {
			patientEmail, ok := extractedData["patient_email"].(string)
			if ok && patientEmail != "" {
				conversation.PatientEmail = patientEmail
				conversation.CollectedData["patient_email"] = patientEmail
			}
		} else {
			for key, value := range extractedData {
				if value != nil {
					if existingValue, exists := conversation.CollectedData[key]; exists {
						if existingSlice, ok := existingValue.([]interface{}); ok {
							if newSlice, ok := value.([]interface{}); ok {
								conversation.CollectedData[key] = append(existingSlice, newSlice...)
							} else {
								conversation.CollectedData[key] = append(existingSlice, value)
							}
						} else if newSlice, ok := value.([]interface{}); ok {
							conversation.CollectedData[key] = append([]interface{}{existingValue}, newSlice...)
						} else {
							conversation.CollectedData[key] = []interface{}{existingValue, value}
						}
					} else {
						conversation.CollectedData[key] = value
					}
				}
			}
		}

		logger.Info("Analyzed patient message",
			"conversation_id", conversationID,
			"extracted_keys", getMapKeys(extractedData))
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

	switch conversation.Status {
	case StatusGreeting:
		if conversation.PatientName != "" {
			conversation.Status = StatusIdentity
			logger.Info("Conversation progressed to identity stage", "conversation_id", conversationID)
		}

	case StatusIdentity:
		if conversation.PatientEmail != "" {
			conversation.Status = StatusSymptoms
			logger.Info("Conversation progressed to symptoms stage", "conversation_id", conversationID)
		}

	case StatusSymptoms:
		if symptoms, ok := conversation.CollectedData["symptoms"].([]interface{}); ok && len(symptoms) > 0 {
			symptomsCount := 0
			for i := len(conversation.Transcript) - 1; i >= 0; i-- {
				if conversation.Transcript[i].Speaker == "ai" &&
					conversation.Status == StatusSymptoms {
					symptomsCount++
				}
				if symptomsCount >= 3 {
					conversation.Status = StatusHistory
					logger.Info("Conversation progressed to history stage", "conversation_id", conversationID)
					break
				}
			}
		}

	case StatusHistory:
		if conditions, ok := conversation.CollectedData["medical_conditions"].([]interface{}); ok && len(conditions) > 0 {
			historyCount := 0
			for i := len(conversation.Transcript) - 1; i >= 0; i-- {
				if conversation.Transcript[i].Speaker == "ai" &&
					conversation.Status == StatusHistory {
					historyCount++
				}
				if historyCount >= 2 {
					conversation.Status = StatusMedication
					logger.Info("Conversation progressed to medication stage", "conversation_id", conversationID)
					break
				}
			}
		}

	case StatusMedication:
		hasMedications := false
		if medications, ok := conversation.CollectedData["medications"].([]interface{}); ok {
			hasMedications = len(medications) > 0
		}
		hasAllergies := false
		if allergies, ok := conversation.CollectedData["allergies"].([]interface{}); ok {
			hasAllergies = len(allergies) > 0
		}

		if hasMedications || hasAllergies {
			medicationCount := 0
			for i := len(conversation.Transcript) - 1; i >= 0; i-- {
				if conversation.Transcript[i].Speaker == "ai" &&
					conversation.Status == StatusMedication {
					medicationCount++
				}
				if medicationCount >= 2 {
					conversation.Status = StatusQuestions
					logger.Info("Conversation progressed to questions stage", "conversation_id", conversationID)
					break
				}
			}
		}

	case StatusQuestions:
		questionCount := 0
		for i := len(conversation.Transcript) - 1; i >= 0; i-- {
			if conversation.Transcript[i].Speaker == "ai" &&
				conversation.Status == StatusQuestions {
				questionCount++
			}
			if questionCount >= 2 {
				conversation.Status = StatusClosing
				logger.Info("Conversation progressed to closing stage", "conversation_id", conversationID)
				break
			}
		}

	case StatusClosing:
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
			logger.Info("Conversation ready for completion", "conversation_id", conversationID)
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
