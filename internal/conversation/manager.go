package conversation

import (
	"fmt"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// MessageType represents the type of message in a conversation
type MessageType string

const (
	MessageTypeAI      MessageType = "ai"
	MessageTypePatient MessageType = "patient"
)

// Message represents a single message in the conversation
type Message struct {
	Type      MessageType `json:"type"`
	Content   string      `json:"content"`
	Timestamp time.Time   `json:"timestamp"`
}

// Conversation represents an ongoing conversation with a patient
type Conversation struct {
	ID            string            `json:"id"`
	PatientPhone  string            `json:"patient_phone"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	IsActive      bool              `json:"is_active"`
	QuestionIndex int               `json:"question_index"` // Current question index
	Transcript    []Message         `json:"transcript"`
	Metadata      map[string]string `json:"metadata"`
}

// ConversationManager handles medical conversations
type ConversationManager struct {
	activeConversations map[string]*Conversation
	questions           []string // List of questions in order
	mu                  sync.RWMutex
}

// NewConversationManager creates a new conversation manager with fixed questions
func NewConversationManager() *ConversationManager {
	return &ConversationManager{
		activeConversations: make(map[string]*Conversation),
		questions: []string{
			"Hello, I'm the medical assistant from Sword Symphony. I need to ask you some questions about your health. What is your name?",
			"Thank you. How old are you?",
			"What is your gender? Male, female, or other?",
			"What symptoms are you experiencing? Please tell me any health issues you're having.",
			"Do you have any existing medical conditions like diabetes, hypertension, or asthma?",
			"What medications are you currently taking? If none, please say 'none'.",
			"Do you have any allergies to medications or other substances?",
			"Thank you for providing all the information. We have everything we need. Have a good day. Goodbye.",
			"Thank you for your time. Your information has been recorded. Goodbye.",
		},
	}
}

// StartConversation begins a new conversation
func (m *ConversationManager) StartConversation(patientPhone string) (*Conversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if there's already an active conversation for this patient
	for _, conv := range m.activeConversations {
		if conv.PatientPhone == patientPhone && conv.IsActive {
			logger.Info("Found existing active conversation",
				"patient_phone", patientPhone,
				"conversation_id", conv.ID)
			return conv, nil
		}
	}

	// Generate a unique conversation ID
	conversationID := fmt.Sprintf("conv_%s", time.Now().Format("20060102150405.000000"))

	// Create a new conversation
	conversation := &Conversation{
		ID:            conversationID,
		PatientPhone:  patientPhone,
		StartTime:     time.Now(),
		IsActive:      true,
		QuestionIndex: 0,
		Transcript:    []Message{},
		Metadata:      make(map[string]string),
	}

	m.activeConversations[conversationID] = conversation

	logger.Info("Started new conversation",
		"conversation_id", conversationID,
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

// AddMessage adds a message to the conversation and advances question index for patient messages
func (m *ConversationManager) AddMessage(conversationID string, messageType MessageType, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return fmt.Errorf("conversation not found: %s", conversationID)
	}

	message := Message{
		Type:      messageType,
		Content:   content,
		Timestamp: time.Now(),
	}

	conversation.Transcript = append(conversation.Transcript, message)

	// If this is a patient message, advance to the next question
	if messageType == MessageTypePatient {
		if conversation.QuestionIndex < len(m.questions)-1 {
			conversation.QuestionIndex++
		}
	}

	logger.Info("Added message to conversation",
		"conversation_id", conversationID,
		"message_type", string(messageType),
		"content_length", len(content),
		"question_index", conversation.QuestionIndex)

	return nil
}

// GetNextQuestion returns the current question based on the conversation state
func (m *ConversationManager) GetNextQuestion(conversationID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "", fmt.Errorf("conversation not found: %s", conversationID)
	}

	// If we're at or past the end of questions, return the goodbye message
	if conversation.QuestionIndex >= len(m.questions) {
		return m.questions[len(m.questions)-1], nil
	}

	// Return the current question
	return m.questions[conversation.QuestionIndex], nil
}

// CompleteConversation marks a conversation as complete
func (m *ConversationManager) CompleteConversation(conversationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return fmt.Errorf("conversation not found: %s", conversationID)
	}

	conversation.IsActive = false
	conversation.EndTime = time.Now()

	logger.Info("Marked conversation as complete",
		"conversation_id", conversationID,
		"duration", conversation.EndTime.Sub(conversation.StartTime).String(),
		"message_count", len(conversation.Transcript))

	return nil
}

// FormatTranscriptForProcessing formats the transcript for AI processing
func (m *ConversationManager) FormatTranscriptForProcessing(conversationID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conversation, exists := m.activeConversations[conversationID]
	if !exists {
		return "", fmt.Errorf("conversation not found: %s", conversationID)
	}

	var formatted string
	for _, message := range conversation.Transcript {
		speaker := "Medical Assistant"
		if message.Type == MessageTypePatient {
			speaker = "Patient"
		}
		formatted += speaker + ": " + message.Content + "\n\n"
	}

	return formatted, nil
}
