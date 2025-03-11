package domain

import (
	"time"

	"github.com/google/uuid"
)

// MessageType defines the type of message being sent between agents
type MessageType string

const (
	TaskAssignment   MessageType = "task_assignment"
	ProcessedData    MessageType = "processed_data"
	DiagnosisResults MessageType = "diagnosis_results"
	TreatmentPlan    MessageType = "treatment_plan"
	StatusUpdate     MessageType = "status_update"
	TaskComplete     MessageType = "task_complete"
)

// Message represents communication between agents
type Message struct {
	ID               uuid.UUID      `json:"id"`
	ThreadID         string         `json:"thread_id"`
	Sender           string         `json:"sender"`
	SenderName       string         `json:"sender_name"`
	Recipient        string         `json:"recipient"`
	MessageType      MessageType    `json:"message_type"`
	Priority         int            `json:"priority"`
	Content          map[string]any `json:"content"`
	RequiresResponse bool           `json:"requires_response"`
	Timestamp        time.Time      `json:"timestamp"`
}

// NewMessage creates a message with defaults
func NewMessage(
	sender, senderName, recipient string,
	msgType MessageType,
	content map[string]any,
) *Message {
	return &Message{
		ID:               uuid.New(),
		ThreadID:         "", // Set by orchestrator
		Sender:           sender,
		SenderName:       senderName,
		Recipient:        recipient,
		MessageType:      msgType,
		Priority:         3, // Default priority
		Content:          content,
		RequiresResponse: false,
		Timestamp:        time.Now(),
	}
}
