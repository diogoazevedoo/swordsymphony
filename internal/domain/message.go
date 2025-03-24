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
	ThreadID         uuid.UUID      `json:"thread_id"`
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
		ThreadID:         uuid.Nil,
		Sender:           sender,
		SenderName:       senderName,
		Recipient:        recipient,
		MessageType:      msgType,
		Priority:         3,
		Content:          content,
		RequiresResponse: false,
		Timestamp:        time.Now(),
	}
}

// TaskThread represents the conversation thread for a specific task
type TaskThread struct {
	TaskID         uuid.UUID              `json:"task_id"`
	ThreadID       uuid.UUID              `json:"thread_id"`
	Status         string                 `json:"status"`
	StartTime      time.Time              `json:"start_time"`
	Messages       []Message              `json:"messages"`
	AgentsInvolved map[string]bool        `json:"agents_involved"`
	AgentStatus    map[string]AgentStatus `json:"agent_status"`
}

// TaskInfo represents the output of StartTask
type TaskInfo struct {
	TaskID   uuid.UUID `json:"task_id"`
	ThreadID uuid.UUID `json:"thread_id"`
}
