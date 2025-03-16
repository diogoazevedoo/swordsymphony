package domain

import (
	"context"
)

// AgentStatus represents the current state of an agent
type AgentStatus string

const (
	AgentIdle     AgentStatus = "idle"
	AgentBusy     AgentStatus = "busy"
	AgentComplete AgentStatus = "complete"
	AgentError    AgentStatus = "error"
)

// Agent defines the interface that all agents must implement
type Agent interface {
	// ID returns the unique identifier for this agent
	ID() string

	// Name returns the human-readable name of this agent
	Name() string

	// ProcessMessage handles an incoming message and returns any response messages
	ProcessMessage(ctx context.Context, msg Message) []Message

	// Status returns the current status of the agent
	Status() AgentStatus

	// SetStatus updates the agent's status
	SetStatus(status AgentStatus)
}
