package actor

import (
	"context"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/google/uuid"
)

// Address represents a unique identifier for an actor
type Address string

// Actor defines the interface that all actors must implement
type Actor interface {
	// Address returns the actor's unique identifier
	Address() Address

	// Receive processes a message and returns any response messages
	Receive(ctx context.Context, envelope *Envelope) error

	// Start initializes the actor
	Start() error

	// Stop gracefully shuts down the actor
	Stop() error
}

// ActorCreator is a function that creates a new actor
type ActorCreator func(ctx context.Context, config ActorConfig, system ActorSystem) (Actor, error)

// ActorConfig represents the configuration for an actor
type ActorConfig struct {
	ID          string         `json:"id"`
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Properties  map[string]any `json:"properties"`
}

// Envelope wraps a message with additional metadata
type Envelope struct {
	ID          uuid.UUID
	Sender      Address
	Recipient   Address
	Message     domain.Message
	ThreadID    uuid.UUID
	Timestamp   int64
	Correlation string
}

// NewEnvelope creates a new message envelope
func NewEnvelope(sender Address, recipient Address, msg domain.Message) *Envelope {
	return &Envelope{
		ID:        uuid.New(),
		Sender:    sender,
		Recipient: recipient,
		Message:   msg,
		ThreadID:  msg.ThreadID,
		Timestamp: time.Now().UnixNano(),
	}
}

// BaseActor provides common functionality for all actors
type BaseActor struct {
	address   Address
	config    ActorConfig
	System    ActorSystem
	state     map[string]any
	stateLock sync.RWMutex
}

// NewBaseActor creates a new base actor
func NewBaseActor(address Address, config ActorConfig, system ActorSystem) *BaseActor {
	return &BaseActor{
		address: address,
		config:  config,
		System:  system,
		state:   make(map[string]any),
	}
}

// Address returns the actor's unique identifier
func (a *BaseActor) Address() Address {
	return a.address
}

// Send sends a message to another actor
func (a *BaseActor) Send(recipient Address, msg domain.Message) error {
	envelope := NewEnvelope(a.address, recipient, msg)
	return a.System.Send(envelope)
}

// GetState retrieves a value from the actor's state
func (a *BaseActor) GetState(key string) (any, bool) {
	a.stateLock.RLock()
	defer a.stateLock.RUnlock()
	value, exists := a.state[key]
	return value, exists
}

// SetState updates a value in the actor's state
func (a *BaseActor) SetState(key string, value any) {
	a.stateLock.Lock()
	defer a.stateLock.Unlock()
	a.state[key] = value
}

// Config returns the actor's configuration
func (a *BaseActor) Config() ActorConfig {
	return a.config
}

// Status returns the actor's current status
func (a *BaseActor) Status() domain.AgentStatus {
	if statusField, ok := a.GetState("status"); ok {
		if status, ok := statusField.(domain.AgentStatus); ok {
			return status
		}
	}

	return domain.AgentIdle
}

// SetStatus updates the actor's status
func (a *BaseActor) SetStatus(status domain.AgentStatus) {
	a.SetState("status", status)
}
