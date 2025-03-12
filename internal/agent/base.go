package agent

import (
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// BaseAgent provides common functionality for all agents
type BaseAgent struct {
	id        string
	name      string
	status    domain.AgentStatus
	mu        sync.RWMutex
	knowledge map[string]any
}

// NewBaseAgent creates a new base agent
func NewBaseAgent(id string, name string) BaseAgent {
	return BaseAgent{
		id:        id,
		name:      name,
		status:    domain.AgentIdle,
		knowledge: make(map[string]any),
	}
}

// ID returns the agent's unique identifier
func (a *BaseAgent) ID() string {
	return a.id
}

// Name returns the agent's human-readable name
func (a *BaseAgent) Name() string {
	return a.name
}

// Status return the agent's current status
func (a *BaseAgent) Status() domain.AgentStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.status
}

// SetStatus updates the agent's status
func (a *BaseAgent) SetStatus(status domain.AgentStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = status
}

// UpdateKnowledge stores information in the agent's knowledge base
func (a *BaseAgent) UpdateKnowledge(key string, value any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.knowledge[key] = value
}

// GetKnowledge retrieves information from the agent's knowledge base
func (a *BaseAgent) GetKnowledge(key string) (any, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	value, exists := a.knowledge[key]
	return value, exists
}
