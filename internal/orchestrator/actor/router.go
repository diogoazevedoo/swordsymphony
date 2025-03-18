package actor

import (
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// Router handles routing of messages between agents
type Router struct {
	routes          map[domain.MessageType][]domain.AgentType
	defaultHandlers map[domain.MessageType]domain.AgentType
	mu              sync.RWMutex
}

// NewRouter creates a new message router
func NewRouter() *Router {
	router := &Router{
		routes:          make(map[domain.MessageType][]domain.AgentType),
		defaultHandlers: make(map[domain.MessageType]domain.AgentType),
	}

	router.RegisterRoute(domain.TaskAssignment, domain.IntakeAgentType)
	router.RegisterRoute(domain.ProcessedData, domain.DiagnosticAgentType)
	router.RegisterRoute(domain.DiagnosisResults, domain.TreatmentAgentType)
	router.RegisterRoute(domain.StatusUpdate, domain.OrchestratorAgentType)
	router.RegisterRoute(domain.TaskComplete, domain.OrchestratorAgentType)

	return router
}

// RegisterRoute adds a route for a message type to an agent
func (r *Router) RegisterRoute(msgType domain.MessageType, agentType domain.AgentType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.routes[msgType]; !exists {
		r.routes[msgType] = make([]domain.AgentType, 0)
	}
	r.routes[msgType] = append(r.routes[msgType], agentType)

	if _, exists := r.defaultHandlers[msgType]; !exists {
		r.defaultHandlers[msgType] = agentType
	}
}

// SetDefaultHandler sets the default handler for a message type
func (r *Router) SetDefaultHandler(msgType domain.MessageType, agentType domain.AgentType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.defaultHandlers[msgType] = agentType
	r.RegisterRoute(msgType, agentType)
}

// RouteMessage determines which agent should handle a message
func (r *Router) RouteMessage(msg domain.Message) actor.Address {
	if msg.Recipient != "" && msg.Recipient != "all" {
		return actor.Address(msg.Recipient)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if agent, exists := r.defaultHandlers[msg.MessageType]; exists {
		return actor.Address(agent)
	}

	return ""
}
