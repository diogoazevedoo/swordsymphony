package orchestrator

import (
	"log"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// MessageRouter handles routing of messages between agents
type MessageRouter struct {
	routes          map[domain.MessageType][]domain.AgentType
	routesByAgent   map[domain.AgentType][]domain.MessageType
	defaultHandlers map[domain.MessageType]domain.AgentType
	mu              sync.RWMutex
}

// NewMessageRouter creates a new message router
func NewMessageRouter() *MessageRouter {
	router := &MessageRouter{
		routes:          make(map[domain.MessageType][]domain.AgentType),
		routesByAgent:   make(map[domain.AgentType][]domain.MessageType),
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
func (r *MessageRouter) RegisterRoute(msgType domain.MessageType, agentType domain.AgentType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.routes[msgType]; !exists {
		r.routes[msgType] = make([]domain.AgentType, 0)
	}
	r.routes[msgType] = append(r.routes[msgType], agentType)

	if _, exists := r.routesByAgent[agentType]; !exists {
		r.routesByAgent[agentType] = make([]domain.MessageType, 0)
	}
	r.routesByAgent[agentType] = append(r.routesByAgent[agentType], msgType)

	if _, exists := r.defaultHandlers[msgType]; !exists {
		r.defaultHandlers[msgType] = agentType
	}
}

// SetDefaultHandler sets the default handler for a message type
func (r *MessageRouter) SetDefaultHandler(msgType domain.MessageType, agentType domain.AgentType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.defaultHandlers[msgType] = agentType
	r.RegisterRoute(msgType, agentType)
}

// GetAgentsForMessageType returns all agents that can handle a message type
func (r *MessageRouter) GetAgentsForMessageType(msgType domain.MessageType) []domain.AgentType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if agents, exists := r.routes[msgType]; exists {
		return agents
	}
	return nil
}

// GetDefaultHandlerForMessageType returns the default handler for a message type
func (r *MessageRouter) GetDefaultHandlerForMessageType(msgType domain.MessageType) (domain.AgentType, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agent, exists := r.defaultHandlers[msgType]
	return agent, exists
}

// GetMessageTypesForAgent returns all message types an agent can handle
func (r *MessageRouter) GetMessageTypesForAgent(agentType domain.AgentType) []domain.MessageType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if msgTypes, exists := r.routesByAgent[agentType]; exists {
		return msgTypes
	}
	return nil
}

// RouteMessage determines which agent should handle a message
func (r *MessageRouter) RouteMessage(msg domain.Message) domain.AgentType {
	if msg.Recipient != "" && msg.Recipient != "all" {
		return domain.AgentType(msg.Recipient)
	}

	if agent, exists := r.GetDefaultHandlerForMessageType(msg.MessageType); exists {
		return agent
	}

	log.Printf("Warning: No route found for message type %s", msg.MessageType)
	return ""
}
