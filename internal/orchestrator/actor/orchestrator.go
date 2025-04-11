package actor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/google/uuid"
)

// OrchestratorActor manages the multi-agent system
type OrchestratorActor struct {
	*actor.BaseActor
	activeThreads map[uuid.UUID]*domain.TaskThread
	subscribers   map[chan domain.Message]bool
	router        *Router
	mu            sync.RWMutex
}

// NewOrchestratorActor creates a new orchestrator actor
func NewOrchestratorActor(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (*OrchestratorActor, error) {
	baseActor := actor.NewBaseActor(actor.Address(domain.OrchestratorAgentType), config, system)

	orchestrator := &OrchestratorActor{
		BaseActor:     baseActor,
		activeThreads: make(map[uuid.UUID]*domain.TaskThread),
		subscribers:   make(map[chan domain.Message]bool),
		router:        NewRouter(),
	}

	return orchestrator, nil
}

// Start initializes the orchestrator
func (o *OrchestratorActor) Start() error {
	logger.Info("Orchestrator actor starting")
	return nil
}

// Stop gracefully shuts down the orchestrator
func (o *OrchestratorActor) Stop() error {
	logger.Info("Orchestrator actor stopping")

	o.mu.Lock()
	defer o.mu.Unlock()

	for ch := range o.subscribers {
		close(ch)
	}
	o.subscribers = make(map[chan domain.Message]bool)

	return nil
}

// Receive processes incoming messages
func (o *OrchestratorActor) Receive(ctx context.Context, envelope *actor.Envelope) error {
	msg := envelope.Message

	o.storeMessage(msg)

	o.broadcast(msg)

	switch msg.MessageType {
	case domain.TaskAssignment:
		return o.handleTaskAssignment(ctx, msg)
	case domain.StatusUpdate:
		return o.handleStatusUpdate(ctx, msg)
	case domain.TaskComplete:
		return o.handleTaskCompletion(ctx, msg)
	default:
		return o.routeMessage(ctx, msg)
	}
}

// Subscribe adds a new channel to receive message broadcasts
func (o *OrchestratorActor) Subscribe() chan domain.Message {
	ch := make(chan domain.Message, 100)

	o.mu.Lock()
	o.subscribers[ch] = true
	o.mu.Unlock()

	return ch
}

// Unsubscribe removes a channel from broadcasts
func (o *OrchestratorActor) Unsubscribe(ch chan domain.Message) {
	o.mu.Lock()
	delete(o.subscribers, ch)
	o.mu.Unlock()

	close(ch)
}

// StartTask initiates a new task with the given details
func (o *OrchestratorActor) StartTask(taskDetails map[string]any) domain.TaskInfo {
	thread := &domain.TaskThread{
		TaskID:         uuid.New(),
		ThreadID:       uuid.New(),
		Status:         "started",
		StartTime:      time.Now(),
		Messages:       []domain.Message{},
		AgentsInvolved: make(map[string]bool),
		AgentStatus:    make(map[string]domain.AgentStatus),
	}

	o.mu.Lock()
	o.activeThreads[thread.ThreadID] = thread
	o.mu.Unlock()

	thread.AgentsInvolved[string(domain.IntakeAgentType)] = true
	thread.AgentStatus[string(domain.IntakeAgentType)] = domain.AgentBusy

	messageContent := map[string]any{
		"task_id": thread.TaskID.String(),
		"action":  "process_patient_data",
		"data":    taskDetails["patient_data"],
	}

	if patientData, ok := taskDetails["patient_data"].(map[string]any); ok {
		if documentAnalyses, ok := patientData["document_analyses"]; ok {
			logger.Info("Including document analyses in intake task message",
				"document_analyses_present", true)
			messageContent["document_analyses"] = documentAnalyses
		}
	}

	taskMsg := domain.NewMessage(
		string(o.Address()),
		"Orchestrator",
		string(domain.IntakeAgentType),
		domain.TaskAssignment,
		messageContent,
	)
	taskMsg.ThreadID = thread.ThreadID

	o.Send(actor.Address(domain.IntakeAgentType), *taskMsg)

	o.mu.Lock()
	thread.Messages = append(thread.Messages, *taskMsg)
	o.mu.Unlock()

	o.broadcast(*taskMsg)

	return domain.TaskInfo{
		TaskID:   thread.TaskID,
		ThreadID: thread.ThreadID,
	}
}

// GetAllMessages returns all messages for a given thread
func (o *OrchestratorActor) GetAllMessages(threadID uuid.UUID) []domain.Message {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if thread, exists := o.activeThreads[threadID]; exists {
		return thread.Messages
	}

	return []domain.Message{}
}

// GetAllSystemMessages returns all messages for the orchestrator
func (o *OrchestratorActor) GetAllSystemMessages() []domain.Message {
	o.mu.RLock()
	defer o.mu.RUnlock()

	messages := make([]domain.Message, 0, len(o.activeThreads))
	for _, thread := range o.activeThreads {
		messages = append(messages, thread.Messages...)
	}

	return messages
}

// GetAgentStatus returns the status of all agents
func (o *OrchestratorActor) GetAgentStatus() map[string]domain.AgentStatus {
	actors := o.System.GetAllActors()

	statuses := make(map[string]domain.AgentStatus)

	for addr, a := range actors {
		if statusProvider, ok := a.(interface{ Status() domain.AgentStatus }); ok {
			statuses[string(addr)] = statusProvider.Status()
		} else {
			statuses[string(addr)] = domain.AgentIdle
		}
	}

	return statuses
}

func (o *OrchestratorActor) broadcast(msg domain.Message) {
	o.mu.RLock()
	subscribers := make([]chan domain.Message, 0, len(o.subscribers))
	for ch := range o.subscribers {
		subscribers = append(subscribers, ch)
	}
	o.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- msg:
			// Message sent successfully
		default:
			logger.Warn("Subscriber channel full, dropping message",
				"message_type", msg.MessageType)
		}
	}
}

func (o *OrchestratorActor) handleTaskAssignment(ctx context.Context, msg domain.Message) error {
	logger.Info("Task assigned",
		"task_id", msg.Content["task_id"],
		"recipient", msg.Recipient)
	return nil
}

func (o *OrchestratorActor) handleStatusUpdate(ctx context.Context, msg domain.Message) error {
	taskID, ok := msg.Content["task_id"].(string)
	if !ok {
		return nil
	}

	status, ok := msg.Content["status"].(string)
	if !ok {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	for _, thread := range o.activeThreads {
		if thread.TaskID.String() == taskID {
			thread.Status = status
			break
		}
	}

	return nil
}

func (o *OrchestratorActor) handleTaskCompletion(ctx context.Context, msg domain.Message) error {
	taskID, ok := msg.Content["task_id"].(string)
	if !ok {
		logger.Error("Task completion message missing task_id")
		return fmt.Errorf("task completion message missing task_id")
	}

	logger.Info("Task completion message received",
		"task_id", taskID,
		"sender", msg.Sender,
		"content_keys", mapKeys(msg.Content))

	o.mu.Lock()
	defer o.mu.Unlock()

	var thread *domain.TaskThread
	for _, t := range o.activeThreads {
		if t.TaskID.String() == taskID {
			thread = t
			break
		}
	}

	if thread == nil {
		return fmt.Errorf("no thread found for task %s", taskID)
	}

	if thread.AgentStatus == nil {
		thread.AgentStatus = make(map[string]domain.AgentStatus)
	}

	thread.AgentStatus[msg.Sender] = domain.AgentComplete

	allComplete := true
	for agent := range thread.AgentsInvolved {
		status, exists := thread.AgentStatus[agent]
		if !exists || status != domain.AgentComplete {
			allComplete = false
			break
		}
	}

	if allComplete {
		logger.Info("All agents completed task",
			"task_id", taskID,
			"thread_id", thread.ThreadID)

		thread.Status = "completed"

		completionMsg := domain.NewMessage(
			string(o.Address()),
			"Orchestrator",
			"all",
			domain.TaskComplete,
			map[string]any{
				"task_id": thread.TaskID.String(),
				"status":  "completed",
				"message": "Task processing complete",
				"summary": thread,
			},
		)
		completionMsg.ThreadID = thread.ThreadID

		o.broadcast(*completionMsg)
	}

	return nil
}

func (o *OrchestratorActor) routeMessage(ctx context.Context, msg domain.Message) error {
	recipient := o.router.RouteMessage(msg)

	if string(recipient) == "" || string(recipient) == string(domain.OrchestratorAgentType) {
		return nil
	}

	envelope := actor.NewEnvelope(actor.Address(msg.Sender), recipient, msg)
	return o.System.Send(envelope)
}

func (o *OrchestratorActor) storeMessage(msg domain.Message) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if thread, exists := o.activeThreads[msg.ThreadID]; exists {
		thread.Messages = append(thread.Messages, msg)

		if msg.Sender != string(o.Address()) {
			thread.AgentsInvolved[msg.Sender] = true
		}
		if msg.Recipient != "all" && msg.Recipient != string(o.Address()) {
			thread.AgentsInvolved[msg.Recipient] = true
		}
	}
}

// AgentExists checks if an agent is registered in the actor system
func (o *OrchestratorActor) AgentExists(address actor.Address) bool {
	_, exists := o.System.GetActor(address)
	return exists
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
