package orchestrator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/google/uuid"
)

// Orchestrator manages communication between agents
type Orchestrator struct {
	agents        map[string]domain.Agent
	messageQueue  chan domain.Message
	activeThreads map[uuid.UUID]*TaskThread
	mu            sync.RWMutex
	subscribers   map[chan domain.Message]bool
	router        *MessageRouter
}

// TaskThread represents the conversation thread for a specific task
type TaskThread struct {
	TaskID         uuid.UUID
	ThreadID       uuid.UUID
	Status         string
	StartTime      time.Time
	Messages       []domain.Message
	AgentsInvolved map[string]bool
}

// NewOrchestrator creates a new orchestration engine
func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		agents:        make(map[string]domain.Agent),
		messageQueue:  make(chan domain.Message, 100),
		activeThreads: make(map[uuid.UUID]*TaskThread),
		subscribers:   make(map[chan domain.Message]bool),
		router:        NewMessageRouter(),
	}
}

// RegisterAgent adds an agent to the orchestrator
func (o *Orchestrator) RegisterAgent(agent domain.Agent) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.agents[agent.ID()] = agent
	logger.Info("Agent registered",
		"agent_id", agent.ID(),
		"agent_name", agent.Name())
}

// TaskInfo represents the output of StartTask
type TaskInfo struct {
	TaskID   uuid.UUID
	ThreadID uuid.UUID
}

// StartTask initiates a new task with the given details
func (o *Orchestrator) StartTask(taskDetails map[string]any) TaskInfo {
	thread := &TaskThread{
		TaskID:         uuid.New(),
		ThreadID:       uuid.New(),
		Status:         "started",
		StartTime:      time.Now(),
		Messages:       []domain.Message{},
		AgentsInvolved: make(map[string]bool),
	}

	o.mu.Lock()
	o.activeThreads[thread.ThreadID] = thread
	o.mu.Unlock()

	o.mu.RLock()
	_, exists := o.agents["intake_agent"]
	o.mu.RUnlock()

	if exists {
		thread.AgentsInvolved["intake_agent"] = true

		taskMsg := domain.NewMessage(
			"orchestrator",
			"Orchestrator",
			"intake_agent",
			domain.TaskAssignment,
			map[string]any{
				"task_id": thread.TaskID,
				"action":  "process_patient_data",
				"data":    taskDetails["patient_data"],
			},
		)
		taskMsg.ThreadID = thread.ThreadID

		o.messageQueue <- *taskMsg

		o.mu.Lock()
		thread.Messages = append(thread.Messages, *taskMsg)
		o.mu.Unlock()

		o.broadcast(*taskMsg)
	}

	return TaskInfo{
		TaskID:   thread.TaskID,
		ThreadID: thread.ThreadID,
	}
}

// StartProcessing begins the message handling loop
func (o *Orchestrator) StartProcessing() {
	logger.Info("Starting orchestration engine")

	go func() {
		for msg := range o.messageQueue {
			o.handleMessage(msg)
		}
	}()
}

// handleMessage processs a single message
func (o *Orchestrator) handleMessage(msg domain.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	o.mu.Lock()
	if thread, exists := o.activeThreads[msg.ThreadID]; exists {
		thread.Messages = append(thread.Messages, msg)
	}
	o.mu.Unlock()

	o.broadcast(msg)

	switch msg.MessageType {
	case domain.TaskComplete:
		o.handleTaskCompletion(msg)
		return
	case domain.StatusUpdate:
		// Simply broadcast status updates
		return
	}

	targetAgentType := o.router.RouteMessage(msg)
	if string(targetAgentType) == "" || string(targetAgentType) == string(domain.OrchestratorAgentType) {
		return
	}

	o.mu.RLock()
	recipient, exists := o.agents[string(targetAgentType)]
	o.mu.RUnlock()

	if !exists {
		logger.Warn("No agent found for recipient",
			"recipient", targetAgentType,
			"message_type", msg.MessageType,
			"thread_id", msg.ThreadID)
		return
	}

	recipient.SetStatus(domain.AgentBusy)
	responses := recipient.ProcessMessage(ctx, msg)

	for _, response := range responses {
		o.messageQueue <- response
	}
}

// handleTaskCompletion checks if all agents are done with a task
func (o *Orchestrator) handleTaskCompletion(msg domain.Message) {
	taskID := msg.Content["task_id"]
	agentID := msg.Sender

	logger.Info("Task completion message received",
		"task_id", taskID,
		"agent_id", agentID,
		"thread_id", msg.ThreadID)

	o.mu.Lock()
	defer o.mu.Unlock()

	var thread *TaskThread
	for _, t := range o.activeThreads {
		if t.TaskID == taskID {
			thread = t
			break
		}
	}

	if thread == nil {
		log.Printf("Warning: No thread found for task %s", taskID)
		return
	}

	if agent, exists := o.agents[agentID]; exists {
		agent.SetStatus(domain.AgentComplete)
	}

	allComplete := true
	for agentID := range thread.AgentsInvolved {
		o.mu.RLock()
		agent, exists := o.agents[agentID]
		o.mu.RUnlock()

		if !exists || agent.Status() != domain.AgentComplete {
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
			"orchestrator",
			"Orchestrator",
			"all",
			domain.TaskComplete,
			map[string]any{
				"task_id": thread.TaskID,
				"status":  "completed",
				"message": "Task processing complete",
				"summary": thread,
			},
		)
		completionMsg.ThreadID = thread.ThreadID

		o.broadcast(*completionMsg)
	}
}

// Subscribe adds a new channel to receive message broadcasts
func (o *Orchestrator) Subscribe() chan domain.Message {
	ch := make(chan domain.Message, 100)

	o.mu.Lock()
	o.subscribers[ch] = true
	o.mu.Unlock()

	return ch
}

// Unsubscribe removes a channel from broadcasts
func (o *Orchestrator) Unsubscribe(ch chan domain.Message) {
	o.mu.Lock()
	delete(o.subscribers, ch)
	o.mu.Unlock()

	close(ch)
}

// broadcast sends a message to all subscribers
func (o *Orchestrator) broadcast(msg domain.Message) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for ch := range o.subscribers {
		ch <- msg
	}
}

// GetAllMessages returns all messages for a given thread
func (o *Orchestrator) GetAllMessages(threadID uuid.UUID) []domain.Message {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if thread, exists := o.activeThreads[threadID]; exists {
		return thread.Messages
	}

	return []domain.Message{}
}

// GetAgentStatus returns the status of all agents
func (o *Orchestrator) GetAgentStatus() map[string]domain.AgentStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()

	status := make(map[string]domain.AgentStatus)
	for id, agent := range o.agents {
		status[id] = agent.Status()
	}

	return status
}

// Shutdown stops the orchestrator gracefully
func (o *Orchestrator) Shutdown(ctx context.Context) error {
	logger.Info("Orchestrator is shutting down")

	close(o.messageQueue)

	shutdownMsg := domain.NewMessage(
		string(domain.OrchestratorAgentType),
		"Orchestrator",
		"all",
		domain.StatusUpdate,
		map[string]any{
			"status":  "shutting_down",
			"message": "System is shutting down",
		},
	)

	o.broadcast(*shutdownMsg)

	o.mu.Lock()
	for ch := range o.subscribers {
		close(ch)
	}
	o.subscribers = make(map[chan domain.Message]bool)
	o.mu.Unlock()

	logger.Info("Orchestrator shutdown complete")
	return nil
}
