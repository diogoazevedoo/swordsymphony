package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// WorkflowDefinition represents a complete workflow definition
type WorkflowDefinition struct {
	ID          string          `json:"id" yaml:"id"`
	Name        string          `json:"name" yaml:"name"`
	Description string          `json:"description" yaml:"description"`
	Version     string          `json:"version" yaml:"version"`
	Steps       []WorkflowStep  `json:"steps" yaml:"steps"`
	Connections []Connection    `json:"connections" yaml:"connections"`
	InputSchema json.RawMessage `json:"input_schema,omitempty" yaml:"input_schema,omitempty"`
}

// WorkflowStep represents a single step in a workflow
type WorkflowStep struct {
	ID          string         `json:"id" yaml:"id"`
	Name        string         `json:"name" yaml:"name"`
	Type        string         `json:"type" yaml:"type"`
	AgentType   string         `json:"agent_type" yaml:"agent_type"`
	Config      map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
	Condition   string         `json:"condition,omitempty" yaml:"condition,omitempty"`
	Parallel    bool           `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	MaxRetries  int            `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	TimeoutSecs int            `json:"timeout_secs,omitempty" yaml:"timeout_secs,omitempty"`
}

// Connection represents a connection between workflow steps
type Connection struct {
	From    string `json:"from" yaml:"from"`
	To      string `json:"to" yaml:"to"`
	OnEvent string `json:"on_event,omitempty" yaml:"on_event,omitempty"`
}

// WorkflowInstance represents a running instance of a workflow
type WorkflowInstance struct {
	ID             uuid.UUID      `json:"id"`
	WorkflowID     string         `json:"workflow_id"`
	Status         string         `json:"status"`
	CurrentSteps   []string       `json:"current_steps"`
	CompletedSteps []string       `json:"completed_steps"`
	StepResults    map[string]any `json:"step_results"`
	Input          map[string]any `json:"input"`
	Output         map[string]any `json:"output"`
	Errors         []string       `json:"errors"`
	TaskID         uuid.UUID      `json:"task_id"`
	ThreadID       uuid.UUID      `json:"thread_id"`
	StartTime      int64          `json:"start_time"`
	EndTime        int64          `json:"end_time"`
}

// WorkflowEngine manages workflow definitions and instances
type WorkflowEngine struct {
	definitions     map[string]WorkflowDefinition
	instances       map[uuid.UUID]*WorkflowInstance
	orchestratorRef any
	mu              sync.RWMutex
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		definitions: make(map[string]WorkflowDefinition),
		instances:   make(map[uuid.UUID]*WorkflowInstance),
	}
}

// LoadWorkflowDefinition loads a workflow definition from a file
func LoadWorkflowDefinition(filePath string) (WorkflowDefinition, error) {
	var workflow WorkflowDefinition

	data, err := os.ReadFile(filePath)
	if err != nil {
		return workflow, fmt.Errorf("failed to read workflow file: %w", err)
	}

	ext := filepath.Ext(filePath)
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &workflow); err != nil {
			return workflow, fmt.Errorf("failed to parse JSON workflow: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &workflow); err != nil {
			return workflow, fmt.Errorf("failed to parse YAML workflow: %w", err)
		}
	default:
		return workflow, fmt.Errorf("unsupported workflow format: %s", ext)
	}

	if workflow.ID == "" {
		return workflow, fmt.Errorf("workflow ID is required")
	}
	if len(workflow.Steps) == 0 {
		return workflow, fmt.Errorf("workflow must have at least one step")
	}

	return workflow, nil
}

// RegisterWorkflow adds a workflow definition to the engine
func (e *WorkflowEngine) RegisterWorkflow(workflow WorkflowDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.definitions[workflow.ID]; exists {
		return fmt.Errorf("workflow with ID %s already exists", workflow.ID)
	}

	e.definitions[workflow.ID] = workflow
	logger.Info("Workflow registered", "id", workflow.ID, "name", workflow.Name)

	return nil
}

// SetOrchestrator sets the orchestrator reference for the workflow engine
func (e *WorkflowEngine) SetOrchestrator(orchestrator any) {
	e.orchestratorRef = orchestrator
}

// StartWorkflow creates and starts a new workflow instance
func (e *WorkflowEngine) StartWorkflow(ctx context.Context, workflowID string, input map[string]any) (*WorkflowInstance, error) {
	e.mu.RLock()
	workflow, exists := e.definitions[workflowID]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow with ID %s not found", workflowID)
	}

	instance := &WorkflowInstance{
		ID:             uuid.New(),
		WorkflowID:     workflow.ID,
		Status:         "started",
		CurrentSteps:   []string{},
		CompletedSteps: []string{},
		StepResults:    make(map[string]any),
		Input:          input,
		Output:         make(map[string]any),
		Errors:         []string{},
		StartTime:      time.Now().UnixNano(),
	}

	e.mu.Lock()
	e.instances[instance.ID] = instance
	e.mu.Unlock()

	go e.executeWorkflow(ctx, instance)

	return instance, nil
}

// GetWorkflowInstance retrieves a workflow instance by ID
func (e *WorkflowEngine) GetWorkflowInstance(instanceID uuid.UUID) (*WorkflowInstance, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	instance, exists := e.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("workflow instance with ID %s not found", instanceID)
	}

	return instance, nil
}

// executeWorkflow runs a workflow instance
func (e *WorkflowEngine) executeWorkflow(ctx context.Context, instance *WorkflowInstance) {
	// TODO: Implement workflow execution logic

	logger.Info("Workflow execution started", "instance_id", instance.ID, "workflow_id", instance.WorkflowID)

	e.mu.Lock()
	instance.Status = "completed"
	instance.EndTime = time.Now().UnixNano()
	e.mu.Unlock()

	logger.Info("Workflow execution completed", "instance_id", instance.ID, "workflow_id", instance.WorkflowID)
}
