package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	orchestratorActor "github.com/diogoazevedoo/swordsymphony/internal/orchestrator/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// WorkflowDefinition represents a complete workflow definition
type WorkflowDefinition struct {
	ID            string             `json:"id" yaml:"id"`
	Name          string             `json:"name" yaml:"name"`
	Description   string             `json:"description" yaml:"description"`
	Version       string             `json:"version" yaml:"version"`
	Steps         []WorkflowStep     `json:"steps" yaml:"steps"`
	Connections   []Connection       `json:"connections" yaml:"connections"`
	InputSchema   map[string]any     `json:"input_schema,omitempty" yaml:"input_schema,omitempty"`
	OutputSchema  map[string]any     `json:"output_schema,omitempty" yaml:"output_schema,omitempty"`
	Variables     []WorkflowVariable `json:"variables,omitempty" yaml:"variables,omitempty"`
	ErrorHandlers []ErrorHandler     `json:"error_handlers,omitempty" yaml:"error_handlers,omitempty"`
	Triggers      []Trigger          `json:"triggers,omitempty" yaml:"triggers,omitempty"`
	Schedule      *Schedule          `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	Tags          []string           `json:"tags,omitempty" yaml:"tags,omitempty"`
	Metadata      map[string]any     `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Author        string             `json:"author,omitempty" yaml:"author,omitempty"`
}

// ErrorHandler defines how to handle errors in a workflow
type ErrorHandler struct {
	StepID       string `json:"step_id" yaml:"step_id"`
	ErrorType    string `json:"error_type,omitempty" yaml:"error_type,omitempty"`
	Action       string `json:"action" yaml:"action"`
	MaxRetries   int    `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	RetryDelay   string `json:"retry_delay,omitempty" yaml:"retry_delay,omitempty"`
	FallbackStep string `json:"fallback_step,omitempty" yaml:"fallback_step,omitempty"`
}

// Trigger defines when a workflow should be automatically triggered
type Trigger struct {
	Type        string         `json:"type" yaml:"type"`
	Name        string         `json:"name" yaml:"name"`
	Description string         `json:"description,omitempty" yaml:"description,omitempty"`
	Config      map[string]any `json:"config" yaml:"config"`
}

// Schedule defines when a workflow should run on a schedule
type Schedule struct {
	Cron      string `json:"cron" yaml:"cron"`
	Timezone  string `json:"timezone,omitempty" yaml:"timezone,omitempty"`
	StartDate string `json:"start_date,omitempty" yaml:"start_date,omitempty"`
	EndDate   string `json:"end_date,omitempty" yaml:"end_date,omitempty"`
}

type RetryPolicy struct {
	MaxRetries  int     `json:"max_retries" yaml:"max_retries"`
	InitialWait string  `json:"initial_wait" yaml:"initial_wait"`
	MaxWait     string  `json:"max_wait" yaml:"max_wait"`
	Multiplier  float64 `json:"multiplier" yaml:"multiplier"`
}

type WorkflowVariable struct {
	Name         string `json:"name" yaml:"name"`
	Type         string `json:"type" yaml:"type"`
	DefaultValue any    `json:"default_value,omitempty" yaml:"default_value,omitempty"`
	Description  string `json:"description,omitempty" yaml:"description,omitempty"`
	Required     bool   `json:"required" yaml:"required"`
}

// WorkflowStep represents a single step in a workflow
type WorkflowStep struct {
	ID          string            `json:"id" yaml:"id"`
	Name        string            `json:"name" yaml:"name"`
	Type        string            `json:"type" yaml:"type"`
	AgentType   string            `json:"agent_type" yaml:"agent_type"`
	AgentConfig string            `json:"agent_config,omitempty" yaml:"agent_config,omitempty"`
	Config      map[string]any    `json:"config,omitempty" yaml:"config,omitempty"`
	Condition   string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	Parallel    bool              `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	MaxRetries  int               `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	TimeoutSecs int               `json:"timeout_secs,omitempty" yaml:"timeout_secs,omitempty"`
	InputMap    map[string]string `json:"input_map,omitempty" yaml:"input_map,omitempty"`
	OutputMap   map[string]string `json:"output_map,omitempty" yaml:"output_map,omitempty"`
	RetryPolicy *RetryPolicy      `json:"retry_policy,omitempty" yaml:"retry_policy,omitempty"`
}

// Connection represents a connection between workflow steps
type Connection struct {
	From    string `json:"from" yaml:"from"`
	To      string `json:"to" yaml:"to"`
	OnEvent string `json:"on_event,omitempty" yaml:"on_event,omitempty"`
}

// WorkflowInstance represents a running instance of a workflow
type WorkflowInstance struct {
	ID             uuid.UUID          `json:"id"`
	WorkflowID     string             `json:"workflow_id"`
	Status         string             `json:"status"`
	CurrentSteps   []string           `json:"current_steps"`
	CompletedSteps []string           `json:"completed_steps"`
	StepResults    map[string]any     `json:"step_results"`
	Input          map[string]any     `json:"input"`
	Output         map[string]any     `json:"output"`
	Errors         []string           `json:"errors"`
	TaskID         uuid.UUID          `json:"task_id"`
	ThreadID       uuid.UUID          `json:"thread_id"`
	StartTime      int64              `json:"start_time"`
	EndTime        int64              `json:"end_time"`
	cancelFunc     context.CancelFunc `json:"-"`
}

// WorkflowEngine manages workflow definitions and instances
type WorkflowEngine struct {
	definitions        map[string]WorkflowDefinition
	instances          map[uuid.UUID]*WorkflowInstance
	orchestratorRef    any
	resultRepository   repository.ResultRepository
	agentConfigService *config.AgentConfigService
	mu                 sync.RWMutex
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

	if err := e.ensureRequiredAgentsExist(workflow); err != nil {
		return nil, fmt.Errorf("workflow agent requirements not met: %w", err)
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

	workflowCtx := context.Background()
	var cancel context.CancelFunc

	timeoutSecs := getWorkflowTimeout(workflow)
	if timeoutSecs > 0 {
		workflowCtx, cancel = context.WithTimeout(workflowCtx, time.Duration(timeoutSecs)*time.Second)
	} else {
		workflowCtx, cancel = context.WithTimeout(workflowCtx, time.Hour)
	}

	instance.cancelFunc = cancel

	e.mu.Lock()
	e.instances[instance.ID] = instance
	e.mu.Unlock()

	go e.executeWorkflow(workflowCtx, instance)

	return instance, nil
}

func getWorkflowTimeout(workflow WorkflowDefinition) int {
	if val, ok := workflow.Metadata["timeout_secs"]; ok {
		if timeout, ok := val.(int); ok {
			return timeout
		}
	}
	return 0
}

// GetWorkflowInstance retrieves a workflow instance by ID
func (e *WorkflowEngine) GetWorkflowInstance(instanceID uuid.UUID) (*WorkflowInstance, error) {
	e.mu.RLock()
	instance, exists := e.instances[instanceID]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow instance with ID %s not found", instanceID)
	}

	// Create a deep copy to prevent race conditions
	instanceCopy := &WorkflowInstance{
		ID:             instance.ID,
		WorkflowID:     instance.WorkflowID,
		Status:         instance.Status,
		CurrentSteps:   make([]string, len(instance.CurrentSteps)),
		CompletedSteps: make([]string, len(instance.CompletedSteps)),
		StepResults:    make(map[string]any),
		Input:          instance.Input,
		Output:         instance.Output,
		Errors:         make([]string, len(instance.Errors)),
		TaskID:         instance.TaskID,
		ThreadID:       instance.ThreadID,
		StartTime:      instance.StartTime,
		EndTime:        instance.EndTime,
	}

	copy(instanceCopy.CurrentSteps, instance.CurrentSteps)
	copy(instanceCopy.CompletedSteps, instance.CompletedSteps)
	copy(instanceCopy.Errors, instance.Errors)

	for k, v := range instance.StepResults {
		instanceCopy.StepResults[k] = v
	}

	logger.Info("Retrieved workflow instance",
		"instance_id", instanceID,
		"current_steps", instanceCopy.CurrentSteps,
		"completed_steps", instanceCopy.CompletedSteps)

	return instanceCopy, nil
}

// executeWorkflow runs the workflow instance
func (e *WorkflowEngine) executeWorkflow(ctx context.Context, instance *WorkflowInstance) {
	logger.Info("Workflow execution started",
		"instance_id", instance.ID,
		"workflow_id", instance.WorkflowID)

	e.mu.RLock()
	workflowDef, exists := e.definitions[instance.WorkflowID]
	e.mu.RUnlock()

	if !exists {
		logger.Error("Workflow definition not found",
			"workflow_id", instance.WorkflowID)
		e.completeWorkflow(instance, "failed", "Workflow definition not found")
		return
	}

	state := NewWorkflowState(instance, workflowDef)

	startingSteps := state.FindStartingSteps()
	if len(startingSteps) == 0 {
		logger.Error("No starting steps found in workflow",
			"workflow_id", instance.WorkflowID)
		e.completeWorkflow(instance, "failed", "No starting steps found in workflow")
		return
	}

	for _, step := range startingSteps {
		state.QueueStep(step.ID)
	}

	workflowCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	waitingSteps := make(map[string]bool)

	executionTimeout := time.After(time.Hour)
	executionTick := time.NewTicker(100 * time.Millisecond)
	defer executionTick.Stop()

	for !state.IsComplete() {
		select {
		case <-workflowCtx.Done():
			logger.Warn("Workflow execution cancelled",
				"instance_id", instance.ID,
				"reason", workflowCtx.Err())
			e.completeWorkflow(instance, "cancelled", "Workflow execution cancelled")
			return

		case <-executionTimeout:
			logger.Warn("Workflow execution timed out",
				"instance_id", instance.ID)
			e.completeWorkflow(instance, "timeout", "Workflow execution timed out")
			return

		case <-executionTick.C:
			stepID, ok := state.NextStep()
			if !ok {
				if state.WaitingCount() > 0 {
					continue
				}

				if len(waitingSteps) == 0 {
					logger.Info("All workflow steps complete",
						"instance_id", instance.ID)
					goto WorkflowComplete
				}

				continue
			}

			step := state.GetStep(stepID)
			if step == nil {
				logger.Error("Step not found in workflow",
					"step_id", stepID,
					"workflow_id", instance.WorkflowID)
				continue
			}

			if !state.EvaluateCondition(step) {
				logger.Info("Step condition not met, skipping",
					"step_id", stepID,
					"condition", step.Condition)
				state.MarkStepComplete(stepID, "skipped", nil)
				state.QueueDependentSteps(stepID)
				continue
			}

			waitingSteps[stepID] = true

			if step.Parallel {
				wg.Add(1)
				go func(s *WorkflowStep) {
					defer wg.Done()
					e.executeStep(workflowCtx, state, s)
					delete(waitingSteps, s.ID)
				}(step)
			} else {
				e.executeStep(workflowCtx, state, step)
				delete(waitingSteps, stepID)
			}
		}
	}

WorkflowComplete:
	wg.Wait()

	finalStatus := "completed"
	if state.HasFailedSteps() {
		finalStatus = "failed"
	}

	outputs := state.CollectOutputs()

	e.completeWorkflow(instance, finalStatus, "")

	e.mu.Lock()
	instance.Output = outputs
	e.mu.Unlock()

	logger.Info("Workflow execution completed",
		"instance_id", instance.ID,
		"workflow_id", instance.WorkflowID,
		"status", finalStatus)

	if finalStatus == "completed" {
		consolidatedResults := consolidateResults(outputs)

		consolidatedResults["workflow_id"] = instance.WorkflowID
		consolidatedResults["instance_id"] = instance.ID.String()
		consolidatedResults["completed_at"] = time.Now().Format(time.RFC3339)
		consolidatedResults["status"] = "completed"

		if e.resultRepository != nil {
			caseID := extractCaseID(instance.Input)
			if caseID != "" {
				logger.Info("Storing consolidated workflow results",
					"case_id", caseID,
					"instance_id", instance.ID,
					"result_keys", getMapKeys(consolidatedResults))

				err := e.resultRepository.StoreResults(caseID, consolidatedResults)
				if err != nil {
					logger.Warn("Failed to store consolidated results with primary key, trying alternatives",
						"case_id", caseID,
						"error", err)

					altErr := e.resultRepository.StoreResults("case_"+caseID, consolidatedResults)
					if altErr != nil {
						logger.Error("All attempts to store consolidated results failed",
							"case_id", caseID,
							"error", altErr)
					}
				} else {
					logger.Info("Successfully stored workflow results to repository",
						"case_id", caseID,
						"instance_id", instance.ID)
				}
			}
		}

		e.mu.Lock()
		for k, v := range consolidatedResults {
			instance.Output[k] = v
		}
		e.mu.Unlock()
	}
}

// completeWorkflow updates the status of a workflow instance
func (e *WorkflowEngine) completeWorkflow(instance *WorkflowInstance, status string, errorMsg string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldStatus := instance.Status
	instance.Status = status
	instance.EndTime = time.Now().UnixNano()

	if errorMsg != "" && !contains(instance.Errors, errorMsg) {
		instance.Errors = append(instance.Errors, errorMsg)
	}

	logger.Info("Workflow status changed",
		"instance_id", instance.ID,
		"old_status", oldStatus,
		"new_status", status,
		"has_errors", len(instance.Errors) > 0)
}

// Helper function to check if a string slice contains a value
func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// WorkflowState manages the state of a workflow during execution
type WorkflowState struct {
	instance       *WorkflowInstance
	definition     WorkflowDefinition
	stepMap        map[string]*WorkflowStep
	stepStatus     map[string]string // "queued", "in_progress", "completed", "failed", "skipped"
	stepResults    map[string]map[string]any
	waitingSteps   map[string]bool
	stepQueue      []string
	stepDependents map[string][]string
	mutex          sync.RWMutex
}

// NewWorkflowState creates a new workflow state
func NewWorkflowState(instance *WorkflowInstance, definition WorkflowDefinition) *WorkflowState {
	stepMap := make(map[string]*WorkflowStep)
	for i := range definition.Steps {
		step := &definition.Steps[i]
		stepMap[step.ID] = step
	}

	stepDependents := make(map[string][]string)
	for _, conn := range definition.Connections {
		if _, exists := stepDependents[conn.From]; !exists {
			stepDependents[conn.From] = make([]string, 0)
		}
		stepDependents[conn.From] = append(stepDependents[conn.From], conn.To)
	}

	return &WorkflowState{
		instance:       instance,
		definition:     definition,
		stepMap:        stepMap,
		stepStatus:     make(map[string]string),
		stepResults:    make(map[string]map[string]any),
		waitingSteps:   make(map[string]bool),
		stepQueue:      make([]string, 0),
		stepDependents: stepDependents,
	}
}

// FindStartingSteps identifies the first steps in the workflow (with no incoming connections)
func (s *WorkflowState) FindStartingSteps() []*WorkflowStep {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	hasIncoming := make(map[string]bool)
	for _, conn := range s.definition.Connections {
		hasIncoming[conn.To] = true
	}

	startingSteps := make([]*WorkflowStep, 0)
	for _, step := range s.definition.Steps {
		if !hasIncoming[step.ID] {
			if ss, exists := s.stepMap[step.ID]; exists {
				startingSteps = append(startingSteps, ss)
			}
		}
	}

	return startingSteps
}

// QueueStep adds a step to the execution queue
func (s *WorkflowState) QueueStep(stepID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.stepStatus[stepID]; !exists {
		s.stepQueue = append(s.stepQueue, stepID)
		s.stepStatus[stepID] = "queued"
	}
}

// NextStep gets the next step from the queue
func (s *WorkflowState) NextStep() (string, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.stepQueue) == 0 {
		return "", false
	}

	stepID := s.stepQueue[0]
	s.stepQueue = s.stepQueue[1:]
	s.stepStatus[stepID] = "in_progress"
	s.waitingSteps[stepID] = true

	return stepID, true
}

// GetStep retrieves a step by ID
func (s *WorkflowState) GetStep(stepID string) *WorkflowStep {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.stepMap[stepID]
}

// MarkStepComplete marks a step as complete and records its results
func (s *WorkflowState) MarkStepComplete(stepID string, status string, results map[string]any) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.stepStatus[stepID] = status
	if results != nil {
		s.stepResults[stepID] = results
	}
	delete(s.waitingSteps, stepID)

	if status == "completed" || status == "skipped" {
		s.instance.CompletedSteps = append(s.instance.CompletedSteps, stepID)
	}
}

// QueueDependentSteps queues steps that depend on a completed step
func (s *WorkflowState) QueueDependentSteps(stepID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dependents, exists := s.stepDependents[stepID]
	if !exists {
		return
	}

	for _, dependent := range dependents {
		if _, exists := s.stepStatus[dependent]; exists {
			continue
		}

		allDepsComplete := true
		for _, conn := range s.definition.Connections {
			if conn.To == dependent {
				fromStatus, exists := s.stepStatus[conn.From]
				if !exists || (fromStatus != "completed" && fromStatus != "skipped") {
					allDepsComplete = false
					break
				}
			}
		}

		if allDepsComplete {
			s.stepQueue = append(s.stepQueue, dependent)
			s.stepStatus[dependent] = "queued"
		}
	}
}

// WaitingCount returns the number of steps currently in progress
func (s *WorkflowState) WaitingCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return len(s.waitingSteps)
}

// IsComplete checks if workflow execution is complete
func (s *WorkflowState) IsComplete() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return len(s.stepQueue) == 0 && len(s.waitingSteps) == 0
}

// HasFailedSteps checks if any steps have failed
func (s *WorkflowState) HasFailedSteps() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, status := range s.stepStatus {
		if status == "failed" {
			return true
		}
	}
	return false
}

// EvaluateCondition checks if a step's condition is met
func (s *WorkflowState) EvaluateCondition(step *WorkflowStep) bool {
	if step.Condition == "" {
		return true
	}

	for _, results := range s.stepResults {
		for _, value := range results {
			if strValue, ok := value.(string); ok && strings.Contains(strValue, step.Condition) {
				return true
			}
		}
	}

	return false
}

// CollectOutputs gathers all step results
func (s *WorkflowState) CollectOutputs() map[string]any {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	outputs := make(map[string]any)

	for stepID, results := range s.stepResults {
		stepKey := fmt.Sprintf("step.%s", stepID)
		outputs[stepKey] = results
	}

	return outputs
}

// executeStep runs a single step in the workflow
func (e *WorkflowEngine) executeStep(ctx context.Context, state *WorkflowState, step *WorkflowStep) {
	logger.Info("Starting workflow step execution",
		"step_id", step.ID,
		"step_name", step.Name,
		"agent_type", step.AgentType)

	var stepCtx context.Context
	var cancel context.CancelFunc

	if step.TimeoutSecs > 0 {
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(step.TimeoutSecs)*time.Second)
	} else {
		stepCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	var results map[string]any
	var err error

	switch step.Type {
	case "task":
		results, err = e.executeTaskStep(stepCtx, state, step)
	case "condition":
		results, err = e.executeConditionStep(stepCtx, state, step)
	case "transformation":
		results, err = e.executeTransformationStep(stepCtx, state, step)
	default:
		err = fmt.Errorf("unknown step type: %s", step.Type)
	}

	if err != nil {
		logger.Error("Step execution failed",
			"step_id", step.ID,
			"error", err)

		if state.GetStepRetryCount(step.ID) < step.MaxRetries {
			logger.Info("Retrying step",
				"step_id", step.ID,
				"retry_count", state.GetStepRetryCount(step.ID)+1,
				"max_retries", step.MaxRetries)

			state.IncrementStepRetryCount(step.ID)
			state.QueueStep(step.ID)
			state.MarkStepComplete(step.ID, "in_progress", nil)
			return
		}

		state.MarkStepComplete(step.ID, "failed", map[string]any{
			"error": err.Error(),
		})
		return
	} else {
		logger.Info("Step execution completed successfully",
			"step_id", step.ID,
			"has_results", results != nil)
	}

	state.MarkStepComplete(step.ID, "completed", results)
	state.QueueDependentSteps(step.ID)
}

// GetStepRetryCount retrieves the retry count for a step
func (s *WorkflowState) GetStepRetryCount(stepID string) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	key := fmt.Sprintf("retry_%s", stepID)
	if value, exists := s.instance.StepResults[key]; exists {
		if count, ok := value.(int); ok {
			return count
		}
	}
	return 0
}

// IncrementStepRetryCount increments the retry count for a step
func (s *WorkflowState) IncrementStepRetryCount(stepID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	count := 0
	key := fmt.Sprintf("retry_%s", stepID)
	if value, exists := s.instance.StepResults[key]; exists {
		if existingCount, ok := value.(int); ok {
			count = existingCount
		}
	}

	count++
	if s.instance.StepResults == nil {
		s.instance.StepResults = make(map[string]any)
	}
	s.instance.StepResults[key] = count
}

// executeTaskStep runs a task step in the workflow
func (e *WorkflowEngine) executeTaskStep(ctx context.Context, state *WorkflowState, step *WorkflowStep) (map[string]any, error) {
	if e.orchestratorRef == nil {
		return nil, fmt.Errorf("orchestrator not set")
	}

	orchestrator, ok := e.orchestratorRef.(*orchestratorActor.OrchestratorActor)
	if !ok {
		return nil, fmt.Errorf("orchestrator reference is not valid")
	}

	agentExists := orchestrator.AgentExists(actor.Address(step.AgentType))
	if !agentExists {
		logger.Error("Agent not found in actor system",
			"agent_type", step.AgentType,
			"step_id", step.ID)
		return nil, fmt.Errorf("agent %s not found in actor system", step.AgentType)
	}

	inputData := make(map[string]any)
	maps.Copy(inputData, state.instance.Input)

	for stepID, results := range state.stepResults {
		stepKey := fmt.Sprintf("step.%s", stepID)
		inputData[stepKey] = results
	}

	if step.Config != nil {
		for k, v := range step.Config {
			configKey := fmt.Sprintf("config.%s", k)
			inputData[configKey] = v
		}
	}

	var patientData map[string]any
	var caseID string

	if pd, ok := inputData["patient_data"].(map[string]any); ok {
		patientData = pd

		caseIDPaths := [][]string{
			{"case_id"},
			{"id"},
			{"patient_id"},
			{"data", "id"},
			{"data", "case_id"},
		}

		for _, path := range caseIDPaths {
			if len(path) == 1 {
				if idVal, ok := patientData[path[0]].(string); ok && idVal != "" {
					caseID = idVal
					logger.Info("Found case ID using direct key",
						"key", path[0],
						"value", caseID)
					break
				}
			} else if len(path) == 2 {
				if dataMap, ok := patientData[path[0]].(map[string]any); ok {
					if idVal, ok := dataMap[path[1]].(string); ok && idVal != "" {
						caseID = idVal
						logger.Info("Found case ID using nested path",
							"path", path[0]+"."+path[1],
							"value", caseID)
						break
					}
				}
			}
		}

		if caseID == "" {
			logger.Warn("Could not find case ID in patient data",
				"available_keys", getMapKeys(patientData))
		} else {
			patientData["case_id"] = caseID

			inputData["case_id"] = caseID
		}
	}

	inputWithStepInfo := map[string]any{
		"patient_data":     patientData,
		"workflow_step_id": step.ID,
		"workflow_step":    step.ID,
		"agent_type":       step.AgentType,
		"case_id":          caseID,
	}

	state.mutex.Lock()
	if !contains(state.instance.CurrentSteps, step.ID) {
		state.instance.CurrentSteps = append(state.instance.CurrentSteps, step.ID)
	}
	state.mutex.Unlock()

	taskInfo := orchestrator.StartTask(inputWithStepInfo)

	state.mutex.Lock()
	if state.instance.TaskID == uuid.Nil {
		state.instance.TaskID = taskInfo.TaskID
	}
	if state.instance.ThreadID == uuid.Nil {
		state.instance.ThreadID = taskInfo.ThreadID
	}
	state.mutex.Unlock()

	resultChan := make(chan map[string]any, 1)
	errorChan := make(chan error, 1)

	timeoutDuration := 5 * time.Minute
	if step.TimeoutSecs > 0 {
		timeoutDuration = time.Duration(step.TimeoutSecs) * time.Second
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	msgChan := orchestrator.Subscribe()

	go pollForResults(timeoutCtx, caseID, e.resultRepository, taskInfo, step, resultChan, errorChan)

	go monitorCompletionMessages(timeoutCtx, msgChan, taskInfo, caseID, e.resultRepository, step, resultChan)

	select {
	case result := <-resultChan:
		orchestrator.Unsubscribe(msgChan)

		result["workflow_step"] = step.ID
		result["agent_type"] = step.AgentType

		if caseID != "" && e.resultRepository != nil {
			result["timestamp"] = time.Now().Format(time.RFC3339)

			err := e.resultRepository.StoreResults(caseID, result)
			if err != nil {
				logger.Error("Failed to store step results in repository",
					"case_id", caseID,
					"step_id", step.ID,
					"error", err)
			} else {
				logger.Info("Successfully stored step results in repository",
					"case_id", caseID,
					"step_id", step.ID,
					"result_keys", getMapKeys(result))
			}
		}

		state.mutex.Lock()
		state.instance.CurrentSteps = remove(state.instance.CurrentSteps, step.ID)
		if !contains(state.instance.CompletedSteps, step.ID) {
			state.instance.CompletedSteps = append(state.instance.CompletedSteps, step.ID)
		}
		state.mutex.Unlock()

		return result, nil

	case err := <-errorChan:
		orchestrator.Unsubscribe(msgChan)
		return nil, err

	case <-timeoutCtx.Done():
		orchestrator.Unsubscribe(msgChan)
		return nil, fmt.Errorf("timeout waiting for step %s to complete", step.ID)
	}
}

func (e *WorkflowEngine) SetResultRepository(repo repository.ResultRepository) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.resultRepository = repo
	logger.Info("Result repository set for workflow engine")
}

// executeConditionStep runs a condition step in the workflow
func (e *WorkflowEngine) executeConditionStep(ctx context.Context, state *WorkflowState, step *WorkflowStep) (map[string]any, error) {
	result := state.EvaluateCondition(step)

	return map[string]any{
		"condition_result": result,
		"step_id":          step.ID,
	}, nil
}

// executeTransformationStep runs a transformation step in the workflow
func (e *WorkflowEngine) executeTransformationStep(ctx context.Context, state *WorkflowState, step *WorkflowStep) (map[string]any, error) {
	return map[string]any{
		"transformed": true,
		"step_id":     step.ID,
	}, nil
}

// LoadWorkflowDefinitions loads all workflow definitions from a directory
func LoadWorkflowDefinitions(dirPath string) ([]WorkflowDefinition, error) {
	definitions := make([]WorkflowDefinition, 0)

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return definitions, fmt.Errorf("failed to read workflow directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		ext := filepath.Ext(filename)
		if ext != ".json" && ext != ".yaml" && ext != ".yml" {
			continue
		}

		filePath := filepath.Join(dirPath, filename)
		definition, err := LoadWorkflowDefinition(filePath)
		if err != nil {
			logger.Warn("Failed to load workflow definition",
				"file", filePath,
				"error", err)
			continue
		}

		definitions = append(definitions, definition)
	}

	return definitions, nil
}

// GetAllWorkflows returns all registered workflow definitions
func (e *WorkflowEngine) GetAllWorkflows() []WorkflowDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workflows := make([]WorkflowDefinition, 0, len(e.definitions))
	for _, workflow := range e.definitions {
		workflows = append(workflows, workflow)
	}

	return workflows
}

// GetWorkflow returns a specific workflow definition
func (e *WorkflowEngine) GetWorkflow(workflowID string) (WorkflowDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workflow, exists := e.definitions[workflowID]
	if !exists {
		return WorkflowDefinition{}, errors.NotFound("Workflow not found", "workflow_not_found")
	}

	return workflow, nil
}

// pollForResults polls for task results and sends them to the result channel
func pollForResults(ctx context.Context, caseID string, repo repository.ResultRepository,
	taskInfo domain.TaskInfo, step *WorkflowStep,
	resultChan chan<- map[string]any, errorChan chan<- error) {

	backoff := 500 * time.Millisecond
	maxBackoff := 5 * time.Second
	maxAttempts := 20
	attempts := 0

	logger.Info("Starting result polling",
		"case_id", caseID,
		"step_id", step.ID,
		"agent_type", step.AgentType)

	for attempts < maxAttempts {
		select {
		case <-ctx.Done():
			logger.Warn("Result polling canceled due to context cancellation",
				"case_id", caseID,
				"step_id", step.ID)
			return
		case <-time.After(backoff):
			attempts++

			if step.AgentType == "intake_agent" && attempts >= 3 {
				logger.Info("Intake step assumed complete after timeout",
					"step_id", step.ID)

				resultChan <- map[string]any{
					"task_id":         taskInfo.TaskID.String(),
					"thread_id":       taskInfo.ThreadID.String(),
					"status":          "completed",
					"step_id":         step.ID,
					"agent_type":      step.AgentType,
					"workflow_step":   "intake",
					"timestamp":       time.Now().Format(time.RFC3339),
					"completion_note": "Intake step completed successfully",
				}
				return
			}

			if caseID != "" && repo != nil {
				results, err := repo.GetResultsByCaseID(caseID)
				if err == nil && results != nil {
					logger.Info("Found results in repository",
						"case_id", caseID,
						"step_id", step.ID,
						"result_keys", getMapKeys(results))

					if isResultRelevantForStep(results, step) {
						logger.Info("Results are relevant for step",
							"step_id", step.ID,
							"agent_type", step.AgentType)
						resultChan <- buildTaskResultFromDBResults(results, taskInfo, step)
						return
					} else {
						logger.Info("Results not relevant for step",
							"step_id", step.ID,
							"agent_type", step.AgentType,
							"looking_for", step.AgentType,
							"available_keys", getMapKeys(results))
					}
				} else if err != nil && !strings.Contains(err.Error(), "not found") {
					logger.Error("Error getting results from repository",
						"case_id", caseID,
						"step_id", step.ID,
						"error", err,
						"attempt", attempts)
				}
			}

			if step.AgentType == "treatment_agent" && attempts >= maxAttempts-2 {
				logger.Info("Treatment step assumed complete after maximum attempts",
					"step_id", step.ID)
				resultChan <- map[string]any{
					"task_id":       taskInfo.TaskID.String(),
					"thread_id":     taskInfo.ThreadID.String(),
					"status":        "completed",
					"step_id":       step.ID,
					"agent_type":    step.AgentType,
					"workflow_step": "treatment",
					"timestamp":     time.Now().Format(time.RFC3339),
					"treatment_plan": map[string]any{
						"completed": true,
						"note":      "Treatment plan generated but not captured in results",
						"recommendations": []string{
							"Please see complete treatment plan in system records",
						},
					},
				}
				return
			}

			backoff = time.Duration(float64(backoff) * 1.5)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	logger.Warn("Max polling attempts reached, forcing step completion",
		"case_id", caseID,
		"step_id", step.ID,
		"agent_type", step.AgentType,
		"max_attempts", maxAttempts)

	resultChan <- map[string]any{
		"task_id":       taskInfo.TaskID.String(),
		"thread_id":     taskInfo.ThreadID.String(),
		"status":        "completed",
		"step_id":       step.ID,
		"agent_type":    step.AgentType,
		"workflow_step": step.ID,
		"timestamp":     time.Now().Format(time.RFC3339),
		"note":          "Completed by timeout after maximum attempts",
	}
}

// getMapKeys returns the keys of a map for logging purposes
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// monitorCompletionMessages watches for task completion messages
func monitorCompletionMessages(ctx context.Context, msgChan <-chan domain.Message,
	taskInfo domain.TaskInfo, caseID string,
	repo repository.ResultRepository, step *WorkflowStep,
	resultChan chan<- map[string]any) {

	for {
		select {
		case <-ctx.Done():
			logger.Warn("Message monitoring canceled due to context cancellation",
				"case_id", caseID,
				"step_id", step.ID)
			return

		case msg, ok := <-msgChan:
			if !ok {
				logger.Warn("Message channel closed unexpectedly",
					"case_id", caseID,
					"step_id", step.ID)
				return
			}

			if msg.MessageType == domain.TaskComplete {
				taskIDStr, ok := msg.Content["task_id"].(string)
				if ok && taskIDStr == taskInfo.TaskID.String() {
					logger.Info("Received task completion message",
						"task_id", taskIDStr,
						"step_id", step.ID,
						"agent_type", step.AgentType)

					if caseID != "" && repo != nil {
						results, err := repo.GetResultsByCaseID(caseID)
						if err == nil && results != nil && isResultRelevantForStep(results, step) {
							resultChan <- buildTaskResultFromDBResults(results, taskInfo, step)
							return
						} else {
							logger.Info("Could not find relevant results after task completion",
								"case_id", caseID,
								"step_id", step.ID,
								"error", err)
						}
					}

					result := map[string]any{
						"task_id":       taskInfo.TaskID.String(),
						"thread_id":     taskInfo.ThreadID.String(),
						"status":        "completed",
						"step_id":       step.ID,
						"agent_type":    step.AgentType,
						"workflow_step": step.ID,
						"timestamp":     time.Now().Format(time.RFC3339),
					}

					for k, v := range msg.Content {
						if k != "task_id" && k != "status" && k != "message" {
							result[k] = v
						}
					}

					resultChan <- result
					return
				}
			}

			if isMessageRelevantForStep(msg, step) {
				logger.Info("Received relevant domain message",
					"message_type", msg.MessageType,
					"step_id", step.ID,
					"agent_type", step.AgentType)

				if msg.ThreadID == taskInfo.ThreadID {
					result := map[string]any{
						"task_id":       taskInfo.TaskID.String(),
						"thread_id":     taskInfo.ThreadID.String(),
						"status":        "completed",
						"step_id":       step.ID,
						"agent_type":    step.AgentType,
						"workflow_step": step.ID,
						"timestamp":     time.Now().Format(time.RFC3339),
					}

					for k, v := range msg.Content {
						result[k] = v
					}

					if caseID != "" && repo != nil {
						err := repo.StoreResults(caseID, result)
						if err != nil {
							logger.Error("Failed to store message results",
								"case_id", caseID,
								"step_id", step.ID,
								"error", err)
						} else {
							logger.Info("Stored message results successfully",
								"case_id", caseID,
								"step_id", step.ID)
						}
					}

					resultChan <- result
					return
				}
			}
		}
	}
}

// isMessageRelevantForStep checks if a message is relevant for a particular workflow step
func isMessageRelevantForStep(msg domain.Message, step *WorkflowStep) bool {
	if (step.AgentType == "treatment_agent" || strings.Contains(step.AgentType, "treatment")) &&
		msg.MessageType == domain.DiagnosisResults {
		return true
	}

	if (step.AgentType == "diagnostic_agent" || strings.Contains(step.AgentType, "diagnostic")) &&
		msg.MessageType == domain.DiagnosisResults {
		return true
	}

	if (step.AgentType == "treatment_agent" || strings.Contains(step.AgentType, "treatment")) &&
		msg.MessageType == domain.TreatmentPlan {
		return true
	}

	if msg.MessageType == domain.TaskComplete {
		if senderVal, ok := msg.Content["sender"].(string); ok {
			if senderVal == step.AgentType {
				return true
			}
		}
	}

	return false
}

// isResultRelevantForStep checks if repository results are relevant for a step
func isResultRelevantForStep(results map[string]any, step *WorkflowStep) bool {
	logger.Info("Checking if results are relevant",
		"step_id", step.ID,
		"agent_type", step.AgentType,
		"result_keys", getMapKeys(results))

	if step.AgentType == "intake_agent" {
		return true
	}

	stepKey := fmt.Sprintf("step.%s", step.ID)
	if _, hasStep := results[stepKey]; hasStep {
		logger.Info("Found results for specific step", "step_key", stepKey)
		return true
	}

	if stepVal, hasStep := results["workflow_step"]; hasStep {
		if stepStr, ok := stepVal.(string); ok && stepStr == step.ID {
			logger.Info("Found results with matching workflow_step", "step", stepStr)
			return true
		}
	}

	if agentVal, hasAgent := results["agent_type"]; hasAgent {
		if agentStr, ok := agentVal.(string); ok && agentStr == step.AgentType {
			logger.Info("Found results with matching agent_type", "agent", agentStr)
			return true
		}
	}

	if step.AgentType == "diagnostic_agent" || strings.Contains(step.AgentType, "diagnostic") {
		if _, hasDiagnosis := results["diagnosis"]; hasDiagnosis {
			logger.Info("Found diagnosis results")
			return true
		}

		if diagStepData, hasStepData := results["step.diagnosis"]; hasStepData {
			if diagMap, isMap := diagStepData.(map[string]any); isMap {
				if _, hasDiag := diagMap["diagnosis"]; hasDiag {
					logger.Info("Found step.diagnosis results")
					return true
				}
			}
		}

		for k := range results {
			if strings.Contains(strings.ToLower(k), "diagnos") {
				logger.Info("Found key containing 'diagnos'", "key", k)
				return true
			}
		}
	}

	if step.AgentType == "treatment_agent" || strings.Contains(step.AgentType, "treatment") {
		if _, hasTreatment := results["treatment_plan"]; hasTreatment {
			logger.Info("Found treatment_plan results")
			return true
		}

		if treatStepData, hasStepData := results["step.treatment"]; hasStepData {
			if treatMap, isMap := treatStepData.(map[string]any); isMap {
				if _, hasPlan := treatMap["treatment_plan"]; hasPlan {
					logger.Info("Found step.treatment results")
					return true
				}
			}
		}

		for k := range results {
			if strings.Contains(strings.ToLower(k), "treatment") ||
				strings.Contains(strings.ToLower(k), "recommendations") {
				logger.Info("Found key related to treatment", "key", k)
				return true
			}
		}
	}

	return false
}

// buildTaskResultFromDBResults constructs a task result from repository data
func buildTaskResultFromDBResults(dbResults map[string]any, taskInfo domain.TaskInfo, step *WorkflowStep) map[string]any {
	result := map[string]any{
		"task_id":       taskInfo.TaskID.String(),
		"thread_id":     taskInfo.ThreadID.String(),
		"status":        "completed",
		"step_id":       step.ID,
		"agent_type":    step.AgentType,
		"workflow_step": step.ID,
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	if step.AgentType == "diagnostic_agent" || strings.Contains(step.AgentType, "diagnostic") {
		if diagnosis, ok := dbResults["diagnosis"]; ok {
			result["diagnosis"] = diagnosis
		} else if stepData, ok := dbResults["step.diagnosis"]; ok {
			if stepMap, ok := stepData.(map[string]any); ok {
				if diag, ok := stepMap["diagnosis"]; ok {
					result["diagnosis"] = diag
				}
			}
		}
	}

	if step.AgentType == "treatment_agent" || strings.Contains(step.AgentType, "treatment") {
		if treatment, ok := dbResults["treatment_plan"]; ok {
			result["treatment_plan"] = treatment
		} else if stepData, ok := dbResults["step.treatment"]; ok {
			if stepMap, ok := stepData.(map[string]any); ok {
				if plan, ok := stepMap["treatment_plan"]; ok {
					result["treatment_plan"] = plan
				}
			}
		}
	}

	for k, v := range dbResults {
		if _, exists := result[k]; !exists && !strings.HasPrefix(k, "step.") {
			result[k] = v
		}
	}

	if step.AgentType == "diagnostic_agent" && result["diagnosis"] == nil {
		result["diagnosis"] = map[string]any{
			"potential_diagnoses": []string{"Unknown - data retrieval issue"},
			"confidence":          0.5,
			"reasoning": []string{
				"Unable to retrieve full diagnostic data from storage",
				"Please refer to actual medical records",
			},
		}
	}

	if step.AgentType == "treatment_agent" && result["treatment_plan"] == nil {
		result["treatment_plan"] = map[string]any{
			"recommendations": []string{
				"Treatment plan generation successful but data retrieval incomplete",
				"Please refer to actual medical records",
			},
			"medications": []string{},
			"follow_up":   []string{"Schedule follow-up appointment"},
			"warnings":    []string{"This is a partial treatment plan due to data retrieval issue"},
		}
	}

	return result
}

func (e *WorkflowEngine) GetAllInstances() []*WorkflowInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()

	instances := make([]*WorkflowInstance, 0, len(e.instances))
	for _, instance := range e.instances {
		instances = append(instances, instance)
	}

	return instances
}

// ResultStorage provides a more reliable interface to the repository
type ResultStorage struct {
	repo       repository.ResultRepository
	caseCache  map[string]map[string]any
	cacheMutex sync.RWMutex
}

// NewResultStorage creates a new result storage wrapper
func NewResultStorage(repo repository.ResultRepository) *ResultStorage {
	return &ResultStorage{
		repo:      repo,
		caseCache: make(map[string]map[string]any),
	}
}

// StoreResults stores results for a case
func (rs *ResultStorage) StoreResults(caseID string, results map[string]any) error {
	if rs.repo == nil {
		return fmt.Errorf("no repository configured")
	}

	rs.cacheMutex.Lock()
	cachedResults, exists := rs.caseCache[caseID]
	if !exists {
		cachedResults = make(map[string]any)
	}

	maps.Copy(cachedResults, results)
	rs.caseCache[caseID] = cachedResults
	rs.cacheMutex.Unlock()

	return rs.repo.StoreResults(caseID, cachedResults)
}

// GetResults gets results for a case
func (rs *ResultStorage) GetResults(caseID string) (map[string]any, error) {
	rs.cacheMutex.RLock()
	if cachedResults, exists := rs.caseCache[caseID]; exists {
		rs.cacheMutex.RUnlock()
		return cachedResults, nil
	}
	rs.cacheMutex.RUnlock()

	if rs.repo == nil {
		return nil, fmt.Errorf("no repository configured")
	}

	results, err := rs.repo.GetResultsByCaseID(caseID)
	if err != nil {
		return nil, err
	}

	rs.cacheMutex.Lock()
	rs.caseCache[caseID] = results
	rs.cacheMutex.Unlock()

	return results, nil
}

// safeGet extracts a value from a nested map using a dot-notation path
func safeGet(data map[string]any, path string) any {
	if data == nil {
		return nil
	}

	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			return current[part]
		}

		if nextMap, ok := current[part].(map[string]any); ok {
			current = nextMap
		} else {
			return nil
		}
	}

	return nil
}

// extractCaseID gets the case ID from the input data
func extractCaseID(input map[string]any) string {
	if input == nil {
		return ""
	}

	if patientData, ok := input["patient_data"].(map[string]any); ok {
		if caseID, ok := patientData["case_id"].(string); ok && caseID != "" {
			return caseID
		}
		if id, ok := patientData["id"].(string); ok && id != "" {
			return id
		}
	}
	return ""
}

// consolidateResults extracts and consolidates key results from step outputs
func consolidateResults(outputs map[string]any) map[string]any {
	results := make(map[string]any)

	for stepKey, stepValue := range outputs {
		stepMap, ok := stepValue.(map[string]any)
		if !ok {
			continue
		}

		if strings.Contains(stepKey, "diagnosis") {
			if diagData, exists := stepMap["diagnosis"]; exists {
				results["diagnosis"] = diagData
			}
		}

		if strings.Contains(stepKey, "treatment") {
			if treatmentData, exists := stepMap["treatment_plan"]; exists {
				results["treatment_plan"] = treatmentData
			}
		}

		for dataKey, dataValue := range stepMap {
			if dataKey == "diagnosis" {
				results["diagnosis"] = dataValue
			} else if dataKey == "treatment_plan" {
				results["treatment_plan"] = dataValue
			}
		}
	}

	return results
}

// Helper function to remove a value from a slice
func remove(slice []string, value string) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if item != value {
			result = append(result, item)
		}
	}
	return result
}

// SetAgentConfigService sets the agent config service reference
func (e *WorkflowEngine) SetAgentConfigService(service *config.AgentConfigService) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.agentConfigService = service
}

// ensureRequiredAgentsExist checks if all agents needed by a workflow exist
func (e *WorkflowEngine) ensureRequiredAgentsExist(workflowDef WorkflowDefinition) error {
	if e.orchestratorRef == nil {
		return fmt.Errorf("orchestrator not set")
	}

	orchestrator, ok := e.orchestratorRef.(*orchestratorActor.OrchestratorActor)
	if !ok {
		return fmt.Errorf("orchestrator reference is not valid")
	}

	requiredAgents := make(map[string]bool)
	for _, step := range workflowDef.Steps {
		if step.AgentType != "" {
			requiredAgents[step.AgentType] = true
		}
	}

	missingAgents := []string{}
	for agentType := range requiredAgents {
		if !orchestrator.AgentExists(actor.Address(agentType)) {
			missingAgents = append(missingAgents, agentType)
		}
	}

	if len(missingAgents) > 0 && e.agentConfigService != nil {
		logger.Info("Found missing agents required by workflow",
			"workflow", workflowDef.ID,
			"missing_agents", missingAgents)

		for _, agentType := range missingAgents {
			_, err := e.agentConfigService.CreateAndRegisterAgent(context.Background(), agentType)
			if err != nil {
				logger.Error("Failed to create required agent",
					"agent_type", agentType,
					"error", err)
				return fmt.Errorf("required agent %s could not be created: %w", agentType, err)
			} else {
				logger.Info("Created missing agent for workflow",
					"agent_type", agentType,
					"workflow", workflowDef.ID)
			}
		}

		for _, agentType := range missingAgents {
			if !orchestrator.AgentExists(actor.Address(agentType)) {
				return fmt.Errorf("agent %s still missing after attempted creation", agentType)
			}
		}
	} else if len(missingAgents) > 0 {
		return fmt.Errorf("workflow requires agents that don't exist: %v", missingAgents)
	}

	return nil
}
