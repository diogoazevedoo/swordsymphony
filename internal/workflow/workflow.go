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
	definitions      map[string]WorkflowDefinition
	instances        map[uuid.UUID]*WorkflowInstance
	orchestratorRef  any
	resultRepository repository.ResultRepository
	mu               sync.RWMutex
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

func (e *WorkflowEngine) TerminateWorkflow(instanceID uuid.UUID) error {
	e.mu.RLock()
	instance, exists := e.instances[instanceID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("workflow instance %s not found", instanceID)
	}

	if instance.cancelFunc != nil {
		instance.cancelFunc()
	}

	return nil
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
	defer e.mu.RUnlock()

	instance, exists := e.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("workflow instance with ID %s not found", instanceID)
	}

	return instance, nil
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

	for !state.IsComplete() {
		select {
		case <-workflowCtx.Done():
			logger.Warn("Workflow execution cancelled",
				"instance_id", instance.ID,
				"reason", workflowCtx.Err())
			e.completeWorkflow(instance, "cancelled", "Workflow execution cancelled")
			return
		default:
			stepID, ok := state.NextStep()
			if !ok {
				if state.WaitingCount() > 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				break
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

			if step.Parallel {
				wg.Add(1)
				go func(s *WorkflowStep) {
					defer wg.Done()
					e.executeStep(workflowCtx, state, s)
				}(step)
			} else {
				e.executeStep(workflowCtx, state, step)
			}
		}
	}

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
}

// completeWorkflow updates the status of a workflow instance
func (e *WorkflowEngine) completeWorkflow(instance *WorkflowInstance, status string, errorMsg string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	instance.Status = status
	instance.EndTime = time.Now().UnixNano()

	if errorMsg != "" && !contains(instance.Errors, errorMsg) {
		instance.Errors = append(instance.Errors, errorMsg)
	}
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
	logger.Info("Executing workflow step",
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

	inputData := make(map[string]any)
	for k, v := range state.instance.Input {
		inputData[k] = v
	}

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

		if caseIdVal, ok := patientData["case_id"].(string); ok && caseIdVal != "" {
			caseID = caseIdVal
		} else if idVal, ok := patientData["id"].(string); ok && idVal != "" {
			caseID = idVal
		}
	}

	inputWithStepInfo := map[string]any{
		"patient_data":     inputData,
		"workflow_step_id": step.ID,
		"agent_type":       step.AgentType,
	}

	taskInfo := orchestrator.StartTask(inputWithStepInfo)

	state.mutex.Lock()
	if state.instance.TaskID == uuid.Nil {
		state.instance.TaskID = taskInfo.TaskID
	}
	if state.instance.ThreadID == uuid.Nil {
		state.instance.ThreadID = taskInfo.ThreadID
	}
	state.mutex.Unlock()

	taskResult := map[string]any{
		"task_id":    taskInfo.TaskID.String(),
		"thread_id":  taskInfo.ThreadID.String(),
		"status":     "completed",
		"step_id":    step.ID,
		"agent_type": step.AgentType,
	}

	time.Sleep(2 * time.Second)

	if caseID != "" && e.resultRepository != nil {
		results, err := e.resultRepository.GetResultsByCaseID(caseID)
		if err == nil && results != nil {
			for k, v := range results {
				taskResult[k] = v
			}

			state.mutex.Lock()
			if state.instance.StepResults == nil {
				state.instance.StepResults = make(map[string]any)
			}

			if step.AgentType == "diagnostic_agent" && results["diagnosis"] != nil {
				state.instance.StepResults["diagnosis"] = results["diagnosis"]
			} else if step.AgentType == "treatment_agent" && results["treatment_plan"] != nil {
				state.instance.StepResults["treatment_plan"] = results["treatment_plan"]
			}
			state.mutex.Unlock()
		}
	}

	return taskResult, nil
}

func (e *WorkflowEngine) SetResultRepository(repo repository.ResultRepository) {
	e.resultRepository = repo
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
