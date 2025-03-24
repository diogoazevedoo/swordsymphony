package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// WorkflowService manages workflow definitions and instances
type WorkflowService struct {
	engine                *WorkflowEngine
	configDir             string
	definitions           map[string]WorkflowDefinition
	instances             map[uuid.UUID]*WorkflowInstance
	instancesByWorkflowID map[string][]*WorkflowInstance
	mu                    sync.RWMutex
}

// NewWorkflowService creates a new workflow service
func NewWorkflowService(engine *WorkflowEngine, configDir string) *WorkflowService {
	return &WorkflowService{
		engine:                engine,
		configDir:             configDir,
		definitions:           make(map[string]WorkflowDefinition),
		instances:             make(map[uuid.UUID]*WorkflowInstance),
		instancesByWorkflowID: make(map[string][]*WorkflowInstance),
	}
}

// Initialize loads all workflow definitions
func (s *WorkflowService) Initialize() error {
	return s.LoadAllWorkflowDefinitions()
}

// LoadAllWorkflowDefinitions loads all workflow definitions from the config directory
func (s *WorkflowService) LoadAllWorkflowDefinitions() error {
	if _, err := os.Stat(s.configDir); os.IsNotExist(err) {
		if err := os.MkdirAll(s.configDir, 0755); err != nil {
			return fmt.Errorf("failed to create workflow directory: %w", err)
		}
		return nil
	}

	definitions, err := LoadWorkflowDefinitions(s.configDir)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, def := range definitions {
		if err := s.engine.RegisterWorkflow(def); err != nil {
			logger.Warn("Failed to register workflow with engine",
				"id", def.ID, "error", err)
			continue
		}

		s.definitions[def.ID] = def
	}

	logger.Info("Loaded workflow definitions", "count", len(definitions))

	return nil
}

// GetWorkflowDefinition retrieves a workflow definition by ID
func (s *WorkflowService) GetWorkflowDefinition(id string) (WorkflowDefinition, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	def, exists := s.definitions[id]
	return def, exists
}

// GetAllWorkflowDefinitions returns all workflow definitions
func (s *WorkflowService) GetAllWorkflowDefinitions() []WorkflowDefinition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	defs := make([]WorkflowDefinition, 0, len(s.definitions))
	for _, def := range s.definitions {
		defs = append(defs, def)
	}

	return defs
}

// SaveWorkflowDefinition saves a workflow definition
func (s *WorkflowService) SaveWorkflowDefinition(def WorkflowDefinition) error {
	if def.ID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	if err := s.engine.RegisterWorkflow(def); err != nil {
		return err
	}

	s.mu.Lock()
	s.definitions[def.ID] = def
	s.mu.Unlock()

	filePath := filepath.Join(s.configDir, fmt.Sprintf("%s.yaml", def.ID))
	if err := saveWorkflowToFile(def, filePath); err != nil {
		return err
	}

	return nil
}

// DeleteWorkflowDefinition deletes a workflow definition
func (s *WorkflowService) DeleteWorkflowDefinition(id string) error {
	s.mu.Lock()

	if _, exists := s.definitions[id]; !exists {
		s.mu.Unlock()
		return fmt.Errorf("workflow %s not found", id)
	}

	delete(s.definitions, id)
	s.mu.Unlock()

	filePath := filepath.Join(s.configDir, fmt.Sprintf("%s.yaml", id))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete workflow file: %w", err)
	}

	return nil
}

// StartWorkflow creates and starts a new workflow instance
func (s *WorkflowService) StartWorkflow(ctx context.Context, workflowID string, input map[string]any) (*WorkflowInstance, error) {
	instance, err := s.engine.StartWorkflow(ctx, workflowID, input)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.instances[instance.ID] = instance

	if _, exists := s.instancesByWorkflowID[workflowID]; !exists {
		s.instancesByWorkflowID[workflowID] = make([]*WorkflowInstance, 0)
	}
	s.instancesByWorkflowID[workflowID] = append(s.instancesByWorkflowID[workflowID], instance)

	return instance, nil
}

// GetWorkflowInstance retrieves a workflow instance by ID
func (s *WorkflowService) GetWorkflowInstance(instanceID uuid.UUID) (*WorkflowInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instance, exists := s.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("workflow instance %s not found", instanceID)
	}

	return instance, nil
}

// GetWorkflowInstances returns all instances of a specific workflow
func (s *WorkflowService) GetWorkflowInstances(workflowID string) ([]*WorkflowInstance, error) {
	s.mu.RLock()
	instances, exists := s.instancesByWorkflowID[workflowID]
	s.mu.RUnlock()

	if !exists || len(instances) == 0 {
		allInstances := s.engine.GetAllInstances()
		filteredInstances := make([]*WorkflowInstance, 0)

		for _, instance := range allInstances {
			if instance.WorkflowID == workflowID {
				filteredInstances = append(filteredInstances, instance)

				s.mu.Lock()
				if _, exists := s.instancesByWorkflowID[workflowID]; !exists {
					s.instancesByWorkflowID[workflowID] = make([]*WorkflowInstance, 0)
				}
				s.instancesByWorkflowID[workflowID] = append(s.instancesByWorkflowID[workflowID], instance)
				s.mu.Unlock()
			}
		}

		if len(filteredInstances) > 0 {
			return filteredInstances, nil
		}
	}

	return instances, nil
}

// GetAllWorkflowInstances returns all workflow instances
func (s *WorkflowService) GetAllWorkflowInstances() []*WorkflowInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]*WorkflowInstance, 0, len(s.instances))
	for _, instance := range s.instances {
		instances = append(instances, instance)
	}

	return instances
}

// Helper function to save workflow to file
func saveWorkflowToFile(workflow WorkflowDefinition, filePath string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := yaml.Marshal(workflow)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write workflow to file: %w", err)
	}

	return nil
}
