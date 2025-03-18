package config

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"gopkg.in/yaml.v3"
)

// AgentConfigService manages agent configurations
type AgentConfigService struct {
	configDir   string
	registry    *actor.Registry
	actorSystem actor.ActorSystem
	configs     map[string]AgentConfig
	templates   map[string]*template.Template
	customTypes map[string]AgentCreatorFunc
	mu          sync.RWMutex
}

// AgentCreatorFunc is a function that creates an actor from a configuration
type AgentCreatorFunc func(ctx context.Context, config AgentConfig, system actor.ActorSystem) (actor.Actor, error)

// NewAgentConfigService creates a new agent configuration service
func NewAgentConfigService(configDir string, registry *actor.Registry, system actor.ActorSystem) *AgentConfigService {
	return &AgentConfigService{
		configDir:   configDir,
		registry:    registry,
		actorSystem: system,
		configs:     make(map[string]AgentConfig),
		templates:   make(map[string]*template.Template),
		customTypes: make(map[string]AgentCreatorFunc),
	}
}

// Initialize loads all agent configurations
func (s *AgentConfigService) Initialize() error {
	if err := s.LoadAllAgentConfigs(); err != nil {
		return err
	}

	if err := s.LoadPromptTemplates(); err != nil {
		logger.Warn("Failed to load prompt templates", "error", err)
	}

	return nil
}

// LoadAllAgentConfigs loads all agent configurations from the config directory
func (s *AgentConfigService) LoadAllAgentConfigs() error {
	configs, err := LoadAgentConfigsFromDirectory(s.configDir)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, config := range configs {
		s.configs[config.ID] = config
	}

	logger.Info("Loaded agent configurations", "count", len(configs))

	return nil
}

// LoadPromptTemplates loads all prompt templates
func (s *AgentConfigService) LoadPromptTemplates() error {
	templateDir := filepath.Join(s.configDir, "prompts")
	if _, err := os.Stat(templateDir); os.IsNotExist(err) {
		return nil
	}

	files, err := os.ReadDir(templateDir)
	if err != nil {
		return fmt.Errorf("failed to read template directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".tmpl" && filepath.Ext(file.Name()) != ".txt" {
			continue
		}

		filePath := filepath.Join(templateDir, file.Name())
		templateName := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

		data, err := os.ReadFile(filePath)
		if err != nil {
			logger.Warn("Failed to read template file", "file", filePath, "error", err)
			continue
		}

		tmpl, err := template.New(templateName).Parse(string(data))
		if err != nil {
			logger.Warn("Failed to parse template", "file", filePath, "error", err)
			continue
		}

		s.mu.Lock()
		s.templates[templateName] = tmpl
		s.mu.Unlock()
	}

	return nil
}

// GetAgentConfig retrieves an agent configuration by ID
func (s *AgentConfigService) GetAgentConfig(id string) (AgentConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, exists := s.configs[id]
	return config, exists
}

// GetAllAgentConfigs returns all agent configurations
func (s *AgentConfigService) GetAllAgentConfigs() []AgentConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	configs := make([]AgentConfig, 0, len(s.configs))
	for _, config := range s.configs {
		configs = append(configs, config)
	}

	return configs
}

// SaveAgentConfig saves an agent configuration
func (s *AgentConfigService) SaveAgentConfig(config AgentConfig) error {
	if config.ID == "" {
		return fmt.Errorf("agent ID cannot be empty")
	}

	s.mu.Lock()
	s.configs[config.ID] = config
	s.mu.Unlock()

	filePath := filepath.Join(s.configDir, fmt.Sprintf("%s.yaml", config.ID))
	if err := saveAgentConfigToFile(config, filePath); err != nil {
		return err
	}

	return nil
}

// RegisterCustomType registers a custom agent type with a creator function
func (s *AgentConfigService) RegisterCustomType(typeName string, creator AgentCreatorFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.customTypes[typeName] = creator
}

// GetPromptTemplate fills a prompt template with data
func (s *AgentConfigService) GetPromptTemplate(name string, data any) (string, error) {
	s.mu.RLock()
	tmpl, exists := s.templates[name]
	s.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("template %s not found", name)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// CreateAndRegisterAgent creates an agent from a configuration and registers it with the system
func (s *AgentConfigService) CreateAndRegisterAgent(ctx context.Context, configID string) (actor.Actor, error) {
	config, exists := s.GetAgentConfig(configID)
	if !exists {
		return nil, fmt.Errorf("agent configuration %s not found", configID)
	}

	s.mu.RLock()
	creator, hasCustomCreator := s.customTypes[config.Type]
	s.mu.RUnlock()

	var agent actor.Actor
	var err error

	if hasCustomCreator {
		agent, err = creator(ctx, config, s.actorSystem)
	} else {
		actorConfig := config.ToActorConfig()
		agent, err = s.registry.Create(ctx, actorConfig, s.actorSystem)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create agent %s: %w", configID, err)
	}

	if err := s.actorSystem.Register(agent); err != nil {
		return nil, fmt.Errorf("failed to register agent %s: %w", configID, err)
	}

	logger.Info("Created and registered agent", "id", configID, "type", config.Type)
	return agent, nil
}

// Helper function to save config to file
func saveAgentConfigToFile(config AgentConfig, filePath string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal agent config: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write agent config to file: %w", err)
	}

	return nil
}
