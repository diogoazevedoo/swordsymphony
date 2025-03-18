package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"maps"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"gopkg.in/yaml.v3"
)

// AgentConfig represents the configuration for an agent
type AgentConfig struct {
	ID              string             `json:"id" yaml:"id"`
	Type            string             `json:"type" yaml:"type"`
	Name            string             `json:"name" yaml:"name"`
	Description     string             `json:"description" yaml:"description"`
	Inputs          []AgentPortConfig  `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs         []AgentPortConfig  `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	AI              *AIAgentConfig     `json:"ai,omitempty" yaml:"ai,omitempty"`
	DataSources     []DataSourceConfig `json:"data_sources,omitempty" yaml:"data_sources,omitempty"`
	Integration     *IntegrationConfig `json:"integration,omitempty" yaml:"integration,omitempty"`
	Permissions     []string           `json:"permissions,omitempty" yaml:"permissions,omitempty"`
	Properties      map[string]any     `json:"properties,omitempty" yaml:"properties,omitempty"`
	MessageTypes    []string           `json:"message_types,omitempty" yaml:"message_types,omitempty"`
	PromptTemplates map[string]string  `json:"prompt_templates,omitempty" yaml:"prompt_templates,omitempty"`
	Version         string             `json:"version,omitempty" yaml:"version,omitempty"`
	Author          string             `json:"author,omitempty" yaml:"author,omitempty"`
}

// AgentPortConfig represents a data port for an agent (input or output)
type AgentPortConfig struct {
	Name         string         `json:"name" yaml:"name"`
	Description  string         `json:"description,omitempty" yaml:"description,omitempty"`
	Required     bool           `json:"required" yaml:"required"`
	Schema       map[string]any `json:"schema,omitempty" yaml:"schema,omitempty"`
	DefaultValue any            `json:"default_value,omitempty" yaml:"default_value,omitempty"`
	Example      any            `json:"example,omitempty" yaml:"example,omitempty"`
}

// AIAgentConfig represents AI-specific configuration for an agent
type AIAgentConfig struct {
	Provider      string         `json:"provider" yaml:"provider"`
	Model         string         `json:"model" yaml:"model"`
	Temperature   float64        `json:"temperature" yaml:"temperature"`
	MaxTokens     int            `json:"max_tokens" yaml:"max_tokens"`
	SystemPrompt  string         `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	KnowledgeBase []string       `json:"knowledge_sources,omitempty" yaml:"knowledge_sources,omitempty"`
	FunctionCalls []FunctionCall `json:"function_calls,omitempty" yaml:"function_calls,omitempty"`
}

// FunctionCall represents an AI function call definition
type FunctionCall struct {
	Name        string         `json:"name" yaml:"name"`
	Description string         `json:"description" yaml:"description"`
	Parameters  map[string]any `json:"parameters" yaml:"parameters"`
	Required    bool           `json:"required" yaml:"required"`
}

// DataSourceConfig represents a data source configuration
type DataSourceConfig struct {
	Type        string            `json:"type" yaml:"type"`
	ID          string            `json:"id" yaml:"id"`
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Connection  map[string]string `json:"connection" yaml:"connection"`
	Schema      map[string]any    `json:"schema,omitempty" yaml:"schema,omitempty"`
	Refresh     string            `json:"refresh,omitempty" yaml:"refresh,omitempty"`
	Credentials string            `json:"credentials,omitempty" yaml:"credentials,omitempty"`
}

// IntegrationConfig represents external service integration settings
type IntegrationConfig struct {
	Type        string            `json:"type" yaml:"type"`
	Endpoint    string            `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Method      string            `json:"method,omitempty" yaml:"method,omitempty"`
	Headers     map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	Auth        *AuthConfig       `json:"auth,omitempty" yaml:"auth,omitempty"`
	Timeout     string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	RetryPolicy *RetryPolicy      `json:"retry_policy,omitempty" yaml:"retry_policy,omitempty"`
}

// RetryPolicy represents retry settings for integrations
type RetryPolicy struct {
	MaxRetries  int     `json:"max_retries" yaml:"max_retries"`
	InitialWait string  `json:"initial_wait" yaml:"initial_wait"`
	MaxWait     string  `json:"max_wait" yaml:"max_wait"`
	Multiplier  float64 `json:"multiplier" yaml:"multiplier"`
}

// AuthConfig represents authentication settings for integrations
type AuthConfig struct {
	Type         string   `json:"type" yaml:"type"`
	Username     string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password     string   `json:"password,omitempty" yaml:"password,omitempty"`
	Token        string   `json:"token,omitempty" yaml:"token,omitempty"`
	ClientID     string   `json:"client_id,omitempty" yaml:"client_id,omitempty"`
	ClientSecret string   `json:"client_secret,omitempty" yaml:"client_secret,omitempty"`
	Scopes       []string `json:"scopes,omitempty" yaml:"scopes,omitempty"`
}

// LoadAgentConfig loads agent configuration from a file
func LoadAgentConfig(filePath string) (AgentConfig, error) {
	var config AgentConfig

	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read agent config file: %w", err)
	}

	ext := filepath.Ext(filePath)
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse JSON agent config: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse YAML agent config: %w", err)
		}
	default:
		return config, fmt.Errorf("unsupported agent config format: %s", ext)
	}

	if config.ID == "" {
		return config, fmt.Errorf("agent ID is required")
	}
	if config.Type == "" {
		return config, fmt.Errorf("agent type is required")
	}
	if config.Name == "" {
		return config, fmt.Errorf("agent name is required")
	}

	return config, nil
}

// LoadAgentConfigsFromDirectory loads all agent configurations from a directory
func LoadAgentConfigsFromDirectory(dirPath string) ([]AgentConfig, error) {
	var configs []AgentConfig

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return configs, fmt.Errorf("agent config directory does not exist: %s", dirPath)
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return configs, fmt.Errorf("failed to read agent config directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		ext := filepath.Ext(file.Name())
		if ext != ".json" && ext != ".yaml" && ext != ".yml" {
			continue
		}

		filePath := filepath.Join(dirPath, file.Name())
		config, err := LoadAgentConfig(filePath)
		if err != nil {
			return configs, fmt.Errorf("failed to load agent config from %s: %w", filePath, err)
		}

		configs = append(configs, config)
	}

	return configs, nil
}

// ToActorConfig converts to actor.ActorConfig
func (c *AgentConfig) ToActorConfig() actor.ActorConfig {
	properties := make(map[string]any)

	maps.Copy(properties, c.Properties)

	if len(c.Inputs) > 0 {
		properties["inputs"] = c.Inputs
	}
	if len(c.Outputs) > 0 {
		properties["outputs"] = c.Outputs
	}

	if c.AI != nil {
		properties["ai"] = c.AI
	}

	return actor.ActorConfig{
		ID:          c.ID,
		Type:        c.Type,
		Name:        c.Name,
		Description: c.Description,
		Properties:  properties,
	}
}
