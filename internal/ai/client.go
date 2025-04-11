package ai

import (
	"context"
	"errors"
)

// Provider represents an AI model provider
type Provider string

const (
	OpenAI Provider = "openai"
)

// Client is an interface for interacting with AI models
type Client interface {
	// GenerateCompletion sends a prompt to an AI model and returns the completion
	GenerateCompletion(ctx context.Context, prompt string, options CompletionOptions) (CompletionResponse, error)

	// GenerateEmbedding creates vector embeddings for text
	GenerateEmbedding(ctx context.Context, text string) ([]float64, error)

	// AnalyzeDocument analyzes a document (PDF, image, etc.) and returns structured analysis
	AnalyzeDocument(ctx context.Context, filePath string, contentType string, documentType string) (map[string]any, error)
}

// CompletionOptions contains parameters for the completion request
type CompletionOptions struct {
	MaxTokens    int     `json:"max_tokens,omitempty"`
	Temperature  float64 `json:"temperature,omitempty"`
	TopP         float64 `json:"top_p,omitempty"`
	ModelName    string  `json:"model_name,omitempty"`
	SystemPrompt string  `json:"system_prompt,omitempty"`
}

// CompletionResponse contains the AI model's response
type CompletionResponse struct {
	Text       string            `json:"text"`
	Confidence float64           `json:"confidence,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Factory is an interface for creating AI clients
type Factory interface {
	CreateClient(provider Provider, apiKey string) (Client, error)
}

// DefaultFactory is the default implementation of Factory
type DefaultFactory struct{}

// CreateClient creates an AI client for the specified provider
func (f *DefaultFactory) CreateClient(provider Provider, apiKey string) (Client, error) {
	switch provider {
	case OpenAI:
		if apiKey == "" {
			return nil, errors.New("OPENAI_API_KEY is not set")
		}
		return NewOpenAIClient(apiKey), nil
	default:
		return nil, errors.New("unsupported AI provider")
	}
}

// NewClient creates an AI client for the specified provider using the default factory
func NewClient(provider Provider, apiKey string) (Client, error) {
	factory := &DefaultFactory{}
	return factory.CreateClient(provider, apiKey)
}
