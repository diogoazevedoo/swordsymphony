package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

type openAIClient struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
	retries    int
}

// NewOpenAIClient creates a new OpenAI client
func NewOpenAIClient(apiKey string) Client {
	return &openAIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		baseURL: "https://api.openai.com/v1",
		retries: 3,
	}
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
	TopP        float64         `json:"top_p,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

// GenerateCompletion implements the AI Client interface
func (c *openAIClient) GenerateCompletion(ctx context.Context, prompt string, options CompletionOptions) (CompletionResponse, error) {
	model := "gpt-4"
	if options.ModelName != "" {
		model = options.ModelName
	}

	messages := []openAIMessage{
		{Role: "user", Content: prompt},
	}

	if options.SystemPrompt != "" {
		messages = append([]openAIMessage{{Role: "system", Content: options.SystemPrompt}}, messages...)
	}

	reqBody := openAIRequest{
		Model:       model,
		Messages:    messages,
		MaxTokens:   options.MaxTokens,
		Temperature: options.Temperature,
		TopP:        options.TopP,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return CompletionResponse{}, errors.Internal(fmt.Sprintf("failed to marshal request: %v", err), "ai_request_error")
	}

	return c.retryWithBackoff(ctx, jsonData, model)
}

func (c *openAIClient) doRequest(ctx context.Context, jsonData []byte, model string) (CompletionResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return CompletionResponse{}, errors.Internal(fmt.Sprintf("failed to create request: %v", err), "ai_request_error")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return CompletionResponse{}, errors.External(err, "OpenAI API request failed", "ai_connection_error")
	}
	defer resp.Body.Close()

	var openAIResp openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&openAIResp); err != nil {
		return CompletionResponse{}, errors.External(err, "Failed to decode OpenAI response", "ai_decode_error")
	}

	if openAIResp.Error != nil {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("%s: %s", openAIResp.Error.Type, openAIResp.Error.Message),
			"OpenAI API returned an error",
			openAIResp.Error.Code,
		)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("status code %d", resp.StatusCode),
			"OpenAI API returned an unexpected status code",
			"ai_status_error",
		)
	}

	if len(openAIResp.Choices) == 0 {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("no choices returned"),
			"OpenAI API returned no completion choices",
			"ai_empty_response",
		)
	}

	return CompletionResponse{
		Text: openAIResp.Choices[0].Message.Content,
		Metadata: map[string]string{
			"model": model,
		},
	}, nil
}

// isTransientError determines if an error is likely temporary and should be retried
func isTransientError(err error) bool {
	if appErr, ok := err.(*errors.AppError); ok {
		if appErr.Type == errors.ErrorTypeExternal {
			if appErr.Code == "rate_limit_exceeded" ||
				appErr.Code == "server_error" ||
				appErr.Code == "service_unavailable" ||
				strings.Contains(appErr.Message, "timeout") ||
				strings.Contains(appErr.Message, "connection") {
				return true
			}
		}
	}
	return false
}

// GenerateEmbedding creates vector embeddings
func (c *openAIClient) GenerateEmbedding(ctx context.Context, text string) ([]float64, error) {
	reqBody := map[string]any{
		"model": "text-embedding-ada-002",
		"input": text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/embeddings", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OpenAI API request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Error *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if result.Error != nil {
		return nil, fmt.Errorf("OpenAI API error: %s (%s)", result.Error.Message, result.Error.Type)
	}

	if len(result.Data) == 0 || len(result.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return result.Data[0].Embedding, nil
}

// retryWithBackoff retries a request with exponential backoff
func (c *openAIClient) retryWithBackoff(ctx context.Context, jsonData []byte, model string) (CompletionResponse, error) {
	var result CompletionResponse
	var lastErr error

	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			backoffDuration := time.Duration(attempt*attempt) * 500 * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond
			backoffWithJitter := backoffDuration + jitter

			logger.Info("Retrying OpenAI API request",
				"attempt", attempt,
				"max_retries", c.retries,
				"backoff", backoffWithJitter.String())

			select {
			case <-time.After(backoffWithJitter):
				// Continue with retry
			case <-ctx.Done():
				return CompletionResponse{}, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			}
		}

		result, lastErr = c.doRequest(ctx, jsonData, model)
		if lastErr == nil {
			return result, nil
		}

		if !isTransientError(lastErr) {
			return CompletionResponse{}, lastErr
		}

		logger.Warn("Transient error from OpenAI API, will retry",
			"attempt", attempt+1,
			"max_retries", c.retries,
			"error", lastErr)
	}

	return CompletionResponse{}, fmt.Errorf("failed after %d retries: %w", c.retries, lastErr)
}
