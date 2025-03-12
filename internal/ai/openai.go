package ai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type openAIClient struct {
	apiKey     string
	httpClient *http.Client
}

func newOpenAIClient(apiKey string) *openAIClient {
	return &openAIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
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
}

// GenerateCompletion implements the AI Client interface
func (c *openAIClient) GenerateCompletion(prompt string, options CompletionOptions) (CompletionResponse, error) {
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
		return CompletionResponse{}, err
	}

	req, err := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return CompletionResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return CompletionResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CompletionResponse{}, fmt.Errorf("OpenAI API returned status code %d", resp.StatusCode)
	}

	var openAIResp openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&openAIResp); err != nil {
		return CompletionResponse{}, err
	}

	if len(openAIResp.Choices) == 0 {
		return CompletionResponse{}, errors.New("no completion choices returned from OpenAI")
	}

	return CompletionResponse{
		Text: openAIResp.Choices[0].Message.Content,
		Metadata: map[string]string{
			"model": model,
		},
	}, nil
}

// GenerateEmbedding creates vector embeddings
func (c *openAIClient) GenerateEmbedding(text string) ([]float64, error) {
	// TODO: simplified for now
	return make([]float64, 1536), nil
}
