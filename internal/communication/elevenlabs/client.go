package elevenlabs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// Client represents an ElevenLabs API client
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	voiceID    string
	modelID    string
	stability  float64
	similarity float64
}

// ClientOption configures an ElevenLabs client
type ClientOption func(*Client)

// VoiceResponse represents a text-to-speech response
type VoiceResponse struct {
	AudioBytes []byte
	MimeType   string
	Duration   float64
}

// VoiceOptions configures the voice settings for synthesis
type VoiceOptions struct {
	Stability       float64 `json:"stability,omitempty"`
	SimilarityBoost float64 `json:"similarity_boost,omitempty"`
	Style           float64 `json:"style,omitempty"`
	SpeakerBoost    bool    `json:"speaker_boost,omitempty"`
}

// NewClient creates a new ElevenLabs client
func NewClient(apiKey string, options ...ClientOption) *Client {
	client := &Client{
		apiKey:  apiKey,
		baseURL: "https://api.elevenlabs.io/v1",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		voiceID:    "EXAVITQu4vr4xnSDxMaL",  // Default voice ID (Rachel)
		modelID:    "eleven_monolingual_v1", // Default model
		stability:  0.5,
		similarity: 0.75,
	}

	for _, option := range options {
		option(client)
	}

	return client
}

// WithBaseURL sets a custom base URL for the ElevenLabs API
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		c.baseURL = baseURL
	}
}

// WithVoiceID sets the voice ID to use for synthesis
func WithVoiceID(voiceID string) ClientOption {
	return func(c *Client) {
		c.voiceID = voiceID
	}
}

// WithModelID sets the model ID to use for synthesis
func WithModelID(modelID string) ClientOption {
	return func(c *Client) {
		c.modelID = modelID
	}
}

// WithStability sets the stability parameter for voice synthesis
func WithStability(stability float64) ClientOption {
	return func(c *Client) {
		c.stability = stability
	}
}

// WithSimilarity sets the similarity boost parameter for voice synthesis
func WithSimilarity(similarity float64) ClientOption {
	return func(c *Client) {
		c.similarity = similarity
	}
}

// WithTimeout sets a custom timeout for HTTP requests
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// GenerateAudio converts text to speech and returns the audio data
func (c *Client) GenerateAudio(text string, options *VoiceOptions) (*VoiceResponse, error) {
	if options == nil {
		options = &VoiceOptions{
			Stability:       c.stability,
			SimilarityBoost: c.similarity,
			Style:           0.0,
			SpeakerBoost:    true,
		}
	}

	requestData := struct {
		Text          string        `json:"text"`
		ModelID       string        `json:"model_id"`
		VoiceSettings *VoiceOptions `json:"voice_settings"`
	}{
		Text:          text,
		ModelID:       c.modelID,
		VoiceSettings: options,
	}

	jsonData, err := json.Marshal(requestData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/text-to-speech/%s", c.baseURL, c.voiceID)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("xi-api-key", c.apiKey)
	req.Header.Set("Accept", "audio/mpeg")

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response from ElevenLabs (status %d): %s", resp.StatusCode, string(body))
	}

	audioData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "audio/mpeg"
	}

	duration := float64(len(audioData)) / 16000.0

	logger.Info("Generated audio from text",
		"text_length", len(text),
		"audio_size", len(audioData),
		"duration", duration,
		"time_taken", time.Since(start))

	return &VoiceResponse{
		AudioBytes: audioData,
		MimeType:   contentType,
		Duration:   duration,
	}, nil
}

// StreamAudio generates audio and returns it chunk by chunk through a channel
func (c *Client) StreamAudio(text string, options *VoiceOptions) (<-chan []byte, <-chan error) {
	audioChunks := make(chan []byte)
	errorChan := make(chan error, 1)

	go func() {
		defer close(audioChunks)
		defer close(errorChan)

		if options == nil {
			options = &VoiceOptions{
				Stability:       c.stability,
				SimilarityBoost: c.similarity,
				Style:           0.0,
				SpeakerBoost:    true,
			}
		}

		requestData := struct {
			Text                         string        `json:"text"`
			ModelID                      string        `json:"model_id"`
			VoiceSettings                *VoiceOptions `json:"voice_settings"`
			StreamingLatencyOptimization bool          `json:"streaming_latency_optimization"`
		}{
			Text:                         text,
			ModelID:                      c.modelID,
			VoiceSettings:                options,
			StreamingLatencyOptimization: true,
		}

		jsonData, err := json.Marshal(requestData)
		if err != nil {
			errorChan <- fmt.Errorf("error marshaling request: %w", err)
			return
		}

		endpoint := fmt.Sprintf("%s/text-to-speech/%s/stream", c.baseURL, c.voiceID)
		req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonData))
		if err != nil {
			errorChan <- fmt.Errorf("error creating request: %w", err)
			return
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("xi-api-key", c.apiKey)
		req.Header.Set("Accept", "audio/mpeg")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			errorChan <- fmt.Errorf("error making request: %w", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			errorChan <- fmt.Errorf("error response from ElevenLabs (status %d): %s", resp.StatusCode, string(body))
			return
		}

		buffer := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buffer)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buffer[:n])
				audioChunks <- chunk
			}
			if err == io.EOF {
				return
			}
			if err != nil {
				errorChan <- fmt.Errorf("error reading stream: %w", err)
				return
			}
		}
	}()

	return audioChunks, errorChan
}

// GetVoices returns the list of available voices
func (c *Client) GetVoices() ([]map[string]any, error) {
	endpoint := fmt.Sprintf("%s/voices", c.baseURL)
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("xi-api-key", c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response from ElevenLabs (status %d): %s", resp.StatusCode, string(body))
	}

	var response struct {
		Voices []map[string]any `json:"voices"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return response.Voices, nil
}
