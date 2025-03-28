package deepgram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gorilla/websocket"
)

// Client represents a Deepgram API client
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	language   string
	model      string
	tier       string
	wsClient   *websocket.Conn
	wsMutex    sync.Mutex
}

// ClientOption configures a Deepgram client
type ClientOption func(*Client)

// TranscriptResponse represents a speech-to-text response
type TranscriptResponse struct {
	Transcript  string
	Confidence  float64
	Words       []Word
	IsFinal     bool
	SpeakerID   string
	UtteranceID string
}

// Word represents a single word in a transcript
type Word struct {
	Word       string
	StartTime  float64
	EndTime    float64
	Confidence float64
	Speaker    string
}

// NewClient creates a new Deepgram client
func NewClient(apiKey string, options ...ClientOption) *Client {
	client := &Client{
		apiKey:  apiKey,
		baseURL: "https://api.deepgram.com/v1",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		language: "en-US",
		model:    "general",
		tier:     "enhanced",
	}

	for _, option := range options {
		option(client)
	}

	return client
}

// WithBaseURL sets a custom base URL for the Deepgram API
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		c.baseURL = baseURL
	}
}

// WithLanguage sets the language to use for transcription
func WithLanguage(language string) ClientOption {
	return func(c *Client) {
		c.language = language
	}
}

// WithModel sets the model to use for transcription
func WithModel(model string) ClientOption {
	return func(c *Client) {
		c.model = model
	}
}

// WithTier sets the tier to use for transcription
func WithTier(tier string) ClientOption {
	return func(c *Client) {
		c.tier = tier
	}
}

// WithTimeout sets a custom timeout for HTTP requests
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// TranscribeAudio converts audio to text
func (c *Client) TranscribeAudio(audioData []byte, mimeType string, options map[string]string) (*TranscriptResponse, error) {
	query := url.Values{}
	query.Set("model", c.model)
	query.Set("tier", c.tier)
	query.Set("language", c.language)
	query.Set("detect_language", "true")
	query.Set("punctuate", "true")
	query.Set("diarize", "true")

	for k, v := range options {
		query.Set(k, v)
	}

	endpoint := fmt.Sprintf("%s/listen?%s", c.baseURL, query.Encode())
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(audioData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", mimeType)
	req.Header.Set("Authorization", "Token "+c.apiKey)

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response from Deepgram (status %d): %s", resp.StatusCode, string(body))
	}

	var response struct {
		Results struct {
			Channels []struct {
				Alternatives []struct {
					Transcript string  `json:"transcript"`
					Confidence float64 `json:"confidence"`
					Words      []struct {
						Word       string  `json:"word"`
						StartTime  float64 `json:"start"`
						EndTime    float64 `json:"end"`
						Confidence float64 `json:"confidence"`
						Speaker    int     `json:"speaker"`
					} `json:"words"`
				} `json:"alternatives"`
			} `json:"channels"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	if len(response.Results.Channels) == 0 ||
		len(response.Results.Channels[0].Alternatives) == 0 {
		return nil, fmt.Errorf("no transcription results found")
	}

	alt := response.Results.Channels[0].Alternatives[0]

	words := make([]Word, len(alt.Words))
	for i, w := range alt.Words {
		words[i] = Word{
			Word:       w.Word,
			StartTime:  w.StartTime,
			EndTime:    w.EndTime,
			Confidence: w.Confidence,
			Speaker:    fmt.Sprintf("speaker_%d", w.Speaker),
		}
	}

	logger.Info("Transcribed audio",
		"transcript_length", len(alt.Transcript),
		"confidence", alt.Confidence,
		"word_count", len(words),
		"time_taken", time.Since(start))

	return &TranscriptResponse{
		Transcript: alt.Transcript,
		Confidence: alt.Confidence,
		Words:      words,
		IsFinal:    true,
	}, nil
}

// StreamingTranscriptionHandler handles streaming transcription results
type StreamingTranscriptionHandler func(response *TranscriptResponse)

// StartStreamingSession starts a WebSocket session for real-time transcription
func (c *Client) StartStreamingSession(handler StreamingTranscriptionHandler) error {
	if c.wsClient != nil {
		return fmt.Errorf("streaming session already active")
	}

	query := url.Values{}
	query.Set("model", c.model)
	query.Set("tier", c.tier)
	query.Set("language", c.language)
	query.Set("detect_language", "true")
	query.Set("punctuate", "true")
	query.Set("diarize", "true")
	query.Set("encoding", "linear16")
	query.Set("sample_rate", "16000")
	query.Set("channels", "1")
	query.Set("interim_results", "true")

	wsURL := fmt.Sprintf("wss://api.deepgram.com/v1/listen/stream?%s", query.Encode())

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	header := http.Header{}
	header.Set("Authorization", "Token "+c.apiKey)

	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		return fmt.Errorf("error connecting to WebSocket: %w", err)
	}

	c.wsClient = conn

	go c.handleWebSocketMessages(handler)

	logger.Info("Started streaming transcription session")
	return nil
}

// SendAudioChunk sends a chunk of audio data to the streaming session
func (c *Client) SendAudioChunk(audioData []byte) error {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()

	if c.wsClient == nil {
		return fmt.Errorf("no active streaming session")
	}

	err := c.wsClient.WriteMessage(websocket.BinaryMessage, audioData)
	if err != nil {
		return fmt.Errorf("error sending audio chunk: %w", err)
	}

	return nil
}

// StopStreamingSession closes the WebSocket connection
func (c *Client) StopStreamingSession() error {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()

	if c.wsClient == nil {
		return nil
	}

	err := c.wsClient.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		logger.Warn("Error sending close message", "error", err)
	}

	err = c.wsClient.Close()
	c.wsClient = nil

	logger.Info("Stopped streaming transcription session")
	return err
}

// handleWebSocketMessages processes incoming WebSocket messages
func (c *Client) handleWebSocketMessages(handler StreamingTranscriptionHandler) {
	for {
		_, message, err := c.wsClient.ReadMessage()
		if err != nil {
			logger.Error("Error reading WebSocket message", "error", err)
			return
		}

		var response struct {
			Type    string `json:"type"`
			IsFinal bool   `json:"is_final"`
			Channel struct {
				Alternatives []struct {
					Transcript string  `json:"transcript"`
					Confidence float64 `json:"confidence"`
					Words      []struct {
						Word       string  `json:"word"`
						StartTime  float64 `json:"start"`
						EndTime    float64 `json:"end"`
						Confidence float64 `json:"confidence"`
						Speaker    int     `json:"speaker"`
						Punctuated string  `json:"punctuated_word"`
					} `json:"words"`
				} `json:"alternatives"`
			} `json:"channel"`
			SpeakerID   string `json:"speaker"`
			UtteranceID string `json:"utterance_id"`
		}

		if err := json.Unmarshal(message, &response); err != nil {
			logger.Error("Error parsing WebSocket message", "error", err)
			continue
		}

		if response.Type != "Results" && response.Type != "Transcript" {
			continue
		}

		if len(response.Channel.Alternatives) == 0 {
			continue
		}

		alt := response.Channel.Alternatives[0]

		words := make([]Word, len(alt.Words))
		for i, w := range alt.Words {
			words[i] = Word{
				Word:       w.Word,
				StartTime:  w.StartTime,
				EndTime:    w.EndTime,
				Confidence: w.Confidence,
				Speaker:    fmt.Sprintf("speaker_%d", w.Speaker),
			}
		}

		transcriptResp := &TranscriptResponse{
			Transcript:  alt.Transcript,
			Confidence:  alt.Confidence,
			Words:       words,
			IsFinal:     response.IsFinal,
			SpeakerID:   response.SpeakerID,
			UtteranceID: response.UtteranceID,
		}

		handler(transcriptResp)
	}
}
