package twilio

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// Client represents a Twilio API client
type Client struct {
	accountSID   string
	authToken    string
	phoneNumber  string
	apiURL       string
	httpClient   *http.Client
	activeCallID string
	callMutex    sync.RWMutex
}

// CallEvent represents a Twilio call event
type CallEvent struct {
	CallSID     string    `json:"call_sid"`
	Direction   string    `json:"direction"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Status      string    `json:"status"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time,omitempty"`
	Duration    int       `json:"duration,omitempty"`
	RecordURL   string    `json:"record_url,omitempty"`
	ErrorCode   string    `json:"error_code,omitempty"`
	ErrorMsg    string    `json:"error_message,omitempty"`
	CallbackURL string    `json:"callback_url,omitempty"`
}

// ClientOption configures a Twilio client
type ClientOption func(*Client)

// NewClient creates a new Twilio client
func NewClient(accountSID, authToken, phoneNumber string, options ...ClientOption) *Client {
	client := &Client{
		accountSID:  accountSID,
		authToken:   authToken,
		phoneNumber: phoneNumber,
		apiURL:      "https://api.twilio.com/2010-04-01/Accounts/",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	for _, option := range options {
		option(client)
	}

	return client
}

// WithBaseURL sets a custom base URL for the Twilio API
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		c.apiURL = baseURL
	}
}

// WithTimeout sets a custom timeout for HTTP requests
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// MakeCall initiates a call to the specified phone number
func (c *Client) MakeCall(to, callbackURL string) (*CallEvent, error) {
	data := url.Values{}
	data.Set("To", to)
	data.Set("From", c.phoneNumber)
	data.Set("Url", callbackURL)
	data.Set("StatusCallback", callbackURL)
	data.Set("StatusCallbackEvent", "initiated,answered,completed")
	data.Set("Record", "true")

	endpoint := fmt.Sprintf("%s%s/Calls.json", c.apiURL, c.accountSID)

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", "Basic "+c.getAuthHeader())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response from Twilio: %s", string(body))
	}

	var callResponse struct {
		Sid         string `json:"sid"`
		Status      string `json:"status"`
		From        string `json:"from"`
		To          string `json:"to"`
		DateCreated string `json:"date_created"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&callResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	startTime, _ := time.Parse(time.RFC3339, callResponse.DateCreated)

	callEvent := &CallEvent{
		CallSID:     callResponse.Sid,
		Direction:   "outbound",
		From:        callResponse.From,
		To:          callResponse.To,
		Status:      callResponse.Status,
		StartTime:   startTime,
		CallbackURL: callbackURL,
	}

	c.callMutex.Lock()
	c.activeCallID = callResponse.Sid
	c.callMutex.Unlock()

	logger.Info("Initiated call to patient",
		"call_sid", callResponse.Sid,
		"status", callResponse.Status,
		"to", callResponse.To)

	return callEvent, nil
}

// EndCall terminates the specified call
func (c *Client) EndCall(callSID string) error {
	if callSID == "" {
		c.callMutex.RLock()
		callSID = c.activeCallID
		c.callMutex.RUnlock()

		if callSID == "" {
			return fmt.Errorf("no active call to end")
		}
	}

	data := url.Values{}
	data.Set("Status", "completed")

	endpoint := fmt.Sprintf("%s%s/Calls/%s.json", c.apiURL, c.accountSID, callSID)

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", "Basic "+c.getAuthHeader())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response from Twilio: %s", string(body))
	}

	logger.Info("Ended call", "call_sid", callSID)
	return nil
}

// SendSMS sends an SMS message to the specified phone number
func (c *Client) SendSMS(to, message string) error {
	data := url.Values{}
	data.Set("To", to)
	data.Set("From", c.phoneNumber)
	data.Set("Body", message)

	endpoint := fmt.Sprintf("%s%s/Messages.json", c.apiURL, c.accountSID)

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", "Basic "+c.getAuthHeader())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response from Twilio: %s", string(body))
	}

	logger.Info("Sent SMS message", "to", to)
	return nil
}

// GetCallDetails retrieves details for a specific call
func (c *Client) GetCallDetails(callSID string) (*CallEvent, error) {
	endpoint := fmt.Sprintf("%s%s/Calls/%s.json", c.apiURL, c.accountSID, callSID)

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Add("Authorization", "Basic "+c.getAuthHeader())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response from Twilio: %s", string(body))
	}

	var callDetails struct {
		Sid          string `json:"sid"`
		Status       string `json:"status"`
		From         string `json:"from"`
		To           string `json:"to"`
		Direction    string `json:"direction"`
		Duration     string `json:"duration"`
		StartTime    string `json:"start_time"`
		EndTime      string `json:"end_time"`
		RecordingUrl string `json:"recording_url"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&callDetails); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	startTime, _ := time.Parse(time.RFC3339, callDetails.StartTime)
	var endTime time.Time
	if callDetails.EndTime != "" {
		endTime, _ = time.Parse(time.RFC3339, callDetails.EndTime)
	}

	duration := 0
	if callDetails.Duration != "" {
		duration, _ = strconv.Atoi(callDetails.Duration)
	}

	return &CallEvent{
		CallSID:   callDetails.Sid,
		Direction: callDetails.Direction,
		From:      callDetails.From,
		To:        callDetails.To,
		Status:    callDetails.Status,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  duration,
		RecordURL: callDetails.RecordingUrl,
	}, nil
}

// GenerateTwiML creates a TwiML response for controlling calls
func GenerateTwiML(actions ...string) string {
	twiml := `<?xml version="1.0" encoding="UTF-8"?><Response>`

	for _, action := range actions {
		twiml += action
	}

	twiml += `</Response>`
	return twiml
}

// SayAction creates a TwiML Say action
func SayAction(message string, voice string, language string) string {
	messageTemplate := `<prosody rate="1.15">%s</prosody>`
	formattedMessage := fmt.Sprintf(messageTemplate, message)
	return fmt.Sprintf(`<Say voice="%s" language="%s">%s</Say>`, voice, language, formattedMessage)
}

// PlayAction creates a TwiML Play action
func PlayAction(url string) string {
	return fmt.Sprintf(`<Play>%s</Play>`, url)
}

// StreamAction creates a TwiML Stream action for real-time audio streaming
func StreamAction(url string) string {
	return fmt.Sprintf(`<Stream url="%s" />`, url)
}

// GatherAction creates a TwiML Gather action for collecting user input
func GatherAction(content string, options map[string]string) string {
	attrs := ""
	for k, v := range options {
		attrs += fmt.Sprintf(` %s="%s"`, k, v)
	}
	return fmt.Sprintf(`<Gather%s>%s</Gather>`, attrs, content)
}

// RecordAction creates a TwiML Record action to record the call
func RecordAction() string {
	return `<Record />`
}

// getAuthHeader creates the base64 encoded authentication header
func (c *Client) getAuthHeader() string {
	auth := c.accountSID + ":" + c.authToken
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
