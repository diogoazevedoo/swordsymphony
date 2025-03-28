package twilio

import (
	"fmt"
	"net/http"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gin-gonic/gin"
)

// WebhookHandler handles Twilio webhook callbacks
type WebhookHandler struct {
	callHandlers      map[string]CallEventHandler
	statusHandlers    map[string]StatusEventHandler
	streamHandlers    map[string]StreamEventHandler
	defaultHandler    CallEventHandler
	conversationStore map[string]*CallConversation
}

// CallEventHandler is a function that handles call events
type CallEventHandler func(c *gin.Context, event CallEvent) (string, error)

// StatusEventHandler is a function that handles call status events
type StatusEventHandler func(c *gin.Context, event CallEvent) error

// StreamEventHandler is a function that handles streaming media events
type StreamEventHandler func(c *gin.Context, callSID string, media []byte) error

// CallConversation represents the state of a conversation with a patient
type CallConversation struct {
	CallSID        string
	PatientPhone   string
	PatientName    string
	StartTime      time.Time
	EndTime        time.Time
	Duration       int
	Status         string
	ConversationID string
	CollectedData  map[string]any
	TranscriptURL  string
}

// NewWebhookHandler creates a new Twilio webhook handler
func NewWebhookHandler() *WebhookHandler {
	return &WebhookHandler{
		callHandlers:      make(map[string]CallEventHandler),
		statusHandlers:    make(map[string]StatusEventHandler),
		streamHandlers:    make(map[string]StreamEventHandler),
		conversationStore: make(map[string]*CallConversation),
	}
}

// RegisterCallHandler registers a handler for call events
func (h *WebhookHandler) RegisterCallHandler(event string, handler CallEventHandler) {
	h.callHandlers[event] = handler
}

// RegisterDefaultHandler registers a default handler for call events
func (h *WebhookHandler) RegisterDefaultHandler(handler CallEventHandler) {
	h.defaultHandler = handler
}

// RegisterStatusHandler registers a handler for call status events
func (h *WebhookHandler) RegisterStatusHandler(status string, handler StatusEventHandler) {
	h.statusHandlers[status] = handler
}

// RegisterStreamHandler registers a handler for streaming media events
func (h *WebhookHandler) RegisterStreamHandler(callSID string, handler StreamEventHandler) {
	h.streamHandlers[callSID] = handler
}

// HandleIncomingCall processes an incoming call webhook request
func (h *WebhookHandler) HandleIncomingCall(c *gin.Context) {
	callSID := c.PostForm("CallSid")
	from := c.PostForm("From")
	to := c.PostForm("To")
	direction := c.PostForm("Direction")
	status := c.PostForm("CallStatus")

	logger.Info("Received call webhook",
		"call_sid", callSID,
		"from", from,
		"status", status)

	event := CallEvent{
		CallSID:   callSID,
		Direction: direction,
		From:      from,
		To:        to,
		Status:    status,
		StartTime: time.Now(),
	}

	conversation, exists := h.conversationStore[callSID]
	if !exists {
		conversation = &CallConversation{
			CallSID:       callSID,
			PatientPhone:  from,
			StartTime:     time.Now(),
			Status:        status,
			CollectedData: make(map[string]any),
		}
		h.conversationStore[callSID] = conversation
	} else {
		conversation.Status = status
	}

	handler, exists := h.callHandlers["voice"]
	if !exists {
		handler = h.defaultHandler
	}

	if handler != nil {
		twiml, err := handler(c, event)
		if err != nil {
			logger.Error("Error handling call", "error", err)
			c.String(http.StatusInternalServerError, GenerateTwiML(
				SayAction("We're sorry, but an error occurred. Please try again later.", "alice", "en-US"),
			))
			return
		}

		c.Header("Content-Type", "text/xml")
		c.String(http.StatusOK, twiml)
		return
	}

	c.Header("Content-Type", "text/xml")
	c.String(http.StatusOK, GenerateTwiML(
		SayAction("Thank you for calling. Our system is currently unavailable.", "alice", "en-US"),
	))
}

// HandleStatusCallback processes a call status webhook request
func (h *WebhookHandler) HandleStatusCallback(c *gin.Context) {
	callSID := c.PostForm("CallSid")
	status := c.PostForm("CallStatus")
	duration := c.PostForm("CallDuration")

	logger.Info("Received status callback",
		"call_sid", callSID,
		"status", status,
		"duration", duration)

	conversation, exists := h.conversationStore[callSID]
	if exists {
		conversation.Status = status
		if status == "completed" {
			conversation.EndTime = time.Now()
			if duration != "" {
				durationInt := 0
				fmt.Sscanf(duration, "%d", &durationInt)
				conversation.Duration = durationInt
			}
		}
	}

	handler, exists := h.statusHandlers[status]
	if exists && handler != nil {
		event := CallEvent{
			CallSID:  callSID,
			Status:   status,
			Duration: 0,
		}
		if duration != "" {
			fmt.Sscanf(duration, "%d", &event.Duration)
		}

		err := handler(c, event)
		if err != nil {
			logger.Error("Error handling status callback", "error", err)
			c.Status(http.StatusInternalServerError)
			return
		}
	}

	c.Status(http.StatusOK)
}

// HandleStreamCallback processes a media stream webhook request
func (h *WebhookHandler) HandleStreamCallback(c *gin.Context) {
	callSID := c.Param("call_sid")

	data, err := c.GetRawData()
	if err != nil {
		logger.Error("Error reading stream data", "error", err)
		c.Status(http.StatusBadRequest)
		return
	}

	handler, exists := h.streamHandlers[callSID]
	if exists && handler != nil {
		err = handler(c, callSID, data)
		if err != nil {
			logger.Error("Error handling stream", "error", err)
			c.Status(http.StatusInternalServerError)
			return
		}
	}

	c.Status(http.StatusOK)
}

// GetConversation retrieves a conversation by call SID
func (h *WebhookHandler) GetConversation(callSID string) (*CallConversation, bool) {
	conversation, exists := h.conversationStore[callSID]
	return conversation, exists
}

// SetConversationData updates the collected data for a conversation
func (h *WebhookHandler) SetConversationData(callSID string, key string, value any) bool {
	conversation, exists := h.conversationStore[callSID]
	if !exists {
		return false
	}

	conversation.CollectedData[key] = value
	return true
}

// GetConversationData retrieves collected data for a conversation
func (h *WebhookHandler) GetConversationData(callSID string) (map[string]any, bool) {
	conversation, exists := h.conversationStore[callSID]
	if !exists {
		return nil, false
	}

	return conversation.CollectedData, true
}
