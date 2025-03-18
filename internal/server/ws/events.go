package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gorilla/websocket"
)

// EventType defines the type of WebSocket event
type EventType string

const (
	EventStatusUpdate    EventType = "status_update"
	EventTaskStart       EventType = "task_start"
	EventTaskComplete    EventType = "task_complete"
	EventAgentMessage    EventType = "agent_message"
	EventDiagnosisResult EventType = "diagnosis_result"
	EventTreatmentPlan   EventType = "treatment_plan"
	EventError           EventType = "error"
)

// Event represents a WebSocket event
type Event struct {
	Type      EventType      `json:"type"`
	Data      map[string]any `json:"data"`
	Timestamp int64          `json:"timestamp"`
	ThreadID  string         `json:"thread_id,omitempty"`
	TaskID    string         `json:"task_id,omitempty"`
}

// Client represents a WebSocket client connection
type Client struct {
	ID       string
	Conn     *websocket.Conn
	Send     chan Event
	ThreadID string
	mu       sync.Mutex
}

// ClientManager manages WebSocket client connections
type ClientManager struct {
	clients    map[string]*Client
	broadcast  chan Event
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// NewClientManager creates a new WebSocket client manager
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:    make(map[string]*Client),
		broadcast:  make(chan Event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Start begins the client manager's event loop
func (manager *ClientManager) Start() {
	for {
		select {
		case client := <-manager.register:
			manager.mu.Lock()
			manager.clients[client.ID] = client
			manager.mu.Unlock()
			logger.Info("WebSocket client connected", "client_id", client.ID)

		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				delete(manager.clients, client.ID)
				close(client.Send)
			}
			manager.mu.Unlock()
			logger.Info("WebSocket client disconnected", "client_id", client.ID)

		case event := <-manager.broadcast:
			manager.mu.RLock()
			for id, client := range manager.clients {
				if event.ThreadID != "" && client.ThreadID != "" && client.ThreadID != event.ThreadID {
					continue
				}

				select {
				case client.Send <- event:
				default:
					close(client.Send)
					delete(manager.clients, id)
				}
			}
			manager.mu.RUnlock()
		}
	}
}

// SendEvent broadcasts an event to all connected clients
func (manager *ClientManager) SendEvent(eventType EventType, data map[string]any, threadID string, taskID string) {
	event := Event{
		Type:      eventType,
		Data:      data,
		Timestamp: time.Now().Unix(),
		ThreadID:  threadID,
		TaskID:    taskID,
	}
	manager.broadcast <- event
}

// ConvertMessageToEvent converts a domain message to a WebSocket event
func ConvertMessageToEvent(msg domain.Message) Event {
	eventType := EventStatusUpdate

	switch msg.MessageType {
	case domain.TaskAssignment:
		eventType = EventTaskStart
	case domain.TaskComplete:
		eventType = EventTaskComplete
	case domain.DiagnosisResults:
		eventType = EventDiagnosisResult
	case domain.TreatmentPlan:
		eventType = EventTreatmentPlan
	case domain.StatusUpdate:
		eventType = EventStatusUpdate
	default:
		eventType = EventAgentMessage
	}

	return Event{
		Type:      eventType,
		Data:      msg.Content,
		Timestamp: time.Now().Unix(),
		ThreadID:  msg.ThreadID.String(),
		TaskID:    getTaskIDFromMessage(msg),
	}
}

// Helper function to extract task ID from message
func getTaskIDFromMessage(msg domain.Message) string {
	if taskID, ok := msg.Content["task_id"]; ok {
		if taskIDStr, ok := taskID.(string); ok {
			return taskIDStr
		}
	}
	return ""
}

// Start begins the client's message handling
func (c *Client) Start(manager *ClientManager) {
	go c.writePump(manager)
	go c.readPump(manager)
}

// writePump sends messages to the WebSocket connection
func (c *Client) writePump(manager *ClientManager) {
	ticker := time.NewTicker(60 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
		manager.Unregister(c)
	}()

	for {
		select {
		case event, ok := <-c.Send:
			c.mu.Lock()
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				c.mu.Unlock()
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				c.mu.Unlock()
				return
			}

			data, err := json.Marshal(event)
			if err != nil {
				logger.Error("Failed to marshal WebSocket event", "error", err)
				c.mu.Unlock()
				continue
			}

			w.Write(data)

			if err := w.Close(); err != nil {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()

		case <-ticker.C:
			c.mu.Lock()
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
		}
	}
}

// readPump reads messages from the WebSocket connection
func (c *Client) readPump(manager *ClientManager) {
	defer func() {
		manager.Unregister(c)
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(1024)

	c.Conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure) {
				logger.Error("WebSocket error", "error", err)
			}
			break
		}

		var clientCommand map[string]any
		if err := json.Unmarshal(message, &clientCommand); err == nil {
			if action, ok := clientCommand["action"].(string); ok {
				if action == "subscribe" && clientCommand["thread_id"] != nil {
					if threadID, ok := clientCommand["thread_id"].(string); ok {
						c.ThreadID = threadID
						logger.Info("Client subscribed to thread",
							"client_id", c.ID,
							"thread_id", threadID)
					}
				}
			}
		}
	}
}

// Register adds a new client to the manager
func (manager *ClientManager) Register(client *Client) {
	manager.register <- client
}

// Broadcast sends an event to all connected clients
func (manager *ClientManager) Broadcast(event Event) {
	manager.broadcast <- event
}

// Unregister removes a client from the manager
func (manager *ClientManager) Unregister(client *Client) {
	manager.unregister <- client
}
