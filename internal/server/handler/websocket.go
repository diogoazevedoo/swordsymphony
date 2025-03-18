package handler

import (
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/server/ws"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// WebSocketHandler handles WebSocket connections
func (h *ActorHandler) WebSocketHandler(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Error("Failed to upgrade WebSocket connection", "error", err)
		return
	}

	clientID := uuid.New().String()
	threadID := c.Query("thread_id")

	client := &ws.Client{
		ID:       clientID,
		Conn:     conn,
		Send:     make(chan ws.Event, 100),
		ThreadID: threadID,
	}

	h.wsManager.Register(client)

	client.Start(h.wsManager)

	orchestrator, err := h.getOrchestrator()
	if err != nil {
		logger.Error("Failed to get orchestrator", "error", err)
		return
	}

	messageCh := orchestrator.Subscribe()
	go func() {
		for msg := range messageCh {
			event := ws.ConvertMessageToEvent(msg)
			h.wsManager.Broadcast(event)
		}
	}()
}
