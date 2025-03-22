package handler

import (
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// GetMessages returns all messages in the system
func (h *ActorHandler) GetMessages(c *gin.Context) {
	threadID := c.Query("thread_id")

	orchestrator, err := h.getOrchestrator()
	if err != nil {
		handleError(c, err)
		return
	}

	if threadID == "" {
		messages := orchestrator.GetAllSystemMessages()
		c.JSON(http.StatusOK, messages)
		return
	}

	threadUUID, err := uuid.Parse(threadID)
	if err != nil {
		handleError(c, errors.Validation("Invalid thread ID format", "invalid_thread_id"))
		return
	}

	messages := orchestrator.GetAllMessages(threadUUID)

	c.JSON(http.StatusOK, messages)
}
