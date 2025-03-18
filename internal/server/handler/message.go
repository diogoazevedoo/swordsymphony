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

	if threadID == "" {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	threadUUID, err := uuid.Parse(threadID)
	if err != nil {
		handleError(c, errors.Validation("Invalid thread ID format", "invalid_thread_id"))
		return
	}

	orchestrator, err := h.getOrchestrator()
	if err != nil {
		handleError(c, err)
		return
	}

	messages := orchestrator.GetAllMessages(threadUUID)

	c.JSON(http.StatusOK, messages)
}
