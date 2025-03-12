package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// GetMessages returns all messages in the system
func (h *Handler) GetMessages(c *gin.Context) {
	threadID := c.Query("thread_id")

	if threadID == "" {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	messages := h.orchestrator.GetAllMessages(uuid.MustParse(threadID))

	c.JSON(http.StatusOK, messages)
}
