package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// HealthCheck provides a simple endpoint to verify the API is running
func (h *ActorHandler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "SwordSymphony API",
	})
}
