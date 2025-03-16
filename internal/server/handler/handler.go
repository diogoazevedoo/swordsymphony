package handler

import (
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/gin-gonic/gin"
)

// Handler contains all the HTTP handlers for the API
type Handler struct {
	orchestrator     *orchestrator.Orchestrator
	caseRepository   repository.CaseRepository
	resultRepository repository.ResultRepository
}

// NewHandler creates a new handler instance
func NewHandler(
	orchestrator *orchestrator.Orchestrator,
	caseRepo repository.CaseRepository,
	resultRepo repository.ResultRepository,
) *Handler {
	return &Handler{
		orchestrator:     orchestrator,
		caseRepository:   caseRepo,
		resultRepository: resultRepo,
	}
}

// handleError templates the errors
func handleError(c *gin.Context, err error) {
	if appErr, ok := err.(*errors.AppError); ok {
		c.JSON(appErr.HTTPStatusCode(), gin.H{
			"error": appErr.Message,
			"code":  appErr.Code,
		})
		return
	}

	c.JSON(http.StatusInternalServerError, gin.H{
		"error": err.Error(),
		"code":  "internal_error",
	})
}
