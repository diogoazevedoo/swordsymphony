package handler

import (
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
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
