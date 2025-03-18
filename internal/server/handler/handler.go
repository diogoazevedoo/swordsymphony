package handler

import (
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	orchestratorActor "github.com/diogoazevedoo/swordsymphony/internal/orchestrator/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/gin-gonic/gin"
)

// ActorHandler contains all the HTTP handlers for the API, using actor system
type ActorHandler struct {
	actorSystem      actor.ActorSystem
	orchestratorAddr actor.Address
	caseRepository   repository.CaseRepository
	resultRepository repository.ResultRepository
}

// NewActorHandler creates a new handler instance using actor system
func NewActorHandler(
	actorSystem actor.ActorSystem,
	orchestratorAddr actor.Address,
	caseRepo repository.CaseRepository,
	resultRepo repository.ResultRepository,
) *ActorHandler {
	return &ActorHandler{
		actorSystem:      actorSystem,
		orchestratorAddr: orchestratorAddr,
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

// getOrchestrator returns the orchestrator actor
func (h *ActorHandler) getOrchestrator() (*orchestratorActor.OrchestratorActor, error) {
	actor, found := h.actorSystem.GetActor(h.orchestratorAddr)
	if !found {
		return nil, errors.Internal("Orchestrator actor not found", "orchestrator_not_found")
	}

	orchestrator, ok := actor.(*orchestratorActor.OrchestratorActor)
	if !ok {
		return nil, errors.Internal("Actor is not an orchestrator", "invalid_orchestrator")
	}

	return orchestrator, nil
}

// GetAgents returns a list of available agents
func (h *ActorHandler) GetAgents(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"agents": []string{
			"intake_agent",
			"diagnostic_agent",
			"treatment_agent",
			"custom_respiratory_diagnostic",
		},
	})
}

// GetAgentDetails returns details about a specific agent
func (h *ActorHandler) GetAgentDetails(c *gin.Context) {
	agentID := c.Param("agent_id")

	c.JSON(http.StatusOK, gin.H{
		"id":     agentID,
		"type":   "agent",
		"status": "active",
	})
}
