package handler

import (
	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/server/response"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// GetWorkflows returns all available workflow definitions
func (h *ActorHandler) GetWorkflows(c *gin.Context) {
	workflowEngine, err := h.getWorkflowEngine()
	if err != nil {
		response.Error(c, err)
		return
	}

	workflows := workflowEngine.GetAllWorkflows()

	workflowList := make([]map[string]any, 0, len(workflows))
	for _, workflow := range workflows {
		workflowList = append(workflowList, map[string]any{
			"id":          workflow.ID,
			"name":        workflow.Name,
			"description": workflow.Description,
			"version":     workflow.Version,
		})
	}

	response.JSON(c, gin.H{
		"workflows": workflowList,
		"count":     len(workflowList),
	})
}

// GetWorkflowDetails returns details of a specific workflow
func (h *ActorHandler) GetWorkflowDetails(c *gin.Context) {
	workflowID := c.Param("workflow_id")

	workflowEngine, err := h.getWorkflowEngine()
	if err != nil {
		response.Error(c, err)
		return
	}

	workflow, err := workflowEngine.GetWorkflow(workflowID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.JSON(c, workflow)
}

// GetWorkflowInstance returns details of a workflow instance
func (h *ActorHandler) GetWorkflowInstance(c *gin.Context) {
	instanceIDStr := c.Param("instance_id")

	instanceID, err := uuid.Parse(instanceIDStr)
	if err != nil {
		response.Error(c, errors.Validation("Invalid instance ID format", "invalid_instance_id"))
		return
	}

	workflowEngine, err := h.getWorkflowEngine()
	if err != nil {
		response.Error(c, err)
		return
	}

	instance, err := workflowEngine.GetWorkflowInstance(instanceID)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.JSON(c, instance)
}
