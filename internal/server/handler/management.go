package handler

import (
	"context"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/server/response"
	"github.com/diogoazevedoo/swordsymphony/internal/workflow"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ManagementHandler contains handlers for administrative functions
type ManagementHandler struct {
	agentService    *config.AgentConfigService
	workflowService *workflow.WorkflowService
}

// NewManagementHandler creates a new admin handler
func NewManagementHandler(agentService *config.AgentConfigService, workflowService *workflow.WorkflowService) *ManagementHandler {
	return &ManagementHandler{
		agentService:    agentService,
		workflowService: workflowService,
	}
}

// --- Agent Configuration Endpoints ---

// GetAgentConfigs returns all agent configurations
func (h *ManagementHandler) GetAgentConfigs(c *gin.Context) {
	configs := h.agentService.GetAllAgentConfigs()

	simplifiedConfigs := make([]map[string]any, 0, len(configs))
	for _, config := range configs {
		simplifiedConfigs = append(simplifiedConfigs, map[string]any{
			"id":          config.ID,
			"type":        config.Type,
			"name":        config.Name,
			"description": config.Description,
			"version":     config.Version,
			"author":      config.Author,
		})
	}

	response.JSON(c, gin.H{
		"configs": simplifiedConfigs,
		"count":   len(simplifiedConfigs),
	})
}

// GetAgentConfig returns details of a specific agent configuration
func (h *ManagementHandler) GetAgentConfig(c *gin.Context) {
	configID := c.Param("id")

	config, exists := h.agentService.GetAgentConfig(configID)
	if !exists {
		response.Error(c, errors.NotFound("Agent configuration not found", "agent_config_not_found"))
		return
	}

	response.JSON(c, config)
}

// CreateAgentConfig creates a new agent configuration
func (h *ManagementHandler) CreateAgentConfig(c *gin.Context) {
	var config config.AgentConfig

	if err := c.ShouldBindJSON(&config); err != nil {
		response.Error(c, errors.Validation("Invalid agent configuration", "invalid_agent_config"))
		return
	}

	if config.ID == "" {
		response.Error(c, errors.Validation("Agent ID is required", "missing_agent_id"))
		return
	}

	if _, exists := h.agentService.GetAgentConfig(config.ID); exists {
		response.Error(c, errors.Internal("Agent with this ID already exists", "agent_already_exists"))
		return
	}

	if err := h.agentService.SaveAgentConfig(config); err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to save agent configuration", "save_agent_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message": "Agent configuration created successfully",
		"id":      config.ID,
	})
}

// UpdateAgentConfig updates an existing agent configuration
func (h *ManagementHandler) UpdateAgentConfig(c *gin.Context) {
	configID := c.Param("id")

	if _, exists := h.agentService.GetAgentConfig(configID); !exists {
		response.Error(c, errors.NotFound("Agent configuration not found", "agent_config_not_found"))
		return
	}

	var config config.AgentConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		response.Error(c, errors.Validation("Invalid agent configuration", "invalid_agent_config"))
		return
	}

	if config.ID != configID {
		response.Error(c, errors.Validation("Agent ID in URL must match ID in request body", "id_mismatch"))
		return
	}

	if err := h.agentService.SaveAgentConfig(config); err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to update agent configuration", "update_agent_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message": "Agent configuration updated successfully",
		"id":      config.ID,
	})
}

// DeployAgent creates and registers an agent from a configuration
func (h *ManagementHandler) DeployAgent(c *gin.Context) {
	configID := c.Param("id")

	if _, exists := h.agentService.GetAgentConfig(configID); !exists {
		response.Error(c, errors.NotFound("Agent configuration not found", "agent_config_not_found"))
		return
	}

	agent, err := h.agentService.CreateAndRegisterAgent(c.Request.Context(), configID)
	if err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to deploy agent", "deploy_agent_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message":  "Agent deployed successfully",
		"agent_id": configID,
		"address":  string(agent.Address()),
	})
}

// --- Workflow Management Endpoints ---

// GetWorkflows returns all workflow definitions
func (h *ManagementHandler) GetWorkflows(c *gin.Context) {
	workflows := h.workflowService.GetAllWorkflowDefinitions()

	simplifiedWorkflows := make([]map[string]any, 0, len(workflows))
	for _, wf := range workflows {
		simplifiedWorkflows = append(simplifiedWorkflows, map[string]any{
			"id":          wf.ID,
			"name":        wf.Name,
			"description": wf.Description,
			"version":     wf.Version,
			"step_count":  len(wf.Steps),
			"author":      wf.Author,
			"tags":        wf.Tags,
		})
	}

	response.JSON(c, gin.H{
		"workflows": simplifiedWorkflows,
		"count":     len(simplifiedWorkflows),
	})
}

// GetWorkflow returns details of a specific workflow
func (h *ManagementHandler) GetWorkflow(c *gin.Context) {
	workflowID := c.Param("id")

	workflow, exists := h.workflowService.GetWorkflowDefinition(workflowID)
	if !exists {
		response.Error(c, errors.NotFound("Workflow not found", "workflow_not_found"))
		return
	}

	response.JSON(c, workflow)
}

// CreateWorkflow creates a new workflow
func (h *ManagementHandler) CreateWorkflow(c *gin.Context) {
	var workflow workflow.WorkflowDefinition

	if err := c.ShouldBindJSON(&workflow); err != nil {
		response.Error(c, errors.Validation("Invalid workflow definition", "invalid_workflow"))
		return
	}

	if workflow.ID == "" {
		response.Error(c, errors.Validation("Workflow ID is required", "missing_workflow_id"))
		return
	}

	if _, exists := h.workflowService.GetWorkflowDefinition(workflow.ID); exists {
		response.Error(c, errors.Internal("Workflow with this ID already exists", "workflow_already_exists"))
		return
	}

	if err := h.workflowService.SaveWorkflowDefinition(workflow); err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to save workflow", "save_workflow_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message": "Workflow created successfully",
		"id":      workflow.ID,
	})
}

// UpdateWorkflow updates an existing workflow
func (h *ManagementHandler) UpdateWorkflow(c *gin.Context) {
	workflowID := c.Param("id")

	if _, exists := h.workflowService.GetWorkflowDefinition(workflowID); !exists {
		response.Error(c, errors.NotFound("Workflow not found", "workflow_not_found"))
		return
	}

	var workflow workflow.WorkflowDefinition
	if err := c.ShouldBindJSON(&workflow); err != nil {
		response.Error(c, errors.Validation("Invalid workflow definition", "invalid_workflow"))
		return
	}

	if workflow.ID != workflowID {
		response.Error(c, errors.Validation("Workflow ID in URL must match ID in request body", "id_mismatch"))
		return
	}

	if err := h.workflowService.SaveWorkflowDefinition(workflow); err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to update workflow", "update_workflow_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message": "Workflow updated successfully",
		"id":      workflow.ID,
	})
}

// DeleteWorkflow deletes a workflow
func (h *ManagementHandler) DeleteWorkflow(c *gin.Context) {
	workflowID := c.Param("id")

	if err := h.workflowService.DeleteWorkflowDefinition(workflowID); err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to delete workflow", "delete_workflow_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message": "Workflow deleted successfully",
		"id":      workflowID,
	})
}

// GetWorkflowInstances returns all instances of a workflow
func (h *ManagementHandler) GetWorkflowInstances(c *gin.Context) {
	workflowID := c.Param("id")

	instances, err := h.workflowService.GetWorkflowInstances(workflowID)
	if err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to get workflow instances", "get_instances_failed"))
		return
	}

	simplifiedInstances := make([]map[string]any, 0, len(instances))
	for _, instance := range instances {
		simplifiedInstances = append(simplifiedInstances, map[string]any{
			"id":              instance.ID,
			"status":          instance.Status,
			"start_time":      time.Unix(0, instance.StartTime),
			"end_time":        time.Unix(0, instance.EndTime),
			"completed_steps": len(instance.CompletedSteps),
			"has_errors":      len(instance.Errors) > 0,
		})
	}

	response.JSON(c, gin.H{
		"workflow_id": workflowID,
		"instances":   simplifiedInstances,
		"count":       len(simplifiedInstances),
	})
}

// GetWorkflowInstance returns details of a specific workflow instance
func (h *ManagementHandler) GetWorkflowInstance(c *gin.Context) {
	instanceIDStr := c.Param("instance_id")

	instanceID, err := uuid.Parse(instanceIDStr)
	if err != nil {
		response.Error(c, errors.Validation("Invalid instance ID format", "invalid_instance_id"))
		return
	}

	instance, err := h.workflowService.GetWorkflowInstance(instanceID)
	if err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeNotFound, "Workflow instance not found", "instance_not_found"))
		return
	}

	response.JSON(c, instance)
}

// StartWorkflowInstance creates and starts a new workflow instance
func (h *ManagementHandler) StartWorkflowInstance(c *gin.Context) {
	workflowID := c.Param("id")

	if _, exists := h.workflowService.GetWorkflowDefinition(workflowID); !exists {
		response.Error(c, errors.NotFound("Workflow not found", "workflow_not_found"))
		return
	}

	var input map[string]any
	if err := c.ShouldBindJSON(&input); err != nil {
		response.Error(c, errors.Validation("Invalid input data", "invalid_input"))
		return
	}

	instance, err := h.workflowService.StartWorkflow(context.Background(), workflowID, input)
	if err != nil {
		response.Error(c, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to start workflow", "start_workflow_failed"))
		return
	}

	response.JSON(c, gin.H{
		"message":     "Workflow started successfully",
		"instance_id": instance.ID,
		"workflow_id": workflowID,
		"status":      instance.Status,
	})
}
