package handler

import (
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gin-gonic/gin"
)

// GetDemoCases returns available demo cases
func (h *ActorHandler) GetDemoCases(c *gin.Context) {
	cases, err := h.caseRepository.GetDemoCases()
	if err != nil {
		handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, cases)
}

// StartCase initiates processing for a demo case
func (h *ActorHandler) StartCase(c *gin.Context) {
	caseID := c.Param("case_id")

	caseData, err := h.caseRepository.GetCaseByID(caseID)
	if err != nil {
		handleError(c, err)
		return
	}

	if caseData != nil {
		caseData["case_id"] = caseID
	}

	orchestrator, err := h.getOrchestrator()
	if err != nil {
		handleError(c, err)
		return
	}

	task := orchestrator.StartTask(map[string]interface{}{
		"patient_data": caseData,
		"scenario":     caseID,
	})

	h.caseRepository.SetCurrentCase(caseData)

	c.JSON(http.StatusOK, gin.H{
		"status":       "started",
		"case_id":      caseID,
		"task_id":      task.TaskID,
		"thread_id":    task.ThreadID,
		"patient_name": caseData["name"],
	})
}

// StartWorkflow initiates a case using a predefined workflow
func (h *ActorHandler) StartWorkflow(c *gin.Context) {
	workflowID := c.Param("workflow_id")
	caseID := c.Param("case_id")

	caseData, err := h.caseRepository.GetCaseByID(caseID)
	if err != nil {
		handleError(c, err)
		return
	}

	if caseData != nil {
		caseData["case_id"] = caseID
	}

	logger.Info("Starting workflow", "workflow_id", workflowID, "case_id", caseID)

	orchestrator, err := h.getOrchestrator()
	if err != nil {
		handleError(c, err)
		return
	}

	task := orchestrator.StartTask(map[string]interface{}{
		"patient_data": caseData,
		"scenario":     caseID,
		"workflow_id":  workflowID,
	})

	h.caseRepository.SetCurrentCase(caseData)

	c.JSON(http.StatusOK, gin.H{
		"status":       "started",
		"workflow_id":  workflowID,
		"case_id":      caseID,
		"task_id":      task.TaskID,
		"thread_id":    task.ThreadID,
		"patient_name": caseData["name"],
	})
}

// GetCaseStatus returns the current status of processing
func (h *ActorHandler) GetCaseStatus(c *gin.Context) {
	orchestrator, err := h.getOrchestrator()
	if err != nil {
		handleError(c, err)
		return
	}

	agentStatus := orchestrator.GetAgentStatus()
	currentCase := h.caseRepository.GetCurrentCase()

	if currentCase == nil {
		c.JSON(http.StatusOK, gin.H{"status": "idle"})
		return
	}

	allComplete := true
	for _, status := range agentStatus {
		if status != domain.AgentComplete {
			allComplete = false
			break
		}
	}

	status := "processing"
	if allComplete {
		status = "completed"
	}

	progress := make(map[string]int)
	for agent, status := range agentStatus {
		switch status {
		case domain.AgentIdle:
			progress[agent] = 0
		case domain.AgentBusy:
			progress[agent] = 50
		case domain.AgentComplete:
			progress[agent] = 100
		default:
			progress[agent] = 0
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       status,
		"case_id":      currentCase["id"],
		"patient_data": currentCase,
		"agent_status": agentStatus,
		"progress":     progress,
	})
}

// GetResults returns the results for a processed case
func (h *ActorHandler) GetResults(c *gin.Context) {
	caseID := c.Param("case_id")

	results, err := h.resultRepository.GetResultsByCaseID(caseID)
	if err != nil {
		handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, results)
}
