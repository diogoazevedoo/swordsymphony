package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetDemoCases returns available demo cases
func (h *Handler) GetDemoCases(c *gin.Context) {
	cases, err := h.caseRepository.GetDemoCases()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cases)
}

// StartCase initiates processing for a demo case
func (h *Handler) StartCase(c *gin.Context) {
	caseID := c.Param("case_id")

	caseData, err := h.caseRepository.GetCaseByID(caseID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Case not found"})
		return
	}

	taskID := h.orchestrator.StartTask(map[string]any{
		"patient_data": caseData,
		"scenario":     caseID,
	})

	c.JSON(http.StatusOK, gin.H{
		"status":       "started",
		"case_id":      caseID,
		"task_id":      taskID,
		"patient_name": caseData["name"],
	})
}

// GetCaseStatus returns the current status of processing
func (h *Handler) GetCaseStatus(c *gin.Context) {
	agentStatus := h.orchestrator.GetAgentStatus()

	currentCase := h.caseRepository.GetCurrentCase()

	if currentCase == nil {
		c.JSON(http.StatusOK, gin.H{"status": "idle"})
		return
	}

	progress := make(map[string]int)
	for agent, status := range agentStatus {
		switch status {
		case "idle":
			progress[agent] = 0
		case "busy":
			progress[agent] = 50
		case "complete":
			progress[agent] = 100
		default:
			progress[agent] = 0
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       "processing",
		"case_id":      currentCase["id"],
		"patient_data": currentCase,
		"agent_status": agentStatus,
		"progress":     progress,
	})
}

// GetResults returns the results for a processed case
func (h *Handler) GetResults(c *gin.Context) {
	caseID := c.Param("case_id")

	results, err := h.resultRepository.GetResultsByCaseID(caseID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No results found for this case"})
		return
	}

	c.JSON(http.StatusOK, results)
}
