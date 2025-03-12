package agent

import (
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// IntakeAgent processes and normalizes patient data
type IntakeAgent struct {
	BaseAgent
}

// NewIntakeAgent creates a new intake agent
func NewIntakeAgent(id string, name string) *IntakeAgent {
	return &IntakeAgent{
		BaseAgent: NewBaseAgent(id, name),
	}
}

// ProcessMessage handles incoming messages for the intake agent
func (a *IntakeAgent) ProcessMessage(msg domain.Message) []domain.Message {
	// Only process task assignments
	if msg.MessageType != domain.TaskAssignment {
		return nil
	}

	a.SetStatus(domain.AgentBusy)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["data"].(map[string]any)
	threadID := msg.ThreadID

	processedData := a.processPatientData(patientData)

	a.UpdateKnowledge("current_patient", processedData)

	responses := []domain.Message{
		// Status update to orchestrator
		*domain.NewMessage(
			a.ID(),
			a.Name(),
			"orchestrator",
			domain.StatusUpdate,
			map[string]any{
				"task_id":  taskID,
				"status":   "processing",
				"progress": 50,
				"message":  "Processing patient data",
			},
		),

		// Send data to diagnostic agent
		*domain.NewMessage(
			a.ID(),
			a.Name(),
			"diagnostic_agent",
			domain.ProcessedData,
			map[string]interface{}{
				"task_id":    taskID,
				"data":       processedData,
				"context":    "Patient intake complete",
				"confidence": 0.95,
				"reasoning":  "Extracted from patient records and normalized",
			},
		),

		// Task completion notification
		*domain.NewMessage(
			a.ID(),
			a.Name(),
			"orchestrator",
			domain.TaskComplete,
			map[string]interface{}{
				"task_id": taskID,
				"status":  "completed",
				"message": "Patient data processing complete",
			},
		),
	}

	for i := range responses {
		responses[i].ThreadID = threadID
	}

	a.SetStatus(domain.AgentComplete)

	return responses
}

// processPatientData normalizes and enhances patient data
func (a *IntakeAgent) processPatientData(rawData map[string]any) map[string]any {
	// TODO: a better implementation of this will be capable to normalize and validate data from various sources

	processed := make(map[string]any)

	for k, v := range rawData {
		processed[k] = v
	}

	riskFactors := []string{}

	if age, ok := rawData["age"].(float64); ok && age > 60 {
		riskFactors = append(riskFactors, "age")
	}

	if medications, ok := rawData["medications"].([]any); ok && len(medications) > 3 {
		riskFactors = append(riskFactors, "polypharmacy")
	}

	processed["risk_factors"] = riskFactors

	processed["processed_at"] = time.Now().Format(time.RFC3339)
	processed["processed_by"] = a.ID()

	return processed
}
