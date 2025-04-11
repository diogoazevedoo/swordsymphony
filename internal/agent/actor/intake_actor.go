package actor

import (
	"context"
	"time"

	"maps"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// IntakeActor processes and normalizes patient data
type IntakeActor struct {
	*actor.BaseActor
	status domain.AgentStatus
}

// NewIntakeActor creates a new intake actor
func NewIntakeActor(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
	baseActor := actor.NewBaseActor(actor.Address(domain.IntakeAgentType), config, system)

	return &IntakeActor{
		BaseActor: baseActor,
		status:    domain.AgentIdle,
	}, nil
}

// Start initializes the intake actor
func (a *IntakeActor) Start() error {
	logger.Info("Intake actor starting")
	a.status = domain.AgentIdle
	return nil
}

// Stop gracefully shuts down the intake actor
func (a *IntakeActor) Stop() error {
	logger.Info("Intake actor stopping")
	return nil
}

// Receive processes incoming messages
func (a *IntakeActor) Receive(ctx context.Context, envelope *actor.Envelope) error {
	msg := envelope.Message

	if msg.MessageType != domain.TaskAssignment {
		return nil
	}

	a.status = domain.AgentBusy
	logger.Info("Intake actor processing message", "message_type", msg.MessageType)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["data"].(map[string]any)
	documentAnalyses, hasDocAnalyses := msg.Content["document_analyses"]
	threadID := msg.ThreadID

	processedData := a.processPatientData(patientData)

	if hasDocAnalyses {
		logger.Info("Including document analyses in processed data",
			"has_analyses", hasDocAnalyses)
		processedData["document_analyses"] = documentAnalyses
	}

	a.SetState("current_patient", processedData)

	responses := []domain.Message{
		*domain.NewMessage(
			string(a.Address()),
			domain.GetAgentName(domain.IntakeAgentType),
			string(domain.OrchestratorAgentType),
			domain.StatusUpdate,
			map[string]any{
				"task_id":  taskID,
				"status":   "processing",
				"progress": 50,
				"message":  "Processing patient data",
			},
		),

		*domain.NewMessage(
			string(a.Address()),
			domain.GetAgentName(domain.IntakeAgentType),
			string(domain.DiagnosticAgentType),
			domain.ProcessedData,
			map[string]any{
				"task_id":    taskID,
				"data":       processedData,
				"context":    "Patient intake complete",
				"confidence": 0.95,
				"reasoning":  "Extracted from patient records and normalized",
			},
		),

		*domain.NewMessage(
			string(a.Address()),
			domain.GetAgentName(domain.IntakeAgentType),
			string(domain.OrchestratorAgentType),
			domain.TaskComplete,
			map[string]any{
				"task_id": taskID,
				"status":  "completed",
				"message": "Patient data processing complete",
			},
		),
	}

	for i := range responses {
		responses[i].ThreadID = threadID
		err := a.Send(actor.Address(responses[i].Recipient), responses[i])
		if err != nil {
			logger.Error("Failed to send message",
				"error", err,
				"recipient", responses[i].Recipient,
				"message_type", responses[i].MessageType)
		}
	}

	a.status = domain.AgentComplete

	return nil
}

// processPatientData normalizes and enhances patient data
func (a *IntakeActor) processPatientData(rawData map[string]any) map[string]any {
	processed := make(map[string]any)

	maps.Copy(processed, rawData)

	riskFactors := []string{}

	if age, ok := rawData["age"].(float64); ok && age > 60 {
		riskFactors = append(riskFactors, "age")
	}

	if medications, ok := rawData["medications"].([]any); ok && len(medications) > 3 {
		riskFactors = append(riskFactors, "polypharmacy")
	}

	processed["risk_factors"] = riskFactors

	processed["processed_at"] = time.Now().Format(time.RFC3339)
	processed["processed_by"] = string(a.Address())

	return processed
}
