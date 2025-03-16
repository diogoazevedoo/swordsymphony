package domain

// AgentType represents a specific type of agent in the system
type AgentType string

const (
	// IntakeAgentType processes and normalizes patient data
	IntakeAgentType AgentType = "intake_agent"

	// DiagnosticAgentType analyzes patient data to generate diagnoses
	DiagnosticAgentType AgentType = "diagnostic_agent"

	// TreatmentAgentType develops treatment plans based on diagnoses
	TreatmentAgentType AgentType = "treatment_agent"

	// OrchestratorAgentType represents the orchestrator itself
	OrchestratorAgentType AgentType = "orchestrator"
)

// AgentNameMap maps agent types to human-readable names
var AgentNameMap = map[AgentType]string{
	IntakeAgentType:       "Patient Intake Agent",
	DiagnosticAgentType:   "Diagnostic Agent",
	TreatmentAgentType:    "Treatment Agent",
	OrchestratorAgentType: "Orchestrator",
}

// GetAgentName returns the human-readable name for an agent type
func GetAgentName(agentType AgentType) string {
	if name, exists := AgentNameMap[agentType]; exists {
		return name
	}
	return string(agentType)
}
