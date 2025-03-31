package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
)

// TreatmentActor develops treatment plans based on diagnoses
type TreatmentActor struct {
	*actor.BaseActor
	status            domain.AgentStatus
	aiClient          ai.Client
	knowledgeBase     *knowledge.MedicalKnowledgeBase
	resultRepository  repository.ResultRepository
	treatmentTemplate string
}

// NewTreatmentActor creates a new treatment actor
func NewTreatmentActor(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem,
	aiClient ai.Client, kb *knowledge.MedicalKnowledgeBase, resultRepo repository.ResultRepository) (actor.Actor, error) {

	baseActor := actor.NewBaseActor(actor.Address(domain.TreatmentAgentType), config, system)

	return &TreatmentActor{
		BaseActor:        baseActor,
		status:           domain.AgentIdle,
		aiClient:         aiClient,
		knowledgeBase:    kb,
		resultRepository: resultRepo,
		treatmentTemplate: `
You are a senior medical specialist with expertise in developing comprehensive treatment plans.
Based on the provided patient information and diagnosis, create an evidence-based treatment plan.

PATIENT INFORMATION:
Age: {{.Age}}
Gender: {{.Gender}}
Medical History: {{.Conditions}}
Current Medications: {{.Medications}}
Known Allergies: {{.Allergies}}

DIAGNOSTIC ASSESSMENT:
Primary Diagnosis: {{.Diagnosis}}

DIAGNOSTIC REASONING:
{{.Reasoning}}

TREATMENT PLAN INSTRUCTIONS:
1. Recommend treatments in order of priority, including non-pharmaceutical interventions
2. Suggest appropriate medications with specific dosing information
3. Recommend lifestyle modifications tailored to the patient's condition
4. Develop a follow-up care plan with specific timeframes
5. List important warning signs that would necessitate immediate medical attention
6. Note any contraindications or precautions based on the patient's profile

FORMAT YOUR RESPONSE AS JSON:
{
  "recommendations": [
    "First-line treatment recommendation",
    "Second-line treatment recommendation",
    "Additional treatment considerations"
  ],
  "medications": [
    "Medication 1: dosage, frequency, duration",
    "Medication 2: dosage, frequency, duration"
  ],
  "lifestyle_changes": [
    "Specific lifestyle modification 1",
    "Specific lifestyle modification 2"
  ],
  "follow_up": [
    "Immediate follow-up in X weeks",
    "Tests to perform at follow-up",
    "Monitoring parameters"
  ],
  "warnings": [
    "Warning sign 1 requiring immediate attention",
    "Warning sign 2 requiring immediate attention"
  ],
  "contraindications": [
    "Specific contraindication 1",
    "Specific contraindication 2"
  ]
}

TREATMENT CONSIDERATIONS:
- Prioritize evidence-based treatments appropriate for the diagnosed condition
- Consider the patient's age, comorbidities, and current medications when recommending treatments
- Include both pharmacological and non-pharmacological interventions where appropriate
- Be specific about medication dosing, frequency, and duration
- Consider treatment cost and accessibility when making recommendations`,
	}, nil
}

// Start initializes the treatment actor
func (a *TreatmentActor) Start() error {
	logger.Info("Treatment actor starting")
	a.status = domain.AgentIdle
	return nil
}

// Stop gracefully shuts down the treatment actor
func (a *TreatmentActor) Stop() error {
	logger.Info("Treatment actor stopping")
	return nil
}

// Receive processes incoming messages
func (a *TreatmentActor) Receive(ctx context.Context, envelope *actor.Envelope) error {
	msg := envelope.Message

	if msg.MessageType != domain.DiagnosisResults {
		return nil
	}

	a.status = domain.AgentBusy
	logger.Info("Treatment actor processing message", "message_type", msg.MessageType)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["patient_data"].(map[string]any)
	diagnosis, _ := msg.Content["diagnosis"].(map[string]any)
	threadID := msg.ThreadID

	a.SetState("current_patient", patientData)
	a.SetState("current_diagnosis", diagnosis)

	responses := []domain.Message{
		*domain.NewMessage(
			string(a.Address()),
			domain.GetAgentName(domain.TreatmentAgentType),
			string(domain.OrchestratorAgentType),
			domain.StatusUpdate,
			map[string]any{
				"task_id":  taskID,
				"status":   "developing_treatment",
				"progress": 25,
				"message":  "Reviewing diagnosis",
			},
		),
	}

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.TreatmentAgentType),
		string(domain.OrchestratorAgentType),
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "developing_treatment",
			"progress": 50,
			"message":  "Developing treatment options",
		},
	))

	treatmentPlan := a.createTreatmentPlan(ctx, patientData, diagnosis)

	a.SetState("current_treatment_plan", treatmentPlan)

	// Get the case ID from the patient data
	caseID := a.extractCaseID(patientData)

	// Log the extracted case ID
	logger.Info("Extracted case ID for treatment storage",
		"case_id", caseID,
		"method", "direct extraction")

	// Store the results
	if a.resultRepository != nil && caseID != "" {
		results := map[string]any{
			"diagnosis":      diagnosis,
			"treatment_plan": treatmentPlan,
			"timestamp":      time.Now().Format(time.RFC3339),
			"workflow_step":  "treatment",
			"agent_type":     "treatment_agent",
		}

		// Attempt to store the results
		err := a.resultRepository.StoreResults(caseID, results)
		if err != nil {
			logger.Error("Failed to store treatment results",
				"case_id", caseID,
				"error", err)
		} else {
			logger.Info("Successfully stored treatment results in repository",
				"case_id", caseID,
				"result_keys", getMapKeys(results))

			// Verify that results were stored by attempting to retrieve them
			verifyResults, verifyErr := a.resultRepository.GetResultsByCaseID(caseID)
			if verifyErr != nil {
				logger.Error("Failed to verify result storage - cannot retrieve results",
					"case_id", caseID,
					"error", verifyErr)
			} else {
				logger.Info("Verified result storage - results retrieved successfully",
					"case_id", caseID,
					"result_keys", getMapKeys(verifyResults))
			}
		}
	} else {
		if a.resultRepository == nil {
			logger.Error("Cannot store treatment results - result repository is nil")
		}
		if caseID == "" {
			logger.Error("Cannot store treatment results - case ID is empty",
				"patient_data_keys", getMapKeys(patientData))
		}
	}

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.TreatmentAgentType),
		string(domain.OrchestratorAgentType),
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "developing_treatment",
			"progress": 90,
			"message":  "Finalizing treatment plan",
		},
	))

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.TreatmentAgentType),
		string(domain.OrchestratorAgentType),
		domain.TreatmentPlan,
		map[string]any{
			"task_id":        taskID,
			"patient_data":   patientData,
			"diagnosis":      diagnosis,
			"treatment_plan": treatmentPlan,
			"warnings":       treatmentPlan["warnings"],
			"is_final":       true,
		},
	))

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.TreatmentAgentType),
		string(domain.OrchestratorAgentType),
		domain.TaskComplete,
		map[string]any{
			"task_id": taskID,
			"status":  "completed",
			"message": "Treatment planning complete",
		},
	))

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

// createTreatmentPlan develops a personalized treatment plan using AI
func (a *TreatmentActor) createTreatmentPlan(ctx context.Context, patientData, diagnosis map[string]any) map[string]any {
	age := getFloat64(patientData, "age")
	gender := getString(patientData, "gender")
	conditions := getStringSlice(patientData, "conditions")
	medications := getStringSlice(patientData, "medications")
	allergies := getStringSlice(patientData, "allergies")

	diagnoses := getStringSlice(diagnosis, "potential_diagnoses")
	reasoning := getStringSlice(diagnosis, "reasoning")

	var interactions []knowledge.InteractionRule
	if a.knowledgeBase != nil {
		interactions = a.knowledgeBase.CheckMedicationInteractions(medications)
	}

	prompt := strings.ReplaceAll(a.treatmentTemplate, "{{.Age}}", fmt.Sprintf("%.0f", age))
	prompt = strings.ReplaceAll(prompt, "{{.Gender}}", gender)
	prompt = strings.ReplaceAll(prompt, "{{.Conditions}}", strings.Join(conditions, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Medications}}", strings.Join(medications, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Allergies}}", strings.Join(allergies, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Diagnosis}}", strings.Join(diagnoses, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Reasoning}}", strings.Join(reasoning, ", "))

	if len(interactions) > 0 {
		interactionInfo := "MEDICATION INTERACTIONS TO CONSIDER:\n"
		for _, interaction := range interactions {
			interactionInfo += fmt.Sprintf("- %s and %s: %s (%s severity)\n",
				interaction.Medication1,
				interaction.Medication2,
				interaction.Description,
				interaction.Severity)
		}
		prompt += "\n\n" + interactionInfo
	}

	prompt = "IMPORTANT: Your response MUST be a valid JSON object in the following format:\n" + prompt

	logger.Info("Patient data for diagnosis", "data", patientData)
	logger.Info("Sending diagnostic prompt to AI", "prompt_length", len(prompt))

	aiResponse, err := a.aiClient.GenerateCompletion(ctx, prompt, ai.CompletionOptions{
		MaxTokens:    1024,
		Temperature:  0.3,
		ModelName:    "gpt-4",
		SystemPrompt: "You are an expert healthcare provider with extensive knowledge of treatment protocols and medication management.",
	})

	if err != nil {
		logger.Error("AI generation failed",
			"error", err,
			"step", "treatment",
			"agent", string(a.Address()))
	}

	logger.Info("Received AI response",
		"response_length", len(aiResponse.Text),
		"step", "treatment",
		"agent", string(a.Address()))

	var aiTreatment map[string]any

	treatmentText := extractJSON(aiResponse.Text)

	if err := json.Unmarshal([]byte(treatmentText), &aiTreatment); err != nil {
		logger.Error("Failed to parse AI treatment response",
			"error", err,
			"response", aiResponse.Text)

		return map[string]any{
			"recommendations": []string{"Treatment information available but not properly formatted"},
			"warnings":        []string{"AI treatment information could not be parsed"},
			"raw_treatment":   aiResponse.Text,
			"follow_up":       []string{"Please review the treatment information and consult with a healthcare provider"},
		}
	}

	validateAndNormalizeTreatmentResponse(aiTreatment)

	if len(interactions) > 0 {
		warnings := make([]any, 0)
		if w, ok := aiTreatment["warnings"].([]any); ok {
			warnings = w
		}

		for _, interaction := range interactions {
			warningText := fmt.Sprintf(
				"CAUTION: Potential %s interaction between %s and %s: %s",
				interaction.Severity,
				interaction.Medication1,
				interaction.Medication2,
				interaction.Description)

			alreadyIncluded := false
			for _, w := range warnings {
				if wStr, ok := w.(string); ok &&
					strings.Contains(wStr, interaction.Medication1) &&
					strings.Contains(wStr, interaction.Medication2) {
					alreadyIncluded = true
					break
				}
			}

			if !alreadyIncluded {
				warnings = append(warnings, warningText)
			}
		}

		aiTreatment["warnings"] = warnings
	}

	medicationRecs, ok := aiTreatment["medications"].([]any)
	if ok {
		medicationWarnings := []string{}

		for _, med := range medicationRecs {
			medStr, ok := med.(string)
			if ok {
				for _, allergy := range allergies {
					if strings.Contains(strings.ToLower(medStr), strings.ToLower(allergy)) {
						warning := fmt.Sprintf("WARNING: Recommended medication '%s' may conflict with patient allergy to '%s'",
							medStr, allergy)
						medicationWarnings = append(medicationWarnings, warning)
					}
				}
			}
		}

		if len(medicationWarnings) > 0 {
			warnings, ok := aiTreatment["warnings"].([]any)
			if !ok {
				warnings = make([]any, 0)
			}

			for _, warning := range medicationWarnings {
				warnings = append(warnings, warning)
			}
			aiTreatment["warnings"] = warnings
		}
	}

	return aiTreatment
}

func validateAndNormalizeTreatmentResponse(treatment map[string]any) {
	if _, exists := treatment["recommendations"]; !exists {
		treatment["recommendations"] = []string{"No specific recommendations provided"}
	} else if _, ok := treatment["recommendations"].([]any); !ok {
		treatment["recommendations"] = []string{"No specific recommendations provided"}
	}

	if _, exists := treatment["medications"]; !exists {
		treatment["medications"] = []string{}
	} else if _, ok := treatment["medications"].([]any); !ok {
		treatment["medications"] = []string{}
	}

	if _, exists := treatment["lifestyle_changes"]; !exists {
		treatment["lifestyle_changes"] = []string{}
	} else if _, ok := treatment["lifestyle_changes"].([]any); !ok {
		treatment["lifestyle_changes"] = []string{}
	}

	if _, exists := treatment["follow_up"]; !exists {
		treatment["follow_up"] = []string{"Schedule follow-up with healthcare provider"}
	} else if _, ok := treatment["follow_up"].([]any); !ok {
		treatment["follow_up"] = []string{"Schedule follow-up with healthcare provider"}
	}

	if _, exists := treatment["warnings"]; !exists {
		treatment["warnings"] = []string{}
	} else if _, ok := treatment["warnings"].([]any); !ok {
		treatment["warnings"] = []string{}
	}

	if _, exists := treatment["contraindications"]; !exists {
		treatment["contraindications"] = []string{}
	} else if _, ok := treatment["contraindications"].([]any); !ok {
		treatment["contraindications"] = []string{}
	}
}

// extractCaseID ensures consistent case ID extraction across the system
func (a *TreatmentActor) extractCaseID(patientData map[string]any) string {
	// Try different possible keys and paths for finding the case ID
	caseIDPaths := [][]string{
		{"case_id"},         // Direct case_id field
		{"id"},              // Direct id field
		{"patient_id"},      // Direct patient_id field
		{"data", "id"},      // Nested in data.id
		{"data", "case_id"}, // Nested in data.case_id
	}

	// First try the direct paths
	for _, path := range caseIDPaths {
		if len(path) == 1 {
			// Try direct key
			if idVal, ok := patientData[path[0]].(string); ok && idVal != "" {
				logger.Info("Found case ID using direct path", "path", path[0], "value", idVal)
				return idVal
			}
		} else if len(path) == 2 {
			// Try nested structure
			if dataMap, ok := patientData[path[0]].(map[string]any); ok {
				if idVal, ok := dataMap[path[1]].(string); ok && idVal != "" {
					logger.Info("Found case ID using nested path", "path", path[0]+"."+path[1], "value", idVal)
					return idVal
				}
			}
		}
	}

	// If direct ID extraction fails, try using the conversation ID as a fallback
	if convID, ok := patientData["conversation_id"].(string); ok && convID != "" {
		logger.Info("Using conversation ID as case ID", "conversation_id", convID)
		return convID
	}

	// If there's a name field, use a combination of name and timestamp as ID
	if name, ok := patientData["name"].(string); ok && name != "" {
		id := fmt.Sprintf("%s_%d", strings.ReplaceAll(name, " ", "_"), time.Now().Unix())
		logger.Info("Generated case ID from name", "name", name, "id", id)
		return id
	}

	// Last resort - generate a unique ID
	generatedID := fmt.Sprintf("case_%d", time.Now().UnixNano())
	logger.Info("Generated fallback case ID", "id", generatedID)
	return generatedID
}

// getMapKeys returns a list of keys in a map
func getMapKeys(data map[string]any) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	return keys
}
