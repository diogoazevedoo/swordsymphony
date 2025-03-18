package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

	resultRepo := a.resultRepository
	if resultRepo != nil {
		results := map[string]any{
			"diagnosis":      diagnosis,
			"treatment_plan": treatmentPlan,
		}

		caseID := ""
		if patientData != nil {
			if caseIdentifier, ok := patientData["case_id"].(string); ok && caseIdentifier != "" {
				caseID = caseIdentifier
			} else if id, ok := patientData["id"].(string); ok && id != "" {
				caseID = id
			}
		}

		if caseID != "" {
			if err := resultRepo.StoreResults(caseID, results); err != nil {
				log.Printf("Failed to store results: %v", err)
			}
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

	aiResponse, err := a.aiClient.GenerateCompletion(ctx, prompt, ai.CompletionOptions{
		MaxTokens:    1024,
		Temperature:  0.3,
		ModelName:    "gpt-4",
		SystemPrompt: "You are an expert healthcare provider with extensive knowledge of treatment protocols and medication management.",
	})

	if err != nil {
		logger.Error("AI treatment plan generation failed", "error", err)
		return map[string]any{
			"recommendations": []string{"Error in treatment planning process", "Please consult with a healthcare provider"},
			"warnings":        []string{"Treatment plan could not be generated due to AI service error: " + err.Error()},
			"follow_up":       []string{"Schedule appointment with primary care physician or specialist"},
		}
	}

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
