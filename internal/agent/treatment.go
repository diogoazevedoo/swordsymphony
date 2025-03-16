package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
)

// TreatmentAgent develops treatment plans based on diagnoses
type TreatmentAgent struct {
	BaseAgent
	treatmentPlans    map[string]any
	aiClient          ai.Client
	knowledgeBase     *knowledge.MedicalKnowledgeBase
	resultRepository  repository.ResultRepository
	treatmentTemplate string
}

// NewTreatmentAgent creates a new treatment agent with AI capabilities
func NewTreatmentAgent(
	aiClient ai.Client,
	kb *knowledge.MedicalKnowledgeBase,
	resultRepo repository.ResultRepository,
) *TreatmentAgent {
	return &TreatmentAgent{
		BaseAgent:        NewBaseAgent(string(domain.TreatmentAgentType), domain.GetAgentName(domain.TreatmentAgentType)),
		treatmentPlans:   make(map[string]any),
		aiClient:         aiClient,
		knowledgeBase:    kb,
		resultRepository: resultRepo,
		treatmentTemplate: `
You are an experienced medical treatment specialist.
Based on the following patient information and diagnosis, create a comprehensive treatment plan:

PATIENT INFORMATION:
Age: {{.Age}}
Gender: {{.Gender}}
Medical Conditions: {{.Conditions}}
Current Medications: {{.Medications}}
Allergies: {{.Allergies}}

DIAGNOSIS:
{{.Diagnosis}}

DIAGNOSTIC REASONING:
{{.Reasoning}}

Please provide:
1. Recommended treatments in order of priority
2. Medication recommendations with dosing
3. Lifestyle modifications
4. Follow-up care plan
5. Warning signs to watch for
6. Any contraindications or precautions

Format your response as JSON with the following structure:
{
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "medications": ["Medication 1", "Medication 2"],
  "lifestyle_changes": ["Change 1", "Change 2"],
  "follow_up": ["Follow-up 1", "Follow-up 2"],
  "warnings": ["Warning 1", "Warning 2"],
  "contraindications": ["Contraindication 1", "Contraindication 2"]
}`,
	}
}

// ProcessMessage handles incoming messages for the treatment agent
func (a *TreatmentAgent) ProcessMessage(msg domain.Message) []domain.Message {
	if msg.MessageType != domain.DiagnosisResults {
		return nil
	}

	a.SetStatus(domain.AgentBusy)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["patient_data"].(map[string]any)
	diagnosis, _ := msg.Content["diagnosis"].(map[string]any)
	threadID := msg.ThreadID

	a.UpdateKnowledge("current_patient", patientData)
	a.UpdateKnowledge("current_diagnosis", diagnosis)

	responses := []domain.Message{
		*domain.NewMessage(
			a.ID(),
			a.Name(),
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
		a.ID(),
		a.Name(),
		string(domain.OrchestratorAgentType),
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "developing_treatment",
			"progress": 50,
			"message":  "Developing treatment options",
		},
	))

	treatmentPlan := a.createTreatmentPlan(patientData, diagnosis)

	a.treatmentPlans[taskID] = treatmentPlan
	a.UpdateKnowledge("current_treatment_plan", treatmentPlan)

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
		a.ID(),
		a.Name(),
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
		a.ID(),
		a.Name(),
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
		a.ID(),
		a.Name(),
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
	}

	a.SetStatus(domain.AgentComplete)

	return responses
}

// createTreatmentPlan develops a personalized treatment plan using AI
func (a *TreatmentAgent) createTreatmentPlan(patientData, diagnosis map[string]any) map[string]any {
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

	aiResponse, err := a.aiClient.GenerateCompletion(prompt, ai.CompletionOptions{
		MaxTokens:    1024,
		Temperature:  0.3,
		ModelName:    "gpt-4",
		SystemPrompt: "You are an AI medical assistant with expertise in treatment planning.",
	})

	if err != nil {
		return map[string]any{
			"recommendations": []string{"Error in treatment planning process", "Please consult with a healthcare provider"},
			"warnings":        []string{"Treatment plan could not be generated due to AI service error: " + err.Error()},
			"follow_up":       []string{"Schedule appointment with primary care physician or specialist"},
		}
	}

	var aiTreatment map[string]any
	if err := json.Unmarshal([]byte(aiResponse.Text), &aiTreatment); err != nil {
		return map[string]any{
			"recommendations": []string{"Treatment information available but not properly formatted"},
			"warnings":        []string{"AI treatment information could not be parsed"},
			"raw_treatment":   aiResponse.Text,
			"follow_up":       []string{"Please review the treatment information and consult with a healthcare provider"},
		}
	}

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
