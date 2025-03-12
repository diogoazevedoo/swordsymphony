package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
)

// DiagnosticAgent analyzes patient data to generate diagnoses
type DiagnosticAgent struct {
	BaseAgent
	diagnoses          map[string]any
	aiClient           ai.Client
	knowledgeBase      *knowledge.MedicalKnowledgeBase
	diagnosticTemplate string
}

// NewDiagnosticAgent creates a new diagnostic agent with AI capabilities
func NewDiagnosticAgent(
	id, name string,
	aiClient ai.Client,
	kb *knowledge.MedicalKnowledgeBase,
) *DiagnosticAgent {
	return &DiagnosticAgent{
		BaseAgent:     NewBaseAgent(id, name),
		diagnoses:     make(map[string]any),
		aiClient:      aiClient,
		knowledgeBase: kb,
		diagnosticTemplate: `
You are an experienced medical diagnostician. 
Given the following patient information, provide a diagnostic assessment:

PATIENT INFORMATION:
Age: {{.Age}}
Gender: {{.Gender}}
Medical Conditions: {{.Conditions}}
Current Medications: {{.Medications}}
Allergies: {{.Allergies}}

PRESENTING SYMPTOMS:
{{.Symptoms}}

VITAL SIGNS:
{{.Vitals}}

Please provide:
1. Most likely diagnoses in order of probability
2. Clinical reasoning for each diagnosis
3. Key risk factors present
4. Recommended diagnostic tests
5. Confidence level for your top diagnosis (0-100%)

Format your response as JSON with the following structure:
{
  "potential_diagnoses": ["Diagnosis 1", "Diagnosis 2"],
  "reasoning": ["Reason 1", "Reason 2"],
  "risk_factors": ["Risk Factor 1", "Risk Factor 2"],
  "recommended_tests": ["Test 1", "Test 2"],
  "confidence": 85
}`,
	}
}

// ProcessMessage handles incoming messages for the diagnostic agent
func (a *DiagnosticAgent) ProcessMessage(msg domain.Message) []domain.Message {
	if msg.MessageType != domain.ProcessedData {
		return nil
	}

	a.SetStatus(domain.AgentBusy)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["data"].(map[string]any)
	threadID := msg.ThreadID

	a.UpdateKnowledge("current_patient", patientData)

	responses := []domain.Message{
		*domain.NewMessage(
			a.ID(),
			a.Name(),
			"orchestrator",
			domain.StatusUpdate,
			map[string]any{
				"task_id":  taskID,
				"status":   "diagnosing",
				"progress": 25,
				"message":  "Starting diagnostic analysis",
			},
		),
	}

	responses = append(responses, *domain.NewMessage(
		a.ID(),
		a.Name(),
		"orchestrator",
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "diagnosing",
			"progress": 50,
			"message":  "Analyzing symptoms and medical history",
		},
	))

	diagnosis := a.analyzeSymptoms(patientData)

	a.diagnoses[taskID] = diagnosis
	a.UpdateKnowledge("current_diagnosis", diagnosis)

	responses = append(responses, *domain.NewMessage(
		a.ID(),
		a.Name(),
		"orchestrator",
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "diagnosing",
			"progress": 90,
			"message":  "Finalizing diagnosis",
		},
	))

	responses = append(responses, *domain.NewMessage(
		a.ID(),
		a.Name(),
		"treatment_agent",
		domain.DiagnosisResults,
		map[string]any{
			"task_id":      taskID,
			"patient_data": patientData,
			"diagnosis":    diagnosis,
			"confidence":   diagnosis["confidence"],
			"reasoning":    diagnosis["reasoning"],
		},
	))

	responses = append(responses, *domain.NewMessage(
		a.ID(),
		a.Name(),
		"orchestrator",
		domain.TaskComplete,
		map[string]any{
			"task_id": taskID,
			"status":  "completed",
			"message": "Diagnostic analysis complete",
		},
	))

	for i := range responses {
		responses[i].ThreadID = threadID
	}

	a.SetStatus(domain.AgentComplete)

	return responses
}

// analyzeSymptoms uses AI and knowledge base to analyze patient data
func (a *DiagnosticAgent) analyzeSymptoms(patientData map[string]any) map[string]any {
	symptoms := getStringSlice(patientData, "symptoms")
	conditions := getStringSlice(patientData, "conditions")
	medications := getStringSlice(patientData, "medications")
	allergies := getStringSlice(patientData, "allergies")
	age := getFloat64(patientData, "age")
	gender := getString(patientData, "gender")
	vitals := getMap(patientData, "vitals")

	var relatedConditions []knowledge.Condition
	if a.knowledgeBase != nil {
		relatedConditions = a.knowledgeBase.GetRelatedConditions(symptoms)
	}

	var interactions []knowledge.InteractionRule
	if a.knowledgeBase != nil {
		interactions = a.knowledgeBase.CheckMedicationInteractions(medications)
	}

	prompt := strings.ReplaceAll(a.diagnosticTemplate, "{{.Age}}", fmt.Sprintf("%.0f", age))
	prompt = strings.ReplaceAll(prompt, "{{.Gender}}", gender)
	prompt = strings.ReplaceAll(prompt, "{{.Conditions}}", strings.Join(conditions, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Medications}}", strings.Join(medications, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Allergies}}", strings.Join(allergies, ", "))
	prompt = strings.ReplaceAll(prompt, "{{.Symptoms}}", strings.Join(symptoms, ", "))

	vitalsText := ""
	if bp := getString(vitals, "blood_pressure"); bp != "" {
		vitalsText += fmt.Sprintf("Blood Pressure: %s\n", bp)
	}
	if hr := getFloat64(vitals, "heart_rate"); hr > 0 {
		vitalsText += fmt.Sprintf("Heart Rate: %.0f bpm\n", hr)
	}
	if temp := getFloat64(vitals, "temperature"); temp > 0 {
		vitalsText += fmt.Sprintf("Temperature: %.1f°F\n", temp)
	}
	if o2 := getFloat64(vitals, "oxygen_saturation"); o2 > 0 {
		vitalsText += fmt.Sprintf("Oxygen Saturation: %.0f%%\n", o2)
	}
	prompt = strings.ReplaceAll(prompt, "{{.Vitals}}", vitalsText)

	if len(relatedConditions) > 0 {
		prompt += "\n\nMEDICAL KNOWLEDGE BASE INFORMATION:\n"
		for i, condition := range relatedConditions {
			if i > 2 {
				break
			}
			prompt += fmt.Sprintf("- Condition: %s - %s\n", condition.Name, condition.Description)
		}
	}

	if len(interactions) > 0 {
		prompt += "\n\nMEDICATION INTERACTIONS:\n"
		for _, interaction := range interactions {
			prompt += fmt.Sprintf("- %s and %s: %s (%s severity)\n",
				interaction.Medication1,
				interaction.Medication2,
				interaction.Description,
				interaction.Severity)
		}
	}

	aiResponse, err := a.aiClient.GenerateCompletion(prompt, ai.CompletionOptions{
		MaxTokens:    1024,
		Temperature:  0.3,
		ModelName:    "gpt-4",
		SystemPrompt: "You are an AI medical assistant with expertise in diagnostics.",
	})

	if err != nil {
		return map[string]any{
			"potential_diagnoses": []string{"Error in diagnostic process"},
			"reasoning":           []string{"Unable to complete diagnosis due to AI service error: " + err.Error()},
			"recommended_tests":   []string{"Please consult with a healthcare provider"},
			"confidence":          0.0,
		}
	}

	var aiDiagnosis map[string]any
	if err := json.Unmarshal([]byte(aiResponse.Text), &aiDiagnosis); err != nil {
		return map[string]any{
			"potential_diagnoses": []string{"Diagnostic information available but not properly formatted"},
			"reasoning":           []string{"AI diagnostic information: " + aiResponse.Text},
			"recommended_tests":   []string{"Please review the diagnostic information and consult with a healthcare provider"},
			"confidence":          0.5,
		}
	}

	if len(relatedConditions) > 0 && aiDiagnosis["reasoning"] != nil {
		reasoning, ok := aiDiagnosis["reasoning"].([]any)
		if ok {
			kbInsight := fmt.Sprintf("Knowledge base suggests possible relation to: %s",
				relatedConditions[0].Name)

			alreadyIncluded := false
			for _, r := range reasoning {
				if rStr, ok := r.(string); ok && strings.Contains(rStr, relatedConditions[0].Name) {
					alreadyIncluded = true
					break
				}
			}

			if !alreadyIncluded {
				aiDiagnosis["reasoning"] = append(reasoning, kbInsight)
			}
		}
	}

	if len(interactions) > 0 {
		warnings := make([]string, 0)
		if w, ok := aiDiagnosis["medication_warnings"].([]any); ok {
			for _, warning := range w {
				if wStr, ok := warning.(string); ok {
					warnings = append(warnings, wStr)
				}
			}
		}

		for _, interaction := range interactions {
			interactionWarning := fmt.Sprintf(
				"Potential %s interaction between %s and %s: %s",
				interaction.Severity,
				interaction.Medication1,
				interaction.Medication2,
				interaction.Description)

			alreadyIncluded := false
			for _, w := range warnings {
				if strings.Contains(w, interaction.Medication1) &&
					strings.Contains(w, interaction.Medication2) {
					alreadyIncluded = true
					break
				}
			}

			if !alreadyIncluded {
				warnings = append(warnings, interactionWarning)
			}
		}

		if len(warnings) > 0 {
			aiDiagnosis["medication_warnings"] = warnings
		}
	}

	if confidence, ok := aiDiagnosis["confidence"].(float64); ok {
		aiDiagnosis["confidence"] = confidence / 100.0
	}

	return aiDiagnosis
}

// Helper functions

func getStringSlice(data map[string]any, key string) []string {
	if data == nil {
		return []string{}
	}

	if val, ok := data[key]; ok {
		if slice, ok := val.([]string); ok {
			return slice
		}
		if slice, ok := val.([]any); ok {
			result := make([]string, 0, len(slice))
			for _, item := range slice {
				if str, ok := item.(string); ok {
					result = append(result, str)
				}
			}
			return result
		}
	}
	return []string{}
}

func getFloat64(data map[string]any, key string) float64 {
	if data == nil {
		return 0
	}

	if val, ok := data[key]; ok {
		if f, ok := val.(float64); ok {
			return f
		}
		if i, ok := val.(int); ok {
			return float64(i)
		}
	}
	return 0
}

func getString(data map[string]any, key string) string {
	if data == nil {
		return ""
	}

	if val, ok := data[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getMap(data map[string]any, key string) map[string]any {
	if data == nil {
		return nil
	}

	if val, ok := data[key]; ok {
		if m, ok := val.(map[string]any); ok {
			return m
		}
	}
	return nil
}
