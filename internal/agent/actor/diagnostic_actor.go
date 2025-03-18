package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// DiagnosticActor analyzes patient data to generate diagnoses
type DiagnosticActor struct {
	*actor.BaseActor
	status             domain.AgentStatus
	aiClient           ai.Client
	knowledgeBase      *knowledge.MedicalKnowledgeBase
	diagnosticTemplate string
}

// NewDiagnosticActor creates a new diagnostic actor
func NewDiagnosticActor(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem,
	aiClient ai.Client, kb *knowledge.MedicalKnowledgeBase) (actor.Actor, error) {

	baseActor := actor.NewBaseActor(actor.Address(domain.DiagnosticAgentType), config, system)

	return &DiagnosticActor{
		BaseActor:     baseActor,
		status:        domain.AgentIdle,
		aiClient:      aiClient,
		knowledgeBase: kb,
		diagnosticTemplate: `
You are an expert medical diagnostician with years of clinical experience. 
Based on the patient information provided, generate a detailed diagnostic assessment.

PATIENT INFORMATION:
Age: {{.Age}}
Gender: {{.Gender}}
Medical History: {{.Conditions}}
Current Medications: {{.Medications}}
Known Allergies: {{.Allergies}}

PRESENTING SYMPTOMS:
{{.Symptoms}}

VITAL SIGNS:
{{.Vitals}}

ASSESSMENT INSTRUCTIONS:
1. Consider the most likely diagnoses based on the symptoms, vital signs, and patient history
2. For each potential diagnosis, provide medical reasoning explaining why it's a possibility
3. Note any relevant risk factors present in the patient profile
4. Suggest appropriate diagnostic tests to confirm or rule out each diagnosis
5. Assign a confidence level (0-100%) for your primary diagnosis based on available information

FORMAT YOUR RESPONSE AS JSON:
{
  "potential_diagnoses": ["Primary diagnosis", "Secondary diagnosis", "Tertiary diagnosis"],
  "reasoning": ["Clinical reasoning for primary diagnosis", "Reasoning for secondary", "Reasoning for tertiary"],
  "risk_factors": ["Specific risk factor 1", "Specific risk factor 2"],
  "recommended_tests": ["Specific test 1", "Specific test 2", "Specific test 3"],
  "confidence": 85
}

DIAGNOSTIC CONSIDERATIONS:
- Consider both common and uncommon causes of the presenting symptoms
- Pay special attention to any red flag symptoms that could indicate serious conditions
- Consider how medications and existing conditions might affect the presentation
- Think about age and gender-specific conditions that match the symptom profile`,
	}, nil
}

// Start initializes the diagnostic actor
func (a *DiagnosticActor) Start() error {
	logger.Info("Diagnostic actor starting")
	a.status = domain.AgentIdle
	return nil
}

// Stop gracefully shuts down the diagnostic actor
func (a *DiagnosticActor) Stop() error {
	logger.Info("Diagnostic actor stopping")
	return nil
}

// Receive processes incoming messages
func (a *DiagnosticActor) Receive(ctx context.Context, envelope *actor.Envelope) error {
	msg := envelope.Message

	if msg.MessageType != domain.ProcessedData {
		return nil
	}

	a.status = domain.AgentBusy
	logger.Info("Diagnostic actor processing message", "message_type", msg.MessageType)

	taskID, _ := msg.Content["task_id"].(string)
	patientData, _ := msg.Content["data"].(map[string]any)
	threadID := msg.ThreadID

	a.SetState("current_patient", patientData)

	responses := []domain.Message{
		*domain.NewMessage(
			string(a.Address()),
			domain.GetAgentName(domain.DiagnosticAgentType),
			string(domain.OrchestratorAgentType),
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
		string(a.Address()),
		domain.GetAgentName(domain.DiagnosticAgentType),
		string(domain.OrchestratorAgentType),
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "diagnosing",
			"progress": 50,
			"message":  "Analyzing symptoms and medical history",
		},
	))

	diagnosis := a.analyzeSymptoms(ctx, patientData)

	a.SetState("current_diagnosis", diagnosis)

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.DiagnosticAgentType),
		string(domain.OrchestratorAgentType),
		domain.StatusUpdate,
		map[string]any{
			"task_id":  taskID,
			"status":   "diagnosing",
			"progress": 90,
			"message":  "Finalizing diagnosis",
		},
	))

	responses = append(responses, *domain.NewMessage(
		string(a.Address()),
		domain.GetAgentName(domain.DiagnosticAgentType),
		string(domain.TreatmentAgentType),
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
		string(a.Address()),
		domain.GetAgentName(domain.DiagnosticAgentType),
		string(domain.OrchestratorAgentType),
		domain.TaskComplete,
		map[string]any{
			"task_id": taskID,
			"status":  "completed",
			"message": "Diagnostic analysis complete",
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

// analyzeSymptoms uses AI and knowledge base to analyze patient data
func (a *DiagnosticActor) analyzeSymptoms(ctx context.Context, patientData map[string]any) map[string]any {
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

	aiResponse, err := a.aiClient.GenerateCompletion(ctx, prompt, ai.CompletionOptions{
		MaxTokens:    1024,
		Temperature:  0.3,
		ModelName:    "gpt-4",
		SystemPrompt: "You are an expert medical diagnostician with comprehensive knowledge of medical conditions, symptoms, and diagnostic procedures.",
	})

	if err != nil {
		logger.Error("AI diagnostic generation failed", "error", err)
		return map[string]any{
			"potential_diagnoses": []string{"Error in diagnostic process"},
			"reasoning":           []string{"Unable to complete diagnosis due to AI service error: " + err.Error()},
			"recommended_tests":   []string{"Please consult with a healthcare provider"},
			"confidence":          0.0,
		}
	}

	var aiDiagnosis map[string]any

	diagnosisText := extractJSON(aiResponse.Text)

	if err := json.Unmarshal([]byte(diagnosisText), &aiDiagnosis); err != nil {
		logger.Error("Failed to parse AI diagnostic response",
			"error", err,
			"response", aiResponse.Text)

		return map[string]any{
			"potential_diagnoses": []string{"Diagnostic information available but not properly formatted"},
			"reasoning":           []string{"AI diagnostic information: " + aiResponse.Text},
			"recommended_tests":   []string{"Please review the diagnostic information and consult with a healthcare provider"},
			"confidence":          0.5,
		}
	}

	validateAndNormalizeDiagnosticResponse(aiDiagnosis)

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

func extractJSON(text string) string {
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")

	if jsonStart == -1 || jsonEnd == -1 || jsonEnd < jsonStart {
		return text
	}

	return text[jsonStart : jsonEnd+1]
}

func validateAndNormalizeDiagnosticResponse(diagnosis map[string]any) {
	if _, exists := diagnosis["potential_diagnoses"]; !exists {
		diagnosis["potential_diagnoses"] = []string{"Unspecified diagnosis"}
	} else if _, ok := diagnosis["potential_diagnoses"].([]any); !ok {
		diagnosis["potential_diagnoses"] = []string{"Unspecified diagnosis"}
	}

	if _, exists := diagnosis["reasoning"]; !exists {
		diagnosis["reasoning"] = []string{"No reasoning provided"}
	} else if _, ok := diagnosis["reasoning"].([]any); !ok {
		diagnosis["reasoning"] = []string{"No reasoning provided"}
	}

	if _, exists := diagnosis["recommended_tests"]; !exists {
		diagnosis["recommended_tests"] = []string{"No tests recommended"}
	} else if _, ok := diagnosis["recommended_tests"].([]any); !ok {
		diagnosis["recommended_tests"] = []string{"No tests recommended"}
	}

	if confidence, exists := diagnosis["confidence"]; !exists {
		diagnosis["confidence"] = 0.5
	} else {
		switch v := confidence.(type) {
		case float64:
			if v > 1.0 {
				diagnosis["confidence"] = v / 100.0
			}
		case int:
			diagnosis["confidence"] = float64(v) / 100.0
		default:
			diagnosis["confidence"] = 0.5
		}
	}
}
