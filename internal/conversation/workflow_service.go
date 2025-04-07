package conversation

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/workflow"
	"github.com/google/uuid"
)

// WorkflowService manages medical workflow selection and execution
type WorkflowService struct {
	workflowEngine  *workflow.WorkflowEngine
	workflowService *workflow.WorkflowService
}

// NewWorkflowService creates a new workflow service
func NewWorkflowService(engine *workflow.WorkflowEngine, service *workflow.WorkflowService) *WorkflowService {
	return &WorkflowService{
		workflowEngine:  engine,
		workflowService: service,
	}
}

// SelectWorkflow chooses the most appropriate workflow for a patient's data
func (s *WorkflowService) SelectWorkflow(patientData map[string]any) (string, error) {
	workflows := s.workflowEngine.GetAllWorkflows()
	if len(workflows) == 0 {
		return "", fmt.Errorf("no workflows available")
	}

	type workflowScore struct {
		ID    string
		Name  string
		Score int
	}

	scores := make([]workflowScore, 0, len(workflows))

	symptoms := extractStringSlice(patientData, "symptoms")
	conditions := extractStringSlice(patientData, "conditions")

	isEmergency := detectEmergency(symptoms, conditions)

	logger.Info("Selecting workflow for patient",
		"symptom_count", len(symptoms),
		"condition_count", len(conditions),
		"is_emergency", isEmergency)

	for _, wf := range workflows {
		score := 0

		for _, tag := range wf.Tags {
			if isEmergency && (strings.Contains(strings.ToLower(tag), "emergency") ||
				strings.Contains(strings.ToLower(tag), "urgent") ||
				strings.Contains(strings.ToLower(wf.Name), "emergency") ||
				strings.Contains(strings.ToLower(wf.Name), "triage")) {
				score += 100
			}

			for _, symptom := range symptoms {
				if matchSymptomToSpecialty(symptom, tag) {
					score += 10
				}
			}

			for _, condition := range conditions {
				if matchConditionToSpecialty(condition, tag) {
					score += 5
				}
			}
		}

		scores = append(scores, workflowScore{
			ID:    wf.ID,
			Name:  wf.Name,
			Score: score,
		})
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	for i, score := range scores {
		if i < 3 {
			logger.Info("Workflow score",
				"position", i+1,
				"workflow_id", score.ID,
				"workflow_name", score.Name,
				"score", score.Score)
		}
	}

	selectedWorkflowID := "standard_diagnostic_workflow"

	if len(scores) > 0 && scores[0].Score > 0 {
		selectedWorkflowID = scores[0].ID
	}

	if isEmergency {
		for _, wf := range workflows {
			if strings.Contains(wf.ID, "emergency") {
				selectedWorkflowID = wf.ID
				break
			}
		}
	}

	logger.Info("Selected workflow",
		"workflow_id", selectedWorkflowID)

	return selectedWorkflowID, nil
}

// RunWorkflow executes a workflow with patient data
func (s *WorkflowService) RunWorkflow(ctx context.Context, workflowID string, patientData map[string]any) (*workflow.WorkflowInstance, error) {
	workflowDef, err := s.workflowEngine.GetWorkflow(workflowID)
	if err != nil {
		logger.Warn("Workflow not found, trying standard workflow",
			"requested_workflow", workflowID,
			"error", err)

		workflowID = "standard_diagnostic_workflow"
		workflowDef, err = s.workflowEngine.GetWorkflow(workflowID)
		if err != nil {
			return nil, fmt.Errorf("standard workflow not available: %w", err)
		}
	}

	input := map[string]any{
		"patient_data": patientData,
	}

	logger.Info("Starting workflow execution",
		"workflow_id", workflowID,
		"workflow_name", workflowDef.Name)

	instance, err := s.workflowEngine.StartWorkflow(ctx, workflowID, input)
	if err != nil {
		return nil, fmt.Errorf("failed to start workflow: %w", err)
	}

	logger.Info("Workflow started",
		"workflow_id", workflowID,
		"instance_id", instance.ID)

	return instance, nil
}

// WaitForWorkflow waits for a workflow to complete
func (s *WorkflowService) WaitForWorkflow(ctx context.Context, instanceID uuid.UUID, timeoutSeconds int) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for workflow completion")
		case <-ticker.C:
			instance, err := s.workflowService.GetWorkflowInstance(instanceID)
			if err != nil {
				logger.Warn("Error getting workflow instance", "error", err)
				continue
			}

			if instance.Status == "completed" || instance.Status == "failed" {
				logger.Info("Workflow completed",
					"instance_id", instanceID,
					"status", instance.Status)

				if len(instance.Output) == 0 {
					logger.Warn("No output data available for workflow", "instance_id", instanceID)
				}

				return instance.Output, nil
			}
		}
	}
}

// GetResults retrieves results from a completed workflow
func (s *WorkflowService) GetResults(instanceID uuid.UUID) (map[string]any, error) {
	instance, err := s.workflowService.GetWorkflowInstance(instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}

	if instance.Status != "completed" {
		return nil, fmt.Errorf("workflow is not complete (status: %s)", instance.Status)
	}

	return instance.Output, nil
}

// detectEmergency checks if the symptoms/conditions indicate an emergency
func detectEmergency(symptoms, conditions []string) bool {
	emergencyIndicators := []string{
		"chest pain", "severe pain", "difficulty breathing", "shortness of breath",
		"unable to breathe", "can't breathe", "heart attack", "stroke", "seizure",
		"unconscious", "unresponsive", "severe bleeding", "head injury", "trauma",
		"broken bone", "fracture", "allergic reaction", "anaphylaxis", "overdose",
		"poisoning", "suicide", "self-harm", "severe headache", "worst headache",
		"sudden vision loss", "sudden weakness", "numbness", "paralysis", "severe abdominal pain",
		"vomiting blood", "coughing blood", "high fever", "dehydration", "confused", "disoriented",
	}

	for _, symptom := range symptoms {
		symptomLower := strings.ToLower(symptom)
		for _, indicator := range emergencyIndicators {
			if strings.Contains(symptomLower, indicator) {
				logger.Info("Emergency symptom detected", "symptom", symptom, "indicator", indicator)
				return true
			}
		}
	}

	for _, condition := range conditions {
		conditionLower := strings.ToLower(condition)
		for _, indicator := range emergencyIndicators {
			if strings.Contains(conditionLower, indicator) {
				logger.Info("Emergency condition detected", "condition", condition, "indicator", indicator)
				return true
			}
		}
	}

	return false
}

// matchSymptomToSpecialty checks if a symptom is related to a medical specialty
func matchSymptomToSpecialty(symptom, specialty string) bool {
	specialtyLower := strings.ToLower(specialty)
	symptomLower := strings.ToLower(symptom)

	specialtySymptoms := map[string][]string{
		"cardio":      {"chest pain", "palpitation", "heart", "blood pressure", "hypertension", "cardiac"},
		"neuro":       {"headache", "migraine", "dizziness", "numbness", "tingling", "seizure", "tremor", "balance", "brain"},
		"respiratory": {"cough", "breathing", "shortness of breath", "wheeze", "asthma", "lung", "pneumonia"},
		"gastro":      {"abdominal pain", "stomach", "nausea", "vomiting", "diarrhea", "constipation", "intestinal"},
		"pediatric":   {"child", "baby", "infant", "toddler", "teenager", "growth", "development"},
		"emergency":   {"severe pain", "acute", "sudden", "trauma", "injury", "accident"},
	}

	for specKey, symptoms := range specialtySymptoms {
		if strings.Contains(specialtyLower, specKey) {
			for _, s := range symptoms {
				if strings.Contains(symptomLower, s) {
					return true
				}
			}
		}
	}

	return false
}

// matchConditionToSpecialty checks if a condition is related to a medical specialty
func matchConditionToSpecialty(condition, specialty string) bool {
	specialtyLower := strings.ToLower(specialty)
	conditionLower := strings.ToLower(condition)

	specialtyConditions := map[string][]string{
		"cardio":      {"heart disease", "hypertension", "arrhythmia", "atrial", "coronary", "angina", "heart attack", "cardiac"},
		"neuro":       {"stroke", "migraine", "epilepsy", "alzheimer", "parkinson", "multiple sclerosis", "ms", "brain"},
		"respiratory": {"asthma", "copd", "emphysema", "pneumonia", "bronchitis", "lung disease", "pulmonary"},
		"gastro":      {"ibs", "crohn", "ulcerative colitis", "gerd", "acid reflux", "liver", "hepatitis", "pancreatitis"},
		"pediatric":   {"childhood", "developmental", "growth", "congenital"},
		"emergency":   {"fracture", "injury", "trauma", "poisoning", "overdose", "acute", "severe"},
	}

	for specKey, conditions := range specialtyConditions {
		if strings.Contains(specialtyLower, specKey) {
			for _, c := range conditions {
				if strings.Contains(conditionLower, c) {
					return true
				}
			}
		}
	}

	return false
}

// extractStringSlice extracts a string slice from a nested map
func extractStringSlice(data map[string]any, key string) []string {
	if data == nil {
		return []string{}
	}

	if val, ok := data[key]; ok {
		if strSlice, ok := val.([]string); ok {
			return strSlice
		}
		if anySlice, ok := val.([]any); ok {
			result := make([]string, 0, len(anySlice))
			for _, item := range anySlice {
				if str, ok := item.(string); ok {
					result = append(result, str)
				}
			}
			return result
		}
	}

	if patientData, ok := data["patient_data"].(map[string]any); ok {
		if val, ok := patientData[key]; ok {
			if strSlice, ok := val.([]string); ok {
				return strSlice
			}
			if anySlice, ok := val.([]any); ok {
				result := make([]string, 0, len(anySlice))
				for _, item := range anySlice {
					if str, ok := item.(string); ok {
						result = append(result, str)
					}
				}
				return result
			}
		}
	}

	return []string{}
}

// GetAllServices returns all services that might be used by the workflow
func (s *WorkflowService) GetAllServices() []interface{} {
	services := make([]interface{}, 0)

	if s.workflowEngine != nil {
		if resultRepo := s.workflowEngine.GetResultRepository(); resultRepo != nil {
			services = append(services, resultRepo)
		}
	}

	services = append(services, s.workflowService)

	return services
}
