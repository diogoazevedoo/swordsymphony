package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/google/uuid"
)

// TranscriptProcessor handles processing conversation transcripts to extract medical data
type TranscriptProcessor struct {
	aiClient         ai.Client
	caseRepository   repository.CaseRepository
	resultRepository repository.ResultRepository
	apiBaseURL       string
	httpClient       *http.Client
}

// PatientData represents structured patient data extracted from a conversation
type PatientData struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Age         float64  `json:"age"`
	Gender      string   `json:"gender"`
	Symptoms    []string `json:"symptoms"`
	Conditions  []string `json:"conditions"`
	Medications []string `json:"medications"`
	Allergies   []string `json:"allergies"`
	Vitals      Vitals   `json:"vitals"`
}

// Vitals represents patient vital signs
type Vitals struct {
	BloodPressure    string  `json:"blood_pressure"`
	HeartRate        float64 `json:"heart_rate"`
	Temperature      float64 `json:"temperature"`
	OxygenSaturation float64 `json:"oxygen_saturation"`
}

// ProcessingResult contains the result of transcript processing
type ProcessingResult struct {
	CaseID        string                 `json:"case_id"`
	PatientData   PatientData            `json:"patient_data"`
	WorkflowID    string                 `json:"workflow_id"`
	InstanceID    uuid.UUID              `json:"instance_id"`
	DiagnosisData map[string]interface{} `json:"diagnosis_data,omitempty"`
	CompletedAt   time.Time              `json:"completed_at"`
}

// NewTranscriptProcessor creates a new transcript processor
func NewTranscriptProcessor(
	aiClient ai.Client,
	caseRepository repository.CaseRepository,
	resultRepository repository.ResultRepository,
	apiBaseURL string,
) *TranscriptProcessor {
	return &TranscriptProcessor{
		aiClient:         aiClient,
		caseRepository:   caseRepository,
		resultRepository: resultRepository,
		apiBaseURL:       apiBaseURL,
		httpClient:       &http.Client{Timeout: 30 * time.Second},
	}
}

// ProcessTranscript processes a conversation transcript to extract patient data,
// store it as a case, and trigger a workflow
func (p *TranscriptProcessor) ProcessTranscript(
	ctx context.Context,
	conversationID string,
	formattedTranscript string,
) (*ProcessingResult, error) {
	logger.Info("Processing transcript",
		"conversation_id", conversationID,
		"transcript_length", len(formattedTranscript))

	// Extract patient data using AI
	patientData, err := p.extractPatientData(ctx, formattedTranscript)
	if err != nil {
		return nil, fmt.Errorf("failed to extract patient data: %w", err)
	}

	// Post-process and clean up patient data
	patientData = sanitizePatientData(patientData)

	// Generate a case ID if needed
	if patientData.ID == "" {
		patientData.ID = fmt.Sprintf("P%d", time.Now().Unix())
	}

	// Log the processed data
	logger.Info("Processed patient data",
		"case_id", patientData.ID,
		"name", patientData.Name,
		"age", patientData.Age,
		"gender", patientData.Gender,
		"symptoms", strings.Join(patientData.Symptoms, ", "),
		"conditions", strings.Join(patientData.Conditions, ", "),
		"medications", strings.Join(patientData.Medications, ", "),
		"allergies", strings.Join(patientData.Allergies, ", "))

	// Store the case
	if err := p.caseRepository.StoreCase(patientData.ID, patientDataToMap(patientData), false); err != nil {
		logger.Error("Failed to store case", "error", err, "case_id", patientData.ID)
		// Continue anyway, as this is non-fatal
	} else {
		logger.Info("Successfully stored case", "case_id", patientData.ID)
	}

	// Always use standard workflow for now
	workflowID := "standard_diagnostic_workflow"

	// Start workflow via API
	instanceID, err := p.startWorkflowViaAPI(ctx, workflowID, patientDataToMap(patientData))
	if err != nil {
		logger.Error("Failed to start workflow via API",
			"error", err,
			"workflow_id", workflowID,
			"case_id", patientData.ID)
	} else {
		logger.Info("Started workflow via API",
			"workflow_id", workflowID,
			"instance_id", instanceID,
			"case_id", patientData.ID)
	}

	// Initial result with no diagnosis yet
	result := &ProcessingResult{
		CaseID:      patientData.ID,
		PatientData: patientData,
		WorkflowID:  workflowID,
		InstanceID:  instanceID,
		CompletedAt: time.Now(),
	}

	// Store initial results
	if p.resultRepository != nil {
		resultMap := map[string]interface{}{
			"patient_data":    patientDataToMap(patientData),
			"workflow_id":     workflowID,
			"instance_id":     instanceID.String(),
			"processed_at":    time.Now().Format(time.RFC3339),
			"conversation_id": conversationID,
		}

		err := p.resultRepository.StoreResults(patientData.ID, resultMap)
		if err != nil {
			logger.Error("Failed to store results", "error", err, "case_id", patientData.ID)
		} else {
			logger.Info("Stored initial results", "case_id", patientData.ID)
		}
	}

	return result, nil
}

// sanitizePatientData cleans and validates the patient data to ensure it's properly categorized
func sanitizePatientData(data PatientData) PatientData {
	// Check if a medication was mistakenly put in gender
	if stringInSlice(strings.ToLower(data.Gender), []string{"male", "female", "other"}) == false {
		// If gender doesn't match expected values, check if it appears to be a medication
		if data.Gender != "" {
			// Move the value to medications if it's not empty
			data.Medications = append(data.Medications, data.Gender)
		}
		// Set proper gender if available in medications
		if stringInSlice("male", lowercaseSlice(data.Medications)) {
			data.Gender = "male"
		} else if stringInSlice("female", lowercaseSlice(data.Medications)) {
			data.Gender = "female"
		} else {
			data.Gender = "" // Reset to empty if invalid
		}
	}
	
	// Remove any medication entries that match gender values
	filteredMeds := make([]string, 0)
	for _, med := range data.Medications {
		if !stringInSlice(strings.ToLower(med), []string{"male", "female", "other"}) {
			filteredMeds = append(filteredMeds, med)
		}
	}
	data.Medications = filteredMeds
	
	// Clean empty entries
	data.Symptoms = removeEmptyStrings(data.Symptoms)
	data.Conditions = removeEmptyStrings(data.Conditions)
	data.Medications = removeEmptyStrings(data.Medications)
	data.Allergies = removeEmptyStrings(data.Allergies)
	
	// Remove "none" entries
	data.Medications = removeStringFromSlice(data.Medications, "none")
	data.Allergies = removeStringFromSlice(data.Allergies, "none")
	data.Symptoms = removeStringFromSlice(data.Symptoms, "none")
	data.Conditions = removeStringFromSlice(data.Conditions, "none")
	
	return data
}

// Helper functions for data sanitization

// stringInSlice checks if a string is in a slice (case insensitive)
func stringInSlice(str string, list []string) bool {
	strLower := strings.ToLower(str)
	for _, v := range list {
		if strings.ToLower(v) == strLower {
			return true
		}
	}
	return false
}

// lowercaseSlice converts all strings in a slice to lowercase
func lowercaseSlice(strs []string) []string {
	result := make([]string, len(strs))
	for i, s := range strs {
		result[i] = strings.ToLower(s)
	}
	return result
}

// removeEmptyStrings removes empty strings from a slice
func removeEmptyStrings(strs []string) []string {
	result := make([]string, 0)
	for _, s := range strs {
		if strings.TrimSpace(s) != "" {
			result = append(result, s)
		}
	}
	return result
}

// removeStringFromSlice removes all occurrences of a string from a slice (case insensitive)
func removeStringFromSlice(strs []string, remove string) []string {
	result := make([]string, 0)
	removeLower := strings.ToLower(remove)
	for _, s := range strs {
		if strings.ToLower(s) != removeLower {
			result = append(result, s)
		}
	}
	return result
}

// startWorkflowViaAPI starts a workflow using the API endpoint
func (p *TranscriptProcessor) startWorkflowViaAPI(
	ctx context.Context,
	workflowID string,
	patientData map[string]interface{},
) (uuid.UUID, error) {
	// Prepare input data for the workflow
	inputData := map[string]interface{}{
		"patient_data": patientData,
	}

	// Create JSON payload
	jsonPayload, err := json.Marshal(inputData)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal workflow input: %w", err)
	}

	// Create request
	apiURL := fmt.Sprintf("%s/api/management/workflows/%s/instances", p.apiBaseURL, workflowID)
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to create API request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to send API request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return uuid.Nil, fmt.Errorf("API returned non-success status: %d", resp.StatusCode)
	}

	// Parse response
	var apiResponse struct {
		Success bool `json:"success"`
		Data    struct {
			InstanceID string `json:"instance_id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return uuid.Nil, fmt.Errorf("failed to decode API response: %w", err)
	}

	// Convert instance ID to UUID
	instanceID, err := uuid.Parse(apiResponse.Data.InstanceID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid instance ID in response: %w", err)
	}

	return instanceID, nil
}

// extractPatientData uses AI to extract structured patient data from the transcript
func (p *TranscriptProcessor) extractPatientData(
	ctx context.Context,
	transcript string,
) (PatientData, error) {
	promptTemplate := `
Extract all patient information from the following medical conversation transcript. The conversation follows a specific sequence of questions and answers.

The questions are asked in this order:
1. What is your name?
2. How old are you?
3. What is your gender? (Male, female, or other)
4. What symptoms are you experiencing?
5. Do you have any existing medical conditions?
6. What medications are you currently taking?
7. Do you have any allergies to medications or other substances?

Analyze EACH QUESTION with its MATCHING ANSWER to accurately extract the information.

I need structured data for a medical system. Include the following fields:
- name: The patient's full name from question 1
- age: The patient's age as a number from question A2
- gender: The patient's gender (male, female, or other) from question 3
- symptoms: All symptoms mentioned in question 4 (as an array of strings)
- conditions: All pre-existing medical conditions mentioned in question 5 (as an array of strings)
- medications: All medications mentioned in question 6 (as an array of strings)
- allergies: All allergies mentioned in question 7 (as an array of strings)

IMPORTANT: Match each answer with the correct corresponding question. Do NOT use answers from one question to fill fields for another question.

If any field is not mentioned or unclear, use empty values (empty string, 0, or empty array).
Return ONLY a valid JSON object containing all these fields, even if they're empty.

Conversation transcript:
%s

Response must be valid JSON only.`

	prompt := fmt.Sprintf(promptTemplate, transcript)

	// Use a more powerful model like GPT-4 for extraction
	response, err := p.aiClient.GenerateCompletion(ctx, prompt, ai.CompletionOptions{
		MaxTokens:   1024,
		Temperature: 0.1,
		ModelName:   "gpt-4", // Use the most capable model for extraction
	})

	if err != nil {
		return PatientData{}, fmt.Errorf("AI completion failed: %w", err)
	}

	// Extract JSON from response
	jsonString := extractJSON(response.Text)

	// Parse the JSON
	var extractedData PatientData
	if err := json.Unmarshal([]byte(jsonString), &extractedData); err != nil {
		return PatientData{}, fmt.Errorf("failed to parse AI response: %w", err)
	}

	// Ensure we have valid data structure
	if extractedData.Symptoms == nil {
		extractedData.Symptoms = []string{}
	}
	if extractedData.Conditions == nil {
		extractedData.Conditions = []string{}
	}
	if extractedData.Medications == nil {
		extractedData.Medications = []string{}
	}
	if extractedData.Allergies == nil {
		extractedData.Allergies = []string{}
	}

	// Generate ID if not present
	if extractedData.ID == "" {
		extractedData.ID = fmt.Sprintf("P%d", time.Now().Unix())
	}

	logger.Info("Extracted raw patient data (pre-cleanup)",
		"name", extractedData.Name,
		"age", extractedData.Age,
		"gender", extractedData.Gender,
		"symptoms_count", len(extractedData.Symptoms),
		"conditions_count", len(extractedData.Conditions),
		"medications_count", len(extractedData.Medications),
		"allergies_count", len(extractedData.Allergies))

	return extractedData, nil
}

// patientDataToMap converts PatientData to a map for storage
func patientDataToMap(data PatientData) map[string]interface{} {
	return map[string]interface{}{
		"id":          data.ID,
		"name":        data.Name,
		"age":         data.Age,
		"gender":      data.Gender,
		"symptoms":    data.Symptoms,
		"conditions":  data.Conditions,
		"medications": data.Medications,
		"allergies":   data.Allergies,
		"vitals": map[string]interface{}{
			"blood_pressure":    data.Vitals.BloodPressure,
			"heart_rate":        data.Vitals.HeartRate,
			"temperature":       data.Vitals.Temperature,
			"oxygen_saturation": data.Vitals.OxygenSaturation,
		},
	}
}

// Helper function to extract JSON from text
func extractJSON(text string) string {
	// Look for the first opening brace and last closing brace
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")

	if start != -1 && end != -1 && end > start {
		return text[start : end+1]
	}

	// If no valid JSON is found, return a minimal JSON structure
	return `{"name":"","age":0,"gender":"","symptoms":[],"conditions":[],"medications":[],"allergies":[]}`
}
