package memory

import (
	"fmt"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// ResultRepository is an in-memory implementation of the ResultRepository interface
type ResultRepository struct {
	results map[string]map[string]any
	mu      sync.RWMutex
}

// NewResultRepository creates a new in-memory result repository
func NewResultRepository() *ResultRepository {
	return &ResultRepository{
		results: make(map[string]map[string]any),
	}
}

// StoreResults saves the results for a case
func (r *ResultRepository) StoreResults(caseID string, results map[string]any) error {
	if caseID == "" {
		return errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	if results == nil {
		return errors.Validation("Results cannot be nil", "nil_results")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existingResults := make(map[string]any)

	if current, exists := r.results[caseID]; exists {
		for k, v := range current {
			existingResults[k] = deepCopy(v)
		}
	}

	// Merge with new results
	for k, v := range results {
		existingResults[k] = deepCopy(v)
	}

	// Store final results
	r.results[caseID] = existingResults

	logger.Info("Successfully stored results in memory repository",
		"case_id", caseID,
		"result_count", len(existingResults),
		"result_keys", getMapKeys(existingResults))

	return nil
}

func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// GetResultsByCaseID retrieves results for a specific case
func (r *ResultRepository) GetResultsByCaseID(caseID string) (map[string]any, error) {
	if caseID == "" {
		return nil, errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	logger.Info("Attempting to retrieve results", "case_id", caseID)

	r.mu.RLock()
	defer r.mu.RUnlock()

	results, exists := r.results[caseID]
	if !exists {
		// Try alternative keys
		for id, res := range r.results {
			// Check if this is a nested case ID
			if patientData, ok := res["patient_data"].(map[string]any); ok {
				if patientID, ok := patientData["id"].(string); ok && patientID == caseID {
					logger.Info("Found results using nested patient ID",
						"case_id", caseID,
						"stored_id", id)
					return deepCopyMap(res), nil
				}
			}
		}

		return nil, errors.NotFound("No results found for case", "results_not_found")
	}

	// Return a deep copy to prevent modification of stored data
	return deepCopyMap(results), nil
}

// deepCopy creates a deep copy of a value to prevent shared references
func deepCopy(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case map[string]any:
		return deepCopyMap(val)
	case []any:
		return deepCopySlice(val)
	default:
		// For primitive types, return as is
		return v
	}
}

// deepCopyMap creates a deep copy of a map
func deepCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = deepCopy(v)
	}
	return result
}

// deepCopySlice creates a deep copy of a slice
func deepCopySlice(s []any) []any {
	if s == nil {
		return nil
	}

	result := make([]any, len(s))
	for i, v := range s {
		result[i] = deepCopy(v)
	}
	return result
}

// DumpAllResults returns all stored results (for debugging)
func (r *ResultRepository) DumpAllResults() map[string]map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dump := make(map[string]map[string]any)
	for id, results := range r.results {
		dump[id] = deepCopyMap(results)
	}

	return dump
}

// PrettyPrintResults returns a formatted string of all results for debugging
func (r *ResultRepository) PrettyPrintResults() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var output string
	output += fmt.Sprintf("Total cases in repository: %d\n", len(r.results))

	for id, results := range r.results {
		output += fmt.Sprintf("Case ID: %s\n", id)
		output += fmt.Sprintf("  Keys: %v\n", getMapKeys(results))

		// Attempt to print details of important fields
		if diagnosis, ok := results["diagnosis"].(map[string]any); ok {
			output += fmt.Sprintf("  Diagnosis keys: %v\n", getMapKeys(diagnosis))
		}

		if treatment, ok := results["treatment_plan"].(map[string]any); ok {
			output += fmt.Sprintf("  Treatment plan keys: %v\n", getMapKeys(treatment))
		}

		output += "-----------------------\n"
	}

	return output
}
