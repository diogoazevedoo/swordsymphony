package memory

import (
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

	existingResults, exists := r.results[caseID]
	if !exists {
		existingResults = make(map[string]any)
	}

	for k, v := range results {
		existingResults[k] = v
	}

	r.results[caseID] = existingResults

	logger.Info("Stored results in memory repository",
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

	r.mu.RLock()
	defer r.mu.RUnlock()

	results, exists := r.results[caseID]
	if !exists {
		return nil, errors.NotFound("No results found for case", "results_not_found")
	}

	return results, nil
}
