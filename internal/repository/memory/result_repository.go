package memory

import (
	"errors"
	"sync"
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
	r.mu.Lock()
	defer r.mu.Unlock()

	r.results[caseID] = results
	return nil
}

// GetResultsByCaseID retrieves results for a specific case
func (r *ResultRepository) GetResultsByCaseID(caseID string) (map[string]any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	results, exists := r.results[caseID]
	if !exists {
		return nil, errors.New("no results found for case")
	}

	return results, nil
}
