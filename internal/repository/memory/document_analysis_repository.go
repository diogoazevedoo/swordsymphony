package memory

import (
	"errors"
	"fmt"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// DocumentAnalysisRepository is an in-memory implementation of the DocumentAnalysisRepository interface
type DocumentAnalysisRepository struct {
	analyses map[string]*domain.DocumentAnalysis
	mutex    sync.RWMutex
}

// NewDocumentAnalysisRepository creates a new in-memory document analysis repository
func NewDocumentAnalysisRepository() *DocumentAnalysisRepository {
	return &DocumentAnalysisRepository{
		analyses: make(map[string]*domain.DocumentAnalysis),
	}
}

// CreateAnalysis creates a new document analysis record
func (r *DocumentAnalysisRepository) CreateAnalysis(analysis *domain.DocumentAnalysis) error {
	if analysis == nil {
		return errors.New("analysis cannot be nil")
	}

	if analysis.ID == "" {
		return errors.New("analysis ID cannot be empty")
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for id, existing := range r.analyses {
		if existing.DocumentID == analysis.DocumentID && id != analysis.ID {
			delete(r.analyses, id)
		}
	}

	r.analyses[analysis.ID] = analysis
	return nil
}

// GetAnalysisByID retrieves an analysis by its ID
func (r *DocumentAnalysisRepository) GetAnalysisByID(id string) (*domain.DocumentAnalysis, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	analysis, exists := r.analyses[id]
	if !exists {
		return nil, fmt.Errorf("document analysis not found: %s", id)
	}

	return analysis, nil
}

// GetAnalysisByDocumentID retrieves an analysis by document ID
func (r *DocumentAnalysisRepository) GetAnalysisByDocumentID(documentID string) (*domain.DocumentAnalysis, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, analysis := range r.analyses {
		if analysis.DocumentID == documentID {
			return analysis, nil
		}
	}

	return nil, fmt.Errorf("document analysis not found for document: %s", documentID)
}

// UpdateAnalysisStatus updates the status of an analysis
func (r *DocumentAnalysisRepository) UpdateAnalysisStatus(id string, status domain.AnalysisStatus) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	analysis, exists := r.analyses[id]
	if !exists {
		return fmt.Errorf("document analysis not found: %s", id)
	}

	analysis.UpdateStatus(status)
	return nil
}

// UpdateAnalysisResults updates the results of an analysis
func (r *DocumentAnalysisRepository) UpdateAnalysisResults(id string, results map[string]interface{}) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	analysis, exists := r.analyses[id]
	if !exists {
		return fmt.Errorf("document analysis not found: %s", id)
	}

	return analysis.SetResults(results)
}

// DeleteAnalysis removes an analysis record
func (r *DocumentAnalysisRepository) DeleteAnalysis(id string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.analyses[id]; !exists {
		return fmt.Errorf("document analysis not found: %s", id)
	}

	delete(r.analyses, id)
	return nil
}
