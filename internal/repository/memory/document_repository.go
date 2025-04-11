package memory

import (
	"errors"
	"fmt"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

// DocumentRepository is an in-memory implementation of the DocumentRepository interface
type DocumentRepository struct {
	documents map[string]*domain.Document
	mutex     sync.RWMutex
}

// NewDocumentRepository creates a new in-memory document repository
func NewDocumentRepository() *DocumentRepository {
	return &DocumentRepository{
		documents: make(map[string]*domain.Document),
	}
}

// StoreDocument saves a new document metadata
func (r *DocumentRepository) StoreDocument(document *domain.Document) error {
	if document == nil {
		return errors.New("document cannot be nil")
	}

	if document.ID == "" {
		return errors.New("document ID cannot be empty")
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.documents[document.ID] = document
	return nil
}

// GetDocumentByID retrieves a single document by ID
func (r *DocumentRepository) GetDocumentByID(id string) (*domain.Document, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	document, exists := r.documents[id]
	if !exists {
		return nil, errors.New("document not found")
	}

	return document, nil
}

// GetDocumentsByCaseID retrieves all documents for a specific case
func (r *DocumentRepository) GetDocumentsByCaseID(caseID string) ([]*domain.Document, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var documents []*domain.Document

	for _, document := range r.documents {
		if document.CaseID == caseID {
			documents = append(documents, document)
		}
	}

	return documents, nil
}

// UpdateDocumentAnalysis updates the AI analysis results for a document
func (r *DocumentRepository) UpdateDocumentAnalysis(id string, analysis map[string]any) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	document, exists := r.documents[id]
	if !exists {
		return errors.New("document not found")
	}

	document.Analysis = analysis
	return nil
}

// DeleteDocument removes a document from the repository
func (r *DocumentRepository) DeleteDocument(id string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.documents[id]; !exists {
		return errors.New("document not found")
	}

	delete(r.documents, id)
	return nil
}

// UpdateDocumentStatus updates the processing status of a document
func (r *DocumentRepository) UpdateDocumentStatus(id string, status domain.DocumentStatus) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	doc, exists := r.documents[id]
	if !exists {
		return fmt.Errorf("document not found: %s", id)
	}

	doc.Status = status
	return nil
}
