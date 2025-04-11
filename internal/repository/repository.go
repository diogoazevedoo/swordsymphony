package repository

import "github.com/diogoazevedoo/swordsymphony/internal/domain"

// CaseRepository defines the interface for accessing patient case data
type CaseRepository interface {
	// GetDemoCases returns all available demo cases
	GetDemoCases() (map[string]map[string]any, error)

	// GetCaseByID returns a specific case by ID
	GetCaseByID(id string) (map[string]any, error)

	// GetAllCases returns all cases in the system (both demo and non-demo)
	GetAllCases() (map[string]map[string]any, error)

	// GetCurrentCase returns the case currently being processes, or nil if none
	GetCurrentCase() map[string]any

	// SetCurrentCase sets the case currently being processed
	SetCurrentCase(caseData map[string]any)

	// ClearCurrentCase clears the currently processing case
	ClearCurrentCase()

	// InitializeDemoCases loads the initial set of demo cases
	InitializeDemoCases() error

	// StoreCase adds or updates a case in the repository
	StoreCase(id string, caseData map[string]any, isDemo bool) error
}

// ResultRepository defines the interface for storing and retrieving processing results
type ResultRepository interface {
	// StoreResults saves the results for a case
	StoreResults(caseID string, results map[string]any) error

	// GetResultsByCaseID retrieves results for a specific case
	GetResultsByCaseID(caseID string) (map[string]any, error)
}

// DocumentRepository defines the interface for accessing document data
type DocumentRepository interface {
	// StoreDocument saves a new document metadata
	StoreDocument(document *domain.Document) error

	// GetDocumentByID retrieves a single document by ID
	GetDocumentByID(id string) (*domain.Document, error)

	// GetDocumentsByCaseID retrieves all documents for a specific case
	GetDocumentsByCaseID(caseID string) ([]*domain.Document, error)

	// UpdateDocumentAnalysis updates the AI analysis results for a document
	UpdateDocumentAnalysis(id string, analysis map[string]any) error

	// UpdateDocumentStatus updates the processing status of a document
	UpdateDocumentStatus(id string, status domain.DocumentStatus) error

	// DeleteDocument removes a document from the repository
	DeleteDocument(id string) error
}

// DocumentAnalysisRepository defines the interface for accessing document analysis data
type DocumentAnalysisRepository interface {
	// CreateAnalysis creates a new document analysis record
	CreateAnalysis(analysis *domain.DocumentAnalysis) error

	// GetAnalysisByID retrieves an analysis by its ID
	GetAnalysisByID(id string) (*domain.DocumentAnalysis, error)

	// GetAnalysisByDocumentID retrieves an analysis by document ID
	GetAnalysisByDocumentID(documentID string) (*domain.DocumentAnalysis, error)

	// UpdateAnalysisStatus updates the status of an analysis
	UpdateAnalysisStatus(id string, status domain.AnalysisStatus) error

	// UpdateAnalysisResults updates the results of an analysis
	UpdateAnalysisResults(id string, results map[string]interface{}) error

	// DeleteAnalysis removes an analysis record
	DeleteAnalysis(id string) error
}
