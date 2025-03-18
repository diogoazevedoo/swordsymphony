package repository

// CaseRepository defines the interface for accessing patient case data
type CaseRepository interface {
	// GetDemoCases returns all available demo cases
	GetDemoCases() (map[string]map[string]any, error)

	// GetCaseByID returns a specific case by ID
	GetCaseByID(id string) (map[string]any, error)

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
