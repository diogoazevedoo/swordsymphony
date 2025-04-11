package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DocumentAnalysis represents the AI analysis of a document
type DocumentAnalysis struct {
	ID          string          `json:"id"`
	DocumentID  string          `json:"document_id"`
	Status      AnalysisStatus  `json:"status"`
	Results     json.RawMessage `json:"results,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Error       string          `json:"error,omitempty"`
}

// AnalysisStatus represents the status of a document analysis
type AnalysisStatus string

const (
	// AnalysisPending indicates analysis has not started
	AnalysisPending AnalysisStatus = "pending"

	// AnalysisInProgress indicates analysis is currently running
	AnalysisInProgress AnalysisStatus = "in_progress"

	// AnalysisCompleted indicates analysis was successful
	AnalysisCompleted AnalysisStatus = "completed"

	// AnalysisFailed indicates analysis failed
	AnalysisFailed AnalysisStatus = "failed"
)

// NewDocumentAnalysis creates a new document analysis record
func NewDocumentAnalysis(documentID string) *DocumentAnalysis {
	return &DocumentAnalysis{
		ID:         uuid.New().String(),
		DocumentID: documentID,
		Status:     AnalysisPending,
		CreatedAt:  time.Now(),
	}
}

// UpdateStatus sets the analysis status and updates timestamps accordingly
func (a *DocumentAnalysis) UpdateStatus(status AnalysisStatus) {
	a.Status = status

	if status == AnalysisCompleted || status == AnalysisFailed {
		now := time.Now()
		a.CompletedAt = &now
	}
}

// SetResults updates the analysis results and sets status to completed
func (a *DocumentAnalysis) SetResults(results map[string]interface{}) error {
	data, err := json.Marshal(results)
	if err != nil {
		return err
	}

	a.Results = data
	a.UpdateStatus(AnalysisCompleted)
	return nil
}

// SetError records an error message and sets status to failed
func (a *DocumentAnalysis) SetError(errMsg string) {
	a.Error = errMsg
	a.UpdateStatus(AnalysisFailed)
}

// GetResults deserializes the raw JSON results into a map
func (a *DocumentAnalysis) GetResults() (map[string]interface{}, error) {
	if len(a.Results) == 0 {
		return nil, nil
	}

	var results map[string]interface{}
	err := json.Unmarshal(a.Results, &results)
	return results, err
}
