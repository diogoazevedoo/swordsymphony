package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// DocumentAnalysisRepository is a PostgreSQL implementation of the DocumentAnalysisRepository interface
type DocumentAnalysisRepository struct {
	db *sql.DB
}

// NewDocumentAnalysisRepository creates a new PostgreSQL document analysis repository
func NewDocumentAnalysisRepository(db *DB) *DocumentAnalysisRepository {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS document_analysis (
			id VARCHAR(50) PRIMARY KEY,
			document_id VARCHAR(50) NOT NULL,
			status VARCHAR(20) NOT NULL,
			results JSONB,
			created_at TIMESTAMP NOT NULL,
			completed_at TIMESTAMP,
			error TEXT,
			UNIQUE(document_id)
		)
	`)
	if err != nil {
		logger.Error("Failed to create document_analysis table", "error", err)
	}

	return &DocumentAnalysisRepository{db: db.DB}
}

// CreateAnalysis creates a new document analysis record
func (r *DocumentAnalysisRepository) CreateAnalysis(analysis *domain.DocumentAnalysis) error {
	var resultsValue interface{} = nil
	if len(analysis.Results) > 0 {
		resultsValue = analysis.Results
	}

	_, err := r.db.Exec(`
		INSERT INTO document_analysis (id, document_id, status, results, created_at, completed_at, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (document_id) DO UPDATE
		SET status = $3, results = $4, created_at = $5, completed_at = $6, error = $7
	`,
		analysis.ID,
		analysis.DocumentID,
		analysis.Status,
		resultsValue,
		analysis.CreatedAt,
		analysis.CompletedAt,
		analysis.Error,
	)
	return err
}

// GetAnalysisByID retrieves an analysis by its ID
func (r *DocumentAnalysisRepository) GetAnalysisByID(id string) (*domain.DocumentAnalysis, error) {
	var analysis domain.DocumentAnalysis
	var completedAt sql.NullTime
	var results []byte
	var errorText sql.NullString

	err := r.db.QueryRow(`
		SELECT id, document_id, status, results, created_at, completed_at, error
		FROM document_analysis
		WHERE id = $1
	`, id).Scan(
		&analysis.ID,
		&analysis.DocumentID,
		&analysis.Status,
		&results,
		&analysis.CreatedAt,
		&completedAt,
		&errorText,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document analysis not found: %s", id)
		}
		return nil, err
	}

	analysis.Results = results
	if completedAt.Valid {
		analysis.CompletedAt = &completedAt.Time
	}
	if errorText.Valid {
		analysis.Error = errorText.String
	}

	return &analysis, nil
}

// GetAnalysisByDocumentID retrieves an analysis by document ID
func (r *DocumentAnalysisRepository) GetAnalysisByDocumentID(documentID string) (*domain.DocumentAnalysis, error) {
	var analysis domain.DocumentAnalysis
	var completedAt sql.NullTime
	var results []byte
	var errorText sql.NullString

	err := r.db.QueryRow(`
		SELECT id, document_id, status, results, created_at, completed_at, error
		FROM document_analysis
		WHERE document_id = $1
	`, documentID).Scan(
		&analysis.ID,
		&analysis.DocumentID,
		&analysis.Status,
		&results,
		&analysis.CreatedAt,
		&completedAt,
		&errorText,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document analysis not found for document: %s", documentID)
		}
		return nil, err
	}

	analysis.Results = results
	if completedAt.Valid {
		analysis.CompletedAt = &completedAt.Time
	}
	if errorText.Valid {
		analysis.Error = errorText.String
	}

	return &analysis, nil
}

// UpdateAnalysisStatus updates the status of an analysis
func (r *DocumentAnalysisRepository) UpdateAnalysisStatus(id string, status domain.AnalysisStatus) error {
	var completedAt *time.Time
	if status == domain.AnalysisCompleted || status == domain.AnalysisFailed {
		now := time.Now()
		completedAt = &now
	}

	_, err := r.db.Exec(`
		UPDATE document_analysis
		SET status = $2, completed_at = $3
		WHERE id = $1
	`, id, status, completedAt)

	return err
}

// UpdateAnalysisResults updates the results of an analysis
func (r *DocumentAnalysisRepository) UpdateAnalysisResults(id string, results map[string]interface{}) error {
	data, err := json.Marshal(results)
	if err != nil {
		return err
	}

	now := time.Now()
	_, err = r.db.Exec(`
		UPDATE document_analysis
		SET results = $2, status = $3, completed_at = $4
		WHERE id = $1
	`, id, data, domain.AnalysisCompleted, now)

	return err
}

// DeleteAnalysis removes an analysis record
func (r *DocumentAnalysisRepository) DeleteAnalysis(id string) error {
	_, err := r.db.Exec(`
		DELETE FROM document_analysis
		WHERE id = $1
	`, id)

	return err
}
