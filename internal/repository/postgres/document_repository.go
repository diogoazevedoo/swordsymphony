package postgres

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// DocumentRepository implements the repository interface for PostgreSQL
type DocumentRepository struct {
	db *sql.DB
}

// NewDocumentRepository creates a new PostgreSQL document repository
func NewDocumentRepository(db *DB) *DocumentRepository {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS documents (
			id VARCHAR(36) PRIMARY KEY,
			case_id VARCHAR(100) NOT NULL,
			name TEXT NOT NULL,
			type VARCHAR(20) NOT NULL,
			status VARCHAR(20) NOT NULL,
			content_type VARCHAR(100) NOT NULL,
			file_path TEXT NOT NULL,
			file_url TEXT NOT NULL,
			size BIGINT NOT NULL,
			uploaded_at TIMESTAMP NOT NULL,
			analysis JSONB
		)
	`)

	if err != nil {
		logger.Error("Failed to create documents table", "error", err)
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_documents_case_id ON documents(case_id)
	`)

	if err != nil {
		logger.Error("Failed to create case_id index", "error", err)
	}

	return &DocumentRepository{db: db.DB}
}

// StoreDocument saves a new document metadata
func (r *DocumentRepository) StoreDocument(document *domain.Document) error {
	var analysisJSON []byte
	var err error
	if document.Analysis != nil && len(document.Analysis) > 0 {
		analysisJSON, err = json.Marshal(document.Analysis)
		if err != nil {
			return err
		}
	}

	_, err = r.db.Exec(`
		INSERT INTO documents (id, case_id, name, type, status, content_type, file_path, file_url, size, uploaded_at, analysis)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, '')::jsonb)
		ON CONFLICT (id) DO UPDATE SET
			case_id = $2,
			name = $3,
			type = $4,
			status = $5,
			content_type = $6,
			file_path = $7,
			file_url = $8,
			size = $9,
			uploaded_at = $10,
			analysis = NULLIF($11, '')::jsonb
	`, document.ID, document.CaseID, document.Name, document.Type, document.Status,
		document.ContentType, document.FilePath, document.FileURL, document.Size,
		document.UploadedAt, analysisJSON)

	return err
}

// GetDocumentByID retrieves a single document by ID
func (r *DocumentRepository) GetDocumentByID(id string) (*domain.Document, error) {
	var doc domain.Document
	var analysisJSON []byte
	var uploadedAt time.Time

	err := r.db.QueryRow(`
		SELECT id, case_id, name, type, status, content_type, file_path, file_url, size, uploaded_at, analysis
		FROM documents
		WHERE id = $1
	`, id).Scan(
		&doc.ID,
		&doc.CaseID,
		&doc.Name,
		&doc.Type,
		&doc.Status,
		&doc.ContentType,
		&doc.FilePath,
		&doc.FileURL,
		&doc.Size,
		&uploadedAt,
		&analysisJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	doc.UploadedAt = uploadedAt

	if len(analysisJSON) > 0 {
		var analysis map[string]any
		if err := json.Unmarshal(analysisJSON, &analysis); err == nil {
			doc.Analysis = analysis
		}
	}

	return &doc, nil
}

// GetDocumentsByCaseID retrieves all documents for a specific case
func (r *DocumentRepository) GetDocumentsByCaseID(caseID string) ([]*domain.Document, error) {
	rows, err := r.db.Query(`
		SELECT id, case_id, name, type, status, content_type, file_path, file_url, size, uploaded_at, analysis
		FROM documents
		WHERE case_id = $1
		ORDER BY uploaded_at DESC
	`, caseID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var documents []*domain.Document

	for rows.Next() {
		var doc domain.Document
		var analysisJSON []byte
		var uploadedAt time.Time

		err := rows.Scan(
			&doc.ID,
			&doc.CaseID,
			&doc.Name,
			&doc.Type,
			&doc.Status,
			&doc.ContentType,
			&doc.FilePath,
			&doc.FileURL,
			&doc.Size,
			&uploadedAt,
			&analysisJSON,
		)

		if err != nil {
			return nil, err
		}

		doc.UploadedAt = uploadedAt

		if len(analysisJSON) > 0 {
			var analysis map[string]any
			if err := json.Unmarshal(analysisJSON, &analysis); err == nil {
				doc.Analysis = analysis
			}
		}

		documents = append(documents, &doc)
	}

	return documents, nil
}

// UpdateDocumentAnalysis updates the AI analysis results for a document
func (r *DocumentRepository) UpdateDocumentAnalysis(id string, analysis map[string]any) error {
	analysisJSON, err := json.Marshal(analysis)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(`
		UPDATE documents
		SET analysis = $1
		WHERE id = $2
	`, analysisJSON, id)

	return err
}

// UpdateDocumentStatus updates the processing status of a document
func (r *DocumentRepository) UpdateDocumentStatus(id string, status domain.DocumentStatus) error {
	_, err := r.db.Exec(`
		UPDATE documents
		SET status = $1
		WHERE id = $2
	`, status, id)

	return err
}

// DeleteDocument removes a document from the repository
func (r *DocumentRepository) DeleteDocument(id string) error {
	_, err := r.db.Exec(`
		DELETE FROM documents
		WHERE id = $1
	`, id)

	return err
}
