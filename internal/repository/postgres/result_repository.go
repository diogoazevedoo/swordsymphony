package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
)

// ResultRepository is a PostgreSQL implementation of the ResultRepository interface
type ResultRepository struct {
	db *DB
}

// NewResultRepository creates a new PostgreSQL result repository
func NewResultRepository(db *DB) *ResultRepository {
	return &ResultRepository{
		db: db,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return errors.Internal("Failed to encode results to JSON", "json_encode_error")
	}

	var existingID int
	checkQuery := `SELECT id FROM results WHERE case_id = $1`
	err = r.db.QueryRowContext(ctx, checkQuery, caseID).Scan(&existingID)

	if err == nil {
		updateQuery := `
            UPDATE results
            SET data = $1, updated_at = NOW()
            WHERE case_id = $2
        `
		_, err = r.db.ExecContext(ctx, updateQuery, resultsJSON, caseID)
		if err != nil {
			return errors.External(err, "Failed to update results", "db_update_error")
		}
		return nil
	}

	if err == sql.ErrNoRows {
		insertQuery := `
            INSERT INTO results (case_id, data, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
        `
		_, err = r.db.ExecContext(ctx, insertQuery, caseID, resultsJSON)
		if err != nil {
			return errors.External(err, "Failed to insert results", "db_insert_error")
		}
		return nil
	}

	return errors.External(err, "Failed to check existing results", "db_check_error")
}

// GetResultsByCaseID retrieves results for a specific case
func (r *ResultRepository) GetResultsByCaseID(id string) (map[string]any, error) {
	if id == "" {
		return nil, errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
        SELECT data 
        FROM results 
        WHERE case_id = $1
    `

	var dataJSON []byte
	err := r.db.QueryRowContext(ctx, query, id).Scan(&dataJSON)

	if err == nil {
		var results map[string]any
		if err := json.Unmarshal(dataJSON, &results); err != nil {
			return nil, errors.Internal("Failed to decode results from JSON", "json_decode_error")
		}
		return results, nil
	}

	if err != sql.ErrNoRows {
		return nil, errors.External(err, "Failed to query results", "db_query_error")
	}

	caseQuery := `
        SELECT id, data 
        FROM cases 
        WHERE data->>'id' = $1
    `

	var caseID string
	var caseDataJSON []byte
	err = r.db.QueryRowContext(ctx, caseQuery, id).Scan(&caseID, &caseDataJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NotFound("No patient or case found with ID", "patient_not_found")
		}
		return nil, errors.External(err, "Failed to query case", "db_case_query_error")
	}

	resultsQuery := `
        SELECT data 
        FROM results 
        WHERE case_id = $1
    `

	err = r.db.QueryRowContext(ctx, resultsQuery, caseID).Scan(&dataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NotFound("No results found for case", "results_not_found")
		}
		return nil, errors.External(err, "Failed to query results", "db_results_query_error")
	}

	var results map[string]any
	if err := json.Unmarshal(dataJSON, &results); err != nil {
		return nil, errors.Internal("Failed to decode results from JSON", "json_decode_error")
	}

	return results, nil
}
