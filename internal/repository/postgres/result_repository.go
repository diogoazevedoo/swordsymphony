package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
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
			return fmt.Errorf("failed to update results: %w", err)
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
			return fmt.Errorf("failed to insert results: %w", err)
		}
		return nil
	}

	return fmt.Errorf("failed to check existing results: %w", err)
}

// GetResultsByCaseID retrieves results for a specific case
func (r *ResultRepository) GetResultsByCaseID(id string) (map[string]any, error) {
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
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}
		return results, nil
	}

	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query results: %w", err)
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
			return nil, fmt.Errorf("no patient or case found with ID: %s", id)
		}
		return nil, fmt.Errorf("failed to query case: %w", err)
	}

	resultsQuery := `
		SELECT data 
		FROM results 
		WHERE case_id = $1
	`

	err = r.db.QueryRowContext(ctx, resultsQuery, caseID).Scan(&dataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no results found for case")
		}
		return nil, fmt.Errorf("failed to query results: %w", err)
	}

	var results map[string]any
	if err := json.Unmarshal(dataJSON, &results); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	return results, nil
}
