package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
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

	logger.Info("Attempting to store results in PostgreSQL",
		"case_id", caseID,
		"result_keys", getMapKeys(results))

	// Start a transaction with elevated isolation level
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		logger.Error("Failed to begin transaction", "error", err)
		return errors.External(err, "Failed to begin transaction", "db_transaction_error")
	}
	defer tx.Rollback()

	// Retrieve existing data if any
	existingDataJSON := []byte("{}")
	var existingData map[string]any

	var existingID int
	checkQuery := `SELECT id, data FROM results WHERE case_id = $1`
	err = tx.QueryRowContext(ctx, checkQuery, caseID).Scan(&existingID, &existingDataJSON)

	if err == nil {
		// We have existing data, parse it
		if err := json.Unmarshal(existingDataJSON, &existingData); err != nil {
			logger.Error("Failed to unmarshal existing data", "error", err)
			existingData = make(map[string]any)
		}

		// Merge existing data with new results
		for k, v := range results {
			existingData[k] = v
		}

		// Marshal merged data
		dataJSON, err := json.Marshal(existingData)
		if err != nil {
			logger.Error("Failed to marshal merged data", "error", err)
			return errors.Internal("Failed to encode results to JSON", "json_encode_error")
		}

		// Update existing record
		updateQuery := `
			UPDATE results
			SET data = $1, updated_at = NOW()
			WHERE case_id = $2
		`
		_, err = tx.ExecContext(ctx, updateQuery, dataJSON, caseID)
		if err != nil {
			logger.Error("Failed to update results", "error", err)
			return errors.External(err, "Failed to update results", "db_update_error")
		}
	} else if err == sql.ErrNoRows {
		// No existing data, create new record
		dataJSON, err := json.Marshal(results)
		if err != nil {
			logger.Error("Failed to marshal new data", "error", err)
			return errors.Internal("Failed to encode results to JSON", "json_encode_error")
		}

		insertQuery := `
			INSERT INTO results (case_id, data, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
		`
		_, err = tx.ExecContext(ctx, insertQuery, caseID, dataJSON)
		if err != nil {
			logger.Error("Failed to insert results", "error", err, "query", insertQuery)
			return errors.External(err, "Failed to insert results", "db_insert_error")
		}
	} else {
		// Unexpected error when checking for existing data
		logger.Error("Failed to check for existing data", "error", err)
		return errors.External(err, "Failed to check existing results", "db_check_error")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		logger.Error("Failed to commit transaction", "error", err)
		return errors.External(err, "Failed to commit transaction", "db_commit_error")
	}

	logger.Info("Successfully stored results in PostgreSQL",
		"case_id", caseID,
		"result_keys", getMapKeys(results))

	return nil
}

// GetResultsByCaseID retrieves results for a specific case
func (r *ResultRepository) GetResultsByCaseID(id string) (map[string]any, error) {
	if id == "" {
		return nil, errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	logger.Info("Attempting to retrieve results from PostgreSQL", "case_id", id)

	ctx, cancel := withTimeout(defaultQueryTimeout)
	defer cancel()

	// First try direct case_id match
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
			logger.Error("Failed to unmarshal results data", "error", err)
			return nil, errors.Internal("Failed to decode results from JSON", "json_decode_error")
		}
		logger.Info("Retrieved results directly from PostgreSQL",
			"case_id", id,
			"result_keys", getMapKeys(results))
		return results, nil
	}

	if err != sql.ErrNoRows {
		logger.Error("Unexpected error retrieving results", "error", err)
		return nil, errors.External(err, "Failed to query results", "db_query_error")
	}

	// If not found by direct case_id, try looking up in cases table first
	caseQuery := `
        SELECT id, data 
        FROM cases 
        WHERE id = $1 OR data->>'id' = $1
    `

	var caseID string
	var caseDataJSON []byte
	err = r.db.QueryRowContext(ctx, caseQuery, id).Scan(&caseID, &caseDataJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("No case found with ID", "case_id", id)
			return nil, errors.NotFound("No patient or case found with ID", "patient_not_found")
		}
		logger.Error("Failed to query case", "error", err)
		return nil, errors.External(err, "Failed to query case", "db_case_query_error")
	}

	// Now try to get results using the case's ID from the database
	resultsQuery := `
        SELECT data 
        FROM results 
        WHERE case_id = $1
    `

	err = r.db.QueryRowContext(ctx, resultsQuery, caseID).Scan(&dataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("No results found for case", "case_id", caseID)
			return nil, errors.NotFound("No results found for case", "results_not_found")
		}
		logger.Error("Failed to query results using case ID", "error", err, "case_id", caseID)
		return nil, errors.External(err, "Failed to query results", "db_results_query_error")
	}

	var results map[string]any
	if err := json.Unmarshal(dataJSON, &results); err != nil {
		logger.Error("Failed to unmarshal results data", "error", err)
		return nil, errors.Internal("Failed to decode results from JSON", "json_decode_error")
	}

	logger.Info("Retrieved results via case lookup from PostgreSQL",
		"requested_id", id,
		"database_id", caseID,
		"result_keys", getMapKeys(results))

	return results, nil
}

// storeResultsWithTransaction stores results for a case within a transaction
func (r *ResultRepository) storeResultsWithTransaction(caseID string, results map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return errors.External(err, "Failed to begin transaction", "db_transaction_error")
	}
	defer tx.Rollback()

	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return errors.Internal("Failed to encode results to JSON", "json_encode_error")
	}

	var existingID int
	checkQuery := `SELECT id FROM results WHERE case_id = $1`
	err = tx.QueryRowContext(ctx, checkQuery, caseID).Scan(&existingID)

	if err == nil {
		updateQuery := `
            UPDATE results
            SET data = $1, updated_at = NOW()
            WHERE case_id = $2
        `
		_, err = tx.ExecContext(ctx, updateQuery, resultsJSON, caseID)
		if err != nil {
			return errors.External(err, "Failed to update results", "db_update_error")
		}
	} else if err == sql.ErrNoRows {
		insertQuery := `
            INSERT INTO results (case_id, data, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
        `
		_, err = tx.ExecContext(ctx, insertQuery, resultsJSON, caseID)
		if err != nil {
			return errors.External(err, "Failed to insert results", "db_insert_error")
		}
	} else {
		return errors.External(err, "Failed to check existing results", "db_check_error")
	}

	if err := tx.Commit(); err != nil {
		return errors.External(err, "Failed to commit transaction", "db_commit_error")
	}

	return nil
}

// getMapKeys extracts all keys from a map for logging
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
