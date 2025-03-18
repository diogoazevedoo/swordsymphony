package postgres

import (
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
)

// CaseRepository is a PostgreSQL implementation of the CaseRepository interface
type CaseRepository struct {
	db          *DB
	currentCase map[string]any
	mu          sync.RWMutex
}

// NewCaseRepository creates a new PostgreSQL case repository
func NewCaseRepository(db *DB) *CaseRepository {
	return &CaseRepository{
		db: db,
	}
}

// GetDemoCases returns all available demo cases
func (r *CaseRepository) GetDemoCases() (map[string]map[string]any, error) {
	ctx, cancel := withTimeout(defaultQueryTimeout)
	defer cancel()

	query := `
        SELECT id, data 
        FROM cases 
        WHERE is_demo = true
    `

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.External(err, "Failed to query demo cases", "db_query_error")
	}
	defer rows.Close()

	cases := make(map[string]map[string]any)
	for rows.Next() {
		var id string
		var dataJSON []byte

		if err := rows.Scan(&id, &dataJSON); err != nil {
			return nil, errors.External(err, "Failed to scan case row", "db_scan_error")
		}

		var caseData map[string]any
		if err := json.Unmarshal(dataJSON, &caseData); err != nil {
			return nil, errors.Internal("Failed to decode case data from JSON", "json_decode_error")
		}

		cases[id] = caseData
	}

	if err := rows.Err(); err != nil {
		return nil, errors.External(err, "Error iterating case rows", "db_iteration_error")
	}

	if len(cases) == 0 {
		return nil, errors.NotFound("No demo cases found", "no_demo_cases")
	}

	return cases, nil
}

// GetCaseByID returns a specific case by ID
func (r *CaseRepository) GetCaseByID(id string) (map[string]any, error) {
	if id == "" {
		return nil, errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	ctx, cancel := withTimeout(defaultQueryTimeout)
	defer cancel()

	query := `
        SELECT data 
        FROM cases 
        WHERE id = $1
    `

	var dataJSON []byte
	err := r.db.QueryRowContext(ctx, query, id).Scan(&dataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NotFound("Case not found", "case_not_found")
		}
		return nil, errors.External(err, "Failed to query case from database", "db_query_error")
	}

	var caseData map[string]any
	if err := json.Unmarshal(dataJSON, &caseData); err != nil {
		return nil, errors.Internal("Failed to decode case data from JSON", "json_decode_error")
	}

	return caseData, nil
}

// GetCurrentCase returns the case currently being processed, or nil if none
func (r *CaseRepository) GetCurrentCase() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentCase
}

// SetCurrentCase sets the case currently being processed
func (r *CaseRepository) SetCurrentCase(caseData map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentCase = caseData
}

// ClearCurrentCase clears the currently processing case
func (r *CaseRepository) ClearCurrentCase() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentCase = nil
}

// InitializeDemoCases loads the initial set of demo cases if they don't exist
func (r *CaseRepository) InitializeDemoCases() error {
	cases, err := r.GetDemoCases()
	if err != nil {
		if appErr, ok := err.(*errors.AppError); !ok || appErr.Type != errors.ErrorTypeNotFound {
			return errors.Wrap(err, errors.ErrorTypeInternal, "Failed to check for existing demo cases", "demo_case_check_error")
		}
	}

	if len(cases) > 0 {
		return nil
	}

	ctx, cancel := withTimeout(defaultTxTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.External(err, "Failed to begin transaction", "db_transaction_error")
	}
	defer tx.Rollback()

	demoCases := []struct {
		id   string
		data map[string]any
	}{
		{
			id: "cardiac_case",
			data: map[string]any{
				"id":     "P12345",
				"name":   "Robert Johnson",
				"age":    65,
				"gender": "male",
				"conditions": []string{
					"Type 2 Diabetes (diagnosed 2015)",
					"Hypertension (diagnosed 2012)",
					"Recent knee replacement (3 months ago)",
				},
				"medications": []string{
					"Metformin 500mg twice daily",
					"Lisinopril 10mg daily",
					"Aspirin 81mg daily",
				},
				"symptoms": []string{
					"Increasing fatigue over past 2 weeks",
					"Mild chest discomfort when walking",
					"Occasional dizziness",
					"Shortness of breath climbing stairs",
				},
				"allergies": []string{
					"Penicillin",
					"Sulfa drugs",
				},
				"vitals": map[string]any{
					"blood_pressure":    "145/90",
					"heart_rate":        88,
					"temperature":       98.6,
					"oxygen_saturation": 96,
				},
			},
		},
		{
			id: "respiratory_case",
			data: map[string]any{
				"id":     "P23456",
				"name":   "Sarah Miller",
				"age":    42,
				"gender": "female",
				"conditions": []string{
					"Asthma (childhood onset)",
					"Seasonal allergies",
				},
				"medications": []string{
					"Albuterol inhaler as needed",
					"Fluticasone nasal spray",
				},
				"symptoms": []string{
					"Productive cough for 10 days",
					"Low-grade fever (100.2°F)",
					"Increased wheezing",
					"Chest tightness",
				},
				"allergies": []string{
					"Dust",
					"Pollen",
					"Cats",
				},
				"vitals": map[string]any{
					"blood_pressure":    "118/76",
					"heart_rate":        92,
					"temperature":       100.2,
					"oxygen_saturation": 94,
				},
			},
		},
	}

	insertQuery := `
        INSERT INTO cases (id, data, is_demo, created_at, updated_at)
        VALUES ($1, $2, true, NOW(), NOW())
    `

	for _, demo := range demoCases {
		dataJSON, err := json.Marshal(demo.data)
		if err != nil {
			return errors.Internal("Failed to encode case data to JSON", "json_encode_error")
		}

		_, err = tx.ExecContext(ctx, insertQuery, demo.id, dataJSON)
		if err != nil {
			return errors.External(err, "Failed to insert demo case", "db_insert_error")
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.External(err, "Failed to commit transaction", "db_commit_error")
	}

	return nil
}

// StoreCase adds or updates a case in the PostgreSQL repository
func (r *CaseRepository) StoreCase(id string, caseData map[string]any, isDemo bool) error {
	if id == "" {
		return errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	ctx, cancel := withTimeout(defaultTxTimeout)
	defer cancel()

	dataJSON, err := json.Marshal(caseData)
	if err != nil {
		return errors.Internal("Failed to encode case data to JSON", "json_encode_error")
	}

	var existingID string
	checkQuery := `SELECT id FROM cases WHERE id = $1`
	err = r.db.QueryRowContext(ctx, checkQuery, id).Scan(&existingID)

	if err == nil {
		updateQuery := `
			UPDATE cases
			SET data = $1, is_demo = $2, updated_at = NOW()
			WHERE id = $3
		`
		_, err = r.db.ExecContext(ctx, updateQuery, dataJSON, isDemo, id)
		if err != nil {
			return errors.External(err, "Failed to update case", "db_update_error")
		}
		return nil
	}

	if err == sql.ErrNoRows {
		insertQuery := `
			INSERT INTO cases (id, data, is_demo, created_at, updated_at)
			VALUES ($1, $2, $3, NOW(), NOW())
		`
		_, err = r.db.ExecContext(ctx, insertQuery, id, dataJSON, isDemo)
		if err != nil {
			return errors.External(err, "Failed to insert case", "db_insert_error")
		}
		return nil
	}

	return errors.External(err, "Failed to check existing case", "db_check_error")
}
