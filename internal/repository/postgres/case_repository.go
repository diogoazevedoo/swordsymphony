package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		SELECT id, data 
		FROM cases 
		WHERE is_demo = true
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query demo cases: %w", err)
	}
	defer rows.Close()

	cases := make(map[string]map[string]any)
	for rows.Next() {
		var id string
		var dataJSON []byte

		if err := rows.Scan(&id, &dataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan case row: %w", err)
		}

		var caseData map[string]any
		if err := json.Unmarshal(dataJSON, &caseData); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		cases[id] = caseData
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating case rows: %w", err)
	}

	return cases, nil
}

// GetCaseByID returns a specific case by ID
func (r *CaseRepository) GetCaseByID(id string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			return nil, fmt.Errorf("case not found")
		}
		return nil, fmt.Errorf("failed to query case: %w", err)
	}

	var caseData map[string]any
	if err := json.Unmarshal(dataJSON, &caseData); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
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
		return fmt.Errorf("failed to check for existing demo cases: %w", err)
	}

	if len(cases) > 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
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
			return fmt.Errorf("failed to encode JSON: %w", err)
		}

		_, err = tx.ExecContext(ctx, insertQuery, demo.id, dataJSON)
		if err != nil {
			return fmt.Errorf("failed to insert demo case: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
