package memory

import (
	"maps"
	"sync"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
)

// CaseRepository is an in-memory implementation of the CaseRepository interface
type CaseRepository struct {
	cases       map[string]map[string]any
	currentCase map[string]any
	mu          sync.RWMutex
}

// NewCaseRepository creates a new in-memory case repository
func NewCaseRepository() *CaseRepository {
	return &CaseRepository{
		cases: make(map[string]map[string]any),
	}
}

// GetDemoCases returns all available demo cases
func (r *CaseRepository) GetDemoCases() (map[string]map[string]any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.cases) == 0 {
		return nil, errors.NotFound("No demo cases available", "no_demo_cases")
	}

	result := make(map[string]map[string]any)
	maps.Copy(result, r.cases)

	return result, nil
}

// GetCaseByID returns a specific case by ID
func (r *CaseRepository) GetCaseByID(id string) (map[string]any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	caseData, exists := r.cases[id]
	if !exists {
		return nil, errors.NotFound("case not found", "case_not_found")
	}

	return caseData, nil
}

// GetAllCases returns all cases in the system
func (r *CaseRepository) GetAllCases() (map[string]map[string]any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.cases) == 0 {
		return nil, errors.NotFound("No cases available", "no_cases")
	}

	result := make(map[string]map[string]any)
	maps.Copy(result, r.cases)

	return result, nil
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

// InitializeDemoCases loads the initial set of demo cases
func (r *CaseRepository) InitializeDemoCases() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cases = map[string]map[string]any{
		"cardiac_case": {
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
		"respiratory_case": {
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
		"complex_eldercare": {
			"id":     "P34567",
			"name":   "Eleanor Williams",
			"age":    79,
			"gender": "female",
			"conditions": []string{
				"Atrial fibrillation",
				"Osteoarthritis",
				"Early-stage dementia",
				"Osteoporosis",
			},
			"medications": []string{
				"Warfarin 5mg daily",
				"Donepezil 10mg at bedtime",
				"Calcium + Vitamin D supplement",
				"Acetaminophen as needed for pain",
			},
			"symptoms": []string{
				"Increased confusion in evenings",
				"Recent fall (no injury)",
				"Poor appetite",
				"Weight loss of 5 pounds in past month",
			},
			"allergies": []string{
				"Codeine",
			},
			"vitals": map[string]any{
				"blood_pressure":    "135/82",
				"heart_rate":        72,
				"temperature":       97.8,
				"oxygen_saturation": 95,
				"weight":            "128 lbs (down from 133)",
			},
		},
	}

	return nil
}

func (r *CaseRepository) StoreCase(id string, caseData map[string]any, isDemo bool) error {
	if id == "" {
		return errors.Validation("Case ID cannot be empty", "empty_case_id")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.cases[id] = caseData
	return nil
}
