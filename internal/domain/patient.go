package domain

import "github.com/google/uuid"

// PatientData represents the medical information for a patient
type PatientData struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Age         int       `json:"age"`
	Gender      string    `json:"gender"`
	Conditions  []string  `json:"conditions"`
	Medications []string  `json:"medications"`
	Symptoms    []string  `json:"symptoms"`
	Allergies   []string  `json:"allergies"`
	Vitals      Vitals    `json:"vitals"`
}

// Vitals represents a patient's vital signs
type Vitals struct {
	BloodPressure    string  `json:"blood_pressure"`
	HeartRate        int     `json:"heart_rate"`
	Temperature      float64 `json:"temperature"`
	OxygenSaturation int     `json:"oxygen_saturation"`
	Weight           string  `json:"weight,omitempty"`
}

// Diagnosis represents the output of the diagnostic agent
type Diagnosis struct {
	PotentialDiagnoses []string `json:"potential_diagnoses"`
	Confidence         float64  `json:"confidence"`
	Reasoning          []string `json:"reasoning"`
	RecommendedTests   []string `json:"recommended_tests"`
}

// TreatmentPlanData represents the output of the treatment agent
type TreatmentPlanData struct {
	Recommendations   []string `json:"recommendations"`
	Medications       []string `json:"medications"`
	LifestyleChanges  []string `json:"lifestyle_changes"`
	FollowUp          []string `json:"follow_up"`
	Warnings          []string `json:"warnings"`
	Contraindications []string `json:"contraindications"`
}
