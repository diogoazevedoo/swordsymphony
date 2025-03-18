package knowledge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// LoadMedicalDataFromJSON loads medical knowledge from JSON files
func LoadMedicalDataFromJSON(dataPath string) (*MedicalKnowledgeBase, error) {
	kb := &MedicalKnowledgeBase{
		conditions:  make(map[string]Condition),
		medications: make(map[string]Medication),
		symptoms:    make(map[string]Symptom),
	}

	if dataPath == "" || dataPath == "embedded" {
		kb.loadEmbeddedKnowledge()
		return kb, nil
	}

	conditionsPath := filepath.Join(dataPath, "conditions.json")
	if err := loadJSONFile(conditionsPath, &kb.conditions); err != nil {
		return nil, fmt.Errorf("failed to load conditions: %w", err)
	}

	medicationsPath := filepath.Join(dataPath, "medications.json")
	if err := loadJSONFile(medicationsPath, &kb.medications); err != nil {
		return nil, fmt.Errorf("failed to load medications: %w", err)
	}

	symptomsPath := filepath.Join(dataPath, "symptoms.json")
	if err := loadJSONFile(symptomsPath, &kb.symptoms); err != nil {
		return nil, fmt.Errorf("failed to load symptoms: %w", err)
	}

	interactionsPath := filepath.Join(dataPath, "interactions.json")
	if err := loadJSONFile(interactionsPath, &kb.interactionRules); err != nil {
		return nil, fmt.Errorf("failed to load interaction rules: %w", err)
	}

	return kb, nil
}

// loadJSONFile loads data from a JSON file into the target data structure
func loadJSONFile(filePath string, target any) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("error parsing JSON: %w", err)
	}

	return nil
}
