package knowledge

import (
	"testing"
)

func TestNewMedicalKnowledgeBase(t *testing.T) {
	kb, err := NewMedicalKnowledgeBase("")

	if err != nil {
		t.Fatalf("NewMedicalKnowledgeBase() error = %v", err)
	}

	if kb == nil {
		t.Fatalf("NewMedicalKnowledgeBase() returned nil KB")
	}

	if len(kb.conditions) == 0 {
		t.Errorf("Embedded conditions not loaded")
	}

	if len(kb.medications) == 0 {
		t.Errorf("Embedded medications not loaded")
	}

	if len(kb.symptoms) == 0 {
		t.Errorf("Embedded symptoms not loaded")
	}

	if len(kb.interactionRules) == 0 {
		t.Errorf("Embedded interaction rules not loaded")
	}
}

func TestMedicalKnowledgeBase_LookupCondition(t *testing.T) {
	kb, _ := NewMedicalKnowledgeBase("")

	condition, exists := kb.LookupCondition("c001")
	if !exists {
		t.Errorf("Condition with ID 'c001' should exist")
	}
	if condition.Name != "Coronary Artery Disease" {
		t.Errorf("Expected 'Coronary Artery Disease', got '%s'", condition.Name)
	}

	condition, exists = kb.LookupCondition("Coronary Artery Disease")
	if !exists {
		t.Errorf("Condition with name 'Coronary Artery Disease' should exist")
	}
	if condition.ID != "c001" {
		t.Errorf("Expected ID 'c001', got '%s'", condition.ID)
	}

	_, exists = kb.LookupCondition("nonexistent")
	if exists {
		t.Errorf("Non-existent condition should not be found")
	}
}

func TestMedicalKnowledgeBase_GetRelatedConditions(t *testing.T) {
	kb, _ := NewMedicalKnowledgeBase("")

	symptoms := []string{"chest pain", "shortness of breath"}
	conditions := kb.GetRelatedConditions(symptoms)

	if len(conditions) == 0 {
		t.Errorf("No conditions found for symptoms that should match")
	}

	if len(conditions) > 0 && conditions[0].Name != "Coronary Artery Disease" {
		t.Errorf("Expected 'Coronary Artery Disease' as first condition, got '%s'", conditions[0].Name)
	}

	noMatchSymptoms := []string{"nonexistent symptom"}
	noMatchConditions := kb.GetRelatedConditions(noMatchSymptoms)

	if len(noMatchConditions) > 0 {
		t.Errorf("Found conditions for non-matching symptoms")
	}
}

func TestMedicalKnowledgeBase_CheckMedicationInteractions(t *testing.T) {
	kb, _ := NewMedicalKnowledgeBase("")

	meds := []string{"aspirin", "warfarin"}
	interactions := kb.CheckMedicationInteractions(meds)

	if len(interactions) == 0 {
		t.Errorf("No interactions found for medications that should interact")
	}

	if len(interactions) > 0 {
		interaction := interactions[0]
		if interaction.Severity != "high" {
			t.Errorf("Expected 'high' severity, got '%s'", interaction.Severity)
		}

		expected := "Increased risk of bleeding when used together"
		if interaction.Description != expected {
			t.Errorf("Expected description '%s', got '%s'", expected, interaction.Description)
		}
	}

	noInteractMeds := []string{"aspirin", "lisinopril"}
	noInteractions := kb.CheckMedicationInteractions(noInteractMeds)

	if len(noInteractions) > 0 {
		t.Errorf("Found interactions for medications that should not interact")
	}
}
