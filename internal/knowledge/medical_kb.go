package knowledge

import (
	"sort"
	"strings"
	"sync"
)

// MedicalKnowledgeBase provides access to medical information
type MedicalKnowledgeBase struct {
	conditions       map[string]Condition
	medications      map[string]Medication
	symptoms         map[string]Symptom
	interactionRules []InteractionRule
	mu               sync.RWMutex
}

// Condition represents a medical condition
type Condition struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Symptoms    []string `json:"symptoms"`
	RiskFactors []string `json:"risk_factors"`
}

// Medication represents a medication
type Medication struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	Description       string   `json:"description"`
	UsedFor           []string `json:"used_for"`
	Contraindications []string `json:"contraindications"`
	SideEffects       []string `json:"side_effects"`
}

// Symptom represents a symptom
type Symptom struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	RelatedTo   []string `json:"related_to"`
}

// InteractionRule represents a medication interaction rule
type InteractionRule struct {
	Medication1 string `json:"medication1"`
	Medication2 string `json:"medication2"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
}

// NewMedicalKnowledgeBase creates a new knowledge base
func NewMedicalKnowledgeBase(dataPath string) (*MedicalKnowledgeBase, error) {
	kb := &MedicalKnowledgeBase{
		conditions:  make(map[string]Condition),
		medications: make(map[string]Medication),
		symptoms:    make(map[string]Symptom),
	}

	// Load embedded knowledge for demos
	kb.loadEmbeddedKnowledge()

	return kb, nil
}

// LookupCondition looks up a condition by name or ID
func (kb *MedicalKnowledgeBase) LookupCondition(identifier string) (Condition, bool) {
	kb.mu.RLock()
	defer kb.mu.RUnlock()

	if condition, exists := kb.conditions[identifier]; exists {
		return condition, true
	}

	for _, condition := range kb.conditions {
		if strings.EqualFold(condition.Name, identifier) {
			return condition, true
		}
	}

	return Condition{}, false
}

// GetRelatedConditions finds conditions related to a set of symptoms
func (kb *MedicalKnowledgeBase) GetRelatedConditions(symptoms []string) []Condition {
	kb.mu.RLock()
	defer kb.mu.RUnlock()

	relatedConditions := make([]Condition, 0)
	conditionScores := make(map[string]int)

	for _, symptom := range symptoms {
		symptomLower := strings.ToLower(symptom)

		for _, s := range kb.symptoms {
			if strings.Contains(strings.ToLower(s.Name), symptomLower) {
				for _, conditionID := range s.RelatedTo {
					conditionScores[conditionID]++
				}
			}
		}
	}

	type scoredCondition struct {
		condition Condition
		score     int
	}

	scored := make([]scoredCondition, 0, len(conditionScores))
	for id, score := range conditionScores {
		if condition, exists := kb.conditions[id]; exists {
			scored = append(scored, scoredCondition{
				condition: condition,
				score:     score,
			})
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	for _, sc := range scored {
		relatedConditions = append(relatedConditions, sc.condition)
	}

	return relatedConditions
}

// CheckMedicationInteractions checks for interactions between medications
func (kb *MedicalKnowledgeBase) CheckMedicationInteractions(medications []string) []InteractionRule {
	kb.mu.RLock()
	defer kb.mu.RUnlock()

	interactions := make([]InteractionRule, 0)

	for i, med1 := range medications {
		for j := i + 1; j < len(medications); j++ {
			med2 := medications[j]

			for _, rule := range kb.interactionRules {
				if (strings.Contains(strings.ToLower(med1), strings.ToLower(rule.Medication1)) &&
					strings.Contains(strings.ToLower(med2), strings.ToLower(rule.Medication2))) ||
					(strings.Contains(strings.ToLower(med1), strings.ToLower(rule.Medication2)) &&
						strings.Contains(strings.ToLower(med2), strings.ToLower(rule.Medication1))) {
					interactions = append(interactions, rule)
				}
			}
		}
	}

	return interactions
}

// loadEmbeddedKnowledge loads a minimal set of knowledge for demos
func (kb *MedicalKnowledgeBase) loadEmbeddedKnowledge() {
	kb.conditions["c001"] = Condition{
		ID:          "c001",
		Name:        "Coronary Artery Disease",
		Description: "Narrowing or blockage of the coronary arteries",
		Symptoms:    []string{"chest pain", "shortness of breath", "fatigue", "dizziness"},
		RiskFactors: []string{"hypertension", "diabetes", "smoking", "high cholesterol", "age > 60"},
	}

	kb.conditions["c002"] = Condition{
		ID:          "c002",
		Name:        "Hypertension",
		Description: "High blood pressure",
		Symptoms:    []string{"headache", "dizziness", "blurred vision", "nosebleeds"},
		RiskFactors: []string{"obesity", "high sodium diet", "sedentary lifestyle", "family history"},
	}

	kb.medications["m001"] = Medication{
		ID:                "m001",
		Name:              "Aspirin",
		Description:       "Blood thinner that prevents clotting",
		UsedFor:           []string{"pain relief", "fever reduction", "heart attack prevention"},
		Contraindications: []string{"bleeding disorders", "ulcers", "aspirin allergy"},
		SideEffects:       []string{"stomach upset", "bleeding", "rash"},
	}

	kb.medications["m002"] = Medication{
		ID:                "m002",
		Name:              "Lisinopril",
		Description:       "ACE inhibitor for blood pressure control",
		UsedFor:           []string{"hypertension", "heart failure", "kidney protection in diabetes"},
		Contraindications: []string{"pregnancy", "history of angioedema", "renal artery stenosis"},
		SideEffects:       []string{"cough", "dizziness", "high potassium levels"},
	}

	kb.symptoms["s001"] = Symptom{
		ID:          "s001",
		Name:        "Chest Pain",
		Description: "Discomfort or pain in the chest",
		RelatedTo:   []string{"c001", "c003", "c004"},
	}

	kb.symptoms["s002"] = Symptom{
		ID:          "s002",
		Name:        "Shortness of Breath",
		Description: "Difficulty breathing or dyspnea",
		RelatedTo:   []string{"c001", "c004", "c005"},
	}

	kb.interactionRules = append(kb.interactionRules, InteractionRule{
		Medication1: "aspirin",
		Medication2: "warfarin",
		Severity:    "high",
		Description: "Increased risk of bleeding when used together",
	})
}
