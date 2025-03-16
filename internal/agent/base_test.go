package agent

import (
	"testing"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
)

func TestNewBaseAgent(t *testing.T) {
	id := "test_agent"
	name := "Test Agent"

	agent := NewBaseAgent(id, name)

	if agent.ID() != id {
		t.Errorf("Expected agent ID %s, got %s", id, agent.ID())
	}

	if agent.Name() != name {
		t.Errorf("Expected agent name %s, got %s", name, agent.Name())
	}

	if agent.Status() != domain.AgentIdle {
		t.Errorf("Expected agent status %s, got %s", domain.AgentIdle, agent.Status())
	}
}

func TestBaseAgent_SetStatus(t *testing.T) {
	agent := NewBaseAgent("test_agent", "Test Agent")

	if agent.Status() != domain.AgentIdle {
		t.Errorf("Expected initial status %s, got %s", domain.AgentIdle, agent.Status())
	}

	agent.SetStatus(domain.AgentBusy)
	if agent.Status() != domain.AgentBusy {
		t.Errorf("Expected status %s, got %s", domain.AgentBusy, agent.Status())
	}

	agent.SetStatus(domain.AgentComplete)
	if agent.Status() != domain.AgentComplete {
		t.Errorf("Expected status %s, got %s", domain.AgentComplete, agent.Status())
	}
}

func TestBaseAgent_KnowledgeManagement(t *testing.T) {
	agent := NewBaseAgent("test_agent", "Test Agent")

	value, exists := agent.GetKnowledge("test_key")
	if exists {
		t.Errorf("Expected non-existent key to return exists=false, got exists=true")
	}
	if value != nil {
		t.Errorf("Expected nil value for non-existent key, got %v", value)
	}

	agent.UpdateKnowledge("string_key", "test_value")
	value, exists = agent.GetKnowledge("string_key")
	if !exists {
		t.Errorf("Expected key to exist after storing")
	}
	if str, ok := value.(string); !ok || str != "test_value" {
		t.Errorf("Expected value 'test_value', got %v", value)
	}

	agent.UpdateKnowledge("int_key", 42)
	value, exists = agent.GetKnowledge("int_key")
	if !exists {
		t.Errorf("Expected key to exist after storing")
	}
	if num, ok := value.(int); !ok || num != 42 {
		t.Errorf("Expected value 42, got %v", value)
	}

	testMap := map[string]any{"name": "test", "value": 100}
	agent.UpdateKnowledge("map_key", testMap)
	value, exists = agent.GetKnowledge("map_key")
	if !exists {
		t.Errorf("Expected key to exist after storing")
	}
	if m, ok := value.(map[string]any); !ok {
		t.Errorf("Expected map type, got %T", value)
	} else {
		if m["name"] != "test" || m["value"] != 100 {
			t.Errorf("Map content doesn't match expected values")
		}
	}

	agent.UpdateKnowledge("string_key", "updated_value")
	value, _ = agent.GetKnowledge("string_key")
	if str, ok := value.(string); !ok || str != "updated_value" {
		t.Errorf("Expected updated value 'updated_value', got %v", value)
	}
}
