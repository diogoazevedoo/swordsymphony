package main

import (
	"log"
	"os"

	"github.com/diogoazevedoo/swordsymphony/internal/agent"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/diogoazevedoo/swordsymphony/internal/server"
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}
}

func main() {
	openAIKey := os.Getenv("OPENAI_API_KEY")
	if openAIKey == "" {
		log.Fatal("OPENAI_API_KEY must exist")
	}

	aiClient, err := ai.NewClient(ai.Provider("openai"), openAIKey)
	if err != nil {
		log.Fatalf("Warning: failed to initialize AI client: %v", err)
	}

	medicalKB, err := knowledge.NewMedicalKnowledgeBase("")
	if err != nil {
		log.Fatalf("Warning: failed to load medical knowledge base: %v", err)
	}

	caseRepo := memory.NewCaseRepository()
	resultRepo := memory.NewResultRepository()

	err = caseRepo.InitializeDemoCases()
	if err != nil {
		log.Fatalf("Failed to initialize demo cases: %v", err)
	}

	orch := orchestrator.NewOrchestrator()

	intakeAgent := agent.NewIntakeAgent("intake_agent", "Patient Intake Agent")
	diagnosticAgent := agent.NewDiagnosticAgent("diagnostic_agent", "Diagnostic Agent", aiClient, medicalKB)
	treatmentAgent := agent.NewTreatmentAgent("treatment_agent", "Treatment Agent", aiClient, medicalKB)

	orch.RegisterAgent(intakeAgent)
	orch.RegisterAgent(diagnosticAgent)
	orch.RegisterAgent(treatmentAgent)

	orch.StartProcessing()

	h := handler.NewHandler(orch, caseRepo, resultRepo)

	srv := server.NewServer(":8080")
	srv.SetupRoutes(h)

	log.Println("Starting Sword Symphony API on port :8080")
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
