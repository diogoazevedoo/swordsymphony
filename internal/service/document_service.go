package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/google/uuid"
)

// DocumentService handles document uploads and processing
type DocumentService struct {
	documentRepo         repository.DocumentRepository
	documentAnalysisRepo repository.DocumentAnalysisRepository
	uploadPath           string
	baseURL              string
	aiClient             ai.Client
}

// NewDocumentService creates a new document service
func NewDocumentService(
	documentRepo repository.DocumentRepository,
	documentAnalysisRepo repository.DocumentAnalysisRepository,
	uploadPath, baseURL string,
	aiClient ai.Client,
) *DocumentService {
	return &DocumentService{
		documentRepo:         documentRepo,
		documentAnalysisRepo: documentAnalysisRepo,
		uploadPath:           uploadPath,
		baseURL:              baseURL,
		aiClient:             aiClient,
	}
}

// SaveDocument handles the document upload process and automatically analyzes it
func (s *DocumentService) SaveDocument(caseID, name, contentType string, fileSize int64, fileType domain.DocumentType, file multipart.File) (*domain.Document, error) {
	caseDir := filepath.Join(s.uploadPath, caseID)
	if err := os.MkdirAll(caseDir, 0755); err != nil {
		logger.Error("Failed to create case directory", "case_id", caseID, "error", err)
		return nil, fmt.Errorf("failed to create case directory: %w", err)
	}

	docID := uuid.New().String()
	fileExt := getFileExtension(contentType, name)
	filename := fmt.Sprintf("%s-%s%s", fileType, docID, fileExt)
	filePath := filepath.Join(caseDir, filename)

	dest, err := os.Create(filePath)
	if err != nil {
		logger.Error("Failed to create file", "path", filePath, "error", err)
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer dest.Close()

	if _, err = io.Copy(dest, file); err != nil {
		logger.Error("Failed to save file", "path", filePath, "error", err)
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to save file: %w", err)
	}

	relativePath := filepath.Join(caseID, filename)
	document := &domain.Document{
		ID:          docID,
		CaseID:      caseID,
		Name:        name,
		Type:        fileType,
		ContentType: contentType,
		FilePath:    relativePath,
		FileURL:     fmt.Sprintf("%s/api/documents/%s/file", s.baseURL, docID),
		Size:        fileSize,
		UploadedAt:  time.Now(),
		Status:      domain.DocumentStatusProcessing,
	}

	if err := s.documentRepo.StoreDocument(document); err != nil {
		logger.Error("Failed to store document metadata", "doc_id", docID, "error", err)
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to store document metadata: %w", err)
	}

	analysis := domain.NewDocumentAnalysis(docID)
	if err := s.documentAnalysisRepo.CreateAnalysis(analysis); err != nil {
		logger.Error("Failed to create document analysis record", "doc_id", docID, "error", err)
	}

	go s.analyzeDocument(docID, filePath, contentType, string(fileType))

	logger.Info("Document saved successfully", "doc_id", docID, "case_id", caseID)
	return document, nil
}

// analyzeDocument uses AI to analyze the document and updates the document with analysis results
func (s *DocumentService) analyzeDocument(documentID, filePath, contentType, documentType string) {
	logger.Info("Starting document analysis", "doc_id", documentID, "type", documentType)

	analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(documentID)
	if err != nil {
		logger.Error("Failed to retrieve analysis record", "doc_id", documentID, "error", err)
		analysis = domain.NewDocumentAnalysis(documentID)
		if err := s.documentAnalysisRepo.CreateAnalysis(analysis); err != nil {
			logger.Error("Failed to create analysis record", "doc_id", documentID, "error", err)
			return
		}
	}

	if err := s.documentRepo.UpdateDocumentStatus(documentID, domain.DocumentStatusProcessing); err != nil {
		logger.Error("Failed to update document status to processing", "doc_id", documentID, "error", err)
	}

	if err := s.documentAnalysisRepo.UpdateAnalysisStatus(analysis.ID, domain.AnalysisInProgress); err != nil {
		logger.Error("Failed to update analysis status to in_progress", "analysis_id", analysis.ID, "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	analysisResults, err := s.aiClient.AnalyzeDocument(ctx, filePath, contentType, documentType)

	if err != nil && analysisResults == nil {
		logger.Error("Document analysis failed completely", "doc_id", documentID, "error", err)

		errorResults := map[string]interface{}{
			"error":       err.Error(),
			"confidence":  0,
			"text":        fmt.Sprintf("Analysis failed: %s", err.Error()),
			"analyzed_at": time.Now().Format(time.RFC3339),
		}

		if updateErr := s.documentRepo.UpdateDocumentAnalysis(documentID, errorResults); updateErr != nil {
			logger.Error("Failed to update document with error analysis", "doc_id", documentID, "error", updateErr)
		}

		if updateErr := s.documentAnalysisRepo.UpdateAnalysisResults(analysis.ID, errorResults); updateErr != nil {
			logger.Error("Failed to update analysis results with error", "analysis_id", analysis.ID, "error", updateErr)
		}

		analysis.SetError(err.Error())
		if updateErr := s.documentAnalysisRepo.CreateAnalysis(analysis); updateErr != nil {
			logger.Error("Failed to update analysis with error", "analysis_id", analysis.ID, "error", updateErr)
		}

		if updateErr := s.documentRepo.UpdateDocumentStatus(documentID, domain.DocumentStatusAnalyzed); updateErr != nil {
			logger.Error("Failed to update document status to analyzed", "doc_id", documentID, "error", updateErr)
		}

		return
	} else if err != nil {
		logger.Warn("Document analysis completed with warnings", "doc_id", documentID, "error", err)
	}

	if analysisResults == nil {
		analysisResults = map[string]interface{}{
			"error":       "Unknown analysis error",
			"confidence":  0,
			"text":        "Analysis failed with an unknown error",
			"analyzed_at": time.Now().Format(time.RFC3339),
		}
	}

	analysisResults["analyzed_at"] = time.Now().Format(time.RFC3339)

	if err := s.documentAnalysisRepo.UpdateAnalysisResults(analysis.ID, analysisResults); err != nil {
		logger.Error("Failed to update analysis results", "analysis_id", analysis.ID, "error", err)
	}

	if err := s.documentRepo.UpdateDocumentAnalysis(documentID, analysisResults); err != nil {
		logger.Error("Failed to update document analysis", "doc_id", documentID, "error", err)
	}

	if err := s.documentRepo.UpdateDocumentStatus(documentID, domain.DocumentStatusAnalyzed); err != nil {
		logger.Error("Failed to update document status to analyzed", "doc_id", documentID, "error", err)
	}

	logger.Info("Document analysis completed", "doc_id", documentID, "status", "success")
}

// GetDocument retrieves document metadata by ID
func (s *DocumentService) GetDocument(id string) (*domain.Document, error) {
	document, err := s.documentRepo.GetDocumentByID(id)
	if err != nil {
		return nil, err
	}

	if document.Status == domain.DocumentStatusAnalyzed && (document.Analysis == nil || len(document.Analysis) == 0) {
		analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(id)
		if err == nil && analysis.Status == domain.AnalysisCompleted {
			results, err := analysis.GetResults()
			if err == nil {
				document.Analysis = results
			}
		}
	}

	return document, nil
}

// GetDocumentsByCaseID retrieves all documents for a case
func (s *DocumentService) GetDocumentsByCaseID(caseID string) ([]*domain.Document, error) {
	documents, err := s.documentRepo.GetDocumentsByCaseID(caseID)
	if err != nil {
		return nil, err
	}

	for _, doc := range documents {
		if doc.Status == domain.DocumentStatusAnalyzed && (doc.Analysis == nil || len(doc.Analysis) == 0) {
			analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(doc.ID)
			if err == nil && analysis.Status == domain.AnalysisCompleted {
				results, err := analysis.GetResults()
				if err == nil {
					doc.Analysis = results
				}
			}
		}
	}

	return documents, nil
}

// GetDocumentContent reads and returns the file content and content type
func (s *DocumentService) GetDocumentContent(id string) ([]byte, string, error) {
	doc, err := s.documentRepo.GetDocumentByID(id)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch document metadata: %w", err)
	}

	filePath := filepath.Join(s.uploadPath, doc.FilePath)
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read document file: %w", err)
	}

	return content, doc.ContentType, nil
}

// GetDocumentAnalysis gets the analysis for a document
func (s *DocumentService) GetDocumentAnalysis(id string) (map[string]interface{}, error) {
	doc, err := s.documentRepo.GetDocumentByID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document: %w", err)
	}

	if doc.Analysis != nil && len(doc.Analysis) > 0 {
		return doc.Analysis, nil
	}

	analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch document analysis: %w", err)
	}

	results, err := analysis.GetResults()
	if err != nil {
		return nil, fmt.Errorf("failed to parse analysis results: %w", err)
	}

	if results != nil {
		s.documentRepo.UpdateDocumentAnalysis(id, results)
	}

	return results, nil
}

// UpdateDocumentAnalysis updates the AI analysis results for a document
func (s *DocumentService) UpdateDocumentAnalysis(id string, analysisResults map[string]any) error {
	if err := s.documentRepo.UpdateDocumentAnalysis(id, analysisResults); err != nil {
		return err
	}

	analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(id)
	if err != nil {
		analysis = domain.NewDocumentAnalysis(id)
		if err := analysis.SetResults(analysisResults); err != nil {
			return err
		}
		return s.documentAnalysisRepo.CreateAnalysis(analysis)
	}

	return s.documentAnalysisRepo.UpdateAnalysisResults(analysis.ID, analysisResults)
}

// DeleteDocument removes a document and its file
func (s *DocumentService) DeleteDocument(id string) error {
	doc, err := s.documentRepo.GetDocumentByID(id)
	if err != nil {
		return fmt.Errorf("failed to fetch document metadata: %w", err)
	}

	filePath := filepath.Join(s.uploadPath, doc.FilePath)
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("Failed to delete document file", "path", filePath, "error", err)
	}

	analysis, err := s.documentAnalysisRepo.GetAnalysisByDocumentID(id)
	if err == nil {
		if err := s.documentAnalysisRepo.DeleteAnalysis(analysis.ID); err != nil {
			logger.Error("Failed to delete document analysis", "analysis_id", analysis.ID, "error", err)
		}
	}

	return s.documentRepo.DeleteDocument(id)
}

// Helper function to get file extension
func getFileExtension(contentType, filename string) string {
	if ext := filepath.Ext(filename); ext != "" {
		return ext
	}

	switch {
	case strings.Contains(contentType, "pdf"):
		return ".pdf"
	case strings.Contains(contentType, "jpeg") || strings.Contains(contentType, "jpg"):
		return ".jpg"
	case strings.Contains(contentType, "png"):
		return ".png"
	case strings.Contains(contentType, "dicom"):
		return ".dcm"
	default:
		return ".bin"
	}
}
