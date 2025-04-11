package handler

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/service"
	"github.com/gin-gonic/gin"
)

// DocumentHandler handles API requests for documents
type DocumentHandler struct {
	documentService *service.DocumentService
}

// NewDocumentHandler creates a new DocumentHandler instance
func NewDocumentHandler(documentService *service.DocumentService) *DocumentHandler {
	return &DocumentHandler{
		documentService: documentService,
	}
}

// UploadDocument handles document uploads
func (h *DocumentHandler) UploadDocument(c *gin.Context) {
	caseID := c.Param("case_id")
	if caseID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Case ID is required"})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 10<<20)

	if err := c.Request.ParseMultipartForm(10 << 20); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File too large (max 10MB)"})
		return
	}

	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	documentType := domain.DocumentType(c.PostForm("type"))
	if documentType == "" {
		documentType = detectDocumentType(header.Filename, header.Header.Get("Content-Type"))
	}

	documentName := c.PostForm("name")
	if documentName == "" {
		documentName = header.Filename
	}

	document, err := h.documentService.SaveDocument(
		caseID,
		documentName,
		header.Header.Get("Content-Type"),
		header.Size,
		documentType,
		file,
	)
	if err != nil {
		logger.Error("Failed to save document", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save document"})
		return
	}

	c.JSON(http.StatusOK, document)
}

// GetDocuments retrieves all documents for a case
func (h *DocumentHandler) GetDocuments(c *gin.Context) {
	caseID := c.Param("case_id")
	if caseID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Case ID is required"})
		return
	}

	documents, err := h.documentService.GetDocumentsByCaseID(caseID)
	if err != nil {
		logger.Error("Failed to get documents", "case_id", caseID, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get documents"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"documents": documents,
		"count":     len(documents),
	})
}

// GetDocument retrieves a single document by ID
func (h *DocumentHandler) GetDocument(c *gin.Context) {
	documentID := c.Param("document_id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Document ID is required"})
		return
	}

	document, err := h.documentService.GetDocument(documentID)
	if err != nil {
		logger.Error("Failed to get document", "document_id", documentID, "error", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "Document not found"})
		return
	}

	c.JSON(http.StatusOK, document)
}

// GetDocumentFile streams the document file content
func (h *DocumentHandler) GetDocumentFile(c *gin.Context) {
	documentID := c.Param("document_id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Document ID is required"})
		return
	}

	document, err := h.documentService.GetDocument(documentID)
	if err != nil {
		logger.Error("Failed to get document metadata", "document_id", documentID, "error", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "Document not found"})
		return
	}

	content, contentType, err := h.documentService.GetDocumentContent(documentID)
	if err != nil {
		logger.Error("Failed to get document content", "document_id", documentID, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get document content"})
		return
	}

	filename := document.Name
	disposition := fmt.Sprintf("inline; filename=%s", filename)

	c.Header("Content-Disposition", disposition)
	c.Header("Content-Type", contentType)
	c.Data(http.StatusOK, contentType, content)
}

// DeleteDocument removes a document
func (h *DocumentHandler) DeleteDocument(c *gin.Context) {
	documentID := c.Param("document_id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Document ID is required"})
		return
	}

	if err := h.documentService.DeleteDocument(documentID); err != nil {
		logger.Error("Failed to delete document", "document_id", documentID, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete document"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Document deleted"})
}

// GetDocumentAnalysis retrieves the detailed analysis for a document
func (h *DocumentHandler) GetDocumentAnalysis(c *gin.Context) {
	documentID := c.Param("document_id")
	if documentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Document ID is required"})
		return
	}

	analysis, err := h.documentService.GetDocumentAnalysis(documentID)
	if err != nil {
		logger.Error("Failed to get document analysis", "document_id", documentID, "error", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "Document analysis not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"document_id": documentID,
		"analysis":    analysis,
	})
}

// Helper function to detect document type from filename and content type
func detectDocumentType(filename, contentType string) domain.DocumentType {
	ext := strings.ToLower(filepath.Ext(filename))

	if strings.Contains(contentType, "pdf") {
		return domain.DocumentTypeLab
	}

	switch ext {
	case ".pdf":
		return domain.DocumentTypeLab
	case ".jpg", ".jpeg", ".png":
		return domain.DocumentTypeXRay
	case ".dcm":
		return domain.DocumentTypeCTScan
	default:
		return domain.DocumentTypeOther
	}
}
