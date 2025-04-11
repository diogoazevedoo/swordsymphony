package domain

import "time"

// DocumentType represents the type of a document
type DocumentType string

const (
	// DocumentTypeLab represents lab test results
	DocumentTypeLab DocumentType = "lab_result"
	// DocumentTypeXRay represents X-ray images
	DocumentTypeXRay DocumentType = "xray"
	// DocumentTypeCTScan represents CT scan images
	DocumentTypeCTScan DocumentType = "ctscan"
	// DocumentTypeMRI represents MRI images
	DocumentTypeMRI DocumentType = "mri"
	// DocumentTypeOther represents other types of documents
	DocumentTypeOther DocumentType = "other"
)

// DocumentStatus represents the processing status of a document
type DocumentStatus string

const (
	DocumentStatusUploaded   DocumentStatus = "uploaded"
	DocumentStatusProcessing DocumentStatus = "processing"
	DocumentStatusAnalyzed   DocumentStatus = "analyzed"
	DocumentStatusFailed     DocumentStatus = "failed"
)

// Document represents a medical document uploaded to the system
type Document struct {
	ID          string         `json:"id"`
	CaseID      string         `json:"case_id"`
	Name        string         `json:"name"`
	Type        DocumentType   `json:"type"`
	Status      DocumentStatus `json:"status"`
	ContentType string         `json:"content_type"`
	FilePath    string         `json:"-"`
	FileURL     string         `json:"file_url"`
	Size        int64          `json:"size"`
	UploadedAt  time.Time      `json:"uploaded_at"`
	Analysis    map[string]any `json:"analysis,omitempty"`
}
