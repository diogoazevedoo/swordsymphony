-- Create document_analysis table for storing AI analysis results
CREATE TABLE IF NOT EXISTS document_analysis (
    id VARCHAR(50) PRIMARY KEY,
    document_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    results JSONB,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error TEXT,
    UNIQUE(document_id)
);

-- Add indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_document_analysis_document_id ON document_analysis(document_id);
CREATE INDEX IF NOT EXISTS idx_document_analysis_status ON document_analysis(status); 