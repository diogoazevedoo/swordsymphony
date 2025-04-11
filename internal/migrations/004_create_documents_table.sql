-- Create documents table for storing document metadata
CREATE TABLE IF NOT EXISTS documents (
    id VARCHAR(36) PRIMARY KEY,
    case_id VARCHAR(100) NOT NULL,
    name TEXT NOT NULL,
    type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_path TEXT NOT NULL,
    file_url TEXT NOT NULL,
    size BIGINT NOT NULL,
    uploaded_at TIMESTAMP NOT NULL,
    analysis JSONB
);

-- Add indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_documents_case_id ON documents(case_id);
CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(type);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status); 