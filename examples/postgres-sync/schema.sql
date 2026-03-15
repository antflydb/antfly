-- Postgres Schema for Antfly Real-time Sync Demo
-- This sets up a table with JSONB data and triggers for LISTEN/NOTIFY

-- Create the documents table
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index on JSONB data for better query performance
CREATE INDEX IF NOT EXISTS idx_documents_data_gin ON documents USING GIN (data);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_documents_updated_at ON documents;
CREATE TRIGGER update_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Notification function for real-time sync
CREATE OR REPLACE FUNCTION notify_document_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
    channel_name TEXT;
BEGIN
    -- Use table name for channel
    channel_name := TG_TABLE_NAME || '_changes';

    -- Build notification payload
    IF (TG_OP = 'DELETE') THEN
        payload = json_build_object(
            'operation', TG_OP,
            'id', OLD.id,
            'timestamp', NOW()
        );
    ELSE
        payload = json_build_object(
            'operation', TG_OP,
            'id', NEW.id,
            'data', NEW.data,
            'timestamp', NOW()
        );
    END IF;

    -- Send notification
    PERFORM pg_notify(channel_name, payload::text);

    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Trigger to send notifications on INSERT/UPDATE/DELETE
DROP TRIGGER IF EXISTS documents_change_trigger ON documents;
CREATE TRIGGER documents_change_trigger
    AFTER INSERT OR UPDATE OR DELETE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION notify_document_change();

-- Insert sample data
INSERT INTO documents (id, data) VALUES
    ('doc_001', '{"title": "Getting Started", "content": "Welcome to Antfly", "category": "tutorial", "tags": ["intro", "basics"]}'),
    ('doc_002', '{"title": "API Reference", "content": "Complete API documentation", "category": "reference", "tags": ["api", "docs"]}'),
    ('doc_003', '{"title": "Architecture Overview", "content": "Learn about Antfly architecture", "category": "guide", "tags": ["architecture", "design"]}'),
    ('doc_004', '{"title": "Installation Guide", "content": "How to install Antfly", "category": "tutorial", "tags": ["install", "setup"]}'),
    ('doc_005', '{"title": "Configuration", "content": "Configure your Antfly instance", "category": "guide", "tags": ["config", "settings"]}')
ON CONFLICT (id) DO NOTHING;

-- Verify setup
SELECT
    COUNT(*) as total_documents,
    MIN(created_at) as oldest,
    MAX(created_at) as newest
FROM documents;

-- Show sample records
SELECT id, data->>'title' as title, data->>'category' as category
FROM documents
ORDER BY id
LIMIT 10;

COMMIT;

-- Instructions for testing the notification system:
--
-- In one terminal, listen for notifications:
--   LISTEN documents_changes;
--
-- In another terminal, make changes:
--   INSERT INTO documents (id, data) VALUES ('test_001', '{"title": "Test", "content": "Testing notifications"}');
--   UPDATE documents SET data = data || '{"updated": true}' WHERE id = 'test_001';
--   DELETE FROM documents WHERE id = 'test_001';
--
-- You should see notifications appear in the first terminal!
