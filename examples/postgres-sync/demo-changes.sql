-- Demo SQL Script: Test Real-time Sync
-- Run these queries while the postgres-sync tool is running to see real-time updates

-- 1. Insert new documents
INSERT INTO documents (id, data) VALUES
    ('demo_001', '{"title": "Real-time Demo", "content": "This document was added in real-time!", "category": "demo", "priority": 1}'),
    ('demo_002', '{"title": "Another Demo", "content": "Watch this sync to Antfly instantly!", "category": "demo", "priority": 2}');

-- Wait a moment, then check the sync tool output
-- You should see: "← Change detected: INSERT demo_001"

-- 2. Update existing documents
UPDATE documents
SET data = data || '{"updated": true, "last_modified": "2024-01-15T10:00:00Z"}'
WHERE id IN ('doc_001', 'doc_002');

-- You should see: "← Change detected: UPDATE doc_001"

-- 3. Update with JSONB manipulation
UPDATE documents
SET data = jsonb_set(data, '{priority}', '10')
WHERE id = 'demo_001';

UPDATE documents
SET data = data || jsonb_build_object('tags', jsonb_build_array('demo', 'realtime', 'sync'))
WHERE id = 'demo_002';

-- 4. Bulk insert (tests batching)
INSERT INTO documents (id, data)
SELECT
    'bulk_' || LPAD(i::TEXT, 3, '0'),
    jsonb_build_object(
        'title', 'Bulk Document ' || i,
        'content', 'Generated document for bulk sync test',
        'category', 'bulk',
        'index', i,
        'generated_at', NOW()
    )
FROM generate_series(1, 20) AS i;

-- Watch the sync tool batch these together!

-- 5. Update multiple records at once
UPDATE documents
SET data = data || '{"batch_updated": true}'
WHERE id LIKE 'bulk_%';

-- 6. Delete some documents
DELETE FROM documents WHERE id IN ('demo_001', 'demo_002');

-- You should see: "← Change detected: DELETE demo_001"

-- 7. Conditional updates
UPDATE documents
SET data = data || jsonb_build_object('has_tags', CASE WHEN data ? 'tags' THEN true ELSE false END)
WHERE data->>'category' = 'tutorial';

-- 8. Test with transaction (all changes notify together)
BEGIN;
    INSERT INTO documents (id, data) VALUES ('tx_001', '{"title": "Transaction Test 1", "in_tx": true}');
    INSERT INTO documents (id, data) VALUES ('tx_002', '{"title": "Transaction Test 2", "in_tx": true}');
    UPDATE documents SET data = data || '{"modified_in_tx": true}' WHERE id = 'tx_001';
COMMIT;

-- All three notifications should arrive after COMMIT

-- 9. Clean up demo data
DELETE FROM documents WHERE id LIKE 'bulk_%';
DELETE FROM documents WHERE id LIKE 'tx_%';

-- 10. View current state
SELECT
    id,
    data->>'title' as title,
    data->>'category' as category,
    updated_at
FROM documents
ORDER BY updated_at DESC
LIMIT 10;

-- 11. Check for documents that should be in Antfly
SELECT
    COUNT(*) as total_docs,
    COUNT(DISTINCT data->>'category') as categories,
    jsonb_object_agg(
        data->>'category',
        COUNT(*)
    ) as docs_per_category
FROM documents
WHERE data->>'category' IS NOT NULL
GROUP BY true;

-- Advanced: Create a function to generate random updates
CREATE OR REPLACE FUNCTION random_update_demo(num_updates INT DEFAULT 10)
RETURNS void AS $$
DECLARE
    doc_id TEXT;
    i INT;
BEGIN
    FOR i IN 1..num_updates LOOP
        -- Pick a random document
        SELECT id INTO doc_id
        FROM documents
        ORDER BY RANDOM()
        LIMIT 1;

        -- Update it with random data
        UPDATE documents
        SET data = data || jsonb_build_object(
            'random_update_' || i, NOW()::TEXT,
            'iteration', i
        )
        WHERE id = doc_id;

        -- Small delay
        PERFORM pg_sleep(0.5);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Usage: SELECT random_update_demo(5);
-- This will make 5 random updates with 0.5s delay between each
