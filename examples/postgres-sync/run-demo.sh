#!/bin/bash

# Postgres Real-time Sync Demo Runner
# This script sets up and demonstrates real-time sync from Postgres to Antfly

set -e

echo "=== Postgres Real-time Sync Demo ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default Postgres URL
POSTGRES_URL="${POSTGRES_URL:-postgresql://postgres:postgres@localhost:5432/antfly_demo}"
ANTFLY_URL="${ANTFLY_URL:-http://localhost:8080/api/v1}"

# Parse command line arguments
SKIP_POSTGRES_CHECK=false
SKIP_ANTFLY_CHECK=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --skip-postgres-check)
      SKIP_POSTGRES_CHECK=true
      shift
      ;;
    --skip-antfly-check)
      SKIP_ANTFLY_CHECK=true
      shift
      ;;
    --postgres)
      POSTGRES_URL="$2"
      shift 2
      ;;
    --antfly)
      ANTFLY_URL="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--skip-postgres-check] [--skip-antfly-check] [--postgres URL] [--antfly URL]"
      exit 1
      ;;
  esac
done

# Check if Postgres is accessible
if [ "$SKIP_POSTGRES_CHECK" = false ]; then
  echo -ne "${BLUE}Checking Postgres connection...${NC} "
  if psql "$POSTGRES_URL" -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
  else
    echo -e "${RED}✗${NC}"
    echo ""
    echo -e "${YELLOW}Postgres is not accessible at: $POSTGRES_URL${NC}"
    echo ""
    echo "Options:"
    echo "  1. Start Postgres with Docker:"
    echo "     docker run --name postgres-demo -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=antfly_demo -p 5432:5432 -d postgres:16"
    echo ""
    echo "  2. Use existing Postgres and set POSTGRES_URL:"
    echo "     export POSTGRES_URL='postgresql://user:pass@host:5432/database'"
    echo ""
    echo "  3. Skip this check:"
    echo "     $0 --skip-postgres-check"
    echo ""
    exit 1
  fi
fi

# Check if Antfly is running
if [ "$SKIP_ANTFLY_CHECK" = false ]; then
  echo -ne "${BLUE}Checking Antfly connection...${NC} "
  if curl -s "${ANTFLY_URL}/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
  else
    echo -e "${RED}✗${NC}"
    echo ""
    echo -e "${YELLOW}Antfly is not running at: $ANTFLY_URL${NC}"
    echo ""
    echo "Start Antfly first:"
    echo "  cd ../../"
    echo "  go run ./cmd/antfly swarm"
    echo ""
    echo "Or skip this check:"
    echo "  $0 --skip-antfly-check"
    echo ""
    exit 1
  fi
fi

# Build the sync tool
echo -e "${BLUE}Building postgres-sync tool...${NC}"
cd ../..
go build -o examples/postgres-sync/postgres-sync ./examples/postgres-sync
cd examples/postgres-sync

echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Set up Postgres schema
echo -e "${BLUE}Setting up Postgres schema and sample data...${NC}"
psql "$POSTGRES_URL" -f schema.sql > /dev/null 2>&1

echo -e "${GREEN}✓ Schema created${NC}"
echo ""

# Show sample data
echo "Sample data in Postgres:"
psql "$POSTGRES_URL" -c "SELECT id, data->>'title' as title, data->>'category' as category FROM documents ORDER BY id LIMIT 5"
echo ""

# Start the sync daemon in background
echo -e "${BLUE}Starting sync daemon...${NC}"
./postgres-sync \
  --postgres "$POSTGRES_URL" \
  --antfly "$ANTFLY_URL" \
  --pg-table documents \
  --antfly-table demo_postgres_docs \
  --create-table \
  --full-sync-interval 1m &

SYNC_PID=$!

# Wait for sync to start
sleep 3

echo -e "${GREEN}✓ Sync daemon running (PID: $SYNC_PID)${NC}"
echo ""

# Function to cleanup on exit
cleanup() {
  echo ""
  echo -e "${YELLOW}Stopping sync daemon...${NC}"
  kill $SYNC_PID 2>/dev/null || true
  wait $SYNC_PID 2>/dev/null || true
  echo -e "${GREEN}✓ Demo complete${NC}"
}

trap cleanup EXIT INT TERM

echo "=== Interactive Demo ==="
echo ""
echo "The sync daemon is now running. Let's test real-time sync!"
echo ""

# Demo 1: Insert
echo -e "${BLUE}Demo 1: Insert new documents${NC}"
echo "Running: INSERT INTO documents ..."
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
INSERT INTO documents (id, data) VALUES
  ('demo_001', '{"title": "Real-time Demo", "content": "This document syncs instantly!", "category": "demo"}'),
  ('demo_002', '{"title": "Another Demo", "content": "Watch the sync happen!", "category": "demo"}');
EOF

echo -e "${GREEN}✓ Inserted 2 documents${NC}"
echo "Check the sync daemon output above - you should see:"
echo "  ← Change detected: INSERT demo_001"
echo ""
sleep 2

# Demo 2: Update
echo -e "${BLUE}Demo 2: Update existing documents${NC}"
echo "Running: UPDATE documents ..."
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
UPDATE documents
SET data = data || '{"updated": true, "timestamp": "$(date -Iseconds)"}'
WHERE id IN ('doc_001', 'doc_002');
EOF

echo -e "${GREEN}✓ Updated 2 documents${NC}"
echo "You should see UPDATE notifications above"
echo ""
sleep 2

# Demo 3: Bulk insert
echo -e "${BLUE}Demo 3: Bulk insert (tests batching)${NC}"
echo "Running: INSERT 20 documents ..."
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
INSERT INTO documents (id, data)
SELECT
  'bulk_' || LPAD(i::TEXT, 3, '0'),
  jsonb_build_object(
    'title', 'Bulk Document ' || i,
    'content', 'Generated for bulk test',
    'category', 'bulk',
    'index', i
  )
FROM generate_series(1, 20) AS i;
EOF

echo -e "${GREEN}✓ Inserted 20 documents${NC}"
echo "Watch how they get batched together!"
echo ""
sleep 3

# Demo 4: Delete
echo -e "${BLUE}Demo 4: Delete documents${NC}"
echo "Running: DELETE FROM documents ..."
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
DELETE FROM documents WHERE id IN ('demo_001', 'demo_002');
EOF

echo -e "${GREEN}✓ Deleted 2 documents${NC}"
echo "You should see DELETE notifications above"
echo ""
sleep 2

# Demo 5: Transaction
echo -e "${BLUE}Demo 5: Transactional changes${NC}"
echo "Running: BEGIN; INSERT; UPDATE; COMMIT;"
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
BEGIN;
INSERT INTO documents (id, data) VALUES ('tx_001', '{"title": "Transaction Test 1", "in_tx": true}');
INSERT INTO documents (id, data) VALUES ('tx_002', '{"title": "Transaction Test 2", "in_tx": true}');
UPDATE documents SET data = data || '{"modified": true}' WHERE id = 'tx_001';
COMMIT;
EOF

echo -e "${GREEN}✓ Transaction committed${NC}"
echo "All notifications should arrive after COMMIT"
echo ""
sleep 2

# Show final state
echo ""
echo "=== Final State ==="
echo ""

echo "Postgres documents:"
psql "$POSTGRES_URL" -c "SELECT COUNT(*) as total, COUNT(DISTINCT data->>'category') as categories FROM documents"

echo ""
echo "Sample documents in Postgres:"
psql "$POSTGRES_URL" -c "SELECT id, data->>'title' as title, data->>'category' as category FROM documents ORDER BY updated_at DESC LIMIT 5"

echo ""
echo -e "${GREEN}=== Demo Complete! ===${NC}"
echo ""
echo "The sync daemon is still running. You can:"
echo ""
echo "  1. Make more changes and watch them sync:"
echo "     psql \"$POSTGRES_URL\""
echo "     > INSERT INTO documents (id, data) VALUES ('test', '{\"title\": \"Test\"}');"
echo ""
echo "  2. Query Antfly to see synced data:"
echo "     curl ${ANTFLY_URL}/tables/demo_postgres_docs/scan?limit=10"
echo ""
echo "  3. Run the demo SQL script:"
echo "     psql \"$POSTGRES_URL\" -f demo-changes.sql"
echo ""
echo "  4. View sync statistics in the daemon output above"
echo ""
echo "Press Enter to clean up and stop the demo..."
read

echo ""
echo -e "${BLUE}Cleaning up demo data...${NC}"
psql "$POSTGRES_URL" > /dev/null 2>&1 <<EOF
DELETE FROM documents WHERE id LIKE 'demo_%';
DELETE FROM documents WHERE id LIKE 'bulk_%';
DELETE FROM documents WHERE id LIKE 'tx_%';
EOF

echo -e "${GREEN}✓ Demo data cleaned up${NC}"
echo ""
echo "To clean up completely:"
echo "  - Drop Antfly table: curl -X DELETE ${ANTFLY_URL}/tables/demo_postgres_docs"
echo "  - Drop Postgres table: psql \"$POSTGRES_URL\" -c 'DROP TABLE documents;'"
echo ""
