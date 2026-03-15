#!/bin/bash

# docsaf Demo Runner
# This script demonstrates the docsaf tool with different workflows

set -e

echo "=== docsaf Demo - Documentation Sync to Antfly ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if antfly is running
if ! curl -s http://localhost:8080/api/v1/health > /dev/null 2>&1; then
    echo -e "${YELLOW}Warning: Antfly doesn't appear to be running${NC}"
    echo "Please start Antfly first:"
    echo "  cd ../../"
    echo "  go run ./cmd/antfly swarm"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Build the example
echo -e "${GREEN}Building docsaf...${NC}"
cd ../..
go build -o examples/docsaf/docsaf ./examples/docsaf
cd examples/docsaf

echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Demo 1: Two-step workflow (prepare + load)
echo -e "${BLUE}=== Demo 1: Two-Step Workflow (Prepare + Load) ===${NC}"
echo ""

echo -e "${GREEN}Step 1: Prepare data from current directory${NC}"
./docsaf prepare \
    --dir . \
    --output /tmp/docsaf-demo.json

echo ""
echo -e "${GREEN}Press Enter to load the prepared data...${NC}"
read

echo -e "${GREEN}Step 2: Load prepared data into Antfly${NC}"
./docsaf load \
    --input /tmp/docsaf-demo.json \
    --table docsaf_demo \
    --create-table

echo ""
echo -e "${GREEN}Press Enter to continue to next demo...${NC}"
read

# Demo 2: One-step sync workflow
echo -e "${BLUE}=== Demo 2: One-Step Sync Workflow ===${NC}"
echo "Syncing Antfly's src/metadata directory..."
./docsaf sync \
    --dir ../../src/metadata \
    --table metadata_docs \
    --create-table

echo ""
echo -e "${GREEN}Press Enter to continue to next demo...${NC}"
read

# Demo 3: Re-sync to show skipping
echo -e "${BLUE}=== Demo 3: Re-sync (Demonstrating Skip Behavior) ===${NC}"
echo "Running sync again on the same directory (should skip all)..."
./docsaf sync \
    --dir . \
    --table docsaf_demo

echo ""
echo -e "${GREEN}Press Enter to continue to next demo...${NC}"
read

# Demo 4: Dry run
echo -e "${BLUE}=== Demo 4: Dry Run Preview ===${NC}"
echo "Preview loading from CLAUDE.md into existing table..."
./docsaf sync \
    --dir ../../ \
    --table docsaf_demo \
    --dry-run

echo ""
echo -e "${GREEN}=== Demo Complete! ===${NC}"
echo ""
echo "You can now:"
echo "  1. Query the documents using Antfly CLI or API"
echo "  2. Run your own imports with custom documentation"
echo "  3. Check the tables in Antfly UI at http://localhost:8080"
echo ""
echo "Example workflows:"
echo "  # Prepare documentation data"
echo "  ./docsaf prepare --dir /path/to/docs --output docs.json"
echo ""
echo "  # Load prepared data"
echo "  ./docsaf load --input docs.json --table my_docs --create-table"
echo ""
echo "  # Full pipeline"
echo "  ./docsaf sync --dir /path/to/docs --table my_docs --create-table"
echo ""
echo "  # Sync www/ website docs with frontmatter parsing and wildcard filtering"
echo "  ./docsaf sync --dir ../../www --include '**/content/**' --table website_docs --create-table"
echo ""
echo "  # Or with exclusions instead"
echo "  ./docsaf sync --dir ../../www --exclude '**/node_modules/**' --exclude '**/.next/**' --exclude '**/out/**' --exclude '**/work-log/**' --exclude '**/scripts/**' --exclude '**/config/**' --exclude '**/components/**' --exclude '**/app/**' --exclude '**/public/**' --table website_docs --create-table"
echo ""
echo "Clean up tables:"
echo "  antfly table drop --table docsaf_demo"
echo "  antfly table drop --table metadata_docs"
