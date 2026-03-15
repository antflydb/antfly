#!/bin/bash
# Run all CI checks locally, continuing even if some fail
# Usage: ./scripts/ci-check.sh
#
# Mirrors checks from:
#   .github/workflows/ci.yml (lint, typecheck, build, test)
#   .github/workflows/playwright.yml (playwright)

set -o pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

FAILED=()
PASSED=()

run_check() {
    local name="$1"
    shift
    echo -e "\n${YELLOW}━━━ Running: $name ━━━${NC}"
    if "$@"; then
        PASSED+=("$name")
        echo -e "${GREEN}✓ $name passed${NC}"
    else
        FAILED+=("$name")
        echo -e "${RED}✗ $name failed${NC}"
    fi
}

cd "$(dirname "$0")/.." || exit 1

echo -e "${YELLOW}Running all CI checks...${NC}"
echo "Working directory: $(pwd)"

# Core CI checks (from ci.yml)
run_check "lint" pnpm lint
run_check "typecheck" pnpm typecheck
run_check "build" pnpm build
run_check "test" pnpm test

# Playwright tests (from playwright.yml) - must run from apps/antfarm directory
# Skip webkit locally as it requires system deps that may not be installed
# CI installs all deps via `playwright install --with-deps`
if [[ -d "apps/antfarm" ]]; then
    run_check "playwright" bash -c "cd apps/antfarm && pnpm exec playwright test --project=chromium --project=firefox --reporter=list"
fi

# Summary
echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}           CI CHECK SUMMARY${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [[ ${#PASSED[@]} -gt 0 ]]; then
    echo -e "\n${GREEN}Passed (${#PASSED[@]}):${NC}"
    for check in "${PASSED[@]}"; do
        echo -e "  ${GREEN}✓${NC} $check"
    done
fi

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo -e "\n${RED}Failed (${#FAILED[@]}):${NC}"
    for check in "${FAILED[@]}"; do
        echo -e "  ${RED}✗${NC} $check"
    done
    echo -e "\n${RED}CI would fail - ${#FAILED[@]} check(s) need attention${NC}"
    exit 1
else
    echo -e "\n${GREEN}All CI checks passed!${NC}"
    exit 0
fi
