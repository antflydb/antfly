#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${ALGEBRAIC_MATRIX_OUT:-bench/results/algebraic-integration-matrix/$STAMP}"

PUBLIC_DOCS="${ALGEBRAIC_MATRIX_PUBLIC_DOCS:-200}"
PUBLIC_DIMS="${ALGEBRAIC_MATRIX_PUBLIC_DIMS:-32}"
PUBLIC_QUERIES="${ALGEBRAIC_MATRIX_PUBLIC_QUERIES:-1}"
PUBLIC_REPEATS="${ALGEBRAIC_MATRIX_PUBLIC_REPEATS:-1}"
PUBLIC_K="${ALGEBRAIC_MATRIX_PUBLIC_K:-5}"
PUBLIC_THREADS="${ALGEBRAIC_MATRIX_PUBLIC_THREADS:-2}"
PUBLIC_MODE="${ALGEBRAIC_MATRIX_PUBLIC_MODE:-handler}"
RUN_UNIT_TEST="${ALGEBRAIC_MATRIX_RUN_UNIT_TEST:-0}"
RUN_E2E="${ALGEBRAIC_MATRIX_RUN_E2E:-0}"
E2E_SELECTOR="${ALGEBRAIC_MATRIX_E2E_SELECTOR:-test_query_string.py test_schema_migration.py test_index_lifecycle.py}"
WARM_BUILDS="${ALGEBRAIC_MATRIX_WARM_BUILDS:-1}"

mkdir -p "$OUT"

{
  echo "timestamp_utc=$STAMP"
  echo "root=$ROOT"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "git_status_porcelain_begin"
  git status --short || true
  echo "git_status_porcelain_end"
  echo "uname=$(uname -a)"
  echo "public_docs=$PUBLIC_DOCS"
  echo "public_dims=$PUBLIC_DIMS"
  echo "public_queries=$PUBLIC_QUERIES"
  echo "public_repeats=$PUBLIC_REPEATS"
  echo "public_k=$PUBLIC_K"
  echo "public_threads=$PUBLIC_THREADS"
  echo "public_mode=$PUBLIC_MODE"
  echo "run_unit_test=$RUN_UNIT_TEST"
  echo "run_e2e=$RUN_E2E"
  echo "e2e_selector=$E2E_SELECTOR"
  echo "warm_builds=$WARM_BUILDS"
} > "$OUT/environment.txt"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
WARM_FILE="$OUT/warm-builds.txt"
: > "$STATUS_FILE"
: > "$COMMAND_FILE"
: > "$WARM_FILE"

warm_case() {
  local name="$1"
  shift
  echo "warming $name"
  printf "%s\t" "$name" >> "$WARM_FILE"
  printf "%q " "$@" >> "$WARM_FILE"
  printf "\n" >> "$WARM_FILE"
  "$@"
}

run_case() {
  local name="$1"
  shift
  echo "running $name"
  printf "%s\t" "$name" >> "$COMMAND_FILE"
  printf "%q " "$@" >> "$COMMAND_FILE"
  printf "\n" >> "$COMMAND_FILE"
  local started ended rc
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$@" > "$OUT/$name.stdout" 2> "$OUT/$name.stderr"
  rc=$?
  set -e
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf "%s\t%s\t%s\t%s\n" "$name" "$rc" "$started" "$ended" >> "$STATUS_FILE"
  if [[ "$rc" != "0" ]]; then
    echo "failed $name rc=$rc"
    return "$rc"
  fi
}

public_query_args=(
  --mode "$PUBLIC_MODE"
  --query-shape hybrid-filter-exclude-project
  --docs "$PUBLIC_DOCS"
  --dims "$PUBLIC_DIMS"
  --queries "$PUBLIC_QUERIES"
  --repeats "$PUBLIC_REPEATS"
  --k "$PUBLIC_K"
  --search-threads "$PUBLIC_THREADS"
)

if [[ "$WARM_BUILDS" == "1" ]]; then
  warm_case roadmap_guardrail zig build algebraic-roadmap-guardrail
  warm_case public_query_guardrail zig build public-query-guardrail-build
  warm_case lib_db_algebraic zig build lib-db-test -- --test-filter algebraic
  warm_case provisioned_distributed_non_algebraic_name zig build lib-db-test -- --test-filter "provisioned distributed aggregations collect path terms nested cardinality"
fi

run_case roadmap_guardrail zig build algebraic-roadmap-guardrail
run_case public_query_default_no_schema zig build public-query-guardrail -- "${public_query_args[@]}"
run_case public_query_schema_only zig build public-query-guardrail -- "${public_query_args[@]}" --with-schema
run_case public_query_schema_algebraic zig build public-query-guardrail -- "${public_query_args[@]}" --with-schema --with-algebraic
run_case lib_db_algebraic zig build lib-db-test -- --test-filter algebraic
run_case provisioned_distributed_non_algebraic_name zig build lib-db-test -- --test-filter "provisioned distributed aggregations collect path terms nested cardinality"

if [[ "$RUN_UNIT_TEST" == "1" ]]; then
  run_case unit_test zig build unit-test
fi

if [[ "$RUN_E2E" == "1" ]]; then
  e2e_args=(e2e/antfly/.venv/bin/pytest -q -x -s)
  for item in $E2E_SELECTOR; do
    e2e_args+=("e2e/antfly/$item")
  done
  run_case e2e_selected env ANTFLY_E2E_PRESERVE_ROOT=1 "${e2e_args[@]}"
fi

echo "wrote $OUT"
