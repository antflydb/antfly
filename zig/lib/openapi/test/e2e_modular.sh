#!/bin/bash
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

# E2E test for modular code generation with import mappings.
#
# Generates three modules from the modular test fixtures:
#   - schema: types-only library module (AntflyType, FieldSchema, TableSchema)
#   - embeddings: types-only library module (EmbedderProvider, EmbedderConfig, ...)
#   - api: types+server module that imports from schema and embeddings
#
# Verifies that:
#   - External $refs generate qualified type names (schema.TableSchema, embeddings.EmbedderConfig)
#   - Import declarations are emitted in generated files
#   - Library modules generate standalone types with no external imports
#   - Server handlers use the correct qualified types for request body parsing

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ZIG="${ZIG:-$HOME/bin/zig}"
GEN_DIR=$(mktemp -d)
FIXTURE_DIR="$PROJECT_DIR/test/fixtures/modular"

cleanup() { rm -rf "$GEN_DIR"; }
trap cleanup EXIT

echo "=== E2E: Modular code generation ==="
echo "Output: $GEN_DIR"

# Step 1: Generate library modules (types only)
echo ""
echo "--- Generating schema module ---"
"$ZIG" build run -- \
  --spec "$FIXTURE_DIR/schema.json" \
  --output "$GEN_DIR/schema" \
  --package schema \
  --generate types

echo ""
echo "--- Generating embeddings module ---"
"$ZIG" build run -- \
  --spec "$FIXTURE_DIR/embeddings.json" \
  --output "$GEN_DIR/embeddings" \
  --package embeddings \
  --generate types

# Step 2: Generate API module with import mappings via config file
echo ""
echo "--- Generating API module (with import mappings) ---"
"$ZIG" build run -- \
  --spec "$FIXTURE_DIR/api.json" \
  --output "$GEN_DIR/api" \
  --package api \
  --generate types,server \
  --import-mapping "schema.json=schema" \
  --import-mapping "embeddings.json=embeddings"

# Step 3: Verify schema module
echo ""
echo "--- Verifying schema module ---"
assert_contains() {
  local file="$1" pattern="$2" desc="$3"
  if grep -qF "$pattern" "$file"; then
    echo "  OK: $desc"
  else
    echo "  FAIL: $desc"
    echo "    Expected pattern: $pattern"
    echo "    In file: $file"
    exit 1
  fi
}

assert_not_contains() {
  local file="$1" pattern="$2" desc="$3"
  if grep -qF "$pattern" "$file"; then
    echo "  FAIL: $desc"
    echo "    Unexpected pattern: $pattern"
    echo "    In file: $file"
    exit 1
  else
    echo "  OK: $desc"
  fi
}

# Schema module: standalone types, no external imports
assert_contains "$GEN_DIR/schema/types.zig" "pub const AntflyType = enum {" "schema has AntflyType enum"
assert_contains "$GEN_DIR/schema/types.zig" "pub const FieldSchema = struct {" "schema has FieldSchema struct"
assert_contains "$GEN_DIR/schema/types.zig" "pub const TableSchema = struct {" "schema has TableSchema struct"
assert_not_contains "$GEN_DIR/schema/types.zig" "@import(\"schema\")" "schema has no external schema import"
assert_not_contains "$GEN_DIR/schema/types.zig" "@import(\"embeddings\")" "schema has no external embeddings import"

# Embeddings module: standalone types
assert_contains "$GEN_DIR/embeddings/types.zig" "pub const EmbedderProvider = enum {" "embeddings has EmbedderProvider enum"
assert_contains "$GEN_DIR/embeddings/types.zig" "pub const EmbedderConfig = struct {" "embeddings has EmbedderConfig struct"
assert_not_contains "$GEN_DIR/embeddings/types.zig" "@import(\"schema\")" "embeddings has no external import"

# API module: types with external imports
echo ""
echo "--- Verifying API module types ---"
assert_contains "$GEN_DIR/api/types.zig" "@import(\"schema\")" "api types imports schema module"
assert_contains "$GEN_DIR/api/types.zig" "@import(\"embeddings\")" "api types imports embeddings module"
assert_contains "$GEN_DIR/api/types.zig" "schema: schema.TableSchema," "api TableInfo uses schema.TableSchema"
assert_contains "$GEN_DIR/api/types.zig" "embedder: ?embeddings.EmbedderConfig" "api TableInfo uses embeddings.EmbedderConfig"
assert_not_contains "$GEN_DIR/api/types.zig" "pub const TableSchema" "api does not regenerate TableSchema"
assert_not_contains "$GEN_DIR/api/types.zig" "pub const EmbedderConfig" "api does not regenerate EmbedderConfig"

# API module: server with correct body types
echo ""
echo "--- Verifying API module server ---"
assert_contains "$GEN_DIR/api/server.zig" "@import(\"schema\")" "api server imports schema module"
assert_contains "$GEN_DIR/api/server.zig" "@import(\"embeddings\")" "api server imports embeddings module"
assert_contains "$GEN_DIR/api/server.zig" "schema.TableSchema" "server uses schema.TableSchema for body parsing"
assert_contains "$GEN_DIR/api/server.zig" "embeddings.EmbedderConfig" "server uses embeddings.EmbedderConfig for body parsing"
assert_contains "$GEN_DIR/api/server.zig" "body: schema.TableSchema" "handler interface shows schema.TableSchema"
assert_contains "$GEN_DIR/api/server.zig" "body: embeddings.EmbedderConfig" "handler interface shows embeddings.EmbedderConfig"

# Step 4: Test config file path
echo ""
echo "--- Testing config file ---"
rm -rf /tmp/modular-gen
"$ZIG" build run -- --config "$FIXTURE_DIR/api-config.json"
assert_contains "/tmp/modular-gen/api/types.zig" "schema.TableSchema" "config file produces same result"
rm -rf /tmp/modular-gen

# Step 5: Test OpenAPI 3.1 spec
echo ""
echo "--- Testing OpenAPI 3.1 (petstore31.json) ---"
"$ZIG" build run -- \
  --spec "$PROJECT_DIR/test/fixtures/petstore31.json" \
  --output "$GEN_DIR/petstore31" \
  --package petstore \
  --generate types,server

assert_contains "$GEN_DIR/petstore31/types.zig" "pub const PetStatus = enum {" "3.1 enum generated"
assert_contains "$GEN_DIR/petstore31/types.zig" "tag: ?[]const u8 = null," "3.1 type array nullable optional"
assert_contains "$GEN_DIR/petstore31/types.zig" "details: ?std.json.Value," "3.1 required+nullable (no default)"
assert_not_contains "$GEN_DIR/petstore31/types.zig" "details: ?std.json.Value = null" "3.1 required+nullable has no = null"
assert_contains "$GEN_DIR/petstore31/types.zig" "/// Initial status (defaults to available)" "3.1 \$ref description sibling"
assert_contains "$GEN_DIR/petstore31/types.zig" "/// Required but nullable error details" "3.1 nullable field description"
assert_contains "$GEN_DIR/petstore31/server.zig" "ServerRouter" "3.1 server generated"

echo ""
echo "=== All E2E tests passed ==="
