/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestDocumentEngineGuideExamplesRoundTrip(t *testing.T) {
	path := filepath.Join("..", "..", "..", "docs", "guides", "document-engine.mdx")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read document engine guide: %v", err)
	}

	blocks := extractJSONFenceBlocks(t, content)
	if got, want := len(blocks), 3; got != want {
		t.Fatalf("json example block count = %d, want %d", got, want)
	}

	var sawSchema bool
	var sawSortedQuery bool
	var sawCursorQuery bool
	for i, block := range blocks {
		if bytes.Contains(block, []byte("doc_values")) {
			t.Fatalf("json block %d exposes internal doc_values knob: %s", i, block)
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(block, &root); err != nil {
			t.Fatalf("json block %d is invalid JSON: %v\n%s", i, err, block)
		}

		switch {
		case root["document_schemas"] != nil:
			if sawSchema {
				t.Fatalf("multiple schema examples found")
			}
			sawSchema = true
			assertDocumentEngineSchemaExample(t, block)
		case root["order_by"] != nil:
			hasCursor := root["search_after"] != nil || root["search_before"] != nil
			if hasCursor {
				if sawCursorQuery {
					t.Fatalf("multiple cursor query examples found")
				}
				sawCursorQuery = true
			} else {
				if sawSortedQuery {
					t.Fatalf("multiple sorted query examples found")
				}
				sawSortedQuery = true
			}
			assertDocumentEngineQueryExample(t, block, hasCursor)
		default:
			t.Fatalf("json block %d is not a recognized document-engine example: %s", i, block)
		}
	}
	if !sawSchema || !sawSortedQuery || !sawCursorQuery {
		t.Fatalf("expected schema, sorted query, and cursor query examples; saw schema=%t sorted=%t cursor=%t", sawSchema, sawSortedQuery, sawCursorQuery)
	}
}

func extractJSONFenceBlocks(t *testing.T, content []byte) [][]byte {
	t.Helper()

	var blocks [][]byte
	var current bytes.Buffer
	inJSON := false
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.TrimSpace(line) == "```json" && !inJSON:
			inJSON = true
			current.Reset()
		case strings.HasPrefix(strings.TrimSpace(line), "```") && inJSON:
			inJSON = false
			blocks = append(blocks, bytes.Clone(bytes.TrimSpace(current.Bytes())))
		case inJSON:
			current.WriteString(line)
			current.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan markdown: %v", err)
	}
	if inJSON {
		t.Fatalf("unterminated json code block")
	}
	return blocks
}

func assertDocumentEngineSchemaExample(t *testing.T, block []byte) {
	t.Helper()

	var schema oapi.TableSchema
	if err := json.Unmarshal(block, &schema); err != nil {
		t.Fatalf("schema example does not unmarshal as TableSchema: %v", err)
	}
	if schema.DocumentSchemas == nil {
		t.Fatalf("schema example has no document_schemas")
	}
	if _, ok := schema.DocumentSchemas["doc"]; !ok {
		t.Fatalf("schema example missing doc document schema")
	}

	var raw map[string]any
	if err := json.Unmarshal(block, &raw); err != nil {
		t.Fatalf("schema example raw decode failed: %v", err)
	}
	documentSchemas := requiredMap(t, raw["document_schemas"], "document_schemas")
	doc := requiredMap(t, documentSchemas["doc"], "document_schemas.doc")
	properties := requiredMap(t, requiredMap(t, doc["schema"], "document_schemas.doc.schema")["properties"], "properties")

	titleMapping := requiredMap(t, requiredMap(t, properties["title"], "title")["x-antfly-field"], "title.x-antfly-field")
	if got, want := requiredString(t, titleMapping, "type"), "text"; got != want {
		t.Fatalf("title mapping type = %q, want %q", got, want)
	}
	keywordMapping := requiredMap(t, requiredMap(t, titleMapping["fields"], "title.fields")["keyword"], "title.keyword")
	if got, want := requiredString(t, keywordMapping, "type"), "keyword"; got != want {
		t.Fatalf("title.keyword mapping type = %q, want %q", got, want)
	}
	if got := requiredBool(t, keywordMapping, "sortable"); !got {
		t.Fatalf("title.keyword sortable = false, want true")
	}

	assertSortableField(t, properties, "created_at", "date")
	assertSortableField(t, properties, "price", "number")
	assertSortableField(t, properties, "published", "boolean")
}

func assertDocumentEngineQueryExample(t *testing.T, block []byte, expectCursor bool) {
	t.Helper()

	var req oapi.QueryRequest
	if err := json.Unmarshal(block, &req); err != nil {
		t.Fatalf("query example does not unmarshal as QueryRequest: %v", err)
	}
	if req.Table != "articles" {
		t.Fatalf("query table = %q, want articles", req.Table)
	}
	if len(req.OrderBy) != 1 {
		t.Fatalf("query order_by len = %d, want 1", len(req.OrderBy))
	}
	if got, want := req.OrderBy[0].Field, "created_at"; got != want {
		t.Fatalf("query order_by field = %q, want %q", got, want)
	}
	if !req.OrderBy[0].Desc {
		t.Fatalf("query order_by desc = false, want true")
	}
	if len(req.FilterQuery) == 0 {
		t.Fatalf("query example missing filter_query")
	}
	var filter oapi.Query
	if err := json.Unmarshal(req.FilterQuery, &filter); err != nil {
		t.Fatalf("filter_query does not unmarshal as Query: %v", err)
	}
	if len(req.FullTextSearch) > 0 {
		var fullText oapi.Query
		if err := json.Unmarshal(req.FullTextSearch, &fullText); err != nil {
			t.Fatalf("full_text_search does not unmarshal as Query: %v", err)
		}
	}
	if expectCursor {
		if got, want := len(req.SearchAfter), 2; got != want {
			t.Fatalf("search_after len = %d, want %d", got, want)
		}
	} else if len(req.SearchAfter) != 0 || len(req.SearchBefore) != 0 {
		t.Fatalf("non-cursor query should not include cursor fields")
	}
}

func assertSortableField(t *testing.T, properties map[string]any, field string, wantType string) {
	t.Helper()

	mapping := requiredMap(t, requiredMap(t, properties[field], field)["x-antfly-field"], field+".x-antfly-field")
	if got := requiredString(t, mapping, "type"); got != wantType {
		t.Fatalf("%s mapping type = %q, want %q", field, got, wantType)
	}
	if got := requiredBool(t, mapping, "sortable"); !got {
		t.Fatalf("%s sortable = false, want true", field)
	}
}

func requiredMap(t *testing.T, value any, name string) map[string]any {
	t.Helper()

	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is %T, want object", name, value)
	}
	return m
}

func requiredString(t *testing.T, m map[string]any, key string) string {
	t.Helper()

	value, ok := m[key].(string)
	if !ok {
		t.Fatalf("%s is %T, want string", key, m[key])
	}
	return value
}

func requiredBool(t *testing.T, m map[string]any, key string) bool {
	t.Helper()

	value, ok := m[key].(bool)
	if !ok {
		t.Fatalf("%s is %T, want bool", key, m[key])
	}
	return value
}
