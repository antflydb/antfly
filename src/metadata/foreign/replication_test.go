// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package foreign

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// helpers to construct pglogrepl types that have unexported fields.

func makeRelationMessageV2(relID uint32, cols []*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessageV2 {
	rel := &pglogrepl.RelationMessageV2{}
	rel.RelationID = relID
	rel.Columns = cols
	rel.ColumnNum = uint16(len(cols))
	return rel
}

func makeTupleData(cols []*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	td := &pglogrepl.TupleData{}
	td.ColumnNum = uint16(len(cols))
	td.Columns = cols
	return td
}

// --- SlotName / PublicationName ---

func TestSlotName(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		pgTable   string
		want      string
	}{
		{"basic", "users", "pg_users", "antfly_users_pg_users"},
		{"special chars", "my-table", "pg.table", "antfly_my_table_pg_table"},
		{"long names truncated", "a_very_long_table_name_that_exceeds_limits", "another_very_long_postgres_table_name", "antfly_a_very_long_table_name_that_exceeds_limits_another_very_"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SlotName(tt.tableName, tt.pgTable)
			if got != tt.want {
				t.Errorf("SlotName(%q, %q) = %q, want %q", tt.tableName, tt.pgTable, got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("SlotName result too long: %d chars", len(got))
			}
		})
	}
}

func TestPublicationName(t *testing.T) {
	got := PublicationName("users", "pg_users")
	want := "antfly_pub_users_pg_users"
	if got != want {
		t.Errorf("PublicationName = %q, want %q", got, want)
	}
}

func TestSanitizePGIdentifier(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		maxLen int
		want   string
	}{
		{"basic", "hello_world", 63, "hello_world"},
		{"special chars", "hello-world.foo", 63, "hello_world_foo"},
		{"truncate", "abcdefghij", 5, "abcde"},
		{"unicode letters preserved", "café_table", 63, "café_table"},
		{"uppercase lowered", "MyTable", 63, "mytable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizePGIdentifier(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("sanitizePGIdentifier(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
		})
	}
}

// --- extractKey ---

func TestExtractKey(t *testing.T) {
	row := map[string]any{
		"id":        "abc123",
		"tenant_id": "t1",
		"user_id":   42,
		"null_col":  nil,
	}

	tests := []struct {
		name     string
		template string
		want     string
		wantErr  bool
	}{
		{"plain column", "id", "abc123", false},
		{"template composite", "{{tenant_id}}:{{user_id}}", "t1:42", false},
		{"missing column", "nonexistent", "", true},
		{"missing template column", "{{missing}}", "", true},
		{"empty template", "", "", true},
		{"nil value column", "null_col", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractKey(row, tt.template)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- tupleToMap ---

func TestTupleToMap(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := makeRelationMessageV2(1, []*pglogrepl.RelationMessageColumn{
		{Name: "id", DataType: pgtype.TextOID},
		{Name: "name", DataType: pgtype.TextOID},
		{Name: "null_col", DataType: pgtype.TextOID},
		{Name: "toast_col", DataType: pgtype.TextOID},
	})

	tuple := makeTupleData([]*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("abc")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("Alice")},
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeToast},
	})

	row, err := tupleToMap(rel, tuple, typeMap)
	if err != nil {
		t.Fatalf("tupleToMap() error = %v", err)
	}

	// Check text value
	if v, ok := row["id"]; !ok || v != "abc" {
		t.Errorf("row[id] = %v, want %q", v, "abc")
	}

	// Check name
	if v, ok := row["name"]; !ok || v != "Alice" {
		t.Errorf("row[name] = %v, want %q", v, "Alice")
	}

	// Check null value is present with nil
	if v, exists := row["null_col"]; !exists {
		t.Error("row[null_col] should exist")
	} else if v != nil {
		t.Errorf("row[null_col] = %v, want nil", v)
	}

	// Check TOAST column is skipped
	if _, ok := row["toast_col"]; ok {
		t.Error("row[toast_col] should not be present (TOAST unchanged)")
	}
}

func TestTupleToMap_NilTuple(t *testing.T) {
	typeMap := pgtype.NewMap()
	rel := makeRelationMessageV2(0, nil)
	_, err := tupleToMap(rel, nil, typeMap)
	if err == nil {
		t.Error("expected error for nil tuple")
	}
}

// --- resolveTransforms ---

func TestResolveTransforms(t *testing.T) {
	row := map[string]any{
		"email":    "alice@example.com",
		"score":    42,
		"active":   true,
		"metadata": map[string]any{"color": "red", "size": "lg"},
	}

	t.Run("column reference", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$set", Path: "email", Value: "{{email}}"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("expected 1 op, got %d", len(resolved))
		}
		if resolved[0].Value != "alice@example.com" {
			t.Errorf("got value %v, want %q", resolved[0].Value, "alice@example.com")
		}
	})

	t.Run("JSONB navigation", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$set", Path: "color", Value: "{{metadata.color}}"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("expected 1 op, got %d", len(resolved))
		}
		if resolved[0].Value != "red" {
			t.Errorf("got value %v, want %q", resolved[0].Value, "red")
		}
	})

	t.Run("literal value", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$set", Path: "source", Value: "postgres"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if resolved[0].Value != "postgres" {
			t.Errorf("got value %v, want %q", resolved[0].Value, "postgres")
		}
	})

	t.Run("non-string literal", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$set", Path: "count", Value: float64(99)},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if resolved[0].Value != float64(99) {
			t.Errorf("got value %v, want %v", resolved[0].Value, float64(99))
		}
	})

	t.Run("$merge expansion", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$merge", Value: "{{metadata}}"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 2 {
			t.Fatalf("expected 2 ops from $merge, got %d", len(resolved))
		}
		// Check that both keys from metadata are present (order non-deterministic)
		foundColor, foundSize := false, false
		for _, op := range resolved {
			if op.Op != "$set" {
				t.Errorf("expected $set op, got %q", op.Op)
			}
			if op.Path == "color" && op.Value == "red" {
				foundColor = true
			}
			if op.Path == "size" && op.Value == "lg" {
				foundSize = true
			}
		}
		if !foundColor || !foundSize {
			t.Errorf("$merge missing expected keys: color=%v, size=%v", foundColor, foundSize)
		}
	})

	t.Run("$unset", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$unset", Path: "email"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 1 || resolved[0].Op != "$unset" || resolved[0].Path != "email" {
			t.Errorf("unexpected $unset result: %+v", resolved)
		}
	})

	t.Run("$delete_document", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$delete_document"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 1 || resolved[0].Op != "$delete_document" {
			t.Errorf("unexpected result: %+v", resolved)
		}
	})

	t.Run("$currentDate", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$currentDate", Path: "updated_at"},
		}
		resolved, err := resolveTransforms(ops, row)
		if err != nil {
			t.Fatalf("resolveTransforms() error = %v", err)
		}
		if len(resolved) != 1 || resolved[0].Op != "$currentDate" || resolved[0].Path != "updated_at" {
			t.Errorf("unexpected $currentDate result: %+v", resolved)
		}
	})

	t.Run("$merge non-map error", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$merge", Value: "{{email}}"},
		}
		_, err := resolveTransforms(ops, row)
		if err == nil {
			t.Error("expected error for $merge on non-map value")
		}
	})

	t.Run("missing column reference error", func(t *testing.T) {
		ops := []store.ReplicationTransformOp{
			{Op: "$set", Path: "foo", Value: "{{nonexistent}}"},
		}
		_, err := resolveTransforms(ops, row)
		if err == nil {
			t.Error("expected error for missing column reference")
		}
	})
}

// --- autoOnUpdate / autoOnDelete ---

func TestAutoOnUpdate(t *testing.T) {
	rel := makeRelationMessageV2(1, []*pglogrepl.RelationMessageColumn{
		{Name: "id"},
		{Name: "name"},
		{Name: "missing"},
	})
	row := map[string]any{
		"id":   "1",
		"name": "Alice",
		// "missing" is not in row (e.g., TOAST column)
	}

	ops := autoOnUpdate(rel, row)
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}
	for _, op := range ops {
		if op.Op != "$set" {
			t.Errorf("expected $set, got %q", op.Op)
		}
	}
}

func TestAutoOnDelete(t *testing.T) {
	rel := makeRelationMessageV2(1, []*pglogrepl.RelationMessageColumn{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	})

	t.Run("with on_update derives unset from set paths", func(t *testing.T) {
		onUpdate := []store.ReplicationTransformOp{
			{Op: "$set", Path: "name", Value: "{{name}}"},
			{Op: "$set", Path: "email", Value: "{{email}}"},
			{Op: "$merge", Value: "{{metadata}}"}, // non-$set, should be skipped
		}
		ops := autoOnDelete(onUpdate, rel)
		if len(ops) != 2 {
			t.Fatalf("expected 2 $unset ops, got %d", len(ops))
		}
		for _, op := range ops {
			if op.Op != "$unset" {
				t.Errorf("expected $unset, got %q", op.Op)
			}
		}
		if ops[0].Path != "name" || ops[1].Path != "email" {
			t.Errorf("unexpected paths: %q, %q", ops[0].Path, ops[1].Path)
		}
	})

	t.Run("without on_update unsets all columns", func(t *testing.T) {
		ops := autoOnDelete(nil, rel)
		if len(ops) != 3 {
			t.Fatalf("expected 3 $unset ops (all columns), got %d", len(ops))
		}
		for _, op := range ops {
			if op.Op != "$unset" {
				t.Errorf("expected $unset, got %q", op.Op)
			}
		}
	})
}

// --- parseOpType ---

func TestParseOpType(t *testing.T) {
	tests := []struct {
		op      string
		want    db.TransformOp_OpType
		wantErr bool
	}{
		{"$set", db.TransformOp_SET, false},
		{"$unset", db.TransformOp_UNSET, false},
		{"$inc", db.TransformOp_INC, false},
		{"$push", db.TransformOp_PUSH, false},
		{"$pull", db.TransformOp_PULL, false},
		{"$addToSet", db.TransformOp_ADD_TO_SET, false},
		{"$pop", db.TransformOp_POP, false},
		{"$mul", db.TransformOp_MUL, false},
		{"$min", db.TransformOp_MIN, false},
		{"$max", db.TransformOp_MAX, false},
		{"$currentDate", db.TransformOp_CURRENT_DATE, false},
		{"$rename", db.TransformOp_RENAME, false},
		{"$unknown", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			got, err := parseOpType(tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseOpType(%q) error = %v, wantErr %v", tt.op, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseOpType(%q) = %v, want %v", tt.op, got, tt.want)
			}
		})
	}
}

// --- normalizeTransformPath ---

func TestNormalizeTransformPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"email", "$.email"},
		{"$.email", "$.email"},
		{"", ""},
		{"nested.path", "$.nested.path"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeTransformPath(tt.input)
			if got != tt.want {
				t.Errorf("normalizeTransformPath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- toDBTransformOps ---

func TestToDBTransformOps(t *testing.T) {
	ops := []resolvedOp{
		{Op: "$set", Path: "email", Value: "alice@example.com"},
		{Op: "$unset", Path: "old_field"},
		{Op: "$inc", Path: "count", Value: float64(1)},
	}

	result, err := toDBTransformOps(ops)
	if err != nil {
		t.Fatalf("toDBTransformOps() error = %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 ops, got %d", len(result))
	}

	if result[0].GetOp() != db.TransformOp_SET {
		t.Errorf("op[0] = %v, want SET", result[0].GetOp())
	}
	if result[0].GetPath() != "$.email" {
		t.Errorf("op[0].Path = %q, want %q", result[0].GetPath(), "$.email")
	}
	if len(result[0].GetValue()) == 0 {
		t.Error("op[0].Value should not be empty")
	}

	if result[1].GetOp() != db.TransformOp_UNSET {
		t.Errorf("op[1] = %v, want UNSET", result[1].GetOp())
	}
	if result[1].GetPath() != "$.old_field" {
		t.Errorf("op[1].Path = %q, want %q", result[1].GetPath(), "$.old_field")
	}

	if result[2].GetOp() != db.TransformOp_INC {
		t.Errorf("op[2] = %v, want INC", result[2].GetOp())
	}
}

func TestToDBTransformOps_InvalidOp(t *testing.T) {
	ops := []resolvedOp{
		{Op: "$invalid", Path: "foo", Value: "bar"},
	}
	_, err := toDBTransformOps(ops)
	if err == nil {
		t.Error("expected error for invalid op type")
	}
}

// --- isPermanentReplicationError ---

func TestIsPermanentReplicationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"generic error", errors.New("some error"), false},
		{"auth failure", &pgconn.PgError{Code: "28P01"}, true},
		{"auth spec", &pgconn.PgError{Code: "28000"}, true},
		{"undefined table", &pgconn.PgError{Code: "42P01"}, true},
		{"db not exist", &pgconn.PgError{Code: "3D000"}, true},
		{"insufficient privilege", &pgconn.PgError{Code: "42501"}, true},
		{"duplicate object (transient)", &pgconn.PgError{Code: "42710"}, false},
		{"connection failure (transient)", &pgconn.PgError{Code: "08006"}, false},
		{"wrapped permanent", fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "28P01"}), true},
		{"wrapped transient", fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "08006"}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPermanentReplicationError(tt.err)
			if got != tt.want {
				t.Errorf("isPermanentReplicationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- pgQuoteIdentifier ---

func TestPgQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", `"simple"`},
		{`has"quote`, `"has""quote"`},
		{"", `""`},
		{"public.users", `"public.users"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := pgQuoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("pgQuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- navigateColumnRef ---

func TestNavigateColumnRef(t *testing.T) {
	row := map[string]any{
		"name":     "Alice",
		"metadata": map[string]any{"color": "red", "nested": map[string]any{"deep": "value"}},
	}

	t.Run("simple", func(t *testing.T) {
		val, err := navigateColumnRef("name", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "Alice" {
			t.Errorf("got %v, want %q", val, "Alice")
		}
	})

	t.Run("dotted", func(t *testing.T) {
		val, err := navigateColumnRef("metadata.color", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "red" {
			t.Errorf("got %v, want %q", val, "red")
		}
	})

	t.Run("deep dotted", func(t *testing.T) {
		val, err := navigateColumnRef("metadata.nested.deep", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "value" {
			t.Errorf("got %v, want %q", val, "value")
		}
	})

	t.Run("missing column", func(t *testing.T) {
		_, err := navigateColumnRef("missing", row)
		if err == nil {
			t.Error("expected error for missing column")
		}
	})

	t.Run("navigate into non-map", func(t *testing.T) {
		_, err := navigateColumnRef("name.sub", row)
		if err == nil {
			t.Error("expected error navigating into non-map")
		}
	})

	t.Run("missing nested key", func(t *testing.T) {
		_, err := navigateColumnRef("metadata.nonexistent", row)
		if err == nil {
			t.Error("expected error for missing nested key")
		}
	})
}

// --- resolveValue ---

func TestResolveValue(t *testing.T) {
	row := map[string]any{
		"id":   "abc",
		"num":  42,
		"meta": map[string]any{"k": "v"},
	}

	t.Run("non-string literal", func(t *testing.T) {
		val, err := resolveValue(float64(3.14), row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != float64(3.14) {
			t.Errorf("got %v, want 3.14", val)
		}
	})

	t.Run("bool literal", func(t *testing.T) {
		val, err := resolveValue(true, row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != true {
			t.Errorf("got %v, want true", val)
		}
	})

	t.Run("string literal no refs", func(t *testing.T) {
		val, err := resolveValue("hello", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "hello" {
			t.Errorf("got %v, want %q", val, "hello")
		}
	})

	t.Run("single column ref preserves type", func(t *testing.T) {
		val, err := resolveValue("{{num}}", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != 42 {
			t.Errorf("got %v (type %T), want 42 (int)", val, val)
		}
	})

	t.Run("single column ref to map preserves type", func(t *testing.T) {
		val, err := resolveValue("{{meta}}", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		m, ok := val.(map[string]any)
		if !ok {
			t.Fatalf("got type %T, want map[string]any", val)
		}
		if m["k"] != "v" {
			t.Errorf("got %v, want map with k=v", m)
		}
	})

	t.Run("mixed template becomes string", func(t *testing.T) {
		val, err := resolveValue("prefix_{{id}}_suffix", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "prefix_abc_suffix" {
			t.Errorf("got %v, want %q", val, "prefix_abc_suffix")
		}
	})

	t.Run("multiple refs become string", func(t *testing.T) {
		val, err := resolveValue("{{id}}-{{num}}", row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != "abc-42" {
			t.Errorf("got %v, want %q", val, "abc-42")
		}
	})

	t.Run("missing ref error", func(t *testing.T) {
		_, err := resolveValue("{{missing}}", row)
		if err == nil {
			t.Error("expected error for missing column reference")
		}
	})

	t.Run("nil value", func(t *testing.T) {
		val, err := resolveValue(nil, row)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if val != nil {
			t.Errorf("got %v, want nil", val)
		}
	})
}

// --- raftLSNStore ---

type mockMetadataKV struct {
	data map[string][]byte
}

func (m *mockMetadataKV) Get(_ context.Context, key []byte) ([]byte, io.Closer, error) {
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil, fmt.Errorf("not found")
	}
	return val, io.NopCloser(nil), nil
}

func (m *mockMetadataKV) Batch(_ context.Context, writes [][2][]byte, deletes [][]byte) error {
	for _, w := range writes {
		m.data[string(w[0])] = w[1]
	}
	for _, d := range deletes {
		delete(m.data, string(d))
	}
	return nil
}

func TestRaftLSNStore_RoundTrip(t *testing.T) {
	kv := &mockMetadataKV{data: make(map[string][]byte)}
	s := newRaftLSNStore(kv)
	ctx := context.Background()

	// Load with no stored value should return 0
	lsn, err := s.LoadLSN(ctx, "test_slot")
	if err != nil {
		t.Fatalf("LoadLSN (empty) error = %v", err)
	}
	if lsn != 0 {
		t.Errorf("LoadLSN (empty) = %v, want 0", lsn)
	}

	// Save and reload
	saveLSN := pglogrepl.LSN(0x16B3748)
	if err := s.SaveLSN(ctx, "test_slot", saveLSN); err != nil {
		t.Fatalf("SaveLSN error = %v", err)
	}

	loaded, err := s.LoadLSN(ctx, "test_slot")
	if err != nil {
		t.Fatalf("LoadLSN (after save) error = %v", err)
	}
	if loaded != saveLSN {
		t.Errorf("LoadLSN = %v, want %v", loaded, saveLSN)
	}
}

func TestRaftLSNStore_DifferentSlots(t *testing.T) {
	kv := &mockMetadataKV{data: make(map[string][]byte)}
	s := newRaftLSNStore(kv)
	ctx := context.Background()

	lsn1 := pglogrepl.LSN(100)
	lsn2 := pglogrepl.LSN(200)

	if err := s.SaveLSN(ctx, "slot_a", lsn1); err != nil {
		t.Fatalf("SaveLSN slot_a error = %v", err)
	}
	if err := s.SaveLSN(ctx, "slot_b", lsn2); err != nil {
		t.Fatalf("SaveLSN slot_b error = %v", err)
	}

	got1, err := s.LoadLSN(ctx, "slot_a")
	if err != nil {
		t.Fatalf("LoadLSN slot_a error = %v", err)
	}
	if got1 != lsn1 {
		t.Errorf("slot_a LSN = %v, want %v", got1, lsn1)
	}

	got2, err := s.LoadLSN(ctx, "slot_b")
	if err != nil {
		t.Fatalf("LoadLSN slot_b error = %v", err)
	}
	if got2 != lsn2 {
		t.Errorf("slot_b LSN = %v, want %v", got2, lsn2)
	}
}

// --- resolveSlotName / resolvePublicationName ---

func TestResolveSlotName(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		got := resolveSlotName("table", "pg_table", "my_custom_slot")
		if got != "my_custom_slot" {
			t.Errorf("got %q, want %q", got, "my_custom_slot")
		}
	})

	t.Run("derived", func(t *testing.T) {
		got := resolveSlotName("users", "pg_users", "")
		want := SlotName("users", "pg_users")
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

func TestResolvePublicationName(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		got := resolvePublicationName("table", "pg_table", "my_pub")
		if got != "my_pub" {
			t.Errorf("got %q, want %q", got, "my_pub")
		}
	})

	t.Run("derived", func(t *testing.T) {
		got := resolvePublicationName("users", "pg_users", "")
		want := PublicationName("users", "pg_users")
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
