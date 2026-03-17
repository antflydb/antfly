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

package join

import (
	"context"
	"testing"
	"time"
)

// mockTableQuerier implements TableQuerier for testing.
type mockTableQuerier struct {
	tables map[string][]Row
}

func newMockQuerier() *mockTableQuerier {
	return &mockTableQuerier{
		tables: make(map[string][]Row),
	}
}

func (m *mockTableQuerier) addTable(name string, rows []Row) {
	m.tables[name] = rows
}

func (m *mockTableQuerier) QueryTable(ctx context.Context, tableName string, filters *Filters, fields []string, limit int) ([]Row, error) {
	rows, ok := m.tables[tableName]
	if !ok {
		return []Row{}, nil
	}

	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	return rows, nil
}

func (m *mockTableQuerier) LookupKeys(ctx context.Context, tableName string, keys []any, keyField string, fields []string) ([]Row, error) {
	allRows, ok := m.tables[tableName]
	if !ok {
		return []Row{}, nil
	}

	keySet := make(map[any]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	var result []Row
	for _, row := range allRows {
		if val := extractFieldValue(row, keyField); val != nil {
			if keySet[val] {
				result = append(result, row)
			}
		}
	}

	return result, nil
}

func (m *mockTableQuerier) GetTableStatistics(ctx context.Context, tableName string) (*TableStatistics, error) {
	rows, ok := m.tables[tableName]
	if !ok {
		return &TableStatistics{}, nil
	}

	return &TableStatistics{
		RowCount:  int64(len(rows)),
		SizeBytes: int64(len(rows) * 200),
	}, nil
}

func TestHashTable(t *testing.T) {
	ht := NewHashTable()

	// Insert some rows
	ht.Insert("key1", Row{ID: "1", Fields: map[string]any{"name": "Alice"}})
	ht.Insert("key1", Row{ID: "2", Fields: map[string]any{"name": "Bob"}})
	ht.Insert("key2", Row{ID: "3", Fields: map[string]any{"name": "Charlie"}})

	// Check size
	if ht.Size() != 2 {
		t.Errorf("Expected 2 unique keys, got %d", ht.Size())
	}

	if ht.RowCount() != 3 {
		t.Errorf("Expected 3 total rows, got %d", ht.RowCount())
	}

	// Lookup
	rows := ht.Lookup("key1")
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows for key1, got %d", len(rows))
	}

	rows = ht.Lookup("key2")
	if len(rows) != 1 {
		t.Errorf("Expected 1 row for key2, got %d", len(rows))
	}

	rows = ht.Lookup("nonexistent")
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for nonexistent key, got %d", len(rows))
	}
}

func TestExtractFieldValue(t *testing.T) {
	row := Row{
		ID: "test",
		Fields: map[string]any{
			"name": "John",
			"age":  30,
			"address": map[string]any{
				"city":    "NYC",
				"country": "USA",
			},
		},
	}

	tests := []struct {
		field    string
		expected any
	}{
		{"name", "John"},
		{"age", 30},
		{"address.city", "NYC"},
		{"address.country", "USA"},
		{"nonexistent", nil},
		{"address.nonexistent", nil},
	}

	for _, tt := range tests {
		result := extractFieldValue(row, tt.field)
		if result != tt.expected {
			t.Errorf("extractFieldValue(%q) = %v, want %v", tt.field, result, tt.expected)
		}
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		left     any
		right    any
		op       Operator
		expected bool
	}{
		// Equality
		{"abc", "abc", OpEqual, true},
		{"abc", "def", OpEqual, false},
		{1, 1, OpEqual, true},
		{1, 2, OpEqual, false},
		{1.0, 1, OpEqual, true},

		// Not equal
		{"abc", "def", OpNotEqual, true},
		{"abc", "abc", OpNotEqual, false},

		// Less than
		{1, 2, OpLessThan, true},
		{2, 1, OpLessThan, false},
		{"abc", "def", OpLessThan, true},

		// Less than or equal
		{1, 2, OpLessEqual, true},
		{2, 2, OpLessEqual, true},
		{3, 2, OpLessEqual, false},

		// Greater than
		{2, 1, OpGreaterThan, true},
		{1, 2, OpGreaterThan, false},

		// Greater than or equal
		{2, 1, OpGreaterEqual, true},
		{2, 2, OpGreaterEqual, true},
		{1, 2, OpGreaterEqual, false},

		// Nil values
		{nil, "abc", OpEqual, false},
		{"abc", nil, OpEqual, false},
	}

	for _, tt := range tests {
		result := compareValues(tt.left, tt.right, tt.op)
		if result != tt.expected {
			t.Errorf("compareValues(%v, %v, %v) = %v, want %v",
				tt.left, tt.right, tt.op, result, tt.expected)
		}
	}
}

func TestBroadcastJoinInner(t *testing.T) {
	ctx := context.Background()
	querier := newMockQuerier()

	// Setup test data
	customers := []Row{
		{ID: "c1", Fields: map[string]any{"id": "c1", "name": "Alice"}},
		{ID: "c2", Fields: map[string]any{"id": "c2", "name": "Bob"}},
		{ID: "c3", Fields: map[string]any{"id": "c3", "name": "Charlie"}},
	}
	querier.addTable("customers", customers)

	orders := []Row{
		{ID: "o1", Fields: map[string]any{"customer_id": "c1", "amount": 100}},
		{ID: "o2", Fields: map[string]any{"customer_id": "c2", "amount": 200}},
		{ID: "o3", Fields: map[string]any{"customer_id": "c1", "amount": 150}},
	}

	executor := NewBroadcastExecutor(DefaultExecutorConfig(), nil, querier)

	plan := &Plan{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinType:   TypeInner,
		Strategy:   StrategyBroadcast,
		Condition: Condition{
			LeftField:  "customer_id",
			RightField: "id",
			Operator:   OpEqual,
		},
	}

	result, joinResult, err := executor.Execute(ctx, plan, orders)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 joined rows, got %d", len(result))
	}

	if joinResult.RowsMatched != 3 {
		t.Errorf("Expected 3 matched rows, got %d", joinResult.RowsMatched)
	}

	if joinResult.StrategyUsed != StrategyBroadcast {
		t.Errorf("Expected broadcast strategy, got %s", joinResult.StrategyUsed)
	}
}

func TestBroadcastJoinLeft(t *testing.T) {
	ctx := context.Background()
	querier := newMockQuerier()

	// Setup test data - c3 has no orders
	customers := []Row{
		{ID: "c1", Fields: map[string]any{"id": "c1", "name": "Alice"}},
		{ID: "c2", Fields: map[string]any{"id": "c2", "name": "Bob"}},
	}
	querier.addTable("customers", customers)

	orders := []Row{
		{ID: "o1", Fields: map[string]any{"customer_id": "c1", "amount": 100}},
		{ID: "o2", Fields: map[string]any{"customer_id": "c2", "amount": 200}},
		{ID: "o3", Fields: map[string]any{"customer_id": "c3", "amount": 150}}, // No matching customer
	}

	executor := NewBroadcastExecutor(DefaultExecutorConfig(), nil, querier)

	plan := &Plan{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinType:   TypeLeft,
		Strategy:   StrategyBroadcast,
		Condition: Condition{
			LeftField:  "customer_id",
			RightField: "id",
			Operator:   OpEqual,
		},
	}

	result, joinResult, err := executor.Execute(ctx, plan, orders)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 joined rows (including unmatched), got %d", len(result))
	}

	if joinResult.RowsMatched != 2 {
		t.Errorf("Expected 2 matched rows, got %d", joinResult.RowsMatched)
	}

	if joinResult.RowsUnmatchedLeft != 1 {
		t.Errorf("Expected 1 unmatched left row, got %d", joinResult.RowsUnmatchedLeft)
	}
}

func TestIndexLookupJoin(t *testing.T) {
	ctx := context.Background()
	querier := newMockQuerier()

	// Setup test data
	customers := []Row{
		{ID: "c1", Fields: map[string]any{"id": "c1", "name": "Alice"}},
		{ID: "c2", Fields: map[string]any{"id": "c2", "name": "Bob"}},
		{ID: "c3", Fields: map[string]any{"id": "c3", "name": "Charlie"}},
	}
	querier.addTable("customers", customers)

	orders := []Row{
		{ID: "o1", Fields: map[string]any{"customer_id": "c1", "amount": 100}},
		{ID: "o2", Fields: map[string]any{"customer_id": "c2", "amount": 200}},
	}

	executor := NewIndexLookupExecutor(DefaultExecutorConfig(), nil, querier)

	plan := &Plan{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinType:   TypeInner,
		Strategy:   StrategyIndexLookup,
		Condition: Condition{
			LeftField:  "customer_id",
			RightField: "id",
			Operator:   OpEqual,
		},
	}

	result, joinResult, err := executor.Execute(ctx, plan, orders)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 joined rows, got %d", len(result))
	}

	if joinResult.RowsMatched != 2 {
		t.Errorf("Expected 2 matched rows, got %d", joinResult.RowsMatched)
	}

	if joinResult.StrategyUsed != StrategyIndexLookup {
		t.Errorf("Expected index_lookup strategy, got %s", joinResult.StrategyUsed)
	}
}

func TestShuffleJoin(t *testing.T) {
	ctx := context.Background()
	querier := newMockQuerier()

	// Setup test data
	customers := []Row{
		{ID: "c1", Fields: map[string]any{"id": "c1", "name": "Alice"}},
		{ID: "c2", Fields: map[string]any{"id": "c2", "name": "Bob"}},
		{ID: "c3", Fields: map[string]any{"id": "c3", "name": "Charlie"}},
	}
	querier.addTable("customers", customers)

	orders := []Row{
		{ID: "o1", Fields: map[string]any{"customer_id": "c1", "amount": 100}},
		{ID: "o2", Fields: map[string]any{"customer_id": "c2", "amount": 200}},
		{ID: "o3", Fields: map[string]any{"customer_id": "c1", "amount": 150}},
	}

	executor := NewShuffleExecutor(DefaultExecutorConfig(), nil, querier)

	plan := &Plan{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinType:   TypeInner,
		Strategy:   StrategyShuffle,
		Condition: Condition{
			LeftField:  "customer_id",
			RightField: "id",
			Operator:   OpEqual,
		},
	}

	result, joinResult, err := executor.Execute(ctx, plan, orders)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 joined rows, got %d", len(result))
	}

	if joinResult.RowsMatched != 3 {
		t.Errorf("Expected 3 matched rows, got %d", joinResult.RowsMatched)
	}

	if joinResult.StrategyUsed != StrategyShuffle {
		t.Errorf("Expected shuffle strategy, got %s", joinResult.StrategyUsed)
	}
}

func TestPlannerStrategySelection(t *testing.T) {
	ctx := context.Background()

	planner, err := NewPlanner(DefaultPlannerConfig(), nil)
	if err != nil {
		t.Fatalf("Failed to create planner: %v", err)
	}

	tests := []struct {
		name          string
		leftStats     *TableStatistics
		rightStats    *TableStatistics
		expectedStrat Strategy
	}{
		{
			name:          "small right table -> broadcast",
			leftStats:     &TableStatistics{RowCount: 1000000, SizeBytes: 100 * 1024 * 1024},
			rightStats:    &TableStatistics{RowCount: 100, SizeBytes: 1 * 1024 * 1024},
			expectedStrat: StrategyBroadcast,
		},
		{
			name:          "both large -> shuffle",
			leftStats:     &TableStatistics{RowCount: 1000000, SizeBytes: 100 * 1024 * 1024},
			rightStats:    &TableStatistics{RowCount: 1000000, SizeBytes: 100 * 1024 * 1024},
			expectedStrat: StrategyShuffle,
		},
		{
			name:          "no stats -> broadcast default",
			leftStats:     nil,
			rightStats:    nil,
			expectedStrat: StrategyBroadcast,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear cache to ensure each test starts fresh
			planner.ClearCache()

			input := &PlanInput{
				LeftTable:  "left",
				LeftStats:  tt.leftStats,
				RightTable: "right",
				RightStats: tt.rightStats,
				JoinClause: &Clause{
					RightTable: "right",
					On: Condition{
						LeftField:  "id",
						RightField: "id",
					},
				},
			}

			plan, err := planner.CreatePlan(ctx, input)
			if err != nil {
				t.Fatalf("CreatePlan failed: %v", err)
			}

			if plan.Strategy != tt.expectedStrat {
				t.Errorf("Expected strategy %s, got %s", tt.expectedStrat, plan.Strategy)
			}
		})
	}
}

func TestValidateJoinClause(t *testing.T) {
	tests := []struct {
		name      string
		clause    *Clause
		wantError bool
	}{
		{
			name:      "nil clause",
			clause:    nil,
			wantError: true,
		},
		{
			name: "missing right_table",
			clause: &Clause{
				On: Condition{LeftField: "a", RightField: "b"},
			},
			wantError: true,
		},
		{
			name: "missing left_field",
			clause: &Clause{
				RightTable: "t",
				On:         Condition{RightField: "b"},
			},
			wantError: true,
		},
		{
			name: "missing right_field",
			clause: &Clause{
				RightTable: "t",
				On:         Condition{LeftField: "a"},
			},
			wantError: true,
		},
		{
			name: "valid clause",
			clause: &Clause{
				RightTable: "t",
				On:         Condition{LeftField: "a", RightField: "b"},
			},
			wantError: false,
		},
		{
			name: "valid clause with all options",
			clause: &Clause{
				RightTable:   "t",
				JoinType:     TypeLeft,
				StrategyHint: StrategyBroadcast,
				On:           Condition{LeftField: "a", RightField: "b", Operator: OpEqual},
			},
			wantError: false,
		},
		{
			name: "invalid join_type",
			clause: &Clause{
				RightTable: "t",
				JoinType:   "invalid",
				On:         Condition{LeftField: "a", RightField: "b"},
			},
			wantError: true,
		},
		{
			name: "invalid operator",
			clause: &Clause{
				RightTable: "t",
				On:         Condition{LeftField: "a", RightField: "b", Operator: "invalid"},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateJoinClause(tt.clause)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateJoinClause() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestNestedJoin(t *testing.T) {
	ctx := context.Background()
	querier := newMockQuerier()

	// Setup test data
	customers := []Row{
		{ID: "c1", Fields: map[string]any{"id": "c1", "name": "Alice", "address_id": "a1"}},
		{ID: "c2", Fields: map[string]any{"id": "c2", "name": "Bob", "address_id": "a2"}},
	}
	addresses := []Row{
		{ID: "a1", Fields: map[string]any{"id": "a1", "city": "NYC"}},
		{ID: "a2", Fields: map[string]any{"id": "a2", "city": "LA"}},
	}
	querier.addTable("customers", customers)
	querier.addTable("addresses", addresses)

	orders := []Row{
		{ID: "o1", Fields: map[string]any{"customer_id": "c1", "amount": 100}},
		{ID: "o2", Fields: map[string]any{"customer_id": "c2", "amount": 200}},
	}

	factory := NewExecutorFactory(DefaultExecutorConfig(), nil, nil, querier)

	plan := &Plan{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinType:   TypeInner,
		Strategy:   StrategyBroadcast,
		Condition: Condition{
			LeftField:  "customer_id",
			RightField: "id",
			Operator:   OpEqual,
		},
		NestedPlan: &Plan{
			LeftTable:  "customers",
			RightTable: "addresses",
			JoinType:   TypeInner,
			Strategy:   StrategyBroadcast,
			Condition: Condition{
				LeftField:  "customers.address_id",
				RightField: "id",
				Operator:   OpEqual,
			},
		},
	}

	result, _, err := factory.ExecuteJoin(ctx, plan, orders)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 rows (each order joined with customer and address)
	if len(result) != 2 {
		t.Errorf("Expected 2 joined rows, got %d", len(result))
	}
}

func TestEstimateJoinCardinality(t *testing.T) {
	tests := []struct {
		name       string
		leftStats  *TableStatistics
		rightStats *TableStatistics
		condition  Condition
		minExpect  int64
		maxExpect  int64
	}{
		{
			name:       "nil stats",
			leftStats:  nil,
			rightStats: nil,
			condition:  Condition{LeftField: "a", RightField: "b"},
			minExpect:  1000,
			maxExpect:  1000,
		},
		{
			name:       "small tables",
			leftStats:  &TableStatistics{RowCount: 100},
			rightStats: &TableStatistics{RowCount: 50},
			condition:  Condition{LeftField: "a", RightField: "b"},
			minExpect:  1,
			maxExpect:  5000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateJoinCardinality(tt.leftStats, tt.rightStats, tt.condition)
			if result < tt.minExpect || result > tt.maxExpect {
				t.Errorf("EstimateJoinCardinality() = %d, want between %d and %d",
					result, tt.minExpect, tt.maxExpect)
			}
		})
	}
}

func TestMemoryManager(t *testing.T) {
	config := &MemoryManagerConfig{
		MaxMemory:      1 * 1024 * 1024, // 1MB
		SpillThreshold: 512 * 1024,      // 512KB
		SpillDir:       t.TempDir(),
	}

	mm, err := NewMemoryManager(config, nil)
	if err != nil {
		t.Fatalf("Failed to create memory manager: %v", err)
	}
	defer mm.Cleanup()

	// Test allocation
	if !mm.Allocate(1000) {
		t.Error("Should be able to allocate 1000 bytes")
	}

	if mm.CurrentUsage() != 1000 {
		t.Errorf("Expected usage 1000, got %d", mm.CurrentUsage())
	}

	// Release
	mm.Release(500)
	if mm.CurrentUsage() != 500 {
		t.Errorf("Expected usage 500, got %d", mm.CurrentUsage())
	}

	// Over allocation
	if mm.Allocate(2 * 1024 * 1024) {
		t.Error("Should not be able to over-allocate")
	}
}

func TestSplitField(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"name", []string{"name"}},
		{"address.city", []string{"address", "city"}},
		{"a.b.c.d", []string{"a", "b", "c", "d"}},
		{"", []string{}},
	}

	for _, tt := range tests {
		result := splitField(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("splitField(%q) = %v, want %v", tt.input, result, tt.expected)
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("splitField(%q)[%d] = %q, want %q", tt.input, i, result[i], tt.expected[i])
			}
		}
	}
}

func TestPlanCaching(t *testing.T) {
	ctx := context.Background()

	config := DefaultPlannerConfig()
	config.PlanCacheTTL = 1 * time.Minute

	planner, err := NewPlanner(config, nil)
	if err != nil {
		t.Fatalf("Failed to create planner: %v", err)
	}

	input := &PlanInput{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinClause: &Clause{
			RightTable: "customers",
			On: Condition{
				LeftField:  "customer_id",
				RightField: "id",
			},
		},
	}

	// First plan creation
	plan1, err := planner.CreatePlan(ctx, input)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}

	// Second call should return cached plan
	plan2, err := planner.CreatePlan(ctx, input)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}

	// Both should have same values
	if plan1.Strategy != plan2.Strategy {
		t.Error("Cached plan should have same strategy")
	}

	// Clear cache
	planner.ClearCache()

	// New plan should be created
	plan3, err := planner.CreatePlan(ctx, input)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}

	if plan3.Strategy != plan1.Strategy {
		t.Error("New plan should have same strategy")
	}
}

func TestPlanCaching_DistinguishesJoinShape(t *testing.T) {
	ctx := context.Background()

	config := DefaultPlannerConfig()
	config.PlanCacheTTL = time.Minute

	planner, err := NewPlanner(config, nil)
	if err != nil {
		t.Fatalf("Failed to create planner: %v", err)
	}

	innerInput := &PlanInput{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinClause: &Clause{
			RightTable: "customers",
			JoinType:   TypeInner,
			On: Condition{
				LeftField:  "customer_id",
				RightField: "customer_id",
			},
		},
	}

	leftInput := &PlanInput{
		LeftTable:  "orders",
		RightTable: "customers",
		JoinClause: &Clause{
			RightTable: "customers",
			JoinType:   TypeLeft,
			On: Condition{
				LeftField:  "customer_id",
				RightField: "customer_id",
			},
		},
	}

	innerPlan, err := planner.CreatePlan(ctx, innerInput)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}
	if innerPlan.JoinType != TypeInner {
		t.Fatalf("Expected inner plan, got %s", innerPlan.JoinType)
	}

	leftPlan, err := planner.CreatePlan(ctx, leftInput)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}
	if leftPlan.JoinType != TypeLeft {
		t.Fatalf("Expected left plan, got %s", leftPlan.JoinType)
	}

	if innerPlan == leftPlan {
		t.Fatal("Expected distinct cached plans for inner and left joins")
	}
}
