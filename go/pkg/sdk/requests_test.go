package sdk

import (
	"bytes"
	"testing"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
)

func rowsWhereAtom(field string, op RowsWhereOp, value interface{}) RowsWhere {
	var where RowsWhere
	if err := where.FromRowsWhere0(RowsWhere0{Field: field, Op: op, Value: value}); err != nil {
		panic(err)
	}
	return where
}

func rowsWhereGroup(group RowsWhere1) RowsWhere {
	var where RowsWhere
	if err := where.FromRowsWhere1(group); err != nil {
		panic(err)
	}
	return where
}

func rowsExprField(field string) RowsExpression {
	var expr RowsExpression
	if err := expr.FromRowsExpressionField(RowsExpressionField{Field: field}); err != nil {
		panic(err)
	}
	return expr
}

func rowsExprFieldSource(field string, source string) RowsExpression {
	var expr RowsExpression
	if err := expr.FromRowsExpressionField(RowsExpressionField{Field: field, Source: RowsExpressionFieldSource(source)}); err != nil {
		panic(err)
	}
	return expr
}

func rowsExprValue(value interface{}) RowsExpression {
	var expr RowsExpression
	if err := expr.FromRowsExpressionValue(RowsExpressionValue{Value: value}); err != nil {
		panic(err)
	}
	return expr
}

func rowsExprOp(op string, args []RowsExpression) RowsExpression {
	return rowsExprOperator(RowsExpressionOperator{Op: RowsExpressionOperatorOp(op), Args: args})
}

func rowsExprOperator(operator RowsExpressionOperator) RowsExpression {
	var expr RowsExpression
	if err := expr.FromRowsExpressionOperator(operator); err != nil {
		panic(err)
	}
	return expr
}

func ptrRowsExpression(expr RowsExpression) *RowsExpression {
	return &expr
}

func rowsWhereBranchAtom(field string, op string, value interface{}) RowsWhereBranch {
	var branch RowsWhereBranch
	if err := branch.FromRowsWhereBranchAtom(RowsWhereBranchAtom{Field: field, Op: RowsWhereBranchAtomOp(op), Value: value}); err != nil {
		panic(err)
	}
	return branch
}

func rowsWhereBranchAll(all []RowsWhereAtom) RowsWhereBranch {
	var branch RowsWhereBranch
	if err := branch.FromRowsWhereBranchAll(RowsWhereBranchAll{All: all}); err != nil {
		panic(err)
	}
	return branch
}

func rowsCoalesceField(field string) RowsCoalesceOperand {
	var operand RowsCoalesceOperand
	if err := operand.FromRowsCoalesceFieldOperand(RowsCoalesceFieldOperand{Field: field}); err != nil {
		panic(err)
	}
	return operand
}

func rowsCoalesceValue(value interface{}) RowsCoalesceOperand {
	var operand RowsCoalesceOperand
	if err := operand.FromRowsCoalesceValueOperand(RowsCoalesceValueOperand{Value: value}); err != nil {
		panic(err)
	}
	return operand
}

func rowsOrderField(field string, direction string) RowsQueryOrder {
	var order RowsQueryOrder
	if err := order.FromRowsQueryOrderField(RowsQueryOrderField{Field: field, Direction: RowsQueryOrderFieldDirection(direction)}); err != nil {
		panic(err)
	}
	return order
}

func rowsOrderExpr(expr RowsExpression) RowsQueryOrder {
	var order RowsQueryOrder
	if err := order.FromRowsQueryOrderExpression(RowsQueryOrderExpression{Expr: expr}); err != nil {
		panic(err)
	}
	return order
}

func TestRowFilterEntryMarshalPreservesTypedFacade(t *testing.T) {
	body, err := json.Marshal(RowFilterEntry{
		Table: "usage_records",
		Filter: map[string]interface{}{
			"term": map[string]interface{}{"tenant_id": "t1"},
		},
	})
	if err != nil {
		t.Fatalf("Marshal row filter entry: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"table":"usage_records"`),
		[]byte(`"term":{"tenant_id":"t1"}`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted row filter entry field %s: %s", want, body)
		}
	}
}

func TestQueryRequestMarshalOmitsZeroJoin(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Table: "files",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(body, []byte(`"join"`)) {
		t.Fatalf("Marshal emitted zero join: %s", body)
	}
}

func TestQueryRequestMarshalPreservesJoin(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Table: "files",
		Join: JoinClause{
			RightTable: "entities",
			On: JoinCondition{
				LeftField:  "entity_id",
				RightField: "id",
			},
		},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(body, []byte(`"join"`)) {
		t.Fatalf("Marshal omitted populated join: %s", body)
	}
	if !bytes.Contains(body, []byte(`"right_table":"entities"`)) {
		t.Fatalf("Marshal encoded unexpected join: %s", body)
	}
}

func TestGraphIndexStatsRuntimeSummaryRoundTrip(t *testing.T) {
	body, err := json.Marshal(GraphIndexStats{
		IndexType:  GraphIndexStatsIndexType("graph"),
		TotalEdges: 4,
		GraphMetricRuntime: GraphMetricRuntimeStats{
			Enabled:             true,
			Role:                GraphMetricRuntimeStatsRole("worker_pool"),
			OwnerIdHash:         17,
			WorkerCount:         3,
			TakeoverCount:       2,
			LostLeases:          1,
			TotalPagesClaimed:   6,
			LastPagesCompleted:  3,
			LastBudgetExhausted: true,
		},
	})
	if err != nil {
		t.Fatalf("Marshal graph stats: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"graph_metric_runtime"`),
		[]byte(`"role":"worker_pool"`),
		[]byte(`"owner_id_hash":17`),
		[]byte(`"last_budget_exhausted":true`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted graph metric runtime field %s: %s", want, body)
		}
	}

	var stats GraphIndexStats
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("Unmarshal graph stats: %v", err)
	}
	if stats.GraphMetricRuntime.Role != GraphMetricRuntimeStatsRole("worker_pool") {
		t.Fatalf("unexpected runtime role: %q", stats.GraphMetricRuntime.Role)
	}
	if stats.GraphMetricRuntime.OwnerIdHash != 17 ||
		stats.GraphMetricRuntime.WorkerCount != 3 ||
		stats.GraphMetricRuntime.TotalPagesClaimed != 6 ||
		!stats.GraphMetricRuntime.LastBudgetExhausted {
		t.Fatalf("unexpected runtime summary: %+v", stats.GraphMetricRuntime)
	}
}

func TestRowsPlanRequestMarshalPreservesTypedJoinAndLateral(t *testing.T) {
	joinBody, err := json.Marshal(RowsJoinPlanRequest{
		Ctes: []RowsCte{
			{
				Name:     "open_orders",
				MaxRows:  100,
				MaxBytes: 4096,
				Query: RowsQueryRequest{
					Where: rowsWhereAtom("status", RowsWhereOp("eq"), "open"),
				},
			},
		},
		Join: RowsJoinRequest{
			Left: RowsQueryRequest{
				SourceCte: "open_orders",
				Select:    []string{"tenant_id", "order_id"},
			},
			Right: RowsQueryRequest{
				Where: rowsWhereAtom("kind", RowsWhereOp("eq"), "customer"),
				ExpressionAny: []RowsExpressionConditionGroup{{
					All: []RowsExpressionCondition{{
						Lhs: rowsExprField("status"),
						Op:  RowsExpressionConditionOp("eq"),
						Rhs: rowsExprValue("active"),
					}},
				}},
				Select: []string{"tenant_id", "customer_id"},
			},
			On:       []RowsJoinOn{{LeftField: "tenant_id", RightField: "tenant_id"}},
			JoinType: RowsJoinRequestJoinType("left"),
			Select: []RowsJoinProjection{
				{As: "order_id", Side: RowsJoinProjectionSide("left"), Field: "order_id"},
				{As: "customer_id", Side: RowsJoinProjectionSide("right"), Field: "customer_id"},
			},
			OrderBy: []RowsQueryOrder{rowsOrderField("order_id", "asc")},
			Limit:   25,
		},
	})
	if err != nil {
		t.Fatalf("Marshal join plan: %v", err)
	}
	if !bytes.Contains(joinBody, []byte(`"source_cte":"open_orders"`)) {
		t.Fatalf("Marshal omitted CTE source: %s", joinBody)
	}
	if !bytes.Contains(joinBody, []byte(`"max_rows":100`)) {
		t.Fatalf("Marshal omitted CTE materialization bound: %s", joinBody)
	}
	if !bytes.Contains(joinBody, []byte(`"max_bytes":4096`)) {
		t.Fatalf("Marshal omitted CTE byte materialization bound: %s", joinBody)
	}
	if !bytes.Contains(joinBody, []byte(`"join_type":"left"`)) {
		t.Fatalf("Marshal omitted join type: %s", joinBody)
	}
	if !bytes.Contains(joinBody, []byte(`"expression_any"`)) {
		t.Fatalf("Marshal omitted expression predicate: %s", joinBody)
	}

	lateralBody, err := json.Marshal(RowsLateralPlanRequest{
		Ctes: []RowsCte{{Name: "tenants", Query: RowsQueryRequest{Select: []string{"tenant_id"}}}},
		Lateral: RowsLateralRequest{
			Left:         RowsQueryRequest{SourceCte: "tenants", Select: []string{"tenant_id"}},
			Right:        RowsQueryRequest{Where: rowsWhereAtom("kind", RowsWhereOp("eq"), "event"), Limit: 3},
			Correlations: []RowsLateralCorrelation{{LeftField: "tenant_id", RightField: "tenant_id"}},
			Select: []RowsJoinProjection{
				{As: "tenant_id", Side: RowsJoinProjectionSide("left"), Field: "tenant_id"},
				{As: "event_id", Side: RowsJoinProjectionSide("right"), Field: "id"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Marshal lateral plan: %v", err)
	}
	if !bytes.Contains(lateralBody, []byte(`"correlations":[{"left_field":"tenant_id","right_field":"tenant_id"}]`)) {
		t.Fatalf("Marshal omitted lateral correlations: %s", lateralBody)
	}
	if !bytes.Contains(lateralBody, []byte(`"limit":3`)) {
		t.Fatalf("Marshal omitted lateral right limit: %s", lateralBody)
	}

}

func TestRowsPlanRequestGenericWrapperValidatesOneOperation(t *testing.T) {
	var plan RowsPlanRequest
	if err := plan.FromRowsQueryPlanRequest(RowsQueryPlanRequest{
		Ranges: []RowsDocKeyRange{{Start: "row:a", End: "row:z"}},
		Query:  RowsQueryRequest{Select: []string{"id"}},
	}); err != nil {
		t.Fatalf("Build generic query plan: %v", err)
	}
	body, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("Marshal generic query plan: %v", err)
	}
	if !bytes.Contains(body, []byte(`"query":{"select":["id"]}`)) {
		t.Fatalf("Marshal omitted query operation: %s", body)
	}

	var decoded RowsPlanRequest
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("Unmarshal generic query plan: %v", err)
	}
	query, err := decoded.AsRowsQueryPlanRequest()
	if err != nil {
		t.Fatalf("Unmarshal decoded wrong generic branch: %v", err)
	}
	if len(query.Ranges) != 1 || len(query.Query.Select) != 1 || query.Query.Select[0] != "id" {
		t.Fatalf("Unmarshal decoded wrong generic branch: %+v", decoded)
	}

}

func TestRowsSdkReexportsCoverRelationalPlanEnumsAndGetResults(t *testing.T) {
	getBody, err := json.Marshal(RowsGetRequest{
		Keys: []RowSelector{{
			Primary: RowPrimarySelector{"tenant_id": "t1", "id": "u1"},
		}},
		IncludePhysicalKey: true,
	})
	if err != nil {
		t.Fatalf("Marshal get request: %v", err)
	}
	if !bytes.Contains(getBody, []byte(`"primary":{"id":"u1","tenant_id":"t1"}`)) {
		t.Fatalf("Marshal get request omitted primary selector: %s", getBody)
	}

	resultBody, err := json.Marshal(RowsGetResultSet{
		Rows: []RowsGetResult{{
			Identity: RowSelector{Primary: RowPrimarySelector{"tenant_id": "t1", "id": "u1"}},
			Found:    true,
			Row:      map[string]interface{}{"id": "u1", "status": "ready"},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal get result set: %v", err)
	}
	if !bytes.Contains(resultBody, []byte(`"found":true`)) {
		t.Fatalf("Marshal get result set omitted found state: %s", resultBody)
	}

	aggregateBody, err := json.Marshal(RowsAggregateSpec{
		Name:  "recent_statuses",
		Op:    RowsAggregateSpecOp("array_agg"),
		Field: "status",
	})
	if err != nil {
		t.Fatalf("Marshal aggregate enum alias: %v", err)
	}
	if !bytes.Contains(aggregateBody, []byte(`"op":"array_agg"`)) {
		t.Fatalf("Marshal aggregate enum alias mismatch: %s", aggregateBody)
	}

	windowBody, err := json.Marshal(RowsWindowFrame{
		Unit:        RowsWindowFrameUnit("rows"),
		Start:       RowsWindowFrameStart("offset_preceding"),
		StartOffset: 1,
		End:         RowsWindowFrameEnd("current_row"),
	})
	if err != nil {
		t.Fatalf("Marshal window frame enum aliases: %v", err)
	}
	if !bytes.Contains(windowBody, []byte(`"unit":"rows"`)) ||
		!bytes.Contains(windowBody, []byte(`"start":"offset_preceding"`)) ||
		!bytes.Contains(windowBody, []byte(`"end":"current_row"`)) {
		t.Fatalf("Marshal window frame enum aliases mismatch: %s", windowBody)
	}
}

func TestRowsQueryOrderValidatesFieldOrExpression(t *testing.T) {
	fieldBody, err := json.Marshal(rowsOrderField("created_at", "desc"))
	if err != nil {
		t.Fatalf("Marshal field order: %v", err)
	}
	if !bytes.Contains(fieldBody, []byte(`"field":"created_at"`)) {
		t.Fatalf("Marshal omitted field order: %s", fieldBody)
	}

	exprBody, err := json.Marshal(rowsOrderExpr(rowsExprOp("lower", []RowsExpression{rowsExprField("email")})))
	if err != nil {
		t.Fatalf("Marshal expression order: %v", err)
	}
	if !bytes.Contains(exprBody, []byte(`"expr":{"args":[{"field":"email"}],"op":"lower"}`)) {
		t.Fatalf("Marshal omitted expression order: %s", exprBody)
	}

}

func TestRowsDocKeyRangeValidatesDeclaredBounds(t *testing.T) {
	validRanges := []RowsDocKeyRange{
		{Start: "row:a"},
		{End: "row:z"},
		{Start: "row:a", End: "row:z"},
	}
	for _, rangeScope := range validRanges {
		if _, err := json.Marshal(rangeScope); err != nil {
			t.Fatalf("Marshal rejected valid range %+v: %v", rangeScope, err)
		}
	}

}

func TestRowsAggregatePlanRequestMarshalPreservesTypedHaving(t *testing.T) {
	body, err := json.Marshal(RowsAggregatePlanRequest{
		Ranges: []RowsDocKeyRange{{Start: "row:a", End: "row:z"}},
		Aggregate: RowsAggregateRequest{
			Source: RowsQueryRequest{
				Where: rowsWhereAtom("status", RowsWhereOp("eq"), "open"),
			},
			GroupBy: []string{"customer_id"},
			Aggregations: []RowsAggregateSpec{
				{Name: "amount_sum", Op: "sum", Field: "amount"},
			},
			Having: RowsAggregateHaving{All: []RowsAggregateHavingPredicate{
				{Field: "amount_sum", Op: RowsAggregateHavingPredicateOp("gt"), Value: 100},
			}},
			OrderBy: []RowsQueryOrder{rowsOrderField("amount_sum", "desc")},
			Limit:   10,
		},
	})
	if err != nil {
		t.Fatalf("Marshal aggregate plan: %v", err)
	}
	if !bytes.Contains(body, []byte(`"having":{"all":[{"field":"amount_sum","op":"gt","value":100}]}`)) {
		t.Fatalf("Marshal omitted typed having group: %s", body)
	}
	if !bytes.Contains(body, []byte(`"ranges":[{"end":"row:z","start":"row:a"}]`)) {
		t.Fatalf("Marshal omitted declared ranges: %s", body)
	}
}

func TestRowsAggregateSpecMarshalValidatesInputContract(t *testing.T) {
	arrayBody, err := json.Marshal(RowsAggregateSpec{
		Name:             "recent_statuses",
		Op:               "array_agg",
		Field:            "status",
		Distinct:         true,
		DistinctMaxItems: 128,
		ArrayMaxItems:    64,
		ArrayOrderBy:     []RowsQueryOrder{rowsOrderField("created_at", "desc")},
	})
	if err != nil {
		t.Fatalf("Marshal array_agg aggregate spec: %v", err)
	}
	if !bytes.Contains(arrayBody, []byte(`"array_max_items":64`)) || !bytes.Contains(arrayBody, []byte(`"distinct_max_items":128`)) {
		t.Fatalf("Marshal omitted aggregate spec controls: %s", arrayBody)
	}

	countBody, err := json.Marshal(RowsAggregateSpec{Name: "row_count", Op: "count"})
	if err != nil {
		t.Fatalf("Marshal count aggregate spec: %v", err)
	}
	if bytes.Contains(countBody, []byte(`"field"`)) || bytes.Contains(countBody, []byte(`"expr"`)) {
		t.Fatalf("Marshal emitted omitted count input: %s", countBody)
	}

	var decoded RowsAggregateSpec
	if err := json.Unmarshal([]byte(`{"name":"row_count","op":"count"}`), &decoded); err != nil {
		t.Fatalf("Unmarshal count aggregate spec: %v", err)
	}
	if decoded.Name != "row_count" || decoded.Op != "count" {
		t.Fatalf("Unmarshal decoded wrong aggregate spec: %#v", decoded)
	}

}

func TestRowsWindowSpecMarshalPreservesFrameShape(t *testing.T) {
	lagBody, err := json.Marshal(RowsWindowSpec{
		As:       "previous_amount",
		Function: "lag",
		Expr:     rowsExprField("amount"),
		OrderBy:  []RowsQueryOrder{rowsOrderField("created_at", "asc")},
		Frame: RowsWindowFrame{
			Unit:        "rows",
			Start:       "offset_preceding",
			StartOffset: 1,
			End:         "current_row",
		},
	})
	if err != nil {
		t.Fatalf("Marshal lag window: %v", err)
	}
	if !bytes.Contains(lagBody, []byte(`"frame":{"end":"current_row","start":"offset_preceding","start_offset":1,"unit":"rows"}`)) {
		t.Fatalf("Marshal omitted window frame: %s", lagBody)
	}
}

func TestRowsJoinAndLateralPlanRequestsMarshalDeclaredRanges(t *testing.T) {
	joinBody, err := json.Marshal(RowsJoinPlanRequest{
		LeftRanges:  []RowsDocKeyRange{{Start: "row:orders:", End: "row:orders;"}},
		RightRanges: []RowsDocKeyRange{{Start: "row:customers:", End: "row:customers;"}},
		Join: RowsJoinRequest{
			Left:  RowsQueryRequest{},
			Right: RowsQueryRequest{},
			On:    []RowsJoinOn{{LeftField: "customer_id", RightField: "id"}},
		},
	})
	if err != nil {
		t.Fatalf("Marshal join plan ranges: %v", err)
	}
	if !bytes.Contains(joinBody, []byte(`"left_ranges":[{"end":"row:orders;","start":"row:orders:"}]`)) ||
		!bytes.Contains(joinBody, []byte(`"right_ranges":[{"end":"row:customers;","start":"row:customers:"}]`)) {
		t.Fatalf("Marshal omitted join ranges: %s", joinBody)
	}

	lateralBody, err := json.Marshal(RowsLateralPlanRequest{
		LeftRanges:  []RowsDocKeyRange{{Start: "row:org:", End: "row:org;"}},
		RightRanges: []RowsDocKeyRange{{Start: "row:bal:", End: "row:bal;"}},
		Lateral: RowsLateralRequest{
			Left:         RowsQueryRequest{},
			Right:        RowsQueryRequest{Limit: 1},
			Correlations: []RowsLateralCorrelation{{LeftField: "id", RightField: "organization_id"}},
		},
	})
	if err != nil {
		t.Fatalf("Marshal lateral plan ranges: %v", err)
	}
	if !bytes.Contains(lateralBody, []byte(`"left_ranges":[{"end":"row:org;","start":"row:org:"}]`)) ||
		!bytes.Contains(lateralBody, []byte(`"right_ranges":[{"end":"row:bal;","start":"row:bal:"}]`)) {
		t.Fatalf("Marshal omitted lateral ranges: %s", lateralBody)
	}
}

func TestRowsQueryRequestMarshalPreservesTypedWhereTree(t *testing.T) {
	body, err := json.Marshal(RowsQueryPlanRequest{
		Query: RowsQueryRequest{
			Where: rowsWhereGroup(RowsWhere1{
				All: []RowsWhereAtom{
					{Field: "status", Op: RowsWhereAtomOp("eq"), Value: "ready"},
					{Field: "tags", Op: RowsWhereAtomOp("array_contains"), Value: "paid"},
					{Field: "metadata", Op: RowsWhereAtomOp("json_path_eq"), Path: []string{"billing", "plan"}, Value: "pro"},
					{Field: "email", Op: RowsWhereAtomOp("text_pattern"), Pattern: "%@example.test", CaseInsensitive: true},
				},
				Any: []RowsWhereBranch{
					rowsWhereBranchAtom("priority", "gt", 10),
					rowsWhereBranchAtom("tags", "array_contains", "paid"),
					rowsWhereBranchAll([]RowsWhereAtom{
						{Field: "tier", Op: RowsWhereAtomOp("eq"), Value: "enterprise"},
						{Field: "metadata", Op: RowsWhereAtomOp("json_path_eq"), Path: []string{"billing", "plan"}, Value: "pro"},
					}),
				},
				Not: []RowsWhereBranch{rowsWhereBranchAtom("tags", "array_contains", "cold")},
			}),
		},
	})
	if err != nil {
		t.Fatalf("Marshal typed where tree: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"op":"array_contains"`),
		[]byte(`"path":["billing","plan"]`),
		[]byte(`"pattern":"%@example.test"`),
		[]byte(`"any":[{"field":"priority","op":"gt","value":10},{"field":"tags","op":"array_contains","value":"paid"},{"all":[{"field":"tier","op":"eq","value":"enterprise"},{"field":"metadata","op":"json_path_eq","path":["billing","plan"],"value":"pro"}]}]`),
		[]byte(`"not":[{"field":"tags","op":"array_contains","value":"cold"}]`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted where field %s: %s", want, body)
		}
	}
}

func TestRowsQueryRequestMarshalPreservesTypedCompactProjections(t *testing.T) {
	body, err := json.Marshal(RowsQueryPlanRequest{
		Query: RowsQueryRequest{
			Select: []string{"id"},
			JsonExtract: []RowsJsonExtractProjection{
				{As: "plan", Field: "metadata", Path: []string{"billing", "plan"}, AsText: true},
			},
			ArrayLength: []RowsArrayLengthProjection{
				{As: "tag_count", Field: "tags"},
			},
			ExpressionArrayContains: []RowsExpressionArrayContainsPredicate{
				{Expr: rowsExprOp("string_to_array", []RowsExpression{rowsExprField("scope")}), Value: []interface{}{"read"}},
			},
			Coalesce: []RowsCoalesceProjection{
				{
					As: "name_or_email",
					Operands: []RowsCoalesceOperand{
						rowsCoalesceField("display_name"),
						rowsCoalesceField("email"),
						rowsCoalesceValue("unknown"),
					},
				},
			},
			FieldAliases: []RowsFieldAliasProjection{
				{As: "raw_id", Field: "id"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Marshal query compact projections: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"json_extract":[{"as":"plan","as_text":true,"field":"metadata","path":["billing","plan"]}]`),
		[]byte(`"array_length":[{"as":"tag_count","field":"tags"}]`),
		[]byte(`"expression_array_contains":[{"expr":{"args":[{"field":"scope"}],"op":"string_to_array"},"value":["read"]}]`),
		[]byte(`"coalesce":[{"as":"name_or_email","operands":[{"field":"display_name"},{"field":"email"},{"value":"unknown"}]}]`),
		[]byte(`"field_aliases":[{"as":"raw_id","field":"id"}]`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted compact projection %s: %s", want, body)
		}
	}
}

func TestRowsBatchRequestMarshalPreservesTypedConflictAndTransforms(t *testing.T) {
	body, err := json.Marshal(RowsBatchRequest{
		Operations: []RowOperation{{
			Op:  RowOperationOp("insert"),
			Row: RowsRowDocument{"id": "u2", "email": "ada@example.test", "status": "active"},
			OnConflict: RowsOnConflict{
				Target: RowsConflictTarget{
					Unique: RowsConflictUniqueTarget{
						Name: "usage_records_active_email_key",
						Where: RowsUniquePredicateGroup{All: []RowsUniquePredicate{{
							Field: "status",
							Op:    RowsUniquePredicateOp("eq"),
							Value: "active",
						}}},
					},
				},
				Action:    RowsOnConflictAction("update"),
				Patch:     RowsFieldPatch{"status": "active"},
				Increment: RowsNumericIncrement{"amount": 2.5},
				PatchExpr: RowsExpressionAssignmentMap{
					"status_key": rowsExprOp("lower", []RowsExpression{rowsExprFieldSource("status", "proposed")}),
				},
				JsonSet: []RowsJsonSetTransform{{
					Field: "metadata",
					Path:  []string{"billing", "plan"},
					Value: "pro",
				}, {
					Field: "metadata",
					Path:  []string{"billing", "status_key"},
					Expr:  rowsExprOp("lower", []RowsExpression{rowsExprFieldSource("status", "proposed")}),
				}},
				ArrayUpdate: []RowsArrayUpdateTransform{{
					Field: "tags",
					Op:    RowsArrayUpdateTransformOp("add_to_set"),
					Value: "paid",
				}},
				WhereExpression: RowsExpressionCondition{
					Lhs: rowsExprFieldSource("status", "proposed"),
					Op:  RowsExpressionConditionOp("is_not_null"),
				},
			},
			Returning:            []string{"id", "status"},
			ReturningExpressions: []RowsExpressionProjection{{As: "status_label", Expr: rowsExprField("status")}},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal row batch conflict: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"on_conflict"`),
		[]byte(`"usage_records_active_email_key"`),
		[]byte(`"json_set"`),
		[]byte(`"value":"pro"`),
		[]byte(`"path":["billing","status_key"]`),
		[]byte(`"expr":{"args":[{"field":"status","source":"proposed"}],"op":"lower"}`),
		[]byte(`"array_update":[{"field":"tags","op":"add_to_set","value":"paid"}]`),
		[]byte(`"returning_expressions"`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted conflict field %s: %s", want, body)
		}
	}
}

func TestRowsExpressionConditionContracts(t *testing.T) {
	body, err := json.Marshal(RowsExpressionConditionGroup{
		All: []RowsExpressionCondition{{
			Lhs: rowsExprField("status"),
			Op:  RowsExpressionConditionOp("eq"),
			Rhs: rowsExprValue("ready"),
		}},
	})
	if err != nil {
		t.Fatalf("Marshal expression condition group: %v", err)
	}
	if !bytes.Equal(body, []byte(`{"all":[{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"ready"}}]}`)) {
		t.Fatalf("Marshal expression condition group mismatch: %s", body)
	}

	nullTest, err := json.Marshal(RowsExpressionCondition{
		Lhs: rowsExprField("status"),
		Op:  RowsExpressionConditionOp("is_not_null"),
	})
	if err != nil {
		t.Fatalf("Marshal expression null-test condition: %v", err)
	}
	if !bytes.Equal(nullTest, []byte(`{"lhs":{"field":"status"},"op":"is_not_null"}`)) {
		t.Fatalf("Marshal expression null-test condition mismatch: %s", nullTest)
	}

}

func TestRowsMutationSourceRequestMarshalPreservesClaimAndExpressions(t *testing.T) {
	body, err := json.Marshal(RowsMutationSourceRequest{
		Op: RowsMutationSourceRequestOp("update"),
		Source: RowsQueryRequest{
			Where: rowsWhereAtom("status", RowsWhereOp("eq"), "ready"),
			RowClaim: RowsRowClaim{
				Mode:          RowsRowClaimMode("for_update"),
				OwnerId:       "worker:1",
				TransactionId: "00112233445566778899aabbccddeeff",
				SkipLocked:    true,
				LeaseMs:       45000,
			},
			OrderBy: []RowsQueryOrder{rowsOrderField("created_at", "asc")},
			Limit:   5,
		},
		PatchExpr: map[string]RowsExpression{
			"status": rowsExprOperator(RowsExpressionOperator{
				Op: RowsExpressionOperatorOp("case"),
				Cases: []RowsExpressionCaseBranch{
					{
						When: RowsExpressionCondition{
							Lhs: rowsExprField("status"),
							Op:  RowsExpressionConditionOp("eq"),
							Rhs: rowsExprValue("ready"),
						},
						Then: rowsExprValue("claimed:ready"),
					},
				},
				Else: ptrRowsExpression(rowsExprOp("concat", []RowsExpression{
					rowsExprValue("claimed:"),
					rowsExprField("status"),
				})),
			}),
		},
		JsonSet: []RowsJsonSetTransform{{
			Field: "metadata",
			Path:  []string{"claim", "status_key"},
			Expr:  rowsExprOp("lower", []RowsExpression{rowsExprFieldSource("status", "existing")}),
		}},
		Returning:            []string{"id", "status"},
		ReturningExpressions: []RowsExpressionProjection{{As: "status_label", Expr: rowsExprField("status")}},
	})
	if err != nil {
		t.Fatalf("Marshal mutation source: %v", err)
	}
	if !bytes.Contains(body, []byte(`"transaction_id":"00112233445566778899aabbccddeeff"`)) {
		t.Fatalf("Marshal omitted row claim transaction: %s", body)
	}
	if !bytes.Contains(body, []byte(`"lease_ms":45000`)) {
		t.Fatalf("Marshal omitted row claim lease: %s", body)
	}
	if !bytes.Contains(body, []byte(`"patch_expr"`)) {
		t.Fatalf("Marshal omitted patch expression: %s", body)
	}
	if !bytes.Contains(body, []byte(`"json_set"`)) ||
		!bytes.Contains(body, []byte(`"path":["claim","status_key"]`)) ||
		!bytes.Contains(body, []byte(`"expr":{"args":[{"field":"status","source":"existing"}],"op":"lower"}`)) {
		t.Fatalf("Marshal omitted json_set expression: %s", body)
	}
	if !bytes.Contains(body, []byte(`"returning_expressions"`)) {
		t.Fatalf("Marshal omitted returning expressions: %s", body)
	}

	resultBody, err := json.Marshal(RowsMutationSourceResultSet{
		Matched:   2,
		Staged:    1,
		Returning: []map[string]interface{}{{"id": "r1", "status": "claimed:ready"}},
	})
	if err != nil {
		t.Fatalf("Marshal mutation source result: %v", err)
	}
	if !bytes.Contains(resultBody, []byte(`"staged":1`)) {
		t.Fatalf("Marshal omitted staged count: %s", resultBody)
	}
	if !bytes.Contains(resultBody, []byte(`"status":"claimed:ready"`)) {
		t.Fatalf("Marshal omitted returning row: %s", resultBody)
	}
}

func TestRowsJoinedMutationSourceRequestMarshalPreservesTypedContract(t *testing.T) {
	body, err := json.Marshal(RowsJoinedMutationSourceRequest{
		Op:          RowsJoinedMutationSourceRequestOp("update"),
		SourceTable: "source_records",
		TargetSide:  RowsJoinedMutationSourceRequestTargetSide("left"),
		Join: RowsJoinRequest{
			Left: RowsQueryRequest{
				Where: rowsWhereAtom("status", RowsWhereOp("eq"), "ready"),
				RowClaim: RowsRowClaim{
					Mode:          RowsRowClaimMode("for_update"),
					OwnerId:       "worker:joined",
					TransactionId: "00112233445566778899aabbccddeeff",
					SkipLocked:    true,
				},
			},
			Right: RowsQueryRequest{
				Where: rowsWhereAtom("source_status", RowsWhereOp("eq"), "source"),
			},
			On:      []RowsJoinOn{{LeftField: "source_id", RightField: "source_pk"}},
			OrderBy: []RowsQueryOrder{rowsOrderField("amount", "desc")},
			Limit:   5,
		},
		SourceAssignments: []RowsJoinedMutationSourceAssignment{{
			TargetField: "quantity",
			Side:        RowsJoinedMutationSourceAssignmentSide("right"),
			Field:       "source_quantity",
		}},
		Patch: RowsFieldPatch{"status": "synced"},
		PatchExpr: RowsExpressionAssignmentMap{
			"status_key": rowsExprOp("lower", []RowsExpression{rowsExprField("status")}),
		},
		Returning:            []string{"id", "quantity"},
		ReturningExpressions: []RowsExpressionProjection{{As: "status_key", Expr: rowsExprField("status_key")}},
	})
	if err != nil {
		t.Fatalf("Marshal joined mutation source: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"source_table":"source_records"`),
		[]byte(`"target_side":"left"`),
		[]byte(`"row_claim":{"mode":"for_update","owner_id":"worker:joined","skip_locked":true,"transaction_id":"00112233445566778899aabbccddeeff"}`),
		[]byte(`"source_assignments":[{"field":"source_quantity","side":"right","target_field":"quantity"}]`),
		[]byte(`"patch_expr":{"status_key":{"args":[{"field":"status"}],"op":"lower"}}`),
		[]byte(`"returning_expressions":[{"as":"status_key","expr":{"field":"status_key"}}]`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted joined mutation-source field %s: %s", want, body)
		}
	}
}

func TestRowsInsertSourceRequestMarshalPreservesTypedContract(t *testing.T) {
	body, err := json.Marshal(RowsInsertSourceRequest{
		Op:          RowsInsertSourceRequestOp("insert"),
		SourceTable: "archived_records",
		Source: RowsQueryRequest{
			Where:   rowsWhereAtom("status", RowsWhereOp("eq"), "ready"),
			OrderBy: []RowsQueryOrder{rowsOrderField("amount", "desc")},
			Limit:   5,
		},
		Assignments: []RowsInsertSourceAssignment{
			{TargetField: "id", Expr: rowsExprField("source_id")},
			{TargetField: "status", Expr: rowsExprOp("lower", []RowsExpression{rowsExprField("status")})},
			{TargetField: "amount", Expr: rowsExprOp("add", []RowsExpression{rowsExprField("amount"), rowsExprValue(float64(1))})},
		},
		OnConflict: RowsOnConflict{
			Target: RowsConflictTarget{Primary: true},
			Action: RowsOnConflictAction("nothing"),
		},
		Returning:            []string{"id", "status"},
		ReturningExpressions: []RowsExpressionProjection{{As: "amount_plus_one", Expr: rowsExprOp("add", []RowsExpression{rowsExprField("amount"), rowsExprValue(float64(1))})}},
	})
	if err != nil {
		t.Fatalf("Marshal insert source: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"op":"insert"`),
		[]byte(`"source_table":"archived_records"`),
		[]byte(`"where":{"field":"status","op":"eq","value":"ready"}`),
		[]byte(`"assignments":[{"expr":{"field":"source_id"},"target_field":"id"},{"expr":{"args":[{"field":"status"}],"op":"lower"},"target_field":"status"},{"expr":{"args":[{"field":"amount"},{"value":1}],"op":"add"},"target_field":"amount"}]`),
		[]byte(`"on_conflict":{"action":"nothing","target":{"primary":true`),
		[]byte(`"returning_expressions":[{"as":"amount_plus_one","expr":{"args":[{"field":"amount"},{"value":1}],"op":"add"}}]`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted insert-source field %s: %s", want, body)
		}
	}
}
