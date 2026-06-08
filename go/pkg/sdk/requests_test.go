package sdk

import (
	"bytes"
	"testing"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
)

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

func TestRowsPlanRequestMarshalPreservesTypedJoinAndLateral(t *testing.T) {
	joinBody, err := json.Marshal(RowsJoinPlanRequest{
		Ctes: []RowsCte{
			{
				Name: "open_orders",
				Query: RowsQueryRequest{
					Where: map[string]interface{}{"field": "status", "op": "eq", "value": "open"},
				},
			},
		},
		Join: RowsJoinRequest{
			Left: RowsQueryRequest{
				SourceCte: "open_orders",
				Select:    []string{"tenant_id", "order_id"},
			},
			Right: RowsQueryRequest{
				Where: map[string]interface{}{"field": "kind", "op": "eq", "value": "customer"},
				ExpressionAny: []RowsExpressionConditionGroup{{
					All: []RowsExpressionCondition{{
						Lhs: RowsExpression{Field: "status"},
						Op:  RowsExpressionConditionOp("eq"),
						Rhs: RowsExpression{Value: "active"},
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
			OrderBy: []RowsQueryOrder{{Field: "order_id", Direction: RowsQueryOrderDirection("asc")}},
			Limit:   25,
		},
	})
	if err != nil {
		t.Fatalf("Marshal join plan: %v", err)
	}
	if !bytes.Contains(joinBody, []byte(`"source_cte":"open_orders"`)) {
		t.Fatalf("Marshal omitted CTE source: %s", joinBody)
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
			Right:        RowsQueryRequest{Where: map[string]interface{}{"field": "kind", "op": "eq", "value": "event"}, Limit: 3},
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

func TestRowsAggregatePlanRequestMarshalPreservesTypedHaving(t *testing.T) {
	body, err := json.Marshal(RowsAggregatePlanRequest{
		Aggregate: RowsAggregateRequest{
			Source: RowsQueryRequest{
				Where: map[string]interface{}{"field": "status", "op": "eq", "value": "open"},
			},
			GroupBy: []string{"customer_id"},
			Aggregations: []RowsAggregateSpec{
				{Name: "amount_sum", Op: "sum", Field: "amount"},
			},
			Having: RowsAggregateHaving{All: []RowsAggregateHavingPredicate{
				{Field: "amount_sum", Op: RowsAggregateHavingPredicateOp("gt"), Value: 100},
			}},
			OrderBy: []RowsQueryOrder{{Field: "amount_sum", Direction: RowsQueryOrderDirection("desc")}},
			Limit:   10,
		},
	})
	if err != nil {
		t.Fatalf("Marshal aggregate plan: %v", err)
	}
	if !bytes.Contains(body, []byte(`"having":{"all":[{"field":"amount_sum","op":"gt","value":100}]}`)) {
		t.Fatalf("Marshal omitted typed having group: %s", body)
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
			Coalesce: []RowsCoalesceProjection{
				{
					As: "name_or_email",
					Operands: []RowsCoalesceOperand{
						{Field: "display_name"},
						{Field: "email"},
						{Value: "unknown"},
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
					"status_key": {Op: RowsExpressionOp("lower"), Args: []RowsExpression{{Field: "status", Source: RowsExpressionSource("proposed")}}},
				},
				JsonSet: []RowsJsonSetTransform{{
					Field: "metadata",
					Path:  []string{"billing", "plan"},
					Value: "pro",
				}},
				ArrayUpdate: []RowsArrayUpdateTransform{{
					Field: "tags",
					Op:    RowsArrayUpdateTransformOp("add_to_set"),
					Value: "paid",
				}},
				WhereExpression: RowsExpressionCondition{
					Lhs: RowsExpression{Field: "status", Source: RowsExpressionSource("proposed")},
					Op:  RowsExpressionConditionOp("is_not_null"),
				},
			},
			Returning:            []string{"id", "status"},
			ReturningExpressions: []RowsExpressionProjection{{As: "status_label", Expr: RowsExpression{Field: "status"}}},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal row batch conflict: %v", err)
	}
	for _, want := range [][]byte{
		[]byte(`"on_conflict"`),
		[]byte(`"usage_records_active_email_key"`),
		[]byte(`"json_set":[{"field":"metadata","path":["billing","plan"],"value":"pro"}]`),
		[]byte(`"array_update":[{"field":"tags","op":"add_to_set","value":"paid"}]`),
		[]byte(`"returning_expressions"`),
	} {
		if !bytes.Contains(body, want) {
			t.Fatalf("Marshal omitted conflict field %s: %s", want, body)
		}
	}
}

func TestRowsMutationSourceRequestMarshalPreservesClaimAndExpressions(t *testing.T) {
	body, err := json.Marshal(RowsMutationSourceRequest{
		Op: RowsMutationSourceRequestOp("update"),
		Source: RowsQueryRequest{
			Where: map[string]interface{}{"field": "status", "op": "eq", "value": "ready"},
			RowClaim: RowsRowClaim{
				Mode:          RowsRowClaimMode("for_update"),
				OwnerId:       "worker:1",
				TransactionId: "00112233445566778899aabbccddeeff",
				SkipLocked:    true,
				LeaseMs:       45000,
			},
			OrderBy: []RowsQueryOrder{{Field: "created_at", Direction: RowsQueryOrderDirection("asc")}},
			Limit:   5,
		},
		PatchExpr: map[string]RowsExpression{
			"status": {Op: RowsExpressionOp("case"), Cases: []RowsExpressionCaseBranch{{
				When: RowsExpressionCondition{
					Lhs: RowsExpression{Field: "status"},
					Op:  RowsExpressionConditionOp("eq"),
					Rhs: RowsExpression{Value: "ready"},
				},
				Then: RowsExpression{Value: "claimed:ready"},
			}}, Else: &RowsExpression{
				Op: RowsExpressionOp("concat"),
				Args: []RowsExpression{
					{Value: "claimed:"},
					{Field: "status"},
				},
			}},
		},
		Returning:            []string{"id", "status"},
		ReturningExpressions: []RowsExpressionProjection{{As: "status_label", Expr: RowsExpression{Field: "status"}}},
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
