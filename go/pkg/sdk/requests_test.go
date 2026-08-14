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

func TestQueryRequestMarshalPreservesHierarchy(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: QueryHierarchy{
			Ancestors: &HierarchyAncestors{
				Source: &HierarchyProjection{Fields: []string{"title", "url"}},
			},
		},
		Fields: []string{"text"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{"ancestors":{"source":{"fields":["title","url"]}}}`)) {
		t.Fatalf("hierarchy missing from request: %s", body)
	}
}

func TestQueryRequestMarshalPreservesHierarchyGrouping(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: QueryHierarchy{
			GroupBy: &HierarchyGroupBy{
				Level: HierarchyGroupByLevelSource,
				Matches: &HierarchyMatches{
					Fields: []string{"text"},
					Limit:  3,
				},
			},
		},
		Fields: []string{"title", "url"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{"group_by":{"level":"source","matches":{"fields":["text"],"limit":3}}}`)) {
		t.Fatalf("hierarchy grouping missing from request: %s", body)
	}
}

func TestQueryRequestMarshalPreservesIdentityOnlyHierarchyProjection(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: QueryHierarchy{
			Ancestors: &HierarchyAncestors{
				Source: &HierarchyProjection{Fields: []string{}},
			},
		},
		Fields: []string{"text"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{"ancestors":{"source":{"fields":[]}}}`)) {
		t.Fatalf("identity-only hierarchy projection missing from request: %s", body)
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
