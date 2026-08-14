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
		Hierarchy: &QueryHierarchy{
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
		Hierarchy: &QueryHierarchy{
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
		Hierarchy: &QueryHierarchy{
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

func TestQueryRequestMarshalPreservesEmptyDirectHierarchy(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: &QueryHierarchy{},
		Fields:    []string{"text"},
		Limit:     5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{}`)) {
		t.Fatalf("empty direct hierarchy missing from request: %s", body)
	}
}

func TestQueryHitUnmarshalUsesTypedHierarchy(t *testing.T) {
	var hit Hit
	err := json.Unmarshal([]byte(`{
		"_id":"doc:a","_score":0.8,
		"hierarchy":{
			"level":"source","parent_doc_key":"doc:a",
			"matches":[{"_id":"chunk:1","_score":0.7,"_source":{"text":"chunk text"}}],
			"evidence":{"local_id":"e0","decision":"match"}
		}
	}`), &hit)
	if err != nil {
		t.Fatal(err)
	}
	if hit.Hierarchy.Level != QueryHitHierarchyLevelSource {
		t.Fatalf("unexpected hierarchy level: %q", hit.Hierarchy.Level)
	}
	if len(hit.Hierarchy.Matches) != 1 || hit.Hierarchy.Matches[0].ID != "chunk:1" {
		t.Fatalf("unexpected typed hierarchy matches: %#v", hit.Hierarchy.Matches)
	}
	if hit.Hierarchy.Evidence.LocalId != "e0" {
		t.Fatalf("unexpected typed hierarchy evidence: %#v", hit.Hierarchy.Evidence)
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
