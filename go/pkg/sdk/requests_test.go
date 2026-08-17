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

func TestQueryRequestMarshalPreservesUnitGrouping(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: &QueryHierarchy{
			GroupBy: &HierarchyGroupBy{Level: HierarchyGroupByLevelUnit},
		},
		Fields: []string{"unit_id", "unit_type"},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{"group_by":{"level":"unit"}}`)) {
		t.Fatalf("unit hierarchy grouping missing from request: %s", body)
	}
}

func TestQueryRequestMarshalPreservesHierarchyChildrenCursor(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: &QueryHierarchy{
			Children: &HierarchyChildren{
				Parent: HierarchyChildParent{
					Level: HierarchyChildParentLevelSource,
					Id:    "doc:a",
				},
				Level: HierarchyChildrenLevelUnit,
			},
		},
		Fields:      []string{},
		OrderBy:     []SortField{{Field: "_hierarchy.position"}},
		SearchAfter: []any{"opaque-position", "af1:asset:unit:page-1"},
		Limit:       20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"hierarchy":{"children":{"level":"unit","parent":{"id":"doc:a","level":"source"}}}`)) {
		t.Fatalf("hierarchy children missing from request: %s", body)
	}
	if !bytes.Contains(body, []byte(`"fields":[]`)) ||
		!bytes.Contains(body, []byte(`"order_by":[{"field":"_hierarchy.position"}]`)) ||
		!bytes.Contains(body, []byte(`"search_after":["opaque-position","af1:asset:unit:page-1"]`)) {
		t.Fatalf("hierarchy cursor projection missing from request: %s", body)
	}
}

func TestQueryRequestMarshalPreservesIdentityOnlyHierarchyGrouping(t *testing.T) {
	body, err := json.Marshal(QueryRequest{
		Hierarchy: &QueryHierarchy{
			GroupBy: &HierarchyGroupBy{Level: HierarchyGroupByLevelSource},
		},
		Fields: []string{},
		Limit:  5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"fields":[]`)) {
		t.Fatalf("identity-only group projection missing from request: %s", body)
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
		"_id":"doc:a","_score":0.8,"_distance":0.2,
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
	if hit.Distance == nil || *hit.Distance != 0.2 {
		t.Fatalf("unexpected raw vector distance: %v", hit.Distance)
	}
	if len(hit.Hierarchy.Matches) != 1 || hit.Hierarchy.Matches[0].ID != "chunk:1" {
		t.Fatalf("unexpected typed hierarchy matches: %#v", hit.Hierarchy.Matches)
	}
	if hit.Hierarchy.Evidence.LocalId != "e0" {
		t.Fatalf("unexpected typed hierarchy evidence: %#v", hit.Hierarchy.Evidence)
	}
}

func TestQueryHitDistanceDistinguishesPerfectDenseMatchFromAbsent(t *testing.T) {
	var perfectDense Hit
	if err := json.Unmarshal([]byte(`{"_id":"doc:dense","_score":1,"_distance":0}`), &perfectDense); err != nil {
		t.Fatal(err)
	}
	if perfectDense.Distance == nil || *perfectDense.Distance != 0 {
		t.Fatalf("perfect dense-match distance lost: %#v", perfectDense.Distance)
	}

	var nonDense Hit
	if err := json.Unmarshal([]byte(`{"_id":"doc:text","_score":1}`), &nonDense); err != nil {
		t.Fatal(err)
	}
	if nonDense.Distance != nil {
		t.Fatalf("absent distance became present: %#v", nonDense.Distance)
	}

	body, err := json.Marshal(perfectDense)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"_distance":0`)) {
		t.Fatalf("perfect dense-match distance omitted during marshal: %s", body)
	}
}

func TestQueryHitHierarchyPreservesPresentZeroMetadata(t *testing.T) {
	var hit Hit
	if err := json.Unmarshal([]byte(`{
		"_id":"chunk:0","_score":1,
		"hierarchy":{
			"level":"chunk",
			"artifact":{
				"name":"body","kind":"chunk","chunk_id":0,
				"source":{"name":"body","kind":"chunk","chunk_id":0}
			},
			"evidence":{"decision":"reject","confidence":0}
		}
	}`), &hit); err != nil {
		t.Fatal(err)
	}
	if hit.Hierarchy.Artifact.ChunkId == nil || *hit.Hierarchy.Artifact.ChunkId != 0 {
		t.Fatalf("present artifact chunk ID lost: %#v", hit.Hierarchy.Artifact.ChunkId)
	}
	if hit.Hierarchy.Artifact.Source.ChunkId == nil || *hit.Hierarchy.Artifact.Source.ChunkId != 0 {
		t.Fatalf("present source chunk ID lost: %#v", hit.Hierarchy.Artifact.Source.ChunkId)
	}
	if hit.Hierarchy.Evidence.Confidence == nil || *hit.Hierarchy.Evidence.Confidence != 0 {
		t.Fatalf("present zero confidence lost: %#v", hit.Hierarchy.Evidence.Confidence)
	}

	body, err := json.Marshal(hit)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(body, []byte(`"chunk_id":0`)) != 2 {
		t.Fatalf("present zero chunk IDs omitted during marshal: %s", body)
	}
	if !bytes.Contains(body, []byte(`"confidence":0`)) {
		t.Fatalf("present zero confidence omitted during marshal: %s", body)
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
