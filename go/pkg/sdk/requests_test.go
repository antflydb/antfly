package sdk

import (
	"bytes"
	"testing"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
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

func TestGenerateRequestMarshalPreservesDisabledPromptCache(t *testing.T) {
	body, err := json.Marshal(oapi.InferenceGenerateRequest{PromptCache: new(false)})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(body, []byte(`"prompt_cache":false`)) {
		t.Fatalf("Marshal omitted disabled prompt cache: %s", body)
	}
}
