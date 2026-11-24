// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestIndexPublicationRecoveryPublicTypes(t *testing.T) {
	const body = `{"index_type":"relational","index_name":"by_email","publication_token":"recovery-handle","table_id":"18446744073709551615","publication_id":"22222222222222222222222222222222","plan_digest":"3333333333333333333333333333333333333333333333333333333333333333","metadata_incarnation":"11111111111111111111111111111111","state":"admission_unknown","replication_pending":true,"phase":"published","owner_count":2}`
	var status IndexPublicationStatus
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatal(err)
	}
	if status.State != IndexPublicationStateAdmissionUnknown || status.Phase != IndexPublicationPhasePublished || !status.ReplicationPending {
		t.Fatalf("unmirrored historical publication must remain unknown: %+v", status)
	}
	if status.TableId != "18446744073709551615" {
		t.Fatalf("table identity lost precision: %q", status.TableId)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip IndexPublicationStatus
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || roundTrip != status {
		t.Fatalf("publication round trip: %v, %+v", err, roundTrip)
	}
	var receipt SQLDDLReceipt
	if err := json.Unmarshal([]byte(`{"index_publication_token":"recovery-handle","fk_generation_publication_id":"22222222222222222222222222222222"}`), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.IndexPublicationToken != status.PublicationToken || receipt.FkGenerationPublicationId != status.PublicationId {
		t.Fatalf("receipt lost recovery identities: %+v", receipt)
	}
}

func TestIndexPublicationRecoveryReadsExactScopeAndNeverReplays(t *testing.T) {
	token := strings.Repeat("A", 171)
	calls := 0
	mode := "valid"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		want := "/db/v1/tables/items%2Fa/indexes/by%20id%2F%23/publication"
		if calls == 2 {
			want = "/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/indexes/by%20id%2F%23/publication"
		}
		if r.Method != "GET" || r.URL.EscapedPath() != want || r.URL.RawQuery != "" || r.Header.Get("X-Antfly-Index-Publication-Token") != token {
			t.Errorf("unexpected recovery request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		if mode == "oversize" {
			_, _ = w.Write([]byte(strings.Repeat("x", 16385)))
			return
		}
		status := IndexPublicationStatus{IndexType: oapi.IndexPublicationStatusIndexTypeRelational, IndexName: "by id/#", PublicationToken: token, TableId: "18446744073709551615", State: IndexPublicationStateAdmissionUnknown, Phase: IndexPublicationPhasePublished, ReplicationPending: true}
		if mode == "foreign" {
			status.PublicationToken = strings.Repeat("B", 171)
		}
		if mode == "unconfirmed" {
			status.State = IndexPublicationStatePublished
		}
		if mode == "invalid_state" {
			status.State = "unknown"
			status.ReplicationPending = false
		}
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()
	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	status, err := client.GetIndexPublication(ctx, "items/a", "by id/#", token)
	if err != nil || status.State != IndexPublicationStateAdmissionUnknown {
		t.Fatalf("exact recovery: %v, %+v", err, status)
	}
	if _, err := client.GetNamespaceTableIndexPublication(ctx, "warehouse", "sales", "items/a", "by id/#", token); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetIndexPublication(ctx, "items/a", "by id/#", "bad"); err == nil || calls != 2 {
		t.Fatalf("invalid token must fail before IO: calls=%d, error=%v", calls, err)
	}
	for _, failure := range []string{"foreign", "unconfirmed", "invalid_state", "oversize"} {
		mode = failure
		before := calls
		if _, err := client.GetIndexPublication(ctx, "items/a", "by id/#", token); err == nil || calls != before+1 {
			t.Fatalf("%s: expected one failed read, calls=%d, error=%v", mode, calls, err)
		}
	}
}
