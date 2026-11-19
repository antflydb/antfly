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

func TestSchemaPublicationReadsExactIdentityWithoutReplayingDDL(t *testing.T) {
	token := strings.Repeat("A", 182)
	calls := 0
	mode := "valid"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.Header.Get("X-Antfly-Schema-Validation-Token") != token {
			t.Errorf("unexpected recovery request: %s %s", r.Method, r.URL)
		}
		if calls <= 2 {
			want := "/db/v1/tables/items%2Fa/schema/publication"
			if calls == 2 {
				want = "/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/schema/publication"
			}
			if r.URL.EscapedPath() != want {
				t.Errorf("wrong scope: %s", r.URL)
			}
		}
		if mode == "oversize" {
			_, _ = w.Write([]byte(strings.Repeat("x", 16385)))
			return
		}
		status := SchemaValidationStatus{SchemaValidationToken: token, TableId: "18446744073709551615", ActiveSchemaVersion: 2, State: SchemaValidationStateAdmissionUnknown}
		if mode == "foreign" {
			status.SchemaValidationToken = strings.Repeat("B", 182)
		}
		if mode == "incomplete" || mode == "negative" {
			status.State = SchemaValidationStateSucceeded
			status.Phase = oapi.SchemaValidationPhaseSucceeded
			status.OwnerCount, status.Ready, status.Installed = 1, 1, 1
			if mode == "negative" {
				status.OwnerCount, status.Ready, status.Installed, status.Released = -1, -1, -1, -1
			}
		}
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()
	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.GetSchemaValidationPublication(context.Background(), "items/a", token)
	if err != nil || status.TableId != "18446744073709551615" || status.State != SchemaValidationStateAdmissionUnknown {
		t.Fatalf("status: %+v, %v", status, err)
	}
	if _, err := client.GetNamespaceSchemaValidationPublication(context.Background(), "warehouse", "sales", "items/a", token); err != nil {
		t.Fatal(err)
	}
	for _, mode = range []string{"foreign", "oversize", "incomplete", "negative"} {
		before := calls
		if _, err := client.GetSchemaValidationPublication(context.Background(), "items/a", token); err == nil {
			t.Fatalf("accepted %s observation", mode)
		}
		if calls != before+1 {
			t.Fatalf("replayed %s request", mode)
		}
	}
	before := calls
	if _, err := client.GetSchemaValidationPublication(context.Background(), "items/a", "bad"); err == nil || calls != before {
		t.Fatal("invalid token reached transport")
	}
}
