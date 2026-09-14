/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

type relationalHTTPDoer func(*http.Request) (*http.Response, error)

func (fn relationalHTTPDoer) Do(req *http.Request) (*http.Response, error) { return fn(req) }

func TestRelationalRowQueryPreservesExactInteger(t *testing.T) {
	client, err := NewAntflyClientWithOptions("http://example.invalid", oapi.WithHTTPClient(relationalHTTPDoer(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/db/v1/tables/rows/rows/query" {
			t.Fatalf("unexpected route %s", req.URL.Path)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"_id":"a","row":{"n":9223372036854775807},"version":"18446744073709551615","schema_version":7}`))}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := client.QueryRelationalRows(context.Background(), "rows", RelationalRowQueryRequest{Fields: []string{"n"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Version != "18446744073709551615" || rows[0].Row["n"] != json.Number("9223372036854775807") {
		t.Fatalf("lossy typed row response: %#v", rows)
	}
}

func TestRelationalMutationPreservesPendingOutcomeWithoutRetry(t *testing.T) {
	calls := 0
	client, err := NewAntflyClientWithOptions("http://example.invalid", oapi.WithHTTPClient(relationalHTTPDoer(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 202, Body: io.NopCloser(strings.NewReader(`{"status":"committed_pending","inserted":1,"deleted":0}`))}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.MutateRelationalRows(context.Background(), "rows", RelationalRowMutationRequest{SchemaVersion: 7, Mutations: []RelationalRowMutation{{Key: "a", ExpectedVersion: "0"}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "committed_pending" || calls != 1 {
		t.Fatalf("outcome=%#v calls=%d", result, calls)
	}
}

func TestRelationalRecoveryRoutesAndOutcomes(t *testing.T) {
	calls := 0
	client, err := NewAntflyClientWithOptions("http://example.invalid", oapi.WithHTTPClient(relationalHTTPDoer(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Path == "/db/v1/tables/rows/constraints/repair" {
			return &http.Response{StatusCode: 202, Body: io.NopCloser(strings.NewReader(`{"status":"committed_pending","inserted":1,"deleted":0}`))}, nil
		}
		if req.URL.Path != "/db/v1/tables/rows/constraints/retry" && req.URL.Path != "/db/v1/tables/rows/constraints/retire" {
			t.Fatalf("unexpected recovery route %s", req.URL.Path)
		}
		return &http.Response{StatusCode: 202, Body: io.NopCloser(strings.NewReader(`{"status":"accepted"}`))}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	repair, err := client.RepairRelationalConstraints(context.Background(), "rows", RelationalRowMutationRequest{SchemaVersion: 2, Mutations: []RelationalRowMutation{{Key: "b", ExpectedVersion: "18446744073709551615"}}})
	if err != nil || repair.Status != "committed_pending" {
		t.Fatalf("repair=%#v err=%v", repair, err)
	}
	retry, err := client.RetryRelationalConstraints(context.Background(), "rows", RelationalConstraintRetryRequest{SchemaVersion: 2})
	if err != nil || retry.Status != "accepted" || calls != 2 {
		t.Fatalf("retry=%#v err=%v calls=%d", retry, err, calls)
	}
	drop := true
	retirement, err := client.RetireRelationalConstraints(context.Background(), "rows", RelationalConstraintRetirementRequest{SchemaVersion: 2, Drop: drop})
	if err != nil || retirement.Status != "accepted" || calls != 3 {
		t.Fatalf("retirement=%#v err=%v calls=%d", retirement, err, calls)
	}
}
