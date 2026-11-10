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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

type memoryRequest struct {
	method string
	path   string
	header http.Header
	body   map[string]any
}

func newMemoryTestServer(t *testing.T, status int, response string) (*httptest.Server, func() []memoryRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []memoryRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("request body is not JSON: %s", raw)
			}
		}
		mu.Lock()
		seen = append(seen, memoryRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(server.Close)
	return server, func() []memoryRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]memoryRequest(nil), seen...)
	}
}

func newMemoryTestClient(t *testing.T, url string) *Client {
	t.Helper()
	client, err := NewClient(Config{BaseURL: url})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

const memoryOperationJSON = `{"action":"observe","assertions_ready":true,"attempts":1,"config_generation":1,"created_at":1,"error_code":null,"evidence_accepted":true,"id":"op-1","next_retry_at":null,"owner":"me","phase":"succeeded","request_hash":"h","revision":3}`

func TestMemorySpaceMutationsGenerateIdempotencyKeys(t *testing.T) {
	server, requests := newMemoryTestServer(t, http.StatusOK, memoryOperationJSON)
	space := newMemoryTestClient(t, server.URL).MemorySpace("agent",
		WithMemoryDatabase("db"), WithMemoryNamespace("ns"), WithMemoryBranch("feature"))
	ctx := context.Background()

	op, err := space.Remember(ctx, "Production deploys with Terraform.")
	if err != nil {
		t.Fatal(err)
	}
	if op.Id != "op-1" || op.Revision != 3 {
		t.Fatalf("operation not decoded: %#v", op)
	}
	if _, err := space.Outcome(ctx, oapi.MemoryOutcome{Id: "o1", TaskId: "t1", Success: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Pin(ctx, "deploy-terraform", "needed"); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Unpin(ctx, "deploy-terraform", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Correct(ctx, oapi.MemoryCorrection{Id: "a", Reason: "wrong"}); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Forget(ctx, oapi.MemoryForgetRequest{EvidenceIds: []string{"e"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Observe(ctx, oapi.MemoryObservationRequest{Evidence: []oapi.MemoryEvidence{{Id: "e", Text: "x"}}}, WithIdempotencyKey("explicit-key")); err != nil {
		t.Fatal(err)
	}

	got := requests()
	wantPaths := []string{"observations", "outcomes", "pins", "pins", "corrections", "forget", "observations"}
	if len(got) != len(wantPaths) {
		t.Fatalf("got %d requests, want %d", len(got), len(wantPaths))
	}
	keys := map[string]bool{}
	for i, req := range got {
		if req.method != http.MethodPost || req.path != "/memory/v1/spaces/agent/"+wantPaths[i] {
			t.Errorf("request %d: %s %s", i, req.method, req.path)
		}
		if req.header.Get("X-Antfly-Database") != "db" || req.header.Get("X-Antfly-Namespace") != "ns" || req.header.Get("X-Antfly-Memory-Branch") != "feature" {
			t.Errorf("request %d missing scope headers: %v", i, req.header)
		}
		key := req.header.Get("Idempotency-Key")
		if key == "" || keys[key] {
			t.Errorf("request %d idempotency key %q is empty or reused", i, key)
		}
		keys[key] = true
	}
	if key := got[len(got)-1].header.Get("Idempotency-Key"); key != "explicit-key" {
		t.Errorf("explicit idempotency key not passed through: %q", key)
	}

	evidence := got[0].body["evidence"].([]any)[0].(map[string]any)
	if evidence["text"] != "Production deploys with Terraform." || evidence["id"] == "" {
		t.Errorf("remember evidence: %#v", evidence)
	}
	if used, ok := got[1].body["used_memory_ids"].([]any); !ok || len(used) != 0 {
		t.Errorf("outcome must send used_memory_ids: %#v", got[1].body)
	}
	if got[2].body["pinned"] != true || got[2].body["reason"] != "needed" || got[3].body["pinned"] != false {
		t.Errorf("pin bodies: %#v / %#v", got[2].body, got[3].body)
	}
}

func TestMemorySpaceRememberEvidenceIDs(t *testing.T) {
	server, requests := newMemoryTestServer(t, http.StatusOK, memoryOperationJSON)
	space := newMemoryTestClient(t, server.URL).MemorySpace("agent")
	ctx := context.Background()
	for range 2 {
		if _, err := space.Remember(ctx, "same text"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := space.Remember(ctx, "same text", WithEvidenceID("runbook-1"), WithSourceID("runbook")); err != nil {
		t.Fatal(err)
	}
	if _, err := space.Remember(ctx, "  "); err == nil {
		t.Fatal("empty text must be rejected")
	}
	got := requests()
	ev := func(i int) map[string]any { return got[i].body["evidence"].([]any)[0].(map[string]any) }
	if ev(0)["id"] == "" || ev(0)["id"] == ev(1)["id"] {
		t.Errorf("generated evidence ids should be unique: %v vs %v", ev(0)["id"], ev(1)["id"])
	}
	if got[0].header.Get("Idempotency-Key") == got[1].header.Get("Idempotency-Key") {
		t.Error("separate calls should get separate idempotency keys")
	}
	if ev(2)["id"] != "runbook-1" || ev(2)["source_id"] != "runbook" {
		t.Errorf("explicit evidence: %#v", ev(2))
	}
	if _, err := space.Remember(ctx, "x", WithIdempotencyKey(string(make([]byte, 257)))); err == nil {
		t.Fatal("oversized idempotency key must be rejected")
	}
}

func TestMemorySpaceReadsOmitIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	limit := uint32(4)

	server, requests := newMemoryTestServer(t, http.StatusOK, `{"complete":true,"config_generation":1,"guidance":"g","items":[],"omitted":0,"revision":7,"scanned":2,"subjects":[],"token_upper_bound":10}`)
	space := newMemoryTestClient(t, server.URL).MemorySpace("agent", WithMemoryBranch("main"))
	brief, err := space.Brief(ctx, oapi.MemoryBriefRequest{Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if brief.Revision != 7 || brief.Guidance != "g" {
		t.Fatalf("brief not decoded: %#v", brief)
	}

	recallServer, recallRequests := newMemoryTestServer(t, http.StatusOK, `{"complete":true,"config_generation":1,"revision":2}`)
	weight := 0.5
	if _, err := newMemoryTestClient(t, recallServer.URL).MemorySpace("agent").Recall(ctx, oapi.MemoryRecallRequest{Query: "deploy", UtilityWeight: &weight}); err != nil {
		t.Fatal(err)
	}

	consolidateServer, consolidateRequests := newMemoryTestServer(t, http.StatusOK, `{"complete":true,"config_generation":1,"findings":[],"omitted":0,"revision":2,"scanned":0,"stats":{}}`)
	if _, err := newMemoryTestClient(t, consolidateServer.URL).MemorySpace("agent").Consolidate(ctx, oapi.MemoryConsolidationRequest{Limit: &limit}); err != nil {
		t.Fatal(err)
	}

	getServer, getRequests := newMemoryTestServer(t, http.StatusOK, `{}`)
	if _, err := newMemoryTestClient(t, getServer.URL).MemorySpace("agent").Get(ctx, "deploy-terraform"); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		req    memoryRequest
		method string
		path   string
	}{
		{requests()[0], http.MethodPost, "/memory/v1/spaces/agent/brief"},
		{recallRequests()[0], http.MethodPost, "/memory/v1/spaces/agent/recall"},
		{consolidateRequests()[0], http.MethodPost, "/memory/v1/spaces/agent/consolidation"},
		{getRequests()[0], http.MethodGet, "/memory/v1/spaces/agent/memories/deploy-terraform"},
	}
	for _, c := range checks {
		if c.req.method != c.method || c.req.path != c.path {
			t.Errorf("got %s %s, want %s %s", c.req.method, c.req.path, c.method, c.path)
		}
		if key := c.req.header.Get("Idempotency-Key"); key != "" {
			t.Errorf("%s sent Idempotency-Key %q", c.path, key)
		}
	}
	if requests()[0].header.Get("X-Antfly-Memory-Branch") != "main" || requests()[0].body["limit"] != float64(4) {
		t.Errorf("brief request: %#v", requests()[0])
	}
	if recallRequests()[0].body["utility_weight"] != 0.5 {
		t.Errorf("recall body: %#v", recallRequests()[0].body)
	}
}

func TestMemorySpaceCreate(t *testing.T) {
	server, requests := newMemoryTestServer(t, http.StatusCreated, memoryOperationJSON)
	op, err := newMemoryTestClient(t, server.URL).MemorySpace("agent").Create(context.Background(), oapi.MemorySpaceConfig{}, WithIdempotencyKey("create-agent"))
	if err != nil {
		t.Fatal(err)
	}
	req := requests()[0]
	if op.Id != "op-1" || req.method != http.MethodPut || req.path != "/memory/v1/spaces/agent" || req.header.Get("Idempotency-Key") != "create-agent" {
		t.Fatalf("create: %#v %#v", op, req)
	}
}

func TestMemorySpaceDecodesErrors(t *testing.T) {
	server, _ := newMemoryTestServer(t, http.StatusConflict, `{"code":"IdempotencyConflict","error":"key reused with a different body","retryable":false}`)
	_, err := newMemoryTestClient(t, server.URL).MemorySpace("agent").Pin(context.Background(), "a", "")
	var apiErr *MemoryAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want MemoryAPIError, got %T %v", err, err)
	}
	if apiErr.StatusCode != http.StatusConflict || apiErr.Code != "IdempotencyConflict" || apiErr.Message != "key reused with a different body" || apiErr.Retryable {
		t.Fatalf("decoded error: %#v", apiErr)
	}

	plain, _ := newMemoryTestServer(t, http.StatusBadGateway, ``)
	_, err = newMemoryTestClient(t, plain.URL).MemorySpace("agent").Recall(context.Background(), oapi.MemoryRecallRequest{})
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadGateway || apiErr.Message != "Bad Gateway" {
		t.Fatalf("non-JSON error: %#v", err)
	}
}

func TestMemoryRememberExplicitKeyRetryHasIdenticalBody(t *testing.T) {
	server, requests := newMemoryTestServer(t, http.StatusOK, memoryOperationJSON)
	space := newMemoryTestClient(t, server.URL).MemorySpace("agent")
	for range 2 {
		if _, err := space.Remember(context.Background(), "same text", WithIdempotencyKey("retry")); err != nil {
			t.Fatal(err)
		}
	}
	got := requests()
	first, _ := json.Marshal(got[0].body)
	retry, _ := json.Marshal(got[1].body)
	if string(first) != string(retry) || got[0].header.Get("Idempotency-Key") != got[1].header.Get("Idempotency-Key") {
		t.Fatalf("retry changed the immutable evidence: %s vs %s", first, retry)
	}
}

func TestMemoryUsageAndReviewProposalTransport(t *testing.T) {
	server, requests := newMemoryTestServer(t, http.StatusOK, memoryOperationJSON)
	space := newMemoryTestClient(t, server.URL).MemorySpace("agent", WithMemoryBranch("feature"))
	ctx := context.Background()
	if _, err := space.Access(ctx, []string{"a"}, WithIdempotencyKey("retrieval")); err != nil {
		t.Fatal(err)
	}
	if _, err := space.PrepareConsolidation(ctx, oapi.MemoryConsolidationRequest{}, WithIdempotencyKey("prepare")); err != nil {
		t.Fatal(err)
	}
	seen := requests()
	if seen[0].path != "/memory/v1/spaces/agent/accesses" || seen[1].path != "/memory/v1/spaces/agent/consolidation-proposals" {
		t.Fatalf("unexpected routes: %+v", seen)
	}
	if seen[0].header.Get("Idempotency-Key") != "retrieval" || seen[1].header.Get("X-Antfly-Memory-Branch") != "feature" {
		t.Fatalf("missing keys or scope: %+v", seen)
	}
}
