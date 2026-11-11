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

package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestClientKeepsExplicitMountAndDoesNotRetryOrFollowRedirects(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer management-token" || r.Header.Get("Idempotency-Key") != "submission-1" {
			t.Error("missing management authorization or idempotency key")
		}
		if r.URL.Path != "/tenant/instance/sources/v1/sources/source/runs" {
			t.Errorf("lost explicit mount: %s", r.URL.Path)
		}
		w.Header().Set("Location", "https://other.example/private")
		w.WriteHeader(302)
	}))
	defer server.Close()
	client, err := NewClient(server.URL+"/tenant/instance/sources/v1", "management-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.StartRun(context.Background(), "source", "submission-1", RunInput{})
	var failure *HTTPError
	if !errors.As(err, &failure) || failure.Status != 302 {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("mutation retried %d times", calls)
	}
}

func TestClientRejectsUnsafeEndpoints(t *testing.T) {
	for _, endpoint := range []string{"http://remote.example/sources/v1", "https://user:secret@example.com/sources/v1", "https://example.com?token=secret"} {
		if _, err := NewClient(endpoint, "token"); err == nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}

type bodyFailureTransport struct{ failure error }

func (b bodyFailureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: bodyFailureReader{b.failure}, Request: r}, nil
}

type bodyFailureReader struct{ failure error }

func (b bodyFailureReader) Read([]byte) (int, error) { return 0, b.failure }
func (b bodyFailureReader) Close() error             { return nil }

type bodyTimeout struct{}

func (bodyTimeout) Error() string   { return "secret URL and token must not escape" }
func (bodyTimeout) Timeout() bool   { return true }
func (bodyTimeout) Temporary() bool { return true }

func TestClientClassifiesBodyReadFailures(t *testing.T) {
	for _, failure := range []error{io.ErrUnexpectedEOF, bodyTimeout{}} {
		client, err := NewClientWithOptions("https://example.com/sources/v1", Options{Token: func(context.Context) (string, error) { return "token", nil }, HTTPClient: &http.Client{Transport: bodyFailureTransport{failure}}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Capabilities(context.Background())
		var transport *TransportError
		if !errors.As(err, &transport) {
			t.Fatalf("untyped body failure: %T %v", err, err)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "example.com") {
			t.Fatalf("leaked transport detail: %v", err)
		}
		_, timedOut := failure.(bodyTimeout)
		if transport.Timeout != timedOut {
			t.Fatalf("timeout=%v, want %v", transport.Timeout, timedOut)
		}
	}
}

func TestSourceBytesBudgetsFinalRevision(t *testing.T) {
	source := Source{Id: "source", Config: map[string]any{"padding": strings.Repeat("x", 20000)}}
	size, err := SourceBytes(source)
	if err != nil {
		t.Fatal(err)
	}
	source.Revision = SourceRevision(source)
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if size != len(raw)+1 {
		t.Fatalf("budgeted %d, wire needs %d", size, len(raw)+1)
	}
	source.ReconciliationStatus = &ReconciliationStatus{State: "failed"}
	if SourceRevision(source) != source.Revision {
		t.Fatal("observation changed declaration revision")
	}
}

type budgetPageBackend struct {
	Backend
	source Source
}

func (b budgetPageBackend) ListSources(_ context.Context, limit int, cursor string) (SourcePage, error) {
	start := 0
	if cursor != "" {
		start, _ = strconv.Atoi(cursor)
	}
	page := SourcePage{Data: []Source{}}
	used := 0
	for i := start; i < 100 && len(page.Data) < limit; i++ {
		source := b.source
		source.Id = fmt.Sprintf("source-%03d", i)
		size, err := SourceBytes(source)
		if err != nil {
			return page, err
		}
		if used+size > SourcePageBudget {
			page.NextCursor = strconv.Itoa(i)
			break
		}
		used += size
		page.Data = append(page.Data, source)
	}
	return page, nil
}
func TestClientCanReadNearLimitSourcePages(t *testing.T) {
	source := Source{Id: "source-000", Config: map[string]any{"padding": ""}}
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	// Before revision stamping, exactly 100 declarations fit the backend budget.
	source.Config["padding"] = strings.Repeat("x", SourcePageBudget/100-len(raw)-1)
	server := httptest.NewServer(Handler(budgetPageBackend{source: source}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.ListSources(t.Context(), 100, "")
	if err != nil {
		t.Fatalf("first page exceeded transport limit: %v", err)
	}
	if first.NextCursor == "" || len(first.Data) == 100 {
		t.Fatal("final wire size must force pagination")
	}
	second, err := client.ListSources(t.Context(), 100, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Data)+len(second.Data) != 100 || second.NextCursor != "" {
		t.Fatal("pagination lost sources")
	}
}
