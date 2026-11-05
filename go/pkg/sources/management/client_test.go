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
	"errors"
	"net/http"
	"net/http/httptest"
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
