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
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolverCurationAcceptedResponseAndEscapedKeys(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/db/v1/tables/notes/resolvers/concepts/redirects/concept%2Falpha" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"resolver":"concepts","changed":true,"revision":3,"replay_sequence":3,"replay_sequence_kind":"resolver_revision","applied":false}`))
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.UnmergeResolverKey(context.Background(), "notes", "concepts", "concept/alpha", nil, UnmergeResolverKeyJSONRequestBody{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Revision != 3 || result.Applied || calls != 1 {
		t.Fatalf("accepted replay must be returned once: %+v, calls=%d", result, calls)
	}
}
