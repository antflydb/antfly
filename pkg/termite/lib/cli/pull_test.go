// Copyright 2025 Antfly, Inc.
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

package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
)

func TestResolveModelName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/index.json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"schemaVersion": 2,
				"models": [
					{"name": "bge-small-en-v1.5", "owner": "BAAI", "type": "embedder", "size": 1000},
					{"name": "chonky-mmbert-small-multilingual-1", "owner": "mirth", "type": "embedder", "size": 2000},
					{"name": "ambiguous-model", "owner": "owner-a", "type": "embedder", "size": 500},
					{"name": "ambiguous-model", "owner": "owner-b", "type": "reranker", "size": 600},
					{"name": "legacy-model", "type": "embedder", "size": 300},
					{"name": "mixed-model", "owner": "acme", "type": "embedder", "size": 400},
					{"name": "mixed-model", "type": "embedder", "size": 400}
				]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := modelregistry.NewClient(modelregistry.WithBaseURL(server.URL + "/v1"))

	t.Run("already qualified name passes through", func(t *testing.T) {
		resolved, err := resolveModelName(context.Background(), client, "mirth/chonky-mmbert-small-multilingual-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resolved != "mirth/chonky-mmbert-small-multilingual-1" {
			t.Errorf("got %q, want %q", resolved, "mirth/chonky-mmbert-small-multilingual-1")
		}
	})

	t.Run("bare name resolves to owner/name", func(t *testing.T) {
		resolved, err := resolveModelName(context.Background(), client, "chonky-mmbert-small-multilingual-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resolved != "mirth/chonky-mmbert-small-multilingual-1" {
			t.Errorf("got %q, want %q", resolved, "mirth/chonky-mmbert-small-multilingual-1")
		}
	})

	t.Run("unknown bare name returns error", func(t *testing.T) {
		_, err := resolveModelName(context.Background(), client, "nonexistent-model")
		if err == nil {
			t.Fatal("expected error for unknown model")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("error %q should contain 'not found'", err.Error())
		}
	})

	t.Run("ambiguous bare name returns error", func(t *testing.T) {
		_, err := resolveModelName(context.Background(), client, "ambiguous-model")
		if err == nil {
			t.Fatal("expected error for ambiguous model")
		}
		if !strings.Contains(err.Error(), "ambiguous") {
			t.Errorf("error %q should contain 'ambiguous'", err.Error())
		}
		if !strings.Contains(err.Error(), "owner-a/ambiguous-model") {
			t.Errorf("error %q should list owner-a/ambiguous-model", err.Error())
		}
		if !strings.Contains(err.Error(), "owner-b/ambiguous-model") {
			t.Errorf("error %q should list owner-b/ambiguous-model", err.Error())
		}
	})

	t.Run("legacy model with empty owner returns bare name", func(t *testing.T) {
		resolved, err := resolveModelName(context.Background(), client, "legacy-model")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resolved != "legacy-model" {
			t.Errorf("got %q, want %q", resolved, "legacy-model")
		}
	})

	t.Run("model with both owner and legacy entry is ambiguous", func(t *testing.T) {
		_, err := resolveModelName(context.Background(), client, "mixed-model")
		if err == nil {
			t.Fatal("expected error for ambiguous model")
		}
		if !strings.Contains(err.Error(), "ambiguous") {
			t.Errorf("error %q should contain 'ambiguous'", err.Error())
		}
	})

	t.Run("fetch index failure propagates error", func(t *testing.T) {
		badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer badServer.Close()

		badClient := modelregistry.NewClient(modelregistry.WithBaseURL(badServer.URL + "/v1"))
		_, err := resolveModelName(context.Background(), badClient, "some-model")
		if err == nil {
			t.Fatal("expected error for failed index fetch")
		}
		if !strings.Contains(err.Error(), "fetching registry index") {
			t.Errorf("error %q should contain 'fetching registry index'", err.Error())
		}
	})
}

func TestParseModelRef(t *testing.T) {
	tests := []struct {
		input       string
		wantName    string
		wantVariant string
	}{
		{"bge-small-en-v1.5", "bge-small-en-v1.5", ""},
		{"bge-small-en-v1.5-i8", "bge-small-en-v1.5", "i8"},
		{"bge-small-en-v1.5-f16", "bge-small-en-v1.5", "f16"},
		// Owner-qualified: variant stripped from name portion only
		{"mirth/model-name-i8", "mirth/model-name", "i8"},
		{"BAAI/bge-small-en-v1.5-f16", "BAAI/bge-small-en-v1.5", "f16"},
		// Owner-qualified without variant
		{"mirth/model-name", "mirth/model-name", ""},
		// No false positive on names that happen to end with variant-like strings
		{"my-model-v1.5", "my-model-v1.5", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, variant := parseModelRef(tt.input)
			if name != tt.wantName {
				t.Errorf("parseModelRef(%q) name = %q, want %q", tt.input, name, tt.wantName)
			}
			if variant != tt.wantVariant {
				t.Errorf("parseModelRef(%q) variant = %q, want %q", tt.input, variant, tt.wantVariant)
			}
		})
	}
}
