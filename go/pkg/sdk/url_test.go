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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestNormalizeBaseURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "local root", in: "http://localhost:8080", want: "http://localhost:8080"},
		{name: "local root trailing slash", in: "http://localhost:8080/", want: "http://localhost:8080"},
		{name: "local api root", in: "http://localhost:8080/db/v1", want: "http://localhost:8080"},
		{name: "local api root trailing slash", in: "http://localhost:8080/db/v1/", want: "http://localhost:8080"},
		{name: "local auth root", in: "http://localhost:8080/auth/v1", want: "http://localhost:8080"},
		{name: "local ai root", in: "http://localhost:8080/ai/v1", want: "http://localhost:8080"},
		{name: "cloud proxy root", in: "https://platform.antfly.io/cloud/v1/instance", want: "https://platform.antfly.io/cloud/v1/instance"},
		{name: "cloud proxy api root", in: "https://platform.antfly.io/cloud/v1/instance/db/v1", want: "https://platform.antfly.io/cloud/v1/instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeBaseURL(tt.in); got != tt.want {
				t.Fatalf("NormalizeBaseURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeServerURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "local root", in: "http://localhost:8080", want: "http://localhost:8080"},
		{name: "local api root", in: "http://localhost:8080/db/v1", want: "http://localhost:8080"},
		{name: "local auth root", in: "http://localhost:8080/auth/v1", want: "http://localhost:8080"},
		{name: "local ai root", in: "http://localhost:8080/ai/v1", want: "http://localhost:8080"},
		{name: "cloud proxy root", in: "https://platform.antfly.io/cloud/v1/instance", want: "https://platform.antfly.io/cloud/v1/instance"},
		{name: "cloud proxy api root", in: "https://platform.antfly.io/cloud/v1/instance/db/v1", want: "https://platform.antfly.io/cloud/v1/instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeServerURL(tt.in); got != tt.want {
				t.Fatalf("NormalizeServerURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestTokenAuthHeaderAndNormalizedPath(t *testing.T) {
	var gotPath, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(
		server.URL,
		oapi.WithHTTPClient(server.Client()),
		oapi.WithRequestEditorFn(WithToken("antflydb_test")),
	)
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	if _, err := client.ListTables(context.Background()); err != nil {
		t.Fatalf("ListTables: %v", err)
	}

	if gotPath != "/db/v1/tables" {
		t.Fatalf("path = %q, want /db/v1/tables", gotPath)
	}
	if gotAuth != "Bearer antflydb_test" {
		t.Fatalf("Authorization = %q, want Bearer antflydb_test", gotAuth)
	}
}
