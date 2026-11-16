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

package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/docsaf/reading"
)

func TestReaderOutputBudgetPrecedenceOnWire(t *testing.T) {
	for _, generate := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			configured int
			opts       *reading.ReadOptions
			want       int
		}{
			{name: "model default"},
			{name: "empty options preserve model default", opts: &reading.ReadOptions{}},
			{name: "configured default", configured: 4096, want: 4096},
			{name: "empty options use configured default", configured: 4096, opts: &reading.ReadOptions{}, want: 4096},
			{name: "request override", configured: 4096, opts: &reading.ReadOptions{MaxTokens: 8192}, want: 8192},
			{name: "small explicit request", opts: &reading.ReadOptions{MaxTokens: 64}, want: 64},
		} {
			t.Run(fmt.Sprintf("generate=%v/%s", generate, tc.name), func(t *testing.T) {
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					var body map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					raw, present := body["max_tokens"]
					if tc.want == 0 {
						if present {
							t.Errorf("max_tokens must be omitted, got %s", raw)
						}
					} else {
						var got int
						if err := json.Unmarshal(raw, &got); err != nil || got != tc.want {
							t.Errorf("max_tokens = %s, want %d", raw, tc.want)
						}
					}
					var model string
					if err := json.Unmarshal(body["model"], &model); err != nil {
						t.Error(err)
					}
					w.Header().Set("Content-Type", "application/json")
					if model == "unavailable" {
						w.WriteHeader(http.StatusNotFound)
						fmt.Fprint(w, `{"error":"MODEL_NOT_FOUND"}`)
						return
					}
					if generate {
						fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"page complete"}}]}`)
					} else {
						fmt.Fprint(w, `{"data":[{"text":"page complete"}]}`)
					}
				}))
				defer server.Close()
				cfg := AntflyConfig{BaseURL: server.URL, Models: []string{"unavailable", "selected-model"}, DefaultMaxTokens: tc.configured}
				var reader reading.Reader
				var err error
				if generate {
					reader, err = NewAntflyGenerateReader(cfg)
				} else {
					reader, err = NewAntflyReadReader(cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				result, err := reader.Read(context.Background(), []reading.BinaryContent{{MIMEType: "image/png", Data: []byte("synthetic image")}}, tc.opts)
				if err != nil {
					t.Fatal(err)
				}
				if len(result) != 1 || result[0] != "page complete" {
					t.Fatalf("unexpected result: %v", result)
				}
				if calls != 2 {
					t.Fatalf("got %d calls; expected fallback to second model", calls)
				}
			})
		}
	}
}
