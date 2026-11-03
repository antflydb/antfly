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

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCorpusRequestsIdentifyApplication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != "antfly-grepstein/1" {
			t.Errorf("unexpected application identity %q", r.UserAgent())
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	if _, err := corpusRequest(server.Client(), http.MethodGet, server.URL, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cloudJobRequest(server.Client(), http.MethodPost, server.URL, "fixture", map[string]any{}); err != nil {
		t.Fatal(err)
	}
}
