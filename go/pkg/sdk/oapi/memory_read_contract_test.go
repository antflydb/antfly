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

package oapi

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// These exact responses are produced and checked by the native policy suite.
func nativeMemoryResponse(t *testing.T, name string) *http.Response {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "zig", "pkg", "antfly-embedded", "src", "memory", "fixtures", "wire", name+".json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}
}

func TestGeneratedMemoryReadsDecodeNativeServiceResponses(t *testing.T) {
	candidates, err := ParseGetMemoryEntityCandidatesResponse(nativeMemoryResponse(t, "candidates"))
	if err != nil {
		t.Fatal(err)
	}
	page := candidates.JSON200
	if page == nil || len(page.Items) != 1 || page.Seed.Entity.Id != "a" || page.Items[0].Entity.Entity.Id != "b" || page.Items[0].View.CanonicalId != "b" || page.Revision != 3 || page.NextCursor != nil {
		t.Fatalf("candidate response lost discovery data: %#v", page)
	}
	proposals, err := ParseListMemoryEntityProposalsResponse(nativeMemoryResponse(t, "proposals"))
	if err != nil {
		t.Fatal(err)
	}
	if proposals.JSON200 == nil || len(proposals.JSON200.Items) != 1 {
		t.Fatalf("missing proposal page: %#v", proposals.JSON200)
	}
	details, err := ParseGetMemoryEntityProposalResponse(nativeMemoryResponse(t, "proposal"))
	if err != nil {
		t.Fatal(err)
	}
	detail := details.JSON200
	if detail == nil || detail.Proposal.Id != proposals.JSON200.Items[0].Id || detail.Inputs.Actor != "codex/consolidation" || detail.Inputs.Entities[0].Revision != 3 || detail.StaleConfiguration || detail.Reason != nil {
		t.Fatalf("proposal response lost its review inputs: %#v", detail)
	}
}
