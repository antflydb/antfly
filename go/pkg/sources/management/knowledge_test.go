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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type knowledgeTransportBackend struct {
	Backend
	calls int
	input KnowledgeRequest
}

func (b *knowledgeTransportBackend) QueryKnowledge(_ context.Context, id string, input KnowledgeRequest) (json.RawMessage, error) {
	b.calls++
	b.input = input
	return json.RawMessage(`{"evidence":18446744073709551615}`), nil
}
func TestKnowledgeTransportIsOptionalStrictAndLossless(t *testing.T) {
	b := &knowledgeTransportBackend{}
	handler := Handler(b)
	r := httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/sources/source/knowledge-query", strings.NewReader(`{"graph":"facts","entity":"people","start":["entity/alice"]}`)))
	require.Equal(t, 200, r.Code)
	require.Equal(t, 1, b.calls)
	require.Equal(t, "people", b.input.Entity)
	require.Contains(t, r.Body.String(), "18446744073709551615")
	r = httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/sources/source/knowledge-query", strings.NewReader(`{"graph":"facts","start":["alice"],"table":"other-tenant"}`)))
	require.Equal(t, 400, r.Code)
	require.Equal(t, 1, b.calls)
}
