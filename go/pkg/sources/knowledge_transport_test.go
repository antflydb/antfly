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

package sources

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
)

type knowledgeBudgetBody struct {
	read   int
	closed bool
}

func (b *knowledgeBudgetBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	b.read += len(p)
	return len(p), nil
}
func (b *knowledgeBudgetBody) Close() error { b.closed = true; return nil }

func TestKnowledgePublisherReadsOnlyBoundedResponse(t *testing.T) {
	body := &knowledgeBudgetBody{}
	publisher := &Publisher{Endpoint: "http://127.0.0.1:8080", Client: &http.Client{Transport: collaborationTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: body}, nil
	})}}
	ctx := context.WithValue(t.Context(), responseLimitKey{}, management.MaxResponseBytes)
	raw, err := publisher.Query(ctx, "docs", []byte(`{"limit":0}`))
	require.ErrorContains(t, err, "exceeds budget")
	require.Nil(t, raw)
	require.Equal(t, management.MaxResponseBytes+1, body.read)
	require.True(t, body.closed)
	var _ io.ReadCloser = body
}
