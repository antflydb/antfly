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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func googleStreamTestRequest() (Request, Item) {
	return Request{Config: SourceConfig(`{"bucket":"docs"}`)}, Item{Path: "data.txt", Size: 5, Revision: "7", Metadata: map[string]any{"bucket": "docs"}}
}

func TestGoogleStreamBoundsHeadersAndBlockedReads(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "body" {
					w.Header().Set("Content-Length", "5")
					w.Header().Set("X-Goog-Generation", "7")
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 100 * time.Millisecond
			connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: client, APIBase: server.URL}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			request, item := googleStreamTestRequest()
			body, err := connector.OpenSource(ctx, request, item)
			if phase == "body" {
				require.NoError(t, err)
				_, err = io.ReadAll(body)
				_ = body.Close()
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err(), "provider idle deadline must beat whole-object deadline")
		})
	}
}

func TestGoogleStreamAllowsLongProgressAndConsumerBackpressure(t *testing.T) {
	next := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("X-Goog-Generation", "7")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		for range 5 {
			select {
			case <-next:
			case <-r.Context().Done():
				return
			}
			fmt.Fprint(w, "x")
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 100 * time.Millisecond
	connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: client, APIBase: server.URL}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request, item := googleStreamTestRequest()
	body, err := connector.OpenSource(ctx, request, item)
	require.NoError(t, err)
	defer body.Close()
	// Pausing the consumer is backpressure, not an inactive provider read.
	time.Sleep(2 * client.Timeout)
	data := make([]byte, 5)
	for i := range data {
		next <- struct{}{}
		_, err := io.ReadFull(body, data[i:i+1])
		require.NoError(t, err)
	}
	require.Equal(t, "xxxxx", string(data))
	require.NoError(t, body.Close())
}

func TestGoogleStreamCancellationAndDefaultBudget(t *testing.T) {
	budget := newGoogleStreamBudget(t.Context(), 0)
	require.Equal(t, time.Minute, budget.timeout)
	budget.close()
	require.ErrorIs(t, budget.begin(), context.Canceled)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("X-Goog-Generation", "7")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: server.Client(), APIBase: server.URL}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request, item := googleStreamTestRequest()
	body, err := connector.OpenSource(ctx, request, item)
	require.NoError(t, err)
	defer body.Close()
	cancel()
	_, err = io.ReadAll(body)
	require.ErrorIs(t, err, context.Canceled)
}

type googleStreamBenchmarkReader struct{}

func (googleStreamBenchmarkReader) Read(out []byte) (int, error) { clear(out); return len(out), nil }

func BenchmarkGoogleStreamRead(b *testing.B) {
	const chunk = 32 << 10
	size := int64(b.N) * chunk
	client := &http.Client{Transport: previewDownloadTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: size, Header: http.Header{"X-Goog-Generation": []string{"7"}}, Body: io.NopCloser(googleStreamBenchmarkReader{})}, nil
	}), Timeout: time.Minute}
	connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: client}
	request, item := googleStreamTestRequest()
	item.Size = size
	body, err := connector.OpenSource(b.Context(), request, item)
	if err != nil {
		b.Fatal(err)
	}
	buffer := make([]byte, chunk)
	b.ReportAllocs()
	b.SetBytes(chunk)
	b.ResetTimer()
	for range b.N {
		if _, err := io.ReadFull(body, buffer); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := body.Close(); err != nil {
		b.Fatal(err)
	}
}
