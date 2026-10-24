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

package admission

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type timeoutTransport func(*http.Request) (*http.Response, error)

func (f timeoutTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestOwnedOperationDeadlineAndBodyLifetime(t *testing.T) {
	const ceiling = 90 * time.Second
	for _, mode := range []string{"owned", "typed_nil", "shorter_caller", "longer_caller", "borrowed"} {
		t.Run(mode, func(t *testing.T) {
			var observed context.Context
			transport := timeoutTransport(func(request *http.Request) (*http.Response, error) {
				observed = request.Context()
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("body"))}, nil
			})
			previous := http.DefaultTransport
			http.DefaultTransport = transport
			defer func() { http.DefaultTransport = previous }()
			var next Doer
			if mode == "typed_nil" {
				next = (*http.Client)(nil)
			} else if mode == "borrowed" {
				next = &http.Client{Transport: transport}
			}
			client, err := New(next, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx := context.Background()
			var original time.Time
			if mode == "shorter_caller" || mode == "longer_caller" {
				duration := time.Minute
				if mode == "longer_caller" {
					duration = 10 * time.Minute
				}
				original = time.Now().Add(duration)
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, original)
				defer cancel()
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://test/", nil)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			deadline, bounded := observed.Deadline()
			if mode == "borrowed" {
				if bounded {
					t.Fatalf("borrowed client acquired a timeout: %v", deadline)
				}
			} else if !bounded {
				t.Fatal("owned operation has no deadline")
			} else if mode == "shorter_caller" {
				if !deadline.Equal(original) {
					t.Fatalf("shorter caller deadline changed: %v -> %v", original, deadline)
				}
			} else if deadline.Before(started.Add(ceiling)) || deadline.After(time.Now().Add(ceiling)) {
				t.Fatalf("owned deadline is outside its operation ceiling: %v", deadline)
			}
			if observed.Err() != nil || client.Stats().Active != 1 {
				t.Fatal("response lost original operation ownership before body retirement")
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if client.Stats().Active != 0 {
				t.Fatal("body close retained admission")
			}
			if mode != "borrowed" && observed.Err() == nil {
				t.Fatal("body close did not cancel the operation context")
			}
			if ctx.Err() != nil {
				t.Fatal("body close canceled the caller context")
			}
		})
	}
}
