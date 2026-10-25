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
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestExplorerCancellationStopsUpstreamAndReleasesSlots(t *testing.T) {
	for _, path := range []string{"/api/search?q=broad", "/api/status", "/pdf?id=source"} {
		t.Run(path, func(t *testing.T) {
			const concurrency = 4
			started := make(chan context.Context, concurrency)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				started <- r.Context()
				select {
				case <-r.Context().Done():
					return nil, r.Context().Err()
				case <-release:
					return response(http.StatusOK, `{}`), nil
				}
			})}
			handler := explorerHandlerWithStorage("https://antfly.example/tables/test", client, "", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			deadline, _ := ctx.Deadline()
			var requests sync.WaitGroup
			defer func() { cancel(); unblock(); requests.Wait() }()
			for i := 0; i < concurrency; i++ {
				requests.Add(1)
				go func() {
					defer requests.Done()
					handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil).WithContext(ctx))
				}()
			}
			upstream := make([]context.Context, 0, concurrency)
			for i := 0; i < concurrency; i++ {
				select {
				case request := <-started:
					upstream = append(upstream, request)
					if actual, ok := request.Deadline(); !ok || !actual.Equal(deadline) {
						t.Fatal("upstream request lost the explorer deadline")
					}
				case <-time.After(time.Second):
					t.Fatal("upstream request did not start")
				}
			}
			saturated := httptest.NewRecorder()
			handler.ServeHTTP(saturated, httptest.NewRequest("GET", "/api/status", nil))
			if saturated.Code != http.StatusTooManyRequests {
				t.Fatal("fixture did not fill the admission slots")
			}
			cancel()
			for _, request := range upstream {
				select {
				case <-request.Done():
				case <-time.After(time.Second):
					t.Fatal("cancelled explorer request left its Antfly operation running")
				}
			}
			completed := make(chan struct{})
			go func() { requests.Wait(); close(completed) }()
			select {
			case <-completed:
			case <-time.After(time.Second):
				t.Fatal("cancelled request retained its admission slot")
			}
			unblock()
			available := httptest.NewRecorder()
			handler.ServeHTTP(available, httptest.NewRequest("GET", "/api/status", nil))
			if available.Code != http.StatusOK {
				t.Fatalf("cancelled requests blocked subsequent work: %d", available.Code)
			}
		})
	}
}
