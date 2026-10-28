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
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cancellationCloseBody struct {
	ctx                       context.Context
	started, canceled, retire chan struct{}
	calls                     atomic.Int32
	err                       error
}

func (b *cancellationCloseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *cancellationCloseBody) Close() error {
	b.calls.Add(1)
	close(b.started)
	<-b.ctx.Done()
	close(b.canceled)
	<-b.retire
	return b.err
}

type closeDoer func(*http.Request) (*http.Response, error)

func (f closeDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestBodyCloseCancelsBeforePhysicalRetirement(t *testing.T) {
	for _, mode := range []string{"response", "operation_error"} {
		for _, policy := range []string{"timed", "untimed", "custom"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				caller, cancelCaller := context.WithCancel(context.Background())
				defer cancelCaller()
				body := &cancellationCloseBody{
					started: make(chan struct{}), canceled: make(chan struct{}), retire: make(chan struct{}),
					err: errors.New("physical close result"),
				}
				var retirement sync.Once
				retire := func() { retirement.Do(func() { close(body.retire) }) }
				defer retire()
				var sent atomic.Int32
				transport := timeoutTransport(func(request *http.Request) (*http.Response, error) {
					sent.Add(1)
					if request.URL.Path == "/peer" {
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: http.NoBody}, nil
					}
					body.ctx = request.Context()
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
				})
				var next Doer = &http.Client{Timeout: time.Hour, Transport: transport}
				if policy == "untimed" {
					next = &http.Client{Transport: transport}
				}
				if policy == "custom" {
					next = closeDoer(transport)
				}
				client, err := New(next, &Config{1, 0, 0})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				request, _ := http.NewRequestWithContext(caller, http.MethodPost, "http://test/first", nil)
				done := make(chan error, 2)
				original := errors.New("original operation failure")
				closes := 1
				if mode == "response" {
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					closes = 2
					for range closes {
						go func() { done <- response.Body.Close() }()
					}
				} else {
					go func() {
						response, err := client.DoOperation(request, func(current *http.Request, _ Doer) (*http.Response, error) {
							body.ctx = current.Context()
							return &http.Response{StatusCode: 503, Header: make(http.Header), Body: body}, original
						})
						if response != nil {
							_ = response.Body.Close()
						}
						done <- err
					}()
				}
				select {
				case <-body.started:
				case <-time.After(time.Second):
					t.Fatal("physical retirement did not start")
				}
				select {
				case <-body.canceled:
				case <-time.After(100 * time.Millisecond):
					t.Error("Close waited for transport before canceling the owned operation context")
					cancelCaller() // Retire the old implementation after reporting its failure.
					select {
					case <-body.canceled:
					case <-time.After(time.Second):
						t.Fatal("caller cancellation did not unblock the transport")
					}
				}
				before := sent.Load()
				peer, _ := http.NewRequest(http.MethodGet, "http://test/peer", nil)
				if result, err := client.Do(peer); err == nil {
					_ = result.Body.Close()
					t.Fatal("peer acquired capacity before physical retirement")
				} else {
					var capacity *CapacityError
					if !errors.As(err, &capacity) || capacity.RequestSent {
						t.Fatalf("unexpected peer refusal: %v", err)
					}
				}
				if client.Stats().Active != 1 || sent.Load() != before {
					t.Fatal("physical retirement lost its admission lease")
				}
				select {
				case <-done:
					t.Fatal("operation completed before physical retirement")
				default:
				}
				retire()
				for range closes {
					select {
					case err := <-done:
						want := body.err
						if mode == "operation_error" {
							want = original
						}
						if !errors.Is(err, want) {
							t.Fatalf("original result changed: %v", err)
						}
					case <-time.After(time.Second):
						t.Fatal("retirement did not finish")
					}
				}
				if client.Stats().Active != 0 || body.calls.Load() != 1 {
					t.Fatal("physical retirement was not exactly once")
				}
				peer, _ = http.NewRequest(http.MethodGet, "http://test/peer", nil)
				if result, err := client.Do(peer); err != nil {
					t.Fatal(err)
				} else {
					_ = result.Body.Close()
				}
				if caller.Err() != nil {
					t.Error("body retirement canceled the caller's context")
				}
			})
		}
	}
}
