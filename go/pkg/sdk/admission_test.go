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

package sdk

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

type admissionDoerFunc func(*http.Request) (*http.Response, error)

func (f admissionDoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type admissionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f admissionRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type admissionTestBody struct {
	closes            atomic.Int32
	reads             atomic.Int32
	readErr, closeErr error
}

func (b *admissionTestBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	if b.readErr != nil {
		return 0, b.readErr
	}
	return 0, io.EOF
}
func (b *admissionTestBody) Close() error { b.closes.Add(1); return b.closeErr }

func admissionRequest(t *testing.T, ctx context.Context, path string) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, "GET", "http://test"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func waitAdmissionQueue(t *testing.T, c *BoundedHTTPClient, want int) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		got := c.Stats().Queued
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admission did not retain %d queued requests", want)
}
func assertAdmissionDrained(t *testing.T, c *BoundedHTTPClient) {
	t.Helper()
	stats := c.Stats()
	if stats.Active != 0 || stats.Queued != 0 {
		t.Fatalf("active=%d queued=%d", stats.Active, stats.Queued)
	}
}
func requireCapacityReason(t *testing.T, err error, reason string) {
	t.Helper()
	var capacity *ClientCapacityError
	if !errors.As(err, &capacity) || capacity.Reason != reason || capacity.RequestSent {
		t.Fatalf("capacity error=%v, want unsent %s", err, reason)
	}
}

func TestBoundedAdmissionSharedProductsFIFOAndBorrowedPool(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var accepted atomic.Int32
	release := make(chan struct{})
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	defer unblock()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/held" {
			w.Header().Set("Content-Length", "2")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_, _ = io.WriteString(w, "[]")
			return
		}
		if r.URL.Path == "/db/v1/tables" {
			_, _ = io.WriteString(w, "[]")
			return
		}
		_, _ = io.WriteString(w, `{"embedders":{},"chunkers":{},"rerankers":{},"generators":{},"recognizers":{},"extractors":{},"rewriters":{},"classifiers":{},"readers":{},"transcribers":{}}`)
	}))
	server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepted.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	defer unblock()
	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client(), RequestAdmission: &RequestAdmissionConfig{1, 2, 2 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.antfly.admission != client.inference.admission {
		t.Fatal("product surfaces have separate gates")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/held", nil)
	held, err := client.antfly.admission.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, err := client.Antfly().ListTables(ctx); first <- err }()
	waitAdmissionQueue(t, client.antfly.admission, 1)
	go func() { _, err := client.Inference().ListModels(ctx); second <- err }()
	waitAdmissionQueue(t, client.antfly.admission, 2)
	_, err = client.Antfly().ListTables(ctx)
	requireCapacityReason(t, err, "queue_full")
	mu.Lock()
	before := append([]string(nil), paths...)
	mu.Unlock()
	if !reflect.DeepEqual(before, []string{"/held"}) {
		t.Fatalf("queued requests sent: %v", before)
	}
	unblock()
	if _, err = io.Copy(io.Discard, held.Body); err != nil {
		t.Fatal(err)
	}
	if err = held.Body.Close(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []<-chan error{first, second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	mu.Lock()
	got := append([]string(nil), paths...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"/held", "/db/v1/tables", "/ai/v1/models"}) {
		t.Fatalf("request order=%v", got)
	}
	if accepted.Load() != 1 {
		t.Fatalf("connections=%d, want one reused connection", accepted.Load())
	}
	assertAdmissionDrained(t, client.antfly.admission)
	_ = client.Close()
	_ = client.Close()
	_, err = client.Antfly().ListTables(ctx)
	requireCapacityReason(t, err, "client_closed")
	// The caller's supplied client and pool must still be usable after SDK close.
	borrowed, err := server.Client().Get(server.URL + "/borrowed")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, borrowed.Body)
	_ = borrowed.Body.Close()
	if accepted.Load() != 1 {
		t.Fatal("SDK close retired the caller-owned pooled connection")
	}
}

func TestBoundedAdmissionCancellationExpiryAndCloseNeverSend(t *testing.T) {
	for _, mode := range []string{"deadline", "client_timeout", "cancel", "close", "wait_timeout"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			respond := func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 200, Body: &admissionTestBody{}, Header: make(http.Header)}, nil
			}
			var next oapi.HttpRequestDoer = admissionDoerFunc(respond)
			if mode == "client_timeout" {
				next = &http.Client{Timeout: 40 * time.Millisecond, Transport: admissionRoundTripFunc(respond)}
			}
			wait := time.Second
			if mode == "wait_timeout" {
				wait = 40 * time.Millisecond
			}
			client, err := NewBoundedHTTPClient(next, &RequestAdmissionConfig{1, 1, wait})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			held, err := client.Do(admissionRequest(t, context.Background(), "/held"))
			if err != nil {
				t.Fatal(err)
			}
			defer held.Body.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
				defer stop()
			}
			request := admissionRequest(t, ctx, "/never-sent")
			done := make(chan error, 1)
			go func() { _, err := client.Do(request); done <- err }()
			waitAdmissionQueue(t, client, 1)
			reason := "deadline_exceeded"
			switch mode {
			case "cancel":
				cancel()
				reason = "canceled"
			case "close":
				_ = client.Close()
				reason = "client_closed"
			case "wait_timeout":
				reason = "wait_timeout"
			}
			select {
			case err := <-done:
				requireCapacityReason(t, err, reason)
				if mode == "deadline" || mode == "client_timeout" {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("deadline identity lost")
					}
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation identity lost")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("waiter did not retire")
			}
			if calls.Load() != 1 {
				t.Fatal("refused operation reached HTTP client")
			}
			_ = held.Body.Close()
			assertAdmissionDrained(t, client)
		})
	}
}

func TestBoundedAdmissionResponseFailureOwnership(t *testing.T) {
	for _, mode := range []string{"read", "close", "transport", "response_and_error"} {
		t.Run(mode, func(t *testing.T) {
			body := &admissionTestBody{}
			if mode == "read" {
				body.readErr = io.ErrUnexpectedEOF
			}
			if mode == "close" {
				body.closeErr = io.ErrClosedPipe
			}
			client, _ := NewBoundedHTTPClient(admissionDoerFunc(func(*http.Request) (*http.Response, error) {
				if mode == "transport" {
					return nil, io.ErrUnexpectedEOF
				}
				response := &http.Response{StatusCode: 200, Body: body}
				if mode == "response_and_error" {
					return response, io.ErrUnexpectedEOF
				}
				return response, nil
			}), &RequestAdmissionConfig{1, 0, 0})
			defer client.Close()
			response, err := client.Do(admissionRequest(t, context.Background(), "/body"))
			if mode == "transport" || mode == "response_and_error" {
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(err)
				}
				if response != nil {
					_ = response.Body.Close()
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				_, readErr := io.ReadAll(response.Body)
				if mode == "read" && !errors.Is(readErr, io.ErrUnexpectedEOF) {
					t.Fatal(readErr)
				}
				_, err = client.Do(admissionRequest(t, context.Background(), "/overflow"))
				requireCapacityReason(t, err, "queue_full")
				closeErr := response.Body.Close()
				if mode == "close" && !errors.Is(closeErr, io.ErrClosedPipe) {
					t.Fatal(closeErr)
				}
				_ = response.Body.Close()
			}
			assertAdmissionDrained(t, client)
			want := int32(1)
			if mode == "transport" {
				want = 0
			}
			if body.closes.Load() != want {
				t.Fatalf("body closes=%d, want %d", body.closes.Load(), want)
			}
		})
	}
}

func TestBoundedAdmissionPreservesSQLNoReplayAndCustomPolicy(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	borrowed := server.Client()
	redirects := 0
	borrowed.CheckRedirect = func(*http.Request, []*http.Request) error { redirects++; return nil }
	bounded, err := NewBoundedHTTPClient(borrowed, &RequestAdmissionConfig{1, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(bounded))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.admission != bounded {
		t.Fatal("custom admission was wrapped in another queue")
	}
	_, err = client.ExecuteSQL(context.Background(), SQLRequest{Statement: "DELETE FROM records"})
	if err == nil {
		t.Fatal("redirect accepted as SQL success")
	}
	if calls.Load() != 1 || redirects != 0 {
		t.Fatalf("SQL replayed: requests=%d redirect callbacks=%d", calls.Load(), redirects)
	}
	assertAdmissionDrained(t, bounded)
	if borrowed.CheckRedirect == nil {
		t.Fatal("borrowed redirect policy changed")
	}
}

func TestBoundedAdmissionOriginalContextDeadlineSurvivesGrant(t *testing.T) {
	var observed time.Time
	client, _ := NewBoundedHTTPClient(admissionDoerFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/queued" {
			observed, _ = r.Context().Deadline()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	}), &RequestAdmissionConfig{1, 1, time.Second})
	defer client.Close()
	held, _ := client.Do(admissionRequest(t, context.Background(), "/held"))
	defer held.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	request := admissionRequest(t, ctx, "/queued")
	done := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	waitAdmissionQueue(t, client, 1)
	_ = held.Body.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !observed.Equal(deadline) {
		t.Fatalf("deadline changed: %v -> %v", deadline, observed)
	}
	assertAdmissionDrained(t, client)
}

func TestBoundedAdmissionDefaultsAndValidation(t *testing.T) {
	db, err := NewAntflyClient("http://test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inference, err := NewInferenceClient("http://test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer inference.Close()
	want := RequestAdmissionConfig{100, 100, time.Second}
	if db.admission.Stats().Policy != want || inference.admission.Stats().Policy != want {
		t.Fatal("legacy constructors did not install default admission")
	}
	for _, policy := range []RequestAdmissionConfig{{0, 1, time.Second}, {1, -1, time.Second}, {1, 1, -time.Second}} {
		if _, err := NewBoundedHTTPClient(nil, &policy); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}

func TestBoundedAdmissionCanceledMiddleWaiterPreservesSurvivorOrder(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	client, err := NewBoundedHTTPClient(admissionDoerFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()
		return &http.Response{StatusCode: 200, Body: &admissionTestBody{}}, nil
	}), &RequestAdmissionConfig{1, 3, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	held, err := client.Do(admissionRequest(t, context.Background(), "/held"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	middleCtx, cancelMiddle := context.WithCancel(ctx)
	defer cancelMiddle()
	done := make([]chan error, 3)
	for i, path := range []string{"/first", "/middle", "/last"} {
		done[i] = make(chan error, 1)
		requestCtx := ctx
		if i == 1 {
			requestCtx = admissionReentrantContext{middleCtx, client}
		}
		request := admissionRequest(t, requestCtx, path)
		go func(result chan error) {
			response, err := client.Do(request)
			if response != nil {
				_ = response.Body.Close()
			}
			result <- err
		}(done[i])
		waitAdmissionQueue(t, client, i+1)
	}
	cancelMiddle()
	select {
	case err := <-done[1]:
		requireCapacityReason(t, err, "canceled")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitAdmissionQueue(t, client, 2)
	_ = held.Body.Close()
	for _, i := range []int{0, 2} {
		select {
		case err := <-done[i]:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(calls, []string{"/held", "/first", "/last"}) {
		t.Fatalf("survivor order=%v", calls)
	}
	assertAdmissionDrained(t, client)
}

func TestBoundedAdmissionUnsentWriteClosesBodyWithoutReading(t *testing.T) {
	var calls atomic.Int32
	client, _ := NewBoundedHTTPClient(admissionDoerFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Body: &admissionTestBody{}}, nil
	}), &RequestAdmissionConfig{1, 0, 0})
	defer client.Close()
	held, err := client.Do(admissionRequest(t, context.Background(), "/held"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	body := &admissionTestBody{}
	request, err := http.NewRequest("POST", "http://test/never-sent", body)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	requireCapacityReason(t, err, "queue_full")
	if calls.Load() != 1 || body.reads.Load() != 0 || body.closes.Load() != 1 {
		t.Fatalf("calls=%d body reads=%d closes=%d", calls.Load(), body.reads.Load(), body.closes.Load())
	}
	_ = held.Body.Close()
	assertAdmissionDrained(t, client)
}

type admissionReentrantContext struct {
	context.Context
	owner *BoundedHTTPClient
}

func (ctx admissionReentrantContext) Err() error {
	_ = ctx.owner.Stats()
	return ctx.Context.Err()
}

func (ctx admissionReentrantContext) Deadline() (time.Time, bool) {
	_ = ctx.owner.Stats()
	return ctx.Context.Deadline()
}

func TestBoundedAdmissionContextCallbacksRunOutsideGateMutex(t *testing.T) {
	client, _ := NewBoundedHTTPClient(admissionDoerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: &admissionTestBody{}}, nil
	}), nil)
	request := admissionRequest(t, admissionReentrantContext{context.Background(), client}, "/reentrant")
	done := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		assertAdmissionDrained(t, client)
		_ = client.Close()
	case <-time.After(2 * time.Second):
		// Avoid another call into a deadlocked owner on the failure path. This
		// isolated regression process can exit and report the held mutex.
		t.Fatal("context callback deadlocked on the admission mutex")
	}
}

func TestBoundedAdmissionOwnedDefaultPoolClosesPeer(t *testing.T) {
	var accepted atomic.Int32
	peerClosed := make(chan struct{})
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			accepted.Add(1)
		}
		if state == http.StateClosed {
			once.Do(func() { close(peerClosed) })
		}
	}
	server.Start()
	defer server.Close()
	client, err := NewAntflyClient(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 2 {
		if _, err := client.ListTables(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if accepted.Load() != 1 {
		t.Fatal("owned default pool did not reuse its connection")
	}
	assertAdmissionDrained(t, client.admission)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peerClosed:
		// Observe EOF before server.Close: cleanup belongs to the SDK owner.
	case <-time.After(2 * time.Second):
		t.Fatal("SDK-owned idle connection remained open after Close")
	}
}

func TestBoundedAdmissionOwnedPoolReusesConcurrentBurst(t *testing.T) {
	const width = 4
	var accepted atomic.Int32
	entered := [2]chan struct{}{make(chan struct{}, width), make(chan struct{}, width)}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var released [2]sync.Once
	free := func(wave int) { released[wave].Do(func() { close(release[wave]) }) }
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wave := 0
		if r.URL.Path == "/second" {
			wave = 1
		}
		entered[wave] <- struct{}{}
		select {
		case <-release[wave]:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "complete")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			accepted.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	defer free(0)
	defer free(1)
	client, err := NewBoundedHTTPClient(nil, &RequestAdmissionConfig{MaxConcurrentRequests: width})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for wave := range 2 {
		path := "/first"
		if wave == 1 {
			path = "/second"
		}
		done := make(chan error, width)
		for range width {
			go func() {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
				if err != nil {
					done <- err
					return
				}
				response, err := client.Do(request)
				if err != nil {
					done <- err
					return
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				done <- errors.Join(readErr, closeErr)
			}()
		}
		for range width {
			select {
			case <-entered[wave]:
			case <-ctx.Done():
				t.Fatalf("burst %d did not reach peer: %v", wave, ctx.Err())
			}
		}
		free(wave)
		for range width {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		assertAdmissionDrained(t, client)
	}
	if accepted.Load() != width {
		t.Fatalf("two admitted bursts opened %d connections, want reuse of %d", accepted.Load(), width)
	}
}

func TestBoundedAdmissionOwnedPoolBoundsIdleConnectionsAcrossTargets(t *testing.T) {
	var live atomic.Int32
	changed := make(chan struct{}, 16)
	servers := make([]*httptest.Server, 0, 3)
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()
	client, err := NewBoundedHTTPClient(nil, &RequestAdmissionConfig{MaxConcurrentRequests: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 3 {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "idle") }))
		server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				live.Add(1)
			}
			if state == http.StateClosed {
				live.Add(-1)
				select {
				case changed <- struct{}{}:
				default:
				}
			}
		}
		server.Start()
		servers = append(servers, server)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatal(err)
		}
	}
	until := time.NewTimer(2 * time.Second)
	defer until.Stop()
	for live.Load() > 2 {
		select {
		case <-changed:
		case <-until.C:
			t.Fatalf("idle targets retained %d sockets above admission capacity", live.Load())
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for live.Load() != 0 {
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("closed owned pool retained %d sockets", live.Load())
		}
	}
}

func TestBoundedAdmissionOwnedPublicMutationsDoNotFollowRedirects(t *testing.T) {
	operations := map[string]func(*AntflyClient, context.Context) error{
		"create": func(c *AntflyClient, ctx context.Context) error {
			return c.CreateTable(ctx, "docs", CreateTableRequest{})
		},
		"drop": func(c *AntflyClient, ctx context.Context) error { return c.DropTable(ctx, "docs") },
	}
	for name, operation := range operations {
		for _, borrowed := range []bool{false, true} {
			t.Run(name+"/"+map[bool]string{false: "owned", true: "borrowed"}[borrowed], func(t *testing.T) {
				var original, redirected atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/redirected" {
						redirected.Add(1)
						w.WriteHeader(http.StatusNoContent)
						return
					}
					original.Add(1)
					io.Copy(io.Discard, r.Body)
					http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
				}))
				defer server.Close()
				var next *http.Client
				if borrowed {
					next = server.Client()
				}
				client, err := NewAntflyClient(server.URL, next)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				err = operation(client, context.Background())
				if original.Load() != 1 {
					t.Fatalf("sent %d original operations", original.Load())
				}
				if borrowed {
					if err != nil || redirected.Load() != 1 {
						t.Fatalf("caller policy changed: %v, redirects %d", err, redirected.Load())
					}
				} else {
					if err == nil || redirected.Load() != 0 {
						t.Fatalf("owned mutation followed redirect: %v, redirects %d", err, redirected.Load())
					}
					var capacity *ClientCapacityError
					if errors.As(err, &capacity) {
						t.Fatal("sent mutation was reported as unsent admission refusal")
					}
				}
				assertAdmissionDrained(t, client.admission)
			})
		}
	}
}
