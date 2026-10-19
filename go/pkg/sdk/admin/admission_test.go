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

package admin

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sdk"
)

type admissionTransport func(*http.Request) (*http.Response, error)

func (f admissionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInternalAdmissionRetainsBodyUntilDecodeAndKeepsBorrowedTransport(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	entered := make(chan struct{})
	var calls atomic.Int32
	borrowed := &http.Client{Transport: admissionTransport(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			return &http.Response{StatusCode: 200, Body: reader, Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: 204, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	client, err := NewInternalClientWithAdmission("http://admin", borrowed, &sdk.RequestAdmissionConfig{MaxConcurrentRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() { _, err := client.GetMetadataStatusContext(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("status did not start")
	}
	err = client.AddMetadataPeerContext(context.Background(), 2, "raft://node-2")
	var capacity *sdk.ClientCapacityError
	if !errors.As(err, &capacity) || capacity.Reason != "queue_full" || capacity.RequestSent {
		t.Fatalf("held response did not retain capacity: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("refused mutation was sent")
	}
	if _, err := io.WriteString(writer, `{"raft_status":{"leader_id":1,"voters":{"1":"raft://node-1"}}}`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("status did not finish")
	}
	if err := client.RemoveMetadataPeerContext(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.AddMetadataPeer(3, "raft://node-3"); !errors.As(err, &capacity) || capacity.Reason != "client_closed" {
		t.Fatalf("closed client: %v", err)
	}
	req, _ := http.NewRequest("GET", "http://admin/borrowed", nil)
	response, err := borrowed.Do(req)
	if err != nil {
		t.Fatalf("borrowed transport closed: %v", err)
	}
	response.Body.Close()
}

func TestInternalAdmissionSharesOriginalQueueDeadlineForEveryOperation(t *testing.T) {
	operations := map[string]func(*InternalClient, context.Context) error{
		"status": func(c *InternalClient, ctx context.Context) error {
			_, err := c.GetMetadataStatusContext(ctx)
			return err
		},
		"add": func(c *InternalClient, ctx context.Context) error {
			return c.AddMetadataPeerContext(ctx, 2, "raft://node-2")
		},
		"remove": func(c *InternalClient, ctx context.Context) error { return c.RemoveMetadataPeerContext(ctx, 2) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			next := &http.Client{Transport: admissionTransport(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("held")), Header: make(http.Header)}, nil
			})}
			shared, err := sdk.NewBoundedHTTPClient(next, &sdk.RequestAdmissionConfig{1, 1, time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer shared.Close()
			client, err := NewInternalClientWithAdmission("http://admin", shared, nil)
			if err != nil {
				t.Fatal(err)
			}
			req, _ := http.NewRequest("GET", "http://admin/held", nil)
			held, err := shared.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Body.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err = operation(client, ctx)
			var capacity *sdk.ClientCapacityError
			if !errors.As(err, &capacity) || !errors.Is(err, context.DeadlineExceeded) || capacity.RequestSent || capacity.Reason != "deadline_exceeded" {
				t.Fatalf("queued operation lost original deadline: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("queued operation sent: %d", calls.Load())
			}
		})
	}
}

func TestInternalAdmissionRejectsInvalidPolicyAndNeverStacksSharedQueues(t *testing.T) {
	if _, err := NewInternalClientWithAdmission("http://admin", nil, &sdk.RequestAdmissionConfig{}); err == nil {
		t.Fatal("accepted zero active capacity")
	}
	shared, err := sdk.NewBoundedHTTPClient(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	if _, err := NewInternalClientWithAdmission("http://admin", shared, &sdk.RequestAdmissionConfig{MaxConcurrentRequests: 1}); err == nil {
		t.Fatal("stacked a second queue")
	}
	client, err := NewInternalClientWithAdmission("http://admin", shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", "http://admin/shared", nil)
	_, err = shared.Do(req)
	var capacity *sdk.ClientCapacityError
	if !errors.As(err, &capacity) || capacity.Reason != "client_closed" {
		t.Fatalf("did not share lifecycle: %v", err)
	}
}

func TestInternalOwnedClientDoesNotRedirectMutations(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "borrowed"}[borrowed], func(t *testing.T) {
			var original, redirected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/redirected" {
					redirected.Add(1)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				original.Add(1)
				http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			var next *http.Client
			if borrowed {
				next = server.Client()
			}
			client := NewInternalClient(server.URL, next)
			defer client.Close()
			err := client.AddMetadataPeer(2, "raft://node-2")
			if original.Load() != 1 {
				t.Fatalf("sent %d original requests", original.Load())
			}
			if borrowed {
				if err != nil || redirected.Load() != 1 {
					t.Fatalf("caller redirect policy changed: %v, redirects %d", err, redirected.Load())
				}
			} else if err == nil || redirected.Load() != 0 {
				t.Fatalf("owned client replayed mutation: %v, redirects %d", err, redirected.Load())
			}
		})
	}
}

func TestStandbyAdmissionSpansNegotiationAndDecodeWithOneDeadline(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	entered := make(chan struct{})
	deadlines := make(chan time.Time, 2)
	var calls atomic.Int32
	borrowed := &http.Client{Timeout: time.Second, Transport: admissionTransport(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		switch req.URL.Path {
		case StandbyPrimaryStatusPath:
			deadline, ok := req.Context().Deadline()
			if !ok {
				return nil, errors.New("missing operation deadline")
			}
			deadlines <- deadline
			return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("not found")), Header: make(http.Header)}, nil
		case HAPrimaryStatusPath:
			deadline, _ := req.Context().Deadline()
			deadlines <- deadline
			close(entered)
			return &http.Response{StatusCode: 200, Body: reader, Header: http.Header{"Content-Type": {"application/json"}}}, nil
		default:
			return &http.Response{StatusCode: 204, Body: http.NoBody, Header: make(http.Header)}, nil
		}
	})}
	shared, err := sdk.NewBoundedHTTPClient(borrowed, &sdk.RequestAdmissionConfig{MaxConcurrentRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	client, err := NewStandbyClientWithAdmission("http://standby-single-owner", shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.WithPathStyle(PathStyleAuto)
	admin, err := NewInternalClientWithAdmission("http://standby-single-owner", shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := client.PrimaryStatusResponse(context.Background(), nil); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("negotiation did not reach legacy route")
	}
	if first, second := <-deadlines, <-deadlines; !first.Equal(second) {
		t.Fatalf("negotiation reset original timeout: %v != %v", first, second)
	}
	err = admin.RemoveMetadataPeerContext(context.Background(), 2)
	var capacity *sdk.ClientCapacityError
	if !errors.As(err, &capacity) || capacity.Reason != "queue_full" || capacity.RequestSent {
		t.Fatalf("negotiated response released admission before decoding: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("refused peer mutation was sent: %d", calls.Load())
	}
	if _, err := io.WriteString(writer, standbyGeneratedPrimaryStatusJSON()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("status did not finish")
	}
	if err := admin.RemoveMetadataPeerContext(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
}

func TestStandbyNegotiationPreservesCancellationInsteadOfOriginal404(t *testing.T) {
	next := &http.Client{Transport: admissionTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == StandbyPrimaryStatusPath {
			return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("not found")), Header: make(http.Header)}, nil
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	client, err := NewStandbyClient("http://standby-cancel-negotiation", next)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.WithPathStyle(PathStyleAuto)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.PrimaryStatusResponse(ctx, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("negotiation hid original cancellation: %v", err)
	}
}

func TestStandbyOwnedClientDoesNotRedirectMutations(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "borrowed"}[borrowed], func(t *testing.T) {
			var redirected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/redirected" {
					redirected.Add(1)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			var next *http.Client
			if borrowed {
				next = server.Client()
			}
			client, err := NewStandbyClient(server.URL, next)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, err = client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{SlotName: "standby-a", InitialLsn: 7})
			if !borrowed && (err == nil || redirected.Load() != 0) {
				t.Fatalf("owned client redirected mutation: %v, count %d", err, redirected.Load())
			}
			if borrowed && redirected.Load() != 1 {
				t.Fatalf("caller redirect policy changed: count %d", redirected.Load())
			}
		})
	}
}

func TestStandbyNegotiationNeverReplaysMutationAfterAmbiguous404(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "undiscovered", true: "pinned"}[pinned], func(t *testing.T) {
			var mutations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					io.WriteString(w, standbyGeneratedPrimaryStatusJSON())
					return
				}
				mutations.Add(1)
				if strings.HasPrefix(r.URL.Path, StandbyPath) {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				io.WriteString(w, standbyAutoSlotCreateJSON)
			}))
			defer server.Close()
			client, err := NewStandbyClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.WithPathStyle(PathStyleAuto)
			if pinned {
				if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
			}
			_, err = client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{SlotName: "standby-a", InitialLsn: 7})
			if err == nil || mutations.Load() != 1 {
				t.Fatalf("ambiguous mutation outcome was replayed: err %v, mutations %d", err, mutations.Load())
			}
		})
	}
}

type admissionUnsentBody struct {
	*strings.Reader
	closes atomic.Int32
}

func (b *admissionUnsentBody) Close() error { b.closes.Add(1); return nil }

func TestStandbyDiscoveryFailureClosesUnsentMutationAndReleasesAdmission(t *testing.T) {
	for _, mode := range []string{"denied", "not_found", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			var recovered atomic.Bool
			var mutations atomic.Int32
			next := &http.Client{Transport: admissionTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					mutations.Add(1)
				}
				if recovered.Load() {
					return &http.Response{StatusCode: 200, Body: http.NoBody, Header: make(http.Header)}, nil
				}
				if mode == "canceled" {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				code := http.StatusUnauthorized
				if mode == "not_found" {
					code = http.StatusNotFound
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("not found")), Header: make(http.Header)}, nil
			})}
			client, err := NewStandbyClientWithAdmission("http://unsent-"+mode, next, &sdk.RequestAdmissionConfig{MaxConcurrentRequests: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.WithPathStyle(PathStyleAuto)
			body := &admissionUnsentBody{Reader: strings.NewReader("private mutation input")}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unsent-"+mode+StandbyReplicationSlotsPath, body)
			if err != nil {
				t.Fatal(err)
			}
			negotiating := &pathNegotiatingDoer{admission: client.admission, client: client}
			resp, err := negotiating.Do(req)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			if mode == "canceled" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if mutations.Load() != 0 || body.closes.Load() != 1 {
				t.Fatalf("discovery sent mutation or leaked unsent body: mutations %d, closes %d", mutations.Load(), body.closes.Load())
			}
			recovered.Store(true)
			healthy, _ := http.NewRequest(http.MethodGet, "http://unsent-"+mode+"/healthy", nil)
			response, err := client.admission.Do(healthy)
			if err != nil {
				t.Fatalf("discovery retained admission: %v", err)
			}
			response.Body.Close()
		})
	}
}

func TestStandbyNegotiationPreservesDeadlineDuringFirst404Body(t *testing.T) {
	var mutations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mutations.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.WithPathStyle(PathStyleAuto)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.CreateReplicationSlot(ctx, ReplicationSlotCreateRequest{SlotName: "standby-a", InitialLsn: 7})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first response hid original deadline: %v", err)
	}
	if mutations.Load() != 0 {
		t.Fatalf("discovery sent %d mutations", mutations.Load())
	}
}

func TestStandbyFailedDiscoveryDoesNotPinRoutes(t *testing.T) {
	for _, legacyFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "canonical_unavailable", true: "legacy_unauthorized"}[legacyFailure], func(t *testing.T) {
			next := &http.Client{Transport: admissionTransport(func(req *http.Request) (*http.Response, error) {
				code := http.StatusServiceUnavailable
				if legacyFailure {
					code = http.StatusNotFound
					if strings.HasPrefix(req.URL.Path, HAPath) {
						code = http.StatusUnauthorized
					}
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("not found")), Header: make(http.Header)}, nil
			})}
			client, err := NewStandbyClient("http://failed-route-"+strconv.FormatBool(legacyFailure), next)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.WithPathStyle(PathStyleAuto)
			if _, err := client.PrimaryStatusResponse(context.Background(), nil); err == nil {
				t.Fatal("failed discovery succeeded")
			}
			if _, pinned := client.NegotiatedPathStyle(); pinned {
				t.Fatal("failed response published a successful route discovery")
			}
		})
	}
}

func TestStandbyRouteCacheBoundsTargetsAndCopiedKeyBytes(t *testing.T) {
	var cache pathStyleCache
	key := func(i int) string { return "http://cluster-" + strconv.Itoa(i) }
	for i := 0; i < negotiatedPathCacheEntries+5; i++ {
		cache.Store(key(i), PathStyleLegacy)
	}
	for i := 0; i < 5; i++ {
		if _, found := cache.Load(key(i)); found {
			t.Fatalf("old target %d was not evicted", i)
		}
	}
	for i := 5; i < negotiatedPathCacheEntries+5; i++ {
		if style, found := cache.Load(key(i)); !found || style != PathStyleLegacy {
			t.Fatalf("recent target %d lost", i)
		}
	}
	cache.Store(key(5), PathStyleCanonical)
	if style, found := cache.Load(key(5)); !found || style != PathStyleCanonical {
		t.Fatal("route update lost")
	}
	tooLarge := strings.Repeat("x", negotiatedPathCacheKeyBytes+1)
	cache.Store(tooLarge, PathStyleLegacy)
	cache.Store("", PathStyleLegacy)
	if _, found := cache.Load(tooLarge); found {
		t.Fatal("oversized key retained")
	}
	if len(cache.entries) != negotiatedPathCacheEntries {
		t.Fatalf("cache targets %d", len(cache.entries))
	}
	cache.Reset()
	if len(cache.entries) != 0 {
		t.Fatal("reset retained targets")
	}
	for _, retained := range cache.keys {
		if retained != "" {
			t.Fatal("reset retained copied key")
		}
	}

	var tasks sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		tasks.Go(func() {
			for i := 0; i < 128; i++ {
				target := key(worker*128 + i)
				cache.Store(target, PathStyleLegacy)
				cache.Load(target)
			}
		})
	}
	tasks.Wait()
	if len(cache.entries) > negotiatedPathCacheEntries {
		t.Fatal("concurrent cache exceeded target ceiling")
	}
}
