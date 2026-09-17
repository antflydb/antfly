// Copyright 2026 The Antfly Contributors
// Licensed under the Apache License, Version 2.0.

package sdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type admissionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f admissionRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func admissionRequest(t *testing.T, ctx context.Context) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://test/db/v1/tables/docs/query", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestClientAdmissionQueueCancellationAndStreamingLifetime(t *testing.T) {
	var sent atomic.Int32
	base := admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		sent.Add(1)
		_ = req.Body.Close()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("result"))}, nil
	})
	transport, err := newAdmissionTransport(base, ClientAdmission{MaxInFlight: 1, MaxQueued: 1, MaxWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	first, err := transport.RoundTrip(admissionRequest(t, context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := admissionRequest(t, ctx)
	result := make(chan error, 1)
	go func() { _, err := transport.RoundTrip(second); result <- err }()
	deadline := time.Now().Add(time.Second)
	for len(transport.outstanding) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second request did not enter waiting")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := transport.RoundTrip(admissionRequest(t, context.Background())); !errors.Is(err, ErrClientBusy) {
		t.Fatalf("full queue: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled queued request: %v", err)
	}
	if sent.Load() != 1 || len(transport.active) != 1 || len(transport.outstanding) != 1 {
		t.Fatal("cancellation dispatched work or released the streaming request")
	}
	if _, err := io.ReadAll(first.Body); err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close() // EOF and Close must return the permit only once.
	if len(transport.active) != 0 || len(transport.outstanding) != 0 {
		t.Fatal("stream retained a permit after EOF")
	}
	next, err := transport.RoundTrip(admissionRequest(t, context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	_ = next.Body.Close()
	if sent.Load() != 2 {
		t.Fatalf("unexpected dispatch count: %d", sent.Load())
	}
}

func TestClientAdmissionBoundsWaitingAndDoesNotRetryWrites(t *testing.T) {
	var sent atomic.Int32
	base := admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		sent.Add(1)
		_ = req.Body.Close()
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("uncertain write"))}, nil
	})
	transport, err := newAdmissionTransport(base, ClientAdmission{MaxInFlight: 1, MaxQueued: 1, MaxWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	req := admissionRequest(t, context.Background())
	req.URL.Path = "/db/v1/tables/docs/batch"
	first, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	if _, err := transport.RoundTrip(admissionRequest(t, context.Background())); !errors.Is(err, ErrClientBusy) {
		t.Fatalf("waiting bound: %v", err)
	}
	if sent.Load() != 1 || len(transport.outstanding) != 1 {
		t.Fatal("timed-out waiter dispatched or ambiguous write retried")
	}
}

func TestClientAdmissionValidatesConfigAndPreservesSuppliedClient(t *testing.T) {
	for _, config := range []ClientAdmission{
		{}, {MaxInFlight: -1}, {MaxInFlight: 1, MaxQueued: -1},
		{MaxInFlight: 1, MaxQueued: 1}, {MaxInFlight: 1, MaxWait: -1},
	} {
		if _, err := newAdmissionTransport(nil, config); err == nil {
			t.Fatalf("accepted invalid config: %+v", config)
		}
	}
	supplied := &http.Client{Timeout: time.Second}
	_, err := NewClient(Config{BaseURL: "http://test", HTTPClient: supplied, Admission: &ClientAdmission{MaxInFlight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if supplied.Transport != nil || supplied.Timeout != time.Second {
		t.Fatal("SDK modified caller-owned HTTP client")
	}
}

func TestAdmissionErrorPreservesUnknownWriteOutcome(t *testing.T) {
	for _, body := range []string{
		`{"error":"AdmissionQueueFull","reason":"instance_busy","stage":"admission","execution_started":false}`,
		`{"error":"backend_unavailable"}`,
	} {
		response := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(body))}
		err := readErrorResponse(response)
		_ = response.Body.Close()
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected API error, got %v", err)
		}
		if apiErr.Code == "AdmissionQueueFull" {
			if apiErr.Reason != "instance_busy" || apiErr.Stage != "admission" || apiErr.ExecutionStarted == nil || *apiErr.ExecutionStarted || apiErr.RetryAfterSeconds != 1 {
				t.Fatalf("lost admission details: %+v", apiErr)
			}
		} else if apiErr.ExecutionStarted != nil {
			t.Fatal("unknown write outcome became definitely unexecuted")
		}
	}
}
