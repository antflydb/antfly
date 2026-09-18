// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// ReadRetryPolicy is opt-in. MaxAttempts includes the original attempt. One
// MaxElapsed budget covers admission, attempts and backoff; an earlier caller
// deadline wins. Only query routes rejected explicitly before execution qualify.
// Queries may observe newer data when eventually admitted.
type ReadRetryPolicy struct {
	MaxAttempts    int
	MaxElapsed     time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type readRetryTransport struct {
	base   http.RoundTripper
	policy ReadRetryPolicy
}

// NewReadRetryTransport wraps a reusable transport. Place this outside client
// admission so rejected attempts release their slots before backoff.
func NewReadRetryTransport(base http.RoundTripper, policy ReadRetryPolicy) (http.RoundTripper, error) {
	if policy.MaxAttempts < 2 || policy.MaxAttempts > 5 || policy.MaxElapsed <= 0 || policy.MaxElapsed > time.Minute || policy.InitialBackoff <= 0 || policy.MaxBackoff < policy.InitialBackoff || policy.MaxBackoff > policy.MaxElapsed {
		return nil, fmt.Errorf("antfly: invalid read retry policy")
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &readRetryTransport{base, policy}, nil
}

var retryQueryPath = regexp.MustCompile(`^/db/v1/(query|tables/[^/]+/query|databases/[^/]+/namespaces/[^/]+/tables/[^/]+/query)$`)

func (t *readRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || !retryQueryPath.MatchString(req.URL.EscapedPath()) || (req.Body != nil && req.GetBody == nil) {
		return t.base.RoundTrip(req)
	}
	ctx, cancel := context.WithTimeout(req.Context(), t.policy.MaxElapsed)
	current := req.Clone(ctx)
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			closeRequestBody(current)
			cancel()
			return nil, err
		}
		response, err := t.base.RoundTrip(current)
		if err != nil {
			cancel()
			return response, err
		}
		finish := func() (*http.Response, error) {
			if response.Body == nil {
				cancel()
			} else {
				response.Body = &admissionBody{ReadCloser: response.Body, release: cancel}
			}
			return response, nil
		}
		if attempt >= t.policy.MaxAttempts || response.StatusCode != http.StatusTooManyRequests || response.Body == nil {
			return finish()
		}
		// Inspect only small, explicitly bounded error bodies. Streaming success
		// bodies are never read or retried here. Preserve malformed/unknown errors.
		if response.ContentLength < 0 || response.ContentLength > 16384 {
			return finish()
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 16385))
		response.Body = &replayedErrorBody{Reader: io.MultiReader(bytes.NewReader(body), response.Body), Closer: response.Body}
		var detail struct {
			Reason  string `json:"reason"`
			Stage   string `json:"stage"`
			Started *bool  `json:"execution_started"`
		}
		if readErr != nil || len(body) > 16384 || json.Unmarshal(body, &detail) != nil || detail.Reason != "instance_busy" || detail.Stage != "admission" || detail.Started == nil || *detail.Started {
			return finish()
		}
		delay := t.policy.InitialBackoff
		for i := 1; i < attempt && delay < t.policy.MaxBackoff; i++ {
			delay = min(delay*2, t.policy.MaxBackoff)
		}
		if value := response.Header.Get("Retry-After"); value != "" {
			seconds, parseErr := strconv.ParseUint(value, 10, 32)
			if parseErr != nil || seconds > uint64(t.policy.MaxBackoff/time.Second) {
				return finish()
			}
			delay = max(delay, time.Duration(seconds)*time.Second)
		}
		end, _ := ctx.Deadline()
		if time.Until(end) <= delay {
			return finish()
		}
		_ = response.Body.Close()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			cancel()
			return nil, ctx.Err()
		case <-timer.C:
		}
		current = req.Clone(ctx)
		if req.Body != nil {
			current.Body, err = req.GetBody()
			if err != nil {
				cancel()
				return nil, err
			}
		}
	}
}

type replayedErrorBody struct {
	io.Reader
	io.Closer
}

func (t *readRetryTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
