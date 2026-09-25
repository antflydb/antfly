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
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ReadRetryPolicy is opt-in. MaxAttempts includes the original attempt. One
// MaxElapsed budget covers admission, attempts and backoff; an earlier caller
// deadline or query-body timeout_ms wins (the shortest line for NDJSON).
// Only query routes rejected explicitly before execution qualify.
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

type timeoutSpan struct{ start, end int }

func skipJSONSpace(data []byte, position int) int {
	for position < len(data) && (data[position] == ' ' || data[position] == '\t' || data[position] == '\r' || data[position] == '\n') {
		position++
	}
	return position
}

func jsonStringEnd(data []byte, position int) int {
	position++
	for position < len(data) {
		if data[position] == '\\' {
			position += 2
		} else if data[position] == '"' {
			return position + 1
		} else {
			position++
		}
	}
	return len(data)
}

// The line is fully validated before this scan. Locate only top-level values;
// nested fields and the spelling of all other bytes remain untouched.
func jsonValueEnd(data []byte, position int) int {
	depth := 0
	inString := false
	for position < len(data) {
		switch data[position] {
		case '"':
			inString = !inString
		case '\\':
			if inString {
				position++
			}
		case '{', '[':
			if !inString {
				depth++
			}
		case '}', ']':
			if !inString {
				if depth == 0 {
					return position
				}
				depth--
			}
		case ',':
			if !inString && depth == 0 {
				return position
			}
		}
		position++
	}
	return position
}

func queryLineTimeouts(line []byte, offset int, maximum *time.Duration, spans *[]timeoutSpan) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(line, &object) != nil || object == nil {
		return false
	}
	position := skipJSONSpace(line, 0) + 1
	seen := make(map[string]bool)
	for {
		position = skipJSONSpace(line, position)
		if line[position] == '}' {
			return true
		}
		end := jsonStringEnd(line, position)
		var key string
		if json.Unmarshal(line[position:end], &key) != nil || seen[key] {
			return false
		}
		seen[key] = true
		position = skipJSONSpace(line, end)
		position = skipJSONSpace(line, position+1) // colon
		start := position
		position = jsonValueEnd(line, position)
		if key == "timeout_ms" {
			valueEnd := position
			for valueEnd > start && (line[valueEnd-1] == ' ' || line[valueEnd-1] == '\t' || line[valueEnd-1] == '\r' || line[valueEnd-1] == '\n') {
				valueEnd--
			}
			value := line[start:valueEnd]
			if !bytes.Equal(value, []byte("null")) {
				var milliseconds uint64
				if !bytes.Equal(value, []byte("-0")) && json.Unmarshal(value, &milliseconds) != nil {
					return false
				}
				if milliseconds <= uint64(*maximum/time.Millisecond) {
					*maximum = min(*maximum, time.Duration(milliseconds)*time.Millisecond)
				}
				*spans = append(*spans, timeoutSpan{offset + start, offset + valueEnd})
			}
		}
		position = skipJSONSpace(line, position)
		if line[position] == '}' {
			return true
		}
		position++ // comma
	}
}

// Inspect a bounded replay copy without changing caller bytes. Invalid or large
// bodies bypass retries rather than guessing their timeout/serialization contract.
func queryRetryBudget(req *http.Request, maximum time.Duration) (time.Duration, bool) {
	budget, _, _, valid := queryRetryPlan(req, maximum)
	return budget, valid
}

func queryRetryPlan(req *http.Request, maximum time.Duration) (time.Duration, []timeoutSpan, []byte, bool) {
	if req.Body == nil || req.GetBody == nil {
		return maximum, nil, nil, false
	}
	copy, err := req.GetBody()
	if err != nil {
		return maximum, nil, nil, false
	}
	defer copy.Close()
	body, err := io.ReadAll(io.LimitReader(copy, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return maximum, nil, nil, false
	}
	ndjson := strings.EqualFold(strings.TrimSpace(strings.Split(req.Header.Get("Content-Type"), ";")[0]), "application/x-ndjson")
	spans := []timeoutSpan{}
	seen := false
	for offset := 0; offset < len(body); {
		end := len(body)
		if ndjson {
			if newline := bytes.IndexByte(body[offset:], '\n'); newline >= 0 {
				end = offset + newline + 1
			}
		}
		line := body[offset:end]
		if len(bytes.TrimSpace(line)) == 0 {
			offset = end
			continue
		}
		if !queryLineTimeouts(line, offset, &maximum, &spans) {
			return maximum, nil, nil, false
		}
		seen = true
		offset = end
	}
	return maximum, spans, body, seen
}

func remainingQueryBody(body []byte, spans []timeoutSpan, deadline time.Time) []byte {
	remaining := max(0, time.Until(deadline)/time.Millisecond)
	result := make([]byte, 0, len(body)+len(spans)*20)
	position := 0
	for _, span := range spans {
		result = append(result, body[position:span.start]...)
		result = strconv.AppendInt(result, int64(remaining), 10)
		position = span.end
	}
	return append(result, body[position:]...)
}

// A rejected-before-execution proof must be unambiguous. encoding/json accepts
// duplicate keys and repairs malformed UTF-8, either of which could otherwise
// turn conflicting rolling-version evidence into a retryable rejection.
func explicitReadNonAdmission(body []byte) bool {
	if !utf8.Valid(body) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return false
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := keyToken.(string)
		if !ok {
			return false
		}
		if _, duplicate := fields[key]; duplicate {
			return false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
		fields[key] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return false
	}
	var reason, stage string
	if json.Unmarshal(fields["reason"], &reason) != nil || json.Unmarshal(fields["stage"], &stage) != nil {
		return false
	}
	return reason == "instance_busy" && stage == "admission" && bytes.Equal(bytes.TrimSpace(fields["execution_started"]), []byte("false"))
}

func (t *readRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || !retryQueryPath.MatchString(req.URL.EscapedPath()) || (req.Body != nil && req.GetBody == nil) {
		return t.base.RoundTrip(req)
	}
	started := time.Now()
	budget, spans, originalBody, recognized := queryRetryPlan(req, t.policy.MaxElapsed)
	if !recognized {
		return t.base.RoundTrip(req)
	}
	ctx, cancel := context.WithDeadline(req.Context(), started.Add(budget))
	current := req.Clone(ctx)
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			closeRequestBody(current)
			cancel()
			return nil, err
		}
		response, err := t.base.RoundTrip(current)
		if err != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			cancel()
			return response, err
		}
		if err := ctx.Err(); err != nil {
			if response.Body != nil {
				_ = response.Body.Close()
			}
			cancel()
			return nil, err
		}
		finish := func() (*http.Response, error) {
			if response.Body == nil {
				cancel()
			} else {
				response.Body = newDeadlineBody(response.Body, ctx, cancel)
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
		if readErr != nil || len(body) > 16384 || !explicitReadNonAdmission(body) {
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
			replay := remainingQueryBody(originalBody, spans, end)
			current.Body = io.NopCloser(bytes.NewReader(replay))
			current.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(replay)), nil }
			current.ContentLength = int64(len(replay))
			current.Header.Del("Content-Length")
		}
	}
}

type replayedErrorBody struct {
	io.Reader
	io.Closer
}

// Keep the original query deadline after RoundTrip has returned headers. A
// custom RoundTripper may ignore request cancellation during body reads.
type deadlineBody struct {
	io.ReadCloser
	ctx     context.Context
	release context.CancelFunc
	once    sync.Once
	done    chan struct{}
}

func newDeadlineBody(body io.ReadCloser, ctx context.Context, release context.CancelFunc) *deadlineBody {
	wrapped := &deadlineBody{ReadCloser: body, ctx: ctx, release: release, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = wrapped.Close()
		case <-wrapped.done:
		}
	}()
	return wrapped
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		_ = b.Close()
		return 0, err
	}
	n, err := b.ReadCloser.Read(p)
	if expired := b.ctx.Err(); expired != nil {
		_ = b.Close()
		return 0, expired
	}
	if err != nil {
		_ = b.Close()
	}
	return n, err
}

func (b *deadlineBody) Close() error {
	var err error
	b.once.Do(func() {
		err = b.ReadCloser.Close()
		b.release()
		close(b.done)
	})
	return err
}

func (t *readRetryTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
