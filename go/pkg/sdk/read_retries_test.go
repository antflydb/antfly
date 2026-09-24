package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const rejectedQuery = `{"reason":"instance_busy","stage":"admission","execution_started":false}`

func retryResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, ContentLength: int64(len(body)), Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

func retryPolicy() ReadRetryPolicy {
	return ReadRetryPolicy{MaxAttempts: 3, MaxElapsed: time.Second, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond}
}

func TestReadRetriesReacquireAdmissionAndPreserveReplay(t *testing.T) {
	calls := 0
	base := admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if string(body) != `{}` {
			t.Fatalf("replayed body: %s", body)
		}
		if calls == 1 {
			return retryResponse(429, rejectedQuery), nil
		}
		return retryResponse(200, "result"), nil
	})
	pool, _ := newAdmissionTransport(base, ClientAdmission{MaxInFlight: 1})
	transport, _ := NewReadRetryTransport(pool, retryPolicy())
	response, err := transport.RoundTrip(admissionRequest(t, context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(pool.active) != 1 {
		t.Fatalf("calls=%d active=%d", calls, len(pool.active))
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "result" || len(pool.active) != 0 {
		t.Fatalf("body=%s active=%d", body, len(pool.active))
	}
}

func TestReadRetriesDoNotRetryWritesAmbiguousErrorsOrLongGuidance(t *testing.T) {
	for _, test := range []struct {
		name, path, body, after string
		status                  int
		network                 bool
	}{
		{"write", "/db/v1/tables/docs/batch", rejectedQuery, "", 429, false},
		{"unknown", "/db/v1/query", `{"reason":"instance_busy"}`, "", 429, false},
		{"executed", "/db/v1/query", `{"reason":"instance_busy","stage":"admission","execution_started":true}`, "", 429, false},
		{"capacity", "/db/v1/query", rejectedQuery, "", 503, false},
		{"long-delay", "/db/v1/query", rejectedQuery, "1", 429, false},
		{"network", "/db/v1/query", "", "", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				_ = req.Body.Close()
				if test.network {
					return nil, errors.New("unknown outcome")
				}
				response := retryResponse(test.status, test.body)
				response.Header.Set("Retry-After", test.after)
				return response, nil
			}), retryPolicy())
			req, _ := http.NewRequest(http.MethodPost, "http://test"+test.path, strings.NewReader(`{}`))
			response, _ := transport.RoundTrip(req)
			if response != nil {
				body, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if string(body) != test.body {
					t.Fatalf("changed rejection: %s", body)
				}
			}
			if calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestReadRetriesCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		_ = req.Body.Close()
		time.AfterFunc(time.Millisecond, cancel)
		return retryResponse(429, rejectedQuery), nil
	}), ReadRetryPolicy{MaxAttempts: 3, MaxElapsed: time.Second, InitialBackoff: 50 * time.Millisecond, MaxBackoff: 50 * time.Millisecond})
	_, err := transport.RoundTrip(admissionRequest(t, ctx))
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestReadRetriesKeepOriginalBodyTimeout(t *testing.T) {
	calls := 0
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		deadline, ok := req.Context().Deadline()
		if !ok || time.Until(deadline) > 80*time.Millisecond {
			t.Fatal("body timeout missing from original deadline")
		}
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if string(body) != `{"timeout_ms":80}` {
			t.Fatalf("body changed: %s", body)
		}
		return retryResponse(429, rejectedQuery), nil
	}), ReadRetryPolicy{MaxAttempts: 3, MaxElapsed: time.Second, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 100 * time.Millisecond})
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader(`{"timeout_ms":80}`))
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 1 {
		t.Fatalf("body timeout restarted across backoff: %d attempts", calls)
	}
	req, _ = http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader("{\"timeout_ms\":100}\n{\"timeout_ms\":20}\n"))
	req.Header.Set("Content-Type", "application/x-ndjson")
	budget, valid := queryRetryBudget(req, time.Second)
	if !valid || budget != 20*time.Millisecond {
		t.Fatalf("NDJSON budget=%v valid=%v", budget, valid)
	}
}

func TestReadRetriesForwardOnlyRemainingBodyTimeout(t *testing.T) {
	original := "{\"large\": 12345678901234567890123456789, \"nested\": {\"timeout_ms\": 999}, \"timeout_ms\": 300 }\n" +
		"{\"timeout_ms\":120,\"other\":1.0000}\n"
	var requests []string
	var deadlines []time.Time
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if req.ContentLength != int64(len(body)) {
			t.Fatalf("stale content length: %d for %d bytes", req.ContentLength, len(body))
		}
		requests = append(requests, string(body))
		deadline, ok := req.Context().Deadline()
		if !ok {
			t.Fatal("missing original deadline")
		}
		deadlines = append(deadlines, deadline)
		if len(requests) == 1 {
			return retryResponse(429, rejectedQuery), nil
		}
		return retryResponse(200, "ok"), nil
	}), ReadRetryPolicy{MaxAttempts: 2, MaxElapsed: time.Second, InitialBackoff: 30 * time.Millisecond, MaxBackoff: 30 * time.Millisecond})
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader(original))
	req.Header.Set("Content-Type", "application/x-ndjson")
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if len(requests) != 2 || requests[0] != original || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("requests=%q deadlines=%v", requests, deadlines)
	}
	lines := strings.Split(strings.TrimSpace(requests[1]), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"large": 12345678901234567890123456789, "nested": {"timeout_ms": 999}`) ||
		!strings.Contains(lines[1], `,"other":1.0000}`) {
		t.Fatalf("unrelated JSON bytes changed: %q", requests[1])
	}
	var first, second map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	firstTimeout := first["timeout_ms"].(float64)
	secondTimeout := second["timeout_ms"].(float64)
	if firstTimeout != secondTimeout || firstTimeout <= 0 || firstTimeout >= 110 {
		t.Fatalf("remaining timeout not forwarded: %v %v", firstTimeout, secondTimeout)
	}
}

func TestReadRetriesDoNotRewriteNonQueryWrites(t *testing.T) {
	original := `{"timeout_ms":120,"large":12345678901234567890123456789}`
	calls := 0
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if string(body) != original {
			t.Fatalf("write body changed: %s", body)
		}
		return retryResponse(429, rejectedQuery), nil
	}), retryPolicy())
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/tables/docs/batch", strings.NewReader(original))
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 1 {
		t.Fatalf("write retried %d times", calls)
	}
}

func TestReadRetriesLeaveAmbiguousDuplicateTimeoutBodyUnchanged(t *testing.T) {
	original := `{"timeout_ms":120,"timeout_ms":20}`
	calls := 0
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if string(body) != original {
			t.Fatalf("ambiguous body changed: %s", body)
		}
		return retryResponse(429, rejectedQuery), nil
	}), retryPolicy())
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader(original))
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 1 {
		t.Fatalf("ambiguous query retried %d times", calls)
	}
}

func TestReadRetriesPreserveOversizedAndUnknownLengthErrors(t *testing.T) {
	for _, length := range []int64{-1, 5} {
		calls := 0
		body := rejectedQuery + strings.Repeat(" ", 16384)
		transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			_ = req.Body.Close()
			response := retryResponse(429, body)
			response.ContentLength = length
			return response, nil
		}), retryPolicy())
		response, err := transport.RoundTrip(admissionRequest(t, context.Background()))
		if err != nil {
			t.Fatal(err)
		}
		actual, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || string(actual) != body || calls != 1 {
			t.Fatalf("length=%d calls=%d err=%v bytes=%d", length, calls, err, len(actual))
		}
	}
}

func TestReadRetriesBoundAttemptsAndCallerDeadline(t *testing.T) {
	calls := 0
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		_ = req.Body.Close()
		return retryResponse(429, rejectedQuery), nil
	}), retryPolicy())
	response, err := transport.RoundTrip(admissionRequest(t, context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
	calls = 0
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Microsecond)
	defer cancel()
	response, err = transport.RoundTrip(admissionRequest(t, ctx))
	if response != nil {
		_ = response.Body.Close()
	}
	if calls > 1 {
		t.Fatalf("deadline extended; calls=%d err=%v", calls, err)
	}
}

type delayedQueryBody struct {
	first  bool
	closed chan struct{}
	once   sync.Once
}

func (b *delayedQueryBody) Read(p []byte) (int, error) {
	if !b.first {
		b.first = true
		return copy(p, "first"), nil
	}
	<-b.closed
	// Simulate a transport that returns data after cancellation.
	return copy(p, "late"), nil
}

func (b *delayedQueryBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func TestReadRetriesDeadlineClosesSuccessfulStreamAndRejectsLateBytes(t *testing.T) {
	body := &delayedQueryBody{closed: make(chan struct{})}
	pool, _ := newAdmissionTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: body}, nil
	}), ClientAdmission{MaxInFlight: 1})
	transport, _ := NewReadRetryTransport(pool, retryPolicy())
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader(`{"timeout_ms":40}`))
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	buffer := make([]byte, 16)
	if n, err := response.Body.Read(buffer); err != nil || string(buffer[:n]) != "first" {
		t.Fatalf("first chunk: %q, %v", buffer[:n], err)
	}
	if len(pool.active) != 1 {
		t.Fatal("stream did not retain admission")
	}
	result := make(chan error, 1)
	go func() {
		n, err := response.Body.Read(buffer)
		if n != 0 {
			result <- errors.New("late bytes escaped deadline")
			return
		}
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not close blocked response")
	}
	if len(pool.active) != 0 {
		t.Fatal("deadline did not release admission")
	}
}

func TestReadRetriesDiscardLateSuccessfulHeaders(t *testing.T) {
	calls := 0
	closed := false
	transport, _ := NewReadRetryTransport(admissionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		<-req.Context().Done()
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: &closeTrackingBody{closed: &closed}}, nil
	}), retryPolicy())
	req, _ := http.NewRequest(http.MethodPost, "http://test/db/v1/query", strings.NewReader(`{"timeout_ms":20}`))
	response, err := transport.RoundTrip(req)
	if response != nil || !errors.Is(err, context.DeadlineExceeded) || !closed || calls != 1 {
		t.Fatalf("response=%v err=%v closed=%v calls=%d", response, err, closed, calls)
	}
}

type closeTrackingBody struct{ closed *bool }

func (b *closeTrackingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *closeTrackingBody) Close() error {
	*b.closed = true
	return nil
}
