package sdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
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
