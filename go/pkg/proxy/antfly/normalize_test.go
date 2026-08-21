package proxy

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNormalizeSuccessfulResponseDoesNotReadUnknownResponseKind(t *testing.T) {
	body := &readTrackingBody{}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       body,
	}

	normalizedBody, normalized, err := normalizeSuccessfulResponse(resp, RequestContext{
		BackendPath: "/query",
	})
	if err != nil {
		t.Fatalf("normalize response: %v", err)
	}
	if normalized {
		t.Fatal("ordinary query response unexpectedly marked normalized")
	}
	if normalizedBody != nil {
		t.Fatalf("ordinary query response unexpectedly buffered: %q", normalizedBody)
	}
	if body.reads != 0 {
		t.Fatalf("ordinary query response body read %d times before forwarding", body.reads)
	}
}

func TestHTTPBackendForwarderWritesUnknownResponseHeadersBeforeReadingBody(t *testing.T) {
	body := &blockingReadBody{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	})}
	forwarder := HTTPBackendForwarder{Client: client}
	w := &headerTrackingWriter{header: make(http.Header)}
	req, err := http.NewRequest(http.MethodPost, "http://proxy.test/query", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	done := make(chan error, 1)

	go func() {
		done <- forwarder.Forward(
			w,
			req,
			"http://upstream.test",
			nil,
			RequestContext{BackendPath: "/query"},
			NamespaceRoute{},
		)
	}()

	select {
	case <-body.started:
	case err := <-done:
		t.Fatalf("forwarding completed before reading the response body: %v", err)
	}
	if !w.wroteHeader.Load() {
		close(body.release)
		<-done
		t.Fatal("ordinary query body was read before downstream headers were written")
	}
	close(body.release)
	if err := <-done; err != nil {
		t.Fatalf("forward response: %v", err)
	}
}

type readTrackingBody struct {
	reads int
}

func (b *readTrackingBody) Read([]byte) (int, error) {
	b.reads++
	return 0, io.EOF
}

func (b *readTrackingBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type blockingReadBody struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingReadBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return 0, io.EOF
}

func (b *blockingReadBody) Close() error { return nil }

type headerTrackingWriter struct {
	header      http.Header
	wroteHeader atomic.Bool
}

func (w *headerTrackingWriter) Header() http.Header { return w.header }

func (w *headerTrackingWriter) WriteHeader(int) { w.wroteHeader.Store(true) }

func (w *headerTrackingWriter) Write(data []byte) (int, error) { return len(data), nil }
