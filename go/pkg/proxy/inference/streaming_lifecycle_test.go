package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestFramedRejectionFinalizesUploadBeforeAndAfterRoutingPrefix(t *testing.T) {
	for _, invalidPrefix := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid-prefix=%v", invalidPrefix), func(t *testing.T) {
			body := testProxyAttachmentEnvelope([]byte(`{"images":[{"url":"attachment:0"}]}`), testProxyAttachment{mime: "image/png", data: []byte{1}})
			prefix, _, err := readProxyAttachmentRoutingPrefix(bytes.NewReader(body), int64(len(body)), int64(len(body)))
			if err != nil {
				t.Fatal(err)
			}
			if invalidPrefix {
				prefix[0] ^= 0xff
			}
			p := NewProxy(Config{Logger: zap.NewNop()})
			settled := make(chan struct{})
			var once sync.Once
			server := httptest.NewUnstartedServer(http.HandlerFunc(p.handleRead))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateIdle || state == http.StateClosed {
					once.Do(func() { close(settled) })
				}
			}
			server.Start()
			defer server.Close()
			conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			if _, err := fmt.Fprintf(conn, "POST /ai/v1/read HTTP/1.1\r\nHost: localhost\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n", proxyAttachmentEnvelopeContentType, len(body)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(prefix); err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q error=%v", resp.StatusCode, data, err)
			}
			select {
			case <-settled:
			case <-time.After(time.Second):
				t.Fatal("server is still draining the unsent attachment after rejecting the request")
			}
		})
	}
}

func TestIncomingUploadFinalizationPreservesFullyReadConnections(t *testing.T) {
	interrupts := 0
	upload := &proxyIncomingUpload{reader: strings.NewReader("body"), total: 4, interruptRead: func() { interrupts++ }}
	if _, err := io.Copy(io.Discard, upload); err != nil {
		t.Fatal(err)
	}
	upload.finish()
	if interrupts != 0 {
		t.Fatal("complete upload must retain keep-alive")
	}
	upload.interrupt()
	upload.interrupt()
	if interrupts != 1 {
		t.Fatal("interruption must be idempotent")
	}
}

func TestFramedCompleteUploadsReuseDownstreamConnection(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "cpu"}, Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://reader.internal", "cpu", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://reader.internal", "read", "owner/reader")
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
	})}
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(p.handleRead))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	defer client.CloseIdleConnections()
	body := testProxyAttachmentEnvelope([]byte(`{"model":"owner/reader","images":[{"url":"attachment:0"}]}`), testProxyAttachment{mime: "image/png", data: []byte{1}})
	for range 2 {
		resp, err := client.Post(server.URL+"/ai/v1/read", proxyAttachmentEnvelopeContentType, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || string(data) != "ok" {
			t.Fatalf("status=%d body=%q error=%v", resp.StatusCode, data, err)
		}
	}
	if connections.Load() != 1 {
		t.Fatalf("complete uploads opened %d connections", connections.Load())
	}
}

func TestFramedEarlyResponseBodySurvivesUploadInterrupt(t *testing.T) {
	metadata := []byte(`{"model":"owner/reader","images":[{"url":"attachment:0"}]}`)
	body := testProxyAttachmentEnvelope(metadata, testProxyAttachment{mime: "image/png", data: []byte{1}})
	prefix, _, err := readProxyAttachmentRoutingPrefix(bytes.NewReader(body), int64(len(body)), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			t.Error(err)
			return
		}
		if _, err := io.CopyN(io.Discard, r.Body, int64(len(prefix))); err != nil {
			return
		}
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusForbidden)
		w.(http.Flusher).Flush()
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, "denied")
	}))
	defer upstream.Close()
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "cpu"}, Logger: zap.NewNop()})
	p.registry.RegisterEndpoint(upstream.URL, "cpu", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, upstream.URL, "read", "owner/reader")
	p.registry.client = upstream.Client()
	server := httptest.NewServer(http.HandlerFunc(p.handleRead))
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = fmt.Fprintf(conn, "POST /ai/v1/read HTTP/1.1\r\nHost: localhost\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n", proxyAttachmentEnvelopeContentType, len(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write(prefix); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 403 || string(got) != "denied" {
		t.Fatalf("response status=%d body=%q error=%v", response.StatusCode, got, err)
	}
}

type contextResponseReader struct {
	ctx context.Context
	io.Reader
}

func (r contextResponseReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func TestForwardAttemptLivesThroughResponseAndHonorsCallerCancellation(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		p := NewProxy(Config{Logger: zap.NewNop()})
		var upstreamCtx context.Context
		p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			upstreamCtx = req.Context()
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(contextResponseReader{req.Context(), strings.NewReader("ok")}), Request: req}, nil
		})}
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodPost, "/ai/v1/read", nil).WithContext(ctx)
		resp, err := p.forwardRequestWithRetry(req, &proxyMemoryReplayBody{}, &Endpoint{address: "http://reader.internal"}, &Route{RetryTimeout: time.Second}, false)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if upstreamCtx.Err() != nil {
			t.Error("attempt canceled before response copy")
		}
		if cancelCaller {
			cancel()
		}
		body, err := io.ReadAll(resp.Body)
		if cancelCaller && !errors.Is(err, context.Canceled) {
			t.Errorf("caller cancellation lost: %v", err)
		}
		if !cancelCaller && (err != nil || string(body) != "ok") {
			t.Errorf("response=%q err=%v", body, err)
		}
		_ = resp.Body.Close()
		if !errors.Is(upstreamCtx.Err(), context.Canceled) {
			t.Error("response close did not release attempt")
		}
		cancel()
	}
}
