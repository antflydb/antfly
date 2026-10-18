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

package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestInferenceHopBudgetsOnlyReduceServerPolicy(t *testing.T) {
	for _, test := range []struct {
		header           string
		configured, want int
	}{{"", 100, 8}, {"2", 5, 2}, {"0", 5, 0}, {"100", 2, 2}, {"-1", 3, 3}, {"invalid", 3, 3}} {
		r := httptest.NewRequest("POST", "/ai/v1/generate", nil)
		r.Header.Set("X-Antfly-Inference-Attempts-Remaining", test.header)
		if got := inferenceAttemptLimit(r, test.configured); got != test.want {
			t.Fatalf("%q: got %d want %d", test.header, got, test.want)
		}
	}
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := httptest.NewRequest("POST", "/ai/v1/generate", nil).WithContext(parent)
	r.Header.Set("X-Antfly-Inference-Timeout-Ms", "600000")
	child, stop := inferenceRequestContext(r)
	defer stop()
	deadline, _ := child.Deadline()
	original, _ := parent.Deadline()
	if deadline.After(original) {
		t.Fatal("hop extended deadline")
	}
	r.Header.Set("X-Antfly-Inference-Timeout-Ms", "0")
	zero, stopZero := inferenceRequestContext(r)
	defer stopZero()
	if zero.Err() != context.DeadlineExceeded {
		t.Fatal("zero budget must expire immediately")
	}
}
func TestTranscriptionSessionsPinEndpointCredentialAndIncarnation(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "default"}, Logger: zap.NewNop()})
	var calls atomic.Int32
	var owner string
	p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if owner == "" {
			owner = r.URL.Host
		} else if r.URL.Host != owner {
			t.Fatalf("session moved from %s to %s", owner, r.URL.Host)
		}
		if r.Header.Get("Authorization") != "Bearer org-a" {
			t.Fatal("credential changed")
		}
		if r.Header.Get("X-Antfly-Inference-Attempts-Remaining") != "1" {
			t.Fatal("hop retries unbounded")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"object":"transcription.session","id":"runtime-session"}`)), Request: r}, nil
	})}
	for _, address := range []string{"http://runtime-a", "http://runtime-b"} {
		p.RegisterEndpoint(address, "default", WorkloadTypeGeneral)
		advertiseModelOperation(p.registry, address, "transcription.sessions", "whisper")
	}
	send := func(method, path, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+key)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		p.handleTranscriptionSession(w, r)
		return w
	}
	created := send("POST", "/ai/v1/transcription/sessions", "org-a", `{"model":"whisper"}`)
	if created.Code != 200 {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var value struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if value.ID == "runtime-session" || value.ID == "" {
		t.Fatal("handle not pinned")
	}
	path := "/ai/v1/transcription/sessions/" + value.ID
	denied := send("GET", path, "org-b", "")
	if denied.Code != 404 || calls.Load() != 1 {
		t.Fatal("cross-credential session visible")
	}
	found := send("GET", path, "org-a", "")
	if found.Code != 200 || !strings.Contains(found.Body.String(), value.ID) || strings.Contains(found.Body.String(), "runtime-session") {
		t.Fatalf("get: %d %s", found.Code, found.Body.String())
	}
	address := "http://" + owner
	p.registry.UnregisterEndpoint(address)
	p.RegisterEndpoint(address, "default", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "transcription.sessions", "whisper")
	stale := send("GET", path, "org-a", "")
	if stale.Code != 409 || calls.Load() != 2 {
		t.Fatalf("replacement reused session: %d %s", stale.Code, stale.Body.String())
	}
}
func TestSessionEventRewritingPreservesPublicHandle(t *testing.T) {
	response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("event: session.open\ndata: {\"type\":\"session.open\",\"session_id\":\"private\"}\n\ndata: [DONE]\n\n"))}
	if err := rewriteSessionResponse(response, "public"); err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body strings.Builder
	buffer := make([]byte, 3)
	for {
		n, err := response.Body.Read(buffer)
		body.Write(buffer[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(body.String(), "private") || !strings.Contains(body.String(), `"session_id":"public"`) || !strings.Contains(body.String(), "[DONE]") {
		t.Fatal(body.String())
	}
}

func TestGenerateBatchPreservesLogicalDeadline(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, timeout := range []string{"0", "80"} {
			t.Run(fmt.Sprintf("mixed=%t/timeout=%s", mixed, timeout), func(t *testing.T) {
				p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
				var calls atomic.Int32
				p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					if timeout == "0" {
						t.Error("expired batch reached upstream")
					}
					deadline, ok := r.Context().Deadline()
					if !ok || time.Until(deadline) > 80*time.Millisecond {
						t.Error("route timeout extended batch deadline")
					}
					remaining, err := strconv.Atoi(r.Header.Get("X-Antfly-Inference-Timeout-Ms"))
					if err != nil || remaining > 80 {
						t.Errorf("forwarded timeout: %q", r.Header.Get("X-Antfly-Inference-Timeout-Ms"))
					}
					<-r.Context().Done()
					return nil, r.Context().Err()
				})}
				p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
				advertiseModelOperation(p.registry, "http://runtime.internal", "generate.batch", "model-a", "model-b")
				if _, err := p.Router().RouteManager().UpsertRoute(&Route{
					Namespace: "default", Name: "deadline", Operations: map[OperationType]bool{"generate.batch": true},
					Destinations: []Destination{{Pool: "primary", Weight: 100}}, RetryTimeout: time.Second,
				}); err != nil {
					t.Fatal(err)
				}
				second := "model-a"
				if mixed {
					second = "model-b"
				}
				body := fmt.Sprintf(`{"requests":[{"custom_id":"a","body":{"model":"model-a"}},{"custom_id":"b","body":{"model":%q}}]}`, second)
				r := httptest.NewRequest("POST", "/ai/v1/generate/batch", strings.NewReader(body))
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("X-Antfly-Inference-Timeout-Ms", timeout)
				w := httptest.NewRecorder()
				p.handleGenerateBatch(w, r)
				if timeout == "0" && (calls.Load() != 0 || w.Code != http.StatusGatewayTimeout) {
					t.Fatalf("expired batch: status=%d calls=%d", w.Code, calls.Load())
				}
				if timeout != "0" && calls.Load() == 0 {
					t.Fatalf("positive budget never reached upstream: %d %s", w.Code, w.Body.String())
				}
			})
		}
	}
}

func TestFramedSessionFollowupsPreserveOwnerAndIncarnation(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "default"}, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://owner.internal", "default", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://owner.internal", "transcription.sessions", "whisper")
	p.RegisterEndpoint("http://other.internal", "default", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://other.internal", "transcription.sessions", "other")
	var calls int
	p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "owner.internal" {
			t.Errorf("session moved to %s", r.URL.Host)
		}
		if r.Header.Get("Authorization") != "Bearer owner" {
			t.Error("session credential changed")
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"backend"}`)), Request: r}, nil
	})}
	create := httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper"}`))
	create.Header.Set("Authorization", "Bearer owner")
	created := httptest.NewRecorder()
	p.handleTranscriptionSession(created, create)
	var session struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &session); err != nil || session.ID == "" {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	send := func(metadata, credential string) *httptest.ResponseRecorder {
		body := testProxyAttachmentEnvelope([]byte(metadata), testProxyAttachment{mime: "audio/wav", data: []byte{1, 2, 3}})
		r := httptest.NewRequest("POST", "/ai/v1/transcription/sessions/"+session.ID+"/audio", bytes.NewReader(body))
		r.Header.Set("Content-Type", proxyAttachmentEnvelopeContentType)
		r.Header.Set("Authorization", "Bearer "+credential)
		w := httptest.NewRecorder()
		p.handleTranscriptionSession(w, r)
		return w
	}
	for _, metadata := range []string{`{"audio":"attachment:0"}`, `{"model":"other","audio":"attachment:0"}`} {
		if w := send(metadata, "owner"); w.Code != 200 {
			t.Fatalf("follow-up: %d %s", w.Code, w.Body.String())
		}
	}
	if w := send(`{"model":"other"}`, "intruder"); w.Code != 404 || calls != 3 {
		t.Fatalf("credential isolation: status=%d calls=%d", w.Code, calls)
	}
	p.registry.UnregisterEndpoint("http://owner.internal")
	p.RegisterEndpoint("http://owner.internal", "default", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://owner.internal", "transcription.sessions", "whisper")
	if w := send(`{"model":"other","audio":"attachment:0"}`, "owner"); w.Code != 409 || calls != 3 {
		t.Fatalf("replacement: status=%d calls=%d", w.Code, calls)
	}
}
