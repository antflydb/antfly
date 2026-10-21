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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestCatalogAdaptersPreserveDiscoveryMetadata(t *testing.T) {
	for _, path := range []string{"/ai/v1/models", "/ml/v1/models", "/ml/v1/predictors"} {
		for _, scenario := range []string{"unscoped", "scoped", "lease capacity", "expired deadline"} {
			t.Run(path+"/"+scenario, func(t *testing.T) {
				p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
				p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"predictors":{"risk":{"task":"regression","num_features":2,"num_outputs":1}}}`)), Request: r}, nil
				})}
				const address = "http://runtime.internal"
				p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
				advertiseModelOperation(p.registry, address, "predict", "risk")
				if scenario == "lease capacity" {
					endpoints := capabilityEndpointSet(p.registry, p.router.ResolveEndpointCandidates("risk", "primary", nil, "predict"))
					for i := 0; i < maxCapabilityLeases; i++ {
						if _, err := p.issueCapabilityLease("risk", "predict", "predict", p.router.RouteManager().Generation(), fmt.Sprintf("revision-%d", i), "", endpoints); err != nil {
							t.Fatal(err)
						}
					}
				}
				url := path
				if scenario != "unscoped" {
					url += "?model=risk&task=predict"
				}
				request := httptest.NewRequest("GET", url, nil)
				request.Header.Set("Origin", "https://console.example")
				if scenario == "expired deadline" {
					request.Header.Set("X-Antfly-Inference-Timeout-Ms", "0")
				}
				w := httptest.NewRecorder()
				if strings.HasPrefix(path, "/ml/") {
					p.handlePredictors(w, request)
				} else {
					p.handleModels(w, request)
				}
				wantStatus := http.StatusOK
				if scenario == "lease capacity" {
					wantStatus = http.StatusServiceUnavailable
				} else if scenario == "expired deadline" {
					wantStatus = http.StatusGatewayTimeout
				}
				if w.Code != wantStatus {
					t.Fatalf("discovery status=%d, want %d: %s", w.Code, wantStatus, w.Body.String())
				}
				exposed := make(map[string]bool)
				for _, value := range w.Header().Values("Access-Control-Expose-Headers") {
					for _, name := range strings.Split(value, ",") {
						exposed[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
					}
				}
				for _, name := range []string{capabilityTokenHeader, capabilityRevisionHeader} {
					if scenario == "scoped" {
						if w.Header().Get(name) == "" || !exposed[name] {
							t.Errorf("lease header %s must be present and browser-readable: %v", name, w.Header())
						}
					} else if w.Header().Get(name) != "" || exposed[name] {
						t.Errorf("%s catalog published lease metadata: %v", scenario, w.Header())
					}
				}
				if wantStatus == http.StatusOK {
					if w.Header().Get("Content-Type") != "application/json" {
						t.Errorf("catalog content type: %v", w.Header())
					}
				} else if w.Header().Get("Content-Type") != "text/plain; charset=utf-8" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
					t.Errorf("error metadata lost: %v", w.Header())
				}
				if scenario == "lease capacity" && w.Header().Get("Retry-After") != "1" {
					t.Errorf("capacity retry guidance lost: %v", w.Header())
				}
			})
		}
	}
}

func TestPredictorCatalogRequiresNativeCompatibleSchemas(t *testing.T) {
	for _, model := range []string{"predictors/risk", "connections/remote/models/predictors/risk"} {
		valid := `{"task":"regression","num_features":2,"num_outputs":1,"feature_names":["first","second"],"model_identity":"risk-v1"}`
		for _, test := range []struct {
			name, second string
			scopedStatus int
			present      bool
		}{
			{"matching", valid, 200, true},
			{"different features", `{"task":"regression","num_features":2,"num_outputs":1,"feature_names":["second","first"],"model_identity":"risk-v1"}`, 503, false},
			{"generic descriptor", `{"task":"predict","id":"predictors/risk"}`, 200, false},
		} {
			t.Run(model+"/"+test.name, func(t *testing.T) {
				p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
				p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					schema := valid
					if r.URL.Host == "second.internal" {
						schema = test.second
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"predictors":{` + strconv.Quote(model) + `:` + schema + `}}`)), Request: r}, nil
				})}
				for _, address := range []string{"http://first.internal", "http://second.internal"} {
					p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
					advertiseModelOperation(p.registry, address, "predict", model)
				}
				w := httptest.NewRecorder()
				p.handlePredictors(w, httptest.NewRequest("GET", "/ml/v1/models", nil))
				if w.Code != 200 {
					t.Fatalf("inventory: %d %s", w.Code, w.Body.String())
				}
				var result struct {
					Predictors map[string]json.RawMessage `json:"predictors"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				schema, present := result.Predictors[model]
				if present != test.present {
					t.Fatalf("schema presence=%v: %s", present, w.Body.String())
				}
				if present && !equalPredictorSchema(schema, json.RawMessage(valid)) {
					t.Fatal("native schema lost")
				}
				w = httptest.NewRecorder()
				p.handlePredictors(w, httptest.NewRequest("GET", "/ml/v1/models?model="+url.QueryEscape(model)+"&task=predict", nil))
				if w.Code != test.scopedStatus {
					t.Fatalf("scoped inventory: %d %s", w.Code, w.Body.String())
				}
				if w.Code != 200 && w.Header().Get(capabilityTokenHeader) != "" {
					t.Fatal("incompatible schema received a lease")
				}
				if w.Code == 200 && w.Header().Get(capabilityTokenHeader) == "" {
					t.Fatal("compatible native schema did not receive a lease")
				}
			})
		}
	}
}

func TestInvalidPredictorSchemasCannotAdvertisePrediction(t *testing.T) {
	for _, schema := range []string{`{}`, `{"task":"predict"}`, `{"task":"regression","num_features":2,"num_outputs":1,"feature_names":["only one"]}`, `{"task":"regression","num_features":2.5,"num_outputs":1}`, `{"task":"regression","num_features":2,"num_outputs":1,"unavailable":true}`} {
		if validPredictorSchema(json.RawMessage(schema)) || equalPredictorSchema(json.RawMessage(schema), json.RawMessage(schema)) {
			t.Fatalf("accepted malformed schema %s", schema)
		}
		operations, err := extractModelOperations(strings.NewReader(`{"predictors":{"bad":` + schema + `}}`))
		if err != nil {
			t.Fatal(err)
		}
		if operations["bad"]["predict"] {
			t.Fatalf("malformed schema advertised prediction: %s", schema)
		}
	}
}

func TestMergedCatalogPreservesPhysicalTaskRoles(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
	p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"embedders":{"connections/runtime/models/sparse":{},"connections/runtime/models/shared":{}},"generators":{"connections/runtime/models/shared":{}},"deciders":{"connections/runtime/models/decision":{}}}`)), Request: r}, nil
	})}
	w := httptest.NewRecorder()
	p.handleModels(w, httptest.NewRequest("GET", "/ai/v1/models", nil))
	if w.Code != 200 {
		t.Fatalf("catalog: %d %s", w.Code, w.Body.String())
	}
	var result struct {
		Data []struct {
			ID    string   `json:"id"`
			Task  string   `json:"task"`
			Tasks []string `json:"tasks"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 3 {
		t.Fatalf("lost model entries: %s", w.Body.String())
	}
	for _, item := range result.Data {
		if strings.HasSuffix(item.ID, "/sparse") && (item.Task != "embed" || len(item.Tasks) != 1) {
			t.Fatalf("lost embedding role: %+v", item)
		}
		if strings.HasSuffix(item.ID, "/decision") && (item.Task != "decide" || len(item.Tasks) != 1) {
			t.Fatalf("lost decision role: %+v", item)
		}
		if strings.HasSuffix(item.ID, "/shared") && (item.Task != "" || len(item.Tasks) != 2) {
			t.Fatalf("guessed ambiguous task: %+v", item)
		}
	}
}

func TestDiscoverySharesCallerDeadlineAcrossAIScopedAndMLCatalogs(t *testing.T) {
	for _, path := range []string{"/ai/v1/models", "/ai/v1/models?model=example&task=embed", "/ml/v1/models?model=risk&task=predict"} {
		t.Run(path, func(t *testing.T) {
			p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
			for _, address := range []string{"http://first.internal", "http://second.internal"} {
				p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
				advertiseModelOperation(p.registry, address, "embed", "example")
				advertiseModelOperation(p.registry, address, "predict", "risk")
			}
			var calls atomic.Int32
			p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				budget, err := strconv.Atoi(r.Header.Get("X-Antfly-Inference-Timeout-Ms"))
				if err != nil || budget < 1 || budget > 20 {
					t.Errorf("invalid remaining budget %q", r.Header.Get("X-Antfly-Inference-Timeout-Ms"))
				}
				if _, ok := r.Context().Deadline(); !ok {
					t.Error("discovery transport has no deadline")
				}
				if r.Header.Get("X-Antfly-Inference-Attempts-Remaining") != "1" {
					t.Error("missing single-hop policy")
				}
				select {
				case <-r.Context().Done():
					return nil, r.Context().Err()
				case <-time.After(time.Second):
					return nil, fmt.Errorf("discovery did not cancel")
				}
			})}
			request := httptest.NewRequest("GET", path, nil)
			request.Header.Set("X-Antfly-Inference-Timeout-Ms", "20")
			w := httptest.NewRecorder()
			start := time.Now()
			if strings.HasPrefix(path, "/ml/") {
				p.handlePredictors(w, request)
			} else {
				p.handleModels(w, request)
			}
			if w.Code != 504 {
				t.Fatalf("deadline response: %d %s", w.Code, w.Body.String())
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("discovery exceeded its shared budget")
			}
			if calls.Load() == 0 {
				t.Fatal("did not exercise transport cancellation")
			}
			if w.Header().Get(capabilityTokenHeader) != "" {
				t.Fatal("expired discovery minted a lease")
			}
			// A timed-out request must not poison the next caller's discovery snapshot.
			p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"embedders":{"example":{}},"predictors":{"risk":{"task":"regression","num_features":2,"num_outputs":1}}}`)), Request: r}, nil
			})}
			w = httptest.NewRecorder()
			request = httptest.NewRequest("GET", path, nil)
			if strings.HasPrefix(path, "/ml/") {
				p.handlePredictors(w, request)
			} else {
				p.handleModels(w, request)
			}
			if w.Code != 200 {
				t.Fatalf("recovery: %d %s", w.Code, w.Body.String())
			}
			// An expired request may not use that now-hot snapshot or mint another lease.
			w = httptest.NewRecorder()
			request = httptest.NewRequest("GET", path, nil)
			request.Header.Set("X-Antfly-Inference-Timeout-Ms", "0")
			if strings.HasPrefix(path, "/ml/") {
				p.handlePredictors(w, request)
			} else {
				p.handleModels(w, request)
			}
			if w.Code != 504 || w.Header().Get(capabilityTokenHeader) != "" {
				t.Fatalf("expired hot catalog: %d", w.Code)
			}
		})
	}
}

func TestDiscoveryDeadlineIncludesByteAdmission(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
	limit := p.catalogAdmission.Limit()
	if err := p.catalogAdmission.Acquire(context.Background(), limit); err != nil {
		t.Fatal(err)
	}
	defer p.catalogAdmission.Release(limit)
	p.registry.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("transport entered before admission")
		return nil, fmt.Errorf("unexpected transport")
	})}
	r := httptest.NewRequest("GET", "/ai/v1/models", nil)
	r.Header.Set("X-Antfly-Inference-Timeout-Ms", "10")
	w := httptest.NewRecorder()
	p.handleModels(w, r)
	if w.Code != 504 {
		t.Fatalf("admission deadline: %d %s", w.Code, w.Body.String())
	}
}

func TestSessionJSONEnvelopesValidatedBeforePinningAndRewriting(t *testing.T) {
	for _, body := range []string{`[]`, `null`, `true`, `42`, `"session"`, `{`, ``, `{"id":null}`, `{"id":1}`, `{"id":""}`, `{"id":".."}`, `{"id":"a\nb"}`, `{"id":"first","session_id":"second"}`} {
		if _, _, err := decodeSessionEnvelope([]byte(body)); err == nil {
			t.Fatalf("accepted malformed envelope %s", body)
		}
		response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
		if err := rewriteSessionResponse(response, "public-session"); err == nil && body != "" {
			t.Fatalf("rewrote malformed envelope %s", body)
		}
	}
	envelope, id, err := decodeSessionEnvelope([]byte(`{"id":"session-123","session_id":"session-123","extra":true}`))
	if err != nil || id != "session-123" || string(envelope["extra"]) != "true" {
		t.Fatalf("valid envelope lost metadata: %v %s", err, id)
	}
}

func TestSessionCapacityReservedBeforeUpstreamAndReleasedOnFailure(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://runtime.internal", "transcription.sessions", "whisper")
	p.sessions = make(map[string]proxySession)
	for index := range 4096 {
		p.sessions[fmt.Sprint(index)] = proxySession{expires: time.Now().Add(time.Hour)}
	}
	var calls atomic.Int32
	malformed := true
	p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		body := `{"id":"backend-session"}`
		if malformed {
			body = `[]`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	send := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		p.handleTranscriptionSession(w, r)
		return w
	}
	if w := send(); w.Code != 429 || calls.Load() != 0 {
		t.Fatalf("full owner called upstream: status=%d calls=%d", w.Code, calls.Load())
	}
	p.sessions["0"] = proxySession{expires: time.Now().Add(-time.Second)}
	if w := send(); w.Code != 502 || len(p.sessions) != 4095 {
		t.Fatalf("failed create retained reservation: %d handles=%d", w.Code, len(p.sessions))
	}
	malformed = false
	if w := send(); w.Code != 200 || len(p.sessions) != 4096 {
		t.Fatalf("slot was not reusable: %d handles=%d", w.Code, len(p.sessions))
	}
	if w := send(); w.Code != 429 || calls.Load() != 2 {
		t.Fatalf("active owner exceeded capacity: status=%d calls=%d", w.Code, calls.Load())
	}
}

func TestConcurrentSessionCreateOwnsLastSlotUntilPublished(t *testing.T) {
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://runtime.internal", "transcription.sessions", "whisper")
	p.sessions = make(map[string]proxySession)
	for index := range 4095 {
		p.sessions[fmt.Sprint(index)] = proxySession{expires: time.Now().Add(time.Hour)}
	}
	entered := make(chan struct{}, 1)
	finish := make(chan struct{})
	var calls atomic.Int32
	p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) > 1 {
			return nil, fmt.Errorf("unexpected concurrent upstream creation")
		}
		entered <- struct{}{}
		select {
		case <-finish:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"backend-session"}`)), Request: r}, nil
	})}
	send := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		p.handleTranscriptionSession(w, r)
		return w
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- send() }()
	defer close(finish)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first creator never reached upstream")
	}
	p.sessionMu.Lock()
	var pendingID string
	for id, session := range p.sessions {
		if session.pending {
			pendingID = id
		}
	}
	p.sessionMu.Unlock()
	if pendingID == "" {
		t.Fatal("upstream create has no reserved handle")
	}
	w := httptest.NewRecorder()
	p.handleTranscriptionSession(w, httptest.NewRequest("GET", "/ai/v1/transcription/sessions/"+pendingID, nil))
	if w.Code != 404 {
		t.Fatalf("pending handle became public: %d", w.Code)
	}
	if w := send(); w.Code != 429 || calls.Load() != 1 {
		t.Fatalf("concurrent creator bypassed reservation: %d calls=%d", w.Code, calls.Load())
	}
	finish <- struct{}{}
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatalf("first create failed: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first creator did not publish")
	}
}

func TestSessionRawStreamIsDuplexAndPreservesTransport(t *testing.T) {
	for _, knownLength := range []bool{false, true} {
		t.Run(fmt.Sprint("known_length=", knownLength), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ai/v1/transcription/sessions" {
					_, _ = io.WriteString(w, `{"id":"backend"}`)
					return
				}
				if r.URL.Path != "/ai/v1/transcription/sessions/backend/stream" || r.URL.RawQuery != "format=pcm_f32&sample_rate=48000&commit=false" || r.Header.Get("Content-Type") != "application/octet-stream" {
					t.Errorf("transport changed: %s %s", r.URL, r.Header.Get("Content-Type"))
					w.WriteHeader(400)
					return
				}
				_ = http.NewResponseController(w).EnableFullDuplex()
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"session.open\",\"session_id\":\"backend\"}\n\n")
				_ = http.NewResponseController(w).Flush()
				var first [4]byte
				if _, err := io.ReadFull(r.Body, first[:]); err != nil {
					t.Errorf("first audio: %v", err)
					return
				}
				_, _ = io.WriteString(w, "data: {\"type\":\"partial\",\"session_id\":\"backend\"}\n\n")
				_ = http.NewResponseController(w).Flush()
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Errorf("remaining audio: %v", err)
					return
				}
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}))
			defer upstream.Close()
			p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "default"}, Logger: zap.NewNop()})
			p.registry.RegisterEndpoint(upstream.URL, "default", WorkloadTypeGeneral)
			advertiseModelOperation(p.registry, upstream.URL, "transcription.sessions", "whisper")
			server := httptest.NewServer(http.HandlerFunc(p.handleTranscriptionSession))
			defer server.Close()
			created, err := http.Post(server.URL+"/ai/v1/transcription/sessions", "application/json", strings.NewReader(`{"model":"whisper"}`))
			if err != nil {
				t.Fatal(err)
			}
			var value struct {
				ID string `json:"id"`
			}
			err = json.NewDecoder(created.Body).Decode(&value)
			created.Body.Close()
			if err != nil || created.StatusCode != 200 {
				t.Fatalf("create %d %v", created.StatusCode, err)
			}
			reader, writer := io.Pipe()
			defer writer.Close()
			defer reader.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/ai/v1/transcription/sessions/"+value.ID+"/stream?format=pcm_f32&sample_rate=48000&commit=false", reader)
			if err != nil {
				t.Fatal(err)
			}
			if knownLength {
				request.ContentLength = 8
			}
			request.Header.Set("Content-Type", "application/octet-stream")
			go func() { _, _ = writer.Write([]byte{0, 0, 0, 0}) }()
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			lines := bufio.NewReader(response.Body)
			opening, err := lines.ReadString('\n')
			if err != nil || !strings.Contains(opening, value.ID) {
				t.Fatalf("opening before audio: %q %v", opening, err)
			}
			_, _ = lines.ReadString('\n')
			partial, err := lines.ReadString('\n')
			if err != nil || !strings.Contains(partial, "partial") || !strings.Contains(partial, value.ID) {
				t.Fatalf("partial before EOF: %q %v", partial, err)
			}
			if _, err = writer.Write([]byte{0, 0, 0, 0}); err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			tail, err := io.ReadAll(lines)
			if err != nil || !bytes.Contains(tail, []byte("[DONE]")) {
				t.Fatalf("finish: %q %v", tail, err)
			}
		})
	}
}

func TestSessionRawStreamEarlyRejectionReleasesBlockedUpload(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/sessions") {
			_, _ = io.WriteString(w, `{"id":"backend"}`)
			return
		}
		_ = http.NewResponseController(w).EnableFullDuplex()
		w.Header().Set("Connection", "close")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"session busy"}`)
		_ = http.NewResponseController(w).Flush()
	}))
	defer upstream.Close()
	p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "default"}, Logger: zap.NewNop()})
	p.registry.RegisterEndpoint(upstream.URL, "default", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, upstream.URL, "transcription.sessions", "whisper")
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stream") {
			defer close(finished)
		}
		p.handleTranscriptionSession(w, r)
	}))
	defer server.Close()
	created, err := http.Post(server.URL+"/ai/v1/transcription/sessions", "application/json", strings.NewReader(`{"model":"whisper"}`))
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(created.Body).Decode(&value)
	created.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	defer writer.Close()
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/ai/v1/transcription/sessions/"+value.ID+"/stream", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	go func() { _, _ = writer.Write([]byte{0, 0, 0, 0}) }()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 409 || !bytes.Contains(body, []byte("session busy")) {
		t.Fatalf("early response: %d %q %v", response.StatusCode, body, err)
	}
	// The pipe remains open: cleanup must interrupt and join the peer read itself.
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("proxy retained rejected upload")
	}
}

func TestSessionLeasesFenceStaleCompletion(t *testing.T) {
	p := &Proxy{sessions: make(map[string]proxySession)}
	now := time.Now()
	p.sessions["id"] = proxySession{expires: now.Add(time.Second), ttl: time.Second, generation: new(byte)}
	_, finish, ok := p.acquireSession("id", [32]byte{}, now)
	if !ok {
		t.Fatal("acquisition failed")
	}
	_, watch, ok := p.acquireSession("id", [32]byte{}, now.Add(2*time.Second))
	if !ok {
		t.Fatal("active session expired")
	}
	finish(now.Add(3*time.Second), true)
	watch(now.Add(4*time.Second), false)
	finish(now.Add(10*time.Second), true)
	if value := p.sessions["id"]; value.active != 0 || !value.expires.Equal(now.Add(4*time.Second)) {
		t.Fatalf("completion: %+v", value)
	}
	_, stale, ok := p.acquireSession("id", [32]byte{}, now.Add(3*time.Second))
	if !ok {
		t.Fatal("renewed session disappeared")
	}
	p.sessions["id"] = proxySession{expires: now.Add(time.Hour), ttl: time.Second, active: 1, generation: new(byte)}
	stale(now.Add(2*time.Hour), true)
	if value := p.sessions["id"]; value.active != 1 || !value.expires.Equal(now.Add(time.Hour)) {
		t.Fatalf("stale completion modified replacement: %+v", value)
	}
	_, removed, ok := p.acquireSession("id", [32]byte{}, now)
	if !ok {
		t.Fatal("replacement unavailable")
	}
	delete(p.sessions, "id")
	removed(now, true)
	if len(p.sessions) != 0 {
		t.Fatal("deleted handle revived")
	}
}

type brokenSessionResponse struct{ io.Reader }

func (b brokenSessionResponse) Read(dest []byte) (int, error) {
	n, err := b.Reader.Read(dest)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
func (brokenSessionResponse) Close() error { return nil }

func TestSessionAppendCompletionRenewsOnlySuccessAndSurvivesSweeps(t *testing.T) {
	for _, action := range []string{"audio", "stream"} {
		for _, outcome := range []string{"success", "upstream failure", "transport failure", "canceled"} {
			t.Run(action+"/"+outcome, func(t *testing.T) {
				p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
				p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
				advertiseModelOperation(p.registry, "http://runtime.internal", "transcription.sessions", "whisper")
				entered, release := make(chan struct{}), make(chan struct{})
				p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/ai/v1/transcription/sessions" {
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"backend-session"}`)), Request: r}, nil
					}
					close(entered)
					<-release
					content := `{"session_id":"backend-session","events":[]}`
					header := make(http.Header)
					if action == "stream" {
						content = "data: {\"session_id\":\"backend-session\"}\n\n"
						header.Set("Content-Type", "text/event-stream")
					}
					var body io.ReadCloser = io.NopCloser(strings.NewReader(content))
					status := 200
					if outcome == "upstream failure" {
						status = 503
					}
					if outcome == "transport failure" {
						body = brokenSessionResponse{Reader: strings.NewReader(content)}
					}
					return &http.Response{StatusCode: status, Header: header, Body: body, Request: r}, nil
				})}
				created := httptest.NewRecorder()
				p.handleTranscriptionSession(created, httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper"}`)))
				var envelope struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(created.Body.Bytes(), &envelope); err != nil || envelope.ID == "" {
					t.Fatalf("create: %d %s", created.Code, created.Body.String())
				}
				id := envelope.ID
				p.sessionMu.Lock()
				session := p.sessions[id]
				session.ttl = time.Minute
				p.sessions[id] = session
				p.sessionMu.Unlock()
				done := make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() {
					defer close(done)
					p.handleTranscriptionSession(httptest.NewRecorder(), httptest.NewRequest("POST", "/ai/v1/transcription/sessions/"+id+"/"+action, strings.NewReader(`{"audio":"aGk="}`)).WithContext(ctx))
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("append never reached upstream")
				}
				p.sessionMu.Lock()
				session = p.sessions[id]
				session.expires = time.Now().Add(-time.Second)
				p.sessions[id] = session
				for i := range 4095 {
					p.sessions[fmt.Sprint(i)] = proxySession{expires: time.Now().Add(time.Hour)}
				}
				p.sessionMu.Unlock()
				endpoint := p.registry.endpointAtIncarnation(session.address, session.incarnation)
				reservation, err := p.reserveSession(httptest.NewRequest("POST", "/ai/v1/transcription/sessions", nil), "whisper", endpoint, nil)
				if reservation != nil {
					reservation.release()
				}
				if err != errSessionCapacity {
					close(release)
					<-done
					t.Fatalf("active session swept: %v", err)
				}
				if outcome == "canceled" {
					cancel()
				}
				close(release)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("append did not finish")
				}
				p.sessionMu.Lock()
				current := p.sessions[id]
				p.sessionMu.Unlock()
				if current.active != 0 {
					t.Fatalf("leaked pin: %d", current.active)
				}
				if renewed := current.expires.After(time.Now()); renewed != (outcome == "success") {
					t.Fatalf("renewed=%v for %s", renewed, outcome)
				}
			})
		}
	}
}

func TestSessionCreationSeparatesRemainingExpiryFromRenewalTTL(t *testing.T) {
	for _, timestamp := range []struct {
		name    string
		present bool
		value   any
	}{
		{name: "missing"},
		{name: "valid", present: true},
		{name: "null", present: true},
		{name: "string", present: true, value: "123"},
		{name: "boolean", present: true, value: true},
		{name: "fractional", present: true, value: 1.5},
		{name: "negative", present: true, value: -1},
		{name: "overflow", present: true, value: json.Number("9223372036854775808")},
	} {
		t.Run(timestamp.name, func(t *testing.T) {
			p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
			p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
			advertiseModelOperation(p.registry, "http://runtime.internal", "transcription.sessions", "whisper")
			created := time.Now().Unix() - 3
			p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				value := map[string]any{"id": "backend", "expires_at": created + 5}
				if timestamp.present {
					value["created"] = timestamp.value
					if timestamp.name == "valid" {
						value["created"] = created
					}
				}
				body, _ := json.Marshal(value)
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
			})}
			w := httptest.NewRecorder()
			p.handleTranscriptionSession(w, httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper","ttl_seconds":5}`)))
			if w.Code != 200 {
				t.Fatalf("create: %d %s", w.Code, w.Body.String())
			}
			var value struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
				t.Fatal(err)
			}
			session := p.sessions[value.ID]
			if session.ttl != 5*time.Second {
				t.Fatalf("renewal TTL changed: %s", session.ttl)
			}
			if delta := session.expires.Sub(time.Unix(created+5, 0)); delta < -10*time.Millisecond || delta > 10*time.Millisecond {
				t.Fatalf("initial expiry shifted: %s", delta)
			}
			// A successful append renews from completion using the full TTL,
			// independently of the initial remaining upstream lifetime.
			_, finish, ok := p.acquireSession(value.ID, session.scope, time.Now())
			if !ok {
				t.Fatal("published session unavailable")
			}
			completed := time.Now()
			finish(completed, true)
			if got := p.sessions[value.ID].expires.Sub(completed); got != 5*time.Second {
				t.Fatalf("renewed lifetime = %s, want 5s", got)
			}
		})
	}
}

// Make transport fragmentation independent of the caller's output buffer.
type sessionByteReader struct{ io.Reader }

func (r sessionByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

func TestSessionEventsUseBoundedSSEFraming(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n", "\r"} {
		t.Run(fmt.Sprintf("newline=%q", newline), func(t *testing.T) {
			input := "\xef\xbb\xbf" + strings.Join([]string{
				": heartbeat", "id: cursor", "event: session.open", "retry: 500", "x-extension: keep",
				`data: {"type":"session.open",`, `data: "session_id":"private","extension":true}`, "",
				"data", "", "data: [DONE]", "", `data: {"session_id":"discard-private"}`, "",
			}, newline)
			response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"Text/Event-Stream; charset=utf-8"}}, Body: io.NopCloser(sessionByteReader{strings.NewReader(input)})}
			if err := rewriteSessionResponse(response, "public"); err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			want := ": heartbeat\nid: cursor\nevent: session.open\nretry: 500\nx-extension: keep\ndata: {\"extension\":true,\"session_id\":\"public\",\"type\":\"session.open\"}\n\ndata\n\ndata: [DONE]\n\n"
			if string(body) != want {
				t.Fatalf("got %q, want %q", body, want)
			}
		})
	}
	for _, input := range []string{
		strings.Repeat(": short\n", maxSessionEventBytes/8+1) + "\n",
		"data: " + strings.Repeat("x", maxSessionEventBytes) + "\n\n",
		"data: {\"session_id\":\"private\",\ndata: invalid}\n\n",
	} {
		response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(input))}
		if err := rewriteSessionResponse(response, "public"); err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err == nil || len(body) != 0 {
			t.Fatalf("invalid/oversized event escaped: bytes=%d, err=%v", len(body), err)
		}
	}
}

func TestSessionEventsDispatchLoneCRWithoutWaitingForMoreInput(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}
	if err := rewriteSessionResponse(response, "public"); err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	completed := make(chan error, 1)
	go func() {
		want := "data: {\"session_id\":\"public\"}\n\n"
		body := make([]byte, len(want))
		_, err := io.ReadFull(response.Body, body)
		if err == nil && string(body) != want {
			err = fmt.Errorf("got %q, want %q", body, want)
		}
		completed <- err
	}()
	if _, err := io.WriteString(writer, "data: {\"session_id\":\"private\"}\r\r"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("complete event waited for another upstream byte")
	}
}

func TestSessionCreationRejectsExpiredAndMalformedExpiryWithoutPublishing(t *testing.T) {
	for _, expiry := range []any{time.Now().Unix() - 10, time.Now().Unix(), 0, -1, nil, "future"} {
		t.Run(fmt.Sprint(expiry), func(t *testing.T) {
			p := NewProxy(Config{DefaultPool: RoutePoolTarget{Pool: "primary"}, Logger: zap.NewNop()})
			p.RegisterEndpoint("http://runtime.internal", "primary", WorkloadTypeGeneral)
			advertiseModelOperation(p.registry, "http://runtime.internal", "transcription.sessions", "whisper")
			p.registry.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				body, _ := json.Marshal(map[string]any{"id": "private", "expires_at": expiry})
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
			})}
			w := httptest.NewRecorder()
			p.handleTranscriptionSession(w, httptest.NewRequest("POST", "/ai/v1/transcription/sessions", strings.NewReader(`{"model":"whisper","ttl_seconds":1}`)))
			if w.Code != http.StatusBadGateway || len(p.sessions) != 0 || w.Header().Get("X-Antfly-Inference-Affinity") != "" {
				t.Fatalf("invalid expiry published: status=%d sessions=%d body=%s", w.Code, len(p.sessions), w.Body.String())
			}
		})
	}
}
