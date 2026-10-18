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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const inferenceMaxAttempts = 8

// Caller budgets only reduce server policy. They convey no tenant identity.
func inferenceRequestContext(r *http.Request) (context.Context, context.CancelFunc) {
	timeout := 120 * time.Second
	if raw := r.Header.Get("X-Antfly-Inference-Timeout-Ms"); raw != "" {
		if value, err := strconv.ParseUint(raw, 10, 32); err == nil && value < uint64(timeout/time.Millisecond) {
			timeout = time.Duration(value) * time.Millisecond
		}
	}
	return context.WithTimeout(r.Context(), timeout)
}

func inferenceAttemptLimit(r *http.Request, configured int) int {
	limit := min(max(configured, 1), inferenceMaxAttempts)
	if raw := r.Header.Get("X-Antfly-Inference-Attempts-Remaining"); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value >= 0 {
			limit = min(limit, value)
		}
	}
	return limit
}

type proxySessionContextKey struct{}
type proxySessionModelKey struct{}
type proxySession struct {
	publicID    string
	scope       [sha256.Size]byte
	address     string
	incarnation uint64
	model       string
	backendID   string
	expires     time.Time
	ttl         time.Duration
}

func (p *Proxy) sessionScope(r *http.Request) ([sha256.Size]byte, error) {
	routing, err := p.routingContextForRequest(r, time.Now())
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	encoded, err := json.Marshal(struct {
		Authorization       string
		CallerAuthorization string
		Source              VerifiedSource
	}{p.upstreamAuthorizationForRequest(r), r.Header.Get("Authorization"), routing.Source})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (p *Proxy) pinSessionResponse(r *http.Request, response *http.Response, endpoint *Endpoint) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	if err != nil || len(body) > 1<<20 {
		return errors.New("session response too large")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	field := "session_id"
	if _, ok := envelope[field]; !ok {
		field = "id"
	}
	var backend string
	if err := json.Unmarshal(envelope[field], &backend); err != nil || backend == "" || len(backend) > 256 || strings.ContainsAny(backend, "/?#%") {
		return errors.New("invalid session ID")
	}
	scope, err := p.sessionScope(r)
	if err != nil {
		return err
	}
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	id := base64.RawURLEncoding.EncodeToString(token[:])
	// The request was already decoded and bounded by the common proxy path.
	model, _ := r.Context().Value(proxySessionModelKey{}).(string)
	if model == "" {
		return errors.New("session model missing")
	}
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()
	if p.sessions == nil {
		p.sessions = make(map[string]proxySession)
	}
	now := time.Now()
	for key, session := range p.sessions {
		if now.After(session.expires) {
			delete(p.sessions, key)
		}
	}
	if len(p.sessions) >= 4096 {
		return errors.New("session capacity exhausted")
	}
	ttl := time.Hour
	if raw := envelope["expires_at"]; raw != nil {
		var expiry int64
		if json.Unmarshal(raw, &expiry) == nil {
			delta := time.Until(time.Unix(expiry, 0))
			if delta > 0 {
				ttl = min(delta, 24*time.Hour)
			}
		}
	}
	p.sessions[id] = proxySession{scope: scope, address: endpoint.address, incarnation: endpoint.incarnation, model: model, backendID: backend, expires: now.Add(ttl), ttl: ttl}
	public, _ := json.Marshal(id)
	for _, key := range []string{"id", "session_id"} {
		if _, ok := envelope[key]; ok {
			envelope[key] = public
		}
	}
	body, err = json.Marshal(envelope)
	if err != nil {
		delete(p.sessions, id)
		return err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
	response.Header.Set("X-Antfly-Inference-Affinity", id)
	return nil
}

// All stages stay on the endpoint incarnation chosen at creation. Deployments
// with multiple proxy replicas must route the opaque affinity handle to its
// owner. Unknown handles and removed endpoints fail closed; sessions never
// fail over or get recreated on another runtime.
func (p *Proxy) handleTranscriptionSession(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ai/v1/transcription/sessions" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// The internal callback receives model through a typed context below,
		// never a caller-controlled routing identity header.
		p.proxyRequest(w, r, "transcription.sessions")
		return
	}
	scope, err := p.sessionScope(r)
	if err != nil {
		http.Error(w, "source authentication failed", http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/ai/v1/transcription/sessions/"), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] != "audio" && parts[1] != "events" && parts[1] != "stream" {
		http.NotFound(w, r)
		return
	}
	validMethod := (len(parts) == 1 && (r.Method == http.MethodGet || r.Method == http.MethodDelete)) || (len(parts) == 2 && ((parts[1] == "events" && r.Method == http.MethodGet) || (parts[1] != "events" && r.Method == http.MethodPost)))
	if !validMethod {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p.sessionMu.Lock()
	session, ok := p.sessions[parts[0]]
	if ok && session.scope == scope && time.Now().Before(session.expires) {
		session.expires = time.Now().Add(session.ttl)
		p.sessions[parts[0]] = session
	}
	p.sessionMu.Unlock()
	if !ok || session.scope != scope || time.Now().After(session.expires) {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	session.publicID = id
	out := r.Clone(context.WithValue(r.Context(), proxySessionContextKey{}, session))
	out.URL.Path = "/ai/v1/transcription/sessions/" + session.backendID
	out.URL.RawPath = ""
	if len(parts) == 2 {
		out.URL.Path += "/" + parts[1]
	}
	status := &sessionStatusWriter{ResponseWriter: w}
	p.proxyRequest(status, out, "transcription.sessions")
	if r.Method == http.MethodDelete && ((status.code >= 200 && status.code < 300) || status.code == 404 || status.code == 410) {
		// A deleted session is never reissued to a different backend.
		p.sessionMu.Lock()
		delete(p.sessions, id)
		p.sessionMu.Unlock()
	}
}

func rewriteSessionResponse(response *http.Response, id string) error {
	if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		response.Body = &sessionStreamBody{source: response.Body, scanner: scanner, id: id}
		response.Header.Del("Content-Length")
		response.ContentLength = -1
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || response.StatusCode == http.StatusNoContent {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	if err != nil || len(body) > 1<<20 {
		return errors.New("session response exceeded limit")
	}
	if len(body) != 0 {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(body, &value); err != nil {
			return err
		}
		public, _ := json.Marshal(id)
		for _, field := range []string{"id", "session_id"} {
			if _, ok := value[field]; ok {
				value[field] = public
			}
		}
		body, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

type sessionStreamBody struct {
	source  io.ReadCloser
	scanner *bufio.Scanner
	id      string
	pending []byte
}

func (s *sessionStreamBody) Close() error { return s.source.Close() }
func (s *sessionStreamBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(s.pending) == 0 {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		line := s.scanner.Bytes()
		if bytes.HasPrefix(line, []byte("data:")) && !bytes.Equal(bytes.TrimSpace(line[5:]), []byte("[DONE]")) {
			var value map[string]json.RawMessage
			if err := json.Unmarshal(bytes.TrimSpace(line[5:]), &value); err != nil {
				return 0, err
			}
			if _, ok := value["session_id"]; ok {
				value["session_id"], _ = json.Marshal(s.id)
				encoded, err := json.Marshal(value)
				if err != nil {
					return 0, err
				}
				line = append([]byte("data: "), encoded...)
			}
		}
		s.pending = append(append([]byte(nil), line...), '\n')
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// ML discovery reuses the authenticated, revision-scoped catalog fanout. Only
// descriptors with a complete matching feature schema may survive merging.
func (p *Proxy) handlePredictors(w http.ResponseWriter, r *http.Request) {
	capture := newBoundedProxyResponseWriter(maxMergedModelCatalogBytes)
	defer capture.releaseBody()
	p.handleModels(capture, r)
	body, err := capture.readBody()
	if err != nil {
		http.Error(w, "predictor catalog unavailable", http.StatusBadGateway)
		return
	}
	if capture.statusCode >= 400 {
		w.WriteHeader(capture.statusCode)
		_, _ = w.Write(body)
		return
	}
	var catalog map[string]json.RawMessage
	if json.Unmarshal(body, &catalog) != nil {
		http.Error(w, "invalid predictor catalog", http.StatusBadGateway)
		return
	}
	predictors := catalog["predictors"]
	if len(predictors) == 0 {
		predictors = json.RawMessage(`{}`)
	}
	for _, header := range []string{capabilityTokenHeader, capabilityRevisionHeader} {
		if value := capture.Header().Get(header); value != "" {
			w.Header().Set(header, value)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "predictors": predictors})
}

type sessionStatusWriter struct {
	http.ResponseWriter
	code int
}

func (w *sessionStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *sessionStatusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *sessionStatusWriter) Write(body []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}
func (w *sessionStatusWriter) Flush() {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
