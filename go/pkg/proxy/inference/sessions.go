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
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// The same hop contract applies to discovery and execution. Derive the timeout
// immediately before dispatch so admission and earlier work consume its budget.
func setInferenceHopBudgets(request *http.Request) {
	request.Header.Set("X-Antfly-Inference-Attempts-Remaining", "1")
	if deadline, ok := request.Context().Deadline(); ok {
		remaining := max(time.Until(deadline).Milliseconds(), 1)
		request.Header.Set("X-Antfly-Inference-Timeout-Ms", strconv.FormatInt(remaining, 10))
	}
}

func writeInferenceContextFailure(w http.ResponseWriter, ctx context.Context) bool {
	if ctx.Err() == nil {
		return false
	}
	status := http.StatusRequestTimeout
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, "inference request lifetime ended", status)
	return true
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
type proxySessionReservationKey struct{}
type proxySession struct {
	pending     bool
	active      uint64
	generation  *byte
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

// HTTP success does not validate a session protocol envelope. Use the same
// checks before pinning handles and before rewriting JSON follow-ups.
func decodeSessionEnvelope(body []byte) (map[string]json.RawMessage, string, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, "", err
	}
	if envelope == nil {
		return nil, "", errors.New("session response must be an object")
	}
	var backend string
	for _, field := range []string{"session_id", "id"} {
		raw, present := envelope[field]
		if !present {
			continue
		}
		var id string
		if json.Unmarshal(raw, &id) != nil || id == "" || len(id) > 256 || id == "." || id == ".." || strings.ContainsAny(id, "/?#%\\") {
			return nil, "", errors.New("invalid session ID")
		}
		for i := range len(id) {
			if id[i] <= 32 || id[i] == 127 {
				return nil, "", errors.New("invalid session ID")
			}
		}
		if backend != "" && backend != id {
			return nil, "", errors.New("inconsistent session IDs")
		}
		backend = id
	}
	return envelope, backend, nil
}

var errSessionCapacity = errors.New("session capacity exhausted")

type proxySessionReservation struct {
	owner *Proxy
	id    string
}

// Insert an unpublished owner record before the upstream creation. Its slot
// remains counted until publication or release, including concurrent creates.
func (p *Proxy) reserveSession(r *http.Request, model string, endpoint *Endpoint, body []byte) (*proxySessionReservation, error) {
	scope, err := p.sessionScope(r)
	if err != nil {
		return nil, err
	}
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	id := base64.RawURLEncoding.EncodeToString(token[:])
	var request struct {
		TTL int64 `json:"ttl_seconds"`
	}
	_ = json.Unmarshal(body, &request)
	var ttl time.Duration
	if request.TTL >= 1 && request.TTL <= 3600 {
		ttl = time.Duration(request.TTL) * time.Second
	}
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()
	now := time.Now()
	for key, session := range p.sessions {
		if !session.pending && session.active == 0 && !now.Before(session.expires) {
			delete(p.sessions, key)
		}
	}
	if len(p.sessions) >= 4096 {
		return nil, errSessionCapacity
	}
	if _, exists := p.sessions[id]; exists {
		return nil, errors.New("session ID collision")
	}
	if p.sessions == nil {
		p.sessions = make(map[string]proxySession)
	}
	p.sessions[id] = proxySession{pending: true, generation: new(byte), scope: scope, address: endpoint.address, incarnation: endpoint.incarnation, model: model, ttl: ttl}
	return &proxySessionReservation{owner: p, id: id}, nil
}

func (reservation *proxySessionReservation) release() {
	p := reservation.owner
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()
	if session, exists := p.sessions[reservation.id]; exists && session.pending {
		delete(p.sessions, reservation.id)
	}
}

func (p *Proxy) pinSessionResponse(r *http.Request, response *http.Response, endpoint *Endpoint) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	if err != nil || len(body) > 1<<20 {
		return errors.New("session response too large")
	}
	envelope, backend, err := decodeSessionEnvelope(body)
	if err != nil {
		return err
	}
	if backend == "" {
		return errors.New("session ID missing")
	}

	reservation, ok := r.Context().Value(proxySessionReservationKey{}).(*proxySessionReservation)
	if !ok || reservation.owner != p {
		return errors.New("session reservation missing")
	}
	id := reservation.id
	now := time.Now()

	ttl := time.Hour
	if raw := envelope["expires_at"]; raw != nil {
		var expiry int64
		if json.Unmarshal(raw, &expiry) != nil || expiry <= 0 {
			return errors.New("invalid session expiry")
		}
		expires := time.Unix(expiry, 0)
		if !expires.After(now) {
			return errors.New("session already expired")
		}
		ttl = min(expires.Sub(now), 24*time.Hour)
	}

	public, _ := json.Marshal(id)
	for _, key := range []string{"id", "session_id"} {
		if _, ok := envelope[key]; ok {
			envelope[key] = public
		}
	}
	body, err = json.Marshal(envelope)
	if err != nil {
		return err
	}
	p.sessionMu.Lock()
	session, exists := p.sessions[id]
	if !exists || !session.pending || session.address != endpoint.address || session.incarnation != endpoint.incarnation {
		p.sessionMu.Unlock()
		return errors.New("session reservation changed")
	}
	session.pending = false
	session.backendID = backend
	session.expires = now.Add(ttl)
	// Optional timestamps must be JSON integers. In particular, null must not
	// become the Unix epoch and override the requested renewal lifetime.
	var created, expiry *int64
	if json.Unmarshal(envelope["created"], &created) == nil && json.Unmarshal(envelope["expires_at"], &expiry) == nil && created != nil && expiry != nil && *created >= 0 && *expiry > *created {
		session.ttl = time.Duration(min(*expiry-*created, 86400)) * time.Second
	} else if session.ttl == 0 {
		session.ttl = ttl
	}
	p.sessions[id] = session
	p.sessionMu.Unlock()
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
	id := parts[0]
	session, finish, ok := p.acquireSession(id, scope, time.Now())
	if !ok {
		http.NotFound(w, r)
		return
	}
	renew := false
	defer func() { finish(time.Now(), renew) }()
	session.publicID = id
	out := r.Clone(context.WithValue(r.Context(), proxySessionContextKey{}, session))
	out.URL.Path = "/ai/v1/transcription/sessions/" + session.backendID
	out.URL.RawPath = ""
	if len(parts) == 2 {
		out.URL.Path += "/" + parts[1]
	}
	status := &sessionStatusWriter{ResponseWriter: w}
	p.proxyRequest(status, out, "transcription.sessions")
	renew = r.Method == http.MethodPost && len(parts) == 2 && (parts[1] == "audio" || parts[1] == "stream") && status.code >= 200 && status.code < 300 && status.transportErr == nil && r.Context().Err() == nil
	if r.Method == http.MethodDelete && ((status.code >= 200 && status.code < 300) || status.code == 404 || status.code == 410) {
		// A deleted session is never reissued to a different backend.
		p.sessionMu.Lock()
		if current, exists := p.sessions[id]; exists && current.generation == session.generation {
			delete(p.sessions, id)
		}
		p.sessionMu.Unlock()
	}
}

// Each in-flight follow-up holds a reference independently of its idle TTL.
// Completion never revives a deleted/replaced record or renews failed requests.
func (p *Proxy) acquireSession(id string, scope [sha256.Size]byte, now time.Time) (proxySession, func(time.Time, bool), bool) {
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()
	session, ok := p.sessions[id]
	if !ok || session.pending || session.scope != scope || (session.active == 0 && !now.Before(session.expires)) || session.active == ^uint64(0) {
		return proxySession{}, nil, false
	}
	if session.generation == nil {
		session.generation = new(byte)
	}
	session.active++
	p.sessions[id] = session
	var once sync.Once
	finish := func(completed time.Time, renew bool) {
		once.Do(func() {
			p.sessionMu.Lock()
			defer p.sessionMu.Unlock()
			current, exists := p.sessions[id]
			if !exists || current.generation != session.generation {
				return
			}
			current.active--
			if expiry := completed.Add(current.ttl); renew && expiry.After(current.expires) {
				current.expires = expiry
			}
			p.sessions[id] = current
		})
	}
	return session, finish, true
}

func rewriteSessionResponse(response *http.Response, id string) error {
	mediaType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaType == "text/event-stream" {
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 4096), maxSessionEventBytes+2)
		stream := &sessionStreamBody{source: response.Body, scanner: scanner, id: id, firstLine: true}
		scanner.Split(stream.splitLine)
		response.Body = stream
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
		value, _, err := decodeSessionEnvelope(body)
		if err != nil {
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

const maxSessionEventBytes = 1 << 20

type sessionStreamBody struct {
	source    io.ReadCloser
	scanner   *bufio.Scanner
	id        string
	pending   []byte
	event     []byte
	firstLine bool
	afterCR   bool
}

// Accept CR immediately, even when the following LF arrives in another read.
func (s *sessionStreamBody) splitLine(data []byte, atEOF bool) (int, []byte, error) {
	if len(data) == 0 {
		return 0, nil, nil
	}
	if s.afterCR {
		s.afterCR = false
		if data[0] == '\n' {
			return 1, nil, nil
		}
	}
	for i, c := range data {
		if c == '\n' || c == '\r' {
			s.afterCR = c == '\r'
			return i + 1, data[:i], nil
		}
	}
	// A final unterminated line cannot dispatch an SSE event.
	if atEOF {
		return len(data), nil, nil
	}
	return 0, nil, nil
}

func sessionSSEData(line []byte) ([]byte, bool) {
	field, value, _ := bytes.Cut(line, []byte(":"))
	if !bytes.Equal(field, []byte("data")) {
		return nil, false
	}
	return bytes.TrimPrefix(value, []byte(" ")), true
}

func (s *sessionStreamBody) rewriteEvent() ([]byte, error) {
	var data []byte
	hasData := false
	for line := range bytes.SplitSeq(s.event, []byte("\n")) {
		if value, ok := sessionSSEData(line); ok {
			if hasData {
				data = append(data, '\n')
			}
			data = append(data, value...)
			hasData = true
		}
	}
	if !hasData || len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return s.event, nil
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	if _, ok := value["session_id"]; !ok {
		return s.event, nil
	}
	value["session_id"], _ = json.Marshal(s.id)
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var output []byte
	replaced := false
	for line := range bytes.SplitSeq(bytes.TrimSuffix(s.event, []byte("\n")), []byte("\n")) {
		if _, ok := sessionSSEData(line); ok {
			if replaced {
				continue
			}
			output = append(output, "data: "...)
			output = append(output, encoded...)
			replaced = true
		} else {
			output = append(output, line...)
		}
		output = append(output, '\n')
	}
	return output, nil
}

func (s *sessionStreamBody) Close() error { return s.source.Close() }
func (s *sessionStreamBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(s.pending) == 0 {
		s.event = s.event[:0]
		for {
			if !s.scanner.Scan() {
				if err := s.scanner.Err(); err != nil {
					return 0, err
				}
				return 0, io.EOF // Discard incomplete events at EOF.
			}
			line := s.scanner.Bytes()
			if s.firstLine {
				line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
				s.firstLine = false
			}
			if len(line)+1 > maxSessionEventBytes-len(s.event) {
				return 0, errors.New("session SSE event exceeded limit")
			}
			s.event = append(s.event, line...)
			s.event = append(s.event, '\n')
			if len(line) == 0 {
				break
			}
		}
		var err error
		s.pending, err = s.rewriteEvent()
		if err != nil {
			return 0, err
		}
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
	// This adapter changes the catalog body, not its discovery metadata.
	// Retain lease exposure and error retry guidance from the canonical handler;
	// the transport must calculate a new length for the rewritten body.
	copyMetadata := func() {
		for name, values := range capture.Header() {
			if !strings.EqualFold(name, "Content-Length") {
				w.Header()[name] = append([]string(nil), values...)
			}
		}
	}
	if capture.statusCode >= 400 {
		copyMetadata()
		w.WriteHeader(capture.statusCode)
		_, _ = w.Write(body)
		return
	}
	var catalog map[string]json.RawMessage
	if json.Unmarshal(body, &catalog) != nil {
		http.Error(w, "invalid predictor catalog", http.StatusBadGateway)
		return
	}
	predictors := make(map[string]json.RawMessage)
	_ = json.Unmarshal(catalog["predictors"], &predictors)
	for name, schema := range predictors {
		if !validPredictorSchema(schema) {
			delete(predictors, name)
		}
	}
	copyMetadata()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "predictors": predictors})
}

type sessionStatusWriter struct {
	http.ResponseWriter
	code         int
	transportErr error
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
func (w *sessionStatusWriter) Flush() { _ = w.FlushError() }
func (w *sessionStatusWriter) FlushError() error {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.recordProxyTransportError(err)
	return err
}
func (w *sessionStatusWriter) recordProxyTransportError(err error) {
	if err != nil {
		w.transportErr = err
	}
	if recorder, ok := w.ResponseWriter.(interface{ recordProxyTransportError(error) }); ok {
		recorder.recordProxyTransportError(err)
	}
}

// A pinned raw stream is single-use and never materialized or replayed. The
// response owns cancellation until its final byte, including early rejections.
type proxySessionUpload struct {
	readBytes int64
	source    io.ReadCloser
	length    int64
	interrupt func()
	mu        sync.Mutex
	done      atomic.Bool
	once      sync.Once
}

func (b *proxySessionUpload) Prepare(attempts int) error {
	if attempts != 1 {
		return errors.New("session streams cannot replay")
	}
	return nil
}
func (b *proxySessionUpload) Open() (io.ReadCloser, error) { return b, nil }
func (b *proxySessionUpload) Len() int64                   { return b.length }
func (b *proxySessionUpload) Read(dest []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.source.Read(dest)
	b.readBytes += int64(n)
	if err != nil || (b.length >= 0 && b.readBytes >= b.length) {
		b.done.Store(true)
	}
	return n, err
}
func (b *proxySessionUpload) Close() error {
	b.once.Do(func() {
		if !b.done.Load() {
			b.interrupt()
		}
	})
	return nil
}
func (b *proxySessionUpload) Finish(_ context.Context, _ bool) error {
	_ = b.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.source.Close()
}
func (p *Proxy) proxySessionStream(w http.ResponseWriter, r *http.Request, operation string, start time.Time, routing RoutingContext) {
	if r.ContentLength > p.maxRequestBodyBytes {
		http.Error(w, "inference request body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	// Bound only the transport chunk, independent of microphone duration.
	if err := p.bodyAdmission.Acquire(r.Context(), 64<<10); err != nil {
		http.Error(w, "session stream admission canceled", http.StatusRequestTimeout)
		return
	}
	defer p.bodyAdmission.Release(64 << 10)
	_ = http.NewResponseController(w).EnableFullDuplex()
	upload := &proxySessionUpload{source: http.MaxBytesReader(w, r.Body, p.maxRequestBodyBytes), length: r.ContentLength, interrupt: func() {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now()); err != nil {
			_ = r.Body.Close()
		}
	}}
	defer upload.Finish(r.Context(), false)
	p.proxyRequestWithReplayBody(w, r, operation, start, routing, nil, upload)
}
