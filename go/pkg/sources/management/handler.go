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

package management

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Backend is implemented by a runtime after binding its authenticated scope.
// Neither provider credentials nor tenant selection come from request bodies.
type Backend interface {
	Capabilities(context.Context) (Capabilities, error)
	ListSources(context.Context, int, string) (SourcePage, error)
	GetSource(context.Context, string) (Source, error)
	PutSource(context.Context, Source) (Source, error)
	RemoveSource(context.Context, string, bool) (Mutation, error)
	ListRuns(context.Context, string, int, string) (RunPage, error)
	GetRun(context.Context, string, string) (Run, error)
	StartRun(context.Context, string, string, RunInput) (Run, error)
	CancelRun(context.Context, string, string) (Run, error)
}

// ProcessingBackend is optional for older runtimes implementing acquisition only.
type KnowledgeBackend interface {
	QueryKnowledge(context.Context, string, KnowledgeRequest) (json.RawMessage, error)
}

type ProcessingBackend interface {
	PreviewProcessing(context.Context, string, ProcessingPreset) (ProcessingPreview, error)
	ProcessingJob(context.Context, string, ProcessingJobRequest) (map[string]any, error)
}

type HTTPError struct {
	Status int
	Code   string
}

func (e *HTTPError) Error() string          { return e.Code }
func Failure(status int, code string) error { return &HTTPError{status, code} }
func WriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func WriteError(w http.ResponseWriter, err error) {
	status, code := 500, "internal_error"
	var e *HTTPError
	if errors.As(err, &e) {
		status, code = e.Status, e.Code
	}
	WriteJSON(w, status, Error{Version: 1, Code: code})
}
func Decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
		return Failure(400, "invalid_request")
	}
	return nil
}
func validID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func page(r *http.Request) (int, string, error) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 100 {
			return 0, "", Failure(400, "invalid_limit")
		}
		limit = n
	}
	cursor := r.URL.Query().Get("cursor")
	if len(cursor) > 512 {
		return 0, "", Failure(400, "invalid_cursor")
	}
	return limit, cursor, nil
}

// Handler expects a base-relative path. The caller mounts and authenticates it.
// Route validation and JSON/error semantics are shared by both hosting runtimes.
func Handler(b Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		var result any
		var err error
		status := 200
		switch {
		case r.URL.Path == "/capabilities" && r.Method == http.MethodGet:
			result, err = b.Capabilities(r.Context())
		case r.URL.Path == "/sources" && r.Method == http.MethodGet:
			n, c, e := page(r)
			if e != nil {
				err = e
				break
			}
			result, err = b.ListSources(r.Context(), n, c)
		case len(parts) >= 2 && parts[0] == "sources" && validID(parts[1]):
			id := parts[1]
			switch {
			case len(parts) == 3 && parts[2] == "knowledge-query" && r.Method == http.MethodPost:
				backend, ok := b.(KnowledgeBackend)
				if !ok {
					err = Failure(501, "knowledge_unsupported")
					break
				}
				var input KnowledgeRequest
				if err = Decode(w, r, &input); err == nil {
					result, err = backend.QueryKnowledge(r.Context(), id, input)
				}
			case len(parts) == 2:
				switch r.Method {
				case http.MethodGet:
					result, err = b.GetSource(r.Context(), id)
				case http.MethodPut:
					var source Source
					if err = Decode(w, r, &source); err != nil {
						break
					}
					if source.Id != id || source.Config == nil || !source.Publication.Valid() || source.Connection == "" || source.Table == "" {
						err = Failure(400, "invalid_source")
						break
					}
					result, err = b.PutSource(r.Context(), source)
				case http.MethodDelete:
					purge := r.URL.Query().Get("purge")
					if purge != "" && purge != "true" && purge != "false" {
						err = Failure(400, "invalid_purge_option")
						break
					}
					result, err = b.RemoveSource(r.Context(), id, purge == "true")
				default:
					err = Failure(405, "method_not_allowed")
				}
			case len(parts) == 3 && (parts[2] == "processing-preview" || parts[2] == "processing-jobs") && r.Method == http.MethodPost:
				backend, ok := b.(ProcessingBackend)
				if !ok {
					err = Failure(503, "processing_unavailable")
					break
				}
				if parts[2] == "processing-preview" {
					var input ProcessingPreset
					if err = Decode(w, r, &input); err == nil {
						result, err = backend.PreviewProcessing(r.Context(), id, input)
					}
				} else {
					var input ProcessingJobRequest
					if err = Decode(w, r, &input); err == nil {
						result, err = backend.ProcessingJob(r.Context(), id, input)
					}
				}
			case len(parts) == 3 && parts[2] == "runs":
				switch r.Method {
				case http.MethodGet:
					n, c, e := page(r)
					if e != nil {
						err = e
						break
					}
					result, err = b.ListRuns(r.Context(), id, n, c)
				case http.MethodPost:
					key := r.Header.Get("Idempotency-Key")
					if !validID(key) {
						err = Failure(400, "invalid_idempotency_key")
						break
					}
					var input RunInput
					if err = Decode(w, r, &input); err != nil {
						break
					}
					if input.Operation == "" {
						input.Operation = "sync"
					}
					if !input.Operation.Valid() || input.Operation == "purge" && input.Restart {
						err = Failure(400, "invalid_run_operation")
						break
					}
					result, err = b.StartRun(r.Context(), id, key, input)
					status = 202
				default:
					err = Failure(405, "method_not_allowed")
				}
			case len(parts) == 4 && parts[2] == "runs" && validID(parts[3]) && r.Method == http.MethodGet:
				result, err = b.GetRun(r.Context(), id, parts[3])
			case len(parts) == 5 && parts[2] == "runs" && validID(parts[3]) && parts[4] == "cancel" && r.Method == http.MethodPost:
				result, err = b.CancelRun(r.Context(), id, parts[3])
				status = 202
			default:
				err = Failure(404, "not_found")
			}
		default:
			err = Failure(404, "not_found")
		}
		if err != nil {
			WriteError(w, err)
			return
		}
		WriteJSON(w, status, result)
	})
}

// MaxResponseBytes is the common management transport bound. A page can contain
// fewer than the requested item limit to fit it; callers follow next_cursor.
const MaxResponseBytes = 2 << 20
const SourcePageBudget = MaxResponseBytes - 1024

// SourceBytes measures the actual wire representation, including JSON escaping.
// Runtime adapters enforce the same budget before materializing a large page.
func SourceBytes(source Source) (int, error) {
	raw, err := json.Marshal(source)
	if err != nil {
		return 0, err
	}
	if len(raw)+1 > SourcePageBudget {
		return 0, Failure(413, "source_response_too_large")
	}
	return len(raw) + 1, nil
}
