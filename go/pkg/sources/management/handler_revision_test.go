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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type revisionHandlerBackend struct {
	Backend
	source   Source
	page     SourcePage
	putCalls int
	received Source
}

func (b *revisionHandlerBackend) GetSource(context.Context, string) (Source, error) {
	return b.source, nil
}

func (b *revisionHandlerBackend) ListSources(context.Context, int, string) (SourcePage, error) {
	return b.page, nil
}

func (b *revisionHandlerBackend) PutSource(_ context.Context, source Source) (Source, error) {
	b.putCalls++
	b.received = source
	return b.source, nil
}

type revisionConditionalBackend struct {
	*revisionHandlerBackend
	conditionalCalls int
	absentCalls      int
	revision         string
	err              error
}

var _ PreconditionBackend = (*revisionConditionalBackend)(nil)

func (b *revisionConditionalBackend) PutSourceIfMatch(_ context.Context, source Source, revision string) (Source, error) {
	b.conditionalCalls++
	b.received = source
	b.revision = revision
	return b.source, b.err
}

func (b *revisionConditionalBackend) PutSourceIfAbsent(_ context.Context, source Source) (Source, error) {
	b.absentCalls++
	b.received = source
	return b.source, b.err
}

func revisionPutRequest(t *testing.T, source Source, tag string) *http.Request {
	t.Helper()
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPut, "/sources/"+source.Id, bytes.NewReader(raw))
	if tag != "" {
		r.Header.Set("If-Match", tag)
	}
	return r
}

func assertRevisionResponse(t *testing.T, response *httptest.ResponseRecorder, source Source) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
	}
	var got Source
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := SourceRevision(source)
	if got.Revision != want {
		t.Errorf("body revision = %q, want %q", got.Revision, want)
	}
	if got := response.Header().Get("ETag"); got != `"`+want+`"` {
		t.Errorf("ETag = %q, want quoted %q", got, want)
	}
}

func assertRevisionHTTPError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Errorf("HTTP status = %d, want %d; body: %s", response.Code, status, response.Body.String())
	}
	var got Error
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if want := (Error{Version: 1, Code: code}); got != want {
		t.Errorf("error body = %#v, want %#v", got, want)
	}
}

func TestHandlerSourceRevisionGET(t *testing.T) {
	source := revisionTestSource()
	source.Revision = "stale-backend-revision"
	backend := &revisionHandlerBackend{source: source}
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sources/source", nil))
	assertRevisionResponse(t, response, source)
}

func TestHandlerSourceRevisionList(t *testing.T) {
	first, second := revisionTestSource(), revisionTestSource()
	first.Revision = "stale-first"
	second.Id, second.Name, second.Revision = "second", "Second documents", "stale-second"
	backend := &revisionHandlerBackend{page: SourcePage{Data: []Source{first, second}, NextCursor: "next"}}
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sources", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
	}
	var got SourcePage
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Data) != 2 || got.NextCursor != "next" {
		t.Fatalf("list page = %#v, want both sources and next cursor", got)
	}
	for i, source := range []Source{first, second} {
		if want := SourceRevision(source); got.Data[i].Revision != want {
			t.Errorf("item %d revision = %q, want %q", i, got.Data[i].Revision, want)
		}
	}
}

func TestHandlerSourceRevisionPUT(t *testing.T) {
	for _, tag := range []string{"", `"r1-previous"`, "r1-previous"} {
		t.Run("If-Match="+tag, func(t *testing.T) {
			input := revisionTestSource()
			input.Revision = "forged"
			stored := revisionTestSource()
			stored.Name, stored.Revision = "Backend normalized name", "stale-backend-revision"
			backend := &revisionConditionalBackend{revisionHandlerBackend: &revisionHandlerBackend{source: stored}}
			response := httptest.NewRecorder()
			Handler(backend).ServeHTTP(response, revisionPutRequest(t, input, tag))
			if tag == "" {
				if backend.putCalls != 1 || backend.conditionalCalls != 0 {
					t.Errorf("unconditional/conditional calls = %d/%d, want 1/0", backend.putCalls, backend.conditionalCalls)
				}
			} else if backend.putCalls != 0 || backend.conditionalCalls != 1 || backend.revision != "r1-previous" {
				t.Errorf("unconditional/conditional calls = %d/%d, revision = %q; want 0/1 and bare r1-previous", backend.putCalls, backend.conditionalCalls, backend.revision)
			}
			if backend.received.Revision != "" {
				t.Errorf("backend received client revision %q", backend.received.Revision)
			}
			assertRevisionResponse(t, response, stored)
		})
	}
}

func TestHandlerSourceRevisionPreconditionErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{"mismatch", http.StatusPreconditionFailed, "source_revision_mismatch"},
		{"missing", http.StatusNotFound, "source_not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &revisionConditionalBackend{revisionHandlerBackend: &revisionHandlerBackend{source: revisionTestSource()}, err: Failure(tc.status, tc.code)}
			response := httptest.NewRecorder()
			Handler(backend).ServeHTTP(response, revisionPutRequest(t, revisionTestSource(), `"r1-previous"`))
			assertRevisionHTTPError(t, response, tc.status, tc.code)
			if backend.conditionalCalls != 1 || backend.putCalls != 0 {
				t.Errorf("conditional/unconditional calls = %d/%d, want 1/0", backend.conditionalCalls, backend.putCalls)
			}
		})
	}
}

func TestHandlerSourceRevisionPreconditionUnsupported(t *testing.T) {
	backend := &revisionHandlerBackend{source: revisionTestSource()}
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, revisionPutRequest(t, revisionTestSource(), `"r1-previous"`))
	assertRevisionHTTPError(t, response, http.StatusNotImplemented, "revision_preconditions_unsupported")
	if backend.putCalls != 0 {
		t.Errorf("unsupported precondition called PutSource %d times", backend.putCalls)
	}
}

func TestHandlerSourceRevisionInvalidPreconditions(t *testing.T) {
	for _, tag := range []string{`W/"x"`, "*", `"a", "b"`} {
		t.Run(tag, func(t *testing.T) {
			for _, conditional := range []bool{false, true} {
				base := &revisionHandlerBackend{source: revisionTestSource()}
				cas := &revisionConditionalBackend{revisionHandlerBackend: base}
				var backend Backend = base
				if conditional {
					backend = cas
				}
				response := httptest.NewRecorder()
				Handler(backend).ServeHTTP(response, revisionPutRequest(t, revisionTestSource(), tag))
				assertRevisionHTTPError(t, response, http.StatusBadRequest, "invalid_precondition")
				if base.putCalls != 0 || cas.conditionalCalls != 0 {
					t.Errorf("invalid precondition reached backend (conditional=%t): PutSource=%d PutSourceIfMatch=%d", conditional, base.putCalls, cas.conditionalCalls)
				}
			}
		})
	}
}

func TestHandlerSourceIfAbsent(t *testing.T) {
	for _, tag := range []string{"*", "  *  "} {
		t.Run(tag, func(t *testing.T) {
			input := revisionTestSource()
			input.Revision = "forged"
			stored := revisionTestSource()
			stored.Name, stored.Revision = "Backend normalized name", "stale"
			backend := &revisionConditionalBackend{revisionHandlerBackend: &revisionHandlerBackend{source: stored}}
			request := revisionPutRequest(t, input, "")
			request.Header.Set("If-None-Match", tag)
			response := httptest.NewRecorder()
			Handler(backend).ServeHTTP(response, request)
			if backend.absentCalls != 1 || backend.putCalls != 0 || backend.conditionalCalls != 0 {
				t.Errorf("absent/unconditional/match calls = %d/%d/%d, want 1/0/0", backend.absentCalls, backend.putCalls, backend.conditionalCalls)
			}
			if backend.received.Id != input.Id || backend.received.Revision != "" {
				t.Errorf("backend received = %#v, want source declaration without revision", backend.received)
			}
			assertRevisionResponse(t, response, stored)
		})
	}
}

func TestHandlerSourceIfAbsentConflict(t *testing.T) {
	backend := &revisionConditionalBackend{
		revisionHandlerBackend: &revisionHandlerBackend{source: revisionTestSource()},
		err:                    Failure(http.StatusPreconditionFailed, "source_already_exists"),
	}
	request := revisionPutRequest(t, revisionTestSource(), "")
	request.Header.Set("If-None-Match", "*")
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, request)
	assertRevisionHTTPError(t, response, http.StatusPreconditionFailed, "source_already_exists")
	if backend.absentCalls != 1 || backend.putCalls != 0 || backend.conditionalCalls != 0 {
		t.Errorf("absent/unconditional/match calls = %d/%d/%d, want 1/0/0", backend.absentCalls, backend.putCalls, backend.conditionalCalls)
	}
}

func TestHandlerSourceIfAbsentUnsupported(t *testing.T) {
	backend := &revisionHandlerBackend{source: revisionTestSource()}
	request := revisionPutRequest(t, revisionTestSource(), "")
	request.Header.Set("If-None-Match", "*")
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, request)
	assertRevisionHTTPError(t, response, http.StatusNotImplemented, "revision_preconditions_unsupported")
	if backend.putCalls != 0 {
		t.Errorf("unsupported precondition called PutSource %d times", backend.putCalls)
	}
}

func TestHandlerSourceIfAbsentInvalidPreconditions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		match  []string
	}{
		{name: "empty", values: []string{""}},
		{name: "whitespace", values: []string{"  "}},
		{name: "bare revision", values: []string{"x"}},
		{name: "quoted revision", values: []string{`"x"`}},
		{name: "weak revision", values: []string{`W/"x"`}},
		{name: "quoted star", values: []string{`"*"`}},
		{name: "comma separated", values: []string{"*, *"}},
		{name: "multiple values", values: []string{"*", "*"}},
		{name: "second empty value", values: []string{"*", ""}},
		{name: "both headers", values: []string{"*"}, match: []string{`"x"`}},
		{name: "empty match header", values: []string{"*"}, match: []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, conditional := range []bool{false, true} {
				base := &revisionHandlerBackend{source: revisionTestSource()}
				cas := &revisionConditionalBackend{revisionHandlerBackend: base}
				var backend Backend = base
				if conditional {
					backend = cas
				}
				request := revisionPutRequest(t, revisionTestSource(), "")
				request.Header["If-None-Match"] = tc.values
				if tc.match != nil {
					request.Header["If-Match"] = tc.match
				}
				response := httptest.NewRecorder()
				Handler(backend).ServeHTTP(response, request)
				assertRevisionHTTPError(t, response, http.StatusBadRequest, "invalid_precondition")
				if base.putCalls != 0 || cas.conditionalCalls != 0 || cas.absentCalls != 0 {
					t.Errorf("invalid precondition reached backend (conditional=%t): PutSource=%d IfMatch=%d IfAbsent=%d", conditional, base.putCalls, cas.conditionalCalls, cas.absentCalls)
				}
			}
		})
	}
}

type declarationHandlerBackend struct {
	*revisionHandlerBackend
	declarations int
}

func (b *declarationHandlerBackend) GetSourceDeclaration(context.Context, string) (Source, error) {
	b.declarations++
	return b.source, nil
}
func (b *declarationHandlerBackend) GetSource(context.Context, string) (Source, error) {
	return Source{}, Failure(503, "native_observations_unavailable")
}
func TestDeclarationViewUsesFastBackendAndPreservesRevision(t *testing.T) {
	backend := &declarationHandlerBackend{revisionHandlerBackend: &revisionHandlerBackend{source: revisionTestSource()}}
	server := httptest.NewServer(Handler(backend))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	source, err := client.GetSourceDeclaration(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if source.Revision != SourceRevision(backend.source) || backend.declarations != 1 {
		t.Fatalf("source=%#v, reads=%d", source, backend.declarations)
	}
	if _, err = client.GetSource(t.Context(), "source"); err == nil {
		t.Fatal("full view did not use observation backend")
	}
	response := httptest.NewRecorder()
	Handler(backend).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sources/source?view=bogus", nil))
	if response.Code != 400 {
		t.Fatalf("invalid view status=%d", response.Code)
	}
	// Optional backends remain compatible with the additive view parameter.
	old := &revisionHandlerBackend{source: revisionTestSource()}
	response = httptest.NewRecorder()
	Handler(old).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sources/source?view=declaration", nil))
	if response.Code != 200 {
		t.Fatalf("old backend status=%d", response.Code)
	}
}

func TestHandlerRejectsContractFieldsInOpaqueConfig(t *testing.T) {
	for _, key := range ReservedSourceConfigFields() {
		t.Run(key, func(t *testing.T) {
			source := revisionTestSource()
			source.Config[key] = nil
			body, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			backend := &revisionHandlerBackend{source: source}
			response := httptest.NewRecorder()
			Handler(backend).ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/sources/source", bytes.NewReader(body)))
			if response.Code != 400 || backend.putCalls != 0 {
				t.Fatalf("status=%d writes=%d", response.Code, backend.putCalls)
			}
		})
	}
}
