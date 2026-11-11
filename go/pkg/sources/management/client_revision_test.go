// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClientSourceRevisionPUT(t *testing.T) {
	for _, name := range []string{"unconditional", "If-Match", "If-None-Match"} {
		t.Run(name, func(t *testing.T) {
			revision := "r1-" + strings.Repeat("a", 64)
			input := revisionTestSource()
			input.Revision = "forged"
			input.SearchStatus = &SearchStatus{Version: 2}
			input.KnowledgeStatus = &KnowledgeStatus{Version: 1}
			want := revisionTestSource()
			want.Revision = revision
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPut || r.URL.Path != "/mount/sources/source" {
					t.Errorf("request = %s %s, want PUT /mount/sources/source", r.Method, r.URL.Path)
				}
				wantTag := ""
				if name == "If-Match" {
					wantTag = `"` + revision + `"`
				}
				if got := r.Header.Get("If-Match"); got != wantTag {
					t.Errorf("If-Match = %q, want %q", got, wantTag)
				}
				wantNoneMatch := ""
				if name == "If-None-Match" {
					wantNoneMatch = "*"
				}
				if got := r.Header.Values("If-None-Match"); (wantNoneMatch == "" && len(got) != 0) || (wantNoneMatch != "" && !reflect.DeepEqual(got, []string{wantNoneMatch})) {
					t.Errorf("If-None-Match = %q, want %q", got, wantNoneMatch)
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decoding request: %v", err)
				}
				for _, field := range []string{"revision", "search_status", "knowledge_status"} {
					if raw, ok := body[field]; ok {
						t.Errorf("request contains observation %s: %s", field, raw)
					}
				}
				if string(body["id"]) != `"source"` || string(body["table"]) != `"docs"` {
					t.Errorf("request lost source declaration: %v", body)
				}
				WriteJSON(w, http.StatusOK, want)
			}))
			defer server.Close()
			client, err := NewClient(server.URL+"/mount", "management-token")
			if err != nil {
				t.Fatal(err)
			}
			var got Source
			switch name {
			case "If-Match":
				got, err = client.PutSourceIfMatch(context.Background(), input, revision)
			case "If-None-Match":
				got, err = client.PutSourceIfAbsent(context.Background(), input)
			default:
				got, err = client.PutSource(context.Background(), input)
			}
			if err != nil {
				t.Errorf("PutSource: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("requests = %d, want 1", calls.Load())
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("returned source = %#v, want %#v", got, want)
			}
			if input.Revision != "forged" || input.SearchStatus == nil || input.KnowledgeStatus == nil {
				t.Error("PUT modified caller's source observations")
			}
		})
	}
}

func TestClientSourceRevisionEmptyPrecondition(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		WriteJSON(w, http.StatusOK, revisionTestSource())
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "management-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutSourceIfMatch(context.Background(), revisionTestSource(), "")
	var failure *HTTPError
	if !errors.As(err, &failure) {
		t.Errorf("error = %T %v, want HTTPError(400, invalid_precondition)", err, err)
	} else if failure.Status != http.StatusBadRequest || failure.Code != "invalid_precondition" {
		t.Errorf("error = %#v, want HTTPError(400, invalid_precondition)", failure)
	}
	if calls.Load() != 0 {
		t.Errorf("empty precondition made %d requests", calls.Load())
	}
}
