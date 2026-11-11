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

package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Extend the existing fakes without modifying pre-existing tests.
type retireFakeAPI struct {
	*runFakeAPI
	removeSource func(string, bool) (management.Mutation, error)
}

var _ API = (*retireFakeAPI)(nil)

func (f *retireFakeAPI) RemoveSource(_ context.Context, id string, purge bool) (management.Mutation, error) {
	f.calls = append(f.calls, apiCall{method: "RemoveSource", id: id})
	if f.removeSource == nil {
		f.t.Fatal("unexpected RemoveSource call")
		return management.Mutation{}, nil
	}
	return f.removeSource(id, purge)
}

func TestArchiveRetainsIndexedContent(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			want := management.Mutation{SourceId: "src-1", State: "retained"}
			api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				listSources: func(int, string) (management.SourcePage, error) {
					return management.SourcePage{Data: []management.Source{{Id: "src-1", Name: "Product Docs"}}}, nil
				},
				getSource: func(id string) (management.Source, error) {
					return management.Source{Id: id, Name: "Product Docs"}, nil
				},
			}}}
			api.removeSource = func(id string, purge bool) (management.Mutation, error) {
				if id != "src-1" || purge {
					t.Fatalf("RemoveSource(%q, %t), want src-1, false", id, purge)
				}
				return want, nil
			}
			var out bytes.Buffer
			got, err := Archive(context.Background(), Env{API: api, Out: &out, JSON: machine}, "Product Docs")
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("mutation = %#v, want %#v", got, want)
			}
			wantCalls := []apiCall{{method: "ListSources", limit: 100}, {method: "GetSource", id: "src-1"}, {method: "RemoveSource", id: "src-1"}}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v (no StartRun)", api.calls, wantCalls)
			}
			if machine {
				var decoded map[string]json.RawMessage
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if len(decoded) != 2 || string(decoded["source_id"]) != `"src-1"` || string(decoded["state"]) != `"retained"` {
					t.Fatalf("JSON must be the Mutation: %s", out.Bytes())
				}
			} else {
				wantText := "Archived source Product Docs (src-1); state retained.\nNew runs and media reads are blocked. Indexed content is retained; purge it explicitly with purge.\n"
				if out.String() != wantText {
					t.Fatalf("output = %q, want %q", out.String(), wantText)
				}
			}
		})
	}
}

func TestArchiveActiveRunConflict(t *testing.T) {
	api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t,
		getSource: func(id string) (management.Source, error) {
			return management.Source{Id: id, Name: "Docs"}, nil
		},
	}}}
	api.removeSource = func(id string, purge bool) (management.Mutation, error) {
		if id != "src-1" || purge {
			t.Fatalf("RemoveSource(%q, %t), want src-1, false", id, purge)
		}
		return management.Mutation{}, &management.HTTPError{Status: http.StatusConflict, Code: "run_in_progress"}
	}
	var out bytes.Buffer
	_, err := Archive(context.Background(), Env{API: api, Out: &out}, "src-1")
	failure := assertCode(t, err, CodeConflict)
	if failure.ExitCode() != 5 {
		t.Fatalf("exit = %d, want 5", failure.ExitCode())
	}
	wantCalls := []apiCall{{method: "GetSource", id: "src-1"}, {method: "RemoveSource", id: "src-1"}}
	if !reflect.DeepEqual(api.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
	}
	if out.Len() != 0 {
		t.Fatalf("archive failure printed success: %q", out.String())
	}
}

func TestPurgeRequiresExplicitConfirmation(t *testing.T) {
	api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t}}}
	_, err := Purge(context.Background(), Env{API: api, Out: io.Discard}, "src-1", PurgeOptions{})
	if len(api.calls) != 0 {
		t.Fatalf("unconfirmed purge made API calls: %#v", api.calls)
	}
	failure := assertCode(t, err, CodeInvalid)
	want := "purge permanently deletes this source's indexed records; confirm explicitly to continue"
	if failure.Message != want {
		t.Fatalf("message = %q, want %q", failure.Message, want)
	}
	if failure.IdempotencyKey != "" {
		t.Errorf("unconfirmed purge error carries submission key %q", failure.IdempotencyKey)
	}
}

func TestPurgeInvalidKeyMakesNoCalls(t *testing.T) {
	for _, key := range []string{"a b", strings.Repeat("a", 129), "a/b", "é"} {
		t.Run(key, func(t *testing.T) {
			api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t}}}
			_, err := Purge(context.Background(), Env{API: api, Out: io.Discard}, "src-1", PurgeOptions{Confirm: true, IdempotencyKey: key})
			if len(api.calls) != 0 {
				t.Fatalf("invalid key made API calls: %#v", api.calls)
			}
			failure := assertCode(t, err, CodeInvalid)
			if failure.IdempotencyKey != "" {
				t.Errorf("validation error carries submission key %q", failure.IdempotencyKey)
			}
		})
	}
}

func TestPurgeCapabilityGates(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps management.Capabilities
	}{
		{"purge_sources", management.Capabilities{Runtime: "colony", PerSourceRuns: true}},
		{"per_source_runs", management.Capabilities{Runtime: "colony", PurgeSources: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) { return tc.caps, nil },
			}}}
			_, err := Purge(context.Background(), Env{API: api, Out: io.Discard}, "src-1", PurgeOptions{Confirm: true})
			failure := assertCode(t, err, CodeUnsupported)
			if failure.ExitCode() != 6 {
				t.Fatalf("exit = %d, want 6", failure.ExitCode())
			}
			if !strings.Contains(failure.Message, tc.name) {
				t.Fatalf("unsupported message does not identify %s: %q", tc.name, failure.Message)
			}
			if len(api.calls) < 1 || len(api.calls) > 2 {
				t.Fatalf("capability calls = %#v", api.calls)
			}
			for _, call := range api.calls {
				if call.method != "Capabilities" {
					t.Fatalf("unsupported purge made non-capability call: %#v", call)
				}
			}
		})
	}
}

func TestPurgeReceiptAndSubmissionOrder(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		machine   bool
	}{
		{name: "generated human"},
		{name: "generated JSON", machine: true},
		{name: "supplied human", key: "Retry_KEY-7"},
		{name: "supplied JSON", key: "a", machine: true},
		{name: "maximum key", key: strings.Repeat("x", 128)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutation := management.Mutation{SourceId: "src-1", State: "purge_pending"}
			run := management.Run{Id: "run-7", SourceId: "src-1", State: "pending"}
			api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{PurgeSources: true, PerSourceRuns: true}, nil
				},
				getSource: func(id string) (management.Source, error) {
					return management.Source{Id: id, Name: "Docs"}, nil
				},
			}}}
			api.removeSource = func(id string, purge bool) (management.Mutation, error) {
				if id != "src-1" || !purge {
					t.Fatalf("RemoveSource(%q, %t), want src-1, true", id, purge)
				}
				return mutation, nil
			}
			var key string
			api.startRun = func(id, gotKey string, in management.RunInput) (management.Run, error) {
				if id != "src-1" || in != (management.RunInput{Operation: management.RunInputOperationPurge}) {
					t.Fatalf("purge submission = %q, %#v", id, in)
				}
				key = gotKey
				return run, nil
			}
			var out bytes.Buffer
			got, err := Purge(context.Background(), Env{API: api, Out: &out, JSON: tc.machine}, "src-1", PurgeOptions{Confirm: true, IdempotencyKey: tc.key})
			if err != nil {
				t.Fatal(err)
			}
			if tc.key == "" {
				if !regexp.MustCompile(`^cli-[0-9a-f]{32}$`).MatchString(key) {
					t.Fatalf("generated key = %q", key)
				}
			} else if key != tc.key {
				t.Fatalf("key = %q, want verbatim %q", key, tc.key)
			}
			want := PurgeReceipt{Mutation: mutation, Run: run, IdempotencyKey: key}
			if got != want {
				t.Fatalf("receipt = %#v, want %#v", got, want)
			}
			assertPurgeCallOrder(t, api.calls)
			if tc.machine {
				var decoded struct {
					Mutation       management.Mutation `json:"mutation"`
					Run            management.Run      `json:"run"`
					IdempotencyKey string              `json:"idempotency_key"`
				}
				decoder := json.NewDecoder(&out)
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&decoded); err != nil {
					t.Fatal(err)
				}
				if decoded.Mutation != mutation || decoded.Run != run || decoded.IdempotencyKey != key {
					t.Fatalf("JSON receipt = %#v, want %#v", decoded, want)
				}
				var extra any
				if err := decoder.Decode(&extra); err != io.EOF {
					t.Fatalf("unexpected output after JSON receipt: %v, %v", extra, err)
				}
			} else {
				wantText := fmt.Sprintf("Purge intent recorded for Docs (src-1); purge run run-7 pending.\nIdempotency key: %s\n", key)
				if out.String() != wantText {
					t.Fatalf("output = %q, want %q", out.String(), wantText)
				}
			}
		})
	}
}

func assertPurgeCallOrder(t *testing.T, calls []apiCall) {
	t.Helper()
	capabilities := 0
	for capabilities < len(calls) && calls[capabilities].method == "Capabilities" {
		capabilities++
	}
	if capabilities < 1 || capabilities > 2 {
		t.Fatalf("expected one or two initial capability calls, got %#v", calls)
	}
	want := []apiCall{{method: "GetSource", id: "src-1"}, {method: "RemoveSource", id: "src-1"}, {method: "StartRun", id: "src-1"}}
	if !reflect.DeepEqual(calls[capabilities:], want) {
		t.Fatalf("calls after capability gates = %#v, want %#v", calls[capabilities:], want)
	}
}

func TestPurgeStartFailurePreservesIntent(t *testing.T) {
	for _, suppliedKey := range []string{"", "retry-purge"} {
		t.Run("key="+suppliedKey, func(t *testing.T) {
			api := &retireFakeAPI{runFakeAPI: &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{PurgeSources: true, PerSourceRuns: true}, nil
				},
				getSource: func(id string) (management.Source, error) {
					return management.Source{Id: id, Name: "Docs"}, nil
				},
			}}}
			mutation := management.Mutation{SourceId: "src-1", State: "purge_pending"}
			api.removeSource = func(id string, purge bool) (management.Mutation, error) {
				if id != "src-1" || !purge {
					t.Fatalf("RemoveSource(%q, %t), want src-1, true", id, purge)
				}
				return mutation, nil
			}
			original := &management.HTTPError{Status: http.StatusServiceUnavailable}
			var submittedKey string
			api.startRun = func(id, key string, in management.RunInput) (management.Run, error) {
				if id != "src-1" || in != (management.RunInput{Operation: management.RunInputOperationPurge}) {
					t.Fatalf("purge submission = %q, %q, %#v", id, key, in)
				}
				submittedKey = key
				return management.Run{}, original
			}
			var out bytes.Buffer
			receipt, err := Purge(context.Background(), Env{API: api, Out: &out}, "src-1", PurgeOptions{Confirm: true, IdempotencyKey: suppliedKey})
			failure := assertCode(t, err, CodeFailed)
			if _, ok := err.(*Error); !ok {
				t.Errorf("submission error = %T, want *Error", err)
			}
			if suppliedKey == "" {
				if !regexp.MustCompile(`^cli-[0-9a-f]{32}$`).MatchString(submittedKey) {
					t.Errorf("generated key = %q", submittedKey)
				}
			} else if submittedKey != suppliedKey {
				t.Errorf("submitted key = %q, want %q", submittedKey, suppliedKey)
			}
			if receipt.Mutation != mutation || receipt.IdempotencyKey != submittedKey || failure.IdempotencyKey != receipt.IdempotencyKey {
				t.Errorf("submitted key=%q receipt=%#v error key=%q", submittedKey, receipt, failure.IdempotencyKey)
			}
			prefix := "purge intent is recorded for src-1; purge can be retried with the same command: "
			suffix := " (idempotency key " + submittedKey + "; retry with this key to avoid a duplicate submission)"
			if !strings.HasPrefix(failure.Message, prefix) || !strings.HasSuffix(failure.Message, suffix) || !strings.Contains(err.Error(), submittedKey) {
				t.Errorf("submission error = %q, want intent prefix %q and key suffix %q", err.Error(), prefix, suffix)
			}
			var httpError *management.HTTPError
			if !errors.As(err, &httpError) || httpError != original {
				t.Errorf("submission error lost original HTTP error: %v", err)
			}
			assertPurgeCallOrder(t, api.calls)
			if out.Len() != 0 {
				t.Fatalf("failed purge printed success: %q", out.String())
			}
		})
	}
}
