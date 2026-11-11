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
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func TestStatusLatestRunAndReadiness(t *testing.T) {
	source := management.Source{Id: "source-id", Name: "demo", Table: "docs", Connection: "local", State: "active", Publication: "native", SearchStatus: &management.SearchStatus{
		Version: 2, Consistency: "retained_snapshot", ObservedAt: 12345,
		Streams:   []management.RepresentationStatus{{Name: "text", State: "ready"}, {Name: "title", State: "ready"}, {Name: "embeddings", State: "pending"}},
		Artifacts: []management.RepresentationStatus{{Name: "pdf", State: "building"}, {Name: "image", State: "failed"}},
	}}
	run := management.Run{Id: "latest-run", SourceId: source.Id, State: "completed", Outcome: "success"}
	wantReadiness := ReadinessSummary{Reported: true, Consistency: "retained_snapshot", ObservedAt: 12345, Streams: map[string]int{"ready": 2, "pending": 1}, Artifacts: map[string]int{"building": 1, "failed": 1}}
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := &fakeAPI{t: t,
				getSource: func(id string) (management.Source, error) {
					if id != source.Id {
						t.Fatalf("source id = %q", id)
					}
					return source, nil
				},
				listRuns: func(id string, limit int, cursor string) (management.RunPage, error) {
					if id != source.Id || limit != 1 || cursor != "" {
						t.Fatalf("ListRuns(%q, %d, %q), want source-id, 1, empty", id, limit, cursor)
					}
					return management.RunPage{Data: []management.Run{run}}, nil
				},
			}
			var out bytes.Buffer
			got, err := Status(context.Background(), Env{API: api, Out: &out, JSON: machine}, source.Id)
			if err != nil {
				t.Fatal(err)
			}
			want := StatusView{Source: source, LatestRun: &run, Readiness: wantReadiness}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("status = %#v, want %#v", got, want)
			}
			if wantCalls := []apiCall{{method: "GetSource", id: source.Id}, {method: "ListRuns", id: source.Id, limit: 1}}; !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
			if machine {
				var decoded StatusView
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decoded, want) {
					t.Fatalf("JSON status = %#v, want %#v", decoded, want)
				}
				if !strings.Contains(out.String(), "\n  \"source\":") || !strings.HasSuffix(out.String(), "\n") {
					t.Fatalf("incorrect JSON format: %q", out.String())
				}
			} else {
				for _, text := range []string{"Source:", "demo", "source-id", "Table:", "docs", "Connection:", "local", "State:", "active", "Publication:", "native", "Acquisition:", "completed", "success", "latest-run", "Search:", "retained_snapshot", "ready=2", "pending=1", "building=1", "failed=1"} {
					if !strings.Contains(out.String(), text) {
						t.Errorf("status missing %q: %s", text, &out)
					}
				}
				if strings.Contains(strings.ToLower(out.String()), "searchable") {
					t.Fatalf("completed acquisition described as searchable while streams pending: %s", &out)
				}
			}
		})
	}
}

func TestStatusSearchNotReportedAndNoRuns(t *testing.T) {
	source := management.Source{Id: "source-id", Name: "demo"}
	api := &fakeAPI{t: t,
		getSource: func(string) (management.Source, error) { return source, nil },
		listRuns:  func(string, int, string) (management.RunPage, error) { return management.RunPage{}, nil },
	}
	var out bytes.Buffer
	got, err := Status(context.Background(), Env{API: api, Out: &out}, source.Id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Readiness.Reported || got.LatestRun != nil {
		t.Fatalf("invented readiness or latest run: %#v", got)
	}
	if !strings.Contains(out.String(), "not reported") || !strings.Contains(out.String(), "no runs yet") {
		t.Fatalf("output = %q", out.String())
	}
	if !strings.Contains(out.String(), "Acquisition:") || !strings.Contains(out.String(), "Search:") {
		t.Fatalf("readiness not visibly distinct: %q", out.String())
	}
}

func TestStatusToleratesUnavailableRunListing(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{501, "not_implemented"}, {400, "runs_unsupported"}, {404, "runs_not_found"}} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.code), func(t *testing.T) {
			source := management.Source{Id: "source-id", SearchStatus: &management.SearchStatus{Consistency: "live_read", Streams: []management.RepresentationStatus{{Name: "text", State: "ready"}}}}
			api := &fakeAPI{t: t,
				getSource: func(string) (management.Source, error) { return source, nil },
				listRuns: func(string, int, string) (management.RunPage, error) {
					return management.RunPage{}, management.Failure(tc.status, tc.code)
				},
			}
			got, err := Status(context.Background(), Env{API: api, Out: io.Discard}, source.Id)
			if err != nil {
				t.Fatal(err)
			}
			if got.LatestRun != nil || !got.Readiness.Reported || got.Readiness.Streams["ready"] != 1 {
				t.Fatalf("unavailable runs lost readiness or invented run: %#v", got)
			}
		})
	}
}

func TestStatusReturnsOtherRunListingErrors(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{403, CodeForbidden}, {500, CodeFailed}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			api := &fakeAPI{t: t,
				getSource: func(string) (management.Source, error) { return management.Source{Id: "source-id"}, nil },
				listRuns: func(string, int, string) (management.RunPage, error) {
					return management.RunPage{}, management.Failure(tc.status, "server_failure")
				},
			}
			_, err := Status(context.Background(), Env{API: api, Out: io.Discard}, "source-id")
			failure := assertCode(t, err, tc.code)
			if failure.HTTPStatus != tc.status {
				t.Fatalf("HTTPStatus = %d, want %d", failure.HTTPStatus, tc.status)
			}
		})
	}
}

func TestStatusReturnsResolutionErrors(t *testing.T) {
	api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) {
		return management.Source{}, management.Failure(403, "access_denied")
	}}
	_, err := Status(context.Background(), Env{API: api, Out: io.Discard}, "private-source")
	assertCode(t, err, CodeForbidden)
	if want := []apiCall{{method: "GetSource", id: "private-source"}}; !reflect.DeepEqual(api.calls, want) {
		t.Fatalf("failed resolution made extra calls: %#v", api.calls)
	}
}
