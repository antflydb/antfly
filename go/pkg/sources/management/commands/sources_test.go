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

type apiCall struct {
	method string
	id     string
	limit  int
	cursor string
}

type fakeAPI struct {
	t            *testing.T
	calls        []apiCall
	capabilities func() (management.Capabilities, error)
	listSources  func(int, string) (management.SourcePage, error)
	getSource    func(string) (management.Source, error)
	listRuns     func(string, int, string) (management.RunPage, error)
}

var _ API = (*fakeAPI)(nil)

func (f *fakeAPI) Capabilities(context.Context) (management.Capabilities, error) {
	f.calls = append(f.calls, apiCall{method: "Capabilities"})
	if f.capabilities == nil {
		f.t.Fatal("unexpected Capabilities call")
		return management.Capabilities{}, nil
	}
	return f.capabilities()
}
func (f *fakeAPI) ListSources(_ context.Context, limit int, cursor string) (management.SourcePage, error) {
	f.calls = append(f.calls, apiCall{method: "ListSources", limit: limit, cursor: cursor})
	if f.listSources == nil {
		f.t.Fatal("unexpected ListSources call")
		return management.SourcePage{}, nil
	}
	return f.listSources(limit, cursor)
}
func (f *fakeAPI) GetSource(_ context.Context, id string) (management.Source, error) {
	f.calls = append(f.calls, apiCall{method: "GetSource", id: id})
	if f.getSource == nil {
		f.t.Fatal("unexpected GetSource call")
		return management.Source{}, nil
	}
	return f.getSource(id)
}
func (f *fakeAPI) ListRuns(_ context.Context, id string, limit int, cursor string) (management.RunPage, error) {
	f.calls = append(f.calls, apiCall{method: "ListRuns", id: id, limit: limit, cursor: cursor})
	if f.listRuns == nil {
		f.t.Fatal("unexpected ListRuns call")
		return management.RunPage{}, nil
	}
	return f.listRuns(id, limit, cursor)
}
func (f *fakeAPI) PutSource(context.Context, management.Source) (management.Source, error) {
	f.t.Fatal("unexpected PutSource call")
	return management.Source{}, nil
}
func (f *fakeAPI) RemoveSource(context.Context, string, bool) (management.Mutation, error) {
	f.t.Fatal("unexpected RemoveSource call")
	return management.Mutation{}, nil
}
func (f *fakeAPI) GetRun(context.Context, string, string) (management.Run, error) {
	f.t.Fatal("unexpected GetRun call")
	return management.Run{}, nil
}
func (f *fakeAPI) StartRun(context.Context, string, string, management.RunInput) (management.Run, error) {
	f.t.Fatal("unexpected StartRun call")
	return management.Run{}, nil
}
func (f *fakeAPI) CancelRun(context.Context, string, string) (management.Run, error) {
	f.t.Fatal("unexpected CancelRun call")
	return management.Run{}, nil
}

func TestCapabilitiesOutput(t *testing.T) {
	want := management.Capabilities{Runtime: "standalone", Version: 1, PerSourceRuns: true, CancelRuns: true, ConfigureSources: true, SearchVersions: []int{1, 2}, ProcessingJobs: true}
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := &fakeAPI{t: t, capabilities: func() (management.Capabilities, error) { return want, nil }}
			var out bytes.Buffer
			got, err := Capabilities(context.Background(), Env{API: api, Out: &out, JSON: machine})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("capabilities = %#v, want %#v", got, want)
			}
			if machine {
				var decoded management.Capabilities
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decoded, want) {
					t.Fatalf("JSON = %#v, want %#v", decoded, want)
				}
				if !strings.Contains(out.String(), "\n  \"runtime\":") || !strings.HasSuffix(out.String(), "\n") {
					t.Fatalf("expected two-space indented JSON with newline: %q", out.String())
				}
			} else {
				for _, text := range []string{"FEATURE", "VALUE", "standalone", "true", "false"} {
					if !strings.Contains(out.String(), text) {
						t.Errorf("output missing %q: %s", text, &out)
					}
				}
			}
		})
	}
}

func TestListSourcesSinglePageAndLimit(t *testing.T) {
	for _, limit := range []int{0, 1, 100} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			want := management.SourcePage{Data: []management.Source{{Id: "first"}}, NextCursor: "next"}
			api := &fakeAPI{t: t, listSources: func(int, string) (management.SourcePage, error) { return want, nil }}
			got, err := ListSources(context.Background(), Env{API: api, Out: io.Discard}, ListOptions{Limit: limit, Cursor: "start"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("page = %#v, want %#v", got, want)
			}
			wantLimit := limit
			if limit == 0 {
				wantLimit = 20
			} // Zero is the specified default, not an invalid limit.
			if wantCalls := []apiCall{{method: "ListSources", limit: wantLimit, cursor: "start"}}; !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
		})
	}
}

func TestListSourcesInvalidLimit(t *testing.T) {
	for _, limit := range []int{-1, 101} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			api := &fakeAPI{t: t}
			_, err := ListSources(context.Background(), Env{API: api, Out: io.Discard}, ListOptions{Limit: limit})
			assertCode(t, err, CodeInvalid)
			if len(api.calls) != 0 {
				t.Fatalf("invalid limit made API calls: %#v", api.calls)
			}
		})
	}
}

func TestListSourcesAllFollowsEmptyPages(t *testing.T) {
	pages := map[string]management.SourcePage{
		"start": {Data: []management.Source{{Id: "one"}, {Id: "two"}}, NextCursor: "a"},
		"a":     {Data: []management.Source{}, NextCursor: "b"},
		"b":     {Data: []management.Source{{Id: "three"}}},
	}
	api := &fakeAPI{t: t, listSources: func(_ int, cursor string) (management.SourcePage, error) {
		page, ok := pages[cursor]
		if !ok {
			t.Fatalf("unexpected cursor %q", cursor)
		}
		return page, nil
	}}
	got, err := ListSources(context.Background(), Env{API: api, Out: io.Discard}, ListOptions{All: true, Limit: 2, Cursor: "start"})
	if err != nil {
		t.Fatal(err)
	}
	want := management.SourcePage{Data: []management.Source{{Id: "one"}, {Id: "two"}, {Id: "three"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("page = %#v, want %#v", got, want)
	}
	wantCalls := []apiCall{{method: "ListSources", limit: 2, cursor: "start"}, {method: "ListSources", limit: 2, cursor: "a"}, {method: "ListSources", limit: 2, cursor: "b"}}
	if !reflect.DeepEqual(api.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
	}
}

func TestListSourcesRepeatedCursor(t *testing.T) {
	for _, initial := range []string{"", "a"} {
		t.Run("initial="+initial, func(t *testing.T) {
			requests := 0
			api := &fakeAPI{t: t, listSources: func(int, string) (management.SourcePage, error) {
				requests++
				if requests > 3 {
					t.Fatal("continued following a repeated cursor")
				}
				return management.SourcePage{NextCursor: "a"}, nil
			}}
			_, err := ListSources(context.Background(), Env{API: api, Out: io.Discard}, ListOptions{All: true, Cursor: initial})
			failure := assertCode(t, err, CodeFailed)
			if !strings.Contains(strings.ToLower(failure.Message), "repeat") || !strings.Contains(failure.Message, "cursor") {
				t.Fatalf("message does not identify repeated cursor: %q", failure.Message)
			}
		})
	}
}

func TestListSourcesPageCap(t *testing.T) {
	requests := 0
	api := &fakeAPI{t: t, listSources: func(int, string) (management.SourcePage, error) {
		requests++
		if requests > 1000 {
			t.Fatal("exceeded 1000-page hard cap")
		}
		return management.SourcePage{NextCursor: fmt.Sprint(requests)}, nil
	}}
	_, err := ListSources(context.Background(), Env{API: api, Out: io.Discard}, ListOptions{All: true})
	assertCode(t, err, CodeFailed)
	if requests != 1000 {
		t.Fatalf("requests = %d, want 1000", requests)
	}
}

func TestListSourcesOutput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		machine bool
		page    management.SourcePage
	}{
		{"empty JSON", true, management.SourcePage{}},
		{"page JSON", true, management.SourcePage{Data: []management.Source{{Id: "id-one", Name: "source-one"}}, NextCursor: "next-token"}},
		{"empty human", false, management.SourcePage{}},
		{"human page", false, management.SourcePage{Data: []management.Source{{Id: "id-one", Name: "source-one", Table: "docs", State: "active", Publication: "native"}}, NextCursor: "next-token"}},
		{"empty human with cursor", false, management.SourcePage{NextCursor: "next-token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeAPI{t: t, listSources: func(int, string) (management.SourcePage, error) { return tc.page, nil }}
			var out bytes.Buffer
			got, err := ListSources(context.Background(), Env{API: api, Out: &out, JSON: tc.machine}, ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if tc.machine {
				var decoded management.SourcePage
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decoded, got) {
					t.Fatalf("JSON = %#v, returned %#v", decoded, got)
				}
				if decoded.NextCursor != tc.page.NextCursor {
					t.Fatalf("lost next cursor: %#v", decoded)
				}
				if len(tc.page.Data) == 0 {
					var fields map[string]json.RawMessage
					if err := json.Unmarshal(out.Bytes(), &fields); err != nil {
						t.Fatal(err)
					}
					if string(fields["data"]) != "[]" {
						t.Fatalf("data = %s, want []", fields["data"])
					}
				}
				if !strings.HasSuffix(out.String(), "\n") || !strings.Contains(out.String(), "\n  \"data\":") {
					t.Fatalf("incorrect JSON formatting: %q", out.String())
				}
			} else {
				if len(tc.page.Data) == 0 {
					if !strings.Contains(out.String(), "No sources.") {
						t.Fatalf("empty output = %q", out.String())
					}
				} else {
					for _, text := range []string{"ID", "NAME", "TABLE", "STATE", "PUBLICATION", "id-one", "source-one", "docs", "active", "native"} {
						if !strings.Contains(out.String(), text) {
							t.Errorf("missing %q: %s", text, &out)
						}
					}
				}
				if tc.page.NextCursor != "" && !strings.Contains(out.String(), "Next cursor: next-token\n") {
					t.Fatalf("missing cursor line: %q", out.String())
				}
			}
		})
	}
}

func TestResolveSourceByID(t *testing.T) {
	want := management.Source{Id: "id_1-A", Name: "source", Connection: "connection", Table: "docs"}
	api := &fakeAPI{t: t, getSource: func(id string) (management.Source, error) {
		if id != want.Id {
			t.Fatalf("id = %q", id)
		}
		return want, nil
	}}
	got, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, want.Id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source = %#v, want %#v", got, want)
	}
	if wantCalls := []apiCall{{method: "GetSource", id: want.Id}}; !reflect.DeepEqual(api.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
	}
}

func TestResolveSourceByNameHydratesUniqueMatch(t *testing.T) {
	for _, status := range []int{400, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			want := management.Source{Id: "source-id", Name: "friendly", Connection: "full-connection", Table: "full-table"}
			api := &fakeAPI{t: t,
				getSource: func(id string) (management.Source, error) {
					if id == "friendly" {
						return management.Source{}, management.Failure(status, "source_not_found")
					}
					if id != want.Id {
						t.Fatalf("unexpected id %q", id)
					}
					return want, nil
				},
				listSources: func(limit int, cursor string) (management.SourcePage, error) {
					if limit != 100 {
						t.Fatalf("lookup limit = %d, want 100", limit)
					}
					switch cursor {
					case "":
						return management.SourcePage{Data: []management.Source{{Id: "other", Name: "Friendly"}}, NextCursor: "a"}, nil
					case "a":
						return management.SourcePage{NextCursor: "b"}, nil
					case "b":
						return management.SourcePage{Data: []management.Source{{Id: want.Id, Name: want.Name}}}, nil
					default:
						t.Fatalf("unexpected cursor %q", cursor)
						return management.SourcePage{}, nil
					}
				},
			}
			got, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, "friendly")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("source = %#v, want hydrated %#v", got, want)
			}
			wantCalls := []apiCall{{method: "GetSource", id: "friendly"}, {method: "ListSources", limit: 100}, {method: "ListSources", limit: 100, cursor: "a"}, {method: "ListSources", limit: 100, cursor: "b"}, {method: "GetSource", id: want.Id}}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
		})
	}
}

func TestResolveSourceNameWithSpacesSkipsIDLookup(t *testing.T) {
	want := management.Source{Id: "resolved-id", Name: "a name with spaces", Table: "docs"}
	api := &fakeAPI{t: t,
		listSources: func(int, string) (management.SourcePage, error) {
			return management.SourcePage{Data: []management.Source{{Id: want.Id, Name: want.Name}}}, nil
		},
		getSource: func(id string) (management.Source, error) {
			if id != want.Id {
				t.Fatalf("GetSource called with name %q", id)
			}
			return want, nil
		},
	}
	got, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, want.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source = %#v, want %#v", got, want)
	}
}

func TestResolveSourceAmbiguousNameAcrossPages(t *testing.T) {
	api := &fakeAPI{t: t,
		getSource: func(string) (management.Source, error) {
			return management.Source{}, management.Failure(404, "source_not_found")
		},
		listSources: func(_ int, cursor string) (management.SourcePage, error) {
			if cursor == "" {
				return management.SourcePage{Data: []management.Source{{Id: "id-one", Name: "duplicate"}}, NextCursor: "a"}, nil
			}
			if cursor != "a" {
				t.Fatalf("unexpected cursor %q", cursor)
			}
			return management.SourcePage{Data: []management.Source{{Id: "id-two", Name: "duplicate"}}}, nil
		},
	}
	_, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, "duplicate")
	failure := assertCode(t, err, CodeInvalid)
	for _, text := range []string{"id-one", "id-two", "ID"} {
		if !strings.Contains(failure.Message, text) {
			t.Errorf("ambiguous message missing %q: %q", text, failure.Message)
		}
	}
}

func TestResolveSourceMissingEmptyAndForbidden(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		api := &fakeAPI{t: t,
			getSource: func(string) (management.Source, error) {
				return management.Source{}, management.Failure(404, "source_not_found")
			},
			listSources: func(int, string) (management.SourcePage, error) {
				return management.SourcePage{Data: []management.Source{{Id: "other", Name: "Missing"}}}, nil
			},
		}
		_, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, "missing")
		failure := assertCode(t, err, CodeNotFound)
		if failure.Message != `source "missing" not found` {
			t.Fatalf("message = %q", failure.Message)
		}
	})
	t.Run("empty", func(t *testing.T) {
		api := &fakeAPI{t: t}
		_, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, "")
		assertCode(t, err, CodeInvalid)
		if len(api.calls) != 0 {
			t.Fatalf("empty reference made API calls: %#v", api.calls)
		}
	})
	t.Run("forbidden", func(t *testing.T) {
		api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) {
			return management.Source{}, management.Failure(403, "access_denied")
		}}
		_, err := ResolveSource(context.Background(), Env{API: api, Out: io.Discard}, "private")
		failure := assertCode(t, err, CodeForbidden)
		if failure.HTTPStatus != 403 || failure.ServerCode != "access_denied" {
			t.Fatalf("lost HTTP fields: %#v", failure)
		}
		if want := []apiCall{{method: "GetSource", id: "private"}}; !reflect.DeepEqual(api.calls, want) {
			t.Fatalf("forbidden fell back to name lookup: %#v", api.calls)
		}
	})
}

func TestCommandHTTPFailuresAreClassified(t *testing.T) {
	for _, command := range []string{"capabilities", "list", "resolve"} {
		t.Run(command, func(t *testing.T) {
			failure := management.Failure(401, "token_expired")
			api := &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) { return management.Capabilities{}, failure },
				listSources:  func(int, string) (management.SourcePage, error) { return management.SourcePage{}, failure },
				getSource:    func(string) (management.Source, error) { return management.Source{}, failure },
			}
			env := Env{API: api, Out: io.Discard}
			var err error
			switch command {
			case "capabilities":
				_, err = Capabilities(context.Background(), env)
			case "list":
				_, err = ListSources(context.Background(), env, ListOptions{})
			case "resolve":
				_, err = ResolveSource(context.Background(), env, "id")
			}
			assertCode(t, err, CodeUnauthorized)
		})
	}
}
