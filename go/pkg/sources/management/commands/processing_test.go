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

func TestProcessingStatusRepresentations(t *testing.T) {
	source := management.Source{
		Id: "source-id", Name: "demo", SearchVersion: 2,
		Streams:   []management.SearchStream{{Name: "z-text"}, {Name: "preview"}},
		Artifacts: []management.ArtifactBinding{{Name: "preview", Artifact: "thumbnail"}},
		Processing: &management.ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{
			{Name: "z-vector"}, {Name: "a-text"},
		}},
		SearchStatus: &management.SearchStatus{
			Version: 2, Consistency: "retained_snapshot", ObservedAt: 12345,
			Streams: []management.RepresentationStatus{{Name: "z-text", State: "ready"}},
			Artifacts: []management.RepresentationStatus{
				{Name: "runtime-only", State: "unsupported", Reason: "coverage unknown"},
				{Name: "preview", State: "failed", Reason: "decoder failed"},
			},
		},
	}
	want := ProcessingView{
		SourceID: source.Id, SearchVersion: 2, Reported: true,
		Consistency: "retained_snapshot", ObservedAt: 12345,
		Indexes: []string{"z-vector", "a-text"},
		Representations: []RepresentationView{
			{Kind: "stream", Name: "z-text", Declared: true, State: "ready"},
			{Kind: "stream", Name: "preview", Declared: true, State: "unreported"},
			{Kind: "artifact", Name: "preview", Declared: true, State: "failed", Reason: "decoder failed"},
			{Kind: "artifact", Name: "runtime-only", State: "unsupported", Reason: "coverage unknown"},
		},
	}
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) { return source, nil }}
			var out bytes.Buffer
			got, err := ProcessingStatus(context.Background(), Env{API: api, Out: &out, JSON: machine}, source.Id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("processing = %#v, want %#v", got, want)
			}
			if machine {
				var decoded ProcessingView
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decoded, want) {
					t.Fatalf("JSON = %#v, want %#v", decoded, want)
				}
				return
			}
			text := strings.Join(strings.Fields(out.String()), " ")
			for _, fragment := range []string{
				"Source: demo (source-id)",
				"Search: search_version 2; retained_snapshot; observed 1970-01-01T03:25:45Z",
				"Indexes: z-vector, a-text",
				"KIND NAME DECLARED STATE REASON",
				"stream z-text true ready",
				"stream preview true unreported",
				"artifact preview true failed decoder failed",
				"artifact runtime-only false unsupported coverage unknown",
			} {
				if !strings.Contains(text, fragment) {
					t.Errorf("human output missing %q: %s", fragment, &out)
				}
			}
			if !strings.HasSuffix(strings.TrimSpace(out.String()), "Acquisition runs are tracked separately: see run history.") {
				t.Errorf("missing closing acquisition note: %s", &out)
			}
			if strings.Contains(strings.ToLower(out.String()), "searchable") {
				t.Errorf("readiness described as searchable: %s", &out)
			}
		})
	}
}

func TestProcessingStatusUnreported(t *testing.T) {
	for _, version := range []int{2, 1, 0} {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("version=%d/json=%t", version, machine), func(t *testing.T) {
				source := management.Source{Id: "source-id", Name: "demo", SearchVersion: version}
				api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) { return source, nil }}
				var out bytes.Buffer
				got, err := ProcessingStatus(context.Background(), Env{API: api, Out: &out, JSON: machine}, source.Id)
				if err != nil {
					t.Fatal(err)
				}
				if got.Reported || got.Consistency != "" || got.ObservedAt != 0 {
					t.Errorf("invented readiness: %#v", got)
				}
				if got.Indexes == nil || len(got.Indexes) != 0 || got.Representations == nil || len(got.Representations) != 0 {
					t.Errorf("empty collections must be non-nil: %#v", got)
				}
				if machine {
					var decoded map[string]json.RawMessage
					if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
						t.Fatal(err)
					}
					for _, key := range []string{"indexes", "representations"} {
						if string(bytes.TrimSpace(decoded[key])) != "[]" {
							t.Errorf("JSON %s = %s, want []", key, decoded[key])
						}
					}
					if string(decoded["reported"]) != "false" {
						t.Errorf("JSON reported = %s, want false", decoded["reported"])
					}
					return
				}
				wantSearch := "not reported by this runtime"
				if version < 2 {
					wantSearch = "readiness reporting requires search_version 2 (this source uses 1)"
				}
				text := strings.Join(strings.Fields(out.String()), " ")
				for _, fragment := range []string{wantSearch, "Indexes: none", "No streams or artifacts declared."} {
					if !strings.Contains(text, fragment) {
						t.Errorf("human output missing %q: %s", fragment, &out)
					}
				}
				if strings.Contains(text, "KIND NAME DECLARED STATE REASON") {
					t.Errorf("empty representations printed a table: %s", &out)
				}
			})
		}
	}
}

func TestProcessingStatusDeclaredWithoutReport(t *testing.T) {
	source := management.Source{
		Id: "source-id", SearchVersion: 2,
		Streams:   []management.SearchStream{{Name: "text"}},
		Artifacts: []management.ArtifactBinding{{Name: "original"}},
	}
	api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) { return source, nil }}
	got, err := ProcessingStatus(context.Background(), Env{API: api, Out: io.Discard}, source.Id)
	if err != nil {
		t.Fatal(err)
	}
	want := ProcessingView{
		SourceID: source.Id, SearchVersion: 2, Indexes: []string{},
		Representations: []RepresentationView{
			{Kind: "stream", Name: "text", Declared: true, State: "unreported"},
			{Kind: "artifact", Name: "original", Declared: true, State: "unreported"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("processing = %#v, want %#v", got, want)
	}
}

func TestProcessingStatusUndeclaredOrdering(t *testing.T) {
	source := management.Source{
		Id: "source-id", SearchVersion: 2,
		Streams:   []management.SearchStream{{Name: "z-declared"}, {Name: "a-declared"}},
		Artifacts: []management.ArtifactBinding{{Name: "z-declared"}, {Name: "a-declared"}},
		SearchStatus: &management.SearchStatus{
			Version: 2, Consistency: "unknown",
			Streams: []management.RepresentationStatus{
				{Name: "z-extra", State: "partial"}, {Name: "a-extra", State: "disabled"},
			},
			Artifacts: []management.RepresentationStatus{
				{Name: "z-extra", State: "building"}, {Name: "a-extra", State: "unsupported", Reason: "coverage unknown"},
			},
		},
	}
	api := &fakeAPI{t: t, getSource: func(string) (management.Source, error) { return source, nil }}
	got, err := ProcessingStatus(context.Background(), Env{API: api, Out: io.Discard}, source.Id)
	if err != nil {
		t.Fatal(err)
	}
	want := []RepresentationView{
		{Kind: "stream", Name: "z-declared", Declared: true, State: "unreported"},
		{Kind: "stream", Name: "a-declared", Declared: true, State: "unreported"},
		{Kind: "stream", Name: "a-extra", State: "disabled"},
		{Kind: "stream", Name: "z-extra", State: "partial"},
		{Kind: "artifact", Name: "z-declared", Declared: true, State: "unreported"},
		{Kind: "artifact", Name: "a-declared", Declared: true, State: "unreported"},
		{Kind: "artifact", Name: "a-extra", State: "unsupported", Reason: "coverage unknown"},
		{Kind: "artifact", Name: "z-extra", State: "building"},
	}
	if !reflect.DeepEqual(got.Representations, want) {
		t.Fatalf("representations = %#v, want %#v", got.Representations, want)
	}
}

func TestListArtifactsUnsupportedBeforeResolution(t *testing.T) {
	api := &fakeAPI{t: t, capabilities: func() (management.Capabilities, error) {
		return management.Capabilities{Runtime: "standalone", ArtifactDiscovery: false}, nil
	}}
	_, err := ListArtifacts(context.Background(), Env{API: api, Out: io.Discard}, "source name")
	failure := assertCode(t, err, CodeUnsupported)
	if failure.ExitCode() != 6 {
		t.Errorf("unsupported exit = %d, want 6", failure.ExitCode())
	}
	if want := []apiCall{{method: "Capabilities"}}; !reflect.DeepEqual(api.calls, want) {
		t.Errorf("unsupported discovery looked up a source: %#v", api.calls)
	}
}

func TestListArtifactsJoinsAndOutput(t *testing.T) {
	source := management.Source{
		Id: "source-id", SearchVersion: 2,
		Artifacts: []management.ArtifactBinding{
			{Name: "z-original", Artifact: "original", ContentType: "application/pdf", Purpose: "download"},
			{Name: "a-preview", Artifact: "thumbnail", ContentType: "image/png", Purpose: "preview", Generated: true},
		},
		SearchStatus: &management.SearchStatus{
			Version: 2,
			Streams: []management.RepresentationStatus{{Name: "a-preview", State: "ready"}},
			Artifacts: []management.RepresentationStatus{
				{Name: "z-extra", State: "building"},
				{Name: "a-extra", State: "unsupported", Reason: "coverage unknown"},
				{Name: "z-original", State: "failed", Reason: "object missing"},
			},
		},
	}
	want := []ArtifactView{
		{Name: "z-original", Artifact: "original", ContentType: "application/pdf", Purpose: "download", State: "failed", Reason: "object missing"},
		{Name: "a-preview", Artifact: "thumbnail", ContentType: "image/png", Purpose: "preview", Generated: true, State: "unreported"},
		{Name: "a-extra", State: "unsupported", Reason: "coverage unknown"},
		{Name: "z-extra", State: "building"},
	}
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{ArtifactDiscovery: true}, nil
				},
				getSource: func(string) (management.Source, error) { return source, nil },
			}
			var out bytes.Buffer
			got, err := ListArtifacts(context.Background(), Env{API: api, Out: &out, JSON: machine}, source.Id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("artifacts = %#v, want %#v", got, want)
			}
			if machine {
				var decoded struct {
					SourceID string         `json:"source_id"`
					Data     []ArtifactView `json:"data"`
				}
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded.SourceID != source.Id || !reflect.DeepEqual(decoded.Data, want) {
					t.Fatalf("JSON envelope = %#v, want source %q and %#v", decoded, source.Id, want)
				}
				return
			}
			text := strings.Join(strings.Fields(out.String()), " ")
			for _, fragment := range []string{
				"NAME ARTIFACT CONTENT_TYPE PURPOSE GENERATED STATE REASON",
				"z-original original application/pdf download false failed object missing",
				"a-preview thumbnail image/png preview true unreported",
				"a-extra false unsupported coverage unknown",
				"z-extra false building",
			} {
				if !strings.Contains(text, fragment) {
					t.Errorf("human output missing %q: %s", fragment, &out)
				}
			}
		})
	}
}

func TestListArtifactsEmpty(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{ArtifactDiscovery: true}, nil
				},
				getSource: func(string) (management.Source, error) { return management.Source{Id: "source-id"}, nil },
			}
			var out bytes.Buffer
			got, err := ListArtifacts(context.Background(), Env{API: api, Out: &out, JSON: machine}, "source-id")
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || len(got) != 0 {
				t.Errorf("empty artifacts = %#v, want non-nil empty slice", got)
			}
			if machine {
				var decoded map[string]json.RawMessage
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if string(decoded["source_id"]) != `"source-id"` || string(bytes.TrimSpace(decoded["data"])) != "[]" {
					t.Errorf("empty JSON envelope = %s", &out)
				}
			} else if !strings.Contains(out.String(), "No artifacts declared.") {
				t.Errorf("empty human output = %q", out.String())
			}
		})
	}
}

func TestProcessingAndArtifactsResolveSourceName(t *testing.T) {
	source := management.Source{
		Id: "source-id", Name: "source name", SearchVersion: 2,
		Streams:   []management.SearchStream{{Name: "text"}},
		Artifacts: []management.ArtifactBinding{{Name: "original", Artifact: "binary", ContentType: "application/pdf"}},
	}
	for _, handler := range []string{"processing", "artifacts"} {
		t.Run(handler, func(t *testing.T) {
			api := &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{ArtifactDiscovery: true}, nil
				},
				listSources: func(int, string) (management.SourcePage, error) {
					return management.SourcePage{Data: []management.Source{{Id: source.Id, Name: source.Name}}}, nil
				},
				getSource: func(id string) (management.Source, error) {
					if id != source.Id {
						t.Fatalf("GetSource id = %q, want %q", id, source.Id)
					}
					return source, nil
				},
			}
			env := Env{API: api, Out: io.Discard}
			if handler == "processing" {
				got, err := ProcessingStatus(context.Background(), env, source.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := ProcessingView{
					SourceID: source.Id, SearchVersion: 2, Indexes: []string{},
					Representations: []RepresentationView{
						{Kind: "stream", Name: "text", Declared: true, State: "unreported"},
						{Kind: "artifact", Name: "original", Declared: true, State: "unreported"},
					},
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("resolved processing = %#v, want %#v", got, want)
				}
			} else {
				got, err := ListArtifacts(context.Background(), env, source.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := []ArtifactView{{Name: "original", Artifact: "binary", ContentType: "application/pdf", State: "unreported"}}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("resolved artifacts = %#v, want %#v", got, want)
				}
			}
			wantCalls := []apiCall{{method: "ListSources", limit: 100}, {method: "GetSource", id: source.Id}}
			if handler == "artifacts" {
				wantCalls = append([]apiCall{{method: "Capabilities"}}, wantCalls...)
			}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Errorf("name resolution calls = %#v, want %#v", api.calls, wantCalls)
			}
		})
	}
}
