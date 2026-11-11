// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type manifestWrite struct {
	source      management.Source
	conditional bool
	absent      bool
	revision    string
}

// Embed the package fake to preserve its unexpected-call checks without editing
// existing tests. Reads and writes copy declarations, as an HTTP API would.
type manifestAPI struct {
	*fakeAPI
	caps        management.Capabilities
	sources     map[string]management.Source
	gets        map[string]int
	writes      []manifestWrite
	removes     int
	beforeWrite func(string)
	putFailures map[string]error
	// revisionFor models a server whose revision encoding the client cannot recompute.
	revisionFor func(management.Source) string
}

func (f *manifestAPI) revision(s management.Source) string {
	if f.revisionFor != nil {
		return f.revisionFor(s)
	}
	return management.SourceRevision(s)
}

var _ API = (*manifestAPI)(nil)

func newManifestAPI(t *testing.T, sources ...management.Source) *manifestAPI {
	t.Helper()
	f := &manifestAPI{
		fakeAPI: &fakeAPI{t: t},
		caps: management.Capabilities{Runtime: "standalone", Version: 1,
			ConfigureSources: true, RevisionPreconditions: true, SearchVersions: []int{1, 2}},
		sources: make(map[string]management.Source), gets: make(map[string]int),
		putFailures: make(map[string]error),
	}
	for _, s := range sources {
		f.sources[s.Id] = f.copySource(s)
	}
	return f
}

func (f *manifestAPI) copySource(s management.Source) management.Source {
	f.t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		f.t.Fatal(err)
	}
	var copy management.Source
	if err := json.Unmarshal(raw, &copy); err != nil {
		f.t.Fatal(err)
	}
	return copy
}

func (f *manifestAPI) Capabilities(context.Context) (management.Capabilities, error) {
	return f.caps, nil
}

func (f *manifestAPI) GetSource(_ context.Context, id string) (management.Source, error) {
	f.gets[id]++
	s, ok := f.sources[id]
	if !ok {
		return management.Source{}, management.Failure(http.StatusNotFound, "source_not_found")
	}
	s = f.copySource(s)
	s.Revision = f.revision(s)
	return s, nil
}

func (f *manifestAPI) PutSource(_ context.Context, s management.Source) (management.Source, error) {
	if f.beforeWrite != nil {
		f.beforeWrite(s.Id)
	}
	f.writes = append(f.writes, manifestWrite{source: f.copySource(s)})
	return f.store(s)
}

func (f *manifestAPI) PutSourceIfAbsent(_ context.Context, s management.Source) (management.Source, error) {
	if f.beforeWrite != nil {
		f.beforeWrite(s.Id)
	}
	f.writes = append(f.writes, manifestWrite{source: f.copySource(s), absent: true})
	if _, exists := f.sources[s.Id]; exists {
		return management.Source{}, management.Failure(http.StatusPreconditionFailed, "source_already_exists")
	}
	return f.store(s)
}

func (f *manifestAPI) PutSourceIfMatch(_ context.Context, s management.Source, revision string) (management.Source, error) {
	if f.beforeWrite != nil {
		f.beforeWrite(s.Id)
	}
	f.writes = append(f.writes, manifestWrite{source: f.copySource(s), conditional: true, revision: revision})
	current, ok := f.sources[s.Id]
	if !ok {
		return management.Source{}, management.Failure(http.StatusNotFound, "source_not_found")
	}
	if f.revision(current) != revision {
		return management.Source{}, management.Failure(http.StatusPreconditionFailed, "source_revision_mismatch")
	}
	return f.store(s)
}

func (f *manifestAPI) store(s management.Source) (management.Source, error) {
	if err := f.putFailures[s.Id]; err != nil {
		delete(f.putFailures, s.Id)
		return management.Source{}, err
	}
	if s.PermissionTtlSeconds == 0 {
		s.PermissionTtlSeconds = 3600
	}
	if s.Publication == "" {
		s.Publication = "native"
	}
	s.Revision = management.SourceRevision(s)
	f.sources[s.Id] = f.copySource(s)
	return f.copySource(s), nil
}

func (f *manifestAPI) RemoveSource(context.Context, string, bool) (management.Mutation, error) {
	f.removes++
	return management.Mutation{}, nil
}

func manifestEntry(key string) ManifestSource {
	return ManifestSource{Key: key, Connection: "docs-connection", Table: "docs",
		Config:      map[string]any{"path": "/docs"},
		Permissions: &management.Permissions{Readers: []string{"team"}}}
}

func manifestFile(entries ...ManifestSource) Manifest {
	return Manifest{Version: 1, Sources: entries}
}

func manifestCurrent(key string) management.Source {
	return management.Source{Id: key, Name: key, Connection: "docs-connection", Table: "docs",
		Config: map[string]any{"path": "/docs"}, Publication: "native", PermissionTtlSeconds: 3600,
		Permissions: management.Permissions{Readers: []string{"team"}}}
}

func planWithoutWrites(t *testing.T, api *manifestAPI, m Manifest, opts PlanOptions) Plan {
	t.Helper()
	t.Cleanup(func() {
		if len(api.writes) != 0 || api.removes != 0 {
			t.Errorf("plan mutated sources: writes=%#v removes=%d", api.writes, api.removes)
		}
	})
	plan, err := PlanManifest(context.Background(), Env{API: api, Out: io.Discard}, m, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != len(m.Sources) {
		t.Fatalf("plan items = %#v, want %d", plan.Items, len(m.Sources))
	}
	return plan
}

func TestPlanManifestCreate(t *testing.T) {
	api := newManifestAPI(t)
	entry := manifestEntry("engineering-docs")
	entry.Permissions = nil
	defaults := management.Permissions{Readers: []string{"instance-members"}}
	plan := planWithoutWrites(t, api, manifestFile(entry), PlanOptions{
		Identity: func(key string) (string, error) { return "instance-" + key, nil },
		ResolveConnection: func(_ context.Context, ref string) (string, error) {
			if ref != entry.Connection {
				t.Fatalf("connection ref = %q", ref)
			}
			return "resolved-connection", nil
		},
		DefaultPermissions: func(context.Context) (management.Permissions, error) { return defaults, nil },
	})
	item := plan.Items[0]
	if item.Key != entry.Key || item.SourceID != "instance-engineering-docs" || item.Action != PlanCreate || item.Revision != "" || len(item.Changes) != 0 {
		t.Fatalf("create item = %#v", item)
	}
	want := management.Source{Id: item.SourceID, Name: entry.Key, Connection: "resolved-connection", Table: entry.Table,
		Config: entry.Config, Publication: "native", Permissions: defaults}
	if !reflect.DeepEqual(item.Desired, want) {
		t.Fatalf("desired = %#v, want %#v", item.Desired, want)
	}
}

func TestPlanManifestMissingCreatePermissions(t *testing.T) {
	api := newManifestAPI(t)
	entry := manifestEntry("docs")
	entry.Permissions = nil
	plan, err := PlanManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
	failure := assertCode(t, err, CodeInvalid)
	if !strings.Contains(failure.Message, "permissions are required to create a source") || len(plan.Items) != 0 || len(api.writes) != 0 {
		t.Fatalf("plan=%#v error=%q writes=%#v", plan, failure.Message, api.writes)
	}
}

func TestPlanManifestUnchangedCarriesServerDefaults(t *testing.T) {
	current := manifestCurrent("docs")
	current.State = "ready"
	current.Rekeys = true
	current.SearchStatus = &management.SearchStatus{}
	current.KnowledgeStatus = &management.KnowledgeStatus{}
	current.Revision = "ignored-response-metadata"
	api := newManifestAPI(t, current)
	entry := manifestEntry("docs")
	entry.Permissions = nil
	plan := planWithoutWrites(t, api, manifestFile(entry), PlanOptions{
		DefaultPermissions: func(context.Context) (management.Permissions, error) {
			t.Fatal("default permissions must not be used on update")
			return management.Permissions{}, nil
		},
	})
	item := plan.Items[0]
	if item.Action != PlanUnchanged || len(item.Changes) != 0 || item.Revision != management.SourceRevision(current) || item.Desired.PermissionTtlSeconds != 3600 || !reflect.DeepEqual(item.Desired.Permissions, current.Permissions) {
		t.Fatalf("unchanged item = %#v", item)
	}
	if item.Desired.State != "" || item.Desired.SearchStatus != nil || item.Desired.KnowledgeStatus != nil || item.Desired.Rekeys || item.Desired.Revision != "" {
		t.Fatalf("desired contains response-only fields: %#v", item.Desired)
	}
}

func TestPlanManifestSortedChangesAndCompleteReplacement(t *testing.T) {
	current := manifestCurrent("docs")
	current.Streams = []management.SearchStream{{Name: "text", Field: "body"}}
	current.Artifacts = []management.ArtifactBinding{{Name: "original", Artifact: "original", ContentType: "application/pdf"}}
	current.Processing = &management.ProcessingDeclaration{}
	current.Identity = management.SourceIdentity{Namespace: "stable-namespace"}
	current.Knowledge = &management.KnowledgeDeclaration{Audience: "team", Version: 1}
	api := newManifestAPI(t, current)
	entry := manifestEntry("docs")
	entry.Name = "Renamed"
	entry.Connection = "another-connection"
	entry.Config = map[string]any{"path": "/other"}
	plan := planWithoutWrites(t, api, manifestFile(entry), PlanOptions{})
	item := plan.Items[0]
	if item.Action != PlanUpdate || item.Revision != management.SourceRevision(current) || !reflect.DeepEqual(item.Changes, []string{"config", "connection", "name"}) {
		t.Fatalf("update item = %#v", item)
	}
	want := current
	want.Name, want.Connection, want.Config = entry.Name, entry.Connection, entry.Config
	if !reflect.DeepEqual(item.Desired, want) {
		t.Fatalf("full desired declaration = %#v, want %#v", item.Desired, want)
	}
}

func TestPlanManifestStreamsPresence(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("present=%t", present), func(t *testing.T) {
			current := manifestCurrent("docs")
			current.Streams = []management.SearchStream{{Name: "text", Field: "body"}}
			api := newManifestAPI(t, current)
			entry := manifestEntry("docs")
			if present {
				empty := []management.SearchStream{}
				entry.Streams = &empty
			}
			item := planWithoutWrites(t, api, manifestFile(entry), PlanOptions{}).Items[0]
			if present {
				if item.Action != PlanUpdate || len(item.Desired.Streams) != 0 || !reflect.DeepEqual(item.Changes, []string{"streams"}) {
					t.Fatalf("explicit empty streams did not clear: %#v", item)
				}
			} else if item.Action != PlanUnchanged || !reflect.DeepEqual(item.Desired.Streams, current.Streams) || len(item.Changes) != 0 {
				t.Fatalf("absent streams were not retained: %#v", item)
			}
		})
	}
}

func TestPlanManifestImmutableTableAbortsWholePlan(t *testing.T) {
	api := newManifestAPI(t, manifestCurrent("existing"))
	entry := manifestEntry("existing")
	entry.Table = "elsewhere"
	var out bytes.Buffer
	plan, err := PlanManifest(context.Background(), Env{API: api, Out: &out}, manifestFile(manifestEntry("new"), entry), PlanOptions{})
	failure := assertCode(t, err, CodeInvalid)
	if !strings.Contains(failure.Message, `destination table is immutable (current "docs")`) || len(plan.Items) != 0 || len(api.writes) != 0 || out.Len() != 0 {
		t.Fatalf("plan=%#v error=%q writes=%#v output=%q", plan, failure.Message, api.writes, &out)
	}
}

func TestPlanManifestCapabilities(t *testing.T) {
	for _, feature := range []string{"configure_sources", "search_version:2", "extension"} {
		t.Run(feature, func(t *testing.T) {
			api := newManifestAPI(t)
			entry := manifestEntry("docs")
			switch feature {
			case "configure_sources":
				api.caps.ConfigureSources = false
			case "search_version:2":
				api.caps.SearchVersions = []int{1}
				version := 2
				entry.SearchVersion = &version
			case "extension":
				entry.Extensions = map[string]json.RawMessage{"cloud": json.RawMessage(`{"enabled":true}`)}
			}
			_, err := PlanManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
			failure := assertCode(t, err, CodeUnsupported)
			if feature == "extension" && !strings.Contains(failure.Message, `manifest extension "cloud"`) {
				t.Fatalf("extension error = %q", failure.Message)
			}
			if len(api.writes) != 0 || api.removes != 0 {
				t.Fatalf("unsupported plan mutated sources: %#v", api)
			}
		})
	}
}

func TestPlanManifestOutput(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := newManifestAPI(t, manifestCurrent("changed"), manifestCurrent("same"))
			changed := manifestEntry("changed")
			changed.Name = "Changed name"
			var out bytes.Buffer
			plan, err := PlanManifest(context.Background(), Env{API: api, Out: &out, JSON: machine}, manifestFile(manifestEntry("new"), changed, manifestEntry("same")), PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Items) != 3 {
				t.Fatalf("items = %#v", plan.Items)
			}
			if machine {
				var decoded struct {
					Items []map[string]json.RawMessage `json:"items"`
				}
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if len(decoded.Items) != 3 {
					t.Fatalf("JSON items = %#v", decoded.Items)
				}
				for i, action := range []PlanAction{PlanCreate, PlanUpdate, PlanUnchanged} {
					var got PlanItem
					raw, err := json.Marshal(decoded.Items[i])
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(raw, &got); err != nil {
						t.Fatal(err)
					}
					if got.Action != action || got.Key != plan.Items[i].Key || got.SourceID != plan.Items[i].SourceID {
						t.Fatalf("JSON item %d = %#v", i, got)
					}
					if _, ok := decoded.Items[i]["desired"]; ok {
						t.Fatal("plan JSON exposed desired declaration")
					}
				}
			} else {
				lines := strings.Split(strings.TrimSpace(out.String()), "\n")
				want := [][]string{{"ACTION", "KEY", "SOURCE_ID", "CHANGES"}, {"create", "new", "new", "-"}, {"update", "changed", "changed", "name"}, {"unchanged", "same", "same", "-"}}
				if len(lines) != 6 {
					t.Fatalf("human output = %q", out.String())
				}
				for i := range want {
					if !reflect.DeepEqual(strings.Fields(lines[i]), want[i]) {
						t.Errorf("row %d = %q, want %q", i, lines[i], want[i])
					}
				}
				if lines[4] != "Plan: 1 to create, 1 to update, 1 unchanged." || lines[5] != "Sources absent from the file are left untouched; use archive or purge to retire them." {
					t.Fatalf("human summary = %q", lines[4:])
				}
			}
			if len(api.writes) != 0 || api.removes != 0 {
				t.Fatal("plan mutated sources")
			}
		})
	}
}

func TestPlanManifestEmptyJSONItems(t *testing.T) {
	var out bytes.Buffer
	_, err := PlanManifest(context.Background(), Env{API: newManifestAPI(t), Out: &out, JSON: true}, manifestFile(), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded["items"]) != "[]" {
		t.Fatalf("empty plan JSON = %q, want items []", out.String())
	}
}

func requireApplyOutcomes(t *testing.T, results []ApplyResult, keys, outcomes []string) {
	t.Helper()
	if len(results) != len(keys) {
		t.Fatalf("results = %#v, want %d entries", results, len(keys))
	}
	for i, key := range keys {
		if results[i].Key != key || results[i].SourceID != key || results[i].Outcome != outcomes[i] {
			t.Errorf("result %d = %#v, want key/id=%s outcome=%s", i, results[i], key, outcomes[i])
		}
	}
}

func TestApplyManifestIdempotentAndLeavesAbsentSources(t *testing.T) {
	unmanaged := manifestCurrent("unmanaged")
	api := newManifestAPI(t, manifestCurrent("existing"), unmanaged)
	changed := manifestEntry("existing")
	changed.Name = "Updated name"
	m := manifestFile(manifestEntry("new"), changed)
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, m, PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"new", "existing"}, []string{"created", "updated"})
	if len(api.writes) != 2 || api.sources["existing"].Name != "Updated name" || api.sources["new"].Name != "new" {
		t.Fatalf("first apply writes=%#v sources=%#v", api.writes, api.sources)
	}
	if !api.writes[0].absent || api.writes[0].conditional || api.writes[1].absent || !api.writes[1].conditional || api.writes[1].revision != management.SourceRevision(manifestCurrent("existing")) {
		t.Errorf("create/update did not use planned preconditions: %#v", api.writes)
	}
	if api.gets["new"] != 1 || api.gets["existing"] != 1 {
		t.Errorf("apply re-read sources after planning: gets=%#v", api.gets)
	}
	for _, result := range results {
		if result.Revision != management.SourceRevision(api.sources[result.SourceID]) || result.Revision == "" {
			t.Errorf("successful write revision = %#v", result)
		}
	}
	api.writes = nil
	api.gets = make(map[string]int)
	results, err = ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, m, PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"new", "existing"}, []string{"unchanged", "unchanged"})
	if len(api.writes) != 0 || api.removes != 0 || !reflect.DeepEqual(api.sources["unmanaged"], unmanaged) {
		t.Fatalf("second apply mutated sources: writes=%#v removes=%d sources=%#v", api.writes, api.removes, api.sources)
	}
	if api.gets["new"] != 1 || api.gets["existing"] != 1 {
		t.Fatalf("unchanged items made requests after planning: gets=%#v", api.gets)
	}
}

func TestApplyManifestRevisionPreconditions(t *testing.T) {
	current := manifestCurrent("docs")
	api := newManifestAPI(t, current)
	api.caps.RevisionPreconditions = true
	entry := manifestEntry("docs")
	entry.Name = "Updated"
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"updated"})
	if len(api.writes) != 1 {
		t.Fatalf("writes = %#v", api.writes)
	}
	write := api.writes[0]
	if !write.conditional || write.revision != management.SourceRevision(current) || api.gets["docs"] != 1 || api.sources["docs"].Name != "Updated" {
		t.Fatalf("conditional write=%#v gets=%#v stored=%#v", write, api.gets, api.sources["docs"])
	}
}

func TestApplyManifestUpdateRace(t *testing.T) {
	api := newManifestAPI(t, manifestCurrent("docs"))
	concurrent := manifestCurrent("docs")
	concurrent.Name = "Concurrent edit"
	api.beforeWrite = func(id string) {
		api.sources[id] = api.copySource(concurrent)
	}
	entry := manifestEntry("docs")
	entry.Name = "Our edit"
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
	failure := assertCode(t, err, CodeConflict)
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"conflict"})
	if failure.ExitCode() != 5 || len(api.writes) != 1 || !api.writes[0].conditional || api.writes[0].revision != management.SourceRevision(manifestCurrent("docs")) || api.gets["docs"] != 1 || !reflect.DeepEqual(api.sources["docs"], concurrent) {
		t.Fatalf("error=%#v writes=%#v gets=%#v stored=%#v", failure, api.writes, api.gets, api.sources["docs"])
	}
}

func TestApplyManifestServerConflicts(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusPreconditionFailed} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			api := newManifestAPI(t, manifestCurrent("docs"))
			api.caps.RevisionPreconditions = true
			api.putFailures["docs"] = management.Failure(status, "source_revision_mismatch")
			entry := manifestEntry("docs")
			entry.Name = "Updated"
			results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry, manifestEntry("later")), PlanOptions{})
			assertCode(t, err, CodeConflict)
			requireApplyOutcomes(t, results, []string{"docs", "later"}, []string{"conflict", "created"})
			if results[0].Error == "" || api.sources["docs"].Name != "docs" || api.sources["later"].Name != "later" {
				t.Fatalf("conflict results=%#v sources=%#v", results, api.sources)
			}
		})
	}
}

func TestApplyManifestPartialFailureAndRerunConverges(t *testing.T) {
	api := newManifestAPI(t)
	api.putFailures["second"] = management.Failure(http.StatusInternalServerError, "acquisition_failed")
	m := manifestFile(manifestEntry("first"), manifestEntry("second"), manifestEntry("third"))
	var out bytes.Buffer
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: &out}, m, PlanOptions{})
	failure := assertCode(t, err, CodeFailed)
	requireApplyOutcomes(t, results, []string{"first", "second", "third"}, []string{"created", "failed", "created"})
	wantMessage := Classify(management.Failure(http.StatusInternalServerError, "acquisition_failed")).Error()
	if results[1].Error != wantMessage || failure.Message != "1 of 3 sources failed to apply; rerunning apply is safe" || len(api.writes) != 3 {
		t.Fatalf("failure=%#v results=%#v writes=%#v", failure, results, api.writes)
	}
	if !strings.Contains(out.String(), "Applied: 2 created, 0 updated, 0 unchanged, 0 conflicts, 1 failed.") || !strings.Contains(out.String(), wantMessage) {
		t.Fatalf("partial failure human output = %q", out.String())
	}
	api.writes = nil
	results, err = ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, m, PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"first", "second", "third"}, []string{"unchanged", "created", "unchanged"})
	if len(api.writes) != 1 || api.writes[0].source.Id != "second" || api.sources["second"].Name != "second" {
		t.Fatalf("rerun failed to converge: writes=%#v sources=%#v", api.writes, api.sources)
	}
}

func TestApplyManifestCreateRace(t *testing.T) {
	api := newManifestAPI(t)
	concurrent := manifestCurrent("docs")
	concurrent.Name = "Racing creator"
	api.beforeWrite = func(id string) {
		api.sources[id] = api.copySource(concurrent)
	}
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(manifestEntry("docs")), PlanOptions{})
	failure := assertCode(t, err, CodeConflict)
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"conflict"})
	if failure.ExitCode() != 5 || len(api.writes) != 1 || !api.writes[0].absent || api.gets["docs"] != 1 || !reflect.DeepEqual(api.sources["docs"], concurrent) {
		t.Fatalf("create race error=%#v writes=%#v gets=%#v sources=%#v", failure, api.writes, api.gets, api.sources)
	}
}

func TestApplyManifestRequiresRevisionPreconditions(t *testing.T) {
	for _, action := range []PlanAction{PlanCreate, PlanUpdate} {
		t.Run(string(action), func(t *testing.T) {
			api := newManifestAPI(t, manifestCurrent("same"), manifestCurrent("existing"))
			api.caps.RevisionPreconditions = false
			entry := manifestEntry("new")
			if action == PlanUpdate {
				entry = manifestEntry("existing")
				entry.Name = "Updated"
			}
			var out bytes.Buffer
			results, err := ApplyManifest(context.Background(), Env{API: api, Out: &out}, manifestFile(manifestEntry("same"), entry), PlanOptions{})
			failure := assertCode(t, err, CodeUnsupported)
			if failure.ExitCode() != 6 || !strings.Contains(failure.Message, "revision_preconditions") || !strings.Contains(failure.Message, api.caps.Runtime) {
				t.Errorf("unsupported error = %#v (exit %d)", failure, failure.ExitCode())
			}
			if results != nil || len(api.writes) != 0 || api.removes != 0 || out.Len() != 0 {
				t.Errorf("unsupported apply returned results=%#v writes=%#v removes=%d output=%q", results, api.writes, api.removes, out.String())
			}
		})
	}
}

func TestApplyManifestUnchangedWithoutRevisionPreconditions(t *testing.T) {
	current := manifestCurrent("docs")
	api := newManifestAPI(t, current)
	api.caps.RevisionPreconditions = false
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(manifestEntry("docs")), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"unchanged"})
	if len(api.writes) != 0 || api.removes != 0 || api.gets["docs"] != 1 || !reflect.DeepEqual(api.sources["docs"], current) {
		t.Fatalf("unchanged apply mutated or re-read source: writes=%#v gets=%#v sources=%#v", api.writes, api.gets, api.sources)
	}
}

func TestPlanManifestWithoutRevisionPreconditions(t *testing.T) {
	api := newManifestAPI(t, manifestCurrent("existing"))
	api.caps.RevisionPreconditions = false
	entry := manifestEntry("existing")
	entry.Name = "Updated"
	plan := planWithoutWrites(t, api, manifestFile(manifestEntry("new"), entry), PlanOptions{})
	if plan.Items[0].Action != PlanCreate || plan.Items[1].Action != PlanUpdate {
		t.Fatalf("read-only plan = %#v, want create and update", plan.Items)
	}
}

func TestApplyManifestPlanErrorWritesNothing(t *testing.T) {
	api := newManifestAPI(t, manifestCurrent("existing"))
	bad := manifestEntry("existing")
	bad.Table = "changed-table"
	var out bytes.Buffer
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: &out, JSON: true}, manifestFile(manifestEntry("new"), bad), PlanOptions{})
	assertCode(t, err, CodeInvalid)
	if len(results) != 0 || len(api.writes) != 0 || api.removes != 0 || out.Len() != 0 {
		t.Fatalf("plan error returned results=%#v writes=%#v removes=%d output=%q", results, api.writes, api.removes, out.String())
	}
}

func TestApplyManifestOutput(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			api := newManifestAPI(t, manifestCurrent("changed"), manifestCurrent("same"))
			changed := manifestEntry("changed")
			changed.Name = "Updated"
			var out bytes.Buffer
			results, err := ApplyManifest(context.Background(), Env{API: api, Out: &out, JSON: machine}, manifestFile(manifestEntry("new"), changed, manifestEntry("same")), PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			requireApplyOutcomes(t, results, []string{"new", "changed", "same"}, []string{"created", "updated", "unchanged"})
			if machine {
				var decoded struct {
					Results []ApplyResult `json:"results"`
				}
				// Unmarshal also rejects a plan document followed by an apply document.
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decoded.Results, results) {
					t.Fatalf("JSON results = %#v, want %#v", decoded.Results, results)
				}
				for i, action := range []PlanAction{PlanCreate, PlanUpdate, PlanUnchanged} {
					if decoded.Results[i].Action != action {
						t.Errorf("result %d action = %q, want %q", i, decoded.Results[i].Action, action)
					}
				}
			} else {
				lines := strings.Split(strings.TrimSpace(out.String()), "\n")
				want := [][]string{{"created", "new", "new"}, {"updated", "changed", "changed"}, {"unchanged", "same", "same"}}
				if len(lines) != 4 {
					t.Fatalf("human apply output = %q", out.String())
				}
				for i := range want {
					if !reflect.DeepEqual(strings.Fields(lines[i]), want[i]) {
						t.Errorf("row %d = %q, want %q", i, lines[i], want[i])
					}
				}
				if lines[3] != "Applied: 1 created, 1 updated, 1 unchanged, 0 conflicts, 0 failed." {
					t.Fatalf("human apply summary = %q", lines[3])
				}
			}
		})
	}
}

// The server's revision is authoritative: its config encoding (for example
// JSON numbers) may differ from what the client would recompute.
func TestApplyManifestUsesServerRevision(t *testing.T) {
	api := newManifestAPI(t, manifestCurrent("docs"))
	api.revisionFor = func(s management.Source) string { return "server-" + management.SourceRevision(s) }
	entry := manifestEntry("docs")
	entry.Name = "Updated"
	results, err := ApplyManifest(context.Background(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
	if err != nil {
		t.Fatalf("apply: %v (results %#v)", err, results)
	}
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"updated"})
	if len(api.writes) != 1 || !api.writes[0].conditional || api.writes[0].revision != api.revision(manifestCurrent("docs")) {
		t.Fatalf("precondition writes = %#v", api.writes)
	}
}
