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
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Capabilities prints the runtime's advertised management features.
func Capabilities(ctx context.Context, env Env) (management.Capabilities, error) {
	c, err := env.API.Capabilities(ctx)
	if err != nil {
		return c, Classify(err)
	}
	if env.JSON {
		return c, Classify(env.writeJSON(c))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "FEATURE\tVALUE\nruntime\t%s\nversion\t%d\n", c.Runtime, c.Version)
	for _, feature := range []struct {
		name  string
		value bool
	}{
		{"per_source_runs", c.PerSourceRuns},
		{"cancel_runs", c.CancelRuns},
		{"configure_sources", c.ConfigureSources},
		{"purge_sources", c.PurgeSources},
		{"processing_jobs", c.ProcessingJobs},
		{"processing_presets", c.ProcessingPresets},
		{"artifact_discovery", c.ArtifactDiscovery},
		{"media_proxy", c.MediaProxy},
		{"serialized_runs", c.SerializedRuns},
	} {
		fmt.Fprintf(&out, "%s\t%t\n", feature.name, feature.value)
	}
	return c, Classify(env.writeHuman(out.String()))
}

// ListOptions controls page size, starting cursor, and traversal of all pages.
type ListOptions struct {
	Limit  int
	Cursor string
	All    bool
}

// ListSources prints one page or a bounded traversal of all source pages.
func ListSources(ctx context.Context, env Env, opts ListOptions) (management.SourcePage, error) {
	page, err := listSources(ctx, env.API, opts)
	if err != nil {
		return page, Classify(err)
	}
	if env.JSON {
		return page, Classify(env.writeJSON(page))
	}
	var out strings.Builder
	if len(page.Data) == 0 {
		out.WriteString("No sources.\n")
	} else {
		out.WriteString("ID\tNAME\tTABLE\tSTATE\tPUBLICATION\n")
		for _, s := range page.Data {
			fmt.Fprintf(&out, "%s\t%s\t%s\t%s\t%s\n", s.Id, s.Name, s.Table, s.State, s.Publication)
		}
	}
	if page.NextCursor != "" {
		fmt.Fprintf(&out, "Next cursor: %s\n", page.NextCursor)
	}
	return page, Classify(env.writeHuman(out.String()))
}

// listSources fetches without printing, so reference resolution stays silent.
func listSources(ctx context.Context, api API, opts ListOptions) (management.SourcePage, error) {
	data, cursor, err := listPages(ctx, opts, "source", func(ctx context.Context, limit int, cursor string) ([]management.Source, string, error) {
		page, err := api.ListSources(ctx, limit, cursor)
		return page.Data, page.NextCursor, err
	})
	return management.SourcePage{Data: data, NextCursor: cursor}, err
}

// listPages normalizes empty pages and bounds traversal, including empty pages
// that carry a next cursor.
func listPages[T any](ctx context.Context, opts ListOptions, resource string, fetch func(context.Context, int, string) ([]T, string, error)) ([]T, string, error) {
	if opts.Limit == 0 {
		opts.Limit = 20
	}
	if opts.Limit < 1 || opts.Limit > 100 {
		return nil, "", &Error{Code: CodeInvalid, Message: resource + " limit must be between 1 and 100"}
	}
	if !opts.All {
		data, cursor, err := fetch(ctx, opts.Limit, opts.Cursor)
		if data == nil {
			data = []T{}
		}
		return data, cursor, err
	}
	all := []T{}
	seen := map[string]bool{opts.Cursor: true}
	cursor := opts.Cursor
	for range 1000 {
		data, next, err := fetch(ctx, opts.Limit, cursor)
		if err != nil {
			return nil, "", err
		}
		all = append(all, data...)
		if next == "" {
			return all, "", nil
		}
		if seen[next] {
			return nil, "", &Error{Code: CodeFailed, Message: "source-management returned a repeated cursor"}
		}
		seen[next] = true
		cursor = next
	}
	return nil, "", &Error{Code: CodeFailed, Message: "source-management exceeded the 1000-page limit"}
}

// ResolveSource resolves an ID first, then an exact, unambiguous source name.
// A name match is fetched again because list rows can be abbreviated.
func ResolveSource(ctx context.Context, env Env, ref string) (management.Source, error) {
	if ref == "" {
		return management.Source{}, &Error{Code: CodeInvalid, Message: "source reference must not be empty"}
	}
	if sourceID(ref) {
		source, err := env.API.GetSource(ctx, ref)
		if err == nil {
			return source, nil
		}
		err = Classify(err)
		var failure *Error
		if !errors.As(err, &failure) || (failure.HTTPStatus != http.StatusNotFound && failure.HTTPStatus != http.StatusBadRequest) {
			return management.Source{}, err
		}
	}
	page, err := listSources(ctx, env.API, ListOptions{Limit: 100, All: true})
	if err != nil {
		return management.Source{}, Classify(err)
	}
	var matches []string
	for _, source := range page.Data {
		if source.Name == ref {
			matches = append(matches, source.Id)
		}
	}
	switch len(matches) {
	case 0:
		return management.Source{}, &Error{Code: CodeNotFound, Message: fmt.Sprintf("source %q not found", ref)}
	case 1:
		source, err := env.API.GetSource(ctx, matches[0])
		return source, Classify(err)
	default:
		return management.Source{}, &Error{Code: CodeInvalid, Message: fmt.Sprintf("source %q matches IDs %s; use an ID", ref, strings.Join(matches, ", "))}
	}
}

func sourceID(ref string) bool {
	if len(ref) == 0 || len(ref) > 128 {
		return false
	}
	for _, c := range ref {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// ReadinessSummary counts search representations by their reported states.
type ReadinessSummary struct {
	Reported    bool           `json:"reported"`
	Consistency string         `json:"consistency,omitempty"`
	ObservedAt  int            `json:"observed_at,omitempty"`
	Streams     map[string]int `json:"streams,omitempty"`
	Artifacts   map[string]int `json:"artifacts,omitempty"`
}

// StatusView keeps acquisition progress separate from reported search readiness.
type StatusView struct {
	Source    management.Source `json:"source"`
	LatestRun *management.Run   `json:"latest_run,omitempty"`
	Readiness ReadinessSummary  `json:"readiness"`
}

// Status prints source configuration, latest acquisition, and search readiness.
func Status(ctx context.Context, env Env, ref string) (StatusView, error) {
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return StatusView{}, err
	}
	view := StatusView{Source: source}
	runs, err := env.API.ListRuns(ctx, source.Id, 1, "")
	if err != nil {
		err = Classify(err)
		var failure *Error
		if !errors.As(err, &failure) || (failure.Code != CodeUnsupported && failure.Code != CodeNotFound) {
			return StatusView{}, err
		}
	} else if len(runs.Data) > 0 {
		view.LatestRun = &runs.Data[0]
	}
	if status := source.SearchStatus; status != nil {
		view.Readiness = ReadinessSummary{
			Reported:    true,
			Consistency: string(status.Consistency),
			ObservedAt:  status.ObservedAt,
			Streams:     countStates(status.Streams),
			Artifacts:   countStates(status.Artifacts),
		}
	}
	if env.JSON {
		return view, Classify(env.writeJSON(view))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Source:\t%s (%s)\nTable:\t%s\nConnection:\t%s\nState:\t%s\nPublication:\t%s\n",
		source.Name, source.Id, source.Table, source.Connection, source.State, source.Publication)
	if source.ReconciliationStatus != nil {
		fmt.Fprintf(&out, "Configuration reconciliation:\t%s\n", source.ReconciliationStatus.State)
	}
	if run := view.LatestRun; run != nil {
		fmt.Fprintf(&out, "Acquisition:\t%s/%s (run %s)\n", run.State, run.Outcome, run.Id)
	} else {
		out.WriteString("Acquisition:\tno runs yet\n")
	}
	if view.Readiness.Reported {
		fmt.Fprintf(&out, "Search:\t%s; streams: %s; artifacts: %s\n",
			view.Readiness.Consistency, formatCounts(view.Readiness.Streams), formatCounts(view.Readiness.Artifacts))
	} else {
		out.WriteString("Search:\tnot reported by this runtime\n")
	}
	return view, Classify(env.writeHuman(out.String()))
}

func countStates(items []management.RepresentationStatus) map[string]int {
	counts := make(map[string]int)
	for _, item := range items {
		counts[string(item.State)]++
	}
	return counts
}

// formatCounts renders "state=n" pairs sorted by state, or "none" when empty.
func formatCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return "none"
	}
	pairs := make([]string, 0, len(counts))
	for _, state := range slices.Sorted(maps.Keys(counts)) {
		pairs = append(pairs, fmt.Sprintf("%s=%d", state, counts[state]))
	}
	return strings.Join(pairs, " ")
}
