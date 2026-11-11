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
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// RepresentationView joins a declared stream or artifact with reported readiness.
type RepresentationView struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Declared bool   `json:"declared"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
}

// ProcessingView keeps declared processing and reported readiness separate.
type ProcessingView struct {
	SourceID        string               `json:"source_id"`
	SearchVersion   int                  `json:"search_version"`
	Reported        bool                 `json:"reported"`
	Consistency     string               `json:"consistency,omitempty"`
	ObservedAt      int                  `json:"observed_at,omitempty"`
	Indexes         []string             `json:"indexes"`
	Representations []RepresentationView `json:"representations"`
}

// ProcessingStatus prints declared processing and runtime-reported readiness.
func ProcessingStatus(ctx context.Context, env Env, ref string) (ProcessingView, error) {
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return ProcessingView{}, err
	}
	view := ProcessingView{
		SourceID: source.Id, SearchVersion: source.SearchVersion,
		Indexes:         []string{},
		Representations: make([]RepresentationView, 0, len(source.Streams)+len(source.Artifacts)),
	}
	if source.Processing != nil {
		view.Indexes = make([]string, 0, len(source.Processing.Indexes))
		for _, index := range source.Processing.Indexes {
			view.Indexes = append(view.Indexes, index.Name)
		}
	}
	var streams, artifacts []management.RepresentationStatus
	if status := source.SearchStatus; status != nil {
		view.Reported = true
		view.Consistency = string(status.Consistency)
		view.ObservedAt = status.ObservedAt
		streams, artifacts = status.Streams, status.Artifacts
	}
	streamNames := make([]string, 0, len(source.Streams))
	for _, stream := range source.Streams {
		streamNames = append(streamNames, stream.Name)
	}
	artifactNames := make([]string, 0, len(source.Artifacts))
	for _, artifact := range source.Artifacts {
		artifactNames = append(artifactNames, artifact.Name)
	}
	view.Representations = append(view.Representations, representations("stream", streamNames, streams)...)
	view.Representations = append(view.Representations, representations("artifact", artifactNames, artifacts)...)
	if env.JSON {
		return view, Classify(env.writeJSON(view))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Source:\t%s (%s)\nSearch:\t", source.Name, source.Id)
	switch {
	case view.Reported:
		fmt.Fprintf(&out, "search_version %d; %s; observed %s\n",
			view.SearchVersion, view.Consistency, time.Unix(int64(view.ObservedAt), 0).UTC().Format(time.RFC3339))
	case view.SearchVersion < 2:
		version := view.SearchVersion
		if version == 0 {
			version = 1
		}
		fmt.Fprintf(&out, "readiness reporting requires search_version 2 (this source uses %d)\n", version)
	default:
		out.WriteString("not reported by this runtime\n")
	}
	indexes := "none"
	if len(view.Indexes) > 0 {
		indexes = strings.Join(view.Indexes, ", ")
	}
	fmt.Fprintf(&out, "Indexes:\t%s\n", indexes)
	if len(view.Representations) == 0 {
		out.WriteString("No streams or artifacts declared.\n")
	} else {
		out.WriteString("KIND\tNAME\tDECLARED\tSTATE\tREASON\n")
		for _, item := range view.Representations {
			fmt.Fprintf(&out, "%s\t%s\t%t\t%s\t%s\n", item.Kind, item.Name, item.Declared, item.State, item.Reason)
		}
	}
	out.WriteString("Acquisition runs are tracked separately: see run history.\n")
	return view, Classify(env.writeHuman(out.String()))
}

// ArtifactView describes an artifact binding and its reported readiness.
type ArtifactView struct {
	Name        string `json:"name"`
	Artifact    string `json:"artifact"`
	ContentType string `json:"content_type"`
	Purpose     string `json:"purpose,omitempty"`
	Generated   bool   `json:"generated"`
	State       string `json:"state"`
	Reason      string `json:"reason,omitempty"`
}

// ListArtifacts prints declared artifacts and any additional reported artifacts.
func ListArtifacts(ctx context.Context, env Env, ref string) ([]ArtifactView, error) {
	if err := requireCapability(ctx, env, "artifact_discovery"); err != nil {
		return nil, err
	}
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return nil, err
	}
	var reported []management.RepresentationStatus
	if source.SearchStatus != nil {
		reported = source.SearchStatus.Artifacts
	}
	statuses := representationReportsByName(reported)
	view := make([]ArtifactView, 0, len(source.Artifacts)+len(reported))
	for _, binding := range source.Artifacts {
		item := ArtifactView{
			Name: binding.Name, Artifact: binding.Artifact, ContentType: binding.ContentType,
			Purpose: binding.Purpose, Generated: binding.Generated, State: "unreported",
		}
		if status, ok := statuses[binding.Name]; ok {
			item.State, item.Reason = string(status.State), status.Reason
			delete(statuses, binding.Name)
		}
		view = append(view, item)
	}
	for _, name := range slices.Sorted(maps.Keys(statuses)) {
		status := statuses[name]
		view = append(view, ArtifactView{Name: name, State: string(status.State), Reason: status.Reason})
	}
	if env.JSON {
		return view, Classify(env.writeJSON(struct {
			SourceID string         `json:"source_id"`
			Data     []ArtifactView `json:"data"`
		}{SourceID: source.Id, Data: view}))
	}
	var out strings.Builder
	if len(view) == 0 {
		out.WriteString("No artifacts declared.\n")
	} else {
		out.WriteString("NAME\tARTIFACT\tCONTENT_TYPE\tPURPOSE\tGENERATED\tSTATE\tREASON\n")
		for _, item := range view {
			fmt.Fprintf(&out, "%s\t%s\t%s\t%s\t%t\t%s\t%s\n",
				item.Name, item.Artifact, item.ContentType, item.Purpose, item.Generated, item.State, item.Reason)
		}
	}
	return view, Classify(env.writeHuman(out.String()))
}

func representationReportsByName(reported []management.RepresentationStatus) map[string]management.RepresentationStatus {
	statuses := make(map[string]management.RepresentationStatus, len(reported))
	for _, status := range reported {
		statuses[status.Name] = status
	}
	return statuses
}

// representations joins declared names of one kind with their reported
// readiness, in declaration order, then appends undeclared reports by name.
func representations(kind string, declared []string, reported []management.RepresentationStatus) []RepresentationView {
	statuses := representationReportsByName(reported)
	view := make([]RepresentationView, 0, len(declared)+len(reported))
	for _, name := range declared {
		item := RepresentationView{Kind: kind, Name: name, Declared: true, State: "unreported"}
		if status, ok := statuses[name]; ok {
			item.State, item.Reason = string(status.State), status.Reason
			delete(statuses, name)
		}
		view = append(view, item)
	}
	for _, name := range slices.Sorted(maps.Keys(statuses)) {
		status := statuses[name]
		view = append(view, RepresentationView{Kind: kind, Name: name, State: string(status.State), Reason: status.Reason})
	}
	return view
}
