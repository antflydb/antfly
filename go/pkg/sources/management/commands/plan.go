// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// PlanOptions supplies runtime-specific identity, connection, and policy hooks.
type PlanOptions struct {
	// Identity defaults to the manifest key.
	Identity func(key string) (string, error)
	// ResolveConnection defaults to returning the reference unchanged.
	ResolveConnection func(ctx context.Context, ref string) (string, error)
	// DefaultPermissions is required only when creating without explicit permissions.
	DefaultPermissions func(ctx context.Context) (management.Permissions, error)
	// Extensions validates runtime extensions; a nil hook rejects any extension.
	Extensions func(key string, ext map[string]json.RawMessage, c management.Capabilities) error
}

type PlanAction string

const (
	PlanCreate    PlanAction = "create"
	PlanUpdate    PlanAction = "update"
	PlanUnchanged PlanAction = "unchanged"
)

type PlanItem struct {
	Key      string            `json:"key"`
	SourceID string            `json:"source_id"`
	Action   PlanAction        `json:"action"`
	Changes  []string          `json:"changes,omitempty"`
	Revision string            `json:"revision,omitempty"`
	Desired  management.Source `json:"-"`
}

type Plan struct {
	Items []PlanItem `json:"items"`
}

type ApplyResult struct {
	Key      string     `json:"key"`
	SourceID string     `json:"source_id"`
	Action   PlanAction `json:"action"`
	Outcome  string     `json:"outcome"`
	Revision string     `json:"revision,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// PlanManifest prints a read-only plan in manifest order.
func PlanManifest(ctx context.Context, env Env, m Manifest, opts PlanOptions) (Plan, error) {
	plan, _, err := planManifest(ctx, env.API, m, opts)
	if err != nil {
		return Plan{}, err
	}
	if env.JSON {
		return plan, Classify(env.writeJSON(plan))
	}
	var out strings.Builder
	out.WriteString("ACTION\tKEY\tSOURCE_ID\tCHANGES\n")
	counts := make(map[PlanAction]int)
	for _, item := range plan.Items {
		changes := "-"
		if len(item.Changes) != 0 {
			changes = strings.Join(item.Changes, ",")
		}
		fmt.Fprintf(&out, "%s\t%s\t%s\t%s\n", item.Action, item.Key, item.SourceID, changes)
		counts[item.Action]++
	}
	fmt.Fprintf(&out, "Plan: %d to create, %d to update, %d unchanged.\n",
		counts[PlanCreate], counts[PlanUpdate], counts[PlanUnchanged])
	out.WriteString("Sources absent from the file are left untouched; use archive or purge to retire them.\n")
	return plan, Classify(env.writeHuman(out.String()))
}

// planManifest stays silent and retains the capabilities used to build the plan.
func planManifest(ctx context.Context, api API, m Manifest, opts PlanOptions) (Plan, management.Capabilities, error) {
	c, err := api.Capabilities(ctx)
	if err != nil {
		return Plan{}, c, Classify(err)
	}
	if err := Require(c, "configure_sources"); err != nil {
		return Plan{}, c, err
	}
	for _, entry := range m.Sources {
		if entry.SearchVersion != nil && *entry.SearchVersion == 2 {
			if err := Require(c, "search_version:2"); err != nil {
				return Plan{}, c, err
			}
		}
		if len(entry.Extensions) != 0 {
			if opts.Extensions == nil {
				first := slices.Min(slices.Collect(maps.Keys(entry.Extensions)))
				return Plan{}, c, Unsupported(c.Runtime, fmt.Sprintf("manifest extension %q", first))
			}
			if err := opts.Extensions(entry.Key, entry.Extensions, c); err != nil {
				return Plan{}, c, Classify(err)
			}
		}
	}
	plan := Plan{Items: make([]PlanItem, 0, len(m.Sources))}
	for i, entry := range m.Sources {
		id := entry.Key
		if opts.Identity != nil {
			id, err = opts.Identity(entry.Key)
			if err != nil {
				return Plan{}, c, Classify(err)
			}
		}
		connection := entry.Connection
		if opts.ResolveConnection != nil {
			connection, err = opts.ResolveConnection(ctx, entry.Connection)
			if err != nil {
				return Plan{}, c, Classify(err)
			}
		}
		current, err := api.GetSource(ctx, id)
		create := hasManifestCode(err, CodeNotFound)
		if err != nil && !create {
			return Plan{}, c, Classify(err)
		}
		if !create && current.Table != entry.Table {
			return Plan{}, c, manifestInvalid(manifestSourcePath(i, entry.Key),
				fmt.Sprintf("destination table is immutable (current %q)", current.Table))
		}
		if create {
			current = management.Source{}
		}
		desired, err := manifestDesired(ctx, current, entry, id, connection, create, opts)
		if err != nil {
			return Plan{}, c, Classify(err)
		}
		item := PlanItem{Key: entry.Key, SourceID: id, Action: PlanCreate, Desired: desired}
		if !create {
			item.Revision = sourceRevision(current)
			item.Action = PlanUnchanged
			if management.SourceRevision(desired) != management.SourceRevision(current) {
				item.Action = PlanUpdate
				item.Changes, err = manifestChanges(current, desired)
				if err != nil {
					return Plan{}, c, Classify(err)
				}
			}
		}
		plan.Items = append(plan.Items, item)
	}
	return plan, c, nil
}

func manifestDesired(ctx context.Context, current management.Source, entry ManifestSource, id, connection string, create bool, opts PlanOptions) (management.Source, error) {
	desired := manifestDeclaration(current)
	desired.Id, desired.Name = id, entry.Name
	if desired.Name == "" {
		desired.Name = entry.Key
	}
	desired.Connection, desired.Table, desired.Config = connection, entry.Table, entry.Config
	if entry.Publication != "" {
		desired.Publication = entry.Publication
	} else if create {
		desired.Publication = management.SourcePublicationNative
	}
	if entry.Permissions != nil {
		desired.Permissions = *entry.Permissions
	} else if create {
		if opts.DefaultPermissions == nil {
			return management.Source{}, manifestInvalid("source ("+entry.Key+")", "permissions are required to create a source")
		}
		permissions, err := opts.DefaultPermissions(ctx)
		if err != nil {
			return management.Source{}, err
		}
		desired.Permissions = permissions
	}
	if entry.PermissionTtlSeconds != nil {
		desired.PermissionTtlSeconds = *entry.PermissionTtlSeconds
	}
	if entry.SearchVersion != nil {
		desired.SearchVersion = *entry.SearchVersion
	}
	if entry.Streams != nil {
		desired.Streams = *entry.Streams
	}
	if entry.Artifacts != nil {
		desired.Artifacts = *entry.Artifacts
	}
	if entry.Processing != nil {
		desired.Processing = entry.Processing
	}
	if entry.Identity != nil {
		desired.Identity = *entry.Identity
	}
	if entry.Knowledge != nil {
		desired.Knowledge = entry.Knowledge
	}
	return desired, nil
}

func manifestDeclaration(source management.Source) management.Source {
	source.State = ""
	source.SearchStatus = nil
	source.KnowledgeStatus = nil
	source.Rekeys = false
	source.Revision = ""
	return source
}

func manifestChanges(current, desired management.Source) ([]string, error) {
	fields := func(source management.Source) (map[string]json.RawMessage, error) {
		raw, err := json.Marshal(manifestDeclaration(source))
		if err != nil {
			return nil, err
		}
		var result map[string]json.RawMessage
		err = json.Unmarshal(raw, &result)
		return result, err
	}
	before, err := fields(current)
	if err != nil {
		return nil, err
	}
	after, err := fields(desired)
	if err != nil {
		return nil, err
	}
	changes := make([]string, 0)
	for key, value := range before {
		if !bytes.Equal(value, after[key]) {
			changes = append(changes, key)
		}
	}
	for key := range after {
		if _, exists := before[key]; !exists {
			changes = append(changes, key)
		}
	}
	slices.Sort(changes)
	return changes, nil
}

// sourceRevision prefers the server's revision, which preconditions are checked
// against; the server may encode declarations (e.g. JSON numbers) differently
// from a client-side recomputation.
func sourceRevision(s management.Source) string {
	if s.Revision != "" {
		return s.Revision
	}
	return management.SourceRevision(s)
}

func hasManifestCode(err error, code string) bool {
	var failure *Error
	return errors.As(Classify(err), &failure) && failure.Code == code
}

// ApplyManifest plans silently, then applies changes with atomic preconditions.
// Changed sources require revision_preconditions; unchanged plans need no writes.
func ApplyManifest(ctx context.Context, env Env, m Manifest, opts PlanOptions) ([]ApplyResult, error) {
	plan, c, err := planManifest(ctx, env.API, m, opts)
	if err != nil {
		return nil, err
	}
	if !c.RevisionPreconditions {
		for _, item := range plan.Items {
			if item.Action == PlanCreate || item.Action == PlanUpdate {
				return nil, Unsupported(c.Runtime, "revision_preconditions")
			}
		}
	}
	results := make([]ApplyResult, 0, len(plan.Items))
	counts := make(map[string]int)
	for _, item := range plan.Items {
		result := ApplyResult{Key: item.Key, SourceID: item.SourceID, Action: item.Action, Outcome: "unchanged"}
		if item.Action != PlanUnchanged {
			var source management.Source
			var err error
			if item.Action == PlanCreate {
				source, err = env.API.PutSourceIfAbsent(ctx, item.Desired)
			} else {
				source, err = env.API.PutSourceIfMatch(ctx, item.Desired, item.Revision)
			}
			switch {
			case hasManifestCode(err, CodeConflict):
				result.Outcome, result.Error = "conflict", Classify(err).Error()
			case err != nil:
				result.Outcome, result.Error = "failed", Classify(err).Error()
			case item.Action == PlanCreate:
				result.Outcome, result.Revision = "created", sourceRevision(source)
			default:
				result.Outcome, result.Revision = "updated", sourceRevision(source)
			}
		}
		counts[result.Outcome]++
		results = append(results, result)
	}
	conflicts, failed := counts["conflict"], counts["failed"]
	var outputErr error
	if env.JSON {
		outputErr = env.writeJSON(struct {
			Results []ApplyResult `json:"results"`
		}{Results: results})
	} else {
		var out strings.Builder
		for _, result := range results {
			fmt.Fprintf(&out, "%s  %s  %s", result.Outcome, result.Key, result.SourceID)
			if result.Error != "" {
				fmt.Fprintf(&out, "  %s", result.Error)
			}
			out.WriteByte('\n')
		}
		fmt.Fprintf(&out, "Applied: %d created, %d updated, %d unchanged, %d conflicts, %d failed.\n",
			counts["created"], counts["updated"], counts["unchanged"], conflicts, failed)
		outputErr = env.writeHuman(out.String())
	}
	if conflicts != 0 {
		return results, &Error{Code: CodeConflict, Message: fmt.Sprintf("%d source(s) changed since plan; review with plan and rerun apply", conflicts)}
	}
	if failed != 0 {
		return results, &Error{Code: CodeFailed, Message: fmt.Sprintf("%d of %d sources failed to apply; rerunning apply is safe", failed, len(results))}
	}
	return results, Classify(outputErr)
}
