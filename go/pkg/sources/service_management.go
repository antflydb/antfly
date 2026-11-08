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

package sources

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func (s *Service) serveManagement(w http.ResponseWriter, r *http.Request) {
	http.StripPrefix("/sources/v1", management.Handler(localManagement{s})).ServeHTTP(w, r)
}

type localManagement struct{ s *Service }

func (b localManagement) Capabilities(context.Context) (management.Capabilities, error) {
	return management.Capabilities{SearchVersions: []int{1, 2}, ArtifactDiscovery: true, Version: 1, Runtime: "standalone", PerSourceRuns: true, CancelRuns: true, ConfigureSources: true, PurgeSources: true, MediaProxy: true, SerializedRuns: true, ConfigurationUrl: "/v1/connections"}, nil
}
func managementSource(source Source) management.Source {
	var cfg map[string]any
	_ = json.Unmarshal(source.Config, &cfg)
	publication := source.Publication
	if publication == "" {
		publication = "native"
	}
	var perms management.Permissions
	if source.Permissions != nil {
		perms = management.Permissions{Public: source.Permissions.Public, Readers: source.Permissions.Readers}
	}
	return management.Source{Processing: source.Processing, SearchVersion: source.SearchVersion, Artifacts: source.Artifacts, Identity: management.SourceIdentity{Namespace: source.Identity.Namespace}, Streams: source.Streams, Id: source.ID, Name: source.Alias, Connection: source.Connection, Table: source.Table, Config: cfg, Permissions: perms, PermissionTtlSeconds: source.PermissionTTLSeconds, Publication: management.SourcePublication(publication), State: "configured"}
}
func (b localManagement) ListSources(ctx context.Context, limit int, cursor string) (management.SourcePage, error) {
	b.s.mu.Lock()
	profile := cloneProfile(b.s.profile)
	b.s.mu.Unlock()
	sort.Slice(profile.Sources, func(i, j int) bool { return profile.Sources[i].ID < profile.Sources[j].ID })
	page := management.SourcePage{Data: []management.Source{}}
	bytes := 0
	for _, source := range profile.Sources {
		if source.ID <= cursor {
			continue
		}
		item := managementSource(source)
		size, err := management.SourceBytes(item)
		if err != nil {
			return page, err
		}
		if size > management.SourcePageBudget {
			return page, management.Failure(413, "source_declaration_too_large")
		}
		if len(page.Data) == limit || bytes+size > management.SourcePageBudget {
			page.NextCursor = page.Data[len(page.Data)-1].Id
			break
		}
		page.Data = append(page.Data, item)
		bytes += size
	}
	return page, nil
}
func (b localManagement) searchPublisher() *Publisher {
	b.s.mu.Lock()
	profile := cloneProfile(b.s.profile)
	b.s.mu.Unlock()
	token := ""
	if profile.DataPlaneTokenEnv != "" {
		token = os.Getenv(profile.DataPlaneTokenEnv)
	}
	return &Publisher{Endpoint: profile.Endpoint, Token: token}
}
func (b localManagement) GetSource(ctx context.Context, id string) (management.Source, error) {
	b.s.mu.Lock()
	profile := cloneProfile(b.s.profile)
	b.s.mu.Unlock()
	for _, source := range profile.Sources {
		if source.ID == id {
			out := managementSource(source)
			if source.SearchVersion == SearchVersion {
				status := ObserveSearch(ctx, b.searchPublisher(), Binding{SearchVersion: source.SearchVersion, Table: source.Table, Streams: source.Streams, Artifacts: source.Artifacts})
				out.SearchStatus = &status
			}
			return out, nil
		}
	}
	return management.Source{}, management.Failure(404, "source_not_found")
}

func (b localManagement) legacy(ctx context.Context, method, path string, input any) ([]byte, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+b.s.token)
	out := &controlResponse{header: make(http.Header)}
	b.s.ServeHTTP(out, r)
	if out.status >= 400 {
		var e management.Error
		_ = json.Unmarshal(out.body.Bytes(), &e)
		return nil, management.Failure(out.status, e.Code)
	}
	return out.body.Bytes(), nil
}
func (b localManagement) PutSource(ctx context.Context, input management.Source) (management.Source, error) {
	if input.SearchStatus != nil {
		return management.Source{}, management.Failure(400, "read_only_search_status")
	}
	cfg, err := json.Marshal(input.Config)
	if err != nil {
		return management.Source{}, err
	}
	source := Source{Processing: input.Processing, SearchVersion: input.SearchVersion, Artifacts: input.Artifacts, Identity: Identity{Namespace: input.Identity.Namespace}, Streams: input.Streams, ID: input.Id, Alias: input.Name, Connection: input.Connection, Table: input.Table, Config: cfg, Publication: string(input.Publication), Permissions: &Permissions{Public: input.Permissions.Public, Readers: input.Permissions.Readers}, PermissionTTLSeconds: input.PermissionTtlSeconds}
	raw, err := b.legacy(ctx, "PUT", "/v1/sources/"+input.Id, source)
	if err != nil {
		return management.Source{}, err
	}
	var mutation struct {
		Rekeys bool `json:"rekeys"`
	}
	if err = json.Unmarshal(raw, &mutation); err != nil {
		return management.Source{}, err
	}
	if source.Processing != nil {
		publisher := b.searchPublisher()
		if err := publisher.EnsureTable(ctx, source.Table); err != nil {
			return management.Source{}, err
		}
		if err := ReconcileSourceSearch(ctx, publisher, source); err != nil {
			return management.Source{}, err
		}
	}
	b.s.mu.Lock()
	profile := cloneProfile(b.s.profile)
	b.s.mu.Unlock()
	runner := Runner{Profile: profile, StateDir: b.s.runnerPath(), Publisher: b.searchPublisher()}
	if !mutation.Rekeys {
		if err := runner.RefreshSearchDiscovery(ctx, input.Id); err != nil {
			return management.Source{}, err
		}
	}
	out, err := b.GetSource(ctx, input.Id)
	out.Rekeys = mutation.Rekeys
	return out, err
}
func (b localManagement) RemoveSource(ctx context.Context, id string, purge bool) (management.Mutation, error) {
	suffix := ""
	if purge {
		suffix = "?purge=true"
	}
	_, err := b.legacy(ctx, "DELETE", "/v1/sources/"+id+suffix, nil)
	state := "retained"
	if purge {
		state = "purge_pending"
	}
	return management.Mutation{SourceId: id, State: state}, err
}
func jobRequestHash(id string, restart, purge bool) string {
	raw, _ := json.Marshal([]any{id, restart, purge})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (s *Service) historyPath(id string) string {
	return filepath.Join(s.directory, "runs", id+".json")
}
func (s *Service) requestPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.directory, "run-requests", hex.EncodeToString(sum[:])+".json")
}
func validJobID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Caller holds the state mutex. Archived receipts are immutable; the latest is
// atomically updated until terminal, then archived before another is admitted.
func (s *Service) readJob(id string) (SyncJob, error) {
	if !validJobID(id) {
		return SyncJob{}, management.Failure(400, "invalid_run_id")
	}
	if s.job != nil && s.job.ID == id {
		return *s.job, nil
	}
	var job SyncJob
	if err := loadJSON(s.historyPath(id), &job, 4096); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return job, management.Failure(404, "run_not_found")
		}
		return job, err
	}
	if job.ID != id {
		return job, management.Failure(500, "invalid_run_receipt")
	}
	return job, nil
}
func (s *Service) idempotentJob(key string) (SyncJob, bool, error) {
	if s.job != nil && s.job.RequestKey == key {
		return *s.job, true, nil
	}
	var ref struct {
		ID string `json:"id"`
	}
	if err := loadJSON(s.requestPath(key), &ref, 4096); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return SyncJob{}, false, nil
		}
		return SyncJob{}, false, err
	}
	job, err := s.readJob(ref.ID)
	return job, err == nil, err
}
func managementRun(job SyncJob) management.Run {
	out := management.Run{Id: job.ID, SourceId: job.SourceID, State: management.RunState(job.State), Outcome: "unknown", CreatedAt: job.StartedAt, FinishedAt: job.FinishedAt, Code: job.Code, Operation: management.RunOperation(job.Operation)}
	if job.State == "completed" {
		out.Outcome = "success"
		out.Authoritative = job.Operation == "sync"
	} else if job.State == "failed" || job.State == "cancelled" || job.State == "interrupted" {
		out.Outcome = "failed"
	}
	return out
}
func (b localManagement) GetRun(ctx context.Context, sourceID, id string) (management.Run, error) {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	job, err := b.s.readJob(id)
	if err != nil {
		return management.Run{}, err
	}
	if job.SourceID != sourceID {
		return management.Run{}, management.Failure(404, "run_not_found")
	}
	return managementRun(job), nil
}
func (b localManagement) ListRuns(ctx context.Context, sourceID string, limit int, cursor string) (management.RunPage, error) {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	page := management.RunPage{Data: []management.Run{}}
	id := cursor
	if id == "" && b.s.job != nil {
		id = b.s.job.ID
	}
	// Bound disk work even when many sibling-source runs intervene. An empty
	// page can have a next cursor; clients continue until it is absent.
	for scanned := 0; id != "" && scanned < limit; scanned++ {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		job, err := b.s.readJob(id)
		if err != nil {
			return page, err
		}
		if job.SourceID == sourceID {
			page.Data = append(page.Data, managementRun(job))
		}
		if job.PreviousID == id {
			return page, management.Failure(500, "invalid_run_receipt")
		}
		id = job.PreviousID
	}
	page.NextCursor = id
	return page, nil
}
func (b localManagement) StartRun(ctx context.Context, id, key string, input management.RunInput) (management.Run, error) {
	raw, err := b.legacy(ctx, "POST", "/v1/sync", map[string]any{"source_id": id, "request_key": id + "/" + key, "restart": input.Restart, "purge_only": input.Operation == "purge"})
	if err != nil {
		return management.Run{}, err
	}
	var job SyncJob
	if err = json.Unmarshal(raw, &job); err != nil {
		return management.Run{}, err
	}
	return managementRun(job), nil
}
func (b localManagement) CancelRun(ctx context.Context, sourceID, id string) (management.Run, error) {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	job, err := b.s.readJob(id)
	if err != nil {
		return management.Run{}, err
	}
	if job.SourceID != sourceID {
		return management.Run{}, management.Failure(404, "run_not_found")
	}
	if b.s.job != nil && b.s.job.ID == id && b.s.cancel != nil {
		b.s.cancel()
	}
	return managementRun(job), nil
}

var _ management.Backend = localManagement{}
