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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Profile struct {
	Version           int                   `json:"version"`
	Endpoint          string                `json:"endpoint"`
	DataPlaneTokenEnv string                `json:"data_plane_token_env,omitempty"`
	Principals        []string              `json:"principals,omitempty"`
	Connections       map[string]Connection `json:"connections"`
	Sources           []Source              `json:"sources"`
}
type Source struct {
	ID                   string          `json:"id"`
	Alias                string          `json:"alias"`
	Connection           string          `json:"connection"`
	Table                string          `json:"table"`
	Config               json.RawMessage `json:"config"`
	Permissions          *Permissions    `json:"permissions"`
	PermissionTTLSeconds int             `json:"permission_ttl_seconds,omitempty"`
}
type Runner struct {
	Profile               Profile
	StateDir, CatalogPath string
	Factory               Factory
	Publisher             *Publisher
	Restart               bool
}
type runState struct {
	Version                               int
	Endpoint, Table, SourceID, ConfigHash string
	Owned                                 map[string]string
	Pending                               *Inventory
	StartedAt                             int64
}

func (r *Runner) Sync(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(r.StateDir, 0700); err != nil {
		return err
	}
	lock := flock.New(filepath.Join(r.StateDir, "runner.lock"))
	locked, err := lock.TryLockContext(ctx, 100*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("source state is locked")
	}
	defer lock.Unlock()
	if r.Factory == nil {
		r.Factory = EnvironmentFactory
	}
	if r.Publisher == nil {
		token := ""
		if name := r.Profile.DataPlaneTokenEnv; name != "" {
			var ok bool
			token, ok = os.LookupEnv(name)
			if !ok || token == "" {
				return fmt.Errorf("data-plane token environment variable %s is not set", name)
			}
		}
		r.Publisher = &Publisher{Endpoint: r.Profile.Endpoint, Token: token}
	}
	if strings.TrimRight(r.Publisher.Endpoint, "/") != strings.TrimRight(r.Profile.Endpoint, "/") {
		return fmt.Errorf("publisher endpoint does not match profile")
	}
	catalog := Catalog{Version: 2, Endpoint: r.Profile.Endpoint, Principals: append([]string{}, r.Profile.Principals...)}
	var existing Catalog
	if err := loadJSON(r.CatalogPath, &existing, 32<<20); err == nil {
		catalog = existing
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load catalog: %w", err)
	}
	if catalog.Version != 2 || strings.TrimRight(catalog.Endpoint, "/") != strings.TrimRight(r.Profile.Endpoint, "/") {
		return fmt.Errorf("catalog belongs to another endpoint")
	}
	old := map[string]Binding{}
	for _, binding := range catalog.Sources {
		if _, exists := old[binding.ID]; exists {
			return fmt.Errorf("duplicate catalog identity")
		}
		old[binding.ID] = binding
	}
	catalog.Sources = nil
	catalog.Principals = append([]string{}, r.Profile.Principals...)
	for _, source := range r.Profile.Sources {
		alias := source.Alias
		if alias == "" {
			alias = source.ID
		}
		binding := Binding{ID: source.ID, Alias: alias, Table: source.Table, DocumentField: "grep", Filter: SourceFilter(source.ID), Status: "pending"}
		if prior, ok := old[source.ID]; ok && prior.Table == source.Table {
			binding.Status = prior.Status
			binding.ValidUntil = prior.ValidUntil
		}
		catalog.Sources = append(catalog.Sources, binding)
	}
	if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
		return err
	}
	for i, source := range r.Profile.Sources {
		catalog.Sources[i].Status = "syncing"
		catalog.Sources[i].ValidUntil = nil
		if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
			return err
		}
		started, err := r.syncSource(ctx, source, r.Profile.Connections[source.Connection])
		if err != nil {
			catalog.Sources[i].Status = "stale"
			return errors.Join(err, writeJSON(r.CatalogPath, catalog, 32<<20))
		}
		ttl := source.PermissionTTLSeconds
		if ttl == 0 {
			ttl = 900
		}
		until := started + int64(ttl)
		catalog.Sources[i].Status = "searchable"
		catalog.Sources[i].ValidUntil = &until
		if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) validate() error {
	if r.Profile.Version != 1 || r.StateDir == "" || r.CatalogPath == "" || len(r.Profile.Sources) == 0 || len(r.Profile.Sources) > 1000 {
		return fmt.Errorf("invalid source profile or state/catalog paths")
	}
	if err := ValidateEndpoint(r.Profile.Endpoint); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, s := range r.Profile.Sources {
		if !canonicalID(s.ID) || seen[s.ID] || s.Table == "" || strings.ContainsAny(s.Table, "\x00\r\n") || s.Table == "." || s.Table == ".." || s.Permissions == nil {
			return fmt.Errorf("source needs a unique UUID, destination and explicit permissions")
		}
		seen[s.ID] = true
		if _, ok := r.Profile.Connections[s.Connection]; !ok {
			return fmt.Errorf("source connection is not configured")
		}
		if s.PermissionTTLSeconds != 0 && (s.PermissionTTLSeconds < 60 || s.PermissionTTLSeconds > 86400) {
			return fmt.Errorf("permission TTL must be 60..86400 seconds")
		}
	}
	return nil
}
func canonicalID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func (r *Runner) syncSource(ctx context.Context, s Source, connection Connection) (int64, error) {
	statePath := filepath.Join(r.StateDir, s.ID+".json")
	hashSource := s
	hashSource.Alias = ""
	hashSource.PermissionTTLSeconds = 0
	identity, _ := json.Marshal(struct {
		Source     Source
		Connection Connection
	}{hashSource, connection})
	digest := sha256.Sum256(identity)
	hash := hex.EncodeToString(digest[:])
	state := runState{Version: 1, Endpoint: r.Profile.Endpoint, Table: s.Table, SourceID: s.ID, ConfigHash: hash, Owned: map[string]string{}}
	var persisted runState
	if err := loadJSON(statePath, &persisted, 64<<20); err == nil {
		if persisted.Version != 1 || persisted.Endpoint != state.Endpoint || persisted.Table != s.Table || persisted.SourceID != s.ID {
			return 0, fmt.Errorf("source state belongs to another destination")
		}
		state = persisted
		if state.ConfigHash != hash {
			if state.Pending != nil && !r.Restart {
				return 0, fmt.Errorf("source configuration changed during a pinned run; use --restart")
			}
			state.Pending = nil
			state.ConfigHash = hash
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if state.Owned == nil {
		state.Owned = map[string]string{}
	}
	if len(state.Owned) > maxImportSourceObjects {
		return 0, fmt.Errorf("source ownership exceeds budget")
	}
	request := Request{SourceNamespace: "job/" + s.ID, Config: s.Config, Mode: ImportModeSource}
	for id := range state.Owned {
		if !ownedID(id, request.SourceNamespace) {
			return 0, fmt.Errorf("invalid persisted ownership identity")
		}
	}
	journalPath := statePath + ".owned"
	if err := replayOwnership(journalPath, request.SourceNamespace, state.Owned); err != nil {
		return 0, err
	}
	connector, err := r.Factory(ctx, connection)
	if err != nil {
		return 0, err
	}
	if r.Restart {
		state.Pending = nil
	}
	if state.Pending == nil {
		inventory, err := connector.Enumerate(ctx, request)
		if err != nil {
			return 0, err
		}
		if !inventory.Authoritative || len(inventory.Objects) > maxImportSourceObjects {
			return 0, fmt.Errorf("complete authoritative inventory required")
		}
		seen := map[string]bool{}
		for _, item := range inventory.Objects {
			if !ownedID(item.ID, request.SourceNamespace) || seen[item.ID] || item.Revision == "" || item.SourceKind != connection.Provider {
				return 0, fmt.Errorf("invalid/duplicate pinned source identity")
			}
			seen[item.ID] = true
		}
		for _, d := range inventory.Diagnostics {
			if d.Level == "error" {
				return 0, fmt.Errorf("inventory is incomplete: %s", d.Code)
			}
		}
		state.Pending = &inventory
		state.StartedAt = time.Now().Unix()
		if err := writeJSON(statePath, state, 64<<20); err != nil {
			return 0, err
		}
	}
	if !state.Pending.Authoritative || len(state.Pending.Objects) > maxImportSourceObjects {
		return 0, fmt.Errorf("invalid persisted inventory authority")
	}
	seenPending := map[string]bool{}
	for _, item := range state.Pending.Objects {
		if !ownedID(item.ID, request.SourceNamespace) || seenPending[item.ID] || item.Revision == "" || item.SourceKind != connection.Provider {
			return 0, fmt.Errorf("invalid persisted inventory identity")
		}
		seenPending[item.ID] = true
	}
	if err := connector.ValidateInventory(ctx, request, *state.Pending); err != nil {
		return 0, err
	}
	if err := writeJSON(statePath, state, 64<<20); err != nil {
		return 0, err
	}
	if err := resetOwnershipJournal(journalPath); err != nil {
		return 0, err
	}
	if err := r.Publisher.EnsureTable(ctx, s.Table); err != nil {
		return 0, err
	}
	selected := map[string]bool{}
	var total int64
	for _, item := range state.Pending.Objects {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		selected[item.ID] = true
		data, err := connector.Read(ctx, request, item)
		if err != nil {
			return 0, err
		}
		total += int64(len(data))
		if total > 10<<30 {
			return 0, fmt.Errorf("source run exceeds 10 GiB")
		}
		item.Metadata = cloneSourceMetadata(item.Metadata)
		item.Metadata["connection_id"] = s.Connection
		doc, err := Normalize(s.ID, item, data, *s.Permissions, time.Now())
		if err != nil {
			return 0, err
		}
		if err := journalOwnership(journalPath, ownershipEvent{ID: item.ID, Revision: item.Revision}); err != nil {
			return 0, err
		}
		state.Owned[item.ID] = item.Revision
		if len(state.Owned) > maxImportSourceObjects {
			return 0, fmt.Errorf("source ownership exceeds budget")
		}
		record := map[string]any{"id": item.ID, "source_id": s.ID, "doc_type": "source_item", "grep": doc}
		if err := r.Publisher.Batch(ctx, s.Table, map[string]any{item.ID: record}, nil); err != nil {
			return 0, err
		}
		if err := r.Publisher.Ready(ctx, s.Table, item.ID, doc); err != nil {
			return 0, err
		}
	}
	absent := []string{}
	for id := range state.Owned {
		if !selected[id] {
			absent = append(absent, id)
		}
	}
	sort.Strings(absent)
	if err := connector.ValidateAbsentSources(ctx, request, absent); err != nil {
		return 0, err
	}
	for start := 0; start < len(absent); start += 100 {
		batch := absent[start:min(start+100, len(absent))]
		if err := r.Publisher.Batch(ctx, s.Table, nil, batch); err != nil {
			return 0, err
		}
		if err := journalOwnership(journalPath, ownershipEvent{Delete: batch}); err != nil {
			return 0, err
		}
		for _, id := range batch {
			delete(state.Owned, id)
		}
	}
	state.Pending = nil
	if err := writeJSON(statePath, state, 64<<20); err != nil {
		return 0, err
	}
	if err := resetOwnershipJournal(journalPath); err != nil {
		return 0, err
	}
	return state.StartedAt, nil
}
func ownedID(id, namespace string) bool {
	scheme, rest, ok := strings.Cut(id, "://"+namespace+"/")
	return ok && scheme != "" && rest != "" && !strings.ContainsAny(id, "\x00\r\n")
}
func loadJSON(path string, out any, limit int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return fmt.Errorf("JSON state exceeds budget")
	}
	return json.Unmarshal(raw, out)
}
func writeJSON(path string, value any, limit int64) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return fmt.Errorf("JSON state exceeds budget")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".source-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(raw); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
