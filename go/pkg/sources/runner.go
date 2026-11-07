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
	"crypto/rand"
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
	"unicode/utf8"
)

// Ownership includes previously attempted writes across interrupted inventories.
// It is bounded separately from the 10,000-item active inventory.
const maxOwnedSourceObjects = 1000000

type Profile struct {
	LocalMigrations   map[string]LocalMigrationReceipt `json:"local_migrations,omitempty"`
	CatalogTable      string                           `json:"catalog_table,omitempty"`
	Version           int                              `json:"version"`
	Endpoint          string                           `json:"endpoint"`
	DataPlaneTokenEnv string                           `json:"data_plane_token_env,omitempty"`
	Principals        []string                         `json:"principals,omitempty"`
	Connections       map[string]Connection            `json:"connections"`
	Sources           []Source                         `json:"sources"`
}

// Identity names the opaque prefix that scopes a source's row keys,
// independent of its connection or config (see IdentityKey). Immutable once
// set: Service rejects any later change.
type Identity struct {
	Namespace string `json:"namespace,omitempty"`
}
type Source struct {
	Streams []TextStream `json:"streams,omitempty"`
	// Publication defaults to native acquisition. Binary rows require a revision-enforcing
	// fetch resolver, supplied by the source service or an embedding application.
	Publication          string          `json:"publication,omitempty"`
	ID                   string          `json:"id"`
	Alias                string          `json:"alias"`
	Connection           string          `json:"connection"`
	Table                string          `json:"table"`
	Config               json.RawMessage `json:"config"`
	Permissions          *Permissions    `json:"permissions"`
	PermissionTTLSeconds int             `json:"permission_ttl_seconds,omitempty"`
	Identity             Identity        `json:"identity,omitzero"`
}
type Runner struct {
	// SourceID selects one source while retaining the complete profile/catalog.
	// Filtering Profile.Sources would incorrectly retire unrelated bindings.
	SourceID string

	// Only Service's resolver opts in: checkMediaAccess proves current access
	// and revision, including any policy imposed by an injected connector.
	mediaReferenceAuthorizes bool
	// MediaInventory publishes a compact authority generation at checkpoint boundaries.
	MediaInventory        func(context.Context, Source, *Inventory, *Inventory) error
	MediaReference        func(context.Context, Source, Request, Item) (MediaReference, error)
	Profile               Profile
	StateDir, CatalogPath string
	Factory               Factory
	Publisher             *Publisher
	Restart               bool
}
type runState struct {
	RecordFormat                          int
	Version                               int
	Endpoint, Table, SourceID, ConfigHash string
	Owned                                 map[string]string
	Pending                               *Inventory
	Published                             *Inventory
	AcquisitionID                         string
	StartedAt                             int64
}

func (r *Runner) Sync(ctx context.Context) error {
	if r.CatalogPath == "" {
		r.CatalogPath = filepath.Join(r.StateDir, "catalog.json")
	}
	if err := r.validate(); err != nil {
		return err
	}
	return r.withStateLock(ctx, func() error { return r.syncLocked(ctx) })
}

// withStateLock serializes every mutation of a Runner's database ownership and
// checkpoint files, including purge. Callbacks must not reacquire this lock.
func (r *Runner) withStateLock(ctx context.Context, work func() error) error {
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
	return work()
}

func (r *Runner) syncLocked(ctx context.Context) error {
	catalogTable := r.Profile.CatalogTable
	if catalogTable == "" {
		catalogTable = DefaultCatalogTable
	}
	destination := struct{ Endpoint, Table string }{r.Profile.Endpoint, catalogTable}
	var priorDestination struct{ Endpoint, Table string }
	destinationPath := filepath.Join(r.StateDir, "catalog-destination.json")
	if err := loadJSON(destinationPath, &priorDestination, 1<<20); err == nil {
		if destination != priorDestination {
			return fmt.Errorf("state belongs to another catalog destination")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeJSON(destinationPath, destination, 1<<20); err != nil {
		return err
	}
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
	catalog.Sources = []Binding{}
	catalog.Principals = append([]string{}, r.Profile.Principals...)
	for _, source := range r.Profile.Sources {
		alias := source.Alias
		if alias == "" {
			alias = source.ID
		}
		binding := Binding{ID: source.ID, Alias: alias, Table: source.Table, DocumentVersion: SourceRecordVersion, Fields: CanonicalDocumentFields(), Streams: source.Streams, Filter: SourceFilter(source.ID), Status: "pending"}
		if len(binding.Streams) == 0 && source.Publication != "media" {
			binding.Streams = DefaultTextStreams()
		}
		binding.Publication = source.Publication
		if binding.Publication == "" {
			binding.Publication = "native"
		}
		if prior, ok := old[source.ID]; ok && prior.Table == source.Table && binding.SameRecordLayout(prior) {
			binding.Status = prior.Status
			binding.ValidUntil = prior.ValidUntil
			binding.PublishedRevision = prior.PublishedRevision
		}
		catalog.Sources = append(catalog.Sources, binding)
	}
	store := CatalogStore{Client: r.Publisher, Table: r.Profile.CatalogTable}
	current := map[string]bool{}
	for _, source := range r.Profile.Sources {
		current[source.ID] = true
	}
	removed := []string{}
	for id := range old {
		if !current[id] {
			removed = append(removed, id)
		}
	}
	if err := store.Retire(ctx, removed); err != nil {
		return err
	}
	if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
		return err
	}
	for i, source := range r.Profile.Sources {
		if r.SourceID != "" && source.ID != r.SourceID {
			continue
		}
		catalog.Sources[i].Status = "syncing"
		catalog.Sources[i].PublishedRevision = ""
		catalog.Sources[i].ValidUntil = nil
		if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
			return err
		}
		connection := r.Profile.Connections[source.Connection]
		if err := store.Publish(ctx, NewCatalogRecord(catalog.Sources[i], connection.Provider, "", *source.Permissions)); err != nil {
			return err
		}
		started, revision, err := r.syncSource(ctx, source, connection)
		if err != nil {
			catalog.Sources[i].Status = "stale"
			return errors.Join(err, store.Publish(ctx, NewCatalogRecord(catalog.Sources[i], connection.Provider, "", *source.Permissions)), writeJSON(r.CatalogPath, catalog, 32<<20))
		}
		ttl := source.PermissionTTLSeconds
		if ttl == 0 {
			ttl = 900
		}
		until := started + int64(ttl)
		// Each successful publication gets a new fence even for an unchanged inventory.
		revision += ":" + rand.Text()
		catalog.Sources[i].PublishedRevision = revision
		catalog.Sources[i].Status = "searchable"
		catalog.Sources[i].ValidUntil = &until
		if err := store.Publish(ctx, NewCatalogRecord(catalog.Sources[i], connection.Provider, revision, *source.Permissions)); err != nil {
			return err
		}
		if err := writeJSON(r.CatalogPath, catalog, 32<<20); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) validate() error {
	if r.Profile.Version != 1 || r.StateDir == "" || r.CatalogPath == "" || len(r.Profile.Sources) > 1000 {
		return fmt.Errorf("invalid source profile or state/catalog paths")
	}
	if err := ValidateEndpoint(r.Profile.Endpoint); err != nil {
		return err
	}
	seen := map[string]bool{}
	selected := r.SourceID == ""
	for _, s := range r.Profile.Sources {
		if s.ID == r.SourceID {
			selected = true
		}
		catalogTable := r.Profile.CatalogTable
		if catalogTable == "" {
			catalogTable = DefaultCatalogTable
		}
		if s.Table == catalogTable {
			return fmt.Errorf("catalog cannot be a content destination")
		}
		if !canonicalID(s.ID) || seen[s.ID] || s.Table == "" || strings.ContainsAny(s.Table, "\x00\r\n") || s.Table == "." || s.Table == ".." || s.Permissions == nil {
			return fmt.Errorf("source needs a unique UUID, destination and explicit permissions")
		}
		if err := ValidateTextStreams(s.Streams); err != nil {
			return err
		}
		if s.Publication != "" && s.Publication != "text" && s.Publication != "media" && s.Publication != "native" {
			return fmt.Errorf("publication must be native, text or media")
		}
		if s.Publication == "media" && r.MediaReference == nil && (r.SourceID == "" || r.SourceID == s.ID) {
			return fmt.Errorf("media publication requires a revision-enforcing fetch resolver; use antfly-source serve")
		}
		seen[s.ID] = true
		if _, ok := r.Profile.Connections[s.Connection]; !ok && (r.SourceID == "" || r.SourceID == s.ID) {
			return fmt.Errorf("source connection is not configured")
		}
		if s.PermissionTTLSeconds != 0 && (s.PermissionTTLSeconds < 60 || s.PermissionTTLSeconds > 86400) {
			return fmt.Errorf("permission TTL must be 60..86400 seconds")
		}
		if ns := s.Identity.Namespace; ns != "" && (len(ns) > 4096 || !utf8.ValidString(ns) || strings.ContainsAny(ns, "\x00\r\n")) {
			return fmt.Errorf("invalid source identity namespace")
		}
	}
	if !selected {
		return fmt.Errorf("selected source is not configured")
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

// Hash the source and credential references, excluding only the fields that do
// not change acquisition identity. Legacy checkpoints used this same encoding
// with the full Workspace service list; an exact match permits safe adoption.
func sourceConfigurationHash(source Source, connection Connection) (string, error) {
	source.Alias = ""
	source.Streams = nil // Search/producer bindings do not change acquired bytes.
	source.PermissionTTLSeconds = 0
	source.Identity = Identity{} // Row keying, not acquisition; see IdentityKey.
	identity, err := json.Marshal(struct {
		Source     Source
		Connection Connection
	}{source, connection})
	if err != nil {
		return "", fmt.Errorf("encode source configuration: %w", err)
	}
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:]), nil
}

func (r *Runner) syncSource(ctx context.Context, s Source, connection Connection) (started int64, resultRevision string, failure error) {
	phase := "load_state"
	defer func() {
		if failure != nil {
			failure = &sourceSyncError{sourceID: s.ID, phase: phase, cause: failure}
		}
	}()
	statePath := filepath.Join(r.StateDir, s.ID+".json")
	identityConnection := connection
	if connection.Provider == ProviderGoogleWorkspace {
		identityConnection.Services = nil
	}
	hash, err := sourceConfigurationHash(s, identityConnection)
	if err != nil {
		return 0, "", err
	}
	state := runState{Version: 1, Endpoint: r.Profile.Endpoint, Table: s.Table, SourceID: s.ID, ConfigHash: hash, Owned: map[string]string{}}
	var persisted runState
	if err := loadJSON(statePath, &persisted, 256<<20); err == nil {
		if persisted.Version != 1 || persisted.Endpoint != state.Endpoint || persisted.Table != s.Table || persisted.SourceID != s.ID {
			return 0, "", fmt.Errorf("source state belongs to another destination")
		}
		if persisted.RecordFormat != 0 && persisted.RecordFormat != 2 {
			return 0, "", fmt.Errorf("unsupported source record format")
		}
		state = persisted
		if connection.Provider == ProviderGoogleWorkspace && state.ConfigHash != hash {
			legacyHash, err := sourceConfigurationHash(s, connection)
			if err != nil {
				return 0, "", err
			}
			if state.ConfigHash == legacyHash {
				// The full old configuration is unchanged. Preserve its inventory,
				// cursors and hydration namespace while adopting normalized identity.
				state.ConfigHash = hash
			}
		}
		if state.ConfigHash != hash {
			if state.Pending != nil && !r.Restart {
				return 0, "", fmt.Errorf("source configuration changed during a pinned run; use --restart")
			}
			state.Pending = nil
			state.ConfigHash = hash
			state.Published = nil
			state.AcquisitionID = ""
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, "", err
	}
	if state.Owned == nil {
		state.Owned = map[string]string{}
	}
	if len(state.Owned) > maxOwnedSourceObjects {
		return 0, "", fmt.Errorf("source ownership exceeds budget")
	}
	request := Request{SourceNamespace: "job/" + s.ID, Config: s.Config, Mode: ImportModeSource}
	for id := range state.Owned {
		if !ownedID(id, request.SourceNamespace) {
			return 0, "", fmt.Errorf("invalid persisted ownership identity")
		}
	}
	journalPath := statePath + ".owned"
	if err := replayOwnership(journalPath, request.SourceNamespace, state.Owned); err != nil {
		return 0, "", err
	}
	phase = "connect"
	connector, err := r.Factory(ctx, connection)
	if err != nil {
		return 0, "", err
	}
	connector, err = publicationConnector(connector, s)
	if err != nil {
		return 0, "", err
	}
	if c, ok := connector.(*CollaborationConnector); ok {
		// Pool immutable provider transports/token sources, never per-acquisition
		// cursors, scope caches, or hydration state across logical sources.
		run := CollaborationConnector{
			ProviderName: c.ProviderName, HTTPClient: c.HTTPClient, AccessToken: c.AccessToken,
			Services: c.Services, APIBase: c.APIBase, GmailBase: c.GmailBase, CalendarBase: c.CalendarBase,
			DriveBase: c.DriveBase, Extractor: c.Extractor, DownloadTransport: c.DownloadTransport,
			NativeAttachments: c.NativeAttachments, ExtractorVersion: c.ExtractorVersion, accessAuditWindow: c.accessAuditWindow, accessWait: c.accessWait,
		}
		c, connector = &run, &run
		c.Baseline = state.Published
		if r.Restart {
			c.Baseline = nil
		}
		if state.AcquisitionID == "" || r.Restart {
			state.AcquisitionID = rand.Text()
			if err := writeJSON(statePath, state, 256<<20); err != nil {
				return 0, "", err
			}
		}
		c.Cache = &FileAcquisitionCache{Directory: filepath.Join(r.StateDir, "acquisition", s.ID, acquisitionKey(state.AcquisitionID))}
		c.ContentStore = &FileAcquisitionContentStore{Directory: filepath.Join(r.StateDir, "content", s.ID)}
	}
	if r.Restart {
		state.Pending = nil
	}
	phase = "enumerate"
	if state.Pending == nil {
		enumerationStarted := time.Now().Unix()
		inventory, err := connector.Enumerate(ctx, request)
		if err != nil {
			return 0, "", err
		}
		if !inventory.Authoritative || len(inventory.Objects) > sourceInventoryLimit(connection.Provider) {
			return 0, "", fmt.Errorf("complete authoritative inventory required")
		}
		seen := map[string]bool{}
		for _, item := range inventory.Objects {
			if !ownedID(item.ID, request.SourceNamespace) || seen[item.ID] || item.Revision == "" || item.SourceKind != connection.Provider {
				return 0, "", fmt.Errorf("invalid/duplicate pinned source identity")
			}
			seen[item.ID] = true
		}
		for _, d := range inventory.Diagnostics {
			if d.Level == "error" {
				return 0, "", fmt.Errorf("inventory is incomplete: %s", d.Code)
			}
		}
		state.Pending = &inventory
		state.StartedAt = enumerationStarted
		if err := writeJSON(statePath, state, 256<<20); err != nil {
			return 0, "", err
		}
	}
	if !state.Pending.Authoritative || len(state.Pending.Objects) > sourceInventoryLimit(connection.Provider) {
		return 0, "", fmt.Errorf("invalid persisted inventory authority")
	}
	seenPending := map[string]bool{}
	for _, item := range state.Pending.Objects {
		if !ownedID(item.ID, request.SourceNamespace) || seenPending[item.ID] || item.Revision == "" || item.SourceKind != connection.Provider {
			return 0, "", fmt.Errorf("invalid persisted inventory identity")
		}
		seenPending[item.ID] = true
	}
	phase = "validate_inventory"
	if err := connector.ValidateInventory(ctx, request, *state.Pending); err != nil {
		return 0, "", err
	}
	if err := writeJSON(statePath, state, 256<<20); err != nil {
		return 0, "", err
	}
	if r.MediaInventory != nil && s.Publication != "text" {
		if err := r.MediaInventory(ctx, s, state.Pending, state.Published); err != nil {
			return 0, "", err
		}
	}
	if err := resetOwnershipJournal(journalPath); err != nil {
		return 0, "", err
	}
	if err := r.Publisher.EnsureTable(ctx, s.Table); err != nil {
		return 0, "", err
	}
	writer, err := NewSyncWriterWithIdentity(r.Publisher, s.Table, s.ID, s.Identity.Namespace, state.Pending.Objects, state.Pending.Authoritative)
	if err != nil {
		return 0, "", err
	}
	legacy := []string{}
	if state.RecordFormat == 0 {
		for id := range state.Owned {
			legacy = append(legacy, id)
		}
		sort.Strings(legacy)
	}
	phase = "publish"
	selected := map[string]bool{}
	var total int64
	for _, item := range state.Pending.Objects {
		if err := ctx.Err(); err != nil {
			return 0, "", err
		}
		selected[item.ID] = true
		item.Metadata = cloneSourceMetadata(item.Metadata)
		item.Metadata["connection_id"] = s.Connection
		var doc Document
		if UsesNativeResource(s, item) {
			if r.MediaReference == nil {
				return 0, "", fmt.Errorf("native binary publication requires a source media service")
			}
			phase = "verify_media"
			if !r.mediaReferenceAuthorizes {
				if err := connector.CheckAccess(ctx, request, item); err != nil {
					return 0, "", err
				}
			}
			ref, err := r.MediaReference(ctx, s, request, item)
			if err != nil {
				return 0, "", err
			}
			doc, err = NormalizeMedia(s.ID, item, ref, *s.Permissions, time.Now())
			if err != nil {
				return 0, "", err
			}
		} else {
			phase = "read_content"
			data, err := connector.Read(ctx, request, item)
			if err != nil {
				return 0, "", err
			}
			total += int64(len(data))
			if total > 10<<30 {
				return 0, "", fmt.Errorf("source run exceeds 10 GiB")
			}
			doc, err = Normalize(s.ID, item, data, *s.Permissions, time.Now())
			if err != nil {
				return 0, "", err
			}
		}

		phase = "publish"
		if _, exists := state.Owned[item.ID]; !exists && len(state.Owned) >= maxOwnedSourceObjects {
			return 0, "", fmt.Errorf("source ownership exceeds budget; complete reconciliation before selecting more items")
		}
		if err := journalOwnership(journalPath, ownershipEvent{ID: item.ID, Revision: item.Revision}); err != nil {
			return 0, "", err
		}
		state.Owned[item.ID] = item.Revision
		if len(state.Owned) > maxOwnedSourceObjects {
			return 0, "", fmt.Errorf("source ownership exceeds budget")
		}
		key, err := IdentityKey(s.ID, s.Identity.Namespace, relativeKeyFor(s.ID, item), item.ID)
		if err != nil {
			return 0, "", err
		}
		if _, err := writer.Upsert(ctx, map[string]Document{key: doc}); err != nil {
			return 0, "", err
		}
	}
	absent := []string{}
	for id := range state.Owned {
		if !selected[id] {
			absent = append(absent, id)
		}
	}
	sort.Strings(absent)
	phase = "validate_absence"
	if err := connector.ValidateAbsentSources(ctx, request, absent); err != nil {
		return 0, "", err
	}
	phase = "reconcile_deletions"
	if _, err := writer.Finish(ctx); err != nil {
		return 0, "", err
	}
	if len(legacy) > 0 {
		if err := writer.RetireLegacy(ctx, legacy); err != nil {
			return 0, "", err
		}
	}
	for start := 0; start < len(absent); start += 100 {
		batch := absent[start:min(start+100, len(absent))]
		if err := journalOwnership(journalPath, ownershipEvent{Delete: batch}); err != nil {
			return 0, "", err
		}
		for _, id := range batch {
			delete(state.Owned, id)
		}
	}
	revision := state.Pending.SourceRevision
	if revision == "" {
		raw, _ := json.Marshal(state.Pending)
		digest := sha256.Sum256(raw)
		revision = hex.EncodeToString(digest[:])
	}
	_, isCollaboration := connector.(*CollaborationConnector)
	_, isDriveMedia := connector.(*driveMediaConnector)
	if isCollaboration || isDriveMedia {
		phase = "audit_permissions"
		ttl := s.PermissionTTLSeconds
		if ttl == 0 {
			ttl = 900
		}
		checkedAt := time.Now()
		auditCtx, cancel := context.WithTimeout(ctx, time.Duration(ttl)*time.Second)
		err := connector.ValidateInventory(auditCtx, request, *state.Pending)
		cancel()
		if err != nil {
			return 0, "", err
		}
		if time.Since(checkedAt) >= time.Duration(ttl)*time.Second {
			return 0, "", fmt.Errorf("%w: source permission audit exceeded lease duration", context.DeadlineExceeded)
		}
		// Every scope in this audit shares its earliest possible check time.
		// Slow audits must never extend the advertised maximum access age.
		state.StartedAt = checkedAt.Unix()
	}
	phase = "commit_state"
	state.Published = state.Pending
	state.Pending = nil
	state.AcquisitionID = ""
	state.RecordFormat = 2
	if err := writeJSON(statePath, state, 256<<20); err != nil {
		return 0, "", err
	}
	if r.MediaInventory != nil && s.Publication != "text" {
		if err := r.MediaInventory(ctx, s, nil, state.Published); err != nil {
			return 0, "", err
		}
	}
	if err := resetOwnershipJournal(journalPath); err != nil {
		return 0, "", err
	}
	if c, ok := connector.(*CollaborationConnector); ok {
		if cache, ok := c.Cache.(*FileAcquisitionCache); ok {
			if err := os.RemoveAll(cache.Directory); err != nil {
				return 0, "", err
			}
		}
	}
	if c, ok := connector.(*CollaborationConnector); ok {
		if content, ok := c.ContentStore.(*FileAcquisitionContentStore); ok {
			if err := content.Prune(state.Published.Objects); err != nil {
				return 0, "", err
			}
		}
	}
	return state.StartedAt, revision, nil
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

// Drive media uses the same inventory, access checks and revision-checked binary
// reads as GoogleConnector, bypassing collaboration's text/OCR extraction.
func publicationConnector(connector Connector, source Source) (Connector, error) {
	if source.Publication == "text" {
		return connector, nil
	}
	if c, ok := connector.(*GoogleConnector); ok && c.ProviderName == ProviderGoogleDrive {
		return &driveMediaConnector{GoogleConnector: c, sourceKind: c.ProviderName}, nil
	}
	if c, ok := connector.(*CollaborationConnector); ok {
		config, err := decodeSourceConfig[CollaborationConfig](source.Config)
		if err != nil {
			return nil, err
		}
		if c.ProviderName != ProviderGoogleWorkspace || config.Resource != "drive" {
			if source.Publication == "media" {
				return nil, fmt.Errorf("mixed collaboration sources require native publication")
			}
			copy := *c
			copy.NativeAttachments = true
			return &copy, nil
		}
		if err := c.Validate(context.Background(), Request{Mode: ImportModeSource, Config: source.Config}); err != nil {
			return nil, err
		}
		return &driveMediaConnector{GoogleConnector: c.drive(), sourceKind: ProviderGoogleWorkspace}, nil
	}
	return connector, nil
}

type driveMediaConnector struct {
	*GoogleConnector
	sourceKind string
}

// A materialized text snapshot remains usable after Drive changes. A media
// reference does not: both acquisition and publication audits must prove that
// the pinned revision can still be fetched, including when a digest is cached.
func (c *driveMediaConnector) ValidateInventory(ctx context.Context, request Request, inventory Inventory) error {
	if err := c.ValidateCachedSnapshot(ctx, request, inventory); err != nil {
		return err
	}
	for _, item := range inventory.Objects {
		if err := c.CheckMedia(ctx, request, item); err != nil {
			return err
		}
	}
	return nil
}

func (c *driveMediaConnector) Enumerate(ctx context.Context, request Request) (Inventory, error) {
	inventory, err := c.GoogleConnector.Enumerate(ctx, request)
	if err != nil {
		return inventory, err
	}
	for i := range inventory.Objects {
		item := &inventory.Objects[i]
		if c.sourceKind != "" {
			item.SourceKind = c.sourceKind
		}
		if configString(item.Metadata, "export_mime_type") == "" {
			continue
		}
		original := configString(item.Metadata, "mime_type")
		switch original {
		case "application/vnd.google-apps.document", "application/vnd.google-apps.presentation":
		default:
			return Inventory{}, fmt.Errorf("unsupported native Drive export %s", original)
		}
		item.Metadata = cloneSourceMetadata(item.Metadata)
		item.Metadata["original_mime_type"] = original
		item.Metadata["export_mime_type"] = "application/pdf"
		item.Metadata["mime_type"] = "application/pdf"
		if request.Preview {
			continue
		} // Review descriptors without exporting originals.
		data, err := c.GoogleConnector.Read(ctx, request, *item)
		if err != nil {
			return Inventory{}, err
		}
		item.Size = int64(len(data))
		item.Metadata["size_is_upper_bound"] = false
		item.Metadata["representation_sha256"] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return inventory, nil
}

func UsesNativeResource(source Source, item Item) bool {
	if source.Publication == "media" {
		return true
	}
	if source.Publication == "text" {
		return false
	}
	if item.SourceKind != ProviderS3 && item.SourceKind != ProviderLocal && item.SourceKind != ProviderGCS && item.SourceKind != ProviderGoogleDrive && item.SourceKind != ProviderGoogleWorkspace && configString(item.Metadata, "attachment_id") == "" {
		return false
	}
	if configString(item.Metadata, "acquisition") != "" && configString(item.Metadata, "attachment_id") == "" {
		return false
	}
	typ := strings.ToLower(strings.Split(MediaMIME(item), ";")[0])
	return !strings.HasPrefix(typ, "text/") && typ != "application/json" && typ != "application/xml"
}

// PublicationConnector selects original-binary acquisition for media sources.
// Provider authentication and resource allowlists remain the runtime's job.
func PublicationConnector(connector Connector, source Source) (Connector, error) {
	return publicationConnector(connector, source)
}
