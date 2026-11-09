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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Service is a single-operator source companion. Deploy one per credential trust
// boundary. It reuses Runner and exposes scoped media capabilities to Antfly.
type Service struct {
	knowledgeAudience         string
	ctx                       context.Context
	stop                      context.CancelFunc
	clients                   providerClients
	authorities               mediaAuthorities
	mu                        sync.Mutex
	profile                   Profile
	mediaRegistry             map[string]mediaRegistrySource
	connectionBindings        map[string]string
	directory, baseURL, token string
	mediaKey                  []byte
	factory                   Factory
	lock                      *flock.Flock
	cancel                    context.CancelFunc
	done                      chan struct{}
	job                       *SyncJob
	closed                    bool
	fetchSlots                chan struct{}
}

type ServiceOptions struct {
	// KnowledgeAudience enables only deployments isolated to one audience across all native data and APIs.
	KnowledgeAudience string
	Profile           Profile
	StateDir          string
	// PublicURL is a stable address reachable by Antfly, including after restart.
	PublicURL  string
	AdminToken string
	// MediaKey must be stable, at least 32 bytes, and different from AdminToken.
	MediaKey []byte
	Factory  Factory
}

type SyncJob struct {
	SourceID    string `json:"source_id,omitempty"`
	PreviousID  string `json:"previous_id,omitempty"`
	RequestKey  string `json:"request_key,omitempty"`
	RequestHash string `json:"request_hash,omitempty"`

	Version           int          `json:"version"`
	Operation         string       `json:"operation,omitempty"`
	PurgedSourceCount int          `json:"purged_source_count,omitempty"`
	ID                string       `json:"id"`
	State             string       `json:"state"`
	StartedAt         int64        `json:"started_at"`
	FinishedAt        int64        `json:"finished_at,omitempty"`
	Code              string       `json:"code,omitempty"`
	Failure           *SyncFailure `json:"failure,omitempty"`
}

func NewService(o ServiceOptions) (*Service, error) {
	if len(o.AdminToken) < 32 || strings.ContainsAny(o.AdminToken, "\r\n") || len(o.MediaKey) < 32 || o.StateDir == "" || hmac.Equal([]byte(o.AdminToken), o.MediaKey) {
		return nil, fmt.Errorf("source service requires state-dir, a 32-byte admin token and a separate 32-byte media key")
	}
	if err := ValidateEndpoint(o.PublicURL); err != nil {
		return nil, err
	}
	s := &Service{knowledgeAudience: o.KnowledgeAudience, profile: cloneProfile(o.Profile), directory: o.StateDir, baseURL: strings.TrimRight(o.PublicURL, "/"), token: o.AdminToken, mediaKey: bytes.Clone(o.MediaKey), factory: o.Factory, fetchSlots: make(chan struct{}, 4)}
	s.ctx, s.stop = context.WithCancel(context.Background())
	if s.factory == nil {
		s.factory = CredentialFactory(s.resolveSecret)
	}
	if err := os.MkdirAll(s.directory, 0700); err != nil {
		return nil, err
	}
	s.lock = flock.New(filepath.Join(s.directory, "service.lock"))
	locked, err := s.lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, fmt.Errorf("source service state is locked")
	}
	success := false
	defer func() {
		if !success {
			_ = s.lock.Unlock()
		}
	}()
	if err := s.checkBootstrapKey(); err != nil {
		return nil, err
	}
	var persisted Profile
	if err := loadJSON(s.profilePath(), &persisted, 1<<20); err == nil {
		if persisted.Endpoint != o.Profile.Endpoint {
			return nil, fmt.Errorf("service state belongs to another endpoint")
		}
		s.profile = persisted
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if s.profile.Connections == nil {
		s.profile.Connections = map[string]Connection{}
	}
	if s.profile.Sources == nil {
		s.profile.Sources = []Source{}
	}
	if err := s.recoverPurges(); err != nil {
		return nil, err
	}
	// Revoked connections may leave source definitions behind. They remain inert
	// until reconnected; their durable ownership must survive a restart.
	validation := cloneProfile(s.profile)
	for _, source := range validation.Sources {
		if _, ok := validation.Connections[source.Connection]; !ok {
			validation.Connections[source.Connection] = Connection{}
		}
	}
	if err := s.validateProfile(validation); err != nil {
		return nil, err
	}
	if err := writeJSON(s.profilePath(), s.profile, 1<<20); err != nil {
		return nil, err
	}
	var job SyncJob
	if err := loadJSON(s.jobPath(), &job, 4096); err == nil {
		if job.State == "running" {
			job.State, job.Code, job.FinishedAt = "interrupted", "service_restarted", time.Now().Unix()
			if err := writeJSON(s.jobPath(), job, 4096); err != nil {
				return nil, err
			}
		}
		s.job = &job
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := s.recoverMediaAuthorities(); err != nil {
		return nil, err
	}
	s.refreshMediaRegistry()
	success = true
	return s, nil
}
func (s *Service) profilePath() string { return filepath.Join(s.directory, "profile.json") }
func (s *Service) jobPath() string     { return filepath.Join(s.directory, "job.json") }
func (s *Service) runnerPath() string  { return filepath.Join(s.directory, "runner") }
func (s *Service) validateProfile(p Profile) error {
	// Revocation is a valid persisted state. Validate structure independently
	// of current runnability so unrelated repairs cannot deadlock each other.
	p = cloneProfile(p)
	for name := range p.Connections {
		if !validConnectionName(name) {
			return fmt.Errorf("invalid connection name")
		}
	}
	for _, source := range p.Sources {
		if !validConnectionName(source.Connection) {
			return fmt.Errorf("invalid source connection name")
		}
		if _, ok := p.Connections[source.Connection]; !ok {
			p.Connections[source.Connection] = Connection{}
		}
	}
	r := Runner{KnowledgeAudience: s.knowledgeAudience, Profile: p, StateDir: s.runnerPath(), CatalogPath: filepath.Join(s.runnerPath(), "catalog.json"), MediaReference: s.pinMedia}
	if err := r.validate(); err != nil {
		return err
	}
	for _, source := range p.Sources {
		provider := p.Connections[source.Connection].Provider
		if source.Publication == "media" && provider != "" && provider != ProviderS3 && provider != ProviderLocal && provider != ProviderGCS && provider != ProviderGoogleWorkspace {
			return fmt.Errorf("media publication supports S3, GCS, local and Drive sources")
		}
	}
	return nil
}
func (s *Service) Close() error {
	s.mu.Lock()
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	done := s.done
	s.mu.Unlock()
	s.stop()
	if done != nil {
		<-done
	}
	s.closeClients()
	return s.lock.Unlock()
}
func serviceJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func serviceError(w http.ResponseWriter, status int, code string) {
	serviceJSON(w, status, map[string]any{"version": 1, "code": code})
}
func serviceDecode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON value")
	}
	return nil
}
func validConnectionName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func cloneProfile(p Profile) Profile {
	b, _ := json.Marshal(p)
	var out Profile
	_ = json.Unmarshal(b, &out)
	return out
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v1/media/") {
		s.serveMedia(w, r)
		return
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || !hmac.Equal([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(s.token)) {
		serviceError(w, 401, "unauthorized")
		return
	}
	if r.URL.Path == "/sources/v1" || strings.HasPrefix(r.URL.Path, "/sources/v1/") {
		s.serveManagement(w, r)
		return
	}
	var adoption *namespaceAdoptionCheck
	// Authentication happens before decoding, but no service lock is held
	// while a caller sends its request body or a provider builds a client.
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		var raw json.RawMessage
		if err := serviceDecode(w, r, &raw); err != nil || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
			serviceError(w, 400, "invalid_request")
			return
		}
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/connections/") {
			var candidate Connection
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if d.Decode(&candidate) != nil {
				serviceError(w, 400, "invalid_connection")
				return
			}
			if _, err := s.providerConnector(r.Context(), candidate); err != nil {
				serviceError(w, 400, "invalid_connection")
				return
			}
		}
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/sources/") {
			var candidate Source
			if json.Unmarshal(raw, &candidate) == nil && canonicalID(candidate.ID) {
				var state runState
				err := loadJSON(filepath.Join(s.runnerPath(), candidate.ID+".json"), &state, 256<<20)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					serviceError(w, 500, "state_read_failed")
					return
				}
				if err == nil && (state.SourceID != candidate.ID || state.Table != candidate.Table) {
					serviceError(w, 409, "source_destination_immutable")
					return
				}
				if candidate.ID == strings.TrimPrefix(r.URL.Path, "/v1/sources/") && candidate.Identity.Namespace != "" {
					adoption = s.prepareNamespaceAdoption(r.Context(), candidate.ID, candidate.Identity.Namespace)
					if r.Context().Err() != nil {
						serviceError(w, http.StatusRequestTimeout, "request_cancelled")
						return
					}
				}
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
	}
	// Serialize state transitions, then send the bounded response after unlocking.
	output := &controlResponse{header: make(http.Header)}
	destination := w
	defer func() { output.flush(destination) }()
	w = output
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		serviceError(w, 503, "service_closed")
		return
	}
	if r.URL.Path == "/v1/profile" && r.Method == http.MethodGet {
		w.Header().Set("X-Antfly-Source-Preconditions", "2")
		serviceJSON(w, 200, s.profile)
		return
	}
	// Migration fences source and connection changes in the same critical
	// section as PUT/sync admission. No provider I/O is needed for this check.
	if !migrationConnectionsMatch(s.profile, r.Header.Get("X-Antfly-Migration-Connections")) {
		serviceError(w, http.StatusPreconditionFailed, "migration_connection_revision_changed")
		return
	}
	if r.URL.Path == "/v1/sync" && r.Method == http.MethodPost {
		var input struct {
			RequestKey string `json:"request_key"`
			SourceID   string `json:"source_id"`
			Restart    bool   `json:"restart"`
			PurgeOnly  bool   `json:"purge_only"`
		}
		if err := serviceDecode(w, r, &input); err != nil {
			serviceError(w, 400, "invalid_request")
			return
		}
		if condition := r.Header.Get("If-Match"); condition != "" {
			matches := false
			for _, source := range s.profile.Sources {
				if source.ID == input.SourceID && sourceRevision(source) == condition {
					matches = true
					break
				}
			}
			if !matches || input.PurgeOnly {
				serviceError(w, http.StatusPreconditionFailed, "source_revision_changed")
				return
			}
		}
		if err := s.validateProfile(s.profile); err != nil {
			serviceError(w, 400, "invalid_source_profile")
			return
		}
		if input.RequestKey != "" {
			old, found, err := s.idempotentJob(input.RequestKey)
			if err != nil {
				serviceError(w, 500, "state_read_failed")
				return
			}
			if found {
				if old.RequestHash != jobRequestHash(input.SourceID, input.Restart, input.PurgeOnly) {
					serviceError(w, 409, "idempotency_conflict")
					return
				}
				serviceJSON(w, 202, old)
				return
			}
		}
		if s.cancel != nil {
			serviceError(w, 409, "sync_in_progress")
			return
		}
		queue, err := s.readPurges()
		if err != nil {
			serviceError(w, 500, "state_read_failed")
			return
		}
		if input.SourceID != "" {
			found := false
			if input.PurgeOnly {
				for _, source := range queue.Sources {
					if source.ID == input.SourceID {
						found = true
						break
					}
				}
			} else {
				for _, source := range s.profile.Sources {
					if source.ID == input.SourceID {
						found = true
						break
					}
				}
			}
			if !found {
				serviceError(w, 404, "source_not_found")
				return
			}
		}
		if !input.PurgeOnly && len(queue.Sources) == 0 {
			for _, source := range s.profile.Sources {
				if input.SourceID != "" && source.ID != input.SourceID {
					continue
				}
				if _, exists := s.profile.Connections[source.Connection]; !exists {
					serviceError(w, 400, "source_connection_revoked")
					return
				}
			}
		}
		profile := cloneProfile(s.profile)
		ctx, cancel := context.WithCancel(context.Background())
		operation := "sync"
		if input.PurgeOnly {
			operation = "purge"
		}
		previous := ""
		if s.job != nil {
			// Archive before replacing the latest receipt. This also migrates the
			// legacy singleton receipt without losing its terminal outcome.
			if err := writeJSON(s.historyPath(s.job.ID), s.job, 4096); err != nil {
				cancel()
				serviceError(w, 500, "state_write_failed")
				return
			}
			if s.job.RequestKey != "" {
				if err := writeJSON(s.requestPath(s.job.RequestKey), map[string]string{"id": s.job.ID}, 4096); err != nil {
					cancel()
					serviceError(w, 500, "state_write_failed")
					return
				}
			}
			previous = s.job.ID
		}
		job := SyncJob{Version: 1, SourceID: input.SourceID, PreviousID: previous, RequestKey: input.RequestKey, RequestHash: jobRequestHash(input.SourceID, input.Restart, input.PurgeOnly), Operation: operation, ID: rand.Text(), State: "running", StartedAt: time.Now().Unix()}
		if err := writeJSON(s.jobPath(), job, 4096); err != nil {
			cancel()
			serviceError(w, 500, "state_write_failed")
			return
		}
		s.job, s.cancel, s.done = &job, cancel, make(chan struct{})
		go s.run(ctx, profile, input.Restart, input.PurgeOnly, s.done, input.SourceID)
		w.Header().Set("Location", "/v1/jobs/"+job.ID)
		serviceJSON(w, 202, job)
		return
	}
	if r.URL.Path == "/v1/jobs" && r.Method == http.MethodGet {
		if s.job == nil {
			serviceError(w, 404, "job_not_found")
			return
		}
		serviceJSON(w, 200, s.job)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/jobs/") {
		if s.job == nil || strings.TrimPrefix(r.URL.Path, "/v1/jobs/") != s.job.ID {
			serviceError(w, 404, "job_not_found")
			return
		}
		switch r.Method {
		case http.MethodGet:
			serviceJSON(w, 200, s.job)
		case http.MethodDelete:
			if s.cancel != nil {
				s.cancel()
			}
			serviceJSON(w, 202, s.job)
		default:
			serviceError(w, 405, "method_not_allowed")
		}
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/secrets/") {
		key := strings.TrimPrefix(r.URL.Path, "/v1/secrets/")
		if !validSecretKey(key) {
			serviceError(w, 400, "invalid_secret_key")
			return
		}
		switch r.Method {
		case http.MethodPut:
			if s.cancel != nil {
				serviceError(w, 409, "sync_in_progress")
				return
			}
			var input struct {
				Value string `json:"value"`
			}
			if err := serviceDecode(w, r, &input); err != nil || input.Value == "" || len(input.Value) > 128<<10 {
				serviceError(w, 400, "invalid_secret")
				return
			}
			if err := s.putSecret(key, input.Value); err != nil {
				serviceError(w, 500, "state_write_failed")
				return
			}
		case http.MethodDelete:
			if err := removeSourceSecret(s.secretPath(key)); err != nil {
				serviceError(w, 500, "state_write_failed")
				return
			}
			s.invalidateSecret(key)
			s.cancelSecretAcquisition(key)
		default:
			serviceError(w, 405, "method_not_allowed")
			return
		}
		serviceJSON(w, 200, map[string]any{"version": 1, "state": "updated"})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/connections/") {
		name := strings.TrimPrefix(r.URL.Path, "/v1/connections/")
		if !validConnectionName(name) {
			serviceError(w, 400, "invalid_connection_name")
			return
		}
		if r.Method == http.MethodDelete {
			if _, ok := s.profile.Connections[name]; !ok {
				serviceError(w, 404, "connection_not_found")
				return
			}
			next := cloneProfile(s.profile)
			delete(next.Connections, name)
			if err := writeJSON(s.profilePath(), next, 1<<20); err != nil {
				serviceError(w, 500, "state_write_failed")
				return
			}
			oldConnection := s.profile.Connections[name]
			s.profile = next
			s.refreshMediaRegistry()
			s.retireConnection(oldConnection)
			s.cancelConnectionAcquisition(name)
			serviceJSON(w, 200, map[string]any{"version": 1, "state": "revoked"})
			return
		}
		if r.Method != http.MethodPut {
			serviceError(w, 405, "method_not_allowed")
			return
		}
		if s.cancel != nil {
			serviceError(w, 409, "sync_in_progress")
			return
		}
		var connection Connection
		if err := serviceDecode(w, r, &connection); err != nil {
			serviceError(w, 400, "invalid_connection")
			return
		}
		next := cloneProfile(s.profile)
		next.Connections[name] = connection
		if err := s.validateProfile(next); err != nil {
			serviceError(w, 400, "invalid_source_profile")
			return
		}
		if err := writeJSON(s.profilePath(), next, 1<<20); err != nil {
			serviceError(w, 500, "state_write_failed")
			return
		}
		oldConnection := s.profile.Connections[name]
		s.profile = next
		s.refreshMediaRegistry()
		s.retireConnection(oldConnection)
		serviceJSON(w, 200, connection)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/sources/") {
		if s.cancel != nil {
			serviceError(w, 409, "sync_in_progress")
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/v1/sources/")
		if !canonicalID(id) {
			serviceError(w, 400, "invalid_source_id")
			return
		}
		next := cloneProfile(s.profile)
		index := -1
		for i := range next.Sources {
			if next.Sources[i].ID == id {
				index = i
				break
			}
		}
		rekeys := false
		switch r.Method {
		case http.MethodPut:
			queue, err := s.readPurges()
			if err != nil {
				serviceError(w, 500, "state_read_failed")
				return
			}
			for _, pending := range queue.Sources {
				if pending.ID == id {
					serviceError(w, 409, "source_purge_pending")
					return
				}
			}
			var source Source
			if err := serviceDecode(w, r, &source); err != nil || source.ID != id {
				serviceError(w, 400, "invalid_source")
				return
			}
			if condition := r.Header.Get("If-Match"); condition != "" && (index < 0 || condition != sourceRevision(next.Sources[index])) {
				serviceError(w, http.StatusPreconditionFailed, "source_revision_changed")
				return
			}
			if adoption != nil && (index < 0 || !adoption.current(s, next.Sources[index], source.Identity.Namespace)) {
				serviceError(w, http.StatusPreconditionFailed, "namespace_adoption_revision_changed")
				return
			}
			if index < 0 {
				// New sources get an opaque namespace at creation so a later
				// connection or provider change never has to re-key anything:
				// nothing is ever keyed before the source has a namespace.
				if source.Identity.Namespace == "" {
					source.Identity.Namespace = id
				}
				next.Sources = append(next.Sources, source)
			} else {
				if next.Sources[index].Table != source.Table {
					serviceError(w, 409, "source_destination_immutable")
					return
				}
				existing := next.Sources[index]
				if source.Knowledge == nil {
					source.Knowledge = existing.Knowledge
				}
				if source.Processing == nil {
					source.Processing = existing.Processing
				}
				switch {
				case source.Identity.Namespace == "":
					// Omitting identity on an update leaves it exactly as it
					// was; it is never cleared by omission.
					source.Identity.Namespace = existing.Identity.Namespace
				case existing.Identity.Namespace != "" && source.Identity.Namespace != existing.Identity.Namespace:
					serviceError(w, 409, "source_identity_namespace_immutable")
					return
				}
				if existing.Identity.Namespace == "" {
					switch {
					case source.Identity.Namespace == "":
						// No namespace involved at all: rows are still keyed by
						// raw acquisition identity (RecordKey's legacy formula),
						// which a connection or config change re-keys exactly as
						// it always has.
						rekeys = existing.Connection != source.Connection || !bytes.Equal(existing.Config, source.Config)
					default:
						// A namespace is being set for the first time, possibly
						// together with a connection/config change in the same
						// request (migrate-local does exactly this). It only
						// avoids a re-key if it is exactly the prefix that
						// reproduces this source's current keys under its
						// EXISTING connection/config -- before this request's own
						// change, if any -- checked for real rather than assumed.
						if adoption == nil {
							serviceError(w, http.StatusPreconditionFailed, "namespace_adoption_revision_changed")
							return
						}
						rekeys = !adoption.matches
					}
				}
				next.Sources[index] = source
			}
			if raw := r.Header.Get("X-Antfly-Local-Migration"); raw != "" {
				if r.Header.Get("If-Match") == "" || r.Header.Get("X-Antfly-Migration-Connections") == "" {
					serviceError(w, 400, "migration_preconditions_required")
					return
				}
				if err := applyMigrationReceipt(s.profile, &next, source, raw); err != nil {
					serviceError(w, http.StatusPreconditionFailed, "migration_lineage_mismatch")
					return
				}
			}

		case http.MethodDelete:
			if index < 0 && r.URL.Query().Get("purge") == "true" {
				// A retained source can be explicitly purged after its definition
				// has already been removed. Durable destination identity is authoritative.
				queue, err := s.readPurges()
				if err != nil {
					serviceError(w, 500, "state_read_failed")
					return
				}
				for _, pending := range queue.Sources {
					if pending.ID == id {
						serviceJSON(w, 200, map[string]any{"version": 1, "state": "purge_pending"})
						return
					}
				}
				var state runState
				if err := loadJSON(filepath.Join(s.runnerPath(), id+".json"), &state, 256<<20); err != nil {
					if errors.Is(err, os.ErrNotExist) {
						serviceError(w, 404, "source_not_found")
					} else {
						serviceError(w, 500, "state_read_failed")
					}
					return
				}
				if state.SourceID != id || state.Endpoint != s.profile.Endpoint || state.Table == "" {
					serviceError(w, 409, "source_destination_mismatch")
					return
				}
				if err := s.queuePurge(Source{ID: id, Table: state.Table}); err != nil {
					serviceError(w, 500, "state_write_failed")
					return
				}
				serviceJSON(w, 200, map[string]any{"version": 1, "state": "purge_pending"})
				return
			}
			if index < 0 {
				serviceError(w, 404, "source_not_found")
				return
			}
			purge := r.URL.Query().Get("purge")
			if purge != "" && purge != "true" && purge != "false" {
				serviceError(w, 400, "invalid_purge_option")
				return
			}
			if purge == "true" {
				if err := s.queuePurge(next.Sources[index]); err != nil {
					serviceError(w, 500, "state_write_failed")
					return
				}
			}
			next.Sources = append(next.Sources[:index], next.Sources[index+1:]...)
		default:
			serviceError(w, 405, "method_not_allowed")
			return
		}
		if err := s.validateProfile(next); err != nil {
			serviceError(w, 400, "invalid_source_profile")
			return
		}
		if err := writeJSON(s.profilePath(), next, 1<<20); err != nil {
			if r.Method == http.MethodDelete && r.URL.Query().Get("purge") == "true" {
				// The durable tombstone already committed. Keep in-memory
				// authority consistent even if the profile write now fails.
				s.profile = next
				s.refreshMediaRegistry()
			}
			serviceError(w, 500, "state_write_failed")
			return
		}
		s.profile = next
		s.refreshMediaRegistry()
		response := map[string]any{"version": 1, "state": "configured"}
		if r.Method == http.MethodPut {
			response["rekeys"] = rekeys
		}
		serviceJSON(w, 200, response)
		return
	}
	serviceError(w, 404, "not_found")
}

// Provider work runs outside s.mu. The result is advisory (failure reports
// a re-key), but its source/connection witnesses must still match at commit.
type namespaceAdoptionCheck struct {
	sourceRevision, connectionBinding, clientGeneration, namespace string
	connectionExists, matches                                      bool
}

// current runs under s.mu, after normal source preconditions are checked.
func (check namespaceAdoptionCheck) current(s *Service, source Source, namespace string) bool {
	connection, exists := s.profile.Connections[source.Connection]
	return check.namespace == namespace && check.sourceRevision == sourceRevision(source) &&
		check.connectionExists == exists && check.connectionBinding == connectionMediaBinding(connection) &&
		check.clientGeneration == s.clientGeneration(connection)
}

func (s *Service) prepareNamespaceAdoption(ctx context.Context, id, namespace string) *namespaceAdoptionCheck {
	s.mu.Lock()
	var existing Source
	found := false
	for _, source := range s.profile.Sources {
		if source.ID == id {
			existing, found = source, true
			break
		}
	}
	if !found || existing.Identity.Namespace != "" || s.closed || s.cancel != nil {
		s.mu.Unlock()
		return nil
	}
	connection, exists := s.profile.Connections[existing.Connection]
	check := &namespaceAdoptionCheck{sourceRevision: sourceRevision(existing), connectionBinding: connectionMediaBinding(connection), clientGeneration: s.clientGeneration(connection), namespace: namespace, connectionExists: exists}
	s.mu.Unlock()
	if !exists {
		return check
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	check.matches = s.namespaceReproducesExistingKeys(ctx, id, existing, connection, namespace)
	return check
}

// namespaceReproducesExistingKeys reports whether namespace is exactly the
// prefix that reproduces id's current row keys under existing's connection
// and config (LegacyNamespace), so adopting it as part of this PUT does not
// re-key anything. existing is the source's definition as persisted BEFORE
// this PUT, so a connection/config change arriving in the same request (as
// migrate-local's repoint does) is checked against what produced the
// CURRENT rows, not against the change about to take effect.
//
// Local identity is derived directly from configuration, even for an empty or
// missing store. Other connectors use a best-effort preview. Unavailable,
// empty, or inconsistent inventories cannot prove that persisted catalog keys
// match the proposed namespace; report a possible re-key in those cases.
func (s *Service) namespaceReproducesExistingKeys(ctx context.Context, id string, existing Source, connection Connection, namespace string) (matches bool) {
	defer func() {
		if recover() != nil {
			matches = false
		}
	}()
	if connection.Provider == ProviderLocal {
		legacy, err := localIdentityNamespace(Request{SourceNamespace: "job/" + id, Config: existing.Config})
		return err == nil && legacy == namespace
	}
	connector, err := s.providerConnector(ctx, connection)
	if err != nil {
		return false
	}
	if collaboration, ok := connector.(*CollaborationConnector); ok {
		connector = collaboration.newAcquisition()
	}

	inventory, err := connector.Enumerate(ctx, Request{SourceNamespace: "job/" + id, Config: existing.Config, Mode: ImportModeSource, Preview: true})
	if err != nil {
		return false
	}
	legacy, err := LegacyNamespace(id, inventory.Objects)
	if err != nil {
		return false
	}
	return legacy == namespace
}
func (s *Service) run(ctx context.Context, profile Profile, restart, purgeOnly bool, done chan struct{}, selection ...string) {
	defer close(done)
	sourceID := ""
	if len(selection) > 0 {
		sourceID = selection[0]
	}
	keep := map[string]bool{}
	resolver := func(ctx context.Context, source Source, request Request, item Item) (MediaReference, error) {
		ref, err := s.pinMedia(ctx, source, request, item)
		if err == nil {
			parts := strings.Split(ref.URI, "/")
			keep[parts[len(parts)-2]+".json"] = true
		}
		return ref, err
	}
	runner := Runner{KnowledgeAudience: s.knowledgeAudience, SourceID: sourceID, mediaReferenceAuthorizes: true, Profile: profile, StateDir: s.runnerPath(), CatalogPath: filepath.Join(s.runnerPath(), "catalog.json"), Factory: s.providerConnector, Restart: restart, MediaReference: resolver, MediaInventory: s.publishMediaAuthority}
	purged := []string{}
	err := runner.withStateLock(ctx, func() error {
		before, err := s.readPurges()
		if err != nil {
			return err
		}
		var purgeErr error
		if sourceID == "" || purgeOnly {
			purgeErr = s.purgeSourcesLocked(ctx, profile, sourceID)
		}
		after, err := s.readPurges()
		if err != nil {
			return err
		}
		remaining := map[string]bool{}
		for _, source := range after.Sources {
			remaining[source.ID] = true
		}
		for _, source := range before.Sources {
			if !remaining[source.ID] {
				purged = append(purged, source.ID)
			}
		}
		if purgeErr != nil {
			return purgeErr
		}
		if purgeOnly {
			return nil
		}
		for _, source := range profile.Sources {
			if sourceID != "" && source.ID != sourceID {
				continue
			}
			if _, ok := profile.Connections[source.Connection]; !ok {
				return &sourceSyncError{sourceID: source.ID, phase: "connect", cause: fmt.Errorf("%w: source connection revoked", ErrInvalidInput)}
			}
		}
		if err := runner.validate(); err != nil {
			return err
		}
		return runner.syncLocked(ctx)
	})
	var cleanupErr error
	if err == nil && !purgeOnly {
		if sourceID != "" {
			cleanupErr = s.pruneSourceMedia(keep, sourceID)
		} else {
			cleanupErr = s.pruneMediaPins(keep)
			if cleanupErr == nil {
				cleanupErr = s.pruneMediaDigests(keep)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel()
	s.cancel = nil
	s.job.State, s.job.FinishedAt = "completed", time.Now().Unix()
	s.job.PurgedSourceCount = len(purged)
	if cleanupErr != nil {
		s.job.Code = "media_cleanup_failed"
	}
	if err != nil {
		s.job.State, s.job.Code = "failed", "sync_failed"
		s.job.Failure = describeSyncFailure(err)
		slog.Error("source sync failed", "job_id", s.job.ID, "source_id", s.job.Failure.SourceID, "phase", s.job.Failure.Phase, "code", s.job.Failure.Code, "retryable", s.job.Failure.Retryable)
		if errors.Is(err, context.Canceled) {
			s.job.State, s.job.Code = "cancelled", "cancelled"
		}
	}
	if err := writeJSON(s.jobPath(), s.job, 4096); err != nil {
		s.job.State, s.job.Code = "failed", "state_write_failed"
	}
}

func (s *Service) pinPath(id string) string { return filepath.Join(s.directory, "media", id+".json") }
func (s *Service) mediaSignature(id string) string {
	mac := hmac.New(sha256.New, s.mediaKey)
	_, _ = mac.Write([]byte("antfly-source-media-v1\x00" + id))
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *Service) pinMedia(ctx context.Context, source Source, request Request, item Item) (MediaReference, error) {
	if item.Size < 1 {
		return MediaReference{}, fmt.Errorf("media requires a positive exact length")
	}
	if upper, _ := item.Metadata["size_is_upper_bound"].(bool); upper || (configString(item.Metadata, "export_mime_type") != "" && !validContentSHA(configString(item.Metadata, "representation_sha256"))) {
		return MediaReference{}, fmt.Errorf("media requires an original binary object with exact length")
	}
	item.ObjectIndex = 0
	item.Metadata = cloneSourceMetadata(item.Metadata)
	delete(item.Metadata, "object_index")
	s.mu.Lock()
	connection, exists := s.profile.Connections[source.Connection]
	s.mu.Unlock()
	if !exists {
		return MediaReference{}, fmt.Errorf("media connection revoked")
	}
	digest, err := s.verifiedMediaDigest(ctx, source, connection, request, item)
	if err != nil {
		return MediaReference{}, err
	}
	pin := mediaPinV2{Version: 2, mediaPinIdentity: mediaPinIdentity{SourceID: source.ID, SourceBinding: sourceMediaBinding(source), Connection: source.Connection, ConnectionBinding: connectionMediaBinding(connection), Item: item}, SHA256: digest, ContentIdentity: s.contentIdentity(connection, item)}
	id := pinIdentityID(pin.mediaPinIdentity)
	pin.MAC = s.pinReceiptMAC(pin)
	// The stable identity and signed receipt are immutable. Reusing a verified
	// descriptor avoids file replacement and two fsyncs per unchanged object.
	existing, readErr := s.readMediaPin(id)
	if readErr == nil && existing.MAC == pin.MAC {
		return s.mediaReference(id, pin, item), nil
	}
	if err := writeJSON(s.pinPath(id), pin, 1<<20); err != nil {
		return MediaReference{}, err
	}
	return s.mediaReference(id, pin, item), nil
}
func (s *Service) mediaReference(id string, pin mediaPinV2, item Item) MediaReference {
	return MediaReference{ContentSHA256: pin.SHA256, Type: "source", URI: s.baseURL + "/v1/media/" + id + "/" + s.mediaSignature(id), MIMEType: mediaMIME(item), Version: `"` + pin.SHA256 + `"`, VersionKind: "strong_etag", Length: item.Size}
}
func mediaMIME(item Item) string { return MediaMIME(item) }

// MediaMIME is shared by publication and delivery so a reference always names
// the same representation MIME, including the filename fallback.
func MediaMIME(item Item) string {
	typ := configString(item.Metadata, "mime_type")
	if typ == "" && item.SourceKind == ProviderLocal {
		typ = localMediaMIMEForPath(item.Path)
	}
	if typ == "" {
		typ = mime.TypeByExtension(filepath.Ext(item.Path))
	}
	if typ == "" {
		typ = "application/octet-stream"
	}
	return typ
}
func (s *Service) serveMedia(w http.ResponseWriter, r *http.Request) { s.serveMediaStream(w, r) }

// A successful full sync authorizes retirement. Failed runs keep both the
// published and pending generations, matching Runner's ownership lifecycle.
func (s *Service) pruneMediaPins(keep map[string]bool) error {
	dir, err := os.Open(filepath.Join(s.directory, "media"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, err := dir.ReadDir(128)
		if err != nil && err != io.EOF {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() || keep[entry.Name()] || len(entry.Name()) != 69 || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			if _, err := hex.DecodeString(strings.TrimSuffix(entry.Name(), ".json")); err != nil {
				continue
			}
			if err := os.Remove(filepath.Join(s.directory, "media", entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

// The state mutex is held by control mutations. Purge jobs have no provider
// dependencies; unrelated revocations must not cancel their durable deletion.
func (s *Service) cancelConnectionAcquisition(name string) {
	if s.cancel == nil || s.job.Operation == "purge" {
		return
	}
	for _, source := range s.profile.Sources {
		if s.job.SourceID != "" && source.ID != s.job.SourceID {
			continue
		}
		if source.Connection == name {
			s.cancel()
			return
		}
	}
}
func (s *Service) cancelSecretAcquisition(key string) {
	if s.cancel == nil || s.job.Operation == "purge" {
		return
	}
	for _, source := range s.profile.Sources {
		if s.job.SourceID != "" && source.ID != s.job.SourceID {
			continue
		}
		connection := s.profile.Connections[source.Connection]
		for _, ref := range connection.SecretRefs {
			if ref == key {
				s.cancel()
				return
			}
		}
	}
}
