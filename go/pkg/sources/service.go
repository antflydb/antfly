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
	mu                        sync.Mutex
	profile                   Profile
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
	Profile  Profile
	StateDir string
	// PublicURL is a stable address reachable by Antfly, including after restart.
	PublicURL  string
	AdminToken string
	// MediaKey must be stable, at least 32 bytes, and different from AdminToken.
	MediaKey []byte
	Factory  Factory
}

type SyncJob struct {
	Version    int    `json:"version"`
	ID         string `json:"id"`
	State      string `json:"state"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at,omitempty"`
	Code       string `json:"code,omitempty"`
}

func NewService(o ServiceOptions) (*Service, error) {
	if len(o.AdminToken) < 32 || strings.ContainsAny(o.AdminToken, "\r\n") || len(o.MediaKey) < 32 || o.StateDir == "" || hmac.Equal([]byte(o.AdminToken), o.MediaKey) {
		return nil, fmt.Errorf("source service requires state-dir, a 32-byte admin token and a separate 32-byte media key")
	}
	if err := ValidateEndpoint(o.PublicURL); err != nil {
		return nil, err
	}
	s := &Service{profile: cloneProfile(o.Profile), directory: o.StateDir, baseURL: strings.TrimRight(o.PublicURL, "/"), token: o.AdminToken, mediaKey: bytes.Clone(o.MediaKey), factory: o.Factory, fetchSlots: make(chan struct{}, 4)}
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
	success = true
	return s, nil
}
func (s *Service) profilePath() string { return filepath.Join(s.directory, "profile.json") }
func (s *Service) jobPath() string     { return filepath.Join(s.directory, "job.json") }
func (s *Service) runnerPath() string  { return filepath.Join(s.directory, "runner") }
func (s *Service) validateProfile(p Profile) error {
	r := Runner{Profile: p, StateDir: s.runnerPath(), CatalogPath: filepath.Join(s.runnerPath(), "catalog.json"), MediaReference: s.pinMedia}
	if err := r.validate(); err != nil {
		return err
	}
	for _, source := range p.Sources {
		provider := p.Connections[source.Connection].Provider
		if source.Publication == "media" && provider != "" && provider != ProviderS3 && provider != ProviderGCS && provider != ProviderGoogleWorkspace {
			return fmt.Errorf("media publication supports S3, GCS and Drive sources")
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
	if done != nil {
		<-done
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		serviceError(w, 503, "service_closed")
		return
	}
	if r.URL.Path == "/v1/profile" && r.Method == http.MethodGet {
		serviceJSON(w, 200, s.profile)
		return
	}
	if r.URL.Path == "/v1/sync" && r.Method == http.MethodPost {
		if s.cancel != nil {
			serviceError(w, 409, "sync_in_progress")
			return
		}
		var input struct {
			Restart bool `json:"restart"`
		}
		if err := serviceDecode(w, r, &input); err != nil {
			serviceError(w, 400, "invalid_request")
			return
		}
		if err := s.validateProfile(s.profile); err != nil {
			serviceError(w, 400, "invalid_source_profile")
			return
		}
		profile := cloneProfile(s.profile)
		ctx, cancel := context.WithCancel(context.Background())
		job := SyncJob{Version: 1, ID: rand.Text(), State: "running", StartedAt: time.Now().Unix()}
		if err := writeJSON(s.jobPath(), job, 4096); err != nil {
			cancel()
			serviceError(w, 500, "state_write_failed")
			return
		}
		s.job, s.cancel, s.done = &job, cancel, make(chan struct{})
		go s.run(ctx, profile, input.Restart, s.done)
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
			if s.cancel != nil {
				s.cancel()
			}
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
			s.profile = next
			if s.cancel != nil {
				s.cancel()
			}
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
		if _, err := s.factory(r.Context(), connection); err != nil {
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
		s.profile = next
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
		switch r.Method {
		case http.MethodPut:
			var source Source
			if err := serviceDecode(w, r, &source); err != nil || source.ID != id {
				serviceError(w, 400, "invalid_source")
				return
			}
			if index < 0 {
				next.Sources = append(next.Sources, source)
			} else {
				next.Sources[index] = source
			}
		case http.MethodDelete:
			if index < 0 {
				serviceError(w, 404, "source_not_found")
				return
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
			serviceError(w, 500, "state_write_failed")
			return
		}
		s.profile = next
		serviceJSON(w, 200, map[string]any{"version": 1, "state": "configured"})
		return
	}
	serviceError(w, 404, "not_found")
}
func (s *Service) run(ctx context.Context, profile Profile, restart bool, done chan struct{}) {
	defer close(done)
	keep := map[string]bool{}
	resolver := func(ctx context.Context, source Source, request Request, item Item) (MediaReference, error) {
		ref, err := s.pinMedia(ctx, source, request, item)
		if err == nil {
			keep[strings.Trim(ref.Version, "\"")+".json"] = true
		}
		return ref, err
	}
	runner := Runner{Profile: profile, StateDir: s.runnerPath(), Factory: s.factory, Restart: restart, MediaReference: resolver}
	err := runner.Sync(ctx)
	var cleanupErr error
	if err == nil {
		cleanupErr = s.pruneMediaPins(keep)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel()
	s.cancel = nil
	s.job.State, s.job.FinishedAt = "completed", time.Now().Unix()
	if cleanupErr != nil {
		s.job.Code = "media_cleanup_failed"
	}
	if err != nil {
		s.job.State, s.job.Code = "failed", "sync_failed"
		if errors.Is(err, context.Canceled) {
			s.job.State, s.job.Code = "cancelled", "cancelled"
		}
	}
	if err := writeJSON(s.jobPath(), s.job, 4096); err != nil {
		s.job.State, s.job.Code = "failed", "state_write_failed"
	}
}

type mediaPin struct {
	Source     Source     `json:"source"`
	Connection Connection `json:"connection"`
	Item       Item       `json:"item"`
}

func (s *Service) pinPath(id string) string { return filepath.Join(s.directory, "media", id+".json") }
func (s *Service) mediaSignature(id string) string {
	mac := hmac.New(sha256.New, s.mediaKey)
	_, _ = mac.Write([]byte("antfly-source-media-v1\x00" + id))
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *Service) pinMedia(_ context.Context, source Source, _ Request, item Item) (MediaReference, error) {
	if item.Size < 1 || item.Size > maxImportSourceObjectBytes {
		return MediaReference{}, fmt.Errorf("media requires an exact length of 1..32 MiB")
	}
	if upper, _ := item.Metadata["size_is_upper_bound"].(bool); upper || configString(item.Metadata, "export_mime_type") != "" {
		return MediaReference{}, fmt.Errorf("media requires a binary object with exact length; Workspace exports use text publication")
	}
	item.ContentRef, item.ObjectIndex = "", 0
	item.Metadata = cloneSourceMetadata(item.Metadata)
	delete(item.Metadata, "object_index")
	source.Alias, source.PermissionTTLSeconds = "", 0
	s.mu.Lock()
	connection, exists := s.profile.Connections[source.Connection]
	s.mu.Unlock()
	if !exists {
		return MediaReference{}, fmt.Errorf("media connection revoked")
	}
	pin := mediaPin{Source: source, Connection: connection, Item: item}
	raw, err := json.Marshal(pin)
	if err != nil {
		return MediaReference{}, err
	}
	digest := sha256.Sum256(raw)
	id := hex.EncodeToString(digest[:])
	if err := os.MkdirAll(filepath.Join(s.directory, "media"), 0700); err != nil {
		return MediaReference{}, err
	}
	if err := writeJSON(s.pinPath(id), pin, 1<<20); err != nil {
		return MediaReference{}, err
	}
	return MediaReference{Type: "source", URI: s.baseURL + "/v1/media/" + id + "/" + s.mediaSignature(id), MIMEType: mediaMIME(item), Version: `"` + id + `"`, VersionKind: "strong_etag", Length: item.Size}, nil
}
func mediaMIME(item Item) string {
	typ := configString(item.Metadata, "mime_type")
	if typ == "" {
		typ = mime.TypeByExtension(filepath.Ext(item.Path))
	}
	if typ == "" {
		typ = "application/octet-stream"
	}
	return typ
}
func (s *Service) serveMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		serviceError(w, 405, "method_not_allowed")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/media/"), "/")
	if len(parts) != 2 || len(parts[0]) != 64 || len(parts[1]) != 64 {
		serviceError(w, 404, "media_not_found")
		return
	}
	id := parts[0]
	if _, err := hex.DecodeString(id); err != nil || !hmac.Equal([]byte(parts[1]), []byte(s.mediaSignature(id))) {
		serviceError(w, 404, "media_not_found")
		return
	}
	select {
	case s.fetchSlots <- struct{}{}:
		defer func() { <-s.fetchSlots }()
	default:
		w.Header().Set("Retry-After", "1")
		serviceError(w, 503, "fetch_busy")
		return
	}
	pin, err := s.loadMediaPin(id)
	if err != nil {
		serviceError(w, 410, "media_revoked")
		return
	}
	s.mu.Lock()
	var source *Source
	for _, candidate := range s.profile.Sources {
		candidate.Alias, candidate.PermissionTTLSeconds = "", 0
		a, _ := json.Marshal(candidate)
		b, _ := json.Marshal(pin.Source)
		if bytes.Equal(a, b) {
			copy := candidate
			source = &copy
			break
		}
	}
	connection, exists := s.profile.Connections[pin.Source.Connection]
	closed := s.closed
	s.mu.Unlock()
	currentConnection, _ := json.Marshal(connection)
	pinnedConnection, _ := json.Marshal(pin.Connection)
	if closed || source == nil || !exists || !bytes.Equal(currentConnection, pinnedConnection) {
		serviceError(w, 410, "media_revoked")
		return
	}
	var state runState
	if err := loadJSON(filepath.Join(s.runnerPath(), source.ID+".json"), &state, 256<<20); err != nil {
		serviceError(w, 410, "media_revoked")
		return
	}
	selected := false
	for _, inv := range []*Inventory{state.Pending, state.Published} {
		if inv == nil {
			continue
		}
		for _, item := range inv.Objects {
			if item.ID == pin.Item.ID && item.Revision == pin.Item.Revision {
				selected = true
				break
			}
		}
	}
	if !selected {
		serviceError(w, 410, "media_revoked")
		return
	}
	connector, err := s.factory(r.Context(), connection)
	if err == nil {
		connector, err = publicationConnector(connector, *source)
	}
	request := Request{SourceNamespace: "job/" + source.ID, Config: source.Config, Mode: ImportModeSource}
	if err == nil {
		err = connector.CheckAccess(r.Context(), request, pin.Item)
	}
	var data []byte
	if err == nil {
		data, err = connector.Read(r.Context(), request, pin.Item)
	}
	if err != nil {
		var changed *RevisionChangedError
		if errors.As(err, &changed) {
			serviceError(w, 412, "source_version_changed")
		} else {
			serviceError(w, 502, "source_fetch_failed")
		}
		return
	}
	if int64(len(data)) != pin.Item.Size || len(data) > maxImportSourceObjectBytes {
		serviceError(w, 502, "source_length_changed")
		return
	}
	w.Header().Set("Content-Type", mediaMIME(pin.Item))
	w.Header().Set("ETag", `"`+id+`"`)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, pin.Item.Filename, time.Time{}, bytes.NewReader(data))
}

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

func (s *Service) loadMediaPin(id string) (mediaPin, error) {
	var pin mediaPin
	file, err := os.Open(s.pinPath(id))
	if err != nil {
		return pin, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return pin, err
	}
	if len(raw) > 1<<20 {
		return pin, fmt.Errorf("media descriptor exceeds budget")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != id {
		return pin, fmt.Errorf("media descriptor integrity failed")
	}
	err = json.Unmarshal(raw, &pin)
	return pin, err
}
