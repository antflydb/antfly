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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// A purge intent is written before removing the profile entry. Replaying it is
// idempotent and never needs the revoked provider connection.
type sourcePurges struct {
	Version int      `json:"version"`
	Sources []Source `json:"sources"`
}

func (s *Service) purgePath() string { return filepath.Join(s.directory, "purges.json") }
func (s *Service) readPurges() (sourcePurges, error) {
	queue := sourcePurges{Version: 1}
	err := loadJSON(s.purgePath(), &queue, 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return queue, nil
	}
	if err != nil {
		return queue, err
	}
	if queue.Version != 1 {
		return queue, fmt.Errorf("invalid purge version")
	}
	for _, source := range queue.Sources {
		if !canonicalID(source.ID) || source.Table == "" {
			return queue, fmt.Errorf("invalid purge identity")
		}
	}
	return queue, nil
}
func (s *Service) recoverPurges() error {
	queue, err := s.readPurges()
	if err != nil {
		return err
	}
	removed := map[string]bool{}
	for _, source := range queue.Sources {
		removed[source.ID] = true
	}
	kept := s.profile.Sources[:0]
	for _, source := range s.profile.Sources {
		if !removed[source.ID] {
			kept = append(kept, source)
		}
	}
	s.profile.Sources = kept
	return nil
}
func (s *Service) queuePurge(source Source) error {
	queue, err := s.readPurges()
	if err != nil {
		return err
	}
	for _, old := range queue.Sources {
		if old.ID == source.ID {
			return nil
		}
	}
	queue.Sources = append(queue.Sources, source)
	return writeJSON(s.purgePath(), queue, 1<<20)
}
func (s *Service) purgeSources(ctx context.Context, profile Profile) error {
	runner := Runner{StateDir: s.runnerPath()}
	return runner.withStateLock(ctx, func() error { return s.purgeSourcesLocked(ctx, profile) })
}
func (s *Service) purgeSourcesLocked(ctx context.Context, profile Profile, selection ...string) error {
	selected := ""
	if len(selection) > 0 {
		selected = selection[0]
	}
	queue, err := s.readPurges()
	if err != nil {
		return err
	}
	if len(queue.Sources) == 0 {
		return nil
	}
	token := ""
	if profile.DataPlaneTokenEnv != "" {
		var exists bool
		token, exists = os.LookupEnv(profile.DataPlaneTokenEnv)
		if !exists || token == "" {
			return fmt.Errorf("data-plane token unavailable")
		}
	}
	publisher := &Publisher{Endpoint: profile.Endpoint, Token: token}
	completed := 0
	removed := map[string]bool{}
	var purgeErr error
	for _, source := range queue.Sources {
		if selected != "" && source.ID != selected {
			continue
		}
		if err := s.purgeSource(ctx, publisher, profile, source); err != nil {
			purgeErr = &sourceSyncError{sourceID: source.ID, phase: "purge", cause: err}
			break
		}
		removed[source.ID] = true
		completed++
	}
	if completed != 0 {
		// One bounded directory walk for the entire batch, even for large queues.
		if err := s.purgeMediaFiles(ctx, removed); err != nil {
			return err
		}
		if err := s.commitPurgeCompletion(queue, completed, removed); err != nil {
			return err
		}
	}
	return purgeErr
}

// Profile removal must be durable before its tombstone can disappear. Use the
// live profile under the control mutex: credential revocation is allowed while
// purge jobs run and must not be overwritten by the job's admission snapshot.
func (s *Service) commitPurgeCompletion(queue sourcePurges, completed int, removed map[string]bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneProfile(s.profile)
	kept := next.Sources[:0]
	for _, source := range next.Sources {
		if !removed[source.ID] {
			kept = append(kept, source)
		}
	}
	next.Sources = kept
	if err := writeJSON(s.profilePath(), next, 1<<20); err != nil {
		return err
	}
	s.profile = next
	s.refreshMediaRegistry()
	retained := queue.Sources[:0]
	for _, source := range queue.Sources {
		if !removed[source.ID] {
			retained = append(retained, source)
		}
	}
	queue.Sources = retained
	return writeJSON(s.purgePath(), queue, 1<<20)
}

// Validate durable identity before issuing any destructive database operation.
func (s *Service) purgeSource(ctx context.Context, publisher *Publisher, profile Profile, source Source) error {
	statePath := filepath.Join(s.runnerPath(), source.ID+".json")
	var state runState
	err := loadJSON(statePath, &state, 256<<20)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && (state.Version != 1 || state.Endpoint != profile.Endpoint || state.Table != source.Table || state.SourceID != source.ID || (state.RecordFormat != 0 && state.RecordFormat != 2)) {
		return fmt.Errorf("purge destination differs from durable ownership")
	}
	owned := state.Owned
	if owned == nil {
		owned = map[string]string{}
	}
	if err := replayOwnership(statePath+".owned", "job/"+source.ID, owned); err != nil {
		return err
	}
	legacy := []string{}
	for id := range owned {
		if !ownedID(id, "job/"+source.ID) {
			return fmt.Errorf("invalid purge ownership")
		}
		if state.RecordFormat == 0 {
			legacy = append(legacy, id)
		}
	}
	catalog := CatalogStore{Client: publisher, Table: profile.CatalogTable}
	exists, err := purgeTableExists(ctx, publisher, catalog.table())
	if err != nil {
		return err
	}
	if exists {
		if err := catalog.Retire(ctx, []string{source.ID}); err != nil {
			return err
		}
	}
	exists, err = purgeTableExists(ctx, publisher, source.Table)
	if err != nil {
		return err
	}
	if exists {
		writer, err := NewSyncWriter(publisher, source.Table, source.ID, nil, true)
		if err != nil {
			return err
		}
		if _, err = writer.Finish(ctx); err != nil {
			return err
		}
		if err := writer.RetireLegacy(ctx, legacy); err != nil {
			return err
		}
	}
	if err := s.removeMediaAuthority(source.ID); err != nil {
		return err
	}
	for _, path := range []string{statePath, statePath + ".owned"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	directory, err := os.Open(s.runnerPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
func purgeTableExists(ctx context.Context, publisher *Publisher, table string) (bool, error) {
	_, err := publisher.call(ctx, "GET", tablePath(table), nil)
	var status *PublishHTTPError
	if errors.As(err, &status) && status.Status == 404 {
		return false, nil
	}
	return err == nil, err
}

// Retain all other sources' published and pending descriptors. Digest receipts
// are shared across sources and may only be retired after the last pin is gone.
// The durable purge intent remains until both directories have been synced.
func (s *Service) purgeMediaFiles(ctx context.Context, sourceIDs map[string]bool) error {
	directory := filepath.Join(s.directory, "media")
	dir, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return s.pruneUnreferencedMediaDigests(nil)
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	digests := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := dir.ReadDir(128)
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".json") || !validContentSHA(strings.TrimSuffix(name, ".json")) {
				continue
			}
			pin, err := s.readMediaPin(strings.TrimSuffix(name, ".json"))
			if err != nil {
				return err
			}
			if sourceIDs[pin.SourceID] {
				if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			} else {
				digests[pin.ContentIdentity+".json"] = true
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	return s.pruneUnreferencedMediaDigests(digests)
}
