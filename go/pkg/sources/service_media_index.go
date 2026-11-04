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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Only compact provider identities enter the serving index. The acquisition
// journal (including historical ownership and provider metadata) is never read
// on a hot media request. A pending index is reconciled once after restart.
type mediaAuthority struct {
	Version  int      `json:"version"`
	SourceID string   `json:"source_id"`
	Pending  bool     `json:"pending"`
	Keys     []string `json:"keys"`
	MAC      string   `json:"mac"`
}
type authorityEntry struct {
	index mediaAuthority
	size  int
	used  uint64
}
type mediaAuthorities struct {
	mu         sync.Mutex
	entries    map[string]authorityEntry
	bytes      int
	tick       uint64
	generation uint64
}

const authorityCacheBytes = 32 << 20

func itemMediaKey(item Item) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", item.ID, item.Revision, item.Size)))
	return hex.EncodeToString(sum[:])
}
func (s *Service) authorityPath(sourceID string) string {
	return filepath.Join(s.directory, "media-authority", sourceID+".json")
}
func (s *Service) authorityMAC(index mediaAuthority) string {
	index.MAC = ""
	raw, _ := json.Marshal(index)
	return s.signMedia("authority-v1", raw)
}
func (s *Service) publishMediaAuthority(_ context.Context, source Source, pending, published *Inventory) error {
	keys := map[string]bool{}
	for _, inventory := range []*Inventory{pending, published} {
		if inventory == nil {
			continue
		}
		for _, item := range inventory.Objects {
			keys[itemMediaKey(item)] = true
		}
	}
	index := mediaAuthority{Version: 1, SourceID: source.ID, Pending: pending != nil, Keys: make([]string, 0, len(keys))}
	for key := range keys {
		index.Keys = append(index.Keys, key)
	}
	sort.Strings(index.Keys)
	index.MAC = s.authorityMAC(index)
	if err := writeJSON(s.authorityPath(source.ID), index, 4<<20); err != nil {
		return err
	}
	s.authorities.mu.Lock()
	defer s.authorities.mu.Unlock()
	s.authorities.generation++
	s.cacheAuthorityLocked(index)
	return nil
}
func (s *Service) cacheAuthorityLocked(index mediaAuthority) {
	cache := &s.authorities
	if cache.entries == nil {
		cache.entries = map[string]authorityEntry{}
	}
	if old, ok := cache.entries[index.SourceID]; ok {
		cache.bytes -= old.size
		delete(cache.entries, index.SourceID)
	}
	size := len(index.Keys)*96 + 256
	if size > authorityCacheBytes {
		return
	}
	for len(cache.entries) >= 64 || cache.bytes+size > authorityCacheBytes {
		var oldest string
		age := ^uint64(0)
		for id, entry := range cache.entries {
			if entry.used < age {
				oldest, age = id, entry.used
			}
		}
		cache.bytes -= cache.entries[oldest].size
		delete(cache.entries, oldest)
	}
	cache.tick++
	cache.entries[index.SourceID] = authorityEntry{index: index, size: size, used: cache.tick}
	cache.bytes += size
}
func (s *Service) mediaAuthorized(sourceID, key string) (bool, error) {
	cache := &s.authorities
	for {
		cache.mu.Lock()
		if entry, ok := cache.entries[sourceID]; ok {
			cache.tick++
			entry.used = cache.tick
			cache.entries[sourceID] = entry
			cache.mu.Unlock()
			i := sort.SearchStrings(entry.index.Keys, key)
			return i < len(entry.index.Keys) && entry.index.Keys[i] == key, nil
		}
		generation := cache.generation
		cache.mu.Unlock()
		var index mediaAuthority
		if err := loadJSON(s.authorityPath(sourceID), &index, 4<<20); err != nil {
			return false, err
		}
		if index.Version != 1 || index.SourceID != sourceID || !sort.StringsAreSorted(index.Keys) || !hmac.Equal([]byte(index.MAC), []byte(s.authorityMAC(index))) {
			return false, fmt.Errorf("invalid media authority receipt")
		}
		cache.mu.Lock()
		if generation != cache.generation {
			cache.mu.Unlock()
			continue
		}
		s.cacheAuthorityLocked(index)
		cache.mu.Unlock()
		i := sort.SearchStrings(index.Keys, key)
		return i < len(index.Keys) && index.Keys[i] == key, nil
	}
}

func (s *Service) recoverMediaAuthorities() error {
	for _, source := range s.profile.Sources {
		if source.Publication != "media" {
			continue
		}
		var index mediaAuthority
		err := loadJSON(s.authorityPath(source.ID), &index, 4<<20)
		if err == nil && index.Version == 1 && !index.Pending && hmac.Equal([]byte(index.MAC), []byte(s.authorityMAC(index))) {
			continue
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		var state runState
		err = loadJSON(filepath.Join(s.runnerPath(), source.ID+".json"), &state, 256<<20)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.publishMediaAuthority(context.Background(), source, state.Pending, state.Published); err != nil {
			return err
		}
	}
	return nil
}
