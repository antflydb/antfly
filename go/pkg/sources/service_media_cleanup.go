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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Per-source runs retire only that source's superseded descriptors. Digest
// receipts may be shared, so delete a candidate only after a bounded-memory walk
// proves no remaining pin references it. Never construct a whole-profile index.
func (s *Service) pruneSourceMedia(keep map[string]bool, sourceID string) error {
	candidates := map[string]bool{}
	walk := func(visit func(string, mediaPinV2) error) error {
		dir, err := os.Open(filepath.Join(s.directory, "media"))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer dir.Close()
		for {
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
				if err = visit(name, pin); err != nil {
					return err
				}
			}
			if readErr == io.EOF {
				return dir.Sync()
			}
		}
	}
	if err := walk(func(name string, pin mediaPinV2) error {
		if pin.SourceID != sourceID || keep[name] {
			return nil
		}
		if err := os.Remove(s.pinPath(strings.TrimSuffix(name, ".json"))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Prior implementations may have retained many historical descriptors.
		// Extra orphan digest receipts are harmless; cap migration bookkeeping.
		if len(candidates) < 10000 {
			candidates[pin.ContentIdentity] = true
		}
		return nil
	}); err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	if err := walk(func(_ string, pin mediaPinV2) error { delete(candidates, pin.ContentIdentity); return nil }); err != nil {
		return err
	}
	for identity := range candidates {
		if err := os.Remove(s.digestPath(identity)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
