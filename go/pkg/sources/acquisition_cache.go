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
	"os"
	"path/filepath"
	"strings"
)

const maxCollaborationSourceObjects = 100000

func sourceInventoryLimit(provider string) int {
	switch provider {
	case ProviderSlack, ProviderDiscord, ProviderGoogleWorkspace:
		return maxCollaborationSourceObjects
	}
	return maxImportSourceObjects
}

// AcquisitionCache persists successful inventory pages for one run. Hosted
// implementations encrypt provider responses; caches never establish access.
type AcquisitionCache interface {
	Load(context.Context, string) ([]byte, bool, error)
	Save(context.Context, string, []byte) error
}
type FileAcquisitionCache struct {
	Directory string
	maxBytes  int
}

func acquisitionKey(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}
func (c *FileAcquisitionCache) Load(_ context.Context, key string) ([]byte, bool, error) {
	file, err := os.Open(filepath.Join(c.Directory, acquisitionKey(key)+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	b, err := readBounded(file)
	return b, err == nil, err
}
func (c *FileAcquisitionCache) Save(_ context.Context, key string, data []byte) error {
	limit := c.maxBytes
	if limit == 0 {
		limit = maxImportSourceObjectBytes
	}
	if len(data) > limit || !json.Valid(data) {
		return fmt.Errorf("invalid acquisition page")
	}
	if err := os.MkdirAll(c.Directory, 0700); err != nil {
		return err
	}
	name := filepath.Join(c.Directory, acquisitionKey(key)+".json")
	f, err := os.CreateTemp(c.Directory, ".page-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	dir, err := os.Open(c.Directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func inventoryPage(path string) bool {
	if strings.Contains(path, "oldest=") || strings.Contains(path, "latest=") {
		return false
	}
	return strings.Contains(path, "?") && (strings.HasPrefix(path, "/messages?") || strings.HasPrefix(path, "/history?") || strings.Contains(path, "/events?") || strings.Contains(path, "/messages?") || strings.Contains(path, "/threads/archived/") || strings.HasPrefix(path, "/conversations.history?") || strings.HasPrefix(path, "/conversations.replies?"))
}

// Resume data contains selection identities/cursors only. Text, attachment URLs,
// signed download queries, and message bodies never enter acquisition checkpoints.
func acquisitionMetadata(resource, path string, data []byte) []byte {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return nil
	}
	keep := func(raw any, fields ...string) map[string]any {
		result := map[string]any{}
		if object, ok := raw.(map[string]any); ok {
			for _, field := range fields {
				if v, ok := object[field]; ok {
					result[field] = v
				}
			}
		}
		return result
	}
	if resource == ProviderDiscord && strings.Contains(path, "/messages?") {
		if rows, ok := value.([]any); ok {
			for i, row := range rows {
				rows[i] = keep(row, "id")
			}
		}
	}
	if root, ok := value.(map[string]any); ok {
		if resource == ProviderSlack {
			if rows, ok := root["messages"].([]any); ok {
				for i, row := range rows {
					rows[i] = keep(row, "ts", "thread_ts", "reply_count", "subtype")
				}
			}
		}
		if resource == "calendar" {
			if rows, ok := root["items"].([]any); ok {
				for i, row := range rows {
					rows[i] = keep(row, "id", "status", "recurringEventId")
				}
			}
		}
		if resource == ProviderDiscord {
			delete(root, "members")
			if rows, ok := root["threads"].([]any); ok {
				for i, row := range rows {
					rows[i] = keep(row, "id", "parent_id", "guild_id", "type", "thread_metadata")
				}
			}
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}
