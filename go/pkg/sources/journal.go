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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Ownership is journaled before writes and after deletes. Retried requests can
// therefore retire every potentially written row without rewriting a full
// inventory for each item. Checkpoints and replay operations are idempotent.
type ownershipEvent struct {
	ID, Revision string
	Delete       []string
}

func replayOwnership(path, namespace string, owned map[string]string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > 64<<20 {
		return fmt.Errorf("ownership journal exceeds budget")
	}
	reader := bufio.NewReader(file)
	var complete int64
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			// An unterminated last append was not fsynced before any publication.
			if len(line) > 0 {
				if err := file.Truncate(complete); err != nil {
					return err
				}
				return file.Sync()
			}
			return nil
		}
		if err != nil {
			return err
		}
		complete += int64(len(line))
		var event ownershipEvent
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return fmt.Errorf("invalid ownership journal: %w", err)
		}
		if event.ID != "" {
			if !ownedID(event.ID, namespace) || event.Revision == "" || len(event.Delete) > 0 {
				return fmt.Errorf("invalid ownership journal identity")
			}
			owned[event.ID] = event.Revision
		} else {
			if len(event.Delete) == 0 || len(event.Delete) > 100 {
				return fmt.Errorf("invalid ownership delete journal")
			}
			for _, id := range event.Delete {
				if !ownedID(id, namespace) {
					return fmt.Errorf("invalid ownership delete identity")
				}
				delete(owned, id)
			}
		}
		if len(owned) > maxImportSourceObjects {
			return fmt.Errorf("ownership count exceeds budget")
		}
	}
}
func journalOwnership(path string, event ownershipEvent) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(event); err != nil {
		return err
	}
	return file.Sync()
}
func resetOwnershipJournal(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
