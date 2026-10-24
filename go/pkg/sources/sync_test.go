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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// Mirrors the public merge contract for HTTP fixtures. Native tests separately
// exercise hashing, range scans and the irreversible storage boundary.
func fixtureScopedMerge(records map[string]map[string]any, page ScopedMergePage) (MergeResult, error) {
	cursor := page.LastMergedID
	if cursor == "" {
		cursor = page.KeyRange.From
	}
	if cursor < page.KeyRange.From || cursor >= page.KeyRange.To || page.KeyRange.From == "" || page.KeyRange.To == "" {
		return MergeResult{}, fmt.Errorf("invalid bounds")
	}
	keys := make([]string, 0, len(page.Records))
	for key := range page.Records {
		if key <= cursor || key >= page.KeyRange.To {
			return MergeResult{}, fmt.Errorf("foreign key")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	end := page.KeyRange.To
	if len(keys) > 0 {
		end = keys[len(keys)-1]
	}
	keep := map[string]bool{}
	if len(keys) == 0 {
		for _, id := range page.RetainIDs {
			if id <= cursor || id >= end || records[id] == nil || keep[id] {
				return MergeResult{}, fmt.Errorf("invalid retained key")
			}
			keep[id] = true
		}
	}
	result := MergeResult{Status: "success", NextCursor: cursor, KeyRange: KeyRange{From: cursor, To: end}}
	if len(keys) > 0 {
		result.NextCursor = end
	} else {
		result.Skipped = len(keep)
	}
	if page.DeleteMissing {
		for id := range records {
			within := id > cursor && (id < end || len(keys) > 0 && id == end)
			if within && !keep[id] && page.Records[id] == nil {
				delete(records, id)
				result.Deleted++
			}
		}
	}
	for _, id := range keys {
		raw, _ := json.Marshal(page.Records[id])
		before, _ := json.Marshal(records[id])
		if string(raw) == string(before) {
			result.Skipped++
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(raw, &row); err != nil {
			return result, err
		}
		records[id] = row
		result.Upserted++
	}
	return result, nil
}

func TestSharedSyncSkipsUnchangedRecordsAndRetainsIndexingTime(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	var before []byte
	for _, row := range backend.records {
		if grep, ok := row["grep"].(map[string]any); ok {
			grep["indexed_at"] = float64(123)
		}
		before, _ = json.Marshal(row)
	}
	upserted := backend.upserted
	backend.mu.Unlock()
	// Force the next normalization time to differ without relying on a sleep.
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.upserted != upserted {
		t.Fatalf("unchanged sync rewrote rows: %d -> %d", upserted, backend.upserted)
	}
	for _, row := range backend.records {
		after, _ := json.Marshal(row)
		if string(after) != string(before) {
			t.Fatal("unchanged row or indexing time changed")
		}
	}
}

func TestSharedSyncPartialAndFailedInventoriesCannotDelete(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	writer, err := NewSyncWriter(runner.Publisher, "sources", testSourceID, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	count := len(backend.records)
	backend.mu.Unlock()
	if count != 1 {
		t.Fatal("partial empty inventory deleted content")
	}
	writer, err = NewSyncWriter(runner.Publisher, "sources", testSourceID, []string{connector.inventory.Objects[0].ID}, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finish(t.Context()); err == nil {
		t.Fatal("unverified full inventory allowed cleanup")
	}
}

func TestSharedSyncMigrationOnlyRetiresJournaledLegacyRows(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	id := connector.inventory.Objects[0].ID
	key, err := RecordKey(testSourceID, id)
	if err != nil {
		t.Fatal(err)
	}
	backend.records[id] = map[string]any{"source_id": testSourceID, "legacy": true}
	backend.records["other-record"] = map[string]any{"source_id": "another-source"}
	if err := osWriteLegacyState(runner, id); err != nil {
		t.Fatal(err)
	}
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.records[id] != nil || backend.records[key] == nil || backend.records["other-record"] == nil {
		t.Fatalf("unsafe migration: %v", backend.records)
	}
}

func osWriteLegacyState(runner *Runner, id string) error {
	if err := os.MkdirAll(runner.StateDir, 0700); err != nil {
		return err
	}
	state := runState{Version: 1, Endpoint: runner.Profile.Endpoint, Table: "sources", SourceID: testSourceID, Owned: map[string]string{id: "old"}}
	return writeJSON(filepath.Join(runner.StateDir, testSourceID+".json"), state, 64<<20)
}

func TestSharedSyncIsolatesSourcesSharingATable(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	otherID := "22222222-2222-4222-8222-222222222222"
	item := Item{ID: "drive://job/" + otherID + "/doc", Path: "doc", URL: "https://docs.google.com/document/d/doc/edit", Revision: "v1", SourceKind: ProviderGoogleDrive}
	doc, err := Normalize(otherID, item, []byte("other text"), Permissions{Public: true, Readers: []string{}}, time.Unix(123, 0))
	if err != nil {
		t.Fatal(err)
	}
	key, err := RecordKey(otherID, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewSyncWriter(runner.Publisher, "sources", otherID, []string{item.ID}, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Upsert(t.Context(), map[string]Document{key: doc}); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.records["application"] = map[string]any{"text": "app"}
	backend.mu.Unlock()
	writer, err = NewSyncWriter(runner.Publisher, "sources", testSourceID, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.records) != 2 || backend.records[key] == nil || backend.records["application"] == nil {
		t.Fatalf("cross-source cleanup: %v", backend.records)
	}
}

func TestSharedSyncPermissionChangesRewriteUnchangedContent(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	before := backend.upserted
	backend.mu.Unlock()
	runner.Profile.Sources[0].Permissions = &Permissions{Readers: []string{"user:different"}}
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.upserted != before+1 {
		t.Fatal("permission change incorrectly skipped")
	}
	for _, row := range backend.records {
		raw, _ := json.Marshal(row)
		if !strings.Contains(string(raw), "user:different") {
			t.Fatal("new permissions not written")
		}
	}
}

type interceptedSyncClient struct {
	SyncClient
	merge   func(context.Context, string, []byte) ([]byte, error)
	batches int
}

func (c *interceptedSyncClient) Merge(ctx context.Context, table string, body []byte) ([]byte, error) {
	return c.merge(ctx, table, body)
}
func (c *interceptedSyncClient) Batch(ctx context.Context, table string, rows map[string]any, ids []string) error {
	c.batches++
	return c.SyncClient.Batch(ctx, table, rows, ids)
}

func TestSharedSyncRejectsUnsupportedOrMalformedMergeWithoutFallback(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply []byte
		err   error
	}{
		{name: "legacy runtime", err: fmt.Errorf("HTTP 400: records required")},
		{name: "legacy acknowledgement", reply: []byte(`{"status":"success","upserted":1}`)},
		{name: "truncated response", reply: []byte(`{"status":"success"`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner, connector, backend := runnerFixture(t)
			item := connector.inventory.Objects[0]
			doc, err := Normalize(testSourceID, item, connector.data[item.ID], *runner.Profile.Sources[0].Permissions, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			key, err := RecordKey(testSourceID, item.ID)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			client := &interceptedSyncClient{SyncClient: runner.Publisher, merge: func(_ context.Context, _ string, body []byte) ([]byte, error) {
				calls++
				var envelope map[string]any
				if json.Unmarshal(body, &envelope) != nil || envelope["records"] != nil || envelope["scoped_merge"] == nil {
					t.Fatal("unsafe downgradeable request")
				}
				return test.reply, test.err
			}}
			writer, err := NewSyncWriter(client, "sources", testSourceID, []string{item.ID}, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Upsert(t.Context(), map[string]Document{key: doc}); err == nil {
				t.Fatal("invalid acknowledgement accepted")
			}
			if _, err = writer.Finish(t.Context()); err == nil {
				t.Fatal("unverified inventory reconciled")
			}
			if calls != 1 || client.batches != 0 || len(backend.records) != 0 {
				t.Fatal("unsupported merge fell back or reconciled")
			}
		})
	}
}
