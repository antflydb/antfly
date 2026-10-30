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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const ScopedMergeContentType = "application/vnd.antfly.scoped-merge.v1+json"

// RecordKey separates the provider's identity from the destination key. Every
// provider belonging to one logical source shares a single exclusive key range.
func RecordKey(sourceID, providerID string) (string, error) {
	if !canonicalID(sourceID) || !ownedID(providerID, "job/"+sourceID) {
		return "", fmt.Errorf("invalid source record identity")
	}
	digest := sha256.Sum256([]byte(providerID))
	return RecordPrefix(sourceID) + hex.EncodeToString(digest[:]), nil
}
func RecordPrefix(sourceID string) string { return "source://job/" + sourceID + "/records/" }
func recordRange(sourceID string) KeyRange {
	prefix := RecordPrefix(sourceID)
	return KeyRange{From: prefix, To: strings.TrimSuffix(prefix, "/") + "0"}
}

type KeyRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}
type ScopedMergePage struct {
	KeyRange      KeyRange       `json:"key_range"`
	Records       map[string]any `json:"records"`
	RetainIDs     []string       `json:"retain_ids,omitempty"`
	LastMergedID  string         `json:"last_merged_id,omitempty"`
	DeleteMissing bool           `json:"delete_missing"`
	SyncLevel     string         `json:"sync_level"`
}
type ScopedMergeRequest struct {
	ScopedMerge ScopedMergePage `json:"scoped_merge"`
}
type MergeResult struct {
	Status     string            `json:"status"`
	Upserted   int               `json:"upserted"`
	Deleted    int               `json:"deleted"`
	Skipped    int               `json:"skipped"`
	NextCursor string            `json:"next_cursor"`
	KeyRange   KeyRange          `json:"key_range"`
	Failed     []json.RawMessage `json:"failed"`
}

type SyncClient interface {
	Query(context.Context, string, []byte) ([]byte, error)
	Merge(context.Context, string, []byte) ([]byte, error)
	Batch(context.Context, string, map[string]any, []string) error
}

// SyncWriter performs non-destructive writes first. Finish reconciles missing
// records only after every item in a complete pinned inventory is query-visible.
// Runtimes must serialize writers for the same source; different source ranges
// may share a table. No fallback to legacy/unbounded LinearMerge is permitted.
type SyncWriter struct {
	client                          SyncClient
	table, sourceID                 string
	bounds                          KeyRange
	inventory, verified             map[string]bool
	authoritative, finished, failed bool
}

func NewSyncWriter(client SyncClient, table, sourceID string, providerIDs []string, authoritative bool) (*SyncWriter, error) {
	if client == nil || table == "" || !canonicalID(sourceID) || len(providerIDs) > maxCollaborationSourceObjects {
		return nil, fmt.Errorf("invalid source sync configuration")
	}
	w := &SyncWriter{client: client, table: table, sourceID: sourceID, bounds: recordRange(sourceID), inventory: map[string]bool{}, verified: map[string]bool{}, authoritative: authoritative}
	for _, id := range providerIDs {
		key, err := RecordKey(sourceID, id)
		if err != nil {
			return nil, err
		}
		if w.inventory[key] {
			return nil, fmt.Errorf("duplicate source inventory identity")
		}
		w.inventory[key] = true
	}
	return w, nil
}

// Upsert verifies unchanged rows against a fresh complete normalized projection.
// Only changed rows are submitted to scoped LinearMerge.
// documents is updated with the exact projection verified by the shared barrier.
// Skipped includes fresh query proofs; read-only no-ops have no merge cursor/range.
// Invalid input closes the writer so earlier proofs cannot authorize cleanup.
func (w *SyncWriter) Upsert(ctx context.Context, documents map[string]Document) (MergeResult, error) {
	if w.finished || w.failed || len(documents) == 0 || len(documents) > 1000 {
		w.failed = true
		return MergeResult{}, fmt.Errorf("invalid source merge page")
	}
	ids := make([]string, 0, len(documents))
	for id, doc := range documents {
		key, err := RecordKey(w.sourceID, configString(doc.Metadata, "identity"))
		if err != nil || key != id || !w.inventory[id] || configString(doc.Metadata, "source_id") != w.sourceID || doc.Version != 1 || configString(doc.Metadata, "sha256") == "" {
			w.failed = true
			return MergeResult{}, fmt.Errorf("record outside pinned source inventory")
		}
		digest := sha256.Sum256([]byte(doc.Content))
		if !utf8.ValidString(doc.Content) || strings.ContainsRune(doc.Content, 0) || configString(doc.Metadata, "sha256") != hex.EncodeToString(digest[:]) {
			w.failed = true
			return MergeResult{}, fmt.Errorf("source content digest does not match normalized text")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		delete(w.verified, id)
	}
	body, _ := json.Marshal(map[string]any{"query": documentIDsQuery(ids), "fields": sourceRowFields(), "limit": len(ids), "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	raw, err := w.client.Query(ctx, w.table, body)
	if err != nil {
		return MergeResult{}, err
	}
	hits, err := decodeSyncHits(raw)
	if err != nil {
		return MergeResult{}, err
	}
	seen := map[string]bool{}
	unchanged := map[string]bool{}
	for _, hit := range hits {
		doc, ok := documents[hit.ID]
		if !ok || seen[hit.ID] {
			return MergeResult{}, fmt.Errorf("unexpected source metadata identity")
		}
		seen[hit.ID] = true
		before, after := hit.Source.Grep, doc
		before.IndexedAt, after.IndexedAt = 0, 0
		a, err := projectionDigest(before)
		if err != nil {
			return MergeResult{}, err
		}
		b, err := projectionDigest(after)
		if err != nil {
			return MergeResult{}, err
		}
		if a == b && sourceRowMatches(hit, w.sourceID) {
			doc.IndexedAt = hit.Source.Grep.IndexedAt
			documents[hit.ID] = doc
			unchanged[hit.ID] = true
		}
	}
	rows := make(map[string]any, len(documents)-len(unchanged))
	changed := map[string]Document{}
	changedIDs := []string{}
	for _, id := range ids {
		if unchanged[id] {
			continue
		}
		doc := documents[id]
		changed[id] = doc
		changedIDs = append(changedIDs, id)
		rows[id] = map[string]any{"id": id, "source_id": w.sourceID, "doc_type": "source_item", "grep": doc}
	}
	result := MergeResult{Status: "success", Skipped: len(unchanged)}
	if len(changedIDs) > 0 {
		// Deletes are disabled: excluding unchanged rows cannot remove gap records.
		cursor := changedIDs[0][:len(changedIDs[0])-1]
		result, err = w.merge(ctx, ScopedMergePage{KeyRange: w.bounds, Records: rows, LastMergedID: cursor, SyncLevel: "write"}, changedIDs[len(changedIDs)-1], len(changedIDs))
		if err != nil {
			return result, err
		}
		if result.Deleted != 0 {
			w.failed = true
			return result, fmt.Errorf("non-destructive source merge reported deletions")
		}
		if err := waitForDocuments(ctx, w.client.Query, w.table, changed, true); err != nil {
			w.failed = true
			return result, err
		}
		result.Skipped += len(unchanged)
	}
	for id := range documents {
		w.verified[id] = true
	}
	return result, nil
}

// Finish never removes missing rows for partial inventories or repairs. A full
// run retains only already verified IDs, avoiding a second upload of all text.
func (w *SyncWriter) Finish(ctx context.Context) (MergeResult, error) {
	if w.finished || w.failed {
		return MergeResult{}, fmt.Errorf("source sync writer is finished or failed; resume the durable source run")
	}
	if !w.authoritative {
		return MergeResult{}, nil
	}
	if len(w.verified) != len(w.inventory) {
		return MergeResult{}, fmt.Errorf("source inventory is not completely verified; refusing reconciliation")
	}
	ids := make([]string, 0, len(w.inventory))
	for id := range w.inventory {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	// A fresh absence proof avoids replaying an already committed cleanup when
	// a prior run lost its acknowledgement. No-op syncs perform no merges.
	clean, err := w.rangeIsClean(ctx, ids)
	if err != nil {
		return MergeResult{}, err
	}
	if clean {
		if err := w.verifyRetainedIDs(ctx, ids); err != nil {
			return MergeResult{}, err
		}
		w.finished = true
		return MergeResult{Status: "success", Skipped: len(ids)}, nil
	}
	page := ScopedMergePage{KeyRange: w.bounds, Records: map[string]any{}, RetainIDs: ids, LastMergedID: w.bounds.From, DeleteMissing: true, SyncLevel: "write"}
	result, err := w.merge(ctx, page, w.bounds.To, len(ids))
	if err != nil {
		return result, err
	}
	if result.Upserted != 0 || result.Skipped != len(ids) {
		w.failed = true
		return result, fmt.Errorf("source reconciliation did not retain every verified record")
	}
	if err := w.waitForCleanRange(ctx, ids); err != nil {
		w.failed = true
		return result, err
	}
	w.finished = true
	return result, nil
}
func (w *SyncWriter) merge(ctx context.Context, page ScopedMergePage, expectedTo string, count int) (result MergeResult, err error) {
	// A lost/invalid acknowledgement cannot authorize another mutation or cleanup
	// on this writer. Recover with a new inventory writer and fresh visibility.
	defer func() {
		if err != nil {
			w.failed = true
		}
	}()

	body, err := json.Marshal(ScopedMergeRequest{ScopedMerge: page})
	if err != nil {
		return MergeResult{}, err
	}
	if len(body) > 64<<20 {
		return MergeResult{}, fmt.Errorf("source merge page exceeds 64 MiB")
	}
	raw, err := w.client.Merge(ctx, w.table, body)
	if err != nil {
		return MergeResult{}, err
	}
	if err := decodeSyncJSON(raw, &result); err != nil {
		return result, err
	}
	cursor := expectedTo
	if len(page.Records) == 0 {
		cursor = page.LastMergedID
	}
	if result.Status != "success" || len(result.Failed) != 0 || result.Upserted < 0 || result.Skipped < 0 || result.Deleted < 0 || result.Upserted+result.Skipped != count || result.NextCursor != cursor || result.KeyRange.From != page.LastMergedID || result.KeyRange.To != expectedTo {
		return result, fmt.Errorf("runtime did not acknowledge a complete bounded source merge")
	}
	return result, nil
}

type syncHit struct {
	ID     string `json:"_id"`
	Source struct {
		ID       string   `json:"id"`
		SourceID string   `json:"source_id"`
		DocType  string   `json:"doc_type"`
		Grep     Document `json:"grep"`
	} `json:"_source"`
}

func decodeSyncJSON(raw []byte, target any) error {
	if len(raw) > 8<<20 {
		return fmt.Errorf("source sync response exceeds budget")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("invalid source sync response trailer")
	}
	return nil
}
func decodeSyncHits(raw []byte) ([]syncHit, error) {
	var result struct {
		Responses []struct {
			Status   int             `json:"status"`
			Error    json.RawMessage `json:"error"`
			Snapshot string          `json:"remote_snapshot"`
			Hits     struct {
				Hits []syncHit `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	if err := decodeSyncJSON(raw, &result); err != nil {
		return nil, err
	}
	if len(result.Responses) != 1 || result.Responses[0].Status != 200 || result.Responses[0].Snapshot == "" || len(result.Responses[0].Error) > 0 && string(result.Responses[0].Error) != "null" {
		return nil, fmt.Errorf("source sync requires a successful fresh snapshot")
	}
	return result.Responses[0].Hits.Hits, nil
}

// A no-op cleanup still needs the retained-ID existence guard ordinarily
// supplied atomically by scoped merge. Recheck bounded batches before finishing.
func (w *SyncWriter) verifyRetainedIDs(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += 1000 {
		keys := ids[start:min(start+1000, len(ids))]
		body, _ := json.Marshal(map[string]any{"query": documentIDsQuery(keys), "fields": []string{"_id"}, "limit": len(keys), "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
		raw, err := w.client.Query(ctx, w.table, body)
		if err != nil {
			return err
		}
		hits, err := decodeSyncHits(raw)
		if err != nil {
			return err
		}
		expected := make(map[string]bool, len(keys))
		for _, id := range keys {
			expected[id] = true
		}
		for _, hit := range hits {
			if !expected[hit.ID] {
				return fmt.Errorf("unexpected retained source identity")
			}
			delete(expected, hit.ID)
		}
		if len(expected) > 0 {
			return fmt.Errorf("verified source records are no longer query-visible; refusing completion")
		}
	}
	return nil
}

func (w *SyncWriter) rangeIsClean(ctx context.Context, ids []string) (bool, error) {
	query := map[string]any{"match_all": map[string]any{}}
	if len(ids) > 0 {
		query = map[string]any{"bool": map[string]any{"must_not": []any{documentIDsQuery(ids)}}}
	}
	body, _ := json.Marshal(map[string]any{"filter_prefix": RecordPrefix(w.sourceID), "query": query, "fields": []string{"_id"}, "limit": 1, "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	raw, err := w.client.Query(ctx, w.table, body)
	if err != nil {
		return false, err
	}
	hits, err := decodeSyncHits(raw)
	if err != nil {
		return false, err
	}
	for _, hit := range hits {
		if !strings.HasPrefix(hit.ID, RecordPrefix(w.sourceID)) {
			return false, fmt.Errorf("source range query returned foreign identity")
		}
	}
	return len(hits) == 0, nil
}
func (w *SyncWriter) waitForCleanRange(ctx context.Context, ids []string) error {
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		clean, err := w.rangeIsClean(wait, ids)
		if err != nil {
			return err
		}
		if clean {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("source reconciliation is not query-visible: %w", wait.Err())
		case <-timer.C:
		}
	}
}

// RetireLegacy removes only individually journaled provider IDs after the new
// bounded representation is complete. It cannot delete v2 or foreign rows.
func (w *SyncWriter) RetireLegacy(ctx context.Context, ids []string) error {
	if !w.finished || !w.authoritative {
		return fmt.Errorf("legacy retirement requires complete authoritative reconciliation")
	}
	for _, id := range ids {
		if !ownedID(id, "job/"+w.sourceID) || strings.HasPrefix(id, RecordPrefix(w.sourceID)) {
			return fmt.Errorf("invalid legacy source ownership")
		}
	}
	for start := 0; start < len(ids); start += 100 {
		batch := ids[start:min(start+100, len(ids))]
		if err := w.client.Batch(ctx, w.table, nil, batch); err != nil {
			return err
		}
		if err := WaitForAbsent(ctx, w.client.Query, w.table, batch); err != nil {
			return err
		}
	}
	return nil
}
