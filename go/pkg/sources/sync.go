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
	client                  SyncClient
	table, sourceID         string
	bounds                  KeyRange
	inventory, verified     map[string]bool
	authoritative, finished bool
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

// Upsert stabilizes indexing time when the complete normalized projection is
// unchanged, then uses scoped LinearMerge's content hashes to avoid rewrites.
// documents is updated with the exact projection verified by the shared barrier.
func (w *SyncWriter) Upsert(ctx context.Context, documents map[string]Document) (MergeResult, error) {
	if w.finished || len(documents) == 0 || len(documents) > 1000 {
		return MergeResult{}, fmt.Errorf("invalid source merge page")
	}
	ids := make([]string, 0, len(documents))
	for id, doc := range documents {
		key, err := RecordKey(w.sourceID, configString(doc.Metadata, "identity"))
		if err != nil || key != id || !w.inventory[id] || configString(doc.Metadata, "source_id") != w.sourceID || doc.Version != 1 || configString(doc.Metadata, "sha256") == "" {
			return MergeResult{}, fmt.Errorf("record outside pinned source inventory")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	body, _ := json.Marshal(map[string]any{"query": map[string]any{"ids": ids}, "fields": QueryDocumentFields(), "limit": len(ids), "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	raw, err := w.client.Query(ctx, w.table, body)
	if err != nil {
		return MergeResult{}, err
	}
	hits, err := decodeSyncHits(raw)
	if err != nil {
		return MergeResult{}, err
	}
	seen := map[string]bool{}
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
		if a == b {
			doc.IndexedAt = hit.Source.Grep.IndexedAt
			documents[hit.ID] = doc
		}
	}
	rows := make(map[string]any, len(documents))
	for id, doc := range documents {
		rows[id] = map[string]any{"id": id, "source_id": w.sourceID, "doc_type": "source_item", "grep": doc}
	}
	// Truncating the fixed-length hex key gives a narrow lower bound. Deletes
	// are disabled, so partial or out-of-order pages never remove gap records.
	cursor := ids[0][:len(ids[0])-1]
	result, err := w.merge(ctx, ScopedMergePage{KeyRange: w.bounds, Records: rows, LastMergedID: cursor, SyncLevel: "write"}, ids[len(ids)-1], len(ids))
	if err != nil {
		return result, err
	}
	if result.Deleted != 0 {
		return result, fmt.Errorf("non-destructive source merge reported deletions")
	}
	if err := WaitForDocuments(ctx, w.client.Query, w.table, documents); err != nil {
		return result, err
	}
	for id := range documents {
		w.verified[id] = true
	}
	return result, nil
}

// Finish never removes missing rows for partial inventories or repairs. A full
// run retains only already verified IDs, avoiding a second upload of all text.
func (w *SyncWriter) Finish(ctx context.Context) (MergeResult, error) {
	if w.finished {
		return MergeResult{}, fmt.Errorf("source sync already finished")
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
	page := ScopedMergePage{KeyRange: w.bounds, Records: map[string]any{}, RetainIDs: ids, LastMergedID: w.bounds.From, DeleteMissing: true, SyncLevel: "write"}
	result, err := w.merge(ctx, page, w.bounds.To, len(ids))
	if err != nil {
		return result, err
	}
	if result.Upserted != 0 || result.Skipped != len(ids) {
		return result, fmt.Errorf("source reconciliation did not retain every verified record")
	}
	if err := w.waitForCleanRange(ctx, ids); err != nil {
		return result, err
	}
	w.finished = true
	return result, nil
}
func (w *SyncWriter) merge(ctx context.Context, page ScopedMergePage, expectedTo string, count int) (MergeResult, error) {
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
	var result MergeResult
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
		Grep Document `json:"grep"`
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
func (w *SyncWriter) waitForCleanRange(ctx context.Context, ids []string) error {
	query := map[string]any{"match_all": map[string]any{}}
	if len(ids) > 0 {
		query = map[string]any{"bool": map[string]any{"must_not": []any{map[string]any{"ids": ids}}}}
	}
	body, _ := json.Marshal(map[string]any{"filter_prefix": RecordPrefix(w.sourceID), "query": query, "fields": []string{"_id"}, "limit": 1, "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		raw, err := w.client.Query(wait, w.table, body)
		if err != nil {
			return err
		}
		hits, err := decodeSyncHits(raw)
		if err != nil {
			return err
		}
		if len(hits) == 0 {
			return nil
		}
		for _, hit := range hits {
			if !strings.HasPrefix(hit.ID, RecordPrefix(w.sourceID)) {
				return fmt.Errorf("source range query returned foreign identity")
			}
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
