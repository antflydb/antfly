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
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// QueryDocumentFields covers the complete canonical metadata and the content
// digest. Returning the original text again is unnecessary for readiness.
func QueryDocumentFields() []string {
	return []string{"grep.version", "grep.uri", "grep.path", "grep.revision", "grep.indexed_at", "grep.permissions", "grep.metadata"}
}

func projectionDigest(doc Document) ([32]byte, error) {
	doc.Content = ""
	raw, err := json.Marshal(doc)
	if err != nil {
		return [32]byte{}, err
	}
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&canonical); err != nil {
		return [32]byte{}, err
	}
	raw, err = json.Marshal(canonical)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

// WaitForDocuments is the common publication barrier for hosted and standalone
// writers. A provider revision alone cannot identify a normalized record: its
// URI, path, sharing, connection metadata or normalization may have changed.
func WaitForDocuments(ctx context.Context, query func(context.Context, string, []byte) ([]byte, error), table string, documents map[string]Document) error {
	return waitForDocuments(ctx, query, table, documents, false)
}

// Scoped source writers also verify the outer ownership/filtering projection.
func sourceRowFields() []string { return append(QueryDocumentFields(), "id", "source_id", "doc_type") }
func sourceRowMatches(hit syncHit, sourceID string) bool {
	return hit.Source.ID == hit.ID && hit.Source.SourceID == sourceID && hit.Source.DocType == "source_item"
}

func waitForDocuments(ctx context.Context, query func(context.Context, string, []byte) ([]byte, error), table string, documents map[string]Document, sourceRows bool) error {
	if len(documents) == 0 {
		return nil
	}
	if len(documents) > 1000 {
		return fmt.Errorf("canonical readiness batch exceeds 1000 records")
	}
	keys := make([]string, 0, len(documents))
	expected := make(map[string][32]byte, len(documents))
	for id, doc := range documents {
		if id == "" || doc.Version != 1 || doc.URI == "" || doc.Path == "" || doc.Revision == "" || configString(doc.Metadata, "sha256") == "" {
			return fmt.Errorf("invalid canonical source readiness record")
		}
		digest, err := projectionDigest(doc)
		if err != nil {
			return err
		}
		expected[id] = digest
		keys = append(keys, id)
	}
	sort.Strings(keys)
	fields := QueryDocumentFields()
	if sourceRows {
		fields = sourceRowFields()
	}
	request, _ := json.Marshal(map[string]any{"query": documentIDsQuery(keys), "fields": fields, "limit": len(keys), "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		raw, err := query(wait, table, request)
		if err != nil {
			return err
		}
		hits, err := decodeSyncHits(raw)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		returned := map[string]bool{}
		for _, hit := range hits {
			want, ok := expected[hit.ID]
			if !ok || returned[hit.ID] {
				return fmt.Errorf("unexpected canonical readiness identity")
			}
			returned[hit.ID] = true
			got, err := projectionDigest(hit.Source.Grep)
			if err != nil {
				return err
			}
			if got == want && (!sourceRows || sourceRowMatches(hit, configString(documents[hit.ID].Metadata, "source_id"))) {
				seen[hit.ID] = true
			}
		}
		if len(seen) == len(expected) {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("canonical source projection is not query-visible: %w", wait.Err())
		case <-timer.C:
		}
	}
}
