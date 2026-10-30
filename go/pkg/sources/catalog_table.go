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
	"reflect"
	"time"
)

// DefaultCatalogTable is an ordinary table, not a reserved database resource.
const DefaultCatalogTable = "antfly_sources"
const CatalogRecordVersion = 1

// CatalogRecord contains only portable search metadata. Credentials, provider
// selectors, reader session identities and job checkpoints never belong here.
type CatalogRecord struct {
	Version           int         `json:"version"`
	DocType           string      `json:"doc_type"`
	Source            Binding     `json:"source"`
	Provider          string      `json:"provider"`
	DocumentVersion   int         `json:"document_version"`
	PublishedRevision string      `json:"published_revision,omitempty"`
	Permissions       Permissions `json:"permissions"`
}

func NewCatalogRecord(binding Binding, provider, revision string, permissions Permissions) CatalogRecord {
	if permissions.Readers == nil {
		permissions.Readers = []string{}
	}
	if binding.Alias == "" {
		binding.Alias = binding.ID
	}
	binding.PublishedRevision = revision
	binding.AccessID = "" // Cloud proofs are derived from current authenticated policy.
	return CatalogRecord{Version: CatalogRecordVersion, DocType: "source_catalog", Source: binding, Provider: provider, DocumentVersion: 1, PublishedRevision: revision, Permissions: permissions}
}
func (r CatalogRecord) Validate() error {
	switch r.Provider {
	case ProviderS3, ProviderGCS, ProviderGoogleDrive, ProviderGitHub, ProviderSlack, ProviderDiscord, ProviderGoogleWorkspace:
	default:
		return fmt.Errorf("unsupported catalog provider")
	}
	if r.Version != CatalogRecordVersion || r.DocType != "source_catalog" || !canonicalID(r.Source.ID) || r.Source.Table == "" || r.Source.DocumentField != "grep" || r.DocumentVersion != 1 || r.Provider == "" || r.Source.AccessID != "" {
		return fmt.Errorf("invalid source catalog record")
	}
	var actual, expected any
	if json.Unmarshal(r.Source.Filter, &actual) != nil || json.Unmarshal(SourceFilter(r.Source.ID), &expected) != nil || !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("catalog source filter does not match identity")
	}
	switch r.Source.Status {
	case "pending", "syncing", "stale":
	case "searchable":
		if r.Source.ValidUntil == nil || r.PublishedRevision == "" {
			return fmt.Errorf("searchable catalog record requires revision and freshness")
		}
	default:
		return fmt.Errorf("invalid catalog source status")
	}
	return nil
}

// CatalogTableClient is implemented by Publisher and hosted table adapters.
type CatalogTableClient interface {
	EnsureTable(context.Context, string) error
	Batch(context.Context, string, map[string]any, []string) error
	Query(context.Context, string, []byte) ([]byte, error)
}
type CatalogStore struct {
	Client CatalogTableClient
	Table  string
}

func (s CatalogStore) table() string {
	if s.Table != "" {
		return s.Table
	}
	return DefaultCatalogTable
}
func (s CatalogStore) Publish(ctx context.Context, record CatalogRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Source.Table == s.table() {
		return fmt.Errorf("catalog cannot be a content destination")
	}
	if err := s.Client.EnsureTable(ctx, s.table()); err != nil {
		return err
	}
	if err := s.Client.Batch(ctx, s.table(), map[string]any{record.Source.ID: record}, nil); err != nil {
		return err
	}
	// Await a fresh snapshot before content mutation (syncing) or announcing
	// publication (searchable). Acknowledged writes alone are insufficient.
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	expected, _ := json.Marshal(record)
	for {
		records, err := s.Read(wait, []string{record.Source.ID})
		if err != nil {
			return err
		}
		if actual, ok := records[record.Source.ID]; ok {
			raw, _ := json.Marshal(actual)
			var got, want any
			_ = json.Unmarshal(raw, &got)
			_ = json.Unmarshal(expected, &want)
			if reflect.DeepEqual(got, want) {
				return nil
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("catalog record is not query-visible: %w", wait.Err())
		case <-timer.C:
		}
	}
}

// Read uses bounded ID queries, never an unbounded tenant-wide scan. Missing
// records are pending, not permission to reconstruct searchable state elsewhere.
func (s CatalogStore) Read(ctx context.Context, ids []string) (map[string]CatalogRecord, error) {
	if len(ids) > 1000 {
		return nil, fmt.Errorf("catalog source budget exceeded")
	}
	out := map[string]CatalogRecord{}
	totalBytes := 0
	for start := 0; start < len(ids); start += 100 {
		keys := ids[start:min(start+100, len(ids))]
		query, _ := json.Marshal(map[string]any{"query": documentIDsQuery(keys), "limit": len(keys), "fields": []string{"version", "doc_type", "source", "provider", "document_version", "published_revision", "permissions"}, "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
		data, err := s.Client.Query(ctx, s.table(), query)
		if err != nil {
			return nil, err
		}
		var result struct {
			Responses []struct {
				Status   int             `json:"status"`
				Error    json.RawMessage `json:"error"`
				Snapshot string          `json:"remote_snapshot"`
				Hits     struct {
					Hits []struct {
						ID     string        `json:"_id"`
						Source CatalogRecord `json:"_source"`
					} `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		totalBytes += len(data)
		if len(data) > 8<<20 || totalBytes > 32<<20 {
			return nil, fmt.Errorf("catalog response exceeds budget")
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		if len(result.Responses) != 1 || result.Responses[0].Status != 200 || result.Responses[0].Snapshot == "" || len(result.Responses[0].Error) > 0 && string(result.Responses[0].Error) != "null" {
			return nil, fmt.Errorf("catalog query requires a successful snapshot")
		}
		allowed := map[string]bool{}
		for _, id := range keys {
			allowed[id] = true
		}
		for _, hit := range result.Responses[0].Hits.Hits {
			if !allowed[hit.ID] || hit.ID != hit.Source.Source.ID {
				return nil, fmt.Errorf("catalog identity mismatch")
			}
			if _, ok := out[hit.ID]; ok {
				return nil, fmt.Errorf("duplicate catalog identity")
			}
			if err := hit.Source.Validate(); err != nil {
				return nil, err
			}
			out[hit.ID] = hit.Source
		}
	}
	return out, nil
}
func (p *Publisher) Query(ctx context.Context, table string, body []byte) ([]byte, error) {
	return p.callRead(ctx, "POST", tablePath(table)+"/query", json.RawMessage(body))
}

// Retire hides only catalog entries owned by the caller's durable source profile;
// content and its ownership state are retained for explicit later recovery/purge.
func (s CatalogStore) Retire(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += 100 {
		batch := ids[start:min(start+100, len(ids))]
		if err := s.Client.Batch(ctx, s.table(), nil, batch); err != nil {
			return err
		}
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := func() error {
			for {
				records, err := s.Read(wait, batch)
				if err != nil {
					return err
				}
				if len(records) == 0 {
					return nil
				}
				timer := time.NewTimer(100 * time.Millisecond)
				select {
				case <-wait.Done():
					timer.Stop()
					return wait.Err()
				case <-timer.C:
				}
			}
		}()
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
