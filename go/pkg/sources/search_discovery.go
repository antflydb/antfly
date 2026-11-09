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
	"os"
	"path/filepath"
)

// RefreshSearchDiscovery republishes consumer declarations over an already
// acquired generation. It never acquires content or extends its access expiry.
// A configuration or destination mismatch requires a normal source run.
func (r *Runner) RefreshSearchDiscovery(ctx context.Context, sourceID string) error {
	if r.Publisher == nil {
		return fmt.Errorf("discovery publication requires a publisher")
	}
	if r.CatalogPath == "" {
		r.CatalogPath = filepath.Join(r.StateDir, "catalog.json")
	}
	return r.withStateLock(ctx, func() error {
		var catalog Catalog
		if err := loadJSON(r.CatalogPath, &catalog, 32<<20); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if catalog.Version != 2 || catalog.Endpoint != r.Profile.Endpoint {
			return fmt.Errorf("discovery destination mismatch")
		}
		catalogTable := r.Profile.CatalogTable
		if catalogTable == "" {
			catalogTable = DefaultCatalogTable
		}
		var destination struct{ Endpoint, Table string }
		if err := loadJSON(filepath.Join(r.StateDir, "catalog-destination.json"), &destination, 1<<20); err != nil {
			return err
		}
		if destination.Endpoint != r.Profile.Endpoint || destination.Table != catalogTable {
			return fmt.Errorf("discovery destination mismatch")
		}
		for _, source := range r.Profile.Sources {
			if source.ID != sourceID {
				continue
			}
			if err := ValidateKnowledgeProcessing(source.Knowledge, source.Processing, r.KnowledgeAudience); err != nil {
				return err
			}
			if source.Permissions == nil {
				return fmt.Errorf("source permissions are required")
			}
			if err := ValidateSearchBindings(source.SearchVersion, source.Streams, source.Artifacts); err != nil {
				return err
			}
			connection, ok := r.Profile.Connections[source.Connection]
			if !ok {
				return nil
			}
			identityConnection := connection
			if connection.Provider == ProviderGoogleWorkspace {
				identityConnection.Services = nil
			}
			hash, err := sourceConfigurationHash(source, identityConnection)
			if err != nil {
				return err
			}
			var state runState
			if err := loadJSON(filepath.Join(r.StateDir, source.ID+".json"), &state, 256<<20); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
			if state.ConfigHash != hash || state.SourceID != source.ID || state.Table != source.Table || state.Endpoint != catalog.Endpoint {
				return nil
			}
			for i := range catalog.Sources {
				binding := &catalog.Sources[i]
				if binding.ID != source.ID || binding.Table != source.Table {
					continue
				}
				if binding.DocumentVersion != SourceRecordVersion {
					return nil
				}
				binding.Knowledge = source.Knowledge
				binding.SearchVersion, binding.Streams, binding.Artifacts = source.SearchVersion, source.Streams, source.Artifacts
				if len(binding.Streams) == 0 && source.Publication != "media" {
					binding.Streams = DefaultSearchStreams(source.SearchVersion)
				}
				if source.Alias != "" {
					binding.Alias = source.Alias
				}
				record := NewCatalogRecord(*binding, connection.Provider, binding.PublishedRevision, *source.Permissions)
				if err := (CatalogStore{Client: r.Publisher, Table: r.Profile.CatalogTable}).Publish(ctx, record); err != nil {
					return err
				}
				return writeJSON(r.CatalogPath, catalog, 32<<20)
			}
		}
		return nil
	})
}
