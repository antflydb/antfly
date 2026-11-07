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
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyMigrationKeepsKeysWhenCopiedLocalObjectDisappears(t *testing.T) {
	opts, root, stateDir, _, _, backend := migrateServiceFixture(t)
	service, err := NewService(ServiceOptions{Profile: opts.Profile, StateDir: stateDir, PublicURL: opts.PublicURL, AdminToken: opts.AdminToken, MediaKey: opts.MediaKey})
	require.NoError(t, err)
	out, err := serviceAdminCall(t.Context(), service, http.MethodPost, "/v1/sync", map[string]any{"source_id": testSourceID})
	require.NoError(t, err)
	var job SyncJob
	require.NoError(t, json.Unmarshal(out.body.Bytes(), &job))
	require.Equal(t, "completed", waitServiceJob(t, service, job.ID).State)
	require.NoError(t, service.Close())
	backend.mu.Lock()
	count := len(backend.records)
	var originalKey string
	for key := range backend.records {
		originalKey = key
	}
	backend.mu.Unlock()
	require.Equal(t, 1, count)
	opts.Progress = func(object MigrateLocalObject) {
		if object.Status == "copied" {
			require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(object.Path))))
		}
	}
	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, result.CatalogRepointed)
	require.False(t, result.Rekeyed)
	backend.mu.Lock()
	defer backend.mu.Unlock()
	_, retained := backend.records[originalKey]
	require.True(t, retained, "copied object must retain its catalog key; reported rekey=%v", result.Rekeyed)
}

func TestLocalIdentityNamespaceDoesNotDependOnInventory(t *testing.T) {
	for _, prefix := range []string{"", ".", "assets", "nested/assets"} {
		t.Run(prefix, func(t *testing.T) {
			config, err := json.Marshal(LocalConfig{Prefix: prefix})
			require.NoError(t, err)
			request := Request{SourceNamespace: "job/" + testSourceID, Config: config}
			namespace, err := localIdentityNamespace(request)
			require.NoError(t, err)
			expected := "local://job/" + testSourceID
			if prefix != "" && prefix != "." {
				expected += "/" + prefix
			}
			require.Equal(t, expected, namespace)
			// A missing mount must not make an arbitrary namespace look equivalent.
			service := &Service{}
			source := Source{Config: config}
			connection := Connection{Provider: ProviderLocal}
			require.True(t, service.namespaceReproducesExistingKeys(t.Context(), testSourceID, source, connection, expected))
			require.False(t, service.namespaceReproducesExistingKeys(t.Context(), testSourceID, source, connection, "unrelated"))
		})
	}
}
