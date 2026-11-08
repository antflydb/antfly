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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceAdoptionResumesLegacyPinnedInventory(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	err := runner.Sync(t.Context())
	require.NoError(t, err)
	originalKey, err := RecordKey(testSourceID, connector.inventory.Objects[0].ID)
	require.NoError(t, err)
	statePath := filepath.Join(runner.StateDir, testSourceID+".json")
	var state runState
	require.NoError(t, loadJSON(statePath, &state, 256<<20))
	state.Pending = state.Published
	for i := range state.Pending.Objects {
		state.Pending.Objects[i].RelativeKey = ""
		state.Pending.Objects[i].RelativeKeySet = false
	}
	require.NoError(t, writeJSON(statePath, state, 256<<20))
	connector.inventory.Objects[0].RelativeKey = "manual.txt"
	ns, err := LegacyNamespace(testSourceID, connector.inventory.Objects)
	require.NoError(t, err)
	runner.Profile.Sources[0].Identity.Namespace = ns
	err = runner.Sync(t.Context())
	require.NoError(t, err)
	backend.mu.Lock()
	_, retained := backend.records[originalKey]
	backend.mu.Unlock()
	require.True(t, retained, "adopting the legacy namespace must not rekey an upgraded pinned run")
	require.Equal(t, 1, connector.enumerations, "resume must preserve the pinned selection without enumeration")
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	_, retained = backend.records[originalKey]
	count := len(backend.records)
	backend.mu.Unlock()
	require.True(t, retained, "the next fresh inventory must agree with the upgraded checkpoint")
	require.Equal(t, 1, count)
}

func TestRestoreInventoryRelativeKeys(t *testing.T) {
	for _, tc := range []struct{ provider, config, suffix, relative string }{
		{ProviderLocal, `{"prefix":"nested/store"}`, "nested/store/objects/sha256/a.txt", "objects/sha256/a.txt"},
		{ProviderS3, `{"bucket":"docs","prefix":"folder"}`, "docs/folder/a", "/a"},
		{ProviderS3, `{"bucket":"docs","prefix":"folder/"}`, "docs/folder/a", "a"},
		{ProviderGCS, `{"bucket":"docs","prefix":"folder"}`, "docs/folder", ""},
		{ProviderGCS, `{"bucket":"docs","prefix":"folder/"}`, "docs/folder//a", "/a"},
	} {
		t.Run(tc.provider+tc.suffix, func(t *testing.T) {
			scheme := tc.provider
			if scheme == ProviderGCS {
				scheme = "gcs"
			}
			item := Item{ID: scheme + "://job/" + testSourceID + "/" + tc.suffix, SourceKind: tc.provider}
			inventory := &Inventory{Objects: []Item{item}}
			source := Source{ID: testSourceID, Config: json.RawMessage(tc.config)}
			require.NoError(t, restoreInventoryRelativeKeys(source, tc.provider, inventory))
			require.Equal(t, tc.relative, inventory.Objects[0].RelativeKey)
			require.True(t, inventory.Objects[0].RelativeKeySet)
			require.NoError(t, restoreInventoryRelativeKeys(source, tc.provider, inventory), "restoration must be idempotent")
			inventory.Objects[0].RelativeKey = "wrong"
			require.Error(t, restoreInventoryRelativeKeys(source, tc.provider, inventory))
			inventory.Objects[0] = item
			inventory.Objects[0].ID = "s3://job/" + testSourceID + "/unrelated/file"
			require.Error(t, restoreInventoryRelativeKeys(source, tc.provider, inventory))
		})
	}
}
