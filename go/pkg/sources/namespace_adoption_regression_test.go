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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyPreviewCannotProveNoRekey(t *testing.T) {
	s, connector, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	backend.mu.Lock()
	var oldKey string
	for key := range backend.records {
		oldKey = key
	}
	backend.mu.Unlock()
	require.NotEmpty(t, oldKey)
	original := connector.inventory
	connector.inventory.Objects = nil
	source := s.profile.Sources[0]
	source.Identity.Namespace = "new-unrelated-namespace"
	out := serviceCall(s, "PUT", "/v1/sources/"+source.ID, source)
	require.Equal(t, 200, out.Code)
	var mutation struct {
		Rekeys bool `json:"rekeys"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &mutation))
	connector.inventory = original
	require.Equal(t, "completed", startServiceJob(t, s).State)
	backend.mu.Lock()
	_, retained := backend.records[oldKey]
	count := len(backend.records)
	backend.mu.Unlock()
	require.False(t, retained, "old key must change under the unrelated namespace")
	require.Equal(t, 1, count)
	require.True(t, mutation.Rekeys, "source PUT reported no rekey despite replacing the catalog key")
}
