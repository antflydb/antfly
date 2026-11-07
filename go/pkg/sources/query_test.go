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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncGateAcceptsOnlyAnExplicitReadSource(t *testing.T) {
	hits := `"hits":{"hits":[{"_id":"a"}]}`
	for name, tc := range map[string]struct {
		body string
		ok   bool
	}{
		"retained snapshot": {`{"responses":[{"status":200,"remote_snapshot":"token",` + hits + `}]}`, true},
		"live read":         {`{"responses":[{"status":200,"live_read":true,` + hits + `}]}`, true},
		"neither":           {`{"responses":[{"status":200,` + hits + `}]}`, false},
		"live read denied":  {`{"responses":[{"status":200,"live_read":false,` + hits + `}]}`, false},
		"failed live read":  {`{"responses":[{"status":500,"live_read":true,` + hits + `}]}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeSyncHits([]byte(tc.body))
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
		})
	}
}

// Model an acknowledged primary row whose insertion is still missing from Lite's
// index. An empty complete inventory must delete it, even when the live query
// returns no hits. No concurrent writers are needed to expose the failure.
type laggingLiteCleanupClient struct {
	t               *testing.T
	primaryHasRow   bool
	merges, queries int
}

func (c *laggingLiteCleanupClient) Query(context.Context, string, []byte) ([]byte, error) {
	c.queries++
	return []byte(`{"responses":[{"status":200,"live_read":true,"hits":{"hits":[]}}]}`), nil
}

func (c *laggingLiteCleanupClient) Merge(_ context.Context, _ string, body []byte) ([]byte, error) {
	var request ScopedMergeRequest
	require.NoError(c.t, json.Unmarshal(body, &request))
	page := request.ScopedMerge
	require.True(c.t, page.DeleteMissing)
	require.Equal(c.t, "full_index", page.SyncLevel)
	c.merges++
	c.primaryHasRow = false
	return json.Marshal(MergeResult{Status: "success", Deleted: 1, NextCursor: page.LastMergedID, KeyRange: KeyRange{From: page.LastMergedID, To: page.KeyRange.To}})
}

func (c *laggingLiteCleanupClient) Batch(context.Context, string, map[string]any, []string) error {
	c.t.Fatal("cleanup must use bounded scoped merge")
	return nil
}

func TestLiteEmptyInventoryReconcilesRowsPendingIndexing(t *testing.T) {
	client := &laggingLiteCleanupClient{t: t, primaryHasRow: true}
	writer, err := NewSyncWriter(client, "docs", testSourceID, nil, true)
	require.NoError(t, err)
	result, err := writer.Finish(t.Context())
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, 1, client.merges)
	require.Equal(t, 2, client.queries)
	require.False(t, client.primaryHasRow)
}
