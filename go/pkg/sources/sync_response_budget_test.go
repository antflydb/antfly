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
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPublicationProofRestoreAdaptsToResponseBudget(t *testing.T) {
	runner, _, _ := runnerFixture(t)
	var items []Item
	for i := 0; i < 64; i++ {
		items = append(items, Item{ID: fmt.Sprintf("s3://job/%s/docs/page-%d.txt", testSourceID, i), Path: fmt.Sprintf("page-%d.txt", i), Filename: "page.txt", URL: "s3://docs/page.txt", Revision: "1", Size: 200 << 10, SourceKind: ProviderS3})
	}
	writer, err := NewSyncWriterWithIdentity(runner.Publisher, "sources", testSourceID, "", items, true)
	require.NoError(t, err)
	writer.TrackPublicationProofs()
	text := []byte(strings.Repeat("x", 200<<10))
	for start := 0; start < len(items); start += 8 {
		docs := map[string]Document{}
		for _, item := range items[start : start+8] {
			doc, err := Normalize(testSourceID, item, text, Permissions{Public: true}, time.Now())
			require.NoError(t, err)
			key, err := RecordKey(testSourceID, item.ID)
			require.NoError(t, err)
			docs[key] = doc
		}
		_, err = writer.Upsert(t.Context(), docs)
		require.NoError(t, err)
	}
	resumed, err := NewSyncWriterWithIdentity(runner.Publisher, "sources", testSourceID, "", items, true)
	require.NoError(t, err)
	matches, err := resumed.RestorePublicationProofs(t.Context(), writer.PublicationProofs())
	require.NoError(t, err, "valid content loaded in bounded pages must also be restorable within the response ceiling")
	require.True(t, matches)
}

type proofQueryFailureClient struct {
	SyncClient
	err   error
	calls int
}

func (c *proofQueryFailureClient) Query(context.Context, string, []byte) ([]byte, error) {
	c.calls++
	return nil, c.err
}
func TestPublicationProofRestoreDoesNotSplitUnrelatedFailures(t *testing.T) {
	for _, tc := range []struct {
		name             string
		count, wantCalls int
		err              error
	}{
		{"provider denial", 2, 1, &PublishHTTPError{Status: http.StatusForbidden}},
		{"single oversized projection", 1, 1, &ResponseBudgetError{LimitBytes: 1024}},
		{"bounded split terminates", 2, 2, &ResponseBudgetError{LimitBytes: 1024}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &proofQueryFailureClient{err: tc.err}
			var items []Item
			proofs := map[string]string{}
			for i := 0; i < tc.count; i++ {
				id := fmt.Sprintf("s3://job/%s/docs/%d.txt", testSourceID, i)
				items = append(items, Item{ID: id})
				key, err := RecordKey(testSourceID, id)
				require.NoError(t, err)
				proofs[key] = strings.Repeat("a", 64)
			}
			writer, err := NewSyncWriterWithIdentity(client, "sources", testSourceID, "", items, true)
			require.NoError(t, err)
			matches, err := writer.RestorePublicationProofs(t.Context(), proofs)
			require.ErrorIs(t, err, tc.err)
			require.False(t, matches)
			require.Equal(t, tc.wantCalls, client.calls)
			require.Empty(t, writer.verified)
		})
	}
}
