// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/metadata"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestE2E_ClusteredMetadataBootstrapsWithAuth(t *testing.T) {
	skipInShortMode(t)

	ctx := testContext(t, 45*time.Second)
	logger := GetTestLogger(t)
	const nodeCount = 3

	apiURLs := make(map[string]string, nodeCount)
	raftURLs := make(map[string]string, nodeCount)
	peers := make(common.Peers, 0, nodeCount)
	for id := 1; id <= nodeCount; id++ {
		nodeID := types.ID(id)
		apiURLs[nodeID.String()] = fmt.Sprintf("http://localhost:%d", GetFreePort(t))
		raftURLs[nodeID.String()] = fmt.Sprintf("http://localhost:%d", GetFreePort(t))
		peers = append(peers, common.Peer{ID: nodeID, URL: raftURLs[nodeID.String()]})
	}

	ready := make([]chan struct{}, 0, nodeCount)
	for id := 1; id <= nodeCount; id++ {
		nodeID := types.ID(id)
		cfg := CreateTestConfig(t, t.TempDir(), nodeID)
		cfg.EnableAuth = true
		cfg.SwarmMode = false
		cfg.ReplicationFactor = 1
		cfg.Metadata.OrchestrationUrls = apiURLs

		readyC := make(chan struct{})
		ready = append(ready, readyC)
		metaConf := &store.StoreInfo{
			ID:      nodeID,
			RaftURL: raftURLs[nodeID.String()],
			ApiURL:  apiURLs[nodeID.String()],
		}

		go metadata.RunAsMetadataServer(
			ctx,
			logger.Named("metadata").With(zap.Uint64("node_id", uint64(nodeID))),
			cfg,
			metaConf,
			peers,
			false,
			readyC,
			nil,
		)
	}

	for i, readyC := range ready {
		select {
		case <-readyC:
		case <-ctx.Done():
			t.Fatalf("metadata node %d did not open its API listener: %v", i+1, context.Cause(ctx))
		}
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for id := 1; id <= nodeCount; id++ {
			nodeID := types.ID(id)
			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				apiURLs[nodeID.String()]+"/db/v1/status",
				nil,
			)
			require.NoError(c, err)
			for k, values := range basicAuth("admin", "admin") {
				for _, value := range values {
					req.Header.Add(k, value)
				}
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(c, err)
			_ = resp.Body.Close()
			require.Equal(c, http.StatusOK, resp.StatusCode)
		}
	}, 20*time.Second, 250*time.Millisecond)
}
