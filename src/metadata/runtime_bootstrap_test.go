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

package metadata

import (
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewRuntimeDoesNotSeedAdminBeforeRaftStart(t *testing.T) {
	cache := pebbleutils.NewCache(8 << 20)
	t.Cleanup(cache.Close)

	conf := &store.StoreInfo{
		ID:      types.ID(1),
		RaftURL: "http://127.0.0.1:0",
		ApiURL:  "http://127.0.0.1:0",
	}
	peers := common.Peers{
		{ID: types.ID(1), URL: "http://127.0.0.1:19000"},
		{ID: types.ID(2), URL: "http://127.0.0.1:19001"},
		{ID: types.ID(3), URL: "http://127.0.0.1:19002"},
	}
	cfg := &common.Config{
		EnableAuth:               true,
		DefaultShardsPerTable:    1,
		ReplicationFactor:        3,
		MaxShardSizeBytes:        64 << 20,
		MaxShardsPerTable:        16,
		ShardCooldownPeriod:      time.Second,
		SplitTimeout:             time.Minute,
		SplitFinalizeGracePeriod: time.Second,
		Storage: common.StorageConfig{
			Data:     common.StorageBackendLocal,
			Metadata: common.StorageBackendLocal,
			Local: common.LocalStorageConfig{
				BaseDir: t.TempDir(),
			},
		},
		Metadata: common.MetadataInfo{
			OrchestrationUrls: map[string]string{
				"1": "http://127.0.0.1:8080",
				"2": "http://127.0.0.1:8081",
				"3": "http://127.0.0.1:8082",
			},
		},
	}

	type result struct {
		runtime *Runtime
		err     error
	}
	done := make(chan result, 1)
	go func() {
		runtime, err := NewRuntime(zap.NewNop(), cfg, conf, peers, false, cache, RuntimeOptions{})
		done <- result{runtime: runtime, err: err}
	}()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.NotNil(t, res.runtime)
		require.NoError(t, res.runtime.Close())
	case <-time.After(2 * time.Second):
		t.Fatal("NewRuntime blocked before Raft start; default admin seed must not run during runtime construction")
	}
}
