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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestNodeShutdownLifecycle(t *testing.T) {
	ms, db := setupTestMetadataStore(t)
	shardID := types.ID(10)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards: map[types.ID]*store.ShardInfo{
			shardID: {Peers: common.NewPeerSet(nodeID), RaftStatus: &common.RaftStatus{Lead: nodeID, Voters: common.NewPeerSet(nodeID)}},
		},
	}))
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    shardID,
		Table: "docs",
		State: store.ShardState_Default,
		ShardInfo: store.ShardInfo{
			Peers:      common.NewPeerSet(nodeID),
			ReportedBy: common.NewPeerSet(nodeID),
			RaftStatus: &common.RaftStatus{Lead: nodeID, Voters: common.NewPeerSet(nodeID)},
		},
	})

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	// Registration after shutdown intent must not clear the durable drain state.
	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))
	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Terminating, storeStatus.State)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"phase":"blocked"`)
	require.Contains(t, statusRec.Body.String(), `"safe_to_terminate":false`)
	require.Contains(t, statusRec.Body.String(), `"blocked":true`)
	require.Contains(t, statusRec.Body.String(), `"blocked_reason":"InsufficientShardVoters"`)
	require.Contains(t, statusRec.Body.String(), `"local_voter_count":1`)
	require.Contains(t, statusRec.Body.String(), `"pending_groups"`)
}

func TestNodeShutdownStatusIgnoresFrozenTerminatingStoreShards(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	shardID := types.ID(10)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards: map[types.ID]*store.ShardInfo{
			shardID: {Peers: common.NewPeerSet(nodeID), RaftStatus: &common.RaftStatus{Lead: nodeID, Voters: common.NewPeerSet(nodeID)}},
		},
	}))

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Terminating, storeStatus.State)
	require.Contains(t, storeStatus.Shards, shardID)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)

	var status nodeShutdownStatus
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &status))
	require.Equal(t, "complete", status.Phase)
	require.True(t, status.SafeToTerminate)
	require.Equal(t, 0, status.Stores[0].GroupStatusCount)
	require.Equal(t, 0, status.Stores[0].RuntimeGroupCount)
}

func TestNodeShutdownStatusUsesGroupStatusVoterCount(t *testing.T) {
	ms, db := setupTestMetadataStore(t)
	shardID := types.ID(10)
	nodeID := types.ID(1)

	shards := nodeGroupStatusReportsToShards(nodeID, []nodeGroupStatusReport{{
		GroupID:     uint64(shardID),
		LocalLeader: true,
		LocalVoter:  true,
		VoterCount:  3,
	}})
	require.Equal(t, 3, shards[shardID].VoterCount)
	writeShardStatus(t, db, &store.ShardStatus{
		ID:        shardID,
		Table:     "docs",
		State:     store.ShardState_Default,
		ShardInfo: *shards[shardID],
	})

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)

	var status nodeShutdownStatus
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &status))
	require.Equal(t, "draining", status.Phase)
	require.False(t, status.Blocked)
	require.Empty(t, status.BlockedReason)
	require.Equal(t, 1, status.Stores[0].LocalVoterCount)
}

func TestNodeShutdownCancellationClearsDrainIntent(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	cancelReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/1/shutdown", nil)
	cancelReq.SetPathValue("node", "1")
	cancelRec := httptest.NewRecorder()
	ms.handleNodeShutdownCancellation(cancelRec, cancelReq)
	require.Equal(t, http.StatusAccepted, cancelRec.Code)

	tombstoned, err := ms.tm.HasStoreTombstone(t.Context(), nodeID)
	require.NoError(t, err)
	require.False(t, tombstoned)

	nodeRecord, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleActive, nodeRecord.Lifecycle)

	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Terminating, storeStatus.State)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"phase":"recovering"`)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))
	statusRec = httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"phase":"active"`)
}

func TestNodeShutdownCancellationDoesNotCreateUnknownNode(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(99)

	cancelReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/99/shutdown", nil)
	cancelReq.SetPathValue("node", "99")
	cancelRec := httptest.NewRecorder()
	ms.handleNodeShutdownCancellation(cancelRec, cancelReq)
	require.Equal(t, http.StatusAccepted, cancelRec.Code)

	_, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.ErrorIs(t, err, tablemgr.ErrNotFound)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/99/shutdown", nil)
	statusReq.SetPathValue("node", "99")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"phase":"not_found"`)
	require.Contains(t, statusRec.Body.String(), `"safe_to_terminate":true`)
}

func TestDeleteTombstonesSkipsCanceledShutdown(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))
	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "test drain"))
	require.NoError(t, ms.tm.ClearStoreTombstone(t.Context(), nodeID))
	require.NoError(t, ms.tm.DeleteTombstones(t.Context(), []types.ID{nodeID}))

	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, nodeID, storeStatus.ID)
	nodeRecord, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleActive, nodeRecord.Lifecycle)
	tombstoned, err := ms.tm.HasStoreTombstone(t.Context(), nodeID)
	require.NoError(t, err)
	require.False(t, tombstoned)
}

func TestDeleteTombstonesPreservesDrainingShutdownUntilFinalized(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))
	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "operator scale-down"))
	require.NoError(t, ms.tm.DeleteTombstones(t.Context(), []types.ID{nodeID}))

	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Terminating, storeStatus.State)
	nodeRecord, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleDraining, nodeRecord.Lifecycle)
	tombstoned, err := ms.tm.HasStoreTombstone(t.Context(), nodeID)
	require.NoError(t, err)
	require.True(t, tombstoned)

	finalizeReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/1", nil)
	finalizeReq.SetPathValue("node", "1")
	finalizeRec := httptest.NewRecorder()
	ms.handleNodeShutdownFinalization(finalizeRec, finalizeReq)
	require.Equal(t, http.StatusAccepted, finalizeRec.Code)

	_, err = ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	_, err = ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	tombstoned, err = ms.tm.HasStoreTombstone(t.Context(), nodeID)
	require.NoError(t, err)
	require.False(t, tombstoned)
}

func TestNodeShutdownFinalizationRefusesActiveNode(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards:    map[types.ID]*store.ShardInfo{},
	}))

	finalizeReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/1", nil)
	finalizeReq.SetPathValue("node", "1")
	finalizeRec := httptest.NewRecorder()
	ms.handleNodeShutdownFinalization(finalizeRec, finalizeReq)
	require.Equal(t, http.StatusConflict, finalizeRec.Code)

	storeStatus, err := ms.tm.GetStoreStatus(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Healthy, storeStatus.State)
	nodeRecord, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleActive, nodeRecord.Lifecycle)
}

func TestNodeShutdownRequestPreservesLifecycleAtomically(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove","reason":"test drain"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	nodeRecord, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleDraining, nodeRecord.Lifecycle)
	require.Equal(t, "test drain", nodeRecord.Reason)
	tombstoned, err := ms.tm.HasStoreTombstone(t.Context(), nodeID)
	require.NoError(t, err)
	require.True(t, tombstoned)
}

func TestNodeShutdownPathUsesDecimalNodeID(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/10/shutdown", strings.NewReader(`{"type":"remove"}`))
	req.SetPathValue("node", "10")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	record, err := ms.tm.GetNodeRecord(t.Context(), types.ID(10))
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleDraining, record.Lifecycle)
	_, err = ms.tm.GetNodeRecord(t.Context(), types.ID(16))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
}

func TestNodeShutdownRejectsInvalidType(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"restart"}`))
	req.SetPathValue("node", "1")
	rec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNodeLifecycleEndpointsRejectZeroNodeID(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	statusReq := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes/0/status", strings.NewReader(`{"health_class":"healthy"}`))
	statusReq.SetPathValue("node", "0")
	statusRec := httptest.NewRecorder()
	ms.handleNodeStatus(statusRec, statusReq)
	require.Equal(t, http.StatusNotFound, statusRec.Code)

	finalizeReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/0", nil)
	finalizeReq.SetPathValue("node", "0")
	finalizeRec := httptest.NewRecorder()
	ms.handleNodeShutdownFinalization(finalizeRec, finalizeReq)
	require.Equal(t, http.StatusNotFound, finalizeRec.Code)

	_, err := ms.tm.GetStoreStatus(t.Context(), types.ID(0))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	_, err = ms.tm.GetNodeRecord(t.Context(), types.ID(0))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
}

func TestNodeShutdownFinalizationRejectsActiveStoreOnlyStatus(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(8)

	require.NoError(t, ms.tm.UpdateStatuses(t.Context(), map[types.ID]*tablemgr.StoreStatus{
		nodeID: {
			StoreInfo: store.StoreInfo{ID: nodeID},
			State:     store.StoreState_Healthy,
			Shards:    map[types.ID]*store.ShardInfo{},
		},
	}))
	_, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.ErrorIs(t, err, tablemgr.ErrNotFound)

	finalizeReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/nodes/8", nil)
	finalizeReq.SetPathValue("node", "8")
	finalizeRec := httptest.NewRecorder()
	ms.handleNodeShutdownFinalization(finalizeRec, finalizeReq)
	require.Equal(t, http.StatusConflict, finalizeRec.Code)
}

func TestNodeShutdownStatusNotFoundIsSafeToTerminate(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)

	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"phase":"not_found"`)
	require.Contains(t, statusRec.Body.String(), `"safe_to_terminate":true`)
}

func TestNodeRegistrationPreservesDrainingLifecycle(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{"node_id":1}`))
	rec := httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)
	record, err := ms.tm.GetNodeRecord(t.Context(), types.ID(1))
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleActive, record.Lifecycle)

	shutdownReq := httptest.NewRequest(http.MethodPut, "/internal/v1/nodes/1/shutdown", strings.NewReader(`{"type":"remove"}`))
	shutdownReq.SetPathValue("node", "1")
	shutdownRec := httptest.NewRecorder()
	ms.handleNodeShutdownRequest(shutdownRec, shutdownReq)
	require.Equal(t, http.StatusAccepted, shutdownRec.Code)

	req = httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{"node_id":1,"lifecycle":"active"}`))
	rec = httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)
	record, err = ms.tm.GetNodeRecord(t.Context(), types.ID(1))
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleDraining, record.Lifecycle)
	require.Equal(t, "operator scale-down", record.Reason)
}

func TestNodeRegistrationRejectsNewDrainingLifecycle(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{"node_id":7,"store_id":7,"lifecycle":"draining"}`))
	rec := httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "node shutdown API")
	_, err := ms.tm.GetNodeRecord(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	_, err = ms.tm.GetStoreStatus(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
}

func TestNodeRegistrationRejectsMissingNodeIdentity(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{"role":"data"}`))
	rec := httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "node_id")
	_, err := ms.tm.GetNodeRecord(t.Context(), types.ID(0))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)

	storeOnlyReq := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{"store_id":7,"role":"data"}`))
	storeOnlyRec := httptest.NewRecorder()
	ms.handleNodeRegistration(storeOnlyRec, storeOnlyReq)

	require.Equal(t, http.StatusBadRequest, storeOnlyRec.Code)
	require.Contains(t, storeOnlyRec.Body.String(), "node_id")
	_, err = ms.tm.GetNodeRecord(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	_, err = ms.tm.GetStoreStatus(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
}

func TestNodeRegistrationUpsertsHostedStore(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{
		"node_id":7,
		"store_id":7,
		"role":"data",
		"health_class":"healthy",
		"live":true,
		"raft_url":"http://store-7:9021",
		"api_url":"http://store-7:12380",
		"shards":{}
	}`))
	rec := httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	status, err := ms.tm.GetStoreStatus(t.Context(), types.ID(7))
	require.NoError(t, err)
	require.Equal(t, types.ID(7), status.ID)
	require.Equal(t, "http://store-7:9021", status.RaftURL)
	require.Equal(t, "http://store-7:12380", status.ApiURL)
}

func TestNodeStatusUpdatesHostedStore(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes", strings.NewReader(`{
		"node_id":7,
		"store_id":7,
		"role":"data",
		"health_class":"healthy",
		"live":true,
		"raft_url":"http://store-7:9021",
		"api_url":"http://store-7:12380"
	}`))
	rec := httptest.NewRecorder()
	ms.handleNodeRegistration(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	statusReq := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes/7/status", strings.NewReader(`{
		"health_class":"degraded",
		"live":true,
		"group_statuses":[{"group_id":11,"local_leader":true,"local_voter":true}]
	}`))
	statusReq.SetPathValue("node", "7")
	statusRec := httptest.NewRecorder()
	ms.handleNodeStatus(statusRec, statusReq)
	require.Equal(t, http.StatusAccepted, statusRec.Code)

	status, err := ms.tm.GetStoreStatus(t.Context(), types.ID(7))
	require.NoError(t, err)
	require.Equal(t, store.StoreState_Unhealthy, status.State)
	require.Equal(t, "http://store-7:9021", status.RaftURL)
	require.Contains(t, status.Shards, types.ID(11))
	require.True(t, status.Shards[types.ID(11)].RaftStatus.Voters.Contains(types.ID(7)))
	require.Equal(t, types.ID(7), status.Shards[types.ID(11)].RaftStatus.Lead)
}

// backdateDrainingSince rewrites the persisted NodeRecord so that the test
// can simulate a drain that has been running long enough to exceed
// nodeShutdownStuckThreshold.
func backdateDrainingSince(t *testing.T, ms *MetadataStore, db interface {
	Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error
}, nodeID types.ID, since time.Time) {
	t.Helper()
	record, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	record.DrainingSince = since
	data, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, db.Batch(t.Context(),
		[][2][]byte{{[]byte("tm:nr:" + nodeID.String()), data}},
		nil,
	))
}

func TestNodeShutdownStatusBypassesStuckShardsAfterThreshold(t *testing.T) {
	ms, db := setupTestMetadataStore(t)
	shardID := types.ID(10)
	nodeID := types.ID(1)
	otherA := types.ID(2)
	otherB := types.ID(3)

	// Three-voter shard so removing nodeID leaves quorum (2 voters >= 1).
	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards: map[types.ID]*store.ShardInfo{
			shardID: {
				Peers:      common.NewPeerSet(nodeID, otherA, otherB),
				ReportedBy: common.NewPeerSet(nodeID, otherA, otherB),
				RaftStatus: &common.RaftStatus{Lead: otherA, Voters: common.NewPeerSet(nodeID, otherA, otherB)},
			},
		},
	}))
	writeShardStatus(t, db, &store.ShardStatus{
		ID: shardID, Table: "docs", State: store.ShardState_Default,
		ShardInfo: store.ShardInfo{
			Peers:      common.NewPeerSet(nodeID, otherA, otherB),
			ReportedBy: common.NewPeerSet(nodeID, otherA, otherB),
			VoterCount: 3,
			RaftStatus: &common.RaftStatus{Lead: otherA, Voters: common.NewPeerSet(nodeID, otherA, otherB)},
		},
	})

	// Request shutdown — node enters Draining with DrainingSince=now.
	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "test"))

	// Within the threshold the shard still counts: SafeToTerminate stays false.
	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	var status nodeShutdownStatus
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &status))
	require.False(t, status.SafeToTerminate)
	require.Equal(t, "draining", status.Phase)
	require.Empty(t, status.BypassedGroups)

	// Backdate so the drain has exceeded the threshold.
	backdateDrainingSince(t, ms, db, nodeID, time.Now().Add(-2*nodeShutdownStuckThreshold))

	statusRec = httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &status))
	require.True(t, status.SafeToTerminate)
	require.Equal(t, "complete", status.Phase)
	require.Contains(t, status.BypassedGroups, shardID)
	require.Equal(t, 0, status.Stores[0].PlacementIntentCount)
	require.Equal(t, 0, status.Stores[0].LocalVoterCount)
	require.Contains(t, status.Message, "drain exceeded")
}

func TestNodeShutdownStatusDoesNotBypassQuorumLossShards(t *testing.T) {
	ms, db := setupTestMetadataStore(t)
	shardID := types.ID(10)
	nodeID := types.ID(1)

	// Single-voter shard: removing this node would leave 0 voters.
	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
		Shards: map[types.ID]*store.ShardInfo{
			shardID: {
				Peers:      common.NewPeerSet(nodeID),
				ReportedBy: common.NewPeerSet(nodeID),
				RaftStatus: &common.RaftStatus{Lead: nodeID, Voters: common.NewPeerSet(nodeID)},
			},
		},
	}))
	writeShardStatus(t, db, &store.ShardStatus{
		ID: shardID, Table: "docs", State: store.ShardState_Default,
		ShardInfo: store.ShardInfo{
			Peers:      common.NewPeerSet(nodeID),
			ReportedBy: common.NewPeerSet(nodeID),
			VoterCount: 1,
			RaftStatus: &common.RaftStatus{Lead: nodeID, Voters: common.NewPeerSet(nodeID)},
		},
	})

	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "test"))
	backdateDrainingSince(t, ms, db, nodeID, time.Now().Add(-2*nodeShutdownStuckThreshold))

	statusReq := httptest.NewRequest(http.MethodGet, "/internal/v1/nodes/1/shutdown", nil)
	statusReq.SetPathValue("node", "1")
	statusRec := httptest.NewRecorder()
	ms.handleNodeShutdownStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)

	var status nodeShutdownStatus
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &status))
	require.False(t, status.SafeToTerminate, "must not bypass shards that would lose quorum")
	require.True(t, status.Blocked)
	require.Equal(t, "InsufficientShardVoters", status.BlockedReason)
	require.Empty(t, status.BypassedGroups)
}

func TestRequestNodeShutdownPreservesDrainingSince(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
	}))

	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "first"))
	first, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.False(t, first.DrainingSince.IsZero())

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "second"))
	second, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, first.DrainingSince, second.DrainingSince,
		"DrainingSince must be preserved across re-requested shutdowns")
}

func TestClearStoreTombstoneClearsDrainingSince(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	nodeID := types.ID(1)

	require.NoError(t, ms.tm.RegisterStore(t.Context(), &store.StoreRegistrationRequest{
		StoreInfo: store.StoreInfo{ID: nodeID},
	}))
	require.NoError(t, ms.tm.RequestNodeShutdown(t.Context(), nodeID, "go"))
	require.NoError(t, ms.tm.ClearStoreTombstone(t.Context(), nodeID))

	record, err := ms.tm.GetNodeRecord(t.Context(), nodeID)
	require.NoError(t, err)
	require.Equal(t, tablemgr.NodeLifecycleActive, record.Lifecycle)
	require.True(t, record.DrainingSince.IsZero())
}

func TestNodeStatusRejectsUnknownNode(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	statusReq := httptest.NewRequest(http.MethodPost, "/internal/v1/nodes/7/status", strings.NewReader(`{
		"health_class":"healthy",
		"live":true,
		"group_statuses":[{"group_id":11,"local_leader":true,"local_voter":true}]
	}`))
	statusReq.SetPathValue("node", "7")
	statusRec := httptest.NewRecorder()
	ms.handleNodeStatus(statusRec, statusReq)

	require.Equal(t, http.StatusNotFound, statusRec.Code)
	_, err := ms.tm.GetStoreStatus(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
	_, err = ms.tm.GetNodeRecord(t.Context(), types.ID(7))
	require.ErrorIs(t, err, tablemgr.ErrNotFound)
}
