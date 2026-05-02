package metadata

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
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
	require.Contains(t, statusRec.Body.String(), `"phase":"draining"`)
	require.Contains(t, statusRec.Body.String(), `"safe_to_terminate":false`)
	require.Contains(t, statusRec.Body.String(), `"local_voter_count":1`)
}
