package metadata

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/tablemgr"
	"go.uber.org/zap"
)

type nodeShutdownRequest struct {
	Type   string `json:"type,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type nodeShutdownStatus struct {
	NodeID          types.ID                  `json:"node_id"`
	Type            string                    `json:"type,omitempty"`
	Phase           string                    `json:"phase"`
	SafeToTerminate bool                      `json:"safe_to_terminate"`
	Stores          []nodeShutdownStoreStatus `json:"stores,omitempty"`
	PendingShards   []types.ID                `json:"pending_shards,omitempty"`
}

type nodeShutdownStoreStatus struct {
	StoreID              types.ID `json:"store_id"`
	PlacementIntentCount int      `json:"placement_intent_count"`
	GroupStatusCount     int      `json:"group_status_count"`
	RuntimeGroupCount    int      `json:"runtime_group_count"`
	LocalVoterCount      int      `json:"local_voter_count"`
	LocalLeaderCount     int      `json:"local_leader_count"`
}

func (ms *MetadataStore) handleNodeShutdownRequest(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}
	if r.Body != nil {
		var req nodeShutdownRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if !errors.Is(err, io.EOF) {
				errorResponse(w, "Invalid node shutdown request", http.StatusBadRequest)
				return
			}
		}
	}

	if err := ms.tm.TombstoneStore(r.Context(), nodeID); err != nil {
		ms.logger.Error("Failed to request node shutdown", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("requesting node shutdown: %w", err).Error(), http.StatusBadRequest)
		return
	}
	ms.TriggerReconciliation()
	ms.logger.Info("Node shutdown requested", zap.Stringer("nodeID", nodeID))

	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("accepted")); err != nil {
		ms.logger.Warn("Failed to write node shutdown response", zap.Error(err))
	}
}

func (ms *MetadataStore) handleNodeShutdownStatus(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}

	status, err := ms.buildNodeShutdownStatus(r, nodeID)
	if err != nil {
		ms.logger.Error("Failed to build node shutdown status", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("getting node shutdown status: %w", err).Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		ms.logger.Warn("Failed to marshal node shutdown status", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func parseNodeIDPathValue(w http.ResponseWriter, r *http.Request) (types.ID, bool) {
	nodeID, err := types.IDFromString(r.PathValue("node"))
	if err != nil {
		errorResponse(w, "Invalid node ID", http.StatusBadRequest)
		return 0, false
	}
	return nodeID, true
}

func (ms *MetadataStore) buildNodeShutdownStatus(r *http.Request, nodeID types.ID) (*nodeShutdownStatus, error) {
	ctx := r.Context()
	tombstoned, err := ms.tm.HasStoreTombstone(ctx, nodeID)
	if err != nil {
		return nil, err
	}

	storeKnown := tombstoned
	storeStatus, err := ms.tm.GetStoreStatus(ctx, nodeID)
	if err == nil {
		storeKnown = true
	} else if err != nil && !errors.Is(err, tablemgr.ErrNotFound) {
		return nil, err
	}

	status := &nodeShutdownStatus{
		NodeID: nodeID,
		Type:   "remove",
		Stores: []nodeShutdownStoreStatus{{StoreID: nodeID}},
	}
	if storeStatus != nil {
		status.Stores[0].GroupStatusCount = len(storeStatus.Shards)
		status.Stores[0].RuntimeGroupCount = len(storeStatus.Shards)
		for shardID := range storeStatus.Shards {
			status.PendingShards = appendUniqueID(status.PendingShards, shardID)
		}
	}

	shards, err := ms.tm.GetShardStatuses()
	if err != nil {
		return nil, err
	}
	for shardID, shard := range shards {
		if shard == nil {
			continue
		}
		if shard.Peers.Contains(nodeID) {
			status.Stores[0].PlacementIntentCount++
			status.PendingShards = appendUniqueID(status.PendingShards, shardID)
		}
		if shard.RaftStatus != nil {
			if shard.RaftStatus.Voters.Contains(nodeID) {
				status.Stores[0].LocalVoterCount++
				status.PendingShards = appendUniqueID(status.PendingShards, shardID)
			}
			if shard.RaftStatus.Lead == nodeID {
				status.Stores[0].LocalLeaderCount++
				status.PendingShards = appendUniqueID(status.PendingShards, shardID)
			}
		}
	}

	shutdownStoreStatus := status.Stores[0]
	status.SafeToTerminate = shutdownStoreStatus.PlacementIntentCount == 0 &&
		shutdownStoreStatus.GroupStatusCount == 0 &&
		shutdownStoreStatus.RuntimeGroupCount == 0 &&
		shutdownStoreStatus.LocalVoterCount == 0 &&
		shutdownStoreStatus.LocalLeaderCount == 0
	switch {
	case !storeKnown && status.SafeToTerminate:
		status.Phase = "not_found"
	case status.SafeToTerminate:
		status.Phase = "complete"
	default:
		status.Phase = "draining"
	}
	return status, nil
}

func appendUniqueID(ids []types.ID, id types.ID) []types.ID {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}
