package metadata

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
	"go.uber.org/zap"
)

type nodeShutdownRequest struct {
	Type   string `json:"type,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type nodeRegistrationRequest struct {
	NodeID      uint64                        `json:"node_id"`
	StoreID     uint64                        `json:"store_id,omitempty"`
	Role        string                        `json:"role,omitempty"`
	HealthClass string                        `json:"health_class,omitempty"`
	Live        *bool                         `json:"live,omitempty"`
	Lifecycle   string                        `json:"lifecycle,omitempty"`
	Reason      string                        `json:"reason,omitempty"`
	RaftURL     string                        `json:"raft_url,omitempty"`
	APIURL      string                        `json:"api_url,omitempty"`
	Shards      map[types.ID]*store.ShardInfo `json:"shards,omitempty"`
	GroupStatus []nodeGroupStatusReport       `json:"group_statuses,omitempty"`
}

type nodeStatusRequest struct {
	StoreID     uint64                  `json:"store_id,omitempty"`
	Live        *bool                   `json:"live,omitempty"`
	HealthClass string                  `json:"health_class,omitempty"`
	GroupStatus []nodeGroupStatusReport `json:"group_statuses,omitempty"`
}

type nodeGroupStatusReport struct {
	GroupID     uint64 `json:"group_id"`
	LocalLeader bool   `json:"local_leader,omitempty"`
	LocalVoter  bool   `json:"local_voter,omitempty"`
	VoterCount  int    `json:"voter_count,omitempty"`
}

type nodeShutdownStatus struct {
	NodeID          uint64                    `json:"node_id"`
	Type            string                    `json:"type,omitempty"`
	Phase           string                    `json:"phase"`
	SafeToTerminate bool                      `json:"safe_to_terminate"`
	Blocked         bool                      `json:"blocked,omitempty"`
	BlockedReason   string                    `json:"blocked_reason,omitempty"`
	Message         string                    `json:"message,omitempty"`
	Stores          []nodeShutdownStoreStatus `json:"stores,omitempty"`
	PendingGroups   []types.ID                `json:"pending_groups,omitempty"`
}

type nodeShutdownStoreStatus struct {
	StoreID              uint64 `json:"store_id"`
	PlacementIntentCount int    `json:"placement_intent_count"`
	GroupStatusCount     int    `json:"group_status_count"`
	RuntimeGroupCount    int    `json:"runtime_group_count"`
	LocalVoterCount      int    `json:"local_voter_count"`
	LocalLeaderCount     int    `json:"local_leader_count"`
}

func (ms *MetadataStore) handleNodeShutdownRequest(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}
	reason := "operator scale-down"
	if r.Body != nil {
		var req nodeShutdownRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if !errors.Is(err, io.EOF) {
				errorResponse(w, "Invalid node shutdown request", http.StatusBadRequest)
				return
			}
		}
		if req.Type != "" && req.Type != "remove" {
			errorResponse(w, `Invalid node shutdown type: expected "remove"`, http.StatusBadRequest)
			return
		}
		if req.Reason != "" {
			reason = req.Reason
		}
	}

	if err := ms.tm.RequestNodeShutdown(r.Context(), nodeID, reason); err != nil {
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

func (ms *MetadataStore) handleNodeShutdownCancellation(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}

	if err := ms.tm.ClearStoreTombstone(r.Context(), nodeID); err != nil {
		ms.logger.Error("Failed to cancel node shutdown", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("canceling node shutdown: %w", err).Error(), http.StatusBadRequest)
		return
	}
	ms.TriggerReconciliation()
	ms.logger.Info("Node shutdown canceled", zap.Stringer("nodeID", nodeID))

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "canceled",
	}); err != nil {
		ms.logger.Warn("Failed to write node shutdown cancellation response", zap.Error(err))
	}
}

func (ms *MetadataStore) handleNodeShutdownFinalization(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}

	if err := ms.tm.FinalizeNodeShutdown(r.Context(), nodeID); err != nil {
		ms.logger.Error("Failed to finalize node shutdown", zap.Stringer("nodeID", nodeID), zap.Error(err))
		if errors.Is(err, tablemgr.ErrActiveNodeFinalizeRejected) {
			errorResponse(w, err.Error(), http.StatusConflict)
			return
		}
		errorResponse(w, fmt.Errorf("finalizing node shutdown: %w", err).Error(), http.StatusBadRequest)
		return
	}
	ms.TriggerReconciliation()
	ms.logger.Info("Node shutdown finalized", zap.Stringer("nodeID", nodeID))

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "finalized",
	}); err != nil {
		ms.logger.Warn("Failed to write node shutdown finalization response", zap.Error(err))
	}
}

func (ms *MetadataStore) handleNodeRegistration(w http.ResponseWriter, r *http.Request) {
	var req nodeRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "Failed to parse node registration request", http.StatusBadRequest)
		return
	}
	if req.NodeID == 0 {
		errorResponse(w, "Node registration requires node_id", http.StatusBadRequest)
		return
	}
	if req.StoreID != 0 && req.StoreID != req.NodeID {
		errorResponse(w, "Store identity must match node identity", http.StatusBadRequest)
		return
	}
	if req.Lifecycle != "" && req.Lifecycle != tablemgr.NodeLifecycleActive {
		errorResponse(w, `Invalid node lifecycle on registration: use "active" or the node shutdown API`, http.StatusBadRequest)
		return
	}
	nodeID := types.ID(req.NodeID)
	record := &tablemgr.NodeRecord{
		NodeID:    nodeID,
		Lifecycle: req.Lifecycle,
		Reason:    req.Reason,
	}
	if err := ms.tm.RegisterNode(r.Context(), record); err != nil {
		ms.logger.Error("Failed to register node", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("registering node: %w", err).Error(), http.StatusBadRequest)
		return
	}
	if req.StoreID != 0 {
		shards := req.Shards
		if len(shards) == 0 && len(req.GroupStatus) > 0 {
			shards = nodeGroupStatusReportsToShards(nodeID, req.GroupStatus)
		}
		storeReq := &store.StoreRegistrationRequest{
			StoreInfo: store.StoreInfo{
				ID:      types.ID(req.StoreID),
				RaftURL: req.RaftURL,
				ApiURL:  req.APIURL,
			},
			Shards: shards,
		}
		if err := ms.tm.RegisterStore(r.Context(), storeReq); err != nil {
			ms.logger.Error("Failed to register node store", zap.Stringer("nodeID", nodeID), zap.Error(err))
			errorResponse(w, fmt.Errorf("registering node store: %w", err).Error(), http.StatusInternalServerError)
			return
		}
	}
	ms.TriggerReconciliation()
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("accepted")); err != nil {
		ms.logger.Warn("Failed to write node registration response", zap.Stringer("nodeID", nodeID), zap.Error(err))
	}
}

func (ms *MetadataStore) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}
	var req nodeStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "Failed to parse node status request", http.StatusBadRequest)
		return
	}
	storeID := uint64(nodeID)
	if req.StoreID != 0 {
		if req.StoreID != storeID {
			errorResponse(w, "Store identity must match node identity", http.StatusBadRequest)
			return
		}
		storeID = req.StoreID
	}

	state := store.StoreState_Healthy
	if (req.Live != nil && !*req.Live) || (req.HealthClass != "" && req.HealthClass != "healthy") {
		state = store.StoreState_Unhealthy
	}
	storeInfo := store.StoreInfo{ID: types.ID(storeID)}
	if existing, err := ms.tm.GetStoreStatus(r.Context(), types.ID(storeID)); err == nil {
		storeInfo = existing.StoreInfo
	} else if errors.Is(err, tablemgr.ErrNotFound) {
		errorResponse(w, "Node not found", http.StatusNotFound)
		return
	} else if err != nil && !errors.Is(err, tablemgr.ErrNotFound) {
		ms.logger.Error("Failed to read existing node store status", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("reading existing node store status: %w", err).Error(), http.StatusInternalServerError)
		return
	}
	status := &tablemgr.StoreStatus{
		StoreInfo: storeInfo,
		State:     state,
		LastSeen:  time.Now(),
		Shards:    nodeGroupStatusReportsToShards(types.ID(storeID), req.GroupStatus),
	}
	if err := ms.tm.UpdateStatuses(r.Context(), map[types.ID]*tablemgr.StoreStatus{types.ID(storeID): status}); err != nil {
		ms.logger.Error("Failed to report node status", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("reporting node status: %w", err).Error(), http.StatusInternalServerError)
		return
	}
	ms.TriggerReconciliation()
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("accepted")); err != nil {
		ms.logger.Warn("Failed to write node status response", zap.Stringer("nodeID", nodeID), zap.Error(err))
	}
}

func (ms *MetadataStore) handleNodeRecord(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := parseNodeIDPathValue(w, r)
	if !ok {
		return
	}
	record, err := ms.tm.GetNodeRecord(r.Context(), nodeID)
	if err != nil {
		if errors.Is(err, tablemgr.ErrNotFound) {
			errorResponse(w, "Node not found", http.StatusNotFound)
			return
		}
		ms.logger.Error("Failed to read node record", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, fmt.Errorf("reading node record: %w", err).Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(record); err != nil {
		ms.logger.Warn("Failed to marshal node record", zap.Stringer("nodeID", nodeID), zap.Error(err))
		errorResponse(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func nodeGroupStatusReportsToShards(nodeID types.ID, reports []nodeGroupStatusReport) map[types.ID]*store.ShardInfo {
	shards := make(map[types.ID]*store.ShardInfo, len(reports))
	for _, report := range reports {
		if report.GroupID == 0 {
			continue
		}
		info := store.NewShardInfo()
		info.ReportedBy.Add(nodeID)
		if report.LocalVoter {
			info.Peers.Add(nodeID)
			info.RaftStatus.Voters = common.NewPeerSet(nodeID)
			if report.VoterCount > 0 {
				info.VoterCount = report.VoterCount
			} else {
				info.VoterCount = 1
			}
		}
		if report.LocalLeader {
			info.RaftStatus.Lead = nodeID
		}
		shards[types.ID(report.GroupID)] = info
	}
	return shards
}

func parseNodeIDPathValue(w http.ResponseWriter, r *http.Request) (types.ID, bool) {
	parsed, err := strconv.ParseUint(r.PathValue("node"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return 0, false
	}
	if parsed == 0 {
		http.NotFound(w, r)
		return 0, false
	}
	return types.ID(parsed), true
}

func (ms *MetadataStore) buildNodeShutdownStatus(r *http.Request, nodeID types.ID) (*nodeShutdownStatus, error) {
	ctx := r.Context()
	tombstoned, err := ms.tm.HasStoreTombstone(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	nodeRecord, err := ms.tm.GetNodeRecord(ctx, nodeID)
	if err != nil && !errors.Is(err, tablemgr.ErrNotFound) {
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
		NodeID: uint64(nodeID),
		Type:   "remove",
		Stores: []nodeShutdownStoreStatus{{StoreID: uint64(nodeID)}},
	}
	if !tombstoned && (nodeRecord == nil || nodeRecord.Lifecycle != tablemgr.NodeLifecycleDraining) {
		if !storeKnown && nodeRecord == nil {
			status.Phase = "not_found"
			status.SafeToTerminate = true
		} else if storeStatus != nil && storeStatus.State == store.StoreState_Terminating {
			status.Phase = "recovering"
			status.Message = "node shutdown was canceled and the store is waiting for a healthy status report before it is routable"
		} else {
			status.Phase = tablemgr.NodeLifecycleActive
		}
		return status, nil
	}
	shards, err := ms.tm.GetShardStatuses()
	if err != nil {
		return nil, err
	}
	for shardID, shard := range shards {
		if shard == nil {
			continue
		}
		if shard.ReportedBy.Contains(nodeID) {
			status.Stores[0].GroupStatusCount++
			status.Stores[0].RuntimeGroupCount++
			status.PendingGroups = appendUniqueID(status.PendingGroups, shardID)
		}
		if shard.Peers.Contains(nodeID) {
			status.Stores[0].PlacementIntentCount++
			status.PendingGroups = appendUniqueID(status.PendingGroups, shardID)
		}
		if shard.RaftStatus != nil {
			if shard.RaftStatus.Voters.Contains(nodeID) {
				status.Stores[0].LocalVoterCount++
				status.PendingGroups = appendUniqueID(status.PendingGroups, shardID)
				if effectiveShardVoterCount(shard) <= 1 {
					status.Blocked = true
					status.BlockedReason = "InsufficientShardVoters"
				}
			}
			if shard.RaftStatus.Lead == nodeID {
				status.Stores[0].LocalLeaderCount++
				status.PendingGroups = appendUniqueID(status.PendingGroups, shardID)
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
	case status.Blocked:
		status.Phase = "blocked"
		status.Message = "node shutdown cannot safely remove the selected store because at least one shard would have no remaining voters"
	default:
		status.Phase = "draining"
	}
	return status, nil
}

func effectiveShardVoterCount(shard *store.ShardStatus) int {
	if shard == nil {
		return 0
	}
	voterCount := shard.VoterCount
	if shard.RaftStatus != nil && len(shard.RaftStatus.Voters) > voterCount {
		voterCount = len(shard.RaftStatus.Voters)
	}
	return voterCount
}

func appendUniqueID(ids []types.ID, id types.ID) []types.ID {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}
