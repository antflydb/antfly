package sim

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
)

type CheckerConfig struct {
	SplitLivenessTimeout time.Duration
	MergeLivenessTimeout time.Duration
}

type Checker struct {
	cfg               CheckerConfig
	splitHealthySince map[types.ID]time.Time
	mergeHealthySince map[types.ID]time.Time
}

const stableReplicaConsistencyTimeout = 10 * time.Second

type ClusterSnapshot struct {
	Now                     time.Time
	Tables                  map[string]*store.Table
	Shards                  map[types.ID]*store.ShardStatus
	Stores                  map[types.ID]*tablemgr.StoreStatus
	MetadataNodes           int
	MetadataLeader          types.ID
	MetadataLeaderAvailable bool
}

func NewChecker(cfg CheckerConfig) *Checker {
	if cfg.SplitLivenessTimeout <= 0 {
		cfg.SplitLivenessTimeout = 45 * time.Second
	}
	if cfg.MergeLivenessTimeout <= 0 {
		cfg.MergeLivenessTimeout = cfg.SplitLivenessTimeout
	}
	return &Checker{
		cfg:               cfg,
		splitHealthySince: make(map[types.ID]time.Time),
		mergeHealthySince: make(map[types.ID]time.Time),
	}
}

func (h *Harness) Snapshot(ctx context.Context) (*ClusterSnapshot, error) {
	if err := h.RefreshStatuses(ctx); err != nil {
		return nil, err
	}

	tables, err := h.tableManager.TablesMap()
	if err != nil {
		return nil, fmt.Errorf("loading tables: %w", err)
	}
	shards, err := h.tableManager.GetShardStatuses()
	if err != nil {
		return nil, fmt.Errorf("loading shard statuses: %w", err)
	}
	stores := make(map[types.ID]*tablemgr.StoreStatus, len(h.storeOrder))
	if err := h.tableManager.RangeStoreStatuses(func(storeID types.ID, status *tablemgr.StoreStatus) bool {
		stores[storeID] = status
		return true
	}); err != nil {
		return nil, fmt.Errorf("loading store statuses: %w", err)
	}
	leaderID, hasLeader := h.currentMetadataLeader()

	return &ClusterSnapshot{
		Now:                     h.clock.Now(),
		Tables:                  tables,
		Shards:                  shards,
		Stores:                  stores,
		MetadataNodes:           len(h.metadataOrder),
		MetadataLeader:          leaderID,
		MetadataLeaderAvailable: hasLeader,
	}, nil
}

func (c *Checker) Check(ctx context.Context, h *Harness) error {
	snapshot, err := h.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := c.checkMetadata(snapshot); err != nil {
		return err
	}
	if err := c.checkSplitConsistency(snapshot, false); err != nil {
		return err
	}
	if err := c.checkMergeConsistency(snapshot, false); err != nil {
		return err
	}
	if err := c.checkLeaderAgreement(snapshot); err != nil {
		return err
	}
	if err := c.checkSplitLiveness(snapshot); err != nil {
		return err
	}
	if err := c.checkMergeLiveness(snapshot); err != nil {
		return err
	}
	if err := c.checkTransactionSafety(ctx, h, snapshot); err != nil {
		return err
	}
	if err := c.checkExpectedDocs(snapshot, h, false); err != nil {
		return err
	}
	return nil
}

func (c *Checker) CheckStable(ctx context.Context, h *Harness) error {
	snapshot, err := h.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := c.checkMetadata(snapshot); err != nil {
		return err
	}
	if err := c.checkSplitConsistency(snapshot, true); err != nil {
		return err
	}
	if err := c.checkMergeConsistency(snapshot, true); err != nil {
		return err
	}
	if err := c.requireAllStoresHealthy(snapshot, h.storeOrder); err != nil {
		return err
	}
	if err := c.requireNoActiveSplits(snapshot); err != nil {
		return err
	}
	if err := c.requireNoActiveMerges(snapshot); err != nil {
		return err
	}
	if err := c.checkLeaderAgreement(snapshot); err != nil {
		return err
	}
	if err := c.checkTransactionStability(ctx, h, snapshot); err != nil {
		return err
	}
	if err := c.checkExpectedDocs(snapshot, h, true); err != nil {
		return err
	}
	return nil
}

func (c *Checker) checkMetadata(snapshot *ClusterSnapshot) error {
	for tableName, table := range snapshot.Tables {
		if table == nil {
			return fmt.Errorf("table %q is nil", tableName)
		}
		if err := c.checkTableRanges(table); err != nil {
			return fmt.Errorf("table %q range check failed: %w", tableName, err)
		}
		for shardID, conf := range table.Shards {
			status := snapshot.Shards[shardID]
			if status == nil {
				return fmt.Errorf("table %q shard %s missing metadata status", tableName, shardID)
			}
			if status.Table != tableName {
				return fmt.Errorf("shard %s points at table %q, expected %q", shardID, status.Table, tableName)
			}
			if !status.ByteRange.Equal(conf.ByteRange) {
				if c.allowActiveSplitRangeMismatch(status, conf.ByteRange) {
					continue
				}
				return fmt.Errorf(
					"shard %s range mismatch: metadata status=%s table=%s",
					shardID,
					status.ByteRange,
					conf.ByteRange,
				)
			}
		}
	}

	for shardID, status := range snapshot.Shards {
		if status == nil {
			return fmt.Errorf("shard %s status is nil", shardID)
		}
		table := snapshot.Tables[status.Table]
		if table == nil {
			return fmt.Errorf("shard %s references missing table %q", shardID, status.Table)
		}
		if table.Shards[shardID] == nil {
			return fmt.Errorf("shard %s exists in metadata status but not in table %q", shardID, status.Table)
		}
		for peerID := range status.Peers {
			if snapshot.Stores[peerID] == nil {
				return fmt.Errorf("shard %s references unknown peer %s", shardID, peerID)
			}
		}
		for reporterID := range status.ReportedBy {
			if snapshot.Stores[reporterID] == nil {
				return fmt.Errorf("shard %s was reported by unknown store %s", shardID, reporterID)
			}
		}
	}
	return nil
}

func (c *Checker) allowActiveSplitRangeMismatch(
	status *store.ShardStatus,
	tableRange [2][]byte,
) bool {
	if status == nil || status.SplitState == nil {
		return false
	}
	if status.SplitState.GetPhase() == storedb.SplitState_PHASE_NONE {
		return false
	}
	splitKey := status.SplitState.GetSplitKey()
	if len(splitKey) == 0 {
		return false
	}
	return bytes.Equal(status.ByteRange[0], tableRange[0]) &&
		bytes.Equal(splitKey, tableRange[1])
}

func (c *Checker) checkTableRanges(table *store.Table) error {
	type shardRange struct {
		id    types.ID
		start []byte
		end   []byte
	}
	ranges := make([]shardRange, 0, len(table.Shards))
	for shardID, conf := range table.Shards {
		if conf == nil {
			return fmt.Errorf("shard %s config is nil", shardID)
		}
		ranges = append(ranges, shardRange{
			id:    shardID,
			start: conf.ByteRange[0],
			end:   conf.ByteRange[1],
		})
	}
	slices.SortFunc(ranges, func(a, b shardRange) int {
		if len(a.start) == 0 && len(b.start) != 0 {
			return -1
		}
		if len(a.start) != 0 && len(b.start) == 0 {
			return 1
		}
		return bytes.Compare(a.start, b.start)
	})

	for i := range ranges {
		if i == 0 {
			continue
		}
		prev := ranges[i-1]
		curr := ranges[i]
		if len(prev.end) == 0 {
			return fmt.Errorf("shard %s has unbounded end before shard %s", prev.id, curr.id)
		}
		if len(curr.start) == 0 {
			return fmt.Errorf("shard %s has unbounded start after shard %s", curr.id, prev.id)
		}
		switch cmp := bytes.Compare(prev.end, curr.start); {
		case cmp < 0:
			return fmt.Errorf("gap between shard %s and shard %s", prev.id, curr.id)
		case cmp > 0:
			return fmt.Errorf("overlap between shard %s and shard %s", prev.id, curr.id)
		}
	}
	return nil
}

func (c *Checker) checkLeaderAgreement(snapshot *ClusterSnapshot) error {
	for shardID, status := range snapshot.Shards {
		if status == nil {
			continue
		}
		if !c.allShardPeersHealthy(snapshot, status.Peers) {
			continue
		}
		leaders := make(map[types.ID]struct{})
		for _, storeStatus := range snapshot.Stores {
			if storeStatus == nil || !storeStatus.IsReachable() {
				continue
			}
			storeShard := storeStatus.Shards[shardID]
			if storeShard == nil || storeShard.RaftStatus == nil || storeShard.RaftStatus.Lead == 0 {
				continue
			}
			leaders[storeShard.RaftStatus.Lead] = struct{}{}
		}
		if len(leaders) > 1 {
			leaderIDs := make([]types.ID, 0, len(leaders))
			for leaderID := range leaders {
				leaderIDs = append(leaderIDs, leaderID)
			}
			slices.Sort(leaderIDs)
			return fmt.Errorf("shard %s has multiple healthy leaders reported: %v", shardID, leaderIDs)
		}
	}
	return nil
}

func (c *Checker) checkSplitLiveness(snapshot *ClusterSnapshot) error {
	activeNow := make(map[types.ID]struct{})
	for shardID, status := range snapshot.Shards {
		if status == nil || !c.isSplitActive(status) {
			delete(c.splitHealthySince, shardID)
			continue
		}
		activeNow[shardID] = struct{}{}
		if snapshot.MetadataNodes > 1 && !snapshot.MetadataLeaderAvailable {
			delete(c.splitHealthySince, shardID)
			continue
		}
		if !c.allShardPeersHealthy(snapshot, status.Peers) {
			delete(c.splitHealthySince, shardID)
			continue
		}
		since, ok := c.splitHealthySince[shardID]
		if !ok {
			c.splitHealthySince[shardID] = snapshot.Now
			continue
		}
		if snapshot.Now.Sub(since) > c.cfg.SplitLivenessTimeout {
			return fmt.Errorf(
				"shard %s split state remained active for %s with healthy peers",
				shardID,
				snapshot.Now.Sub(since),
			)
		}
	}
	for shardID := range c.splitHealthySince {
		if _, ok := activeNow[shardID]; !ok {
			delete(c.splitHealthySince, shardID)
		}
	}
	return nil
}

func (c *Checker) checkSplitConsistency(snapshot *ClusterSnapshot, requireChildPresence bool) error {
	childParents := make(map[types.ID]types.ID)
	for shardID, status := range snapshot.Shards {
		if status == nil || status.SplitState == nil {
			continue
		}

		splitState := status.SplitState
		childID := types.ID(splitState.GetNewShardId())
		if childID == 0 {
			if requireChildPresence {
				return fmt.Errorf("shard %s has active split state without child shard id", shardID)
			}
			continue
		}
		childStatus := snapshot.Shards[childID]
		if childStatus == nil {
			if requireChildPresence {
				return fmt.Errorf("shard %s references missing split child %s", shardID, childID)
			}
			continue
		}
		if childStatus.Table != status.Table {
			return fmt.Errorf(
				"shard %s split child %s belongs to table %q, expected %q",
				shardID,
				childID,
				childStatus.Table,
				status.Table,
			)
		}
		if prevParent, ok := childParents[childID]; ok && prevParent != shardID {
			return fmt.Errorf("split child %s is referenced by multiple parents %s and %s", childID, prevParent, shardID)
		}
		childParents[childID] = shardID

		splitKey := splitState.GetSplitKey()
		if len(splitKey) == 0 {
			return fmt.Errorf("shard %s split child %s has empty split key", shardID, childID)
		}
		if len(status.ByteRange[0]) > 0 && bytes.Compare(splitKey, status.ByteRange[0]) <= 0 {
			return fmt.Errorf(
				"shard %s split key %q is not above shard start %q",
				shardID,
				splitKey,
				status.ByteRange[0],
			)
		}
		if originalEnd := splitState.GetOriginalRangeEnd(); len(originalEnd) > 0 && bytes.Compare(splitKey, originalEnd) >= 0 {
			return fmt.Errorf(
				"shard %s split key %q is not below original range end %q",
				shardID,
				splitKey,
				originalEnd,
			)
		}
	}
	return nil
}

func (c *Checker) checkMergeLiveness(snapshot *ClusterSnapshot) error {
	activeNow := make(map[types.ID]struct{})
	for shardID, status := range snapshot.Shards {
		if status == nil || !mergeActive(status) {
			delete(c.mergeHealthySince, shardID)
			continue
		}
		if status.MergeState == nil || types.ID(status.MergeState.GetReceiverShardId()) != shardID {
			delete(c.mergeHealthySince, shardID)
			continue
		}
		activeNow[shardID] = struct{}{}
		if snapshot.MetadataNodes > 1 && !snapshot.MetadataLeaderAvailable {
			delete(c.mergeHealthySince, shardID)
			continue
		}
		donorID := types.ID(status.MergeState.GetDonorShardId())
		if donorID == 0 || !c.shardHasHealthyQuorum(snapshot, donorID) || !c.shardHasHealthyQuorum(snapshot, shardID) {
			delete(c.mergeHealthySince, shardID)
			continue
		}
		since, ok := c.mergeHealthySince[shardID]
		if !ok {
			c.mergeHealthySince[shardID] = snapshot.Now
			continue
		}
		if snapshot.Now.Sub(since) > c.cfg.MergeLivenessTimeout {
			return fmt.Errorf(
				"shard %s merge state remained active for %s with healthy donor/receiver quorums",
				shardID,
				snapshot.Now.Sub(since),
			)
		}
	}
	for shardID := range c.mergeHealthySince {
		if _, ok := activeNow[shardID]; !ok {
			delete(c.mergeHealthySince, shardID)
		}
	}
	return nil
}

func (c *Checker) checkMergeConsistency(snapshot *ClusterSnapshot, requirePeerPresence bool) error {
	for shardID, status := range snapshot.Shards {
		if status == nil || status.MergeState == nil {
			continue
		}
		mergeState := status.MergeState
		if mergeState.GetPhase() == storedb.MergeState_PHASE_NONE {
			continue
		}
		donorID := types.ID(mergeState.GetDonorShardId())
		receiverID := types.ID(mergeState.GetReceiverShardId())
		if donorID == 0 || receiverID == 0 {
			return fmt.Errorf("shard %s has active merge state without donor/receiver ids", shardID)
		}
		if shardID != donorID && shardID != receiverID {
			return fmt.Errorf("shard %s merge state references donor=%s receiver=%s", shardID, donorID, receiverID)
		}
		if shardID == receiverID && !mergeState.GetAcceptDonorRange() {
			return fmt.Errorf("receiver shard %s merge state is not marked accept_donor_range", shardID)
		}
		if shardID == donorID && mergeState.GetAcceptDonorRange() {
			return fmt.Errorf("donor shard %s merge state incorrectly accepts donor range", shardID)
		}
		donorStatus := snapshot.Shards[donorID]
		receiverStatus := snapshot.Shards[receiverID]
		if donorStatus == nil || receiverStatus == nil {
			if requirePeerPresence {
				return fmt.Errorf("merge pair donor=%s receiver=%s missing from metadata", donorID, receiverID)
			}
			continue
		}
		if donorStatus.Table != receiverStatus.Table {
			return fmt.Errorf(
				"merge pair donor=%s receiver=%s span different tables %q/%q",
				donorID,
				receiverID,
				donorStatus.Table,
				receiverStatus.Table,
			)
		}
		if receiverStatus.MergeState == nil || donorStatus.MergeState == nil {
			if requirePeerPresence {
				return fmt.Errorf("merge pair donor=%s receiver=%s missing mirrored merge state", donorID, receiverID)
			}
			continue
		}
		if types.ID(receiverStatus.MergeState.GetDonorShardId()) != donorID ||
			types.ID(receiverStatus.MergeState.GetReceiverShardId()) != receiverID {
			return fmt.Errorf("receiver shard %s merge state does not mirror donor=%s receiver=%s", receiverID, donorID, receiverID)
		}
		if types.ID(donorStatus.MergeState.GetDonorShardId()) != donorID ||
			types.ID(donorStatus.MergeState.GetReceiverShardId()) != receiverID {
			return fmt.Errorf("donor shard %s merge state does not mirror donor=%s receiver=%s", donorID, donorID, receiverID)
		}
	}
	return nil
}

func (c *Checker) checkExpectedDocs(snapshot *ClusterSnapshot, h *Harness, requireReplicaConsistency bool) error {
	tableNames := make([]string, 0, len(snapshot.Tables))
	for tableName := range snapshot.Tables {
		tableNames = append(tableNames, tableName)
	}
	slices.Sort(tableNames)

	for key, expected := range h.expectedDocs {
		tableName, ok := h.expectedKeys[key]
		if !ok && len(tableNames) == 1 {
			tableName = tableNames[0]
			ok = true
		}
		if !ok {
			continue
		}
		table := snapshot.Tables[tableName]
		if table == nil {
			return fmt.Errorf("expected key %q references missing table %q", key, tableName)
		}
		shardID, err := table.FindShardForKey(key)
		if err != nil {
			return fmt.Errorf("routing expected key %q: %w", key, err)
		}
		if !c.shardHasHealthyQuorum(snapshot, shardID) {
			continue
		}
		actual, found, err := c.lookupFromHealthyLeader(snapshot, h, shardID, key)
		if err != nil {
			return fmt.Errorf("lookup for expected key %q: %w", key, err)
		}
		if !found {
			if requireReplicaConsistency {
				return fmt.Errorf("no healthy leader found for shard %s", shardID)
			}
			continue
		}
		if !bytes.Equal(actual, expected) {
			return fmt.Errorf("lookup mismatch for key %q", key)
		}

		if !requireReplicaConsistency || !c.isTableStable(snapshot, table) {
			continue
		}
		if err := h.WaitFor(stableReplicaConsistencyTimeout, func() error {
			return c.checkExpectedDocReplicas(snapshot, h, table, shardID, key, expected)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Checker) checkExpectedDocReplicas(
	snapshot *ClusterSnapshot,
	h *Harness,
	table *store.Table,
	shardID types.ID,
	key string,
	expected []byte,
) error {
	for storeID, storeStatus := range snapshot.Stores {
		if storeStatus == nil || !storeStatus.IsReachable() {
			continue
		}
		if storeStatus.Shards[shardID] == nil {
			continue
		}
		results, err := h.LookupFromStore(storeID, shardID, []string{key})
		if err != nil {
			return fmt.Errorf("replica lookup for key %q on store %s shard %s: %w", key, storeID, shardID, err)
		}
		actualValue, ok := results[key]
		if !ok {
			return fmt.Errorf("replica store %s shard %s missing key %q", storeID, shardID, key)
		}
		if !bytes.Equal(actualValue, expected) {
			return fmt.Errorf("replica store %s shard %s mismatch for key %q", storeID, shardID, key)
		}
		for otherShardID := range table.Shards {
			if otherShardID == shardID || storeStatus.Shards[otherShardID] == nil {
				continue
			}
			results, err := h.LookupFromStore(storeID, otherShardID, []string{key})
			if err != nil {
				return fmt.Errorf("cross-shard lookup for key %q on store %s shard %s: %w", key, storeID, otherShardID, err)
			}
			if _, ok := results[key]; ok {
				return fmt.Errorf(
					"key %q appeared on non-owner shard %s for store %s",
					key,
					otherShardID,
					storeID,
				)
			}
		}
	}
	return nil
}

func (c *Checker) requireAllStoresHealthy(snapshot *ClusterSnapshot, storeOrder []types.ID) error {
	for _, storeID := range storeOrder {
		status := snapshot.Stores[storeID]
		if status == nil {
			return fmt.Errorf("store %s missing from metadata statuses", storeID)
		}
		if !status.IsReachable() {
			return fmt.Errorf("store %s is not healthy", storeID)
		}
	}
	return nil
}

func (c *Checker) requireNoActiveSplits(snapshot *ClusterSnapshot) error {
	for shardID, status := range snapshot.Shards {
		if status != nil && c.isSplitActive(status) {
			return fmt.Errorf("shard %s still has active split state", shardID)
		}
	}
	return nil
}

func (c *Checker) requireNoActiveMerges(snapshot *ClusterSnapshot) error {
	for shardID, status := range snapshot.Shards {
		if status != nil && mergeActive(status) {
			return fmt.Errorf("shard %s still has active merge state", shardID)
		}
	}
	return nil
}

func (c *Checker) checkTransactionSafety(ctx context.Context, h *Harness, snapshot *ClusterSnapshot) error {
	txnSnapshot, err := h.TransactionSnapshot(ctx, snapshot)
	if err != nil {
		return err
	}
	return c.checkTransactionSafetySnapshot(txnSnapshot)
}

func (c *Checker) checkTransactionSafetySnapshot(txnSnapshot *ClusterTxnSnapshot) error {
	for txnKey, intents := range txnSnapshot.Intents {
		if _, ok := txnSnapshot.Records[txnKey]; ok {
			continue
		}
		if len(intents) == 0 {
			continue
		}
		intent := intents[0]
		return fmt.Errorf(
			"transaction %s has orphaned intents on shard %s store %s without coordinator record",
			txnKey,
			intent.ShardID,
			intent.StoreID,
		)
	}
	return nil
}

func (c *Checker) checkTransactionStability(ctx context.Context, h *Harness, snapshot *ClusterSnapshot) error {
	txnSnapshot, err := h.TransactionSnapshot(ctx, snapshot)
	if err != nil {
		return err
	}
	return c.checkTransactionStabilitySnapshot(txnSnapshot)
}

func (c *Checker) checkTransactionStabilitySnapshot(txnSnapshot *ClusterTxnSnapshot) error {
	for txnKey, recordRef := range txnSnapshot.Records {
		record := recordRef.Record
		if record.Status == storedb.TxnStatusPending {
			return fmt.Errorf(
				"pending transaction %s remains on shard %s store %s",
				txnKey,
				recordRef.ShardID,
				recordRef.StoreID,
			)
		}
	}

	for txnKey, intents := range txnSnapshot.Intents {
		if len(intents) == 0 {
			continue
		}
		recordRef, ok := txnSnapshot.Records[txnKey]
		if !ok {
			intent := intents[0]
			return fmt.Errorf(
				"transaction %s has orphaned intents on shard %s store %s without coordinator record",
				txnKey,
				intent.ShardID,
				intent.StoreID,
			)
		}

		switch recordRef.Record.Status {
		case storedb.TxnStatusPending:
			return fmt.Errorf(
				"transaction %s still has pending coordinator record with %d unresolved intents",
				txnKey,
				len(intents),
			)
		case storedb.TxnStatusCommitted, storedb.TxnStatusAborted:
			intent := intents[0]
			return fmt.Errorf(
				"transaction %s finalized with status=%d but still has %d intents (example shard=%s store=%s key=%q)",
				txnKey,
				recordRef.Record.Status,
				len(intents),
				intent.ShardID,
				intent.StoreID,
				intent.Intent.UserKey,
			)
		}
	}

	return nil
}

func (c *Checker) isTableStable(snapshot *ClusterSnapshot, table *store.Table) bool {
	if table == nil {
		return false
	}
	for shardID := range table.Shards {
		status := snapshot.Shards[shardID]
		if status == nil || c.isSplitActive(status) || mergeActive(status) ||
			!c.allShardPeersHealthy(snapshot, status.Peers) {
			return false
		}
	}
	return true
}

func (c *Checker) isSplitActive(status *store.ShardStatus) bool {
	if status == nil {
		return false
	}
	if status.SplitState != nil {
		return true
	}
	if status.State.Transitioning() || status.State == store.ShardState_SplitOffPreSnap {
		return true
	}
	return status.SplitReplayRequired && !status.SplitCutoverReady
}

func mergeActive(status *store.ShardStatus) bool {
	if status == nil {
		return false
	}
	if status.MergeState != nil && status.MergeState.GetPhase() != storedb.MergeState_PHASE_NONE {
		return true
	}
	return status.State == store.ShardState_PreMerge
}

func (c *Checker) allShardPeersHealthy(snapshot *ClusterSnapshot, peers map[types.ID]struct{}) bool {
	if len(peers) == 0 {
		return false
	}
	for peerID := range peers {
		status := snapshot.Stores[peerID]
		if status == nil || !status.IsReachable() {
			return false
		}
	}
	return true
}

func (c *Checker) shardHasHealthyQuorum(snapshot *ClusterSnapshot, shardID types.ID) bool {
	status := snapshot.Shards[shardID]
	if status == nil {
		return false
	}
	healthy := 0
	for peerID := range status.Peers {
		storeStatus := snapshot.Stores[peerID]
		if storeStatus != nil && storeStatus.IsReachable() {
			healthy++
		}
	}
	return healthy >= len(status.Peers)/2+1
}

func (c *Checker) lookupFromHealthyLeader(
	snapshot *ClusterSnapshot,
	h *Harness,
	shardID types.ID,
	key string,
) ([]byte, bool, error) {
	for storeID, storeStatus := range snapshot.Stores {
		if storeStatus == nil || !storeStatus.IsReachable() {
			continue
		}
		shardStatus := storeStatus.Shards[shardID]
		if shardStatus == nil || shardStatus.RaftStatus == nil {
			continue
		}
		if shardStatus.RaftStatus.Lead != storeID {
			continue
		}
		results, err := h.LookupFromStore(storeID, shardID, []string{key})
		if err != nil {
			return nil, false, err
		}
		return results[key], true, nil
	}
	return nil, false, nil
}
