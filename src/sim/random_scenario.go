package sim

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
)

type RandomScenarioConfig struct {
	Seed                        int64
	BaseDir                     string
	Start                       time.Time
	MetadataID                  types.ID
	MetadataIDs                 []types.ID
	StoreIDs                    []types.ID
	ReplicationFactor           uint64
	TableName                   string
	TableStartID                types.ID
	Steps                       int
	MaxShardSizeBytes           uint64
	MinShardSizeBytes           uint64
	MinShardsPerTable           uint64
	MaxShardsPerTable           uint64
	SplitTimeout                time.Duration
	SplitFinalizeGracePeriod    time.Duration
	CheckerSplitLivenessTimeout time.Duration
	StabilizeTimeout            time.Duration
	ActionSettle                time.Duration
	StabilizeEvery              int
	PreferSplits                bool
}

type RandomScenarioResult struct {
	Seed           int64
	Actions        []ScenarioAction
	ReducedActions []ScenarioAction
	Trace          []string
	Events         []TraceEvent
	Digests        []ClusterDigest
	Writes         int
	FinalShards    int
}

type randomScenarioRunner struct {
	cfg             RandomScenarioConfig
	h               *Harness
	checker         *Checker
	rng             *rand.Rand
	actionRNG       *rand.Rand
	trace           []string
	crashed         map[types.ID]bool
	partitioned     map[types.ID]bool
	crashedMeta     map[types.ID]bool
	partitionedMeta map[types.ID]bool
	cutLinks        map[transportLink]transportFaultTarget
	latencyLinks    map[transportLink]transportFaultTarget
	writes          int
	planned         []ScenarioAction
	reduceFailures  bool
}

func RunRandomScenario(ctx context.Context, cfg RandomScenarioConfig) (*RandomScenarioResult, error) {
	return RunRandomScenarioWithActions(ctx, cfg, ScenarioRecord{
		Kind: ScenarioKindDocuments,
		Seed: cfg.Seed,
	})
}

func RunRandomScenarioWithActions(
	ctx context.Context,
	cfg RandomScenarioConfig,
	record ScenarioRecord,
) (*RandomScenarioResult, error) {
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	record.Kind = ScenarioKindDocuments
	record.Seed = cfg.Seed
	if len(record.Actions) > 0 {
		if err := record.Validate(); err != nil {
			return nil, err
		}
		cfg.Steps = len(record.Actions)
	}
	if cfg.BaseDir == "" {
		cfg.BaseDir = filepath.Join(".", "tmp", fmt.Sprintf("sim-random-%d", cfg.Seed))
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Unix(1_700_300_000, 0).UTC()
	}
	if cfg.MetadataID == 0 {
		cfg.MetadataID = 100
	}
	if len(cfg.MetadataIDs) == 0 {
		cfg.MetadataIDs = []types.ID{cfg.MetadataID}
	}
	if len(cfg.StoreIDs) == 0 {
		cfg.StoreIDs = []types.ID{1, 2, 3}
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = uint64(len(cfg.StoreIDs))
	}
	if cfg.TableName == "" {
		cfg.TableName = "docs"
	}
	if cfg.TableStartID == 0 {
		cfg.TableStartID = 0x4000
	}
	if cfg.Steps <= 0 {
		cfg.Steps = 24
	}
	if cfg.MaxShardSizeBytes == 0 {
		cfg.MaxShardSizeBytes = 64 * 1024
	}
	if cfg.MinShardsPerTable == 0 {
		cfg.MinShardsPerTable = 1
	}
	if cfg.MaxShardsPerTable == 0 {
		cfg.MaxShardsPerTable = 1
	}
	if cfg.SplitTimeout <= 0 {
		cfg.SplitTimeout = 30 * time.Second
	}
	if cfg.SplitFinalizeGracePeriod <= 0 {
		cfg.SplitFinalizeGracePeriod = 1500 * time.Millisecond
	}
	if cfg.ActionSettle <= 0 {
		cfg.ActionSettle = 1200 * time.Millisecond
	}
	if cfg.StabilizeEvery <= 0 {
		cfg.StabilizeEvery = 6
	}
	if cfg.StabilizeTimeout <= 0 {
		cfg.StabilizeTimeout = 90 * time.Second
	}

	h, err := NewHarness(HarnessConfig{
		BaseDir:                  cfg.BaseDir,
		Start:                    cfg.Start,
		MetadataID:               cfg.MetadataID,
		MetadataIDs:              cfg.MetadataIDs,
		StoreIDs:                 cfg.StoreIDs,
		ReplicationFactor:        cfg.ReplicationFactor,
		MaxShardSizeBytes:        cfg.MaxShardSizeBytes,
		MinShardSizeBytes:        cfg.MinShardSizeBytes,
		MinShardsPerTable:        cfg.MinShardsPerTable,
		MaxShardsPerTable:        cfg.MaxShardsPerTable,
		SplitTimeout:             cfg.SplitTimeout,
		SplitFinalizeGracePeriod: cfg.SplitFinalizeGracePeriod,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = h.Close() }()

	if _, err := h.CreateTable(cfg.TableName, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   cfg.TableStartID,
	}); err != nil {
		return nil, fmt.Errorf("creating table %q: %w", cfg.TableName, err)
	}
	if _, err := startRandomScenarioTable(ctx, h, cfg.TableName); err != nil {
		return nil, err
	}

	runner := &randomScenarioRunner{
		cfg:             cfg,
		h:               h,
		checker:         NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second}),
		rng:             rand.New(rand.NewSource(cfg.Seed)),       //nolint:gosec // deterministic seed for reproducible simulations
		actionRNG:       rand.New(rand.NewSource(cfg.Seed ^ 0x5deece66d)), //nolint:gosec // deterministic seed for reproducible simulations
		crashed:         make(map[types.ID]bool),
		partitioned:     make(map[types.ID]bool),
		crashedMeta:     make(map[types.ID]bool),
		partitionedMeta: make(map[types.ID]bool),
		cutLinks:        make(map[transportLink]transportFaultTarget),
		latencyLinks:    make(map[transportLink]transportFaultTarget),
		planned:         append([]ScenarioAction(nil), record.Actions...),
		reduceFailures:  len(record.Actions) == 0,
	}
	if cfg.CheckerSplitLivenessTimeout > 0 {
		runner.checker = NewChecker(CheckerConfig{SplitLivenessTimeout: cfg.CheckerSplitLivenessTimeout})
	}

	if err := runner.checker.CheckStable(ctx, runner.h); err != nil {
		return nil, fmt.Errorf("initial stable check failed: %w", err)
	}
	_ = runner.h.RecordDigest("initial")

	for step := 0; step < cfg.Steps; step++ {
		if err := runner.step(ctx, step); err != nil {
			return nil, err
		}
		if cfg.StabilizeEvery > 0 && (step+1)%cfg.StabilizeEvery == 0 {
			if err := runner.stabilize(ctx, 30*time.Second, false); err != nil {
				return nil, err
			}
		}
	}

	if err := runner.stabilize(ctx, cfg.StabilizeTimeout, true); err != nil {
		return nil, err
	}

	table, err := runner.h.GetTable(cfg.TableName)
	if err != nil {
		return nil, runner.wrapErr("load final table", err)
	}
	return &RandomScenarioResult{
		Seed:        cfg.Seed,
		Actions:     append([]ScenarioAction(nil), runner.planned...),
		Trace:       append([]string(nil), runner.trace...),
		Events:      runner.h.Trace().Events(),
		Digests:     runner.h.Trace().Digests(),
		Writes:      runner.writes,
		FinalShards: len(table.Shards),
	}, nil
}

func startRandomScenarioTable(ctx context.Context, h *Harness, tableName string) (*store.Table, error) {
	table, err := h.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("load table %q: %w", tableName, err)
	}
	shardIDs := make([]types.ID, 0, len(table.Shards))
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)
	for _, shardID := range shardIDs {
		if err := h.StartShardOnAllStores(shardID); err != nil {
			return nil, err
		}
	}
	for _, shardID := range shardIDs {
		if _, err := h.WaitForLeader(shardID, 30*time.Second); err != nil {
			return nil, err
		}
	}
	return h.GetTable(tableName)
}

func (r *randomScenarioRunner) step(ctx context.Context, step int) error {
	action := r.nextAction(ctx, step)
	r.trace = append(r.trace, fmt.Sprintf("%02d:%s", step, action))
	r.h.recordEvent("action", "step=%d action=%s", step, action)

	var err error
	switch action {
	case ActionWrite:
		err = r.writeDoc(ctx)
	case ActionReallocate:
		err = r.requestReconcile(ctx)
	case ActionCrash:
		err = r.crashStore()
	case ActionRestart:
		err = r.restartStore()
	case ActionPartition:
		err = r.partitionStore()
	case ActionHeal:
		err = r.healStore()
	case ActionMetadataCrash:
		err = r.crashMetadataNode()
	case ActionMetadataRestart:
		err = r.restartMetadataNode()
	case ActionMetadataPartition:
		err = r.partitionMetadataNode(ctx)
	case ActionMetadataHeal:
		err = r.healMetadataNode()
	case ActionDropNextMsg:
		err = r.dropNextMessage(ctx)
	case ActionDuplicateNextMsg:
		err = r.duplicateNextMessage(ctx)
	case ActionCutLink:
		err = r.cutLink(ctx)
	case ActionHealLink:
		err = r.healLink()
	case ActionSetLinkLatency:
		err = r.setLinkLatency(ctx)
	case ActionResetLinkLatency:
		err = r.resetLinkLatency()
	case ActionTick:
		err = nil
	default:
		err = fmt.Errorf("unknown action %q", action)
	}
	if err != nil {
		return r.wrapErr(string(action), err)
	}

	if err := r.h.Advance(r.cfg.ActionSettle); err != nil {
		return r.wrapErr(string(action)+" advance", err)
	}
	if isMetadataAction(action) && len(r.cfg.MetadataIDs) > 1 {
		if _, err := r.h.WaitForMetadataLeader(10 * time.Second); err != nil {
			if r.shouldDeferStepError(ctx, action, err) {
				r.h.recordEvent("transient_error", "action=%s stage=metadata_leader err=%v", action, err)
			} else {
				return r.wrapErr(string(action)+" metadata leader", err)
			}
		}
	}
	if err := r.h.ReconcileOnce(ctx); err != nil {
		if r.shouldDeferStepError(ctx, action, err) {
			r.h.recordEvent("transient_error", "action=%s stage=reconcile err=%v", action, err)
		} else {
			return r.wrapErr(string(action)+" reconcile", err)
		}
	}
	if err := r.checker.Check(ctx, r.h); err != nil {
		if r.shouldDeferStepError(ctx, action, err) {
			r.h.recordEvent("transient_error", "action=%s stage=check err=%v", action, err)
		} else {
			return r.wrapErr(string(action)+" check", err)
		}
	}
	_ = r.h.RecordDigest(fmt.Sprintf("step-%02d-%s", step, action))
	return nil
}

func (r *randomScenarioRunner) nextAction(ctx context.Context, step int) ScenarioAction {
	if step < len(r.planned) {
		return r.planned[step]
	}
	action := r.pickAction(ctx)
	r.planned = append(r.planned, action)
	return action
}

func (r *randomScenarioRunner) pickAction(ctx context.Context) ScenarioAction {
	if r.hasActiveSplit(ctx) {
		actions := []ScenarioAction{ActionTick, ActionTick, ActionDropNextMsg, ActionDuplicateNextMsg}
		if r.cfg.PreferSplits {
			actions = append(actions, ActionCutLink, ActionTick, ActionDropNextMsg)
		}
		actions = append(actions, r.metadataActions(true)...)
		if len(r.latencyLinks) == 0 {
			actions = append(actions, ActionSetLinkLatency)
		} else {
			actions = append(actions, ActionResetLinkLatency)
		}
		if len(r.partitioned) > 0 {
			actions = append(actions, ActionHeal)
		}
		if len(r.crashed) > 0 {
			actions = append(actions, ActionRestart)
		}
		if len(r.cutLinks) > 0 {
			actions = append(actions, ActionHealLink)
		}
		return actions[r.actionRNG.Intn(len(actions))]
	}

	if r.cfg.PreferSplits && r.tableCanStillSplit() {
		actions := []ScenarioAction{
			ActionWrite,
			ActionWrite,
			ActionWrite,
			ActionReallocate,
			ActionTick,
		}
		actions = append(actions, r.metadataActions(true)...)
		if len(r.cutLinks) == 0 && len(r.partitioned) == 0 && len(r.crashed) == 0 {
			actions = append(actions, ActionCutLink)
		} else if len(r.cutLinks) > 0 {
			actions = append(actions, ActionHealLink)
		}
		if len(r.latencyLinks) == 0 {
			actions = append(actions, ActionSetLinkLatency)
		} else {
			actions = append(actions, ActionResetLinkLatency)
		}
		if len(r.partitioned) > 0 {
			actions = append(actions, ActionHeal)
		}
		if len(r.crashed) > 0 {
			actions = append(actions, ActionRestart)
		}
		return actions[r.actionRNG.Intn(len(actions))]
	}

	actions := []ScenarioAction{
		ActionWrite,
		ActionWrite,
		ActionTick,
		ActionReallocate,
		ActionDropNextMsg,
		ActionDuplicateNextMsg,
	}
	actions = append(actions, r.metadataActions(false)...)
	if len(r.latencyLinks) == 0 {
		actions = append(actions, ActionSetLinkLatency)
	} else {
		actions = append(actions, ActionResetLinkLatency)
	}
	if len(r.crashed) == 0 {
		actions = append(actions, ActionCrash)
	} else {
		actions = append(actions, ActionRestart)
	}
	if len(r.partitioned) == 0 && len(r.crashed) == 0 {
		actions = append(actions, ActionPartition)
	} else if len(r.partitioned) > 0 {
		actions = append(actions, ActionHeal)
	}
	if len(r.cutLinks) == 0 && len(r.partitioned) == 0 && len(r.crashed) == 0 {
		actions = append(actions, ActionCutLink)
	} else if len(r.cutLinks) > 0 {
		actions = append(actions, ActionHealLink)
	}
	return actions[r.actionRNG.Intn(len(actions))]
}

func (r *randomScenarioRunner) metadataActions(duringSplit bool) []ScenarioAction {
	if len(r.cfg.MetadataIDs) <= 1 {
		return nil
	}
	actions := make([]ScenarioAction, 0, 4)
	if len(r.crashedMeta) == 0 && len(r.partitionedMeta) == 0 {
		if !r.canDisruptMetadata() {
			return nil
		}
		actions = append(actions, ActionMetadataCrash, ActionMetadataPartition)
		if duringSplit {
			actions = append(actions, ActionMetadataCrash)
		}
	} else {
		if len(r.crashedMeta) > 0 {
			actions = append(actions, ActionMetadataRestart)
		}
		if len(r.partitionedMeta) > 0 {
			actions = append(actions, ActionMetadataHeal)
		}
	}
	return actions
}

func (r *randomScenarioRunner) hasActiveSplit(ctx context.Context) bool {
	snapshot, err := r.h.Snapshot(ctx)
	if err != nil {
		return false
	}
	for _, status := range snapshot.Shards {
		if r.checker.isSplitActive(status) {
			return true
		}
	}
	return false
}

func (r *randomScenarioRunner) tableCanStillSplit() bool {
	if r.cfg.MaxShardsPerTable <= 1 {
		return false
	}
	table, err := r.h.GetTable(r.cfg.TableName)
	if err != nil || table == nil {
		return false
	}
	return uint64(len(table.Shards)) < r.cfg.MaxShardsPerTable
}

func (r *randomScenarioRunner) writeDoc(ctx context.Context) error {
	key := fmt.Sprintf(
		"%x/docs/%02d/%08x",
		r.rng.Intn(16),
		r.writes,
		r.rng.Uint32(),
	)
	payload := fmt.Sprintf(
		`{"value":"seed-%d-write-%d-%s"}`,
		r.cfg.Seed,
		r.writes,
		strings.Repeat(string(rune('a'+r.rng.Intn(26))), 96), //nolint:gosec // bounded to [0,26), no overflow
	)
	var lastErr error
	for range 4 {
		if err := r.h.WriteKey(r.cfg.TableName, key, []byte(payload)); err == nil {
			r.writes++
			return nil
		} else {
			lastErr = err
		}
		if err := r.h.Advance(500 * time.Millisecond); err != nil {
			return err
		}
		if err := r.h.ReconcileOnce(ctx); err != nil {
			return err
		}
	}
	return lastErr
}

func (r *randomScenarioRunner) requestReconcile(ctx context.Context) error {
	if err := r.h.TableManager().EnqueueReallocationRequest(ctx); err != nil {
		return err
	}
	return r.h.ReconcileOnce(ctx)
}

func (r *randomScenarioRunner) crashStore() error {
	storeID, err := r.pickRunningStore()
	if err != nil {
		return err
	}
	if err := r.h.CrashStore(storeID); err != nil {
		return err
	}
	r.crashed[storeID] = true
	delete(r.partitioned, storeID)
	return nil
}

func (r *randomScenarioRunner) restartStore() error {
	storeIDs := make([]types.ID, 0, len(r.crashed))
	for storeID := range r.crashed {
		storeIDs = append(storeIDs, storeID)
	}
	if len(storeIDs) == 0 {
		return fmt.Errorf("no crashed store to restart")
	}
	slices.Sort(storeIDs)
	storeID := storeIDs[r.rng.Intn(len(storeIDs))]
	if err := r.h.RestartStore(storeID); err != nil {
		return err
	}
	delete(r.crashed, storeID)
	return nil
}

func (r *randomScenarioRunner) partitionStore() error {
	storeID, err := r.pickRunningStore()
	if err != nil {
		return err
	}
	r.h.PartitionStore(storeID)
	r.partitioned[storeID] = true
	return nil
}

func (r *randomScenarioRunner) healStore() error {
	storeIDs := make([]types.ID, 0, len(r.partitioned))
	for storeID := range r.partitioned {
		storeIDs = append(storeIDs, storeID)
	}
	if len(storeIDs) == 0 {
		return fmt.Errorf("no partitioned store to heal")
	}
	slices.Sort(storeIDs)
	storeID := storeIDs[r.rng.Intn(len(storeIDs))]
	r.h.HealStore(storeID)
	delete(r.partitioned, storeID)
	return nil
}

func (r *randomScenarioRunner) crashMetadataNode() error {
	nodeID, err := r.pickMetadataNodeForDisruption(context.Background())
	if err != nil {
		return err
	}
	if err := r.h.CrashMetadataNode(nodeID); err != nil {
		return err
	}
	r.crashedMeta[nodeID] = true
	delete(r.partitionedMeta, nodeID)
	return nil
}

func (r *randomScenarioRunner) restartMetadataNode() error {
	nodeIDs := make([]types.ID, 0, len(r.crashedMeta))
	for nodeID := range r.crashedMeta {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if len(nodeIDs) == 0 {
		return fmt.Errorf("no crashed metadata node to restart")
	}
	slices.Sort(nodeIDs)
	nodeID := nodeIDs[r.rng.Intn(len(nodeIDs))]
	if err := r.h.RestartMetadataNode(nodeID); err != nil {
		return err
	}
	delete(r.crashedMeta, nodeID)
	return nil
}

func (r *randomScenarioRunner) partitionMetadataNode(ctx context.Context) error {
	nodeID, err := r.pickMetadataNodeForDisruption(ctx)
	if err != nil {
		return err
	}
	r.h.PartitionMetadataNode(nodeID)
	r.partitionedMeta[nodeID] = true
	return nil
}

func (r *randomScenarioRunner) healMetadataNode() error {
	nodeIDs := make([]types.ID, 0, len(r.partitionedMeta))
	for nodeID := range r.partitionedMeta {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if len(nodeIDs) == 0 {
		return fmt.Errorf("no partitioned metadata node to heal")
	}
	slices.Sort(nodeIDs)
	nodeID := nodeIDs[r.rng.Intn(len(nodeIDs))]
	r.h.HealMetadataNode(nodeID)
	delete(r.partitionedMeta, nodeID)
	return nil
}

func (r *randomScenarioRunner) dropNextMessage(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		r.availableTransportClasses(ctx),
		r.allowTransportLink,
	)
	if err != nil {
		if r.skipUnavailableTransportFault("drop_next_msg", err) {
			return nil
		}
		return err
	}
	r.h.DropNextTransportMessage(target)
	return nil
}

func (r *randomScenarioRunner) duplicateNextMessage(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		r.availableTransportClasses(ctx),
		r.allowTransportLink,
	)
	if err != nil {
		if r.skipUnavailableTransportFault("duplicate_next_msg", err) {
			return nil
		}
		return err
	}
	r.h.DuplicateNextTransportMessage(target, 1)
	return nil
}

func (r *randomScenarioRunner) cutLink(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		[]RaftMessageClass{
			RaftMessageClassAppend,
			RaftMessageClassHeartbeat,
			RaftMessageClassAppendResp,
			RaftMessageClassHeartbeatResp,
		},
		func(link transportLink) bool {
			if _, ok := r.cutLinks[link]; ok {
				return false
			}
			return r.allowTransportLink(link)
		},
	)
	if err != nil {
		if r.skipUnavailableTransportFault("cut_link", err) {
			return nil
		}
		return err
	}
	r.h.CutTransportLink(target)
	r.cutLinks[target.Link] = target
	return nil
}

func (r *randomScenarioRunner) healLink() error {
	targets := make([]transportFaultTarget, 0, len(r.cutLinks))
	for _, target := range r.cutLinks {
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return fmt.Errorf("no cut link to heal")
	}
	slices.SortFunc(targets, func(a, b transportFaultTarget) int {
		switch {
		case a.Link.From < b.Link.From:
			return -1
		case a.Link.From > b.Link.From:
			return 1
		case a.Link.To < b.Link.To:
			return -1
		case a.Link.To > b.Link.To:
			return 1
		default:
			return 0
		}
	})
	target := targets[r.rng.Intn(len(targets))]
	r.h.HealTransportLink(target)
	delete(r.cutLinks, target.Link)
	return nil
}

func (r *randomScenarioRunner) setLinkLatency(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		[]RaftMessageClass{
			RaftMessageClassAppend,
			RaftMessageClassHeartbeat,
			RaftMessageClassAppendResp,
			RaftMessageClassHeartbeatResp,
		},
		func(link transportLink) bool {
			if _, ok := r.latencyLinks[link]; ok {
				return false
			}
			return r.allowTransportLink(link)
		},
	)
	if err != nil {
		if r.skipUnavailableTransportFault("set_link_latency", err) {
			return nil
		}
		return err
	}
	target.Latency = selectTransportLatency(r.rng)
	r.h.SetTransportLinkLatency(target)
	r.latencyLinks[target.Link] = target
	return nil
}

func (r *randomScenarioRunner) resetLinkLatency() error {
	targets := make([]transportFaultTarget, 0, len(r.latencyLinks))
	for _, target := range r.latencyLinks {
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return fmt.Errorf("no link latency override to reset")
	}
	slices.SortFunc(targets, func(a, b transportFaultTarget) int {
		switch {
		case a.Link.From < b.Link.From:
			return -1
		case a.Link.From > b.Link.From:
			return 1
		case a.Link.To < b.Link.To:
			return -1
		case a.Link.To > b.Link.To:
			return 1
		default:
			return 0
		}
	})
	target := targets[r.rng.Intn(len(targets))]
	r.h.ResetTransportLinkLatency(target)
	delete(r.latencyLinks, target.Link)
	return nil
}

func (r *randomScenarioRunner) availableTransportClasses(ctx context.Context) []RaftMessageClass {
	classes := []RaftMessageClass{
		RaftMessageClassAppend,
		RaftMessageClassAppendResp,
		RaftMessageClassHeartbeat,
		RaftMessageClassHeartbeatResp,
	}
	if len(r.crashed) > 0 || len(r.partitioned) > 0 {
		classes = append(classes, RaftMessageClassVote, RaftMessageClassVoteResp)
	}
	if r.hasActiveSplit(ctx) {
		classes = append(classes, RaftMessageClassSnapshot)
		if r.cfg.PreferSplits {
			classes = append(classes, RaftMessageClassSnapshot, RaftMessageClassSnapshot)
		}
	}
	return classes
}

func (r *randomScenarioRunner) allowTransportLink(link transportLink) bool {
	return !r.crashed[link.From] &&
		!r.crashed[link.To] &&
		!r.partitioned[link.From] &&
		!r.partitioned[link.To]
}

func (r *randomScenarioRunner) stabilize(ctx context.Context, timeout time.Duration, requireStable bool) error {
	for storeID := range r.partitioned {
		r.h.HealStore(storeID)
	}
	clear(r.partitioned)
	for _, target := range r.cutLinks {
		r.h.HealTransportLink(target)
	}
	clear(r.cutLinks)
	for _, target := range r.latencyLinks {
		r.h.ResetTransportLinkLatency(target)
	}
	clear(r.latencyLinks)
	for storeID := range r.crashed {
		if err := r.h.RestartStore(storeID); err != nil {
			return r.wrapErr("stabilize restart", err)
		}
	}
	clear(r.crashed)
	for nodeID := range r.partitionedMeta {
		r.h.HealMetadataNode(nodeID)
	}
	clear(r.partitionedMeta)
	for nodeID := range r.crashedMeta {
		if err := r.h.RestartMetadataNode(nodeID); err != nil {
			return r.wrapErr("stabilize metadata restart", err)
		}
	}
	clear(r.crashedMeta)

	deadline := r.h.Clock().Now().Add(timeout)
	var lastErr error
	for !r.h.Clock().Now().After(deadline) {
		if err := r.h.Advance(2 * time.Second); err != nil {
			return r.wrapErr("stabilize advance", err)
		}
		if len(r.cfg.MetadataIDs) > 1 {
			if _, err := r.h.WaitForMetadataLeader(5 * time.Second); err != nil {
				lastErr = err
				continue
			}
		}
		if err := r.h.TableManager().EnqueueReallocationRequest(ctx); err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "metadata leader unavailable") {
				continue
			}
			return r.wrapErr("stabilize enqueue", err)
		}
		if err := r.h.ReconcileOnce(ctx); err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "metadata leader unavailable") {
				continue
			}
			return r.wrapErr("stabilize reconcile", err)
		}
		var err error
		if requireStable {
			err = r.checker.CheckStable(ctx, r.h)
		} else {
			err = r.checker.Check(ctx, r.h)
		}
		if err == nil {
			_ = r.h.RecordDigest("stabilized")
			return nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return r.wrapErr("stabilize check", lastErr)
	}
	return r.wrapErr("stabilize check", fmt.Errorf("timed out after %s", timeout))
}

func (r *randomScenarioRunner) pickRunningStore() (types.ID, error) {
	candidates := make([]types.ID, 0, len(r.h.storeOrder))
	for _, storeID := range r.h.storeOrder {
		if r.crashed[storeID] || r.partitioned[storeID] {
			continue
		}
		candidates = append(candidates, storeID)
	}
	if len(candidates) == 0 {
		return 0, fmt.Errorf("no running store available")
	}
	return candidates[r.rng.Intn(len(candidates))], nil
}

func (r *randomScenarioRunner) canDisruptMetadata() bool {
	quorum := len(r.cfg.MetadataIDs)/2 + 1
	disrupted := len(r.crashedMeta) + len(r.partitionedMeta)
	return len(r.cfg.MetadataIDs)-disrupted > quorum
}

func (r *randomScenarioRunner) pickMetadataNodeForDisruption(ctx context.Context) (types.ID, error) {
	if !r.canDisruptMetadata() {
		return 0, fmt.Errorf("metadata quorum would be lost")
	}
	candidates := make([]types.ID, 0, len(r.cfg.MetadataIDs))
	for _, nodeID := range r.cfg.MetadataIDs {
		if r.crashedMeta[nodeID] || r.partitionedMeta[nodeID] {
			continue
		}
		candidates = append(candidates, nodeID)
	}
	if len(candidates) == 0 {
		return 0, fmt.Errorf("no running metadata node available")
	}
	if leaderID, err := r.h.WaitForMetadataLeader(5 * time.Second); err == nil {
		if r.hasActiveSplit(ctx) {
			for _, nodeID := range candidates {
				if nodeID == leaderID {
					return nodeID, nil
				}
			}
		}
	}
	return candidates[r.rng.Intn(len(candidates))], nil
}

func (r *randomScenarioRunner) wrapErr(action string, err error) error {
	trace := r.h.Trace().CompactTraceRetainKinds(20, 10, "split")
	category := ClassifyFailureWithTrace(err, trace)
	if category == FailureCategoryUnknown && strings.Contains(trace, "[split]") {
		msg := err.Error()
		if strings.Contains(msg, "metadata leader unavailable") ||
			strings.Contains(msg, "metadata leader not elected within") ||
			strings.Contains(msg, "references missing split child") ||
			strings.Contains(msg, "active split state without child shard id") ||
			strings.Contains(msg, "operation did not complete within") {
			category = FailureCategorySplitLiveness
		}
	}
	actions := append([]ScenarioAction(nil), r.planned...)
	var reduced []ScenarioAction
	if r.reduceFailures {
		var reduceErr error
		reduced, reduceErr = runFailureReduction(func(ctx context.Context) ([]ScenarioAction, error) {
			return ReduceRandomScenarioFailure(ctx, r.cfg, actions, category)
		})
		if reduceErr != nil {
			r.h.recordEvent("reduce", "failed: %v", reduceErr)
		}
	}
	return &ScenarioRunError{
		Kind:           ScenarioKindDocuments,
		Seed:           r.cfg.Seed,
		Action:         action,
		Category:       category,
		Actions:        actions,
		ReducedActions: reduced,
		Trace:          trace,
		Cause:          err,
	}
}

func (r *randomScenarioRunner) shouldDeferStepError(
	ctx context.Context,
	action ScenarioAction,
	err error,
) bool {
	if len(r.cfg.MetadataIDs) <= 1 {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "metadata leader unavailable") ||
		strings.Contains(msg, "metadata leader not elected within") ||
		strings.Contains(msg, "operation did not complete within") ||
		strings.Contains(msg, "references missing split child") {
		return true
	}
	if !isMetadataAction(action) && !r.hasActiveSplit(ctx) &&
		len(r.crashedMeta) == 0 && len(r.partitionedMeta) == 0 {
		return false
	}
	_, hasLeader := r.h.currentMetadataLeader()
	return !hasLeader
}

func (r *randomScenarioRunner) skipUnavailableTransportFault(action string, err error) bool {
	if err == nil {
		return false
	}
	if !strings.Contains(err.Error(), "no transport fault target available") {
		return false
	}
	r.h.recordEvent("transport_fault_skipped", "action=%s reason=%v", action, err)
	return true
}

func isMetadataAction(action ScenarioAction) bool {
	switch action {
	case ActionMetadataCrash, ActionMetadataRestart, ActionMetadataPartition, ActionMetadataHeal:
		return true
	default:
		return false
	}
}
