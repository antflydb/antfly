package sim

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/types"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

type RandomTransactionScenarioConfig struct {
	Seed           int64
	BaseDir        string
	Start          time.Time
	Steps          int
	ActionSettle   time.Duration
	StabilizeEvery int
}

type RandomTransactionScenarioResult struct {
	Seed           int64
	Actions        []ScenarioAction
	ReducedActions []ScenarioAction
	Trace          []string
	Events         []TraceEvent
	Digests        []ClusterDigest
	Txns           int
	Writes         int
}

type randomTransactionRunner struct {
	cfg            RandomTransactionScenarioConfig
	h              *Harness
	checker        *Checker
	rng            *rand.Rand
	actionRNG      *rand.Rand
	trace          []string
	crashed        map[types.ID]bool
	partitioned    map[types.ID]bool
	cutLinks       map[transportLink]transportFaultTarget
	latencyLinks   map[transportLink]transportFaultTarget
	seq            uint64
	txns           int
	writes         int
	faultsUsed     int
	planned        []ScenarioAction
	reduceFailures bool
}

func RunRandomTransactionScenario(ctx context.Context, cfg RandomTransactionScenarioConfig) (*RandomTransactionScenarioResult, error) {
	return RunRandomTransactionScenarioWithActions(ctx, cfg, ScenarioRecord{
		Kind: ScenarioKindTransactions,
		Seed: cfg.Seed,
	})
}

func RunRandomTransactionScenarioWithActions(
	ctx context.Context,
	cfg RandomTransactionScenarioConfig,
	record ScenarioRecord,
) (*RandomTransactionScenarioResult, error) {
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	record.Kind = ScenarioKindTransactions
	record.Seed = cfg.Seed
	if len(record.Actions) > 0 {
		if err := record.Validate(); err != nil {
			return nil, err
		}
		cfg.Steps = len(record.Actions)
	}
	if cfg.BaseDir == "" {
		cfg.BaseDir = filepath.Join(".", "tmp", fmt.Sprintf("sim-random-txn-%d", cfg.Seed))
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Unix(1_700_400_000, 0).UTC()
	}
	if cfg.Steps <= 0 {
		cfg.Steps = 20
	}
	if cfg.ActionSettle <= 0 {
		cfg.ActionSettle = 1200 * time.Millisecond
	}
	if cfg.StabilizeEvery <= 0 {
		cfg.StabilizeEvery = 5
	}

	runner := &randomTransactionRunner{
		cfg:            cfg,
		rng:            rand.New(rand.NewSource(cfg.Seed)),
		actionRNG:      rand.New(rand.NewSource(cfg.Seed ^ 0x5deece66d)),
		crashed:        make(map[types.ID]bool),
		partitioned:    make(map[types.ID]bool),
		cutLinks:       make(map[transportLink]transportFaultTarget),
		latencyLinks:   make(map[transportLink]transportFaultTarget),
		planned:        append([]ScenarioAction(nil), record.Actions...),
		reduceFailures: len(record.Actions) == 0,
	}

	h, err := NewHarness(HarnessConfig{
		BaseDir:           cfg.BaseDir,
		Start:             cfg.Start,
		MetadataID:        100,
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
		TxnIDGenerator: func() uuid.UUID {
			id := makeTxnID(cfg.Seed, runner.seq)
			runner.seq++
			return id
		},
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = h.Close() }()
	runner.h = h
	runner.checker = NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second})

	if _, err := h.CreateTable("accounts", tablemgr.TableConfig{
		NumShards: 4,
		StartID:   0x3000,
	}); err != nil {
		return nil, fmt.Errorf("creating accounts table: %w", err)
	}
	if _, err := startRandomScenarioTable(ctx, h, "accounts"); err != nil {
		return nil, err
	}

	if err := runner.checker.CheckStable(ctx, h); err != nil {
		return nil, fmt.Errorf("initial txn stable check failed: %w", err)
	}
	_ = h.RecordDigest("txn-initial")

	for step := 0; step < cfg.Steps; step++ {
		if err := runner.step(ctx, step); err != nil {
			return nil, err
		}
		if (step+1)%cfg.StabilizeEvery == 0 {
			if err := runner.stabilize(ctx, 20*time.Second); err != nil {
				return nil, err
			}
		}
	}
	if err := runner.stabilize(ctx, 45*time.Second); err != nil {
		return nil, err
	}

	return &RandomTransactionScenarioResult{
		Seed:    cfg.Seed,
		Actions: append([]ScenarioAction(nil), runner.planned...),
		Trace:   append([]string(nil), runner.trace...),
		Events:  h.Trace().Events(),
		Digests: h.Trace().Digests(),
		Txns:    runner.txns,
		Writes:  runner.writes,
	}, nil
}

func (r *randomTransactionRunner) step(ctx context.Context, step int) error {
	action := r.nextAction(ctx, step)
	r.trace = append(r.trace, fmt.Sprintf("%02d:%s", step, action))
	r.h.recordEvent("action", "txn-step=%d action=%s", step, action)

	var err error
	switch action {
	case ActionWrite:
		err = r.singleWrite()
	case ActionTxn:
		err = r.executeTransactionStep(storedb.Op_SyncLevelWrite, "")
	case ActionTxnCommitCrash:
		err = r.executeTransactionStep(storedb.Op_SyncLevelWrite, "commit_crash")
	case ActionTxnResolveCrash:
		err = r.executeTransactionStep(storedb.Op_SyncLevelWrite, "resolve_crash")
	case ActionCrash:
		err = r.crashStore()
	case ActionRestart:
		err = r.restartStore()
	case ActionPartition:
		err = r.partitionStore()
	case ActionHeal:
		err = r.healStore()
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
		err = fmt.Errorf("unknown txn action %q", action)
	}
	if err != nil {
		return r.wrapErr(string(action), err)
	}
	if err := r.h.Advance(r.cfg.ActionSettle); err != nil {
		return r.wrapErr(string(action)+" advance", err)
	}
	if action == ActionRestart || action == ActionHeal || action == ActionTxnCommitCrash || action == ActionTxnResolveCrash {
		if err := r.stabilize(ctx, 20*time.Second); err != nil {
			return r.wrapErr(string(action)+" stabilize", err)
		}
	}
	if err := r.checker.Check(ctx, r.h); err != nil {
		return r.wrapErr(string(action)+" check", err)
	}
	_ = r.h.RecordDigest(fmt.Sprintf("txn-step-%02d-%s", step, action))
	return nil
}

func (r *randomTransactionRunner) nextAction(ctx context.Context, step int) ScenarioAction {
	if step < len(r.planned) {
		return r.planned[step]
	}
	action := r.pickAction(ctx)
	r.planned = append(r.planned, action)
	return action
}

func (r *randomTransactionRunner) pickAction(ctx context.Context) ScenarioAction {
	if !r.txnReady(ctx) {
		actions := []ScenarioAction{ActionTick, ActionTick}
		if len(r.crashed) > 0 {
			actions = append(actions, ActionRestart)
		}
		if len(r.partitioned) > 0 {
			actions = append(actions, ActionHeal)
		}
		if len(r.cutLinks) > 0 {
			actions = append(actions, ActionHealLink)
		}
		if len(r.latencyLinks) > 0 {
			actions = append(actions, ActionResetLinkLatency)
		}
		return actions[r.actionRNG.Intn(len(actions))]
	}

	if len(r.crashed) > 0 || len(r.partitioned) > 0 {
		actions := []ScenarioAction{ActionTick, ActionTick}
		if len(r.crashed) > 0 {
			actions = append(actions, ActionRestart)
		}
		if len(r.partitioned) > 0 {
			actions = append(actions, ActionHeal)
		}
		if len(r.cutLinks) > 0 {
			actions = append(actions, ActionHealLink)
		}
		if len(r.latencyLinks) > 0 {
			actions = append(actions, ActionResetLinkLatency)
		}
		return actions[r.actionRNG.Intn(len(actions))]
	}

	actions := []ScenarioAction{
		ActionWrite,
		ActionTxn,
		ActionTxn,
		ActionTick,
		ActionCrash,
		ActionPartition,
		ActionDropNextMsg,
		ActionDuplicateNextMsg,
	}
	if len(r.latencyLinks) == 0 {
		actions = append(actions, ActionSetLinkLatency)
	} else {
		actions = append(actions, ActionResetLinkLatency)
	}
	if len(r.cutLinks) == 0 {
		actions = append(actions, ActionCutLink)
	} else {
		actions = append(actions, ActionHealLink)
	}
	return actions[r.actionRNG.Intn(len(actions))]
}

func (r *randomTransactionRunner) singleWrite() error {
	key, value := r.makeAccountDoc("solo")
	if err := r.h.WriteKey("accounts", key, value); err != nil {
		return err
	}
	r.writes++
	return nil
}

func (r *randomTransactionRunner) executeTransactionStep(syncLevel storedb.Op_SyncLevel, fault string) error {
	lowKey, lowValue := r.makeAccountDoc("low")
	highKey, highValue := r.makeHighAccountDoc()
	lowShard, err := r.h.ShardForKey("accounts", lowKey)
	if err != nil {
		return err
	}
	highShard, err := r.h.ShardForKey("accounts", highKey)
	if err != nil {
		return err
	}
	if lowShard == highShard {
		return fmt.Errorf("transaction keys landed on same shard %s", lowShard)
	}

	txnID := makeTxnID(r.cfg.Seed, r.seq)
	coordinatorShard := pickCoordinatorForSeed(txnID, lowShard, highShard)
	participantShard := highShard
	if coordinatorShard == highShard {
		participantShard = lowShard
	}

	switch fault {
	case "commit_crash":
		r.faultsUsed++
		r.h.AddAfterRPCFault(1, func(event RPCEvent) bool {
			return event.Method == "CommitTransaction" && event.ShardID == coordinatorShard
		}, func(ctx context.Context, event RPCEvent) error {
			if err := r.h.CrashStore(event.NodeID); err != nil {
				return err
			}
			r.crashed[event.NodeID] = true
			delete(r.partitioned, event.NodeID)
			return nil
		})
	case "resolve_crash":
		r.faultsUsed++
		r.h.AddAfterRPCFault(1, func(event RPCEvent) bool {
			return event.Method == "ResolveIntent" && event.ShardID == participantShard
		}, func(ctx context.Context, event RPCEvent) error {
			if err := r.h.CrashStore(event.NodeID); err != nil {
				return err
			}
			r.crashed[event.NodeID] = true
			delete(r.partitioned, event.NodeID)
			return nil
		})
	}

	execTxn := func() error {
		return r.h.ExecuteTransaction(
			45*time.Second,
			map[types.ID][][2][]byte{
				lowShard:  {{[]byte(lowKey), lowValue}},
				highShard: {{[]byte(highKey), highValue}},
			},
			nil,
			nil,
			nil,
			syncLevel,
		)
	}
	if err := execTxn(); err != nil {
		if fault != "" {
			return err
		}
		var lastErr = err
		for range 3 {
			if err := r.h.Advance(1500 * time.Millisecond); err != nil {
				return err
			}
			if retryErr := execTxn(); retryErr == nil {
				lastErr = nil
				break
			} else {
				lastErr = retryErr
			}
		}
		if lastErr != nil {
			return lastErr
		}
	}
	r.h.TrackExpectedDoc("accounts", lowKey, lowValue)
	r.h.TrackExpectedDoc("accounts", highKey, highValue)
	r.txns++
	return nil
}

func (r *randomTransactionRunner) stabilize(ctx context.Context, timeout time.Duration) error {
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
			return r.wrapErr("txn stabilize restart", err)
		}
	}
	clear(r.crashed)

	deadline := r.h.Clock().Now().Add(timeout)
	var lastErr error
	for !r.h.Clock().Now().After(deadline) {
		if err := r.h.Advance(1500 * time.Millisecond); err != nil {
			return r.wrapErr("txn stabilize advance", err)
		}
		if err := r.checker.CheckStable(ctx, r.h); err == nil {
			_ = r.h.RecordDigest("txn-stabilized")
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return r.wrapErr("txn stabilize check", lastErr)
	}
	return r.wrapErr("txn stabilize check", fmt.Errorf("timed out after %s", timeout))
}

func (r *randomTransactionRunner) crashStore() error {
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

func (r *randomTransactionRunner) restartStore() error {
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

func (r *randomTransactionRunner) partitionStore() error {
	storeID, err := r.pickRunningStore()
	if err != nil {
		return err
	}
	r.h.PartitionStore(storeID)
	r.partitioned[storeID] = true
	return nil
}

func (r *randomTransactionRunner) healStore() error {
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

func (r *randomTransactionRunner) dropNextMessage(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		r.availableTransportClasses(),
		r.allowTransportLink,
	)
	if err != nil {
		return err
	}
	r.h.DropNextTransportMessage(target)
	return nil
}

func (r *randomTransactionRunner) duplicateNextMessage(ctx context.Context) error {
	target, err := selectTransportFaultTarget(
		ctx,
		r.h,
		r.rng,
		r.availableTransportClasses(),
		r.allowTransportLink,
	)
	if err != nil {
		return err
	}
	r.h.DuplicateNextTransportMessage(target, 1)
	return nil
}

func (r *randomTransactionRunner) cutLink(ctx context.Context) error {
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
		return err
	}
	r.h.CutTransportLink(target)
	r.cutLinks[target.Link] = target
	return nil
}

func (r *randomTransactionRunner) healLink() error {
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

func (r *randomTransactionRunner) setLinkLatency(ctx context.Context) error {
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
		return err
	}
	target.Latency = selectTransportLatency(r.rng)
	r.h.SetTransportLinkLatency(target)
	r.latencyLinks[target.Link] = target
	return nil
}

func (r *randomTransactionRunner) resetLinkLatency() error {
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

func (r *randomTransactionRunner) pickRunningStore() (types.ID, error) {
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

func (r *randomTransactionRunner) availableTransportClasses() []RaftMessageClass {
	classes := []RaftMessageClass{
		RaftMessageClassAppend,
		RaftMessageClassAppendResp,
		RaftMessageClassHeartbeat,
		RaftMessageClassHeartbeatResp,
	}
	if len(r.crashed) > 0 || len(r.partitioned) > 0 {
		classes = append(classes, RaftMessageClassVote, RaftMessageClassVoteResp)
	}
	return classes
}

func (r *randomTransactionRunner) allowTransportLink(link transportLink) bool {
	return !r.crashed[link.From] &&
		!r.crashed[link.To] &&
		!r.partitioned[link.From] &&
		!r.partitioned[link.To]
}

func (r *randomTransactionRunner) makeAccountDoc(class string) (string, []byte) {
	key := fmt.Sprintf("%x/acct/%s/%02d/%08x", 1+r.rng.Intn(6), class, r.txns+r.writes, r.rng.Uint32())
	value := fmt.Appendf(nil, `{"value":"%s-%d-%08x"}`, class, r.txns+r.writes, r.rng.Uint32())
	return key, value
}

func (r *randomTransactionRunner) makeHighAccountDoc() (string, []byte) {
	prefixes := []string{"a", "b", "d", "f"}
	prefix := prefixes[r.rng.Intn(len(prefixes))]
	key := fmt.Sprintf("%s/acct/high/%02d/%08x", prefix, r.txns+r.writes, r.rng.Uint32())
	value := fmt.Appendf(nil, `{"value":"high-%d-%08x"}`, r.txns+r.writes, r.rng.Uint32())
	return key, value
}

func (r *randomTransactionRunner) wrapErr(action string, err error) error {
	category := ClassifyFailure(err)
	actions := append([]ScenarioAction(nil), r.planned...)
	var reduced []ScenarioAction
	if r.reduceFailures {
		var reduceErr error
		reduced, reduceErr = ReduceRandomTransactionScenarioFailure(context.Background(), r.cfg, actions, category)
		if reduceErr != nil {
			r.h.recordEvent("reduce", "failed: %v", reduceErr)
		}
	}
	return &ScenarioRunError{
		Kind:           ScenarioKindTransactions,
		Seed:           r.cfg.Seed,
		Action:         action,
		Category:       category,
		Actions:        actions,
		ReducedActions: reduced,
		Trace:          r.h.Trace().CompactTrace(24, 10),
		Cause:          err,
	}
}

func (r *randomTransactionRunner) txnReady(ctx context.Context) bool {
	snapshot, err := r.h.Snapshot(ctx)
	if err != nil {
		return false
	}
	for _, storeStatus := range snapshot.Stores {
		if storeStatus == nil || !storeStatus.IsReachable() {
			return false
		}
	}
	for _, shardStatus := range snapshot.Shards {
		if shardStatus == nil || shardStatus.RaftStatus == nil || shardStatus.RaftStatus.Lead == 0 {
			return false
		}
	}
	return true
}

func makeTxnID(seed int64, seq uint64) uuid.UUID {
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[0:8], uint64(seed))
	binary.BigEndian.PutUint64(id[8:16], seq+1)
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

func pickCoordinatorForSeed(txnID uuid.UUID, shardIDs ...types.ID) types.ID {
	ordered := append([]types.ID(nil), shardIDs...)
	slices.Sort(ordered)
	return ordered[xxhash.Sum64(txnID[:])%uint64(len(ordered))]
}
