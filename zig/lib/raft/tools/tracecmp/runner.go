// Copyright 2026 Antfly, Inc.
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

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"unsafe"

	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type TraceFile struct {
	Version int         `json:"version"`
	Peers   []uint64    `json:"peers"`
	Config  TraceConfig `json:"config"`
	Steps   []TraceStep `json:"steps"`
}

type TraceConfig struct {
	ElectionTick                int            `json:"election_tick"`
	HeartbeatTick               int            `json:"heartbeat_tick"`
	RandomSeed                  uint64         `json:"random_seed"`
	MaxSizePerMsg               uint64         `json:"max_size_per_msg"`
	MaxCommittedSizePerReady    uint64         `json:"max_committed_size_per_ready"`
	MaxInflightMsgs             int            `json:"max_inflight_msgs"`
	MaxInflightBytes            uint64         `json:"max_inflight_bytes"`
	MaxUncommittedEntriesSize   uint64         `json:"max_uncommitted_entries_size"`
	AsyncStorageWrites          bool           `json:"async_storage_writes"`
	CheckQuorum                 bool           `json:"check_quorum"`
	PreVote                     bool           `json:"pre_vote"`
	StepDownOnRemoval           bool           `json:"step_down_on_removal"`
	DisableProposalForwarding   bool           `json:"disable_proposal_forwarding"`
	DisableConfChangeValidation bool           `json:"disable_conf_change_validation"`
	ReadOnlyOption              string         `json:"read_only_option"`
	InitialConfState            TraceConfState `json:"initial_conf_state"`
}

type TraceConfState struct {
	Voters         []uint64 `json:"voters"`
	VotersOutgoing []uint64 `json:"voters_outgoing"`
	Learners       []uint64 `json:"learners"`
	LearnersNext   []uint64 `json:"learners_next"`
	AutoLeave      bool     `json:"auto_leave"`
}

type TraceStep struct {
	Action     TraceAction        `json:"action"`
	Nodes      []NodeSnapshot     `json:"nodes"`
	Messages   []MessageSummary   `json:"messages"`
	Committed  []CommittedSummary `json:"committed"`
	ReadStates []ReadStateSummary `json:"read_states"`
	ConfStates []ConfStateSummary `json:"conf_states"`
}

type TraceAction struct {
	Kind         string                  `json:"kind"`
	NodeID       uint64                  `json:"node_id,omitempty"`
	Count        int                     `json:"count,omitempty"`
	Timeout      int                     `json:"timeout,omitempty"`
	Applied      uint64                  `json:"applied,omitempty"`
	PreVote      bool                    `json:"pre_vote,omitempty"`
	Data         string                  `json:"data,omitempty"`
	RequestCtx   string                  `json:"request_ctx,omitempty"`
	From         uint64                  `json:"from,omitempty"`
	To           uint64                  `json:"to,omitempty"`
	LogIndex     uint64                  `json:"log_index,omitempty"`
	CompactIndex uint64                  `json:"compact_index,omitempty"`
	ChangeType   string                  `json:"change_type,omitempty"`
	Transition   string                  `json:"transition,omitempty"`
	TargetNodeID uint64                  `json:"target_node_id,omitempty"`
	Changes      []TraceConfChangeAction `json:"changes,omitempty"`
}

type TraceConfChangeAction struct {
	ChangeType   string `json:"change_type"`
	TargetNodeID uint64 `json:"target_node_id"`
}

type NodeSnapshot struct {
	NodeID      uint64  `json:"node_id"`
	Role        string  `json:"role"`
	LeaderID    *uint64 `json:"leader_id"`
	Term        uint64  `json:"term"`
	VotedFor    *uint64 `json:"voted_for"`
	CommitIndex uint64  `json:"commit_index"`
}

type MessageSummary struct {
	Type            string `json:"type"`
	From            uint64 `json:"from"`
	To              uint64 `json:"to"`
	Term            uint64 `json:"term"`
	LogIndex        uint64 `json:"log_index"`
	LogTerm         uint64 `json:"log_term"`
	CommitIndex     uint64 `json:"commit_index"`
	Reject          bool   `json:"reject"`
	RejectHint      uint64 `json:"reject_hint"`
	EntriesLen      int    `json:"entries_len"`
	FirstEntryIndex uint64 `json:"first_entry_index"`
	LastEntryIndex  uint64 `json:"last_entry_index"`
}

type CommittedSummary struct {
	NodeID     uint64 `json:"node_id"`
	Count      int    `json:"count"`
	FirstIndex uint64 `json:"first_index"`
	LastIndex  uint64 `json:"last_index"`
}

type ReadStateSummary struct {
	NodeID     uint64 `json:"node_id"`
	Index      uint64 `json:"index"`
	RequestCtx string `json:"request_ctx"`
}

type ConfStateSummary struct {
	NodeID         uint64   `json:"node_id"`
	Voters         []uint64 `json:"voters"`
	VotersOutgoing []uint64 `json:"voters_outgoing"`
	Learners       []uint64 `json:"learners"`
	LearnersNext   []uint64 `json:"learners_next"`
	AutoLeave      bool     `json:"auto_leave"`
}

type mismatchError struct {
	StepIndex int
	Action    TraceAction
	Expected  TraceStep
	Actual    TraceStep
}

func (m *mismatchError) Error() string {
	expectedJSON, _ := json.MarshalIndent(m.Expected, "", "  ")
	actualJSON, _ := json.MarshalIndent(m.Actual, "", "  ")
	return fmt.Sprintf(
		"trace mismatch at step %d (%s)\nexpected:\n%s\nactual:\n%s",
		m.StepIndex,
		m.Action.Kind,
		expectedJSON,
		actualJSON,
	)
}

func CompareTraceFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read trace file: %w", err)
	}
	return CompareTraceData(data)
}

func CompareTraceData(data []byte) error {
	traceFile, err := LoadTraceData(data)
	if err != nil {
		return err
	}

	cluster, err := newReplayCluster(traceFile)
	if err != nil {
		return err
	}

	for i, step := range traceFile.Steps {
		if err := cluster.applyAction(step.Action); err != nil {
			return fmt.Errorf("step %d (%s): %w", i, step.Action.Kind, err)
		}

		actual := cluster.snapshot(step.Action)
		if !reflect.DeepEqual(step, actual) {
			return &mismatchError{
				StepIndex: i,
				Action:    step.Action,
				Expected:  step,
				Actual:    actual,
			}
		}
	}

	return nil
}

func LoadTraceFile(path string) (*TraceFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read trace file: %w", err)
	}
	return LoadTraceData(data)
}

func LoadTraceData(data []byte) (*TraceFile, error) {
	var traceFile TraceFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&traceFile); err != nil {
		return nil, fmt.Errorf("decode trace file: %w", err)
	}
	if traceFile.Version != 1 {
		return nil, fmt.Errorf("unsupported trace version %d", traceFile.Version)
	}
	if len(traceFile.Peers) == 0 {
		return nil, errors.New("trace peers must not be empty")
	}
	return &traceFile, nil
}

type SeededSweepOptions struct {
	ZigBin      string
	SeedStart   uint64
	Count       int
	Steps       int
	CheckQuorum bool
	PreVote     bool
	Profile     string
}

func RunSeededSweep(opts SeededSweepOptions) error {
	if opts.Count <= 0 {
		return errors.New("seeded sweep count must be positive")
	}
	if opts.Steps <= 0 {
		return errors.New("seeded sweep steps must be positive")
	}

	zigBin := opts.ZigBin
	if zigBin == "" {
		zigBin = discoverZigBinary()
	}

	for i := 0; i < opts.Count; i++ {
		seed := opts.SeedStart + uint64(i)
		data, err := emitSeededTrace(zigBin, seed, opts.Steps, opts.CheckQuorum, opts.PreVote, opts.Profile)
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		if err := CompareTraceData(data); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
	}

	return nil
}

func emitSeededTrace(zigBin string, seed uint64, steps int, checkQuorum bool, preVote bool, profile string) ([]byte, error) {
	if profile == "" {
		profile = "stable"
	}
	args := []string{
		"run",
		"../../src/emit_seeded_trace.zig",
		"--",
		fmt.Sprintf("%d", seed),
		fmt.Sprintf("%d", steps),
		fmt.Sprintf("%t", checkQuorum),
		fmt.Sprintf("%t", preVote),
		profile,
	}
	cmd := exec.Command(zigBin, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("emit seeded trace: %w\n%s", err, output)
	}
	return output, nil
}

func discoverZigBinary() string {
	if zig := os.Getenv("ZIG"); zig != "" {
		return zig
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidate := filepath.Join(home, "bin", "zig")
		if _, statErr := os.Stat(candidate); statErr == nil {
			return candidate
		}
	}
	return "zig"
}

type replayCluster struct {
	peerOrder   []uint64
	config      TraceConfig
	nodes       map[uint64]*replayNode
	network     []pb.Message
	blocked     map[[2]uint64]struct{}
	nodePreVote map[uint64]bool
	active      map[uint64]struct{}
	committed   map[uint64][]pb.Entry
	readStates  map[uint64][]raft.ReadState
	confStates  map[uint64]pb.ConfState
}

type replayNode struct {
	raw     *raft.RawNode
	storage *seededStorage
}

func setRandomizedElectionTimeout(raw *raft.RawNode, timeout int) {
	rawValue := reflect.ValueOf(raw).Elem()
	raftField := rawValue.FieldByName("raft")
	raftValue := reflect.NewAt(raftField.Type(), unsafe.Pointer(raftField.UnsafeAddr())).Elem().Elem()
	timeoutField := raftValue.FieldByName("randomizedElectionTimeout")
	reflect.NewAt(timeoutField.Type(), unsafe.Pointer(timeoutField.UnsafeAddr())).Elem().SetInt(int64(timeout))
}

type seededStorage struct {
	*raft.MemoryStorage
	confState pb.ConfState
}

func newReplayCluster(traceFile *TraceFile) (*replayCluster, error) {
	peerOrder := slices.Clone(traceFile.Peers)
	nodes := make(map[uint64]*replayNode, len(peerOrder))
	nodePreVote := make(map[uint64]bool, len(peerOrder))

	for _, id := range peerOrder {
		initialConfState := traceConfStateToPB(traceFile.Config.InitialConfState)
		if len(initialConfState.Voters) == 0 && len(initialConfState.VotersOutgoing) == 0 && len(initialConfState.Learners) == 0 && len(initialConfState.LearnersNext) == 0 {
			initialConfState.Voters = slices.Clone(peerOrder)
		}
		storage := &seededStorage{
			MemoryStorage: raft.NewMemoryStorage(),
			confState:     initialConfState,
		}

		raw, err := raft.NewRawNode(&raft.Config{
			ID:                          id,
			ElectionTick:                traceFile.Config.ElectionTick,
			HeartbeatTick:               traceFile.Config.HeartbeatTick,
			Storage:                     storage,
			MaxSizePerMsg:               defaultMaxSizePerMsg(traceFile.Config.MaxSizePerMsg),
			MaxCommittedSizePerReady:    defaultMaxCommittedSizePerReady(traceFile.Config.MaxCommittedSizePerReady, traceFile.Config.MaxSizePerMsg),
			MaxInflightMsgs:             defaultMaxInflight(traceFile.Config.MaxInflightMsgs),
			MaxInflightBytes:            traceFile.Config.MaxInflightBytes,
			MaxUncommittedEntriesSize:   defaultMaxUncommittedEntriesSize(traceFile.Config.MaxUncommittedEntriesSize),
			AsyncStorageWrites:          traceFile.Config.AsyncStorageWrites,
			CheckQuorum:                 traceFile.Config.CheckQuorum,
			PreVote:                     traceFile.Config.PreVote,
			StepDownOnRemoval:           traceFile.Config.StepDownOnRemoval,
			DisableProposalForwarding:   traceFile.Config.DisableProposalForwarding,
			DisableConfChangeValidation: traceFile.Config.DisableConfChangeValidation,
			ReadOnlyOption:              mapReadOnlyOption(traceFile.Config.ReadOnlyOption),
			Logger:                      &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
		})
		if err != nil {
			return nil, fmt.Errorf("init raw node %d: %w", id, err)
		}

		nodePreVote[id] = traceFile.Config.PreVote
		nodes[id] = &replayNode{
			raw:     raw,
			storage: storage,
		}
	}

	initialConfState := traceConfStateToPB(traceFile.Config.InitialConfState)
	if len(initialConfState.Voters) == 0 && len(initialConfState.VotersOutgoing) == 0 && len(initialConfState.Learners) == 0 && len(initialConfState.LearnersNext) == 0 {
		initialConfState.Voters = slices.Clone(peerOrder)
	}

	return &replayCluster{
		peerOrder:   peerOrder,
		config:      traceFile.Config,
		nodes:       nodes,
		blocked:     make(map[[2]uint64]struct{}),
		nodePreVote: nodePreVote,
		active:      seedActiveNodes(peerOrder, initialConfState),
		committed:   make(map[uint64][]pb.Entry, len(peerOrder)),
		readStates:  make(map[uint64][]raft.ReadState, len(peerOrder)),
		confStates:  seedConfStates(peerOrder, initialConfState),
	}, nil
}

func (s *seededStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	hardState, _, err := s.MemoryStorage.InitialState()
	return hardState, s.confState, err
}

func (c *replayCluster) applyAction(action TraceAction) error {
	switch action.Kind {
	case "tick":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		for range action.Count {
			node.raw.Tick()
		}
		return c.collectReady(action.NodeID)
	case "set_randomized_election_timeout":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		setRandomizedElectionTimeout(node.raw, action.Timeout)
		return nil
	case "campaign":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		if err := node.raw.Campaign(); err != nil {
			return err
		}
		return c.collectReady(action.NodeID)
	case "campaign_settle":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		if err := node.raw.Campaign(); err != nil {
			return err
		}
		if err := c.collectReady(action.NodeID); err != nil {
			return err
		}
		for node.raw.HasReady() {
			if err := c.collectReady(action.NodeID); err != nil {
				return err
			}
		}
		return nil
	case "transfer_leader":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		node.raw.TransferLeader(action.TargetNodeID)
		return c.collectReady(action.NodeID)
	case "forget_leader":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		if err := node.raw.ForgetLeader(); err != nil {
			return err
		}
		return c.collectReady(action.NodeID)
	case "propose":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		if err := node.raw.Propose([]byte(action.Data)); err != nil {
			return err
		}
		return c.collectReady(action.NodeID)
	case "propose_dropped":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		err := node.raw.Propose([]byte(action.Data))
		if !errors.Is(err, raft.ErrProposalDropped) {
			return fmt.Errorf("expected ErrProposalDropped, got %v", err)
		}
		return c.collectReady(action.NodeID)
	case "read_index":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		node.raw.ReadIndex([]byte(action.RequestCtx))
		return c.collectReady(action.NodeID)
	case "read_index_not_leader":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		node.raw.ReadIndex([]byte(action.RequestCtx))
		return c.collectReady(action.NodeID)
	case "propose_conf_change":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		var changeType pb.ConfChangeType
		switch action.ChangeType {
		case "add_node":
			changeType = pb.ConfChangeAddNode
		case "remove_node":
			changeType = pb.ConfChangeRemoveNode
		case "add_learner_node":
			changeType = pb.ConfChangeAddLearnerNode
		default:
			return fmt.Errorf("unsupported conf change type %q", action.ChangeType)
		}
		if err := node.raw.ProposeConfChange(pb.ConfChange{
			Type:   changeType,
			NodeID: action.TargetNodeID,
		}); err != nil {
			return err
		}
		return c.collectReady(action.NodeID)
	case "propose_conf_change_v2":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		initialConfState := cloneConfState(c.confStates[action.NodeID])
		changes := make([]pb.ConfChangeSingle, 0, max(len(action.Changes), 1))
		if len(action.Changes) > 0 {
			for _, change := range action.Changes {
				changeType, err := mapConfChangeType(change.ChangeType)
				if err != nil {
					return err
				}
				changes = append(changes, pb.ConfChangeSingle{
					Type:   changeType,
					NodeID: change.TargetNodeID,
				})
			}
		} else {
			changeType, err := mapConfChangeType(action.ChangeType)
			if err != nil {
				return err
			}
			changes = append(changes, pb.ConfChangeSingle{
				Type:   changeType,
				NodeID: action.TargetNodeID,
			})
		}
		var transition pb.ConfChangeTransition
		switch action.Transition {
		case "auto":
			transition = pb.ConfChangeTransitionAuto
		case "joint_explicit":
			transition = pb.ConfChangeTransitionJointExplicit
		case "joint_implicit":
			transition = pb.ConfChangeTransitionJointImplicit
		default:
			return fmt.Errorf("unsupported conf change transition %q", action.Transition)
		}
		if err := node.raw.ProposeConfChange(pb.ConfChangeV2{
			Transition: transition,
			Changes:    changes,
		}); err != nil {
			return err
		}
		for {
			if err := c.collectReady(action.NodeID); err != nil {
				return err
			}
			changed := !reflect.DeepEqual(c.confStates[action.NodeID], initialConfState)
			settledImplicit := action.Transition == "joint_implicit" &&
				changed &&
				len(c.confStates[action.NodeID].VotersOutgoing) == 0 &&
				!node.raw.HasReady()
			if (action.Transition != "joint_implicit" && changed) || settledImplicit || !node.raw.HasReady() {
				return nil
			}
		}
	case "leave_joint":
		node, ok := c.nodes[action.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %d", action.NodeID)
		}
		initialConfState := cloneConfState(c.confStates[action.NodeID])
		if err := node.raw.ProposeConfChange(pb.ConfChangeV2{}); err != nil {
			return err
		}
		for {
			if err := c.collectReady(action.NodeID); err != nil {
				return err
			}
			if !reflect.DeepEqual(c.confStates[action.NodeID], initialConfState) || !node.raw.HasReady() {
				return nil
			}
		}
	case "collect_ready":
		return c.collectReady(action.NodeID)
	case "drain_committed":
		c.committed[action.NodeID] = nil
		return nil
	case "restart_node":
		return c.restartNode(action.NodeID, c.nodePreVote[action.NodeID], 0)
	case "restart_node_with_applied":
		return c.restartNode(action.NodeID, c.nodePreVote[action.NodeID], action.Applied)
	case "restart_node_with_pre_vote":
		c.nodePreVote[action.NodeID] = action.PreVote
		return c.restartNode(action.NodeID, action.PreVote, 0)
	case "compact_node":
		return c.compactNode(action.NodeID, action.CompactIndex)
	case "reject_snapshot":
		return c.rejectSnapshot(action.From, action.To)
	case "abort_snapshot":
		return c.abortSnapshot(action.From, action.To, action.LogIndex)
	case "deliver_one":
		return c.deliverOne()
	case "deliver_all":
		return c.deliverAll()
	case "block_link":
		c.blocked[[2]uint64{action.From, action.To}] = struct{}{}
		return nil
	case "unblock_link":
		delete(c.blocked, [2]uint64{action.From, action.To})
		return nil
	case "clear_blocks":
		clear(c.blocked)
		return nil
	default:
		return fmt.Errorf("unsupported action %q", action.Kind)
	}
}

func (c *replayCluster) restartNode(nodeID uint64, preVote bool, applied uint64) error {
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}

	raw, err := raft.NewRawNode(&raft.Config{
		ID:                          nodeID,
		ElectionTick:                c.config.ElectionTick,
		HeartbeatTick:               c.config.HeartbeatTick,
		Storage:                     node.storage,
		Applied:                     applied,
		MaxSizePerMsg:               defaultMaxSizePerMsg(c.config.MaxSizePerMsg),
		MaxCommittedSizePerReady:    defaultMaxCommittedSizePerReady(c.config.MaxCommittedSizePerReady, c.config.MaxSizePerMsg),
		MaxInflightMsgs:             defaultMaxInflight(c.config.MaxInflightMsgs),
		MaxInflightBytes:            c.config.MaxInflightBytes,
		MaxUncommittedEntriesSize:   defaultMaxUncommittedEntriesSize(c.config.MaxUncommittedEntriesSize),
		AsyncStorageWrites:          c.config.AsyncStorageWrites,
		CheckQuorum:                 c.config.CheckQuorum,
		PreVote:                     preVote,
		StepDownOnRemoval:           c.config.StepDownOnRemoval,
		DisableProposalForwarding:   c.config.DisableProposalForwarding,
		DisableConfChangeValidation: c.config.DisableConfChangeValidation,
		ReadOnlyOption:              mapReadOnlyOption(c.config.ReadOnlyOption),
		Logger:                      &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	})
	if err != nil {
		return fmt.Errorf("restart raw node %d: %w", nodeID, err)
	}

	node.raw = raw
	return c.collectReady(nodeID)
}

func defaultMaxInflight(v int) int {
	if v <= 0 {
		return 256
	}
	return v
}

func defaultMaxSizePerMsg(v uint64) uint64 {
	if v == 0 {
		return math.MaxUint64
	}
	return v
}

func defaultMaxCommittedSizePerReady(v uint64, maxSizePerMsg uint64) uint64 {
	if v != 0 {
		return v
	}
	return defaultMaxSizePerMsg(maxSizePerMsg)
}

func defaultMaxUncommittedEntriesSize(v uint64) uint64 {
	if v == 0 {
		return math.MaxUint64
	}
	return v
}

func (c *replayCluster) compactNode(nodeID uint64, index uint64) error {
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}

	confState := cloneConfState(c.confStates[nodeID])
	snapshot, err := node.storage.CreateSnapshot(index, &confState, nil)
	if err != nil {
		return fmt.Errorf("create snapshot for node %d at %d: %w", nodeID, index, err)
	}
	if err := node.storage.Compact(index); err != nil {
		return fmt.Errorf("compact node %d at %d: %w", nodeID, index, err)
	}
	_ = snapshot
	return nil
}

func (c *replayCluster) rejectSnapshot(from uint64, to uint64) error {
	for {
		for i, msg := range c.network {
			if msg.Type != pb.MsgSnap || msg.From != from || msg.To != to {
				continue
			}

			c.network = append(c.network[:i], c.network[i+1:]...)
			leader, ok := c.nodes[from]
			if !ok {
				return fmt.Errorf("unknown leader %d", from)
			}
			leader.raw.ReportSnapshot(to, raft.SnapshotFailure)
			return c.collectReady(from)
		}

		if len(c.network) == 0 {
			return fmt.Errorf("snapshot message %d->%d not found", from, to)
		}
		if err := c.deliverOne(); err != nil {
			return err
		}
	}
}

func (c *replayCluster) abortSnapshot(from uint64, to uint64, logIndex uint64) error {
	for {
		for i, msg := range c.network {
			if msg.Type != pb.MsgSnap || msg.From != from || msg.To != to {
				continue
			}

			c.network = append(c.network[:i], c.network[i+1:]...)
			leader, ok := c.nodes[from]
			if !ok {
				return fmt.Errorf("unknown leader %d", from)
			}
			if err := leader.raw.Step(pb.Message{
				Type:  pb.MsgAppResp,
				From:  to,
				To:    from,
				Term:  msg.Term,
				Index: logIndex,
			}); err != nil {
				return err
			}
			return c.collectReady(from)
		}

		if len(c.network) == 0 {
			return fmt.Errorf("snapshot message %d->%d not found", from, to)
		}
		if err := c.deliverOne(); err != nil {
			return err
		}
	}
}

func (c *replayCluster) collectReady(nodeID uint64) error {
	node := c.nodes[nodeID]
	if !node.raw.HasReady() {
		return nil
	}

	rd := node.raw.Ready()
	if c.config.AsyncStorageWrites {
		if len(rd.ReadStates) > 0 {
			c.readStates[nodeID] = append(c.readStates[nodeID], cloneReadStates(rd.ReadStates)...)
		}
		return c.handleAsyncReady(nodeID, rd)
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := node.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("apply snapshot for node %d: %w", nodeID, err)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := node.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("persist hard state for node %d: %w", nodeID, err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := node.storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("append entries for node %d: %w", nodeID, err)
		}
	}
	if len(rd.CommittedEntries) > 0 {
		c.committed[nodeID] = append(c.committed[nodeID], cloneEntries(rd.CommittedEntries)...)
		if err := c.applyConfChanges(nodeID, rd.CommittedEntries); err != nil {
			return err
		}
	}
	if len(rd.ReadStates) > 0 {
		c.readStates[nodeID] = append(c.readStates[nodeID], cloneReadStates(rd.ReadStates)...)
	}
	if len(rd.Messages) > 0 {
		c.network = append(c.network, cloneMessages(rd.Messages)...)
	}

	node.raw.Advance(rd)
	return nil
}

func (c *replayCluster) handleAsyncReady(nodeID uint64, rd raft.Ready) error {
	node := c.nodes[nodeID]
	for _, msg := range rd.Messages {
		switch msg.Type {
		case pb.MsgStorageAppend:
			if msg.Snapshot != nil {
				if err := node.storage.ApplySnapshot(*msg.Snapshot); err != nil {
					return fmt.Errorf("apply snapshot for node %d: %w", nodeID, err)
				}
			}
			hs := pb.HardState{
				Term:   msg.Term,
				Vote:   msg.Vote,
				Commit: msg.Commit,
			}
			if !raft.IsEmptyHardState(hs) {
				if err := node.storage.SetHardState(hs); err != nil {
					return fmt.Errorf("persist hard state for node %d: %w", nodeID, err)
				}
			}
			if len(msg.Entries) > 0 {
				if err := node.storage.Append(msg.Entries); err != nil {
					return fmt.Errorf("append entries for node %d: %w", nodeID, err)
				}
			}
			if err := c.dispatchAsyncResponses(nodeID, msg.Responses); err != nil {
				return err
			}
		case pb.MsgStorageApply:
			if len(msg.Entries) > 0 {
				c.committed[nodeID] = append(c.committed[nodeID], cloneEntries(msg.Entries)...)
				if err := c.applyConfChanges(nodeID, msg.Entries); err != nil {
					return err
				}
			}
			if err := c.dispatchAsyncResponses(nodeID, msg.Responses); err != nil {
				return err
			}
		default:
			c.network = append(c.network, cloneMessages([]pb.Message{msg})...)
		}
	}
	return nil
}

func (c *replayCluster) dispatchAsyncResponses(nodeID uint64, responses []pb.Message) error {
	for _, response := range responses {
		if response.To == nodeID {
			node := c.nodes[nodeID]
			if err := node.raw.Step(response); err != nil {
				return fmt.Errorf("deliver local async response %s to %d: %w", response.Type, nodeID, err)
			}
			if err := c.collectReady(nodeID); err != nil {
				return err
			}
		} else {
			c.network = append(c.network, cloneMessages([]pb.Message{response})...)
		}
	}
	return nil
}

func (c *replayCluster) applyConfChanges(nodeID uint64, entries []pb.Entry) error {
	node := c.nodes[nodeID]
	for _, entry := range entries {
		switch entry.Type {
		case pb.EntryConfChange:
			before := cloneConfState(c.confStates[nodeID])
			var confChange pb.ConfChange
			if err := confChange.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("decode conf change on node %d: %w", nodeID, err)
			}
			cs := node.raw.ApplyConfChange(confChange.AsV2())
			c.confStates[nodeID] = cloneConfState(*cs)
			node.storage.confState = cloneConfState(*cs)
			c.syncNodeActivity(before, *cs)
			c.dropRemovedConfStateMessages(before, *cs)
		case pb.EntryConfChangeV2:
			before := cloneConfState(c.confStates[nodeID])
			var confChange pb.ConfChangeV2
			if err := confChange.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("decode conf change v2 on node %d: %w", nodeID, err)
			}
			if len(confChange.Changes) > 0 && len(before.VotersOutgoing) > 0 {
				continue
			}
			if len(confChange.Changes) == 0 && len(before.VotersOutgoing) == 0 {
				continue
			}
			cs := node.raw.ApplyConfChange(confChange)
			c.confStates[nodeID] = cloneConfState(*cs)
			node.storage.confState = cloneConfState(*cs)
			c.syncNodeActivity(before, *cs)
			c.dropRemovedConfStateMessages(before, *cs)
		}
	}
	return nil
}

func (c *replayCluster) dropMessagesForNode(nodeID uint64) {
	filtered := c.network[:0]
	for _, msg := range c.network {
		if msg.From == nodeID || msg.To == nodeID {
			continue
		}
		filtered = append(filtered, msg)
	}
	c.network = filtered
}

func (c *replayCluster) activateNode(nodeID uint64) {
	c.active[nodeID] = struct{}{}
}

func (c *replayCluster) nodeActive(nodeID uint64) bool {
	_, ok := c.active[nodeID]
	return ok
}

func (c *replayCluster) syncNodeActivity(before pb.ConfState, after pb.ConfState) {
	for _, nodeID := range c.peerOrder {
		beforeContains := confStateContainsNode(before, nodeID)
		afterContains := confStateContainsNode(after, nodeID)
		if !beforeContains && afterContains {
			c.activateNode(nodeID)
		}
	}
}

func (c *replayCluster) dropRemovedConfStateMessages(before pb.ConfState, after pb.ConfState) {
	for _, nodeID := range c.peerOrder {
		if !confStateContainsNode(before, nodeID) {
			continue
		}
		if confStateContainsNode(after, nodeID) {
			continue
		}
		c.dropMessagesForNode(nodeID)
	}
}

func confStateContainsNode(confState pb.ConfState, nodeID uint64) bool {
	return slices.Contains(confState.Voters, nodeID) ||
		slices.Contains(confState.VotersOutgoing, nodeID) ||
		slices.Contains(confState.Learners, nodeID) ||
		slices.Contains(confState.LearnersNext, nodeID)
}

func (c *replayCluster) deliverAll() error {
	for {
		if len(c.network) > 0 {
			if err := c.deliverOne(); err != nil {
				return err
			}
			continue
		}
		collected, err := c.collectOneReady()
		if err != nil {
			return err
		}
		if collected {
			continue
		}
		return nil
	}
}

func (c *replayCluster) deliverOne() error {
	if len(c.network) == 0 {
		return errors.New("no pending messages")
	}

	msg := c.network[0]
	c.network = c.network[1:]

	if _, blocked := c.blocked[[2]uint64{msg.From, msg.To}]; blocked {
		return nil
	}
	if !c.nodeActive(msg.From) || !c.nodeActive(msg.To) {
		return nil
	}

	target, ok := c.nodes[msg.To]
	if !ok {
		return fmt.Errorf("unknown message target %d", msg.To)
	}
	if err := target.raw.Step(msg); err != nil {
		return fmt.Errorf("deliver %s %d->%d: %w", msg.Type, msg.From, msg.To, err)
	}
	return c.collectReady(msg.To)
}

func (c *replayCluster) collectOneReady() (bool, error) {
	for _, nodeID := range c.peerOrder {
		if !c.nodeActive(nodeID) {
			continue
		}
		node := c.nodes[nodeID]
		if !node.raw.HasReady() {
			continue
		}
		if err := c.collectReady(nodeID); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (c *replayCluster) snapshot(action TraceAction) TraceStep {
	step := TraceStep{
		Action: action,
		Nodes:  make([]NodeSnapshot, 0, len(c.peerOrder)),
	}

	for _, id := range c.peerOrder {
		status := c.nodes[id].raw.Status()
		step.Nodes = append(step.Nodes, NodeSnapshot{
			NodeID:      id,
			Role:        mapRole(status.RaftState),
			LeaderID:    optionalUint64(status.Lead),
			Term:        status.Term,
			VotedFor:    optionalUint64(status.Vote),
			CommitIndex: status.Commit,
		})
	}

	step.Messages = make([]MessageSummary, 0, len(c.network))
	for _, msg := range c.network {
		firstIndex := uint64(0)
		lastIndex := uint64(0)
		logIndex := msg.Index
		logTerm := msg.LogTerm
		if msg.Type == pb.MsgSnap {
			logIndex = msg.Snapshot.Metadata.Index
			logTerm = msg.Snapshot.Metadata.Term
			firstIndex = msg.Snapshot.Metadata.Index
			lastIndex = msg.Snapshot.Metadata.Index
		} else if len(msg.Entries) > 0 {
			firstIndex = msg.Entries[0].Index
			lastIndex = msg.Entries[len(msg.Entries)-1].Index
		}
		entriesLen := len(msg.Entries)
		if msg.Type == pb.MsgReadIndex || msg.Type == pb.MsgReadIndexResp {
			entriesLen = 0
			firstIndex = 0
			lastIndex = 0
		}
		step.Messages = append(step.Messages, MessageSummary{
			Type:            mapMessageType(msg.Type),
			From:            msg.From,
			To:              msg.To,
			Term:            msg.Term,
			LogIndex:        logIndex,
			LogTerm:         logTerm,
			CommitIndex:     msg.Commit,
			Reject:          msg.Reject,
			RejectHint:      msg.RejectHint,
			EntriesLen:      entriesLen,
			FirstEntryIndex: firstIndex,
			LastEntryIndex:  lastIndex,
		})
	}

	step.Committed = make([]CommittedSummary, 0, len(c.peerOrder))
	for _, id := range c.peerOrder {
		entries := c.committed[id]
		if len(entries) == 0 {
			continue
		}
		step.Committed = append(step.Committed, CommittedSummary{
			NodeID:     id,
			Count:      len(entries),
			FirstIndex: entries[0].Index,
			LastIndex:  entries[len(entries)-1].Index,
		})
	}

	step.ReadStates = make([]ReadStateSummary, 0)
	for _, id := range c.peerOrder {
		for _, readState := range c.readStates[id] {
			step.ReadStates = append(step.ReadStates, ReadStateSummary{
				NodeID:     id,
				Index:      readState.Index,
				RequestCtx: string(readState.RequestCtx),
			})
		}
	}

	step.ConfStates = make([]ConfStateSummary, 0, len(c.peerOrder))
	for _, id := range c.peerOrder {
		confState := c.confStates[id]
		step.ConfStates = append(step.ConfStates, ConfStateSummary{
			NodeID:         id,
			Voters:         slices.Clone(confState.Voters),
			VotersOutgoing: slices.Clone(confState.VotersOutgoing),
			Learners:       slices.Clone(confState.Learners),
			LearnersNext:   slices.Clone(confState.LearnersNext),
			AutoLeave:      confState.AutoLeave,
		})
	}

	return step
}

func optionalUint64(value uint64) *uint64 {
	if value == 0 {
		return nil
	}
	out := value
	return &out
}

func seedConfStates(peerOrder []uint64, initial pb.ConfState) map[uint64]pb.ConfState {
	out := make(map[uint64]pb.ConfState, len(peerOrder))
	for _, id := range peerOrder {
		out[id] = cloneConfState(initial)
	}
	return out
}

func seedActiveNodes(peerOrder []uint64, initial pb.ConfState) map[uint64]struct{} {
	out := make(map[uint64]struct{}, len(peerOrder))
	for _, id := range peerOrder {
		if !confStateContainsNode(initial, id) {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

func traceConfStateToPB(in TraceConfState) pb.ConfState {
	return pb.ConfState{
		Voters:         cloneUint64Slice(in.Voters),
		VotersOutgoing: cloneUint64Slice(in.VotersOutgoing),
		Learners:       cloneUint64Slice(in.Learners),
		LearnersNext:   cloneUint64Slice(in.LearnersNext),
		AutoLeave:      in.AutoLeave,
	}
}

func cloneReadStates(in []raft.ReadState) []raft.ReadState {
	out := make([]raft.ReadState, 0, len(in))
	for _, readState := range in {
		out = append(out, raft.ReadState{
			Index:      readState.Index,
			RequestCtx: slices.Clone(readState.RequestCtx),
		})
	}
	return out
}

func cloneConfState(in pb.ConfState) pb.ConfState {
	return pb.ConfState{
		Voters:         slices.Clone(in.Voters),
		VotersOutgoing: cloneUint64Slice(in.VotersOutgoing),
		Learners:       cloneUint64Slice(in.Learners),
		LearnersNext:   cloneUint64Slice(in.LearnersNext),
		AutoLeave:      in.AutoLeave,
	}
}

func cloneUint64Slice(in []uint64) []uint64 {
	if len(in) == 0 {
		return []uint64{}
	}
	return slices.Clone(in)
}

func mapConfChangeType(kind string) (pb.ConfChangeType, error) {
	switch kind {
	case "add_node":
		return pb.ConfChangeAddNode, nil
	case "remove_node":
		return pb.ConfChangeRemoveNode, nil
	case "add_learner_node":
		return pb.ConfChangeAddLearnerNode, nil
	default:
		return 0, fmt.Errorf("unsupported conf change type %q", kind)
	}
}

func mapReadOnlyOption(kind string) raft.ReadOnlyOption {
	switch kind {
	case "lease_based":
		return raft.ReadOnlyLeaseBased
	default:
		return raft.ReadOnlySafe
	}
}

func mapRole(state raft.StateType) string {
	switch state {
	case raft.StateFollower:
		return "follower"
	case raft.StateCandidate:
		return "candidate"
	case raft.StateLeader:
		return "leader"
	case raft.StatePreCandidate:
		return "pre_candidate"
	default:
		return state.String()
	}
}

func mapMessageType(msgType pb.MessageType) string {
	switch msgType {
	case pb.MsgProp:
		return "propose"
	case pb.MsgVote:
		return "request_vote"
	case pb.MsgVoteResp:
		return "request_vote_response"
	case pb.MsgApp:
		return "append_entries"
	case pb.MsgAppResp:
		return "append_entries_response"
	case pb.MsgHeartbeat:
		return "heartbeat"
	case pb.MsgHeartbeatResp:
		return "heartbeat_response"
	case pb.MsgPreVote:
		return "pre_vote"
	case pb.MsgPreVoteResp:
		return "pre_vote_response"
	case pb.MsgSnap:
		return "snapshot"
	case pb.MsgSnapStatus:
		return "snapshot_response"
	case pb.MsgTransferLeader:
		return "transfer_leader"
	case pb.MsgTimeoutNow:
		return "timeout_now"
	case pb.MsgReadIndex:
		return "read_index"
	case pb.MsgReadIndexResp:
		return "read_index_response"
	default:
		return msgType.String()
	}
}

func cloneMessages(messages []pb.Message) []pb.Message {
	out := make([]pb.Message, len(messages))
	for i, msg := range messages {
		out[i] = cloneMessage(msg)
	}
	return out
}

func cloneMessage(msg pb.Message) pb.Message {
	cloned := msg
	cloned.Context = slices.Clone(msg.Context)
	cloned.Entries = cloneEntries(msg.Entries)
	if msg.Snapshot != nil && !raft.IsEmptySnap(*msg.Snapshot) {
		snapshot := *msg.Snapshot
		snapshot.Data = slices.Clone(msg.Snapshot.Data)
		snapshot.Metadata.ConfState.Voters = slices.Clone(msg.Snapshot.Metadata.ConfState.Voters)
		snapshot.Metadata.ConfState.Learners = slices.Clone(msg.Snapshot.Metadata.ConfState.Learners)
		snapshot.Metadata.ConfState.VotersOutgoing = slices.Clone(msg.Snapshot.Metadata.ConfState.VotersOutgoing)
		snapshot.Metadata.ConfState.LearnersNext = slices.Clone(msg.Snapshot.Metadata.ConfState.LearnersNext)
		cloned.Snapshot = &snapshot
	}
	return cloned
}

func cloneEntries(entries []pb.Entry) []pb.Entry {
	out := make([]pb.Entry, len(entries))
	for i, entry := range entries {
		out[i] = entry
		out[i].Data = slices.Clone(entry.Data)
	}
	return out
}
