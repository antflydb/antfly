// Copyright 2025 Antfly, Inc.
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

package sim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/types"
	afraft "github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// ErrSnapshotNotFound indicates that no reachable peer had the requested snapshot.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// MessageEnvelope is a single simulated raft network delivery.
type MessageEnvelope struct {
	ShardID   types.ID
	From      types.ID
	To        types.ID
	DeliverAt time.Time
	Message   raftpb.Message
}

// MessageMatch selects simulated messages for one-shot drops/duplication.
type MessageMatch func(MessageEnvelope) bool

type linkKey struct {
	from types.ID
	to   types.ID
}

type snapshotKey struct {
	nodeID  types.ID
	shardID types.ID
}

type SnapshotTransferEvent struct {
	At         time.Time
	Kind       string
	ShardID    types.ID
	From       types.ID
	To         types.ID
	SnapshotID string
}

type queuedMessage struct {
	seq uint64
	env MessageEnvelope
}

type dropRule struct {
	match     MessageMatch
	remaining int
}

type duplicateRule struct {
	match     MessageMatch
	remaining int
	extra     int
}

// Network is a deterministic shared network for simulated raft transports.
type Network struct {
	mu sync.Mutex

	now            time.Time
	defaultLatency time.Duration
	nextSeq        uint64

	transports     map[types.ID]*Transport
	pausedNodes    map[types.ID]bool
	cutLinks       map[linkKey]bool
	linkLatencies  map[linkKey]time.Duration
	pending        []*queuedMessage
	dropRules      []dropRule
	duplicateRules []duplicateRule
	snapshots      map[snapshotKey]snapstore.SnapStore
	snapshotOpen   func(nodeID, shardID types.ID) (snapstore.SnapStore, error)
	snapshotTrace  func(SnapshotTransferEvent)
}

// Transport is a per-node raft.Transport backed by a shared simulated network.
type Transport struct {
	nodeID  types.ID
	network *Network
	errorC  chan error

	mu         sync.RWMutex
	active     map[types.ID]afraft.Raft
	shardPeers map[types.ID]map[types.ID]struct{}
}

var _ afraft.Transport = (*Transport)(nil)

// NewNetwork constructs a deterministic network starting at the provided time.
func NewNetwork(now time.Time) *Network {
	return &Network{
		now:            now,
		defaultLatency: time.Millisecond,
		transports:     make(map[types.ID]*Transport),
		pausedNodes:    make(map[types.ID]bool),
		cutLinks:       make(map[linkKey]bool),
		linkLatencies:  make(map[linkKey]time.Duration),
		snapshots:      make(map[snapshotKey]snapstore.SnapStore),
	}
}

// Now returns the network's current simulated time.
func (n *Network) Now() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

// NewTransport registers a node transport on the network.
func (n *Network) NewTransport(nodeID types.ID) *Transport {
	n.mu.Lock()
	defer n.mu.Unlock()

	if existing := n.transports[nodeID]; existing != nil {
		return existing
	}
	t := &Transport{
		nodeID:     nodeID,
		network:    n,
		errorC:     make(chan error, 16),
		active:     make(map[types.ID]afraft.Raft),
		shardPeers: make(map[types.ID]map[types.ID]struct{}),
	}
	n.transports[nodeID] = t
	return t
}

// SetDefaultLatency sets the base delivery delay for newly-sent messages.
func (n *Network) SetDefaultLatency(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.defaultLatency = d
}

// SetLinkLatency overrides the delay for traffic in one direction.
func (n *Network) SetLinkLatency(from, to types.ID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.linkLatencies[linkKey{from: from, to: to}] = d
}

// ResetLinkLatency removes a directional latency override.
func (n *Network) ResetLinkLatency(from, to types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.linkLatencies, linkKey{from: from, to: to})
}

// CutLink drops traffic in one direction.
func (n *Network) CutLink(from, to types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cutLinks[linkKey{from: from, to: to}] = true
}

// HealLink restores traffic in one direction.
func (n *Network) HealLink(from, to types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.cutLinks, linkKey{from: from, to: to})
}

// Partition cuts traffic in both directions.
func (n *Network) Partition(a, b types.ID) {
	n.CutLink(a, b)
	n.CutLink(b, a)
}

// Heal restores traffic in both directions.
func (n *Network) Heal(a, b types.ID) {
	n.HealLink(a, b)
	n.HealLink(b, a)
}

// PauseNode suspends message delivery to a node until resumed.
func (n *Network) PauseNode(nodeID types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pausedNodes[nodeID] = true
}

// ResumeNode allows queued messages for a node to be delivered again.
func (n *Network) ResumeNode(nodeID types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.pausedNodes, nodeID)
}

// DropNext drops the next matching sent message.
func (n *Network) DropNext(match MessageMatch) {
	n.DropNextN(match, 1)
}

// DropNextN drops the next n matching sent messages.
func (n *Network) DropNextN(match MessageMatch, count int) {
	if count <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRules = append(n.dropRules, dropRule{match: match, remaining: count})
}

// DuplicateNext duplicates the next matching sent message with the given extra copy count.
func (n *Network) DuplicateNext(match MessageMatch, extraCopies int) {
	if extraCopies <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.duplicateRules = append(n.duplicateRules, duplicateRule{
		match:     match,
		remaining: 1,
		extra:     extraCopies,
	})
}

// RegisterSnapshotStore makes a node's shard snapshot store available for simulated fetches.
func (n *Network) RegisterSnapshotStore(nodeID, shardID types.ID, store snapstore.SnapStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshots[snapshotKey{nodeID: nodeID, shardID: shardID}] = store
}

// SetSnapshotResolver installs a lazy snapshot-store opener used when no explicit
// store registration exists for a given node/shard pair.
func (n *Network) SetSnapshotResolver(open func(nodeID, shardID types.ID) (snapstore.SnapStore, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotOpen = open
}

// SetSnapshotObserver installs a callback for snapshot transfer activity.
func (n *Network) SetSnapshotObserver(trace func(SnapshotTransferEvent)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotTrace = trace
}

// UnregisterSnapshotStore removes a previously-registered snapshot store.
func (n *Network) UnregisterSnapshotStore(nodeID, shardID types.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.snapshots, snapshotKey{nodeID: nodeID, shardID: shardID})
}

// Pending returns the number of queued messages.
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.pending)
}

// Advance moves simulated time forward and delivers all messages now due.
func (n *Network) Advance(d time.Duration) int {
	n.mu.Lock()
	n.now = n.now.Add(d)
	n.mu.Unlock()
	return n.DeliverReady()
}

// DeliverReady delivers all messages due at the current simulated time.
func (n *Network) DeliverReady() int {
	delivered := 0
	for {
		if !n.DeliverNext() {
			return delivered
		}
		delivered++
	}
}

// DeliverNext delivers the next due, deliverable message if one exists.
func (n *Network) DeliverNext() bool {
	var (
		targetTransport *Transport
		targetRaft      afraft.Raft
		env             MessageEnvelope
	)

	n.mu.Lock()
	now := n.now
	for idx := 0; idx < len(n.pending); {
		pending := n.pending[idx]
		if pending.env.DeliverAt.After(now) {
			break
		}
		if n.pausedNodes[pending.env.To] {
			idx++
			continue
		}
		if n.cutLinks[linkKey{from: pending.env.From, to: pending.env.To}] {
			n.pending = append(n.pending[:idx], n.pending[idx+1:]...)
			continue
		}
		targetTransport = n.transports[pending.env.To]
		if targetTransport == nil {
			n.pending = append(n.pending[:idx], n.pending[idx+1:]...)
			continue
		}
		targetRaft = targetTransport.lookupShard(pending.env.ShardID)
		if targetRaft == nil {
			n.pending = append(n.pending[:idx], n.pending[idx+1:]...)
			continue
		}
		env = pending.env
		n.pending = append(n.pending[:idx], n.pending[idx+1:]...)
		n.mu.Unlock()

		if err := targetRaft.Process(context.Background(), env.Message); err != nil {
			targetTransport.pushError(fmt.Errorf(
				"processing shard %s message %s from %s to %s: %w",
				env.ShardID,
				env.Message.Type,
				env.From,
				env.To,
				err,
			))
		}
		return true
	}
	n.mu.Unlock()
	return false
}

func (n *Network) enqueue(from, shardID types.ID, msg raftpb.Message) {
	to := types.ID(msg.To)
	if to == 0 || to == from {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.cutLinks[linkKey{from: from, to: to}] {
		return
	}

	env := MessageEnvelope{
		ShardID:   shardID,
		From:      from,
		To:        to,
		DeliverAt: n.now.Add(n.latencyFor(from, to)),
		Message:   msg,
	}
	if n.shouldDropLocked(env) {
		return
	}

	n.pending = append(n.pending, &queuedMessage{
		seq: n.nextSeq,
		env: env,
	})
	n.nextSeq++

	for extra := 0; extra < n.duplicateCountLocked(env); extra++ {
		n.pending = append(n.pending, &queuedMessage{
			seq: n.nextSeq,
			env: env,
		})
		n.nextSeq++
	}

	slices.SortFunc(n.pending, func(a, b *queuedMessage) int {
		if a.env.DeliverAt.Before(b.env.DeliverAt) {
			return -1
		}
		if a.env.DeliverAt.After(b.env.DeliverAt) {
			return 1
		}
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		default:
			return 0
		}
	})
}

func (n *Network) shouldDropLocked(env MessageEnvelope) bool {
	if len(n.dropRules) == 0 {
		return false
	}
	for i := range n.dropRules {
		rule := &n.dropRules[i]
		if rule.remaining <= 0 {
			continue
		}
		if rule.match == nil || rule.match(env) {
			rule.remaining--
			return true
		}
	}
	return false
}

func (n *Network) duplicateCountLocked(env MessageEnvelope) int {
	if len(n.duplicateRules) == 0 {
		return 0
	}
	for i := range n.duplicateRules {
		rule := &n.duplicateRules[i]
		if rule.remaining <= 0 {
			continue
		}
		if rule.match == nil || rule.match(env) {
			rule.remaining--
			return rule.extra
		}
	}
	return 0
}

func (n *Network) latencyFor(from, to types.ID) time.Duration {
	if d, ok := n.linkLatencies[linkKey{from: from, to: to}]; ok {
		return d
	}
	return n.defaultLatency
}

func (n *Network) snapshotTransferBlockedLocked(from, to types.ID) bool {
	return n.pausedNodes[from] ||
		n.pausedNodes[to] ||
		n.cutLinks[linkKey{from: from, to: to}] ||
		n.cutLinks[linkKey{from: to, to: from}]
}

func (n *Network) snapshotTraceFn() func(SnapshotTransferEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotTrace
}

func (t *Transport) ErrorC() <-chan error {
	return t.errorC
}

func (t *Transport) Send(shardID types.ID, msgs []raftpb.Message) {
	for _, msg := range msgs {
		t.network.enqueue(t.nodeID, shardID, msg)
	}
}

func (t *Transport) GetSnapshot(
	ctx context.Context,
	shardID types.ID,
	dest snapstore.SnapStore,
	id string,
) error {
	if exists, err := dest.Exists(ctx, id); err == nil && exists {
		return nil
	}

	peerIDs := t.peerIDsForShard(shardID)
	if len(peerIDs) == 0 {
		peerIDs = t.allOtherPeerIDs()
	}

	for _, peerID := range peerIDs {
		if peerID == t.nodeID {
			continue
		}

		t.network.mu.Lock()
		if t.network.snapshotTransferBlockedLocked(peerID, t.nodeID) {
			t.network.mu.Unlock()
			continue
		}
		source := t.network.snapshots[snapshotKey{nodeID: peerID, shardID: shardID}]
		openSnapshot := t.network.snapshotOpen
		now := t.network.now
		t.network.mu.Unlock()
		if source == nil && openSnapshot != nil {
			var err error
			source, err = openSnapshot(peerID, shardID)
			if err != nil {
				return fmt.Errorf("opening snapshot store for node %s shard %s: %w", peerID, shardID, err)
			}
		}
		if source == nil {
			continue
		}

		exists, err := source.Exists(ctx, id)
		if err != nil || !exists {
			continue
		}

		env := MessageEnvelope{
			ShardID:   shardID,
			From:      peerID,
			To:        t.nodeID,
			DeliverAt: now,
			Message: raftpb.Message{
				Type: raftpb.MsgSnap,
				From: uint64(peerID),
				To:   uint64(t.nodeID),
			},
		}

		traceSnapshot := t.network.snapshotTraceFn()
		if traceSnapshot != nil {
			traceSnapshot(SnapshotTransferEvent{
				At:         now,
				Kind:       "attempt",
				ShardID:    shardID,
				From:       peerID,
				To:         t.nodeID,
				SnapshotID: id,
			})
		}

		t.network.mu.Lock()
		if t.network.snapshotTransferBlockedLocked(peerID, t.nodeID) {
			t.network.mu.Unlock()
			if traceSnapshot != nil {
				traceSnapshot(SnapshotTransferEvent{
					At:         now,
					Kind:       "blocked",
					ShardID:    shardID,
					From:       peerID,
					To:         t.nodeID,
					SnapshotID: id,
				})
			}
			continue
		}
		if t.network.shouldDropLocked(env) {
			t.network.mu.Unlock()
			if traceSnapshot != nil {
				traceSnapshot(SnapshotTransferEvent{
					At:         now,
					Kind:       "dropped",
					ShardID:    shardID,
					From:       peerID,
					To:         t.nodeID,
					SnapshotID: id,
				})
			}
			continue
		}
		t.network.mu.Unlock()

		reader, err := source.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("reading snapshot %s from node %s shard %s: %w", id, peerID, shardID, err)
		}
		err = dest.Put(ctx, id, reader)
		closeErr := reader.Close()
		if err != nil {
			return fmt.Errorf("copying snapshot %s from node %s shard %s: %w", id, peerID, shardID, err)
		}
		if closeErr != nil {
			return fmt.Errorf("closing snapshot %s from node %s shard %s: %w", id, peerID, shardID, closeErr)
		}
		if traceSnapshot != nil {
			traceSnapshot(SnapshotTransferEvent{
				At:         now,
				Kind:       "copied",
				ShardID:    shardID,
				From:       peerID,
				To:         t.nodeID,
				SnapshotID: id,
			})
		}
		return nil
	}

	return fmt.Errorf("snapshot %s for shard %s: %w", id, shardID, ErrSnapshotNotFound)
}

func (t *Transport) AddPeer(shardID, nodeID types.ID, _ []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.shardPeers[shardID] == nil {
		t.shardPeers[shardID] = make(map[types.ID]struct{})
	}
	t.shardPeers[shardID][nodeID] = struct{}{}
}

func (t *Transport) RemovePeer(shardID, nodeID types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	peers := t.shardPeers[shardID]
	if peers == nil {
		return
	}
	delete(peers, nodeID)
}

func (t *Transport) ServeRaft(shardID types.ID, shard afraft.Raft, peers []raft.Peer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.active[shardID] = shard
	peerSet := make(map[types.ID]struct{}, len(peers))
	for _, peer := range peers {
		id := types.ID(peer.ID)
		if id == t.nodeID {
			continue
		}
		peerSet[id] = struct{}{}
	}
	t.shardPeers[shardID] = peerSet
	return nil
}

func (t *Transport) StopServeRaft(shardID types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.active, shardID)
	delete(t.shardPeers, shardID)
}

func (t *Transport) lookupShard(shardID types.ID) afraft.Raft {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.active[shardID]
}

func (t *Transport) peerIDsForShard(shardID types.ID) []types.ID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	peers := t.shardPeers[shardID]
	if len(peers) == 0 {
		return nil
	}
	ids := make([]types.ID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (t *Transport) allOtherPeerIDs() []types.ID {
	t.network.mu.Lock()
	defer t.network.mu.Unlock()

	ids := make([]types.ID, 0, len(t.network.transports))
	for id := range t.network.transports {
		if id == t.nodeID {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (t *Transport) pushError(err error) {
	select {
	case t.errorC <- err:
	default:
	}
}

// CopySnapshot copies a snapshot between two stores.
func CopySnapshot(ctx context.Context, src snapstore.SnapStore, dst snapstore.SnapStore, id string) error {
	reader, err := src.Get(ctx, id)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	if err := dst.Put(ctx, id, reader); err != nil {
		return err
	}
	return nil
}

// ReadSnapshot loads an entire snapshot payload from a store.
func ReadSnapshot(ctx context.Context, store snapstore.SnapStore, id string) ([]byte, error) {
	reader, err := store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}
