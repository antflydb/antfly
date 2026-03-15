// Copyright 2015 The etcd Authors
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

package multirafthttp

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"

	"github.com/panjf2000/ants/v2"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	DefaultConnReadTimeout  = 5 * time.Second
	DefaultConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
)

var (
	ConnReadTimeout  = DefaultConnReadTimeout
	ConnWriteTimeout = DefaultConnWriteTimeout

	// shardNotFoundLogRateLimiter rate-limits "shard not found" warnings which can
	// occur frequently during normal operation (shard rebalancing, node restarts).
	shardNotFoundLogRateLimiter = rate.NewLimiter(rate.Every(10*time.Second), 1)
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	send(m multiMessage)

	// sendSnapshotRequest sends a snapshot data request to the remote peer.
	sendSnapshotRequest(shardID types.ID, snapStore SnapStore, id string) error

	// update updates the urls of remote peer.
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	lg *zap.Logger

	localID types.ID
	// id of the remote raft peer node
	id types.ID

	r MultiRaft

	status *peerStatus

	picker *urlPicker

	msgAppV2Writer *streamWriter
	writer         *streamWriter
	pipeline       *pipeline
	msgAppV2Reader *streamReader
	msgAppReader   *streamReader

	recvc chan multiMessage
	propc chan multiMessage

	recvcq *inflight.DedupeQueue
	propcq *inflight.DedupeQueue

	mu     sync.Mutex
	paused bool

	workerPool *ants.Pool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

type multiMessage struct {
	msg     raftpb.Message
	shardID types.ID
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.Stringer("remote-peer-id", peerID))
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.Stringer("remote-peer-id", peerID))
		}
	}()

	status := newPeerStatus(t.Logger, t.ID, peerID)
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	r := t.MultiRaft
	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		multiRaft:     r,
		errorc:        errorc,
	}
	pipeline.start()

	p := &peer{
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		pipeline:       pipeline,
		recvc:          make(chan multiMessage, recvBufSize),
		recvcq:         inflight.NewDedupeQueue(recvBufSize, recvBufSize),
		propcq:         inflight.NewDedupeQueue(recvBufSize, recvBufSize),
		propc:          make(chan multiMessage, maxPendingProposals),
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: long-lived peer context, cancel stored in p.cancel
	p.cancel = cancel
	var err error
	p.workerPool, err = ants.NewPool(100)
	if err != nil {
		t.Logger.Fatal("failed to create ants pool", zap.Error(err))
	}
	go func() {
		for p.recvcq.Dequeue(func(opSet *inflight.OpSet) {
			_ = p.workerPool.Submit(func() {
				// Coalesce stacked heartbeat messages
				var seenHeartbeat *raftpb.Message
				var seenHeartbeatResp *raftpb.Message
				for _, op := range opSet.Ops() {
					mm := op.Data.(multiMessage)
					if mm.msg.Type == raftpb.MsgHeartbeat {
						if seenHeartbeat != nil && seenHeartbeat.Term == mm.msg.Term {
							// skip this heartbeat
							// TODO (ajr) metrics for skipped heartbeats?
							continue
						}
						seenHeartbeat = &mm.msg
					}
					if mm.msg.Type == raftpb.MsgHeartbeatResp {
						if seenHeartbeatResp != nil && seenHeartbeatResp.Term == mm.msg.Term {
							// skip this heartbeat response
							// TODO (ajr) metrics for skipped heartbeat responses?
							continue
						}
						seenHeartbeatResp = &mm.msg
					}

					if err := r.Process(ctx, uint64(mm.shardID), mm.msg); err != nil {
						logProcessError(t.Logger, err, mm, p.id)
					}
				}
			})
		}) {
		}
	}()
	go func() {
		for {
			select {
			case mm := <-p.recvc:
				if err := p.recvcq.Enqueue(&inflight.Op{
					ID:   inflight.ID(mm.shardID),
					Data: mm,
				}); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to enqueue Raft message",
							zap.Error(err),
							zap.String("message type", mm.msg.Type.String()),
							zap.Stringer("remote-peer-id", p.id),
							zap.Stringer("shardID", mm.shardID))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	go func() {
		for p.propcq.Dequeue(func(opSet *inflight.OpSet) {
			_ = p.workerPool.Submit(func() {
				// TODO (ajr) Coalesce stacked proposal messages?
				for _, op := range opSet.Ops() {
					mm := op.Data.(multiMessage)
					if err := r.Process(ctx, uint64(mm.shardID), mm.msg); err != nil {
						logProcessError(t.Logger, err, mm, p.id)
					}
				}
			})
		}) {
		}
	}()
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := p.propcq.Enqueue(&inflight.Op{
					ID:   inflight.ID(mm.shardID),
					Data: mm,
				}); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to enqueue proposal message",
							zap.Error(err),
							zap.String("message type", mm.msg.Type.String()),
							zap.Stringer("remote-peer-id", p.id),
							zap.Stringer("shardID", mm.shardID))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}
func (p *peer) sendSnapshotRequest(shardID types.ID, snapStore SnapStore, id string) error {
	return p.pipeline.getSnap(shardID, snapStore, id)
}

func (p *peer) send(m multiMessage) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	writec, name := p.pick(m)
	select {
	case writec <- m:
	default:
		p.r.ReportUnreachable(uint64(m.shardID), m.msg.To)
		if isMsgSnap(m.msg) {
			p.r.ReportSnapshot(uint64(m.shardID), m.msg.To, raft.SnapshotFailure)
		}
		if p.lg != nil {
			p.lg.Warn(
				"dropped internal Raft message since sending buffer is full",
				zap.Stringer("message-type", m.msg.Type),
				zap.Stringer("local-member-id", p.localID),
				zap.Stringer("from", types.ID(m.msg.From)),
				zap.Stringer("remote-peer-id", p.id),
				zap.String("remote-peer-name", name),
				zap.Bool("remote-peer-active", p.status.isActive()),
			)
		}
		sentFailures.WithLabelValues(types.ID(m.msg.To).String()).Inc()
	}
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.Stringer("type", conn.t))
		}
	}
	if !ok {
		_ = conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.Stringer("remote-peer-id", p.id))
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.Stringer("remote-peer-id", p.id))
		}
	}()

	close(p.stopc)
	if p.recvcq != nil {
		p.recvcq.Close()
	}
	if p.propcq != nil {
		p.propcq.Close()
	}
	if p.workerPool != nil {
		p.workerPool.Release()
	}
	p.cancel()
	if p.lg != nil {
		p.lg.Info("stopping remote peer message writers", zap.Stringer("remote-peer-id", p.id))
	}
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	if p.lg != nil {
		p.lg.Info("stopping remote peer message readers", zap.Stringer("remote-peer-id", p.id))
	}
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m multiMessage) (writec chan<- multiMessage, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m.msg) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m.msg) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

// logProcessError logs an error from r.Process with appropriate rate limiting
// for shard-not-found errors that are expected during rebalancing.
func logProcessError(lg *zap.Logger, err error, mm multiMessage, peerID types.ID) {
	if errors.Is(err, ErrShardNotFound) {
		shardNotFoundTotal.WithLabelValues(mm.shardID.String(), mm.msg.Type.String()).Inc()
		if lg != nil && shardNotFoundLogRateLimiter.Allow() {
			lg.Warn("shard not found during message processing",
				zap.Stringer("shardID", mm.shardID),
				zap.Stringer("remote-peer-id", peerID),
				zap.String("message type", mm.msg.Type.String()))
		}
	} else if lg != nil {
		lg.Warn("failed to process Raft message",
			zap.Error(err),
			zap.String("message type", mm.msg.Type.String()),
			zap.Stringer("remote-peer-id", peerID),
			zap.Stringer("shardID", mm.shardID))
	}
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
