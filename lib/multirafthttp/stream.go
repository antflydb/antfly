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
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/types"

	"github.com/Masterminds/semver"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	streamTypeMessage  streamType = "message"
	streamTypeMsgAppV2 streamType = "msgappv2"

	streamBufSize = 4096
)

type streamType string

func (t streamType) endpoint(lg *zap.Logger) string {
	switch t {
	case streamTypeMsgAppV2:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		if lg != nil {
			lg.Panic("unhandled stream type", zap.Stringer("stream-type", t))
		}
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMsgAppV2:
		return "stream MsgApp v2"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

// linkHeartbeatMessage is a special message used as heartbeat message in
// link layer. It never conflicts with messages from raft because raft
// doesn't send out messages without From and To fields.
var linkHeartbeatMessage = multiMessage{msg: raftpb.Message{Type: raftpb.MsgHeartbeat}}

func isLinkHeartbeatMessage(m *multiMessage) bool {
	return m.shardID == 0 && m.msg.Type == raftpb.MsgHeartbeat && m.msg.From == 0 && m.msg.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	lg *zap.Logger

	localID types.ID
	peerID  types.ID

	status *peerStatus
	fs     *stats.FollowerStats
	r      MultiRaft

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgc  chan multiMessage
	connc chan *outgoingConn
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, fs *stats.FollowerStats, r MultiRaft) *streamWriter {
	w := &streamWriter{
		lg: lg,

		localID: local,
		peerID:  id,

		status: status,
		fs:     fs,
		r:      r,
		msgc:   make(chan multiMessage, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgc       chan multiMessage
		heartbeatc <-chan time.Time
		t          streamType
		enc        encoder
		flusher    http.Flusher
		batched    int
	)
	tickc := time.NewTicker(ConnReadTimeout / 3)
	defer tickc.Stop()

	timerc := time.NewTimer(ConnReadTimeout / 3)
	defer timerc.Stop()

	unflushed := 0

	if cw.lg != nil {
		cw.lg.Info(
			"started stream writer with remote peer",
			zap.Stringer("local-member-id", cw.localID),
			zap.Stringer("remote-peer-id", cw.peerID),
		)
	}

	for {
		select {
		case <-heartbeatc:
			err := enc.encode(&linkHeartbeatMessage)
			unflushed += linkHeartbeatMessage.msg.Size()
			if err == nil {
				flusher.Flush()
				batched = 0
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
				unflushed = 0
				timerc.Reset(ConnReadTimeout / 3)
				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close()
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.Stringer("stream-writer-type", t),
					zap.Stringer("local-member-id", cw.localID),
					zap.Stringer("remote-peer-id", cw.peerID),
				)
			}
			heartbeatc, msgc = nil, nil
		case m := <-msgc:
			err := enc.encode(&m)
			if err == nil {
				unflushed += m.msg.Size()

				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
					timerc.Reset(ConnReadTimeout / 3)
				} else {
					batched++
				}

				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.Stringer("stream-writer-type", t),
					zap.Stringer("local-member-id", cw.localID),
					zap.Stringer("remote-peer-id", cw.peerID),
				)
			}
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(uint64(m.shardID), m.msg.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
		case conn := <-cw.connc:
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t
			switch conn.t {
			case streamTypeMsgAppV2:
				enc = newMsgAppV2Encoder(conn.Writer, cw.fs)
			case streamTypeMessage:
				enc = &messageEncoder{w: conn.Writer}
			default:
				if cw.lg != nil {
					cw.lg.Panic("unhandled stream type", zap.Stringer("stream-type", t))
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"set message encoder",
					zap.Stringer("from", conn.localID),
					zap.Stringer("to", conn.peerID),
					zap.Stringer("stream-type", t),
				)
			}
			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()

			if closed {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.Stringer("stream-writer-type", t),
						zap.Stringer("local-member-id", cw.localID),
						zap.Stringer("remote-peer-id", cw.peerID),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.Stringer("stream-writer-type", t),
					zap.Stringer("local-member-id", cw.localID),
					zap.Stringer("remote-peer-id", cw.peerID),
				)
			}
			timerc.Reset(ConnReadTimeout / 3)
			heartbeatc, msgc = timerc.C, cw.msgc

		case <-cw.stopc:
			if cw.close() {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.Stringer("stream-writer-type", t),
						zap.Stringer("remote-peer-id", cw.peerID),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"stopped TCP streaming connection with remote peer",
					zap.Stringer("stream-writer-type", t),
					zap.Stringer("remote-peer-id", cw.peerID),
				)
			}
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- multiMessage, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		if cw.lg != nil {
			cw.lg.Warn(
				"failed to close connection with remote peer",
				zap.Stringer("remote-peer-id", cw.peerID),
				zap.Error(err),
			)
		}
	}
	cw.msgc = make(chan multiMessage, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	lg *zap.Logger

	peerID types.ID
	typ    streamType

	tr     *Transport
	picker *urlPicker
	status *peerStatus
	recvc  chan<- multiMessage
	propc  chan<- multiMessage

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ

	if cr.lg != nil {
		cr.lg.Info(
			"started stream reader with remote peer",
			zap.Stringer("stream-reader-type", t),
			zap.Stringer("local-member-id", cr.tr.ID),
			zap.Stringer("remote-peer-id", cr.peerID),
		)
	}

	for {
		rc, err := cr.dial(t)
		if err != nil {
			if !errors.Is(err, errUnsupportedStreamType) {
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {
			cr.status.activate()
			if cr.lg != nil {
				cr.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.Stringer("stream-reader-type", cr.typ),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
				)
			}
			err = cr.decodeLoop(rc, t)
			if cr.lg != nil {
				cr.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.Stringer("stream-reader-type", cr.typ),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
					zap.Error(err),
				)
			}
			switch {
			// all data is read out
			case errors.Is(err, io.EOF):
			// connection is closed by the remote
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			if cr.lg != nil {
				cr.lg.Info(
					"stopped stream reader with remote peer",
					zap.Stringer("stream-reader-type", t),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
				)
			}
			close(cr.done)
			return
		}
		if err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"rate limit on stream reader with remote peer",
					zap.Stringer("stream-reader-type", t),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
					zap.Error(err),
				)
			}
		}
	}
}

func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	switch t {
	case streamTypeMsgAppV2:
		dec = newMsgAppV2Decoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}
	default:
		if cr.lg != nil {
			cr.lg.Panic("unknown stream type", zap.Stringer("type", t))
		}
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		// gofail-go: var raftDropHeartbeat struct{}
		// continue labelRaftDropHeartbeat
		receivedBytes.WithLabelValues(types.ID(m.msg.From).String()).Add(float64(m.msg.Size()))

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {
			continue
		}

		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		recvc := cr.recvc
		if m.msg.Type == raftpb.MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m:
		default:
			if cr.lg != nil {
				cr.lg.Warn(
					"dropped Raft message since receiving buffer is full (overloaded network)",
					zap.Stringer("message-type", m.msg.Type),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("from", types.ID(m.msg.From)),
					zap.Stringer("remote-peer-id", types.ID(m.msg.To)),
					zap.Bool("remote-peer-active", cr.status.isActive()),
				)
			}
			recvFailures.WithLabelValues(types.ID(m.msg.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

var dialDebugLogRateLimiter = rate.NewLimiter(rate.Every(2*time.Second), 1)

// GracefulClose drains http.Response.Body until it hits EOF
// and closes it. This prevents TCP/TLS connections from closing,
// therefore available for reuse.
// Borrowed from golang/net/context/ctxhttp/cancelreq.go.
func GracefulClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick()
	uu := u
	uu.Path = path.Join(t.endpoint(cr.lg), cr.tr.ID.String())

	if cr.lg != nil {
		if dialDebugLogRateLimiter.Allow() {
			cr.lg.Debug(
				"dial stream reader",
				zap.Stringer("from", cr.tr.ID),
				zap.Stringer("to", cr.peerID),
				zap.String("address", uu.String()),
			)
		}
	}
	req, err := http.NewRequest(http.MethodGet, uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%w)", u, err)
	}
	req.Header.Set("X-Server-From", cr.tr.ID.String())
	req.Header.Set("X-Server-Version", Version)
	req.Header.Set("X-Min-Cluster-Version", MinClusterVersion)
	req.Header.Set("X-Raft-To", cr.peerID.String())

	setPeerURLsHeader(req, cr.tr.URLs)

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.StreamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	rv := serverVersion(resp.Header)
	lv := semver.MustParse(Version)
	if rv.Compare(lv) == -1 && !checkStreamSupport(rv, t) {
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}

	switch resp.StatusCode {
	case http.StatusGone:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("node %s failed to find peer %s", cr.tr.ID, cr.peerID)
	case http.StatusPreconditionFailed:
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to server version incompatibility",
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
					zap.Error(errIncompatibleVersion),
				)
			}
			return nil, errIncompatibleVersion

		case ErrClusterIDMismatch.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to cluster ID mismatch",
					zap.Stringer("remote-peer-id", cr.peerID),
					zap.String("remote-peer-cluster-id", resp.Header.Get("X-Raft-Shard-ID")),
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Error(ErrClusterIDMismatch),
				)
			}
			return nil, ErrClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.Stringer("local-member-id", cr.tr.ID),
					zap.Stringer("remote-peer-id", cr.peerID),
					zap.Error(err),
				)
			}
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}
