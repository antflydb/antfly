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
	"os"
	"path"
	"strings"

	"github.com/antflydb/antfly/lib/types"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix       = "/raft"
	SnapPrefix       = "/snapshots"
	ProbingPrefix    = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix = path.Join(RaftPrefix, "stream")

	errIncompatibleVersion = errors.New("incompatible version")
	ErrClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

type pipelineHandler struct {
	lg      *zap.Logger
	localID types.ID
	tr      Transporter
	r       MultiRaft
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(t *Transport, r MultiRaft) http.Handler {
	h := &pipelineHandler{
		lg:      t.Logger,
		localID: t.ID,
		tr:      t,
		r:       r,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	shardID, ok := getShardID(w, r)
	if !ok {
		return
	}

	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	// Limit the data size that could be read from the request body, which ensures that read from
	// connection will not time out accidentally due to possible blocking in underlying implementation.
	limitedr := io.LimitReader(r.Body, connReadLimitByte)
	b, err := io.ReadAll(limitedr)
	if err != nil {
		h.lg.Warn(
			"failed to read Raft message",
			zap.Stringer("local-member-id", h.localID),
			zap.Error(err),
		)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		h.lg.Warn(
			"failed to unmarshal Raft message",
			zap.Stringer("local-member-id", h.localID),
			zap.Error(err),
		)
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	if err := h.r.Process(context.TODO(), uint64(shardID), m); err != nil {
		var writerErr writerToResponse
		switch {
		case errors.As(err, &writerErr):
			writerErr.WriteTo(w)
		case errors.Is(err, ErrShardNotFound):
			http.Error(w, "shard not found", http.StatusNotFound)
			w.(http.Flusher).Flush()
		default:
			h.lg.Warn("failed to process Raft message", zap.Error(err), zap.Stringer("shardID", shardID))
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)
}

type snapHandler struct {
	id            types.ID
	lg            *zap.Logger
	snapStoreFunc func(shardID types.ID) (SnapStore, error)
}

func getShardID(w http.ResponseWriter, r *http.Request) (types.ID, bool) {
	shardID, err := types.IDFromString(r.Header.Get("X-Raft-Shard-ID"))
	if err != nil {
		http.Error(w, "Failed to get shard ID", http.StatusBadRequest)
		return 0, false
	}
	if shardID == 0 {
		http.Error(w, "Shard ID cannot be 0", http.StatusBadRequest)
		return 0, false
	}
	return shardID, true
}

// Add this to your raft http server handling
func (sh *snapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	shardID, ok := getShardID(w, r)
	if !ok {
		return
	}

	if err := checkClusterCompatibilityFromHeader(sh.lg, sh.id, r.Header); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	snapshotID := path.Base(r.URL.Path)

	sh.lg.Info("Snapshot requested", zap.String("shardID", shardID.String()), zap.String("snapshotID", snapshotID))

	// Get the snapshot store for this shard
	snapStore, err := sh.snapStoreFunc(shardID)
	if err != nil {
		sh.lg.Error("Failed to get snapshot store", zap.String("shardID", shardID.String()), zap.Error(err))
		http.Error(w, fmt.Sprintf("failed to get snapshot store: %v", err), http.StatusInternalServerError)
		return
	}

	// Open snapshot via SnapStore abstraction
	f, err := snapStore.Get(r.Context(), snapshotID)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 5*1024*1024) // 5MB buffer
	// Stream the snapshot file (Content-Length cannot be set because size is unknown until streamed)
	w.Header().Set("Content-Type", "application/octet-stream")
	if bytesCopied, err := io.CopyBuffer(w, f, buf); err != nil {
		// Headers already sent if any bytes were written; log the error
		sh.lg.Error("Failed to stream snapshot",
			zap.String("shardID", shardID.String()),
			zap.String("snapshotID", snapshotID),
			zap.Error(err))
	} else {
		sh.lg.Info("Snapshot sent as requested",
			zap.Int64("bytesCopied", bytesCopied),
			zap.String("shardID", shardID.String()),
			zap.String("snapshotID", snapshotID))
	}
}

type streamHandler struct {
	lg         *zap.Logger
	tr         *Transport
	peerGetter peerGetter
	r          MultiRaft
	id         types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r MultiRaft, id types.ID) http.Handler {
	h := &streamHandler{
		lg:         t.Logger,
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

// Cluster only keeps the major.minor.
func Cluster(v string) string {
	vs := strings.Split(v, ".")
	if len(vs) <= 2 {
		return v
	}
	return fmt.Sprintf("%s.%s", vs[0], vs[1])
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Server-Version", Version)
	if err := checkClusterCompatibilityFromHeader(h.lg, h.id, r.Header); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMsgAppV2.endpoint(h.lg):
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint(h.lg):
		t = streamTypeMessage
	default:
		h.lg.Debug(
			"ignored unexpected streaming request path",
			zap.Stringer("local-member-id", h.tr.ID),
			zap.Stringer("remote-peer-id-stream-handler", h.id),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		h.lg.Warn(
			"failed to parse path into ID",
			zap.Stringer("local-member-id", h.tr.ID),
			zap.Stringer("remote-peer-id-stream-handler", h.id),
			zap.String("path", fromStr),
			zap.Error(err),
		)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	p := h.peerGetter.Get(from)
	if p == nil {
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		h.lg.Warn(
			"ignored streaming request; ID mismatch",
			zap.Stringer("local-member-id", h.tr.ID),
			zap.Stringer("remote-peer-id-stream-handler", h.id),
			zap.String("remote-peer-id-header", gto),
			zap.Stringer("remote-peer-id-from", from),
		)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.ID,
		peerID:  from,
	}
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

// checkClusterCompatibilityFromHeader checks the cluster compatibility of
// the local member from the given header.
// It checks whether the version of local member is compatible with
// the versions in the header.
func checkClusterCompatibilityFromHeader(lg *zap.Logger, localID types.ID, header http.Header) error {
	remoteName := header.Get("X-Server-From")

	remoteServer := serverVersion(header)
	remoteVs := ""
	if remoteServer != nil {
		remoteVs = remoteServer.String()
	}

	remoteMinClusterVer := minClusterVersion(header)
	remoteMinClusterVs := ""
	if remoteMinClusterVer != nil {
		remoteMinClusterVs = remoteMinClusterVer.String()
	}

	localServer, localMinCluster, err := checkVersionCompatibility(remoteName, remoteServer, remoteMinClusterVer)

	localVs := ""
	if localServer != nil {
		localVs = localServer.String()
	}
	localMinClusterVs := ""
	if localMinCluster != nil {
		localMinClusterVs = localMinCluster.String()
	}

	if err != nil {
		lg.Warn(
			"failed version compatibility check",
			zap.Stringer("local-member-id", localID),
			zap.String("local-member-server-version", localVs),
			zap.String("local-member-server-minimum-cluster-version", localMinClusterVs),
			zap.String("remote-peer-server-name", remoteName),
			zap.String("remote-peer-server-version", remoteVs),
			zap.String("remote-peer-server-minimum-cluster-version", remoteMinClusterVs),
			zap.Error(err),
		)
		return errIncompatibleVersion
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
