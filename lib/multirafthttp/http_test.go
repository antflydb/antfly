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
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/pbutil"
	"github.com/antflydb/antfly/lib/types"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
)

func TestServeRaftPrefix(t *testing.T) {
	defer goleak.VerifyNone(t,
		// Ants starts a goroutine even on import
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).purgeStaleWorkers"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).ticktock"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).goTicktock"),
		// Bleve starts analysis workers on import
		goleak.IgnoreTopFunction("github.com/blevesearch/bleve_index_api.AnalysisWorker"),
	)
	testCases := []struct {
		method string
		body   io.Reader
		p      MultiRaft

		wcode int
	}{
		{
			// bad method
			"GET",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{},
			http.StatusMethodNotAllowed,
		},
		{
			// bad method
			"PUT",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{},
			http.StatusMethodNotAllowed,
		},
		{
			// bad method
			"DELETE",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{},
			http.StatusMethodNotAllowed,
		},
		{
			// bad request body
			"POST",
			&errReader{},
			&fakeMultiRaft{},
			http.StatusBadRequest,
		},
		{
			// bad request protobuf
			"POST",
			strings.NewReader("malformed garbage"),
			&fakeMultiRaft{},
			http.StatusBadRequest,
		},
		{
			// good request, wrong cluster ID
			"POST",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{
				shards: map[types.ID]baseRaft{2: &fakeRaft{}},
			},
			http.StatusNotFound,
		},
		{
			// good request, Processor failure
			"POST",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{
				shards: map[types.ID]baseRaft{
					1: &fakeRaft{err: &resWriterToError{code: http.StatusForbidden}},
				},
			},
			http.StatusForbidden,
		},
		{
			// good request, Processor failure
			"POST",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{
				shards: map[types.ID]baseRaft{
					1: &fakeRaft{err: &resWriterToError{code: http.StatusInternalServerError}},
				},
			},
			http.StatusInternalServerError,
		},
		{
			// good request, Processor failure
			"POST",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{
				shards: map[types.ID]baseRaft{
					1: &fakeRaft{err: errors.New("blah")},
				},
			},
			http.StatusInternalServerError,
		},
		{
			// good request
			"POST",
			bytes.NewReader(
				pbutil.MustMarshal(&raftpb.Message{}),
			),
			&fakeMultiRaft{},
			http.StatusNoContent,
		},
	}
	for i, tt := range testCases {
		req, err := http.NewRequest(tt.method, "foo", tt.body)
		if err != nil {
			t.Fatalf("#%d: could not create request: %#v", i, err)
		}
		req.Header.Set("X-Raft-Shard-ID", types.ID(1).String())
		req.Header.Set("X-Server-Version", Version)
		rw := httptest.NewRecorder()
		tr := &Transport{Logger: zaptest.NewLogger(t)}
		tr.Start(types.ID(1))
		defer tr.Close()
		h := newPipelineHandler(tr, tt.p)

		// goroutine because the handler panics to disconnect on raft error
		donec := make(chan struct{})
		go func() {
			defer func() {
				recover()
				close(donec)
			}()
			h.ServeHTTP(rw, req)
		}()
		<-donec

		if rw.Code != tt.wcode {
			t.Errorf("#%d: got code=%d, want %d", i, rw.Code, tt.wcode)
		}
	}
}

func TestServeRaftStreamPrefix(t *testing.T) {
	tests := []struct {
		path  string
		wtype streamType
	}{
		{
			RaftStreamPrefix + "/message/1",
			streamTypeMessage,
		},
		{
			RaftStreamPrefix + "/msgapp/1",
			streamTypeMsgAppV2,
		},
	}
	for i, tt := range tests {
		req, err := http.NewRequest(http.MethodGet, "http://localhost:2380"+tt.path, nil)
		if err != nil {
			t.Fatalf("#%d: could not create request: %#v", i, err)
		}
		req.Header.Set("X-Raft-Shard-ID", "1")
		req.Header.Set("X-Server-Version", Version)
		req.Header.Set("X-Raft-To", "2")

		peer := newFakePeer()
		peerGetter := &fakePeerGetter{peers: map[types.ID]Peer{types.ID(1): peer}}
		tr := &Transport{}
		h := newStreamHandler(tr, peerGetter, &fakeMultiRaft{}, types.ID(2))

		rw := httptest.NewRecorder()
		go h.ServeHTTP(rw, req)

		var conn *outgoingConn
		select {
		case conn = <-peer.connc:
		case <-time.After(time.Second):
			t.Fatalf("#%d: failed to attach outgoingConn", i)
		}
		if g := rw.Header().Get("X-Server-Version"); g != Version {
			t.Errorf("#%d: X-Server-Version = %s, want %s", i, g, Version)
		}
		if conn.t != tt.wtype {
			t.Errorf("#%d: type = %s, want %s", i, conn.t, tt.wtype)
		}
		conn.Close()
	}
}

func TestServeRaftStreamPrefixBad(t *testing.T) {
	removedID := uint64(5)
	tests := []struct {
		method string
		path   string
		remote string

		wcode int
	}{
		// bad method
		{
			"PUT",
			RaftStreamPrefix + "/message/1",
			"1",
			http.StatusMethodNotAllowed,
		},
		// bad method
		{
			"POST",
			RaftStreamPrefix + "/message/1",
			"1",
			http.StatusMethodNotAllowed,
		},
		// bad method
		{
			"DELETE",
			RaftStreamPrefix + "/message/1",
			"1",
			http.StatusMethodNotAllowed,
		},
		// bad path
		{
			"GET",
			RaftStreamPrefix + "/strange/1",
			"1",
			http.StatusNotFound,
		},
		// bad path
		{
			"GET",
			RaftStreamPrefix + "/strange",
			"1",
			http.StatusNotFound,
		},
		// non-existent peer
		{
			"GET",
			RaftStreamPrefix + "/message/2",
			"1",
			http.StatusNotFound,
		},
		// removed peer
		// {
		// 	"GET",
		// 	RaftStreamPrefix + "/message/" + fmt.Sprint(removedID),
		// 	"1",
		// 	http.StatusGone,
		// },
		// wrong remote ID
		{
			"GET",
			RaftStreamPrefix + "/message/1",
			"2",
			http.StatusPreconditionFailed,
		},
	}
	for i, tt := range tests {
		req, err := http.NewRequest(tt.method, "http://localhost:2380"+tt.path, nil)
		if err != nil {
			t.Fatalf("#%d: could not create request: %#v", i, err)
		}
		shardID := types.ID(0)
		req.Header.Set("X-Raft-Shard-ID", shardID.String())
		req.Header.Set("X-Server-Version", Version)
		req.Header.Set("X-Raft-To", tt.remote)
		rw := httptest.NewRecorder()
		tr := &Transport{}
		peerGetter := &fakePeerGetter{peers: map[types.ID]Peer{types.ID(1): newFakePeer()}}
		r := &fakeMultiRaft{
			shards: map[types.ID]baseRaft{
				shardID: &fakeRaft{removedID: removedID},
			},
		}
		h := newStreamHandler(tr, peerGetter, r, types.ID(1))
		h.ServeHTTP(rw, req)

		if rw.Code != tt.wcode {
			t.Errorf("#%d: code = %d, want %d", i, rw.Code, tt.wcode)
		}
	}
}

func TestCloseNotifier(t *testing.T) {
	c := newCloseNotifier()
	select {
	case <-c.closeNotify():
		t.Fatalf("received unexpected close notification")
	default:
	}
	c.Close()
	select {
	case <-c.closeNotify():
	default:
		t.Fatalf("failed to get close notification")
	}
}

// errReader implements io.Reader to facilitate a broken request.
type errReader struct{}

func (er *errReader) Read(_ []byte) (int, error) { return 0, errors.New("some error") }

type resWriterToError struct {
	code int
}

func (e *resWriterToError) Error() string                 { return "" }
func (e *resWriterToError) WriteTo(w http.ResponseWriter) { w.WriteHeader(e.code) }

type fakePeerGetter struct {
	peers map[types.ID]Peer
}

func (pg *fakePeerGetter) Get(id types.ID) Peer { return pg.peers[id] }

type fakePeer struct {
	msgs     []multiMessage
	peerURLs types.URLs
	connc    chan *outgoingConn
	paused   bool
}

func newFakePeer() *fakePeer {
	fakeURL, _ := url.Parse("http://localhost")
	return &fakePeer{
		connc:    make(chan *outgoingConn, 1),
		peerURLs: types.URLs{*fakeURL},
	}
}

func (pr *fakePeer) send(m multiMessage) {
	if pr.paused {
		return
	}
	pr.msgs = append(pr.msgs, m)
}

func (pr *fakePeer) sendSnapshotRequest(shardID types.ID, snapStore SnapStore, id string) error {
	return nil
}
func (pr *fakePeer) update(urls types.URLs)                { pr.peerURLs = urls }
func (pr *fakePeer) attachOutgoingConn(conn *outgoingConn) { pr.connc <- conn }
func (pr *fakePeer) activeSince() time.Time                { return time.Time{} }
func (pr *fakePeer) stop()                                 {}
func (pr *fakePeer) Pause()                                { pr.paused = true }
func (pr *fakePeer) Resume()                               { pr.paused = false }
