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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/pbutil"
	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

type pipeline struct {
	peerID types.ID

	tr            *Transport
	picker        *urlPicker
	status        *peerStatus
	multiRaft     MultiRaft
	errorc        chan error
	followerStats *stats.FollowerStats

	msgc chan multiMessage
	// wait for the handling routines
	wg    sync.WaitGroup
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan multiMessage, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	for range connPerPipeline {
		go p.handle()
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.Stringer("local-member-id", p.tr.ID),
			zap.Stringer("remote-peer-id", p.peerID),
		)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.Stringer("local-member-id", p.tr.ID),
			zap.Stringer("remote-peer-id", p.peerID),
		)
	}
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc:
			start := time.Now()
			err := p.post(m.shardID, pbutil.MustMarshal(&m.msg))
			end := time.Now()

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if isMsgApp(m.msg) && p.followerStats != nil {
					p.followerStats.Fail()
				}
				p.multiRaft.ReportUnreachable(uint64(m.shardID), uint64(m.msg.To))
				if isMsgSnap(m.msg) {
					p.multiRaft.ReportSnapshot(uint64(m.shardID), m.msg.To, raft.SnapshotFailure)
				}
				sentFailures.WithLabelValues(types.ID(m.msg.To).String()).Inc()
				continue
			}

			p.status.activate()
			if isMsgApp(m.msg) && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			if isMsgSnap(m.msg) {
				p.multiRaft.ReportSnapshot(uint64(m.shardID), m.msg.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.msg.To).String()).Add(float64(m.msg.Size()))
		case <-p.stopc:
			return
		}
	}
}

// getSnap fetches a snapshot from a remote peer via HTTP GET.
func (p *pipeline) getSnap(shardID types.ID, snapStore SnapStore, id string) (err error) {
	u := p.picker.pick()
	req := createGetRequest(p.tr.Logger, u, SnapPrefix+"/"+id, p.tr.URLs, p.tr.ID, shardID)
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() {
		select {
		case <-done:
			cancel()
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()
	defer func() {
		done <- struct{}{}
	}()

	resp, err := p.tr.PipelineRt.RoundTrip(req)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("snapshot %s on peer %s: %w", id, p.peerID, ErrSnapshotNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			p.picker.unreachable(u)
			return err
		}

		err = checkGetResponse(p.tr.Logger, resp, b, req, p.peerID)
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if errors.Is(err, errMemberRemoved) {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	// Use SnapStore to save the snapshot (handles atomic write with temp file + rename)
	if err := snapStore.Put(ctx, id, resp.Body); err != nil {
		return fmt.Errorf("failed to store snapshot: %w", err)
	}

	p.tr.Logger.Info("Successfully fetched and saved snapshot",
		zap.String("snapshotID", id),
		zap.Stringer("fromPeerID", p.peerID),
		zap.String("shardID", shardID.String()))
	return nil
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
func (p *pipeline) post(shardID types.ID, data []byte) (err error) {
	u := p.picker.pick()
	req := createPostRequest(p.tr.Logger, u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, shardID)

	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() {
		select {
		case <-done:
			cancel()
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()

	resp, err := p.tr.PipelineRt.RoundTrip(req)
	done <- struct{}{}
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}

	err = checkPostResponse(p.tr.Logger, resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if errors.Is(err, errMemberRemoved) {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { runtime.Gosched() }
