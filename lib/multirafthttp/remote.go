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
	"github.com/antflydb/antfly/lib/types"

	"go.uber.org/zap"
)

type remote struct {
	lg       *zap.Logger
	localID  types.ID
	id       types.ID
	status   *peerStatus
	pipeline *pipeline
}

func startRemote(tr *Transport, urls types.URLs, id types.ID) *remote {
	picker := newURLPicker(urls)
	status := newPeerStatus(tr.Logger, tr.ID, id)
	pipeline := &pipeline{
		peerID:    id,
		tr:        tr,
		picker:    picker,
		status:    status,
		multiRaft: tr.MultiRaft,
		errorc:    tr.ErrorC,
	}
	pipeline.start()

	return &remote{
		lg:       tr.Logger,
		localID:  tr.ID,
		id:       id,
		status:   status,
		pipeline: pipeline,
	}
}

func (g *remote) send(m multiMessage) {
	select {
	case g.pipeline.msgc <- m:
	default:
		if g.lg != nil {
			g.lg.Warn(
				"dropped Raft message since sending buffer is full (overloaded network)",
				zap.Stringer("message-type", m.msg.Type),
				zap.Stringer("local-member-id", g.localID),
				zap.Stringer("from", types.ID(m.msg.From)),
				zap.Stringer("remote-peer-id", g.id),
				zap.Stringer("shard-id", m.shardID),
				zap.Bool("remote-peer-active", g.status.isActive()),
			)
		}
		sentFailures.WithLabelValues(types.ID(m.msg.To).String()).Inc()
	}
}

func (g *remote) stop() {
	g.pipeline.stop()
}

func (g *remote) Pause() {
	g.stop()
}

func (g *remote) Resume() {
	g.pipeline.start()
}
