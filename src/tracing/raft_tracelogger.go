// Copyright 2026 Antfly, Inc.
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

//go:build with_tla

package tracing

import (
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

type zapTraceLogger struct {
	lg *zap.Logger
}

// NewRaftTraceLogger returns a raft.TraceLogger that emits TracingEvents as
// ndjson via the provided zap logger. The output format matches what
// Traceetcdraft.tla expects: each line is a JSON object with tag="trace"
// and the TracingEvent nested under "event".
//
// When built without the with_tla tag, the nop variant returns nil, which
// causes etcd/raft to skip all trace instrumentation with zero overhead.
func NewRaftTraceLogger(lg *zap.Logger) raft.TraceLogger {
	if lg == nil {
		return nil
	}
	return &zapTraceLogger{lg: lg}
}

func (t *zapTraceLogger) TraceEvent(ev *raft.TracingEvent) {
	t.lg.Debug("trace",
		zap.String("tag", "trace"),
		zap.Any("event", ev),
	)
}
