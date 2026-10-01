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

const raft_engine = @import("raft_engine");
const raft_trace_logger = @import("raft_trace_logger.zig");
const stderr_writer = @import("stderr_writer.zig");

/// Server Raft events share the process output sink with local transaction events.
pub fn stderrRaftTraceLogger() raft_engine.core.TraceLogger {
    const S = struct {
        const sink = stderr_writer.traceSink();
        var ndjson_logger: raft_trace_logger.RaftNdjsonTraceLogger = .{ .writer = sink.writer, .shared_mutex = sink.mutex };
    };
    return S.ndjson_logger.traceLogger();
}
