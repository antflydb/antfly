// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

const raft_engine = @import("raft_engine");
const raft_trace_logger = @import("raft_trace_logger.zig");
const stderr_writer = @import("antfly_local_sources").tracing_stderr_writer;

/// Server Raft events share the process output sink with local transaction events.
pub fn stderrRaftTraceLogger() raft_engine.core.TraceLogger {
    const S = struct {
        const sink = stderr_writer.traceSink();
        var ndjson_logger: raft_trace_logger.RaftNdjsonTraceLogger = .{ .writer = sink.writer, .shared_mutex = sink.mutex };
    };
    return S.ndjson_logger.traceLogger();
}
