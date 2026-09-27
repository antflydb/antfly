// Copyright 2026 Antfly, Inc.
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

pub const raft_trace_logger = @import("raft_trace_logger.zig");
pub const antfly_trace_writer = @import("antfly_trace_writer.zig");
pub const stderr_writer = @import("stderr_writer.zig");

pub const RaftNdjsonTraceLogger = raft_trace_logger.RaftNdjsonTraceLogger;
pub const AntflyTraceWriter = antfly_trace_writer.AntflyTraceWriter;
pub const AntflyNdjsonTraceWriter = antfly_trace_writer.AntflyNdjsonTraceWriter;
pub const stderrAntflyTraceWriter = stderr_writer.stderrAntflyTraceWriter;
pub const stderrRaftTraceLogger = stderr_writer.stderrRaftTraceLogger;

test {
    // File ownership is part of the normal Raft unit gate too. Without an
    // explicit import its tests are discovered only when TLA logging is used.
    _ = @import("trace_file.zig");
}
