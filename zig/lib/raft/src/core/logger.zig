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

const std = @import("std");
const types = @import("types.zig");
const message_mod = @import("message.zig");

pub const LogLevel = enum {
    debug,
    info,
    warning,
    @"error",
    fatal,
    panic,
};

pub const Logger = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        log: *const fn (ptr: *anyopaque, level: LogLevel, msg: []const u8) void,
    };

    pub fn log(self: Logger, level: LogLevel, msg: []const u8) void {
        self.vtable.log(self.ptr, level, msg);
    }
};

pub const TraceEventType = enum {
    init_state,
    ready,
    commit,
    become_follower,
    become_pre_candidate,
    become_candidate,
    become_leader,
    send_message,
    receive_message,
    replicate,
};

pub const TraceEvent = struct {
    event_type: TraceEventType,
    node_id: types.NodeId,
    leader_id: ?types.NodeId,
    role: types.StateRole,
    term: types.Term,
    vote: ?types.NodeId,
    commit_index: types.Index,
    applied_index: types.Index,
    last_index: types.Index,
    voters: []const types.NodeId,
    voters_outgoing: []const types.NodeId,
    learners: []const types.NodeId,
    learners_next: []const types.NodeId,
    auto_leave: bool,
    message: ?*const message_mod.Message = null,
};

pub const TraceLogger = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        trace_event: *const fn (ptr: *anyopaque, event: *const TraceEvent) void,
    };

    pub fn traceEvent(self: TraceLogger, event: *const TraceEvent) void {
        self.vtable.trace_event(self.ptr, event);
    }
};

var default_logger_state: u8 = 0;
var stderr_logger_state: u8 = 0;

pub fn defaultLogger() Logger {
    return .{
        .ptr = @constCast(&default_logger_state),
        .vtable = &.{
            .log = discardLog,
        },
    };
}

pub fn stdErrLogger() Logger {
    return .{
        .ptr = @constCast(&stderr_logger_state),
        .vtable = &.{
            .log = stdErrLog,
        },
    };
}

fn discardLog(ptr: *anyopaque, level: LogLevel, msg: []const u8) void {
    _ = ptr;
    _ = level;
    _ = msg;
}

fn stdErrLog(ptr: *anyopaque, level: LogLevel, msg: []const u8) void {
    _ = ptr;
    std.debug.print("raft {s}: {s}\n", .{ levelLabel(level), msg });
}

fn levelLabel(level: LogLevel) []const u8 {
    return switch (level) {
        .debug => "DEBUG",
        .info => "INFO",
        .warning => "WARN",
        .@"error" => "ERROR",
        .fatal => "FATAL",
        .panic => "PANIC",
    };
}
