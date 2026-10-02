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

//! Provider-neutral inference request cancellation, deadline, and progress.

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform_time = @import("antfly_platform").time;

pub const Phase = enum(u8) {
    queued,
    loading_model,
    loading_weights,
    preparing_weights,
    tokenizing,
    executing,
    serializing,
    publishing,
};

pub const Progress = struct {
    phase: Phase,
    completed: u64 = 0,
    total: u64 = 0,
    model: []const u8 = "",
    backend: []const u8 = "",
    deadline_ns: ?u64 = null,
};

pub const ProgressSink = struct {
    ptr: ?*anyopaque = null,
    update_fn: *const fn (?*anyopaque, Progress) void,

    pub fn update(self: ProgressSink, progress: Progress) void {
        self.update_fn(self.ptr, progress);
    }
};

/// Invocation-local control plane shared by all model families and checked
/// callback boundaries. It borrows its cancellation and progress targets for
/// the duration of one admitted invocation.
pub const RequestContext = struct {
    io: std.Io,
    deadline_ns: ?u64,
    cancellation: ?CancellationToken = null,
    progress: ?ProgressSink = null,

    pub fn check(self: RequestContext) !void {
        if (self.cancellation) |value| if (value.isCancelled()) return error.Cancelled;
        const deadline = self.deadline_ns orelse return;
        if (platform_time.monotonicNs() >= deadline) return error.Timeout;
    }

    pub fn remainingTimeoutMs(self: RequestContext) !?u64 {
        try self.check();
        const deadline = self.deadline_ns orelse return null;
        const remaining_ns = deadline -| platform_time.monotonicNs();
        return @max(@as(u64, 1), std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1);
    }

    pub fn update(self: RequestContext, phase: Phase, completed: u64, total: u64) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{ .phase = phase, .completed = completed, .total = total, .deadline_ns = self.deadline_ns });
    }

    pub fn updateDetail(self: RequestContext, phase: Phase, completed: u64, total: u64, model: []const u8, backend: []const u8) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{
            .phase = phase,
            .completed = completed,
            .total = total,
            .model = model,
            .backend = backend,
            .deadline_ns = self.deadline_ns,
        });
    }
};
