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

//! Diagnostic-only write-amplification counting layer for Lite.
//!
//! Every public function here is gated by `enabled` (== `builtin.is_test`),
//! a comptime-known constant, so every call site compiles to nothing at all
//! outside test binaries. This module exists solely to back the
//! `zig build lite-write-amplification` harness
//! (`zig/pkg/antfly-embedded/src/storage/lite/write_amplification_bench_test.zig`);
//! see `docs/design/lite-performance/README.md` for how to run it and read
//! its report.
//!
//! A handful of call sites elsewhere in `storage/` push a `Stage` tag onto a
//! thread-local stack before doing durable I/O and pop it afterward
//! (`scope`/`Scope.end`). The actual syscalls happen deep inside the shared
//! `std.Io` implementation the harness supplies, so the harness's `std.Io`
//! wrapper (not production code) calls `recordSync`/`recordWrite` at the
//! real leaf, and those read whatever `Stage` is currently on top of the
//! calling thread's stack.

const std = @import("std");
const builtin = @import("builtin");

/// True only inside test binaries. Every function below is a no-op (and,
/// because this is comptime-known, dead code eliminated) when false.
pub const enabled = builtin.is_test;

pub const Stage = enum(u8) {
    primary_commit,
    artifact_stage,
    artifact_link,
    artifact_pin,
    lsm_wal,
    progress,
    status_snapshot,
    replay_retirement,
    artifact_cleanup,
    other,
};
const stage_count = std.meta.tags(Stage).len;

pub const Op = enum(u8) {
    source_turn,
    maintenance_lease_acquired,
    serving_runtime_refresh,
    derived_worker_turn,
};
const op_count = std.meta.tags(Op).len;

pub const Lane = enum(u8) { calling_thread, background };
const lane_count = std.meta.tags(Lane).len;

fn zeroed(comptime T: type, comptime n: usize) [n]T {
    var out: [n]T = undefined;
    for (&out) |*slot| {
        slot.* = T.init(0);
    }
    return out;
}

fn zeroed2D(comptime T: type, comptime a: usize, comptime b: usize) [a][b]T {
    var out: [a][b]T = undefined;
    for (0..a) |i| {
        for (0..b) |j| {
            out[i][j] = T.init(0);
        }
    }
    return out;
}

var sync_calls: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var write_calls: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var write_bytes: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var durable_commits: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var background_ops: [op_count]std.atomic.Value(u64) = zeroed(std.atomic.Value(u64), op_count);

const no_calling_thread: u64 = 0;
var calling_threads: [16]std.atomic.Value(u64) = zeroed(std.atomic.Value(u64), 16);

threadlocal var stage_stack: [16]Stage = undefined;
threadlocal var stage_depth: usize = 0;

/// Resets every counter and clears the calling-thread registry. Call once,
/// from the harness, before each measured run.
pub fn reset() void {
    if (!enabled) return;
    for (&sync_calls) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&write_calls) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&write_bytes) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&durable_commits) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&background_ops) |*cell| cell.store(0, .monotonic);
    for (&calling_threads) |*slot| slot.store(no_calling_thread, .monotonic);
}

/// Marks the current OS thread as a "calling thread" (the thread that issued
/// an update and is waiting on its acknowledgement), as opposed to a
/// background maintenance/derived-worker thread. Call once per writer
/// thread; safe to call redundantly. Lock-free: registration only grows a
/// small fixed table, so a plain CAS loop is enough.
pub fn registerCallingThread() void {
    if (!enabled) return;
    const id: u64 = @intCast(std.Thread.getCurrentId());
    for (&calling_threads) |*slot| if (slot.load(.monotonic) == id) return;
    for (&calling_threads) |*slot| {
        if (slot.cmpxchgStrong(no_calling_thread, id, .monotonic, .monotonic) == null) return;
    }
}

fn currentLane() Lane {
    const id: u64 = @intCast(std.Thread.getCurrentId());
    for (&calling_threads) |*slot| if (slot.load(.monotonic) == id) return .calling_thread;
    return .background;
}

/// A scoped stage tag. Hold for the duration of one logical durability
/// stage (e.g. one `wal.append` call) on the current thread only.
pub const Scope = struct {
    active: bool,
    pub fn end(self: Scope) void {
        if (self.active) popStage();
    }
};

pub fn scope(stage: Stage) Scope {
    if (!enabled) return .{ .active = false };
    pushStage(stage);
    return .{ .active = true };
}

fn pushStage(stage: Stage) void {
    if (stage_depth < stage_stack.len) stage_stack[stage_depth] = stage;
    stage_depth += 1;
}

fn popStage() void {
    if (stage_depth > 0) stage_depth -= 1;
}

fn currentStage() Stage {
    if (stage_depth == 0) return .other;
    return stage_stack[@min(stage_depth, stage_stack.len) - 1];
}

/// Record one durable sync (fsync/fdatasync/F_FULLFSYNC/msync) attributed to
/// the calling thread's current stage tag.
pub fn recordSync() void {
    if (!enabled) return;
    const lane = currentLane();
    _ = sync_calls[@backingInt(lane)][@backingInt(currentStage())].fetchAdd(1, .monotonic);
}

/// Record one write/pwrite submission of `bytes` attributed to the calling
/// thread's current stage tag.
pub fn recordWrite(bytes: u64) void {
    if (!enabled) return;
    const lane = currentLane();
    const stage = @backingInt(currentStage());
    _ = write_calls[@backingInt(lane)][stage].fetchAdd(1, .monotonic);
    _ = write_bytes[@backingInt(lane)][stage].fetchAdd(bytes, .monotonic);
}

/// Record one durable commit (native store commit_sequence advance, or LSM
/// WAL append) attributed to the calling thread's current stage tag.
pub fn recordCommit() void {
    if (!enabled) return;
    const lane = currentLane();
    _ = durable_commits[@backingInt(lane)][@backingInt(currentStage())].fetchAdd(1, .monotonic);
}

/// Record one background/maintenance operation, independent of stage.
pub fn recordOp(op: Op) void {
    if (!enabled) return;
    _ = background_ops[@backingInt(op)].fetchAdd(1, .monotonic);
}

pub const StageCounts = struct {
    sync_calling_thread: u64,
    sync_background: u64,
    write_calls_calling_thread: u64,
    write_calls_background: u64,
    write_bytes_calling_thread: u64,
    write_bytes_background: u64,
    commits_calling_thread: u64,
    commits_background: u64,
};

pub const Report = struct {
    stages: [stage_count]StageCounts,
    ops: [op_count]u64,

    pub fn stage(self: *const Report, s: Stage) StageCounts {
        return self.stages[@backingInt(s)];
    }

    pub fn op(self: *const Report, o: Op) u64 {
        return self.ops[@backingInt(o)];
    }

    pub fn totalSyncs(self: *const Report, lane: Lane) u64 {
        var total: u64 = 0;
        for (self.stages) |counts| total += switch (lane) {
            .calling_thread => counts.sync_calling_thread,
            .background => counts.sync_background,
        };
        return total;
    }
};

pub fn stageName(s: Stage) []const u8 {
    return @tagName(s);
}

pub fn opName(o: Op) []const u8 {
    return @tagName(o);
}

/// Snapshot every counter. Does not reset them.
pub fn snapshot() Report {
    var report: Report = undefined;
    for (0..stage_count) |i| {
        report.stages[i] = .{
            .sync_calling_thread = sync_calls[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .sync_background = sync_calls[@backingInt(Lane.background)][i].load(.monotonic),
            .write_calls_calling_thread = write_calls[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .write_calls_background = write_calls[@backingInt(Lane.background)][i].load(.monotonic),
            .write_bytes_calling_thread = write_bytes[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .write_bytes_background = write_bytes[@backingInt(Lane.background)][i].load(.monotonic),
            .commits_calling_thread = durable_commits[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .commits_background = durable_commits[@backingInt(Lane.background)][i].load(.monotonic),
        };
    }
    for (0..op_count) |i| report.ops[i] = background_ops[i].load(.monotonic);
    return report;
}

test "write amplification trace scopes attribute to the right stage and lane" {
    reset();
    registerCallingThread();
    {
        const s = scope(.primary_commit);
        defer s.end();
        recordSync();
        recordWrite(128);
        recordCommit();
    }
    {
        const s = scope(.lsm_wal);
        defer s.end();
        recordSync();
    }
    recordOp(.source_turn);
    recordOp(.source_turn);

    const report = snapshot();
    try std.testing.expectEqual(@as(u64, 1), report.stage(.primary_commit).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.primary_commit).sync_background);
    try std.testing.expectEqual(@as(u64, 128), report.stage(.primary_commit).write_bytes_calling_thread);
    try std.testing.expectEqual(@as(u64, 1), report.stage(.primary_commit).commits_calling_thread);
    try std.testing.expectEqual(@as(u64, 1), report.stage(.lsm_wal).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.other).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 2), report.op(.source_turn));

    const background = struct {
        fn run() void {
            const s = scope(.replay_retirement);
            defer s.end();
            recordSync();
        }
    };
    const thread = try std.Thread.spawn(.{}, background.run, .{});
    thread.join();
    const after = snapshot();
    try std.testing.expectEqual(@as(u64, 1), after.stage(.replay_retirement).sync_background);
    try std.testing.expectEqual(@as(u64, 0), after.stage(.replay_retirement).sync_calling_thread);
}
