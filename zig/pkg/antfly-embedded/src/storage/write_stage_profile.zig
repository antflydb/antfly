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

//! Wall time per write stage, summed across every writer in the process.
//! Off unless `ANTFLY_LITE_WRITE_PROFILE` is set; then every
//! `report_every` writes print the running totals to stderr, one line per
//! stage. Stages nest (a write's `ticket_wait` contains its `redo_sync`),
//! so totals do not add up to `write`.

const std = @import("std");
const builtin = @import("builtin");
const platform_time = @import("antfly_platform").time;

pub const Stage = enum(u8) {
    /// `capi` entry: admission, writer lease and refresh.
    entry,
    /// `Owner.refreshImpl` under that entry.
    refresh,
    /// `DB.batchInternal`, the whole write.
    write,
    /// Inline full-text preparation (tokenize, build level-0).
    inline_text_prepare,
    /// Staging the primary write into the group commit.
    primary_stage,
    /// The primary write's ticket wait: group apply plus durability.
    ticket_wait,
    /// The group leader applying requests (participants, resolvers).
    group_apply,
    /// `syncRedoData` for a commit.
    redo_sync,
    /// The redo frontier's write and sync.
    frontier_sync,
    /// Installing inline publications after the wait.
    inline_install,
    /// `waitForSyncLevel` after the commit.
    wait_sync_level,
    /// One merge task (`runOnceTask`).
    merge,
    /// A merge commit waiting for the writer lease.
    merge_lease_wait,
};

const stage_count = @typeInfo(Stage).@"enum".field_names.len;
const report_every = 2000;

var state: std.atomic.Value(u8) = .init(0); // 0 unknown, 1 off, 2 on
var total_ns: [stage_count]std.atomic.Value(u64) = @splat(.init(0));
var counts: [stage_count]std.atomic.Value(u64) = @splat(.init(0));

pub fn enabled() bool {
    if (builtin.os.tag == .freestanding) return false;
    switch (state.load(.monotonic)) {
        1 => return false,
        2 => return true,
        else => {},
    }
    const on = std.c.getenv("ANTFLY_LITE_WRITE_PROFILE") != null;
    state.store(if (on) 2 else 1, .monotonic);
    return on;
}

fn nowNs() u64 {
    return @max(platform_time.monotonicNs(), 1);
}

pub const Span = struct {
    stage: Stage,
    start_ns: u64,

    pub fn end(self: Span) void {
        if (self.start_ns == 0) return;
        const i = @backingInt(self.stage);
        _ = total_ns[i].fetchAdd(nowNs() -| self.start_ns, .monotonic);
        const n = counts[i].fetchAdd(1, .monotonic) + 1;
        if (self.stage == .write and n % report_every == 0) report(n);
    }
};

pub fn begin(stage: Stage) Span {
    return .{ .stage = stage, .start_ns = if (enabled()) nowNs() else 0 };
}

fn report(writes: u64) void {
    std.debug.print("lite_write_profile writes={d}", .{writes});
    for (0..stage_count) |i| {
        const n = counts[i].load(.monotonic);
        if (n == 0) continue;
        const ns = total_ns[i].load(.monotonic);
        std.debug.print(" {s}={d}us/{d}", .{ @tagName(@as(Stage, @fromBackingInt(@intCast(i)))), ns / 1000, n });
    }
    std.debug.print("\n", .{});
}
