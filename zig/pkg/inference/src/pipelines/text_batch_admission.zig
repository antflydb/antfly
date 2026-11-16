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

//! Split refused text cohorts before dispatch; preserve owned results and order.
const std = @import("std");
const Runner = *const fn (*anyopaque, []const []const u8) anyerror![][]f32;

fn destroy(a: std.mem.Allocator, rows: [][]f32) void {
    for (rows) |row| a.free(row);
    a.free(rows);
}

pub fn run(a: std.mem.Allocator, context: *anyopaque, texts: []const []const u8, execute: Runner, legacy_resource_limits: bool) anyerror![][]f32 {
    return execute(context, texts) catch |err| {
        if (texts.len <= 1 or (err != error.EmbeddingBatchAdmissionDenied and !(legacy_resource_limits and err == error.ResourceLimitExceeded))) return err;
        const midpoint = (texts.len + 1) / 2;
        const left = try run(a, context, texts[0..midpoint], execute, legacy_resource_limits);
        var left_owned = true;
        defer if (left_owned) destroy(a, left);
        if (left.len != midpoint) return error.UnexpectedOutputShape;
        const right = try run(a, context, texts[midpoint..], execute, legacy_resource_limits);
        var right_owned = true;
        defer if (right_owned) destroy(a, right);
        if (right.len != texts.len - midpoint) return error.UnexpectedOutputShape;
        const combined = try a.alloc([]f32, texts.len);
        @memcpy(combined[0..left.len], left);
        @memcpy(combined[left.len..], right);
        a.free(left);
        left_owned = false;
        a.free(right);
        right_owned = false;
        return combined;
    };
}

test "read pool microbatch admission splits before dispatch and preserves output order" {
    const a = std.testing.allocator;
    const Probe = struct {
        calls: usize = 0,
        forwards: usize = 0,
        fn execute(raw: *anyopaque, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (texts.len > 2) return error.EmbeddingBatchAdmissionDenied;
            self.forwards += 1;
            const rows = try std.testing.allocator.alloc([]f32, texts.len);
            var done: usize = 0;
            errdefer {
                for (rows[0..done]) |row| std.testing.allocator.free(row);
                std.testing.allocator.free(rows);
            }
            for (texts, rows) |text, *row| {
                row.* = try std.testing.allocator.dupe(f32, &.{@floatFromInt(text.len)});
                done += 1;
            }
            return rows;
        }
    };
    var probe = Probe{};
    const rows = try run(a, &probe, &.{ "a", "bb", "ccc", "dddd" }, Probe.execute, false);
    defer destroy(a, rows);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 2), probe.forwards);
    for (rows, 1..) |row, i| try std.testing.expectEqual(@as(f32, @floatFromInt(i)), row[0]);
}

test "read pool microbatch never replays GPU failures or a singleton refusal" {
    const Probe = struct {
        failure: anyerror,
        calls: usize = 0,
        fn execute(raw: *anyopaque, _: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return self.failure;
        }
    };
    for ([_]anyerror{ error.ResourceLimitExceeded, error.MemoryBudgetExceeded, error.CudaMemoryLimitExceeded, error.CudaDriverError, error.Canceled, error.DeadlineExceeded }) |failure| {
        var probe = Probe{ .failure = failure };
        try std.testing.expectError(failure, run(std.testing.allocator, &probe, &.{ "a", "b" }, Probe.execute, false));
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
    }
    var singleton = Probe{ .failure = error.EmbeddingBatchAdmissionDenied };
    try std.testing.expectError(singleton.failure, run(std.testing.allocator, &singleton, &.{"a"}, Probe.execute, false));
    try std.testing.expectEqual(@as(usize, 1), singleton.calls);
}
