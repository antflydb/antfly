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

//! Ownership and physical-workspace limits for read-model classification cohorts.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const span = @import("../extractors/gliner_span_v2_executor.zig");

pub fn classificationBudget(limits: memory.Limits, config: span.EncoderConfig, sequence: usize, rows: usize) !memory.RunBudget {
    if (rows == 0 or rows > 8 or sequence == 0 or sequence > 512) return error.InvalidMicrobatchLimits;
    const device = try std.math.mul(usize, try span.deviceScratchUpperBound(config, sequence), rows);
    var budget = memory.RunBudget.init(limits);
    try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 32 * 1024 * 1024, .scratch_tier = .host });
    try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = device, .scratch_tier = .backend });
    return budget;
}

pub const OutputOwner = struct {
    lease: memory.AdmissionLease,
    remaining: std.atomic.Value(usize),
    pub fn release(self: *@This()) void {
        if (self.remaining.fetchSub(1, .acq_rel) == 1) {
            self.lease.release();
            std.heap.smp_allocator.destroy(self);
        }
    }
};
pub const Rows = struct { rows: [][]f64, owner: ?*OutputOwner = null };

pub fn clone(a: std.mem.Allocator, value: Rows) !Rows {
    const copy = try a.alloc([]f64, value.rows.len);
    var done: usize = 0;
    errdefer {
        for (copy[0..done]) |row| a.free(row);
        a.free(copy);
    }
    for (value.rows, copy) |row, *dest| {
        dest.* = try a.dupe(f64, row);
        done += 1;
    }
    return .{ .rows = copy };
}

pub fn destroy(a: std.mem.Allocator, value: Rows) void {
    for (value.rows) |row| a.free(row);
    a.free(value.rows);
    if (value.owner) |owner| owner.release();
}

test "read pool microbatch counts every padded row and never widens owner limits" {
    const config = span.EncoderConfig{ .deberta = .{} };
    const one = try span.deviceScratchUpperBound(config, 128);
    const limits = memory.Limits{ .backend_limit_bytes = one * 2, .scratch_limit_bytes = 32 * 1024 * 1024 + one * 2 };
    const budget = try classificationBudget(limits, config, 128, 2);
    try std.testing.expectEqual(one * 2, budget.backend_scratch_bytes);
    try std.testing.expectEqualDeep(limits, budget.limits);
    try std.testing.expectError(error.MemoryBudgetExceeded, classificationBudget(limits, config, 128, 3));
    _ = try classificationBudget(limits, config, 128, 1);
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 513, 1));
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 128, 9));
}

test "read pool microbatch output admission outlives independent consumers and clone failures" {
    const a = std.heap.smp_allocator;
    var controller = memory.AdmissionController{};
    const owner = try a.create(OutputOwner);
    owner.* = .{ .lease = try controller.tryAcquire(.gpu, .{}, .{ .host_scratch_bytes = 128 }, false), .remaining = .init(2) };
    const first_rows = try a.alloc([]f64, 1);
    first_rows[0] = try a.dupe(f64, &.{ 1, 2 });
    const second_rows = try a.alloc([]f64, 1);
    second_rows[0] = try a.dupe(f64, &.{ 3, 4, 5 });
    const first = Rows{ .rows = first_rows, .owner = owner };
    const second = Rows{ .rows = second_rows, .owner = owner };
    const Probe = struct {
        fn run(alloc: std.mem.Allocator, value: Rows) !void {
            const copy = try clone(alloc, value);
            defer destroy(alloc, copy);
            try std.testing.expect(copy.owner == null);
            try std.testing.expectEqualSlices(f64, value.rows[0], copy.rows[0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{first});
    destroy(a, first);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroy(a, second);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
