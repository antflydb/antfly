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

//! A synchronous cohort borrows all row admissions; it never widens a cap or
//! allocates under only the leader's budget. Durable allocations stay owned by
//! their original request and the process DeviceBuffer guard remains final.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");

pub const Budget = struct {
    run: memory.RunBudget,

    pub fn init(owners: []const *memory.RunBudget) !Budget {
        if (owners.len < 2 or owners.len > 2) return error.InvalidBatchDecodeState;
        var limits = owners[0].limits;
        var host_weights: usize = 0;
        var backend_weights: usize = 0;
        for (owners, 0..) |owner, i| {
            for (owners[0..i]) |previous| if (previous == owner) return error.InvalidBatchDecodeState;
            inline for (comptime std.meta.fieldNames(memory.Limits)) |field| {
                const a = @field(limits, field);
                const b = @field(owner.limits, field);
                @field(limits, field) = if (a == 0) b else if (b == 0) a else @min(a, b);
            }
            host_weights = @max(host_weights, owner.host_weight_bytes);
            backend_weights = @max(backend_weights, owner.backend_weight_bytes);
        }
        var run = memory.RunBudget.init(limits);
        _ = try run.tryReserveWeight(.host, host_weights);
        _ = try run.tryReserveWeight(.backend, backend_weights);
        for (owners) |owner| {
            try run.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = owner.host_kv_bytes, .kv_tier = .host, .scratch_bytes = owner.host_scratch_bytes, .scratch_tier = .host });
            try run.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = owner.backend_kv_bytes, .kv_tier = .backend, .scratch_bytes = owner.backend_scratch_bytes, .scratch_tier = .backend });
        }
        // Shared arena growth may consume only scratch already admitted for
        // these rows, even when an operator configured a much larger limit.
        const admitted = run.scratchTotalBytes();
        if (admitted == 0) return error.MemoryBudgetExceeded;
        run.limits.scratch_limit_bytes = if (limits.scratch_limit_bytes == 0) admitted else @min(admitted, limits.scratch_limit_bytes);
        return .{ .run = run };
    }
};

test "execution batch budgets include every owner without widening limits or duplicating weights" {
    var a = memory.RunBudget.init(.{ .backend_limit_bytes = 1000, .scratch_limit_bytes = 500 });
    var b = memory.RunBudget.init(.{ .backend_limit_bytes = 900, .scratch_limit_bytes = 400 });
    _ = try a.tryReserveWeight(.backend, 100);
    _ = try b.tryReserveWeight(.backend, 100);
    try a.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 200, .kv_tier = .backend, .scratch_bytes = 50, .scratch_tier = .backend });
    try b.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 300, .kv_tier = .backend, .scratch_bytes = 60, .scratch_tier = .backend });
    const batch = try Budget.init(&.{ &a, &b });
    try std.testing.expectEqual(@as(usize, 100), batch.run.backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 500), batch.run.backend_kv_bytes);
    try std.testing.expectEqual(@as(usize, 110), batch.run.limits.scratch_limit_bytes);
    try std.testing.expectEqual(@as(usize, 900), batch.run.limits.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 200), a.backend_kv_bytes);
    try std.testing.expectError(error.InvalidBatchDecodeState, Budget.init(&.{ &a, &a }));
    b.limits.backend_limit_bytes = 600;
    try std.testing.expectError(error.MemoryBudgetExceeded, Budget.init(&.{ &a, &b }));
}
