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

const backends = @import("backends/backends.zig");
const ops = @import("ops/ops.zig");
const quant_matmul = @import("graph/quant_matmul.zig");
const session_factory = @import("architectures/session_factory.zig");

pub const Stats = struct {
    q2_k: u64 = 0,
    q2_k_bias: u64 = 0,
    q2_k_bias_gelu: u64 = 0,
    q3_k: u64 = 0,
    q3_k_bias: u64 = 0,
    q3_k_bias_gelu: u64 = 0,
    q4_0: u64 = 0,
    q4_1: u64 = 0,
    q5_0: u64 = 0,
    q5_1: u64 = 0,
    q4_k: u64 = 0,
    q4_k_bias: u64 = 0,
    q4_k_bias_gelu: u64 = 0,
    q5_k: u64 = 0,
    q5_k_bias: u64 = 0,
    q5_k_bias_gelu: u64 = 0,
    q6_k: u64 = 0,
    q6_k_bias: u64 = 0,
    q6_k_bias_gelu: u64 = 0,
    q8_0: u64 = 0,
    q8_0_bias: u64 = 0,
    q8_0_bias_gelu: u64 = 0,
    q8_0_relu: u64 = 0,
    q8_1: u64 = 0,
    q8_k: u64 = 0,
    q4_k_rows_1: u64 = 0,
    q4_k_rows_2_8: u64 = 0,
    q4_k_rows_9_64: u64 = 0,
    q4_k_rows_65_plus: u64 = 0,
    q6_k_rows_1: u64 = 0,
    q6_k_rows_2_8: u64 = 0,
    q6_k_rows_9_64: u64 = 0,
    q6_k_rows_65_plus: u64 = 0,
    q8_0_rows_1: u64 = 0,
    q8_0_rows_2_8: u64 = 0,
    q8_0_rows_9_64: u64 = 0,
    q8_0_rows_65_plus: u64 = 0,
    jit_exact_q4_0: u64 = 0,
    jit_exact_q4_k: u64 = 0,

    pub fn generatedTotal(self: Stats) u64 {
        return self.q2_k +
            self.q2_k_bias +
            self.q2_k_bias_gelu +
            self.q3_k +
            self.q3_k_bias +
            self.q3_k_bias_gelu +
            self.q4_0 +
            self.q4_1 +
            self.q5_0 +
            self.q5_1 +
            self.q4_k +
            self.q4_k_bias +
            self.q4_k_bias_gelu +
            self.q5_k +
            self.q5_k_bias +
            self.q5_k_bias_gelu +
            self.q6_k +
            self.q6_k_bias +
            self.q6_k_bias_gelu +
            self.q8_0 +
            self.q8_0_bias +
            self.q8_0_bias_gelu +
            self.q8_0_relu +
            self.q8_1 +
            self.q8_k;
    }

    pub const FamilySummary = struct {
        name: []const u8 = "none",
        count: u64 = 0,
    };

    pub fn topFamily(self: Stats) FamilySummary {
        var best = FamilySummary{};
        best = chooseTopFamily(best, "q2_k", self.q2_k + self.q2_k_bias + self.q2_k_bias_gelu);
        best = chooseTopFamily(best, "q3_k", self.q3_k + self.q3_k_bias + self.q3_k_bias_gelu);
        best = chooseTopFamily(best, "q4_0", self.q4_0);
        best = chooseTopFamily(best, "q4_1", self.q4_1);
        best = chooseTopFamily(best, "q5_0", self.q5_0);
        best = chooseTopFamily(best, "q5_1", self.q5_1);
        best = chooseTopFamily(best, "q4_k", self.q4_k + self.q4_k_bias + self.q4_k_bias_gelu);
        best = chooseTopFamily(best, "q5_k", self.q5_k + self.q5_k_bias + self.q5_k_bias_gelu);
        best = chooseTopFamily(best, "q6_k", self.q6_k + self.q6_k_bias + self.q6_k_bias_gelu);
        best = chooseTopFamily(best, "q8_0", self.q8_0 + self.q8_0_bias + self.q8_0_bias_gelu + self.q8_0_relu);
        best = chooseTopFamily(best, "q8_1", self.q8_1);
        best = chooseTopFamily(best, "q8_k", self.q8_k);
        return best;
    }

    pub fn nonzeroFamilyCount(self: Stats) u64 {
        var count: u64 = 0;
        if (self.q2_k + self.q2_k_bias + self.q2_k_bias_gelu != 0) count += 1;
        if (self.q3_k + self.q3_k_bias + self.q3_k_bias_gelu != 0) count += 1;
        if (self.q4_0 != 0) count += 1;
        if (self.q4_1 != 0) count += 1;
        if (self.q5_0 != 0) count += 1;
        if (self.q5_1 != 0) count += 1;
        if (self.q4_k + self.q4_k_bias + self.q4_k_bias_gelu != 0) count += 1;
        if (self.q5_k + self.q5_k_bias + self.q5_k_bias_gelu != 0) count += 1;
        if (self.q6_k + self.q6_k_bias + self.q6_k_bias_gelu != 0) count += 1;
        if (self.q8_0 + self.q8_0_bias + self.q8_0_bias_gelu + self.q8_0_relu != 0) count += 1;
        if (self.q8_1 != 0) count += 1;
        if (self.q8_k != 0) count += 1;
        return count;
    }

    pub fn add(lhs: Stats, rhs: Stats) Stats {
        return .{
            .q2_k = lhs.q2_k + rhs.q2_k,
            .q2_k_bias = lhs.q2_k_bias + rhs.q2_k_bias,
            .q2_k_bias_gelu = lhs.q2_k_bias_gelu + rhs.q2_k_bias_gelu,
            .q3_k = lhs.q3_k + rhs.q3_k,
            .q3_k_bias = lhs.q3_k_bias + rhs.q3_k_bias,
            .q3_k_bias_gelu = lhs.q3_k_bias_gelu + rhs.q3_k_bias_gelu,
            .q4_0 = lhs.q4_0 + rhs.q4_0,
            .q4_1 = lhs.q4_1 + rhs.q4_1,
            .q5_0 = lhs.q5_0 + rhs.q5_0,
            .q5_1 = lhs.q5_1 + rhs.q5_1,
            .q4_k = lhs.q4_k + rhs.q4_k,
            .q4_k_bias = lhs.q4_k_bias + rhs.q4_k_bias,
            .q4_k_bias_gelu = lhs.q4_k_bias_gelu + rhs.q4_k_bias_gelu,
            .q5_k = lhs.q5_k + rhs.q5_k,
            .q5_k_bias = lhs.q5_k_bias + rhs.q5_k_bias,
            .q5_k_bias_gelu = lhs.q5_k_bias_gelu + rhs.q5_k_bias_gelu,
            .q6_k = lhs.q6_k + rhs.q6_k,
            .q6_k_bias = lhs.q6_k_bias + rhs.q6_k_bias,
            .q6_k_bias_gelu = lhs.q6_k_bias_gelu + rhs.q6_k_bias_gelu,
            .q8_0 = lhs.q8_0 + rhs.q8_0,
            .q8_0_bias = lhs.q8_0_bias + rhs.q8_0_bias,
            .q8_0_bias_gelu = lhs.q8_0_bias_gelu + rhs.q8_0_bias_gelu,
            .q8_0_relu = lhs.q8_0_relu + rhs.q8_0_relu,
            .q8_1 = lhs.q8_1 + rhs.q8_1,
            .q8_k = lhs.q8_k + rhs.q8_k,
            .q4_k_rows_1 = lhs.q4_k_rows_1 + rhs.q4_k_rows_1,
            .q4_k_rows_2_8 = lhs.q4_k_rows_2_8 + rhs.q4_k_rows_2_8,
            .q4_k_rows_9_64 = lhs.q4_k_rows_9_64 + rhs.q4_k_rows_9_64,
            .q4_k_rows_65_plus = lhs.q4_k_rows_65_plus + rhs.q4_k_rows_65_plus,
            .q6_k_rows_1 = lhs.q6_k_rows_1 + rhs.q6_k_rows_1,
            .q6_k_rows_2_8 = lhs.q6_k_rows_2_8 + rhs.q6_k_rows_2_8,
            .q6_k_rows_9_64 = lhs.q6_k_rows_9_64 + rhs.q6_k_rows_9_64,
            .q6_k_rows_65_plus = lhs.q6_k_rows_65_plus + rhs.q6_k_rows_65_plus,
            .q8_0_rows_1 = lhs.q8_0_rows_1 + rhs.q8_0_rows_1,
            .q8_0_rows_2_8 = lhs.q8_0_rows_2_8 + rhs.q8_0_rows_2_8,
            .q8_0_rows_9_64 = lhs.q8_0_rows_9_64 + rhs.q8_0_rows_9_64,
            .q8_0_rows_65_plus = lhs.q8_0_rows_65_plus + rhs.q8_0_rows_65_plus,
            .jit_exact_q4_0 = lhs.jit_exact_q4_0 + rhs.jit_exact_q4_0,
            .jit_exact_q4_k = lhs.jit_exact_q4_k + rhs.jit_exact_q4_k,
        };
    }

    pub fn diff(before: Stats, after: Stats) Stats {
        return .{
            .q2_k = counterDelta(before.q2_k, after.q2_k),
            .q2_k_bias = counterDelta(before.q2_k_bias, after.q2_k_bias),
            .q2_k_bias_gelu = counterDelta(before.q2_k_bias_gelu, after.q2_k_bias_gelu),
            .q3_k = counterDelta(before.q3_k, after.q3_k),
            .q3_k_bias = counterDelta(before.q3_k_bias, after.q3_k_bias),
            .q3_k_bias_gelu = counterDelta(before.q3_k_bias_gelu, after.q3_k_bias_gelu),
            .q4_0 = counterDelta(before.q4_0, after.q4_0),
            .q4_1 = counterDelta(before.q4_1, after.q4_1),
            .q5_0 = counterDelta(before.q5_0, after.q5_0),
            .q5_1 = counterDelta(before.q5_1, after.q5_1),
            .q4_k = counterDelta(before.q4_k, after.q4_k),
            .q4_k_bias = counterDelta(before.q4_k_bias, after.q4_k_bias),
            .q4_k_bias_gelu = counterDelta(before.q4_k_bias_gelu, after.q4_k_bias_gelu),
            .q5_k = counterDelta(before.q5_k, after.q5_k),
            .q5_k_bias = counterDelta(before.q5_k_bias, after.q5_k_bias),
            .q5_k_bias_gelu = counterDelta(before.q5_k_bias_gelu, after.q5_k_bias_gelu),
            .q6_k = counterDelta(before.q6_k, after.q6_k),
            .q6_k_bias = counterDelta(before.q6_k_bias, after.q6_k_bias),
            .q6_k_bias_gelu = counterDelta(before.q6_k_bias_gelu, after.q6_k_bias_gelu),
            .q8_0 = counterDelta(before.q8_0, after.q8_0),
            .q8_0_bias = counterDelta(before.q8_0_bias, after.q8_0_bias),
            .q8_0_bias_gelu = counterDelta(before.q8_0_bias_gelu, after.q8_0_bias_gelu),
            .q8_0_relu = counterDelta(before.q8_0_relu, after.q8_0_relu),
            .q8_1 = counterDelta(before.q8_1, after.q8_1),
            .q8_k = counterDelta(before.q8_k, after.q8_k),
            .q4_k_rows_1 = counterDelta(before.q4_k_rows_1, after.q4_k_rows_1),
            .q4_k_rows_2_8 = counterDelta(before.q4_k_rows_2_8, after.q4_k_rows_2_8),
            .q4_k_rows_9_64 = counterDelta(before.q4_k_rows_9_64, after.q4_k_rows_9_64),
            .q4_k_rows_65_plus = counterDelta(before.q4_k_rows_65_plus, after.q4_k_rows_65_plus),
            .q6_k_rows_1 = counterDelta(before.q6_k_rows_1, after.q6_k_rows_1),
            .q6_k_rows_2_8 = counterDelta(before.q6_k_rows_2_8, after.q6_k_rows_2_8),
            .q6_k_rows_9_64 = counterDelta(before.q6_k_rows_9_64, after.q6_k_rows_9_64),
            .q6_k_rows_65_plus = counterDelta(before.q6_k_rows_65_plus, after.q6_k_rows_65_plus),
            .q8_0_rows_1 = counterDelta(before.q8_0_rows_1, after.q8_0_rows_1),
            .q8_0_rows_2_8 = counterDelta(before.q8_0_rows_2_8, after.q8_0_rows_2_8),
            .q8_0_rows_9_64 = counterDelta(before.q8_0_rows_9_64, after.q8_0_rows_9_64),
            .q8_0_rows_65_plus = counterDelta(before.q8_0_rows_65_plus, after.q8_0_rows_65_plus),
            .jit_exact_q4_0 = counterDelta(before.jit_exact_q4_0, after.jit_exact_q4_0),
            .jit_exact_q4_k = counterDelta(before.jit_exact_q4_k, after.jit_exact_q4_k),
        };
    }

    pub fn fromProviderStats(stats: ops.NativeQuantTimingStats) Stats {
        const generated = &stats.metal_runtime_antfly_generated_dispatch_counts;
        return .{
            .q2_k = quant_matmul.generatedQuantDispatchCount(generated, .q2_k, .none),
            .q2_k_bias = quant_matmul.generatedQuantDispatchCount(generated, .q2_k, .bias),
            .q2_k_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q2_k, .bias_gelu),
            .q3_k = quant_matmul.generatedQuantDispatchCount(generated, .q3_k, .none),
            .q3_k_bias = quant_matmul.generatedQuantDispatchCount(generated, .q3_k, .bias),
            .q3_k_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q3_k, .bias_gelu),
            .q4_0 = quant_matmul.generatedQuantDispatchCount(generated, .q4_0, .none),
            .q4_1 = quant_matmul.generatedQuantDispatchCount(generated, .q4_1, .none),
            .q5_0 = quant_matmul.generatedQuantDispatchCount(generated, .q5_0, .none),
            .q5_1 = quant_matmul.generatedQuantDispatchCount(generated, .q5_1, .none),
            .q4_k = quant_matmul.generatedQuantDispatchCount(generated, .q4_k, .none),
            .q4_k_bias = quant_matmul.generatedQuantDispatchCount(generated, .q4_k, .bias),
            .q4_k_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q4_k, .bias_gelu),
            .q5_k = quant_matmul.generatedQuantDispatchCount(generated, .q5_k, .none),
            .q5_k_bias = quant_matmul.generatedQuantDispatchCount(generated, .q5_k, .bias),
            .q5_k_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q5_k, .bias_gelu),
            .q6_k = quant_matmul.generatedQuantDispatchCount(generated, .q6_k, .none),
            .q6_k_bias = quant_matmul.generatedQuantDispatchCount(generated, .q6_k, .bias),
            .q6_k_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q6_k, .bias_gelu),
            .q8_0 = quant_matmul.generatedQuantDispatchCount(generated, .q8_0, .none),
            .q8_0_bias = quant_matmul.generatedQuantDispatchCount(generated, .q8_0, .bias),
            .q8_0_bias_gelu = quant_matmul.generatedQuantDispatchCount(generated, .q8_0, .bias_gelu),
            .q8_0_relu = quant_matmul.generatedQuantDispatchCount(generated, .q8_0, .relu),
            .q8_1 = quant_matmul.generatedQuantDispatchCount(generated, .q8_1, .none),
            .q8_k = quant_matmul.generatedQuantDispatchCount(generated, .q8_k, .none),
            .q4_k_rows_1 = stats.metal_runtime_q4_k_linear_reduce_rows_1,
            .q4_k_rows_2_8 = stats.metal_runtime_q4_k_linear_reduce_rows_2_8,
            .q4_k_rows_9_64 = stats.metal_runtime_q4_k_linear_reduce_rows_9_64,
            .q4_k_rows_65_plus = stats.metal_runtime_q4_k_linear_reduce_rows_65_plus,
            .q6_k_rows_1 = stats.metal_runtime_q6_k_linear_reduce_rows_1,
            .q6_k_rows_2_8 = stats.metal_runtime_q6_k_linear_reduce_rows_2_8,
            .q6_k_rows_9_64 = stats.metal_runtime_q6_k_linear_reduce_rows_9_64,
            .q6_k_rows_65_plus = stats.metal_runtime_q6_k_linear_reduce_rows_65_plus,
            .q8_0_rows_1 = stats.metal_runtime_q8_0_linear_rows_1,
            .q8_0_rows_2_8 = stats.metal_runtime_q8_0_linear_rows_2_8,
            .q8_0_rows_9_64 = stats.metal_runtime_q8_0_linear_rows_9_64,
            .q8_0_rows_65_plus = stats.metal_runtime_q8_0_linear_rows_65_plus,
            .jit_exact_q4_0 = stats.metal_runtime_jit_exact_q4_0_hits,
            .jit_exact_q4_k = stats.metal_runtime_jit_exact_q4_k_hits,
        };
    }
};

fn chooseTopFamily(current: Stats.FamilySummary, name: []const u8, count: u64) Stats.FamilySummary {
    return if (count > current.count) .{ .name = name, .count = count } else current;
}

pub fn snapshotForSession(
    allocator: std.mem.Allocator,
    session: backends.Session,
) Stats {
    var cb = session_factory.getComputeBackend(session, allocator) catch return .{};
    defer cb.deinit();
    const snapshot = cb.debugTimingSnapshot();
    return Stats.fromProviderStats(snapshot.provider);
}

fn counterDelta(before: u64, after: u64) u64 {
    return if (after >= before) after - before else 0;
}

test "quant kernel compiler metal generated quant stats total covers every generated provider counter" {
    var provider = ops.NativeQuantTimingStats{};
    for (quant_matmul.generated_quant_counter_names) |counter| {
        provider.metal_runtime_antfly_generated_dispatch_counts[@backingInt(counter.format)][@backingInt(counter.epilogue)] = 1;
    }
    provider.metal_runtime_jit_exact_q4_0_hits = 7;
    provider.metal_runtime_jit_exact_q4_k_hits = 9;

    const stats = Stats.fromProviderStats(provider);
    try std.testing.expectEqual(@as(u64, 25), stats.generatedTotal());
    try std.testing.expectEqual(@as(u64, 25), Stats.diff(.{}, stats).generatedTotal());
    try std.testing.expectEqual(@as(u64, 50), stats.add(stats).generatedTotal());
    try std.testing.expectEqual(@as(u64, 12), stats.nonzeroFamilyCount());
    try std.testing.expectEqualStrings("q8_0", stats.topFamily().name);
    try std.testing.expectEqual(@as(u64, 4), stats.topFamily().count);
    try std.testing.expectEqual(@as(u64, 7), stats.jit_exact_q4_0);
    try std.testing.expectEqual(@as(u64, 9), stats.jit_exact_q4_k);
    try std.testing.expectEqual(@as(u64, 18), stats.add(stats).jit_exact_q4_k);
    try std.testing.expectEqual(@as(u64, 7), Stats.diff(.{}, stats).jit_exact_q4_0);
}
