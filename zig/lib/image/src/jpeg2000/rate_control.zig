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
const tier1_encode = @import("tier1_encode.zig");

pub const native_port_available = true;

/// Information about a single codeblock's encoding for rate control.
pub const CodeblockRDInfo = struct {
    /// Cumulative bytes after each coding pass.
    pass_lengths: []u32,
    /// Cumulative distortion reduction after each coding pass.
    pass_distortions: []f64,
    /// Total number of coding passes available.
    num_passes: u16,
};

/// Result of rate control: how many passes to keep for each codeblock.
pub const TruncationResult = struct {
    /// Number of passes to include for each codeblock.
    pass_counts: []u16,
    /// Total bytes used.
    total_bytes: u64,

    pub fn deinit(self: *TruncationResult, allocator: std.mem.Allocator) void {
        allocator.free(self.pass_counts);
        self.* = undefined;
    }
};

/// Compute the truncation points for each codeblock to meet a target byte budget.
/// Uses the PCRD bisection algorithm:
/// 1. For each codeblock, compute rate-distortion slopes at each truncation point
/// 2. Binary search for the optimal slope threshold lambda
/// 3. For each codeblock, include passes whose R-D slope exceeds lambda
pub fn optimizeTruncation(
    allocator: std.mem.Allocator,
    codeblocks: []const CodeblockRDInfo,
    target_bytes: u64,
) !TruncationResult {
    const pass_counts = try allocator.alloc(u16, codeblocks.len);
    errdefer allocator.free(pass_counts);

    // Start with all passes included
    var total: u64 = 0;
    for (codeblocks, 0..) |cb, i| {
        pass_counts[i] = cb.num_passes;
        if (cb.num_passes > 0 and cb.pass_lengths.len > 0) {
            total += cb.pass_lengths[cb.num_passes - 1];
        }
    }

    // If already under budget, keep everything
    if (total <= target_bytes) {
        return .{ .pass_counts = pass_counts, .total_bytes = total };
    }

    // Collect all R-D slopes
    // slope[i] = (distortion_reduction) / (bytes_added) for each pass
    var num_slopes: usize = 0;
    for (codeblocks) |cb| {
        num_slopes += cb.num_passes;
    }

    if (num_slopes == 0) {
        @memset(pass_counts, 0);
        return .{ .pass_counts = pass_counts, .total_bytes = 0 };
    }

    const slopes = try allocator.alloc(f64, num_slopes);
    defer allocator.free(slopes);

    var slope_count: usize = 0;
    for (codeblocks) |cb| {
        var prev_bytes: u32 = 0;
        var prev_dist: f64 = 0;
        var p: u16 = 0;
        while (p < cb.num_passes) : (p += 1) {
            const bytes = cb.pass_lengths[p];
            const dist = cb.pass_distortions[p];
            const delta_bytes: u32 = if (bytes >= prev_bytes) bytes - prev_bytes else 0;
            const delta_dist = dist - prev_dist;
            if (delta_bytes > 0) {
                slopes[slope_count] = delta_dist / @as(f64, @floatFromInt(delta_bytes));
                slope_count += 1;
            }
            prev_bytes = bytes;
            prev_dist = dist;
        }
    }

    if (slope_count == 0) {
        @memset(pass_counts, 0);
        return .{ .pass_counts = pass_counts, .total_bytes = 0 };
    }

    // Sort slopes to find the bisection range
    std.mem.sort(f64, slopes[0..slope_count], {}, lessThanF64);

    // Binary search for the optimal slope threshold (lambda).
    // Higher threshold => fewer passes included => fewer bytes.
    // We want the lowest threshold such that total_bytes <= target_bytes.
    var lo: f64 = 0;
    var hi: f64 = slopes[slope_count - 1] * 2 + 1;

    var iterations: u32 = 0;
    while (iterations < 64) : (iterations += 1) {
        const mid = (lo + hi) / 2;
        const result = computeTotalBytesForThreshold(codeblocks, pass_counts, mid);
        if (result <= target_bytes) {
            hi = mid;
        } else {
            lo = mid;
        }
        if (hi - lo < 1e-10) break;
    }

    // Use hi (the upper bound) to guarantee we stay within budget
    total = computeTotalBytesForThreshold(codeblocks, pass_counts, hi);

    return .{ .pass_counts = pass_counts, .total_bytes = total };
}

fn lessThanF64(_: void, a: f64, b: f64) bool {
    return a < b;
}

fn computeTotalBytesForThreshold(
    codeblocks: []const CodeblockRDInfo,
    pass_counts: []u16,
    threshold: f64,
) u64 {
    var total: u64 = 0;
    for (codeblocks, 0..) |cb, i| {
        var best_pass: u16 = 0;
        var prev_bytes: u32 = 0;
        var prev_dist: f64 = 0;
        var p: u16 = 0;
        while (p < cb.num_passes) : (p += 1) {
            const bytes = cb.pass_lengths[p];
            const dist = cb.pass_distortions[p];
            const delta_bytes: u32 = if (bytes >= prev_bytes) bytes - prev_bytes else 0;
            const delta_dist = dist - prev_dist;
            if (delta_bytes > 0) {
                const slope = delta_dist / @as(f64, @floatFromInt(delta_bytes));
                if (slope >= threshold) {
                    best_pass = p + 1;
                }
            }
            prev_bytes = bytes;
            prev_dist = dist;
        }
        pass_counts[i] = best_pass;
        if (best_pass > 0 and cb.pass_lengths.len > 0) {
            total += cb.pass_lengths[best_pass - 1];
        }
    }
    return total;
}

/// Estimate distortion reduction for a coding pass.
/// Simple MSE-based estimate: distortion = sum of squared coefficient magnitudes
/// at the current bit plane. Each bit plane contributes (1 << bp)^2 per significant coefficient.
pub fn estimatePassDistortion(
    coefficients: []const i32,
    width: usize,
    height: usize,
    pass_index: u16,
    bits_per_component: u8,
    zero_bit_planes: u8,
) f64 {
    _ = width;
    _ = height;
    // Compute the bit plane for this pass
    const first_bp: i32 = @as(i32, bits_per_component) - 1 - @as(i32, zero_bit_planes);
    // Each 3-pass group covers one bit plane
    const bp = first_bp - @as(i32, pass_index / 3);
    if (bp < 0) return 0;

    const bp_value: f64 = @floatFromInt(@as(u64, 1) << @intCast(bp));
    var dist: f64 = 0;
    for (coefficients) |coeff| {
        const mag = if (coeff < 0) -coeff else coeff;
        if (mag >= @as(i32, 1) << @intCast(bp)) {
            dist += bp_value * bp_value;
        }
    }
    return dist;
}

/// Fast bitplane-weighted distortion estimate for a codeblock's coding passes.
/// Each pass within a bitplane triplet (SPP / MRP / CUP) is assigned the same
/// bitplane weight (1 << bp)^2, which models the contribution a coefficient
/// becoming significant at that bitplane makes to the MSE. Caller owns the
/// returned slice.
pub fn bitplaneWeightedPassDistortions(
    allocator: std.mem.Allocator,
    num_coding_passes: u16,
    bits_per_component: u8,
    zero_bit_planes: u8,
) ![]f64 {
    const out = try allocator.alloc(f64, num_coding_passes);
    errdefer allocator.free(out);
    const first_bp: i32 = @as(i32, bits_per_component) - 1 - @as(i32, zero_bit_planes);
    var cumulative: f64 = 0;
    var p: u16 = 0;
    while (p < num_coding_passes) : (p += 1) {
        const bp = first_bp - @as(i32, @divFloor(p, 3));
        if (bp < 0) {
            out[p] = cumulative;
            continue;
        }
        const bp_value: f64 = @floatFromInt(@as(u64, 1) << @intCast(bp));
        cumulative += bp_value * bp_value;
        out[p] = cumulative;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "optimizeTruncation with generous target keeps all passes" {
    const allocator = std.testing.allocator;

    var lengths_0 = [_]u32{ 10, 25, 40 };
    var dists_0 = [_]f64{ 100.0, 180.0, 220.0 };
    var lengths_1 = [_]u32{ 5, 15, 30 };
    var dists_1 = [_]f64{ 50.0, 120.0, 160.0 };

    const codeblocks = [_]CodeblockRDInfo{
        .{ .pass_lengths = &lengths_0, .pass_distortions = &dists_0, .num_passes = 3 },
        .{ .pass_lengths = &lengths_1, .pass_distortions = &dists_1, .num_passes = 3 },
    };

    // Target 1000 bytes is well above total of 40 + 30 = 70
    var result = try optimizeTruncation(allocator, &codeblocks, 1000);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(u16, 3), result.pass_counts[0]);
    try std.testing.expectEqual(@as(u16, 3), result.pass_counts[1]);
    try std.testing.expectEqual(@as(u64, 70), result.total_bytes);
}

test "optimizeTruncation with tight target reduces pass counts" {
    const allocator = std.testing.allocator;

    var lengths_0 = [_]u32{ 10, 25, 40 };
    var dists_0 = [_]f64{ 100.0, 180.0, 220.0 };
    var lengths_1 = [_]u32{ 5, 15, 30 };
    var dists_1 = [_]f64{ 50.0, 120.0, 160.0 };

    const codeblocks = [_]CodeblockRDInfo{
        .{ .pass_lengths = &lengths_0, .pass_distortions = &dists_0, .num_passes = 3 },
        .{ .pass_lengths = &lengths_1, .pass_distortions = &dists_1, .num_passes = 3 },
    };

    // Target 20 bytes is well below total of 70
    var result = try optimizeTruncation(allocator, &codeblocks, 20);
    defer result.deinit(allocator);

    // With a tight budget, some passes must be dropped
    const total_passes = @as(u32, result.pass_counts[0]) + @as(u32, result.pass_counts[1]);
    try std.testing.expect(total_passes < 6);
    try std.testing.expect(result.total_bytes <= 20);
}

test "optimizeTruncation with zero target produces zero passes" {
    const allocator = std.testing.allocator;

    var lengths_0 = [_]u32{ 10, 25, 40 };
    var dists_0 = [_]f64{ 100.0, 180.0, 220.0 };

    const codeblocks = [_]CodeblockRDInfo{
        .{ .pass_lengths = &lengths_0, .pass_distortions = &dists_0, .num_passes = 3 },
    };

    var result = try optimizeTruncation(allocator, &codeblocks, 0);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(u16, 0), result.pass_counts[0]);
    try std.testing.expectEqual(@as(u64, 0), result.total_bytes);
}

test "estimatePassDistortion returns positive values for non-zero coefficients" {
    const coefficients = [_]i32{ 10, -5, 3, 0, -7, 2, 0, 1 };

    const dist = estimatePassDistortion(
        &coefficients,
        4,
        2,
        0, // first pass
        8,
        4, // zero_bit_planes
    );

    try std.testing.expect(dist > 0);
}
