// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared warm-start contract. Only PageRank has a unique damped fixed point
//! independent of support in the seed. Spectral metrics cold-start: even a
//! normalized old vector can be orthogonal to a newly dominant component.
const std = @import("std");

pub fn supported(kind: anytype) bool {
    return kind == .pagerank;
}

pub const Mass = struct {
    sum: f64 = 0,
    correction: f64 = 0,

    pub fn add(self: *Mass, value: f64) !void {
        if (!std.math.isFinite(value) or value < 0) return error.InvalidGraphMetricWarmStart;
        const next = self.sum + value;
        self.correction += if (@abs(self.sum) >= value) (self.sum - next) + value else (value - next) + self.sum;
        self.sum = next;
    }

    pub fn total(self: Mass) !f64 {
        const result = self.sum + self.correction;
        if (!std.math.isFinite(result) or result < 0) return error.InvalidGraphMetricWarmStart;
        return result;
    }
};

pub fn normalized(value: f64, mass: f64, node_count: usize) !f64 {
    if (node_count == 0 or !std.math.isFinite(value) or value < 0 or !std.math.isFinite(mass) or mass < 0)
        return error.InvalidGraphMetricWarmStart;
    // A deleted/zero prior component is a cold start, never a zero vector.
    // Divide rather than multiplying by 1/mass, which can overflow for a
    // perfectly valid subnormal seed mass.
    return if (mass == 0) 1.0 / @as(f64, @floatFromInt(node_count)) else value / mass;
}

test "graph metric warm start mass handles removed nodes and subnormal seeds" {
    var mass = Mass{};
    try mass.add(0.5);
    try mass.add(0.5);
    try mass.add(0.25);
    try mass.add(0.25);
    try std.testing.expectEqual(@as(f64, 1.5), try mass.total());
    try std.testing.expectEqual(@as(f64, 0.25), try normalized(0, 0, 4));
    try std.testing.expectEqual(@as(f64, 1), try normalized(5e-324, 5e-324, 1));
    try std.testing.expectError(error.InvalidGraphMetricWarmStart, mass.add(-1));
}
