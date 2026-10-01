// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared loss code owns masking, maxima and reduced scalar cotangents. The
//! backend preserves CUDA logsumexp/VJP arithmetic and optional broadcast VJP.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Request = struct {
    logits: []const f32,
    /// Bit 0: valid candidate; bit 1: gold candidate, in input storage order.
    masks: []const i32,
    /// All-candidate maxima followed by gold maxima, each [B,Q].
    maxima: []const f32,
    seeds: []const f32,
    gradient: []f32,
    batch: usize,
    queries: usize,
    candidates: usize,
    candidate_major: bool = false,
    /// Return [B,C], differentiating a shared [B,C] input broadcast over Q.
    reduce_queries: bool = false,
    max_elements: usize,
    control: ?Control = null,

    pub fn validate(self: Request) !void {
        const rows = try std.math.mul(usize, self.batch, self.queries);
        const n = try std.math.mul(usize, rows, self.candidates);
        const out = if (self.reduce_queries) try std.math.mul(usize, self.batch, self.candidates) else n;
        const maxima_count = try std.math.mul(usize, rows, 2);
        if (n > self.max_elements or out > self.max_elements or n > std.math.maxInt(i32) or maxima_count > std.math.maxInt(i32))
            return error.ListwiseLossMathLimitExceeded;
        if (self.logits.len != n or self.masks.len != n or self.seeds.len != rows or
            self.maxima.len != maxima_count or self.gradient.len != out) return error.InvalidListwiseLossMathShape;
        if (self.control) |control| try control.check();
        for (self.seeds, 0..) |seed, row| {
            if (row % 4096 == 0) if (self.control) |control| try control.check();
            if (!std.math.isFinite(seed) or (seed != 0 and
                (!std.math.isFinite(self.maxima[row]) or !std.math.isFinite(self.maxima[rows + row])))) return error.NonFiniteListwiseLossMath;
        }
        for (self.logits, self.masks, 0..) |x, mask, i| {
            if (i % 4096 == 0) if (self.control) |control| try control.check();
            if (mask < 0 or mask > 3) return error.InvalidListwiseLossMathMask;
            const row = if (self.candidate_major) (i / (self.queries * self.candidates)) * self.queries + i % self.queries else i / self.candidates;
            if (self.seeds[row] != 0 and mask & 1 != 0 and !std.math.isFinite(x)) return error.NonFiniteListwiseLossMath;
        }
    }
};
pub const Backend = struct {
    ptr: *anyopaque,
    apply: *const fn (*anyopaque, *const Request) anyerror!void,
};

test "listwise loss math validates geometry active values masks and limits" {
    var out: [2]f32 = undefined;
    var request = Request{ .logits = &.{ 0, std.math.nan(f32) }, .masks = &.{ 3, 0 }, .maxima = &.{ 0, 0 }, .seeds = &.{1}, .gradient = &out, .batch = 1, .queries = 1, .candidates = 2, .max_elements = 2 };
    try request.validate();
    request.masks = &.{ 3, 1 };
    try std.testing.expectError(error.NonFiniteListwiseLossMath, request.validate());
    request.seeds = &.{0};
    try request.validate();
    request.masks = &.{ 3, 4 };
    try std.testing.expectError(error.InvalidListwiseLossMathMask, request.validate());
    request.max_elements = 1;
    try std.testing.expectError(error.ListwiseLossMathLimitExceeded, request.validate());
    request.max_elements = 2;
    request.queries = 2;
    try std.testing.expectError(error.ListwiseLossMathLimitExceeded, request.validate());
    request.max_elements = 4;
    try std.testing.expectError(error.InvalidListwiseLossMathShape, request.validate());
}
