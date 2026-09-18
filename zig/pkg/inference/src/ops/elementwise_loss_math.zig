// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Backend arithmetic for already validated, reduced elementwise-loss cotangents.
//! Labels, masking, reductions and scalar objectives remain in the shared loss
//! implementation. The callback overwrites cotangents only after reading them.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Kind = enum(u32) { bce, asymmetric_focal, poisson_count };
pub const Settings = extern struct {
    kind: Kind = .bce,
    gamma_positive: f32 = 0,
    gamma_negative: f32 = 2,
    clip: f32 = 0.05,
    negative_weight: f32 = 1,
    positive_backward_power: f32 = -1,
    negative_backward_power: f32 = 1,
};
pub const Request = struct {
    logits: []const f32,
    targets: []const f32,
    /// Zero disables an element. Masked logits/targets may be nonfinite.
    cotangents: []f32,
    settings: Settings,
    max_elements: usize,
    control: ?Control = null,

    pub fn validate(self: Request) !void {
        if (self.logits.len != self.targets.len or self.logits.len != self.cotangents.len)
            return error.InvalidElementwiseLossMathShape;
        if (self.logits.len > self.max_elements or self.logits.len > std.math.maxInt(i32))
            return error.ElementwiseLossMathLimitExceeded;
        const s = self.settings;
        for ([_]f32{ s.gamma_positive, s.gamma_negative, s.clip, s.negative_weight, s.positive_backward_power, s.negative_backward_power }) |v|
            if (!std.math.isFinite(v)) return error.InvalidElementwiseLossMathSettings;
        if (s.gamma_positive < 0 or s.gamma_negative < 0 or s.clip < 0 or s.clip > 1 or s.negative_weight < 0)
            return error.InvalidElementwiseLossMathSettings;
        if (self.control) |control| try control.check();
        for (self.logits, self.targets, self.cotangents, 0..) |x, y, seed, i| {
            if (i % 4096 == 0) if (self.control) |control| try control.check();
            if (!std.math.isFinite(seed)) return error.NonFiniteElementwiseLossMath;
            if (seed != 0 and (!std.math.isFinite(x) or !std.math.isFinite(y) or y < 0 or (s.kind != .poisson_count and y > 1)))
                return error.NonFiniteElementwiseLossMath;
        }
    }
};
pub const Backend = struct {
    ptr: *anyopaque,
    apply: *const fn (*anyopaque, *const Request) anyerror!void,
};

test "elementwise loss math validates active payloads and limits before dispatch" {
    var seeds = [_]f32{ 1, 0 };
    var request = Request{ .logits = &.{ 0, std.math.nan(f32) }, .targets = &.{ 1, std.math.nan(f32) }, .cotangents = &seeds, .settings = .{}, .max_elements = 2 };
    try request.validate();
    seeds[1] = 1;
    try std.testing.expectError(error.NonFiniteElementwiseLossMath, request.validate());
    seeds[1] = 0;
    request.max_elements = 1;
    try std.testing.expectError(error.ElementwiseLossMathLimitExceeded, request.validate());
    request.max_elements = 2;
    request.settings.gamma_negative = -1;
    try std.testing.expectError(error.InvalidElementwiseLossMathSettings, request.validate());
    request.settings = .{};
    request.targets = &.{1};
    try std.testing.expectError(error.InvalidElementwiseLossMathShape, request.validate());
    request.targets = &.{ 5, 0 };
    try std.testing.expectError(error.NonFiniteElementwiseLossMath, request.validate());
    request.settings.kind = .poisson_count;
    try request.validate();
    request.targets = &.{ -1, 0 };
    try std.testing.expectError(error.NonFiniteElementwiseLossMath, request.validate());
}
