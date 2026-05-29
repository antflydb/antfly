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

// Training-health guards: loss canary, gradient-norm spike, and NaN/Inf detector.
//
// All three guards are EDGE-TRIGGERED: `observe` returns true only on the first
// step the condition trips, then latches silent until the caller resets state.

const std = @import("std");

pub const CanaryConfig = struct {
    baseline_loss: f32,
    max_ratio: f32 = 0.10,
    patience: u32 = 2,
};

pub const CanaryState = struct {
    baseline: f32,
    max_ratio: f32,
    patience: u32,
    bad_count: u32 = 0,
    tripped: bool = false,

    pub fn init(config: CanaryConfig) CanaryState {
        return .{
            .baseline = config.baseline_loss,
            .max_ratio = config.max_ratio,
            .patience = config.patience,
        };
    }

    pub fn observe(self: *CanaryState, current_loss: f32) bool {
        const threshold = self.baseline * (1.0 + self.max_ratio);
        if (current_loss > threshold) {
            self.bad_count += 1;
        } else {
            self.bad_count = 0;
        }
        if (!self.tripped and self.bad_count >= self.patience) {
            self.tripped = true;
            return true;
        }
        return false;
    }
};

pub const KLGuardConfig = struct {
    max_kl: f32 = 10.0,
    consecutive_steps: u32 = 3,
};

pub const KLGuardState = struct {
    max_kl: f32,
    consecutive_steps: u32,
    bad_count: u32 = 0,
    tripped: bool = false,

    pub fn init(config: KLGuardConfig) KLGuardState {
        return .{
            .max_kl = config.max_kl,
            .consecutive_steps = config.consecutive_steps,
        };
    }

    pub fn observe(self: *KLGuardState, kl: f32) bool {
        if (kl > self.max_kl) {
            self.bad_count += 1;
        } else {
            self.bad_count = 0;
        }
        if (!self.tripped and self.bad_count >= self.consecutive_steps) {
            self.tripped = true;
            return true;
        }
        return false;
    }
};

pub const NaNEvent = enum {
    none,
    grad_non_finite,
    loss_non_finite,
};

pub const NaNGuardConfig = struct {
    loss_patience: u32 = 1,
    grad_patience: u32 = 3,
};

pub const NaNGuardState = struct {
    cfg: NaNGuardConfig,
    bad_loss: u32 = 0,
    bad_grad: u32 = 0,
    rollback_requested: bool = false,

    pub fn init(config: NaNGuardConfig) NaNGuardState {
        return .{ .cfg = config };
    }

    pub fn observeLoss(self: *NaNGuardState, loss: f32) NaNEvent {
        if (isFinite(loss)) {
            self.bad_loss = 0;
            return .none;
        }
        self.bad_loss += 1;
        if (self.bad_loss >= self.cfg.loss_patience) {
            self.rollback_requested = true;
            return .loss_non_finite;
        }
        return .none;
    }

    pub fn observeGrad(self: *NaNGuardState, grads: []const f32) NaNEvent {
        if (firstNonFinite(grads) == null) {
            self.bad_grad = 0;
            return .none;
        }
        self.bad_grad += 1;
        if (self.bad_grad >= self.cfg.grad_patience) {
            self.rollback_requested = true;
            return .grad_non_finite;
        }
        return .none;
    }

    pub fn shouldRollback(self: *const NaNGuardState) bool {
        return self.rollback_requested;
    }

    pub fn clear(self: *NaNGuardState) void {
        self.rollback_requested = false;
        self.bad_loss = 0;
        self.bad_grad = 0;
    }
};

pub fn isFinite(x: f32) bool {
    return !std.math.isNan(x) and !std.math.isInf(x);
}

pub fn firstNonFinite(xs: []const f32) ?usize {
    for (xs, 0..) |v, i| {
        if (!isFinite(v)) return i;
    }
    return null;
}

test "canary edge trip and no re-trip" {
    var c = CanaryState.init(.{ .baseline_loss = 1.0, .max_ratio = 0.1, .patience = 2 });
    try std.testing.expectEqual(false, c.observe(1.05));
    try std.testing.expectEqual(@as(u32, 0), c.bad_count);
    try std.testing.expectEqual(false, c.observe(1.15));
    try std.testing.expectEqual(@as(u32, 1), c.bad_count);
    try std.testing.expectEqual(true, c.observe(1.20));
    try std.testing.expectEqual(@as(u32, 2), c.bad_count);
    try std.testing.expectEqual(true, c.tripped);
    try std.testing.expectEqual(false, c.observe(1.20));
}

test "canary reset on good value" {
    var c = CanaryState.init(.{ .baseline_loss = 1.0, .max_ratio = 0.1, .patience = 3 });
    _ = c.observe(1.5);
    _ = c.observe(1.5);
    try std.testing.expectEqual(@as(u32, 2), c.bad_count);
    _ = c.observe(1.0);
    try std.testing.expectEqual(@as(u32, 0), c.bad_count);
    try std.testing.expectEqual(false, c.tripped);
}

test "kl guard edge detection" {
    var k = KLGuardState.init(.{ .max_kl = 5.0, .consecutive_steps = 3 });
    try std.testing.expectEqual(false, k.observe(6.0));
    try std.testing.expectEqual(false, k.observe(7.0));
    try std.testing.expectEqual(true, k.observe(8.0));
    try std.testing.expectEqual(true, k.tripped);
    try std.testing.expectEqual(false, k.observe(9.0));
}

test "kl guard reset" {
    var k = KLGuardState.init(.{ .max_kl = 5.0, .consecutive_steps = 3 });
    _ = k.observe(6.0);
    _ = k.observe(6.0);
    _ = k.observe(1.0);
    try std.testing.expectEqual(@as(u32, 0), k.bad_count);
    try std.testing.expectEqual(false, k.tripped);
}

test "nan guard finite loss keeps clear" {
    var g = NaNGuardState.init(.{ .loss_patience = 1, .grad_patience = 3 });
    try std.testing.expectEqual(NaNEvent.none, g.observeLoss(0.5));
    try std.testing.expectEqual(false, g.shouldRollback());
    try std.testing.expectEqual(@as(u32, 0), g.bad_loss);
}

test "nan guard single nan loss triggers rollback with patience 1" {
    var g = NaNGuardState.init(.{ .loss_patience = 1, .grad_patience = 3 });
    const nan = std.math.nan(f32);
    try std.testing.expectEqual(NaNEvent.loss_non_finite, g.observeLoss(nan));
    try std.testing.expectEqual(true, g.shouldRollback());
}

test "nan guard grad patience 3 with two nans no rollback" {
    var g = NaNGuardState.init(.{ .loss_patience = 1, .grad_patience = 3 });
    const nan = std.math.nan(f32);
    const bad = [_]f32{ 0.1, nan, 0.2 };
    try std.testing.expectEqual(NaNEvent.none, g.observeGrad(&bad));
    try std.testing.expectEqual(NaNEvent.none, g.observeGrad(&bad));
    try std.testing.expectEqual(false, g.shouldRollback());
    try std.testing.expectEqual(NaNEvent.grad_non_finite, g.observeGrad(&bad));
    try std.testing.expectEqual(true, g.shouldRollback());
}

test "nan guard clear resets state" {
    var g = NaNGuardState.init(.{ .loss_patience = 1, .grad_patience = 3 });
    const nan = std.math.nan(f32);
    _ = g.observeLoss(nan);
    try std.testing.expectEqual(true, g.shouldRollback());
    g.clear();
    try std.testing.expectEqual(false, g.shouldRollback());
    try std.testing.expectEqual(@as(u32, 0), g.bad_loss);
    try std.testing.expectEqual(@as(u32, 0), g.bad_grad);
}

test "firstNonFinite indices" {
    const nan = std.math.nan(f32);
    const inf = std.math.inf(f32);
    const a = [_]f32{ nan, 1.0, 2.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };
    const c = [_]f32{ 1.0, 2.0, inf };
    try std.testing.expectEqual(@as(?usize, 0), firstNonFinite(&a));
    try std.testing.expectEqual(@as(?usize, null), firstNonFinite(&b));
    try std.testing.expectEqual(@as(?usize, 2), firstNonFinite(&c));
}

test "isFinite basic" {
    try std.testing.expect(isFinite(0.0));
    try std.testing.expect(isFinite(-1.5));
    try std.testing.expect(!isFinite(std.math.nan(f32)));
    try std.testing.expect(!isFinite(std.math.inf(f32)));
    try std.testing.expect(!isFinite(-std.math.inf(f32)));
}
