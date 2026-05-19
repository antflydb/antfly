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

const VEC_LEN = 8;
const F32xN = @Vector(VEC_LEN, f32);

// ─── Learning Rate Schedules ───────────────────────────────────────────────────

pub const LearningRateSchedule = union(enum) {
    constant: f32,
    cosine: struct {
        initial_lr: f32,
        min_lr: f32,
        total_steps: u32,
    },
    warmup_cosine: struct {
        initial_lr: f32,
        min_lr: f32,
        warmup_steps: u32,
        total_steps: u32,
    },

    pub fn lr(self: LearningRateSchedule, step_num: u32) f32 {
        return switch (self) {
            .constant => |val| val,
            .cosine => |c| blk: {
                const progress: f32 = @as(f32, @floatFromInt(step_num)) / @as(f32, @floatFromInt(c.total_steps));
                const cosine_factor = 0.5 * (1.0 + @cos(std.math.pi * progress));
                break :blk c.min_lr + (c.initial_lr - c.min_lr) * cosine_factor;
            },
            .warmup_cosine => |wc| blk: {
                if (step_num < wc.warmup_steps) {
                    // Linear warmup: lr increases from 0 to initial_lr
                    const warmup_progress: f32 = @as(f32, @floatFromInt(step_num)) / @as(f32, @floatFromInt(wc.warmup_steps));
                    break :blk wc.initial_lr * warmup_progress;
                }
                // Cosine decay from initial_lr to min_lr over remaining steps
                const decay_steps = wc.total_steps - wc.warmup_steps;
                const decay_step = step_num - wc.warmup_steps;
                const progress: f32 = @as(f32, @floatFromInt(decay_step)) / @as(f32, @floatFromInt(decay_steps));
                const cosine_factor = 0.5 * (1.0 + @cos(std.math.pi * progress));
                break :blk wc.min_lr + (wc.initial_lr - wc.min_lr) * cosine_factor;
            },
        };
    }
};

// ─── Optimizer Configs ─────────────────────────────────────────────────────────

pub const SGDConfig = struct {
    momentum: f32 = 0.0,
};

pub const AdamConfig = struct {
    beta1: f32 = 0.9,
    beta2: f32 = 0.999,
    eps: f32 = 1e-8,
};

pub const AdamWConfig = struct {
    beta1: f32 = 0.9,
    beta2: f32 = 0.999,
    eps: f32 = 1e-8,
    weight_decay: f32 = 0.01,
};

pub const Optimizer = union(enum) {
    sgd: SGDConfig,
    adam: AdamConfig,
    adamw: AdamWConfig,
    schedule_free_adamw: AdamWConfig,
};

// ─── Per-Parameter State ───────────────────────────────────────────────────────

pub const ParamState = struct {
    m: []f32, // first moment (momentum / Adam m)
    v: []f32, // second moment (Adam v); empty slice for SGD without momentum
    z: ?[]f32 = null, // base iterate for Schedule-Free AdamW; null until first SF step
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, size: usize, needs_v: bool) !ParamState {
        const m = try allocator.alloc(f32, size);
        @memset(m, 0.0);

        const v: []f32 = if (needs_v) blk: {
            const buf = try allocator.alloc(f32, size);
            @memset(buf, 0.0);
            break :blk buf;
        } else &.{};

        return ParamState{
            .m = m,
            .v = v,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ParamState) void {
        self.allocator.free(self.m);
        if (self.v.len > 0) {
            self.allocator.free(self.v);
        }
        if (self.z) |z| {
            self.allocator.free(z);
        }
        self.* = undefined;
    }
};

pub const OptimizerState = struct {
    param_states: std.StringHashMapUnmanaged(ParamState),
    step_count: u32 = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) OptimizerState {
        return OptimizerState{
            .param_states = std.StringHashMapUnmanaged(ParamState){},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *OptimizerState) void {
        var it = self.param_states.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.param_states.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn getOrCreate(self: *OptimizerState, name: []const u8, size: usize, needs_v: bool) !*ParamState {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        const gop = try self.param_states.getOrPut(self.allocator, owned_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = try ParamState.init(self.allocator, size, needs_v);
        } else {
            self.allocator.free(owned_name);
        }
        return gop.value_ptr;
    }
};

// ─── Optimizer Step ────────────────────────────────────────────────────────────

pub fn step(
    config: Optimizer,
    state: *OptimizerState,
    current_lr: f32,
    name: []const u8,
    param: []f32,
    grad: []const f32,
) !void {
    std.debug.assert(param.len == grad.len);

    switch (config) {
        .sgd => |sgd| {
            const has_momentum = sgd.momentum != 0.0;
            const ps = try state.getOrCreate(name, param.len, false);
            stepSlices(config, state.step_count, current_lr, param, grad, if (has_momentum) ps.m else &.{}, &.{});
        },
        .adam => |adam| {
            _ = adam;
            const ps = try state.getOrCreate(name, param.len, true);
            stepSlices(config, state.step_count, current_lr, param, grad, ps.m, ps.v);
        },
        .adamw => |adamw| {
            _ = adamw;
            const ps = try state.getOrCreate(name, param.len, true);
            stepSlices(config, state.step_count, current_lr, param, grad, ps.m, ps.v);
        },
        .schedule_free_adamw => {
            const ps = try state.getOrCreate(name, param.len, true);
            // Lazily initialise the base iterate z to the current param values.
            if (ps.z == null) {
                const z_buf = try state.allocator.alloc(f32, param.len);
                @memcpy(z_buf, param);
                ps.z = z_buf;
            }
            stepScheduleFreeAdamW(state.step_count, current_lr, config.schedule_free_adamw, param, grad, ps.m, ps.v, ps.z.?);
        },
    }
}

pub fn stepSlices(
    config: Optimizer,
    step_count: u32,
    current_lr: f32,
    param: []f32,
    grad: []const f32,
    m: []f32,
    v: []f32,
) void {
    std.debug.assert(param.len == grad.len);

    switch (config) {
        .sgd => |sgd| {
            const has_momentum = sgd.momentum != 0.0;
            if (has_momentum) std.debug.assert(m.len == param.len);

            if (has_momentum) {
                for (param, grad, m) |*p, g, *m_item| {
                    m_item.* = sgd.momentum * m_item.* + g;
                    p.* -= current_lr * m_item.*;
                }
            } else {
                for (param, grad) |*p, g| {
                    p.* -= current_lr * g;
                }
            }
        },
        .adam => |adam| {
            std.debug.assert(m.len == param.len);
            std.debug.assert(v.len == param.len);
            const t: f32 = @floatFromInt(step_count);
            const bias_correction1 = 1.0 - std.math.pow(f32, adam.beta1, t);
            const bias_correction2 = 1.0 - std.math.pow(f32, adam.beta2, t);
            stepAdamLikeSlices(param, grad, m, v, .{
                .beta1 = adam.beta1,
                .beta2 = adam.beta2,
                .eps = adam.eps,
                .current_lr = current_lr,
                .bias_correction1 = bias_correction1,
                .bias_correction2 = bias_correction2,
                .weight_decay = 0.0,
                .use_weight_decay = false,
            });
        },
        .adamw => |adamw| {
            std.debug.assert(m.len == param.len);
            std.debug.assert(v.len == param.len);
            const t: f32 = @floatFromInt(step_count);
            const bias_correction1 = 1.0 - std.math.pow(f32, adamw.beta1, t);
            const bias_correction2 = 1.0 - std.math.pow(f32, adamw.beta2, t);
            stepAdamLikeSlices(param, grad, m, v, .{
                .beta1 = adamw.beta1,
                .beta2 = adamw.beta2,
                .eps = adamw.eps,
                .current_lr = current_lr,
                .bias_correction1 = bias_correction1,
                .bias_correction2 = bias_correction2,
                .weight_decay = adamw.weight_decay,
                .use_weight_decay = true,
            });
        },
        // schedule_free_adamw cannot be used via stepSlices (needs z slice); use step() instead.
        .schedule_free_adamw => unreachable,
    }
}

const AdamLikeStepConfig = struct {
    beta1: f32,
    beta2: f32,
    eps: f32,
    current_lr: f32,
    bias_correction1: f32,
    bias_correction2: f32,
    weight_decay: f32,
    use_weight_decay: bool,
};

fn stepAdamLikeSlices(
    param: []f32,
    grad: []const f32,
    m: []f32,
    v: []f32,
    cfg: AdamLikeStepConfig,
) void {
    const beta1_v: F32xN = @splat(cfg.beta1);
    const beta2_v: F32xN = @splat(cfg.beta2);
    const one_minus_beta1_v: F32xN = @splat(1.0 - cfg.beta1);
    const one_minus_beta2_v: F32xN = @splat(1.0 - cfg.beta2);
    const lr_v: F32xN = @splat(cfg.current_lr);
    const eps_v: F32xN = @splat(cfg.eps);
    const bias1_v: F32xN = @splat(cfg.bias_correction1);
    const bias2_v: F32xN = @splat(cfg.bias_correction2);
    const decay_v: F32xN = @splat(cfg.weight_decay * cfg.current_lr);

    var i: usize = 0;
    while (i + VEC_LEN <= param.len) : (i += VEC_LEN) {
        var pv: F32xN = param[i..][0..VEC_LEN].*;
        const gv: F32xN = grad[i..][0..VEC_LEN].*;
        var mv: F32xN = m[i..][0..VEC_LEN].*;
        var vv: F32xN = v[i..][0..VEC_LEN].*;

        if (cfg.use_weight_decay) {
            pv -= decay_v * pv;
        }

        mv = beta1_v * mv + one_minus_beta1_v * gv;
        vv = beta2_v * vv + one_minus_beta2_v * gv * gv;

        const m_hat = mv / bias1_v;
        const v_hat = vv / bias2_v;
        pv -= lr_v * m_hat / (@sqrt(v_hat) + eps_v);

        param[i..][0..VEC_LEN].* = pv;
        m[i..][0..VEC_LEN].* = mv;
        v[i..][0..VEC_LEN].* = vv;
    }

    while (i < param.len) : (i += 1) {
        if (cfg.use_weight_decay) {
            param[i] -= cfg.weight_decay * cfg.current_lr * param[i];
        }
        m[i] = cfg.beta1 * m[i] + (1.0 - cfg.beta1) * grad[i];
        v[i] = cfg.beta2 * v[i] + (1.0 - cfg.beta2) * grad[i] * grad[i];
        const m_hat = m[i] / cfg.bias_correction1;
        const v_hat = v[i] / cfg.bias_correction2;
        param[i] -= cfg.current_lr * m_hat / (@sqrt(v_hat) + cfg.eps);
    }
}

// ─── Schedule-Free AdamW (Defazio 2024) ───────────────────────────────────────
//
// Maintains a base iterate z (same shape as param) alongside the second-moment
// EMA v.  The update rule per step t is:
//
//   v  = beta2 * v + (1 - beta2) * g^2
//   v_hat = v / (1 - beta2^t)
//   z_new = z - lr * g / (sqrt(v_hat) + eps) - lr * weight_decay * z
//   c   = beta1  (Polyak mixing coefficient)
//   param = (c/t) * z_new + (1 - c/t) * param
//   z   = z_new
//
// `param` therefore tracks the Polyak average of the z iterates.  beta1 is
// reused as the mixing weight c (typical value: 0.9).

fn stepScheduleFreeAdamW(
    step_count: u32,
    current_lr: f32,
    cfg: AdamWConfig,
    param: []f32,
    grad: []const f32,
    m: []f32, // unused slot kept for uniform ParamState layout; treated as scratch
    v: []f32,
    z: []f32,
) void {
    _ = m; // Schedule-Free does not use a first-moment EMA
    std.debug.assert(param.len == grad.len);
    std.debug.assert(v.len == param.len);
    std.debug.assert(z.len == param.len);

    const t: f32 = @floatFromInt(step_count);
    const beta2_t = std.math.pow(f32, cfg.beta2, t);
    const bias_correction2 = 1.0 - beta2_t;
    // Mixing weight: Schedule-Free Polyak mix = min(beta1, 1/t).
    const mix = @min(cfg.beta1, 1.0 / t);

    var i: usize = 0;
    while (i + VEC_LEN <= param.len) : (i += VEC_LEN) {
        var pv: F32xN = param[i..][0..VEC_LEN].*;
        const gv: F32xN = grad[i..][0..VEC_LEN].*;
        var vv: F32xN = v[i..][0..VEC_LEN].*;
        const zv: F32xN = z[i..][0..VEC_LEN].*;

        vv = @as(F32xN, @splat(cfg.beta2)) * vv + @as(F32xN, @splat(1.0 - cfg.beta2)) * gv * gv;
        const v_hat = vv / @as(F32xN, @splat(bias_correction2));
        const z_new = zv - @as(F32xN, @splat(current_lr)) * gv / (@sqrt(v_hat) + @as(F32xN, @splat(cfg.eps))) - @as(F32xN, @splat(current_lr * cfg.weight_decay)) * zv;
        pv = @as(F32xN, @splat(mix)) * z_new + @as(F32xN, @splat(1.0 - mix)) * pv;

        param[i..][0..VEC_LEN].* = pv;
        v[i..][0..VEC_LEN].* = vv;
        z[i..][0..VEC_LEN].* = z_new;
    }

    while (i < param.len) : (i += 1) {
        v[i] = cfg.beta2 * v[i] + (1.0 - cfg.beta2) * grad[i] * grad[i];
        const v_hat = v[i] / bias_correction2;
        const z_new = z[i] - current_lr * grad[i] / (@sqrt(v_hat) + cfg.eps) - current_lr * cfg.weight_decay * z[i];
        param[i] = mix * z_new + (1.0 - mix) * param[i];
        z[i] = z_new;
    }
}

// ─── Gradient Clipping ─────────────────────────────────────────────────────────

pub const GradientClipConfig = union(enum) {
    none: void,
    max_norm: f32, // L2 norm clipping
    max_value: f32, // per-element clipping
};

/// Clip gradients across all parameters (modifies in place).
pub fn clipGradients(gradients: [][]f32, config: GradientClipConfig) void {
    switch (config) {
        .none => {},
        .max_norm => |max_norm| {
            // Compute global L2 norm across all params
            var total_norm_sq: f32 = 0.0;
            for (gradients) |grad| {
                for (grad) |g| {
                    total_norm_sq += g * g;
                }
            }
            const total_norm = @sqrt(total_norm_sq);

            // Scale if exceeds max_norm
            if (total_norm > max_norm) {
                const scale = max_norm / total_norm;
                for (gradients) |grad| {
                    for (grad) |*g| {
                        g.* *= scale;
                    }
                }
            }
        },
        .max_value => |max_val| {
            for (gradients) |grad| {
                for (grad) |*g| {
                    g.* = std.math.clamp(g.*, -max_val, max_val);
                }
            }
        },
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

const expectApproxEqAbs = std.testing.expectApproxEqAbs;

test "SGD step" {
    const allocator = std.testing.allocator;
    var state = OptimizerState.init(allocator);
    defer state.deinit();

    var param = [_]f32{ 1.0, 2.0, 3.0 };
    const grad = [_]f32{ 0.1, 0.2, 0.3 };
    const config = Optimizer{ .sgd = SGDConfig{} };
    const lr: f32 = 0.1;

    state.step_count = 1;
    try step(config, &state, lr, "p", &param, &grad);

    // param -= lr * grad => [1.0 - 0.01, 2.0 - 0.02, 3.0 - 0.03]
    try expectApproxEqAbs(0.99, param[0], 1e-6);
    try expectApproxEqAbs(1.98, param[1], 1e-6);
    try expectApproxEqAbs(2.97, param[2], 1e-6);
}

test "SGD with momentum" {
    const allocator = std.testing.allocator;
    var state = OptimizerState.init(allocator);
    defer state.deinit();

    var param = [_]f32{ 1.0, 2.0 };
    const grad = [_]f32{ 1.0, 1.0 };
    const config = Optimizer{ .sgd = SGDConfig{ .momentum = 0.9 } };
    const lr: f32 = 0.01;

    // Step 1: m = 0.9*0 + 1.0 = 1.0, param -= 0.01*1.0 = 0.99
    state.step_count = 1;
    try step(config, &state, lr, "p", &param, &grad);
    try expectApproxEqAbs(0.99, param[0], 1e-6);
    try expectApproxEqAbs(1.99, param[1], 1e-6);

    // Step 2: m = 0.9*1.0 + 1.0 = 1.9, param -= 0.01*1.9 = 0.99 - 0.019 = 0.971
    state.step_count = 2;
    try step(config, &state, lr, "p", &param, &grad);
    try expectApproxEqAbs(0.971, param[0], 1e-6);
    try expectApproxEqAbs(1.971, param[1], 1e-6);
}

test "Adam bias correction" {
    const allocator = std.testing.allocator;
    var state = OptimizerState.init(allocator);
    defer state.deinit();

    var param = [_]f32{1.0};
    const grad = [_]f32{1.0};
    const config = Optimizer{ .adam = AdamConfig{} };
    const lr: f32 = 0.001;

    // After step 1 with constant gradient of 1.0:
    //   m = 0.1*1.0 = 0.1,  bias_corr1 = 1-0.9 = 0.1,  m_hat = 1.0
    //   v = 0.001*1.0 = 0.001, bias_corr2 = 1-0.999 = 0.001, v_hat = 1.0
    //   update = lr * 1.0 / (sqrt(1.0) + 1e-8) ~ 0.001
    state.step_count = 1;
    try step(config, &state, lr, "p", &param, &grad);
    try expectApproxEqAbs(0.999, param[0], 1e-4);

    // After step 2, bias correction factors change
    const before = param[0];
    state.step_count = 2;
    try step(config, &state, lr, "p", &param, &grad);
    // Should still decrease
    try std.testing.expect(param[0] < before);
}

test "AdamW weight decay" {
    const allocator = std.testing.allocator;
    var state_adamw = OptimizerState.init(allocator);
    defer state_adamw.deinit();
    var state_adam = OptimizerState.init(allocator);
    defer state_adam.deinit();

    var param_adamw = [_]f32{5.0};
    var param_adam = [_]f32{5.0};
    // Zero gradient — only weight decay should affect AdamW params
    const zero_grad = [_]f32{0.0};
    // Use a non-zero gradient so Adam actually runs
    const grad = [_]f32{0.1};
    const lr: f32 = 0.01;

    const config_adamw = Optimizer{ .adamw = AdamWConfig{ .weight_decay = 0.1 } };
    const config_adam = Optimizer{ .adam = AdamConfig{} };

    // With zero gradient, Adam does nothing but AdamW still applies weight decay
    state_adamw.step_count = 1;
    try step(config_adamw, &state_adamw, lr, "p", &param_adamw, &zero_grad);
    // Weight decay: param -= 0.1 * 0.01 * 5.0 = 0.005 => 4.995
    try expectApproxEqAbs(4.995, param_adamw[0], 1e-4);

    // Adam with zero gradient stays the same
    state_adam.step_count = 1;
    try step(config_adam, &state_adam, lr, "a", &param_adam, &zero_grad);
    try expectApproxEqAbs(5.0, param_adam[0], 1e-6);

    // Now with a real gradient, AdamW params should be smaller due to weight decay
    _ = grad;
}

test "AdamW SIMD slice path matches scalar reference with tail" {
    var param_simd = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    var m_simd = [_]f32{ 0.0, 0.1, 0.2, 0.0, 0.1, 0.2, 0.0, 0.1, 0.2, 0.0, 0.1 };
    var v_simd = [_]f32{ 0.01, 0.02, 0.03, 0.01, 0.02, 0.03, 0.01, 0.02, 0.03, 0.01, 0.02 };
    const grad = [_]f32{ 0.5, -0.25, 0.75, -0.5, 0.25, -0.75, 0.5, -0.25, 0.75, -0.5, 0.25 };

    var param_ref = param_simd;
    var m_ref = m_simd;
    var v_ref = v_simd;

    const cfg = AdamWConfig{
        .beta1 = 0.9,
        .beta2 = 0.999,
        .eps = 1e-8,
        .weight_decay = 0.01,
    };
    const step_count: u32 = 3;
    const lr: f32 = 0.001;

    stepSlices(.{ .adamw = cfg }, step_count, lr, &param_simd, &grad, &m_simd, &v_simd);
    scalarAdamWReference(cfg, step_count, lr, &param_ref, &grad, &m_ref, &v_ref);

    for (param_simd, param_ref) |got, want| {
        try expectApproxEqAbs(want, got, 1e-6);
    }
    for (m_simd, m_ref) |got, want| {
        try expectApproxEqAbs(want, got, 1e-6);
    }
    for (v_simd, v_ref) |got, want| {
        try expectApproxEqAbs(want, got, 1e-6);
    }
}

test "cosine LR schedule" {
    const schedule = LearningRateSchedule{ .cosine = .{
        .initial_lr = 0.1,
        .min_lr = 0.01,
        .total_steps = 100,
    } };

    // Step 0: cos(0) = 1 => initial_lr
    try expectApproxEqAbs(0.1, schedule.lr(0), 1e-6);

    // Step 50 (midpoint): cos(pi/2) = 0 => (initial+min)/2
    try expectApproxEqAbs(0.055, schedule.lr(50), 1e-4);

    // Step 100 (end): cos(pi) = -1 => min_lr
    try expectApproxEqAbs(0.01, schedule.lr(100), 1e-6);
}

test "warmup cosine LR schedule" {
    const schedule = LearningRateSchedule{ .warmup_cosine = .{
        .initial_lr = 0.1,
        .min_lr = 0.01,
        .warmup_steps = 10,
        .total_steps = 110,
    } };

    // Step 0: warmup start
    try expectApproxEqAbs(0.0, schedule.lr(0), 1e-6);

    // Step 5: halfway through warmup => 0.05
    try expectApproxEqAbs(0.05, schedule.lr(5), 1e-6);

    // Step 10: end of warmup => initial_lr = 0.1, start of cosine at cos(0)
    try expectApproxEqAbs(0.1, schedule.lr(10), 1e-4);

    // Step 110: end => min_lr
    try expectApproxEqAbs(0.01, schedule.lr(110), 1e-6);
}

test "gradient clipping L2" {
    // Two gradient arrays: [3, 0] and [4, 0]
    // Global L2 norm = sqrt(9 + 16) = 5
    var g1 = [_]f32{ 3.0, 0.0 };
    var g2 = [_]f32{ 4.0, 0.0 };
    var gradients = [_][]f32{ &g1, &g2 };

    clipGradients(&gradients, .{ .max_norm = 2.5 });

    // Scale = 2.5 / 5.0 = 0.5
    try expectApproxEqAbs(1.5, g1[0], 1e-6);
    try expectApproxEqAbs(0.0, g1[1], 1e-6);
    try expectApproxEqAbs(2.0, g2[0], 1e-6);
    try expectApproxEqAbs(0.0, g2[1], 1e-6);

    // Verify new norm
    var norm_sq: f32 = 0.0;
    for (&gradients) |grad| {
        for (grad) |g| {
            norm_sq += g * g;
        }
    }
    try expectApproxEqAbs(2.5, @sqrt(norm_sq), 1e-5);
}

test "gradient clipping value" {
    var g1 = [_]f32{ -5.0, 3.0, 0.5, -0.1 };
    var gradients = [_][]f32{&g1};

    clipGradients(&gradients, .{ .max_value = 1.0 });

    try expectApproxEqAbs(-1.0, g1[0], 1e-6);
    try expectApproxEqAbs(1.0, g1[1], 1e-6);
    try expectApproxEqAbs(0.5, g1[2], 1e-6);
    try expectApproxEqAbs(-0.1, g1[3], 1e-6);
}

fn scalarAdamWReference(
    cfg: AdamWConfig,
    step_count: u32,
    current_lr: f32,
    param: []f32,
    grad: []const f32,
    m: []f32,
    v: []f32,
) void {
    const t: f32 = @floatFromInt(step_count);
    const bias_correction1 = 1.0 - std.math.pow(f32, cfg.beta1, t);
    const bias_correction2 = 1.0 - std.math.pow(f32, cfg.beta2, t);

    for (param, grad, m, v) |*p, g, *m_item, *v_item| {
        p.* -= cfg.weight_decay * current_lr * p.*;
        m_item.* = cfg.beta1 * m_item.* + (1.0 - cfg.beta1) * g;
        v_item.* = cfg.beta2 * v_item.* + (1.0 - cfg.beta2) * g * g;
        const m_hat = m_item.* / bias_correction1;
        const v_hat = v_item.* / bias_correction2;
        p.* -= current_lr * m_hat / (@sqrt(v_hat) + cfg.eps);
    }
}
