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

//! Shared optimizer planning for DPO and GRPO policy updates.
//!
//! Preference objectives expand one logical training unit into multiple
//! backward graph executions (chosen/rejected for DPO and one per completion
//! for GRPO).  Keeping that expansion here prevents gradient accumulation,
//! LR schedules, and `max_steps` from silently acquiring different meanings
//! in each recipe implementation.

const std = @import("std");
const ml = @import("ml");

const optimizers = ml.graph.optimizers;

pub const Options = struct {
    learning_rate: f32 = 1e-4,
    weight_decay: f32 = 0.01,
    lr_scheduler: []const u8 = "constant",
    warmup_ratio: f32 = 0.0,
    warmup_steps: ?u32 = null,
    num_cycles: f32 = 1.0,
    max_steps: ?usize = null,
    epochs: usize = 1,
    micro_batch_size: usize = 1,
    gradient_accumulation_steps: u32 = 1,
    max_grad_norm: f32 = 1.0,
    schedule_free: bool = false,
    llrd_decay: ?f32 = null,
    lora_dropout: f32 = 0.0,
};

pub const Plan = struct {
    optimizer: optimizers.AdamWConfig,
    lr_schedule: optimizers.LearningRateSchedule,
    max_grad_norm: f32,
    lora_dropout: f32,
    /// Number of physical backward calls accumulated per optimizer update.
    physical_gradient_accumulation_steps: u32,
    /// Logical preference units to consume. `max_steps`, when present,
    /// overrides epochs and cycles the dataset until this exact limit.
    preference_units: usize,
    target_optimizer_steps: u32,
};

pub fn build(
    units_per_epoch: usize,
    objective_components: usize,
    options: Options,
) !Plan {
    if (units_per_epoch == 0 or objective_components == 0) return error.EmptyPreferenceTrainingPlan;
    if (!std.math.isFinite(options.learning_rate) or options.learning_rate <= 0.0) return error.InvalidLearningRate;
    if (!std.math.isFinite(options.weight_decay) or options.weight_decay < 0.0) return error.InvalidWeightDecay;
    if (!std.math.isFinite(options.max_grad_norm) or options.max_grad_norm < 0.0) return error.InvalidMaxGradNorm;
    if (!std.math.isFinite(options.warmup_ratio) or options.warmup_ratio < 0.0 or options.warmup_ratio > 1.0) return error.InvalidWarmupRatio;
    if (!std.math.isFinite(options.num_cycles) or options.num_cycles <= 0.0) return error.InvalidLrCycles;
    if (!std.math.isFinite(options.lora_dropout) or options.lora_dropout < 0.0 or options.lora_dropout >= 1.0) return error.InvalidLoRADropout;
    if (options.epochs == 0) return error.InvalidEpochCount;
    if (options.micro_batch_size != 1) return error.UnsupportedPreferenceMicroBatchSize;
    if (options.gradient_accumulation_steps == 0) return error.InvalidGradientAccumulation;
    if (options.schedule_free) return error.UnsupportedPreferenceScheduleFree;
    if (options.llrd_decay != null) return error.UnsupportedPreferenceLlrd;

    const component_count = std.math.cast(u32, objective_components) orelse return error.PreferenceTrainingPlanOverflow;
    const physical_accumulation = std.math.mul(
        u32,
        options.gradient_accumulation_steps,
        component_count,
    ) catch return error.PreferenceTrainingPlanOverflow;

    const preference_units, const target_steps_usize = if (options.max_steps) |max_steps| blk: {
        if (max_steps == 0) return error.InvalidMaxSteps;
        const units = std.math.mul(
            usize,
            max_steps,
            @as(usize, options.gradient_accumulation_steps),
        ) catch return error.PreferenceTrainingPlanOverflow;
        break :blk .{ units, max_steps };
    } else blk: {
        const units = std.math.mul(usize, units_per_epoch, options.epochs) catch
            return error.PreferenceTrainingPlanOverflow;
        const rounded = std.math.add(
            usize,
            units,
            @as(usize, options.gradient_accumulation_steps) - 1,
        ) catch return error.PreferenceTrainingPlanOverflow;
        break :blk .{ units, rounded / @as(usize, options.gradient_accumulation_steps) };
    };
    const target_steps = std.math.cast(u32, target_steps_usize) orelse return error.TooManyOptimizerSteps;
    if (target_steps == 0) return error.EmptyPreferenceTrainingPlan;

    const resolved_warmup_steps: u32 = if (options.warmup_steps) |explicit|
        explicit
    else
        @intFromFloat(@floor(@as(f64, @floatFromInt(target_steps)) * @as(f64, options.warmup_ratio)));
    if (resolved_warmup_steps > target_steps) return error.InvalidWarmupSteps;

    const schedule = try learningRateSchedule(
        options.lr_scheduler,
        options.learning_rate,
        resolved_warmup_steps,
        target_steps,
        options.num_cycles,
    );
    return .{
        .optimizer = .{ .weight_decay = options.weight_decay },
        .lr_schedule = schedule,
        .max_grad_norm = options.max_grad_norm,
        .lora_dropout = options.lora_dropout,
        .physical_gradient_accumulation_steps = physical_accumulation,
        .preference_units = preference_units,
        .target_optimizer_steps = target_steps,
    };
}

fn learningRateSchedule(
    name: []const u8,
    learning_rate: f32,
    warmup_steps: u32,
    total_steps: u32,
    num_cycles: f32,
) !optimizers.LearningRateSchedule {
    if (std.ascii.eqlIgnoreCase(name, "linear")) {
        return .{ .warmup_linear = .{
            .initial_lr = learning_rate,
            .warmup_steps = warmup_steps,
            .total_steps = total_steps,
        } };
    }
    if (std.ascii.eqlIgnoreCase(name, "cosine")) {
        return .{ .warmup_cosine = .{
            .initial_lr = learning_rate,
            .min_lr = 0.0,
            .warmup_steps = warmup_steps,
            .total_steps = total_steps,
        } };
    }
    if (std.ascii.eqlIgnoreCase(name, "cosine_restarts") or
        std.ascii.eqlIgnoreCase(name, "cosine-with-restarts") or
        std.ascii.eqlIgnoreCase(name, "cosine_with_restarts"))
    {
        return .{ .warmup_cosine_restarts = .{
            .initial_lr = learning_rate,
            .warmup_steps = warmup_steps,
            .total_steps = total_steps,
            .num_cycles = num_cycles,
        } };
    }
    if (std.ascii.eqlIgnoreCase(name, "constant") or
        std.ascii.eqlIgnoreCase(name, "constant_with_warmup") or
        std.ascii.eqlIgnoreCase(name, "constant-with-warmup"))
    {
        return .{ .warmup_constant = .{
            .initial_lr = learning_rate,
            .warmup_steps = warmup_steps,
            .total_steps = total_steps,
        } };
    }
    return error.UnsupportedPreferenceLrScheduler;
}

test "preference optimizer maps logical accumulation to physical backwards" {
    const plan = try build(5, 2, .{
        .epochs = 2,
        .gradient_accumulation_steps = 3,
        .lr_scheduler = "linear",
        .warmup_ratio = 0.25,
    });
    try std.testing.expectEqual(@as(u32, 6), plan.physical_gradient_accumulation_steps);
    try std.testing.expectEqual(@as(usize, 10), plan.preference_units);
    try std.testing.expectEqual(@as(u32, 4), plan.target_optimizer_steps);
    try std.testing.expectEqual(@as(u32, 1), plan.lr_schedule.warmup_linear.warmup_steps);
}

test "preference max steps overrides epochs and cycles whole accumulation windows" {
    const plan = try build(2, 4, .{
        .epochs = 99,
        .max_steps = 7,
        .gradient_accumulation_steps = 3,
    });
    try std.testing.expectEqual(@as(usize, 21), plan.preference_units);
    try std.testing.expectEqual(@as(u32, 7), plan.target_optimizer_steps);
    try std.testing.expectEqual(@as(u32, 12), plan.physical_gradient_accumulation_steps);
}

test "preference optimizer rejects silently unsupported recipe knobs" {
    try std.testing.expectError(error.UnsupportedPreferenceMicroBatchSize, build(1, 2, .{ .micro_batch_size = 2 }));
    try std.testing.expectError(error.UnsupportedPreferenceScheduleFree, build(1, 2, .{ .schedule_free = true }));
    try std.testing.expectError(error.UnsupportedPreferenceLlrd, build(1, 2, .{ .llrd_decay = 0.9 }));
    try std.testing.expectError(error.InvalidGradientAccumulation, build(1, 2, .{ .gradient_accumulation_steps = 0 }));
    try std.testing.expectError(error.InvalidLoRADropout, build(1, 2, .{ .lora_dropout = 1.0 }));
    try std.testing.expectError(error.InvalidWarmupSteps, build(1, 2, .{ .warmup_steps = 2 }));
}
