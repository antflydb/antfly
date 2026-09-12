// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable run settings and a replayable, bounded dataset order. Progress is
//! derived from the existing optimizer owner's durable counters: there is no
//! second cursor file that can commit before or after its checkpoint. This is a
//! native replay protocol, not a claim to reproduce PyTorch's RNG bitstream.
const std = @import("std");
const controller = @import("seeded_gradient_trainer.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Mode = enum { full, heads, lora, dora };
pub const Scheduler = enum { linear, cosine, cosine_restarts, constant };
pub const Config = struct {
    mode: Mode,
    epochs: u32 = 10,
    max_optimizer_steps: ?u32 = null,
    batch_size: u32 = 2,
    accumulation: u32 = 1,
    encoder_lr: f32 = 1e-5,
    task_lr: f32 = 5e-4,
    weight_decay: f32 = 0.01,
    beta1: f32 = 0.9,
    beta2: f32 = 0.999,
    epsilon: f32 = 1e-8,
    max_grad_norm: f32 = 1,
    scheduler: Scheduler = .linear,
    num_cycles: f32 = 0.5,
    warmup_steps: ?u32 = null,
    warmup_ratio: f64 = 0.1,
    seed: u64 = 42,
    shuffle: bool = true,
    /// Digest of the selected PEFT graph configuration (kind, rank, alpha,
    /// dropout, exact targets). Mandatory for either adapter training mode.
    adapter_config_sha256: ?[32]u8 = null,
};
pub const Data = struct {
    /// Hash of the immutable ordered training examples, annotations, and their
    /// occurrence policy. The dataset loader must verify the consumed bytes.
    train_sha256: [32]u8,
    schema_sha256: [32]u8,
    calibration_sha256: ?[32]u8 = null,
    test_sha256: ?[32]u8 = null,
    examples: u32,
};
pub const Limits = struct { max_examples: u32 = 16 * 1024 * 1024, max_order_bytes: usize = 64 * 1024 * 1024, max_batch_size: u32 = 1024, max_parameters: usize = 4096 };
pub const Position = struct {
    epoch: u64,
    batch: u32,
    /// Zero-based positions within that epoch's deterministic order.
    offset: u32,
    count: u32,
    /// End-of-epoch partial windows must flush before another example is read.
    requires_flush: bool,
    complete: bool,
};
pub const Parameter = struct {
    name: []const u8,
    /// Canonical upstream name, before the native encoder prefix rewrite.
    canonical_name: []const u8,
    dimensions: []const i32,
    values: []const f32,
    kind: enum { original, adapter },
};

pub const Plan = struct {
    config: Config,
    source: bundle.Identity,
    data: Data,
    limits: Limits,
    batches_per_epoch: u32,
    updates_per_epoch: u32,
    total_optimizer_steps: u32,
    warmup_steps: u32,
    groups: [2]controller.Group,

    pub fn init(config: Config, source: bundle.Identity, data: Data, limits: Limits) !Plan {
        if (source.precision != .fp32) return error.QuantizedBoundaryTrainingUnsupported;
        if (data.examples == 0 or data.examples > limits.max_examples or config.batch_size == 0 or config.batch_size > limits.max_batch_size or config.accumulation == 0 or config.accumulation > 65536 or config.epochs == 0) return error.InvalidBoundaryTrainingRun;
        const order_bytes = std.math.mul(usize, data.examples, @sizeOf(u32)) catch return error.BoundaryTrainingRunLimitExceeded;
        if (order_bytes > limits.max_order_bytes) return error.BoundaryTrainingRunLimitExceeded;
        for ([_]f64{ config.encoder_lr, config.task_lr, config.weight_decay, config.max_grad_norm, config.warmup_ratio, config.num_cycles }) |value| if (!std.math.isFinite(value) or value < 0) return error.InvalidBoundaryTrainingRun;
        if (config.warmup_ratio > 1 or !std.math.isFinite(config.epsilon) or config.epsilon <= 0) return error.InvalidBoundaryTrainingRun;
        for ([_]f32{ config.beta1, config.beta2 }) |value| if (!std.math.isFinite(value) or value < 0 or value >= 1) return error.InvalidBoundaryTrainingRun;
        const adapter = config.mode == .lora or config.mode == .dora;
        if (adapter != (config.adapter_config_sha256 != null)) return error.InvalidBoundaryTrainingRun;
        const batches = (data.examples - 1) / config.batch_size + 1;
        const updates = (batches - 1) / config.accumulation + 1;
        const total = config.max_optimizer_steps orelse (std.math.mul(u32, updates, config.epochs) catch return error.BoundaryTrainingRunLimitExceeded);
        if (total == 0) return error.InvalidBoundaryTrainingRun;
        const warmup = config.warmup_steps orelse @as(u32, @intFromFloat(@floor(@as(f64, @floatFromInt(total)) * config.warmup_ratio)));
        if (warmup > total) return error.InvalidBoundaryTrainingRun;
        const optimizer = @import("ml").graph.optimizers.AdamWConfig{ .beta1 = config.beta1, .beta2 = config.beta2, .eps = config.epsilon, .weight_decay = config.weight_decay };
        var groups: [2]controller.Group = undefined;
        for (&groups, [_]f32{ config.encoder_lr, config.task_lr }) |*group, rate| group.* = .{ .optimizer = optimizer, .schedule = switch (config.scheduler) {
            .linear => .{ .warmup_linear = .{ .initial_lr = rate, .warmup_steps = warmup, .total_steps = total } },
            .cosine => .{ .warmup_cosine = .{ .initial_lr = rate, .min_lr = 0, .warmup_steps = warmup, .total_steps = total } },
            .cosine_restarts => .{ .warmup_cosine_restarts = .{ .initial_lr = rate, .warmup_steps = warmup, .total_steps = total, .num_cycles = config.num_cycles } },
            .constant => .{ .warmup_constant = .{ .initial_lr = rate, .warmup_steps = warmup, .total_steps = total } },
        } };
        return .{ .config = config, .source = source, .data = data, .limits = limits, .batches_per_epoch = batches, .updates_per_epoch = updates, .total_optimizer_steps = total, .warmup_steps = warmup, .groups = groups };
    }

    pub fn optimizerConfig(self: *const Plan, limits: controller.Limits) controller.Config {
        return .{ .groups = if (self.config.mode == .lora or self.config.mode == .dora) self.groups[1..] else &self.groups, .grad_accum_steps = self.config.accumulation, .max_grad_norm = self.config.max_grad_norm, .limits = limits };
    }

    /// Select each named physical parameter once. Upstream normal-training
    /// groups use the substring "encoder", including boundary_encoder and
    /// candidate_encoder task modules. Head-only freezing, independently, only
    /// removes the outer model.encoder. Every group decays biases and norms.
    pub fn selectParameters(self: *const Plan, a: Allocator, parameters: []const Parameter) ![]controller.Parameter {
        if (parameters.len == 0 or parameters.len > self.limits.max_parameters) return error.InvalidBoundaryTrainingParameters;
        var selected = std.ArrayListUnmanaged(controller.Parameter).empty;
        errdefer selected.deinit(a);
        const adapter = self.config.mode == .lora or self.config.mode == .dora;
        for (parameters, 0..) |parameter, i| {
            if (parameter.name.len == 0 or parameter.name.len > 1024 or parameter.canonical_name.len == 0 or parameter.canonical_name.len > 1024) return error.InvalidBoundaryTrainingParameters;
            for (parameters[0..i]) |prior| if (std.mem.eql(u8, prior.name, parameter.name) or std.mem.eql(u8, prior.canonical_name, parameter.canonical_name)) return error.DuplicateTrainingParameter;
            const frozen_encoder = std.mem.startsWith(u8, parameter.canonical_name, "encoder.");
            const encoder = std.mem.indexOf(u8, parameter.canonical_name, "encoder") != null;
            const include = if (adapter) parameter.kind == .adapter else parameter.kind == .original and (self.config.mode == .full or !frozen_encoder);
            if (include) try selected.append(a, .{ .name = parameter.name, .values = parameter.values, .dimensions = parameter.dimensions, .group = if (adapter or encoder) 0 else 1 });
        }
        if (selected.items.len == 0) return error.EmptyBoundaryTrainingParameters;
        return selected.toOwnedSlice(a);
    }

    /// Bind run settings, all artifact bytes, data and schema to the existing
    /// controller checkpoint fingerprint. Paths and output directories do not
    /// participate, so a verified immutable run can move between machines.
    pub fn fingerprint(self: *const Plan, a: Allocator) ![32]u8 {
        const settings = try std.json.Stringify.valueAlloc(a, .{ .family = "gliner_boundary_training_run/v1", .source = self.source, .data = self.data, .config = self.config, .order = "sha256_splitmix64_fisher_yates_v1", .dropout = "boundary_counter_replay_v1", .dtype = "f32", .partial_window = "flush_each_epoch_actual_count", .total_optimizer_steps = self.total_optimizer_steps, .warmup_steps = self.warmup_steps }, .{});
        defer a.free(settings);
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(settings, &result, .{});
        return result;
    }

    pub fn position(self: *const Plan, state: controller.Identity, accumulated: u32) !Position {
        if (accumulated >= self.config.accumulation or accumulated > state.microbatch_step or state.optimizer_step > self.total_optimizer_steps) return error.InvalidBoundaryTrainingProgress;
        const epoch = state.microbatch_step / self.batches_per_epoch;
        const batch: u32 = @intCast(state.microbatch_step % self.batches_per_epoch);
        const epoch_updates = std.math.mul(u64, epoch, self.updates_per_epoch) catch return error.InvalidBoundaryTrainingProgress;
        var expected_updates = std.math.add(u64, epoch_updates, batch / self.config.accumulation) catch return error.InvalidBoundaryTrainingProgress;
        const pending_flush = batch == 0 and accumulated != 0;
        if (pending_flush) {
            if (epoch == 0 or accumulated != self.batches_per_epoch % self.config.accumulation) return error.InvalidBoundaryTrainingProgress;
            expected_updates -= 1;
        } else if (accumulated != batch % self.config.accumulation) return error.InvalidBoundaryTrainingProgress;
        if (state.optimizer_step != expected_updates) return error.InvalidBoundaryTrainingProgress;
        const complete = state.optimizer_step == self.total_optimizer_steps;
        if (complete and accumulated != 0) return error.InvalidBoundaryTrainingProgress;
        const offset = std.math.mul(u32, batch, self.config.batch_size) catch return error.InvalidBoundaryTrainingProgress;
        return .{ .epoch = epoch, .batch = batch, .offset = offset, .count = if (complete or pending_flush) 0 else @min(self.config.batch_size, self.data.examples - offset), .requires_flush = pending_flush, .complete = complete };
    }

    pub fn epochOrder(self: *const Plan, a: Allocator, epoch: u64, control: ?Control) ![]u32 {
        if (control) |active| try active.check();
        const order = try a.alloc(u32, self.data.examples);
        errdefer a.free(order);
        for (order, 0..) |*value, i| {
            if ((i & 4095) == 0) if (control) |active| try active.check();
            value.* = @intCast(i);
        }
        if (self.config.shuffle) {
            var rng = Random.init(self.config.seed, epoch, "epoch_order");
            var n = order.len;
            while (n > 1) {
                if ((n & 4095) == 0) if (control) |active| try active.check();
                const index: usize = @intCast(rng.bounded(n));
                n -= 1;
                std.mem.swap(u32, &order[n], &order[index]);
            }
        }
        if (control) |active| try active.check();
        return order;
    }
};

/// Versioned random stream used only for native data-order/decision replay.
/// Separate domains prevent batching changes from consuming dropout draws.
pub const Random = struct {
    state: u64,
    pub fn init(seed: u64, counter: u64, domain: []const u8) Random {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.boundary-run.random.v1\x00");
        var bytes: [16]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], seed, .little);
        std.mem.writeInt(u64, bytes[8..16], counter, .little);
        hash.update(&bytes);
        hash.update(domain);
        const digest = hash.finalResult();
        return .{ .state = std.mem.readInt(u64, digest[0..8], .little) };
    }
    pub fn next(self: *Random) u64 {
        self.state +%= 0x9e3779b97f4a7c15;
        var value = self.state;
        value = (value ^ (value >> 30)) *% 0xbf58476d1ce4e5b9;
        value = (value ^ (value >> 27)) *% 0x94d049bb133111eb;
        return value ^ (value >> 31);
    }
    pub fn bounded(self: *Random, bound: u64) u64 {
        std.debug.assert(bound != 0);
        const threshold = (0 -% bound) % bound;
        while (true) {
            const value = self.next();
            if (value >= threshold) return value % bound;
        }
    }
    pub fn uniform(self: *Random) f32 {
        return @as(f32, @floatFromInt(self.next() >> 40)) * 0x1p-24;
    }
};

fn testSource() bundle.Identity {
    return .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of("immutable test weights"), .sidecars = .{ bundle.Digest.of("model config"), bundle.Digest.of("encoder config"), bundle.Digest.of("tokenizer"), bundle.Digest.of("tokenizer config") } };
}

test "boundary training run preserves upstream encoder substring groups and explicit training modes" {
    const a = std.testing.allocator;
    const parameters = [_]Parameter{
        .{ .name = "embeddings.LayerNorm.bias", .canonical_name = "encoder.embeddings.LayerNorm.bias", .dimensions = &.{1}, .values = &.{0}, .kind = .original },
        .{ .name = "boundary_head.boundary_encoder.bos_state", .canonical_name = "boundary_head.boundary_encoder.bos_state", .dimensions = &.{1}, .values = &.{1}, .kind = .original },
        .{ .name = "boundary_head.candidate_encoder.bias", .canonical_name = "boundary_head.candidate_encoder.bias", .dimensions = &.{1}, .values = &.{0}, .kind = .original },
        .{ .name = "classifier.3.bias", .canonical_name = "classifier.3.bias", .dimensions = &.{1}, .values = &.{0}, .kind = .original },
        .{ .name = "base_model.model.encoder.query.lora_A.default.weight", .canonical_name = "base_model.model.encoder.query.lora_A.default.weight", .dimensions = &.{1}, .values = &.{0.5}, .kind = .adapter },
    };
    for ([_]Mode{ .full, .heads, .lora, .dora }) |mode| {
        const plan = try Plan.init(.{ .mode = mode, .adapter_config_sha256 = if (mode == .lora or mode == .dora) @splat(1) else null }, testSource(), .{ .examples = 11, .train_sha256 = @splat(2), .schema_sha256 = @splat(3) }, .{});
        const selected = try plan.selectParameters(a, &parameters);
        defer a.free(selected);
        const groups = switch (mode) {
            .full => &[_]usize{ 0, 0, 0, 1 },
            .heads => &[_]usize{ 0, 0, 1 },
            .lora, .dora => &[_]usize{0},
        };
        try std.testing.expectEqual(groups.len, selected.len);
        for (groups, selected) |group, parameter| try std.testing.expectEqual(group, parameter.group);
        try std.testing.expectEqual(@as(f32, 0.01), plan.groups[0].optimizer.weight_decay);
        try std.testing.expectEqual(@as(f32, 0.01), plan.groups[1].optimizer.weight_decay);
        const digest = try plan.fingerprint(a);
        var changed = plan;
        changed.data.schema_sha256[0] ^= 1;
        try std.testing.expect(!std.mem.eql(u8, &digest, &(try changed.fingerprint(a))));
        changed = plan;
        changed.config.seed += 1;
        try std.testing.expect(!std.mem.eql(u8, &digest, &(try changed.fingerprint(a))));
    }
}

test "boundary training run resumes ordered examples from optimizer counters across partial epoch flushes" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const plan = try Plan.init(.{ .mode = .heads, .epochs = 3, .batch_size = 2, .accumulation = 4 }, testSource(), .{ .examples = 11, .train_sha256 = @splat(2), .schema_sha256 = @splat(3) }, .{});
    try std.testing.expectEqual(@as(u32, 6), plan.total_optimizer_steps);
    const parameters = [_]controller.Parameter{.{ .name = "classifier.3.bias", .dimensions = &.{1}, .values = &.{0.5}, .group = 1 }};
    var trainer = try controller.Trainer.init(a, &cb, &parameters, plan.optimizerConfig(.{}));
    defer trainer.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/run.safetensors", .{temporary.sub_path});
    defer a.free(path);
    const digest = try plan.fingerprint(a);
    var seen = [_][11]u8{.{0} ** 11} ** 3;
    var flushes: usize = 0;
    while (true) {
        const position = try plan.position(trainer.identity(), trainer.owner.accum_count);
        if (position.complete) break;
        if (position.requires_flush) {
            try std.testing.expectEqual(@as(u32, 0), position.count);
            _ = try trainer.flush(trainer.identity(), null);
            flushes += 1;
            continue;
        }
        const order = try plan.epochOrder(a, position.epoch, null);
        defer a.free(order);
        for (order[position.offset..][0..position.count]) |index| seen[position.epoch][index] += 1;
        _ = try trainer.submit(trainer.identity(), 1, &.{.{ .name = "classifier.3.bias", .values = &.{0.1} }}, null);
        // Save both during an epoch and immediately before its partial flush.
        if (trainer.owner.step_count == 1 or trainer.owner.step_count == 6) {
            try trainer.save(path, digest, null);
            const before = try plan.position(trainer.identity(), trainer.owner.accum_count);
            try trainer.restore(path, digest, null);
            try std.testing.expectEqual(before, try plan.position(trainer.identity(), trainer.owner.accum_count));
            const replay = try plan.epochOrder(a, position.epoch, null);
            defer a.free(replay);
            try std.testing.expectEqualSlices(u32, order, replay);
        }
    }
    try std.testing.expectEqual(@as(usize, 3), flushes);
    try std.testing.expectEqual(@as(u64, 18), trainer.owner.step_count);
    for (seen) |epoch| try std.testing.expectEqualSlices(u8, &(.{1} ** 11), &epoch);
    try std.testing.expectError(error.InvalidBoundaryTrainingProgress, plan.position(.{ .optimizer_step = 0, .microbatch_step = 6 }, 2));
}

fn exerciseOrder(a: Allocator) !void {
    const plan = try Plan.init(.{ .mode = .full, .seed = 0x12345678 }, testSource(), .{ .examples = 17, .train_sha256 = @splat(2), .schema_sha256 = @splat(3) }, .{});
    const first = try plan.epochOrder(a, 1, null);
    defer a.free(first);
    const again = try plan.epochOrder(a, 1, null);
    defer a.free(again);
    const second = try plan.epochOrder(a, 2, null);
    defer a.free(second);
    try std.testing.expectEqualSlices(u32, first, again);
    try std.testing.expect(!std.mem.eql(u32, first, second));
    var seen = [_]bool{false} ** 17;
    for (first) |index| {
        try std.testing.expect(index < 17 and !seen[index]);
        seen[index] = true;
    }
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, plan.epochOrder(a, 2, .{ .check_fn = Cancel.check }));
    _ = try plan.fingerprint(a);
}

test "boundary training run bounds memory rejects quantized profiles and replays independent random streams" {
    try exerciseOrder(std.testing.allocator);
    var source = testSource();
    source.precision = .q8_0;
    const data = Data{ .examples = 17, .train_sha256 = @splat(2), .schema_sha256 = @splat(3) };
    try std.testing.expectError(error.QuantizedBoundaryTrainingUnsupported, Plan.init(.{ .mode = .full }, source, data, .{}));
    try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, Plan.init(.{ .mode = .full }, testSource(), data, .{ .max_order_bytes = 4 }));
    try std.testing.expectError(error.InvalidBoundaryTrainingRun, Plan.init(.{ .mode = .dora }, testSource(), data, .{}));
    var order = Random.init(7, 9, "epoch_order");
    var injection = Random.init(7, 9, "gold_injection");
    try std.testing.expect(order.next() != injection.next());
    for (0..1000) |_| {
        try std.testing.expect(order.bounded(17) < 17);
        const draw = injection.uniform();
        try std.testing.expect(draw >= 0 and draw < 1);
    }
}

test "boundary training run allocation failures reclaim order and fingerprint buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseOrder, .{});
}

test "boundary training run selected profiles match exact pinned optimizer group membership" {
    const a = std.testing.allocator;
    const Fixture = struct {
        parameter_names: []const []const u8,
        cases: []const struct {
            id: []const u8,
            additional_parameter_names: []const []const u8,
            trainable: []const bool,
            expected: ?struct { groups: []const struct { params: []const []const u8, lr: f32, weight_decay: f32 } } = null,
        },
    };
    const bytes = try @import("../util/c_file.zig").readFileMax(a, "testdata/gliner25/training_optimizer_groups.json", 1024 * 1024);
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 334), parsed.value.parameter_names.len);
    var seen: [4]bool = @splat(false);
    for (parsed.value.cases) |case| {
        const mode: Mode = if (std.mem.eql(u8, case.id, "full_cpu")) .full else if (std.mem.eql(u8, case.id, "head_only_cpu")) .heads else if (std.mem.eql(u8, case.id, "lora_cpu")) .lora else if (std.mem.eql(u8, case.id, "dora_cpu")) .dora else return error.InvalidOptimizerGroupFixture;
        try std.testing.expect(!seen[@intFromEnum(mode)]);
        seen[@intFromEnum(mode)] = true;
        const names = try a.alloc([]const u8, try std.math.add(usize, parsed.value.parameter_names.len, case.additional_parameter_names.len));
        defer a.free(names);
        @memcpy(names[0..parsed.value.parameter_names.len], parsed.value.parameter_names);
        @memcpy(names[parsed.value.parameter_names.len..], case.additional_parameter_names);
        try std.testing.expectEqual(names.len, case.trainable.len);
        const parameters = try a.alloc(Parameter, names.len);
        defer a.free(parameters);
        for (parameters, names) |*parameter, name| parameter.* = .{ .name = name, .canonical_name = name, .dimensions = &.{1}, .values = &.{0}, .kind = if (std.mem.indexOf(u8, name, ".lora_")) |_| .adapter else .original };
        const plan = try Plan.init(.{ .mode = mode, .adapter_config_sha256 = if (mode == .lora or mode == .dora) @splat(1) else null }, testSource(), .{ .examples = 11, .train_sha256 = @splat(2), .schema_sha256 = @splat(3) }, .{});
        const selected = try plan.selectParameters(a, parameters);
        defer a.free(selected);
        const groups = plan.optimizerConfig(.{}).groups;
        const expected = case.expected.?;
        try std.testing.expectEqual(expected.groups.len, groups.len);
        var total: usize = 0;
        for (expected.groups, groups, 0..) |want, group, group_id| {
            var actual_names = std.ArrayListUnmanaged([]const u8).empty;
            defer actual_names.deinit(a);
            for (selected) |parameter| if (parameter.group == group_id) try actual_names.append(a, parameter.name);
            try std.testing.expectEqual(want.params.len, actual_names.items.len);
            for (want.params, actual_names.items) |name, actual| try std.testing.expectEqualStrings(name, actual);
            total += want.params.len;
            try std.testing.expectEqual(want.lr, group.schedule.lr(plan.warmup_steps));
            try std.testing.expectEqual(want.weight_decay, group.optimizer.weight_decay);
        }
        try std.testing.expectEqual(total, selected.len);
        for (names, case.trainable) |name, trainable| {
            var present = false;
            for (selected) |parameter| if (std.mem.eql(u8, parameter.name, name)) {
                present = true;
                break;
            };
            try std.testing.expectEqual(trainable, present);
        }
    }
    for (seen) |present| try std.testing.expect(present);
}
