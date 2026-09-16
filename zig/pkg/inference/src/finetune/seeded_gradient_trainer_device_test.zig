// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resident optimizer ownership proof through the public Controller only.
//! The independent Torch gradients are inputs; this does not qualify complete
//! model backward, a GPU training job, or model convergence.
const std = @import("std");
const ml = @import("ml").graph;
const options = @import("build_options");
const controller = @import("seeded_gradient_trainer.zig");
const ops = @import("../ops/ops.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const device_fixture = @import("../graph/resident_training_fixture.zig");
const parity = @import("../architectures/gliner_boundary_parity_test.zig");
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const run_identity: [32]u8 = @splat(17);

const ExpectedParameter = struct { weight: []const f32, step: u32, exp_avg: []const f32, exp_avg_sq: []const f32 };
const Flush = struct { after_microbatch: u64, grad_norm: f64, parameters: std.json.ArrayHashMap(ExpectedParameter) };
const Microbatch = struct { gradients: std.json.ArrayHashMap(?[]const f32), flush: bool };
const Fixture = struct {
    format_version: u32,
    optimizer: struct { betas: [2]f32, eps: f32, gradient_accumulation_steps: u32, max_grad_norm: f32 },
    parameters: []const struct { name: []const u8, shape: []const i32, initial: []const f32, lr: f32, weight_decay: f32 },
    microbatches: []const Microbatch,
    flushes: []const Flush,
};

fn load(a: Allocator) !std.json.Parsed(Fixture) {
    const bytes = try parity.fixtureBytes(a, "training_adamw.json");
    defer a.free(bytes);
    if (bytes.len > mib) return error.InvalidOptimizerFixture;
    const parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    errdefer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 1), parsed.value.format_version);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.parameters.len);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.microbatches.len);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.flushes.len);
    return parsed;
}

fn create(a: Allocator, cb: *const ops.ComputeBackend, fixture: Fixture) !controller.Trainer {
    const parameters = try a.alloc(controller.Parameter, fixture.parameters.len);
    defer a.free(parameters);
    const groups = try a.alloc(controller.Group, fixture.parameters.len);
    defer a.free(groups);
    for (fixture.parameters, parameters, groups, 0..) |source, *parameter, *group, i| {
        parameter.* = .{ .name = source.name, .values = source.initial, .dimensions = source.shape, .group = i };
        group.* = .{ .optimizer = .{ .beta1 = fixture.optimizer.betas[0], .beta2 = fixture.optimizer.betas[1], .eps = fixture.optimizer.eps, .weight_decay = source.weight_decay }, .schedule = .{ .constant = source.lr } };
    }
    return controller.Trainer.init(a, cb, parameters, .{
        .execution = .resident_metal,
        .groups = groups,
        .grad_accum_steps = fixture.optimizer.gradient_accumulation_steps,
        .max_grad_norm = fixture.optimizer.max_grad_norm,
        .limits = .{ .max_state_bytes = 8 * mib, .max_transaction_bytes = 16 * mib, .max_checkpoint_header_bytes = 64 * 1024, .max_checkpoint_header_heap_bytes = mib },
    });
}

const Gradients = struct {
    allocator: Allocator,
    backend: *const ops.ComputeBackend,
    incoming: std.ArrayListUnmanaged(controller.ResidentGradient) = .empty,
    owned: std.ArrayListUnmanaged(ops.CT) = .empty,

    fn init(a: Allocator, cb: *const ops.ComputeBackend, fixture: Fixture, microbatch: Microbatch) !Gradients {
        var self = Gradients{ .allocator = a, .backend = cb };
        errdefer self.deinit();
        for (fixture.parameters) |parameter| {
            const maybe_values = microbatch.gradients.map.get(parameter.name) orelse continue;
            const values = maybe_values orelse continue; // Omitted means grad=None.
            try std.testing.expectEqual(parameter.initial.len, values.len);
            const all_zero = for (values) |value| {
                if (value != 0) break false;
            } else true;
            if (all_zero) {
                try self.incoming.append(a, .{ .name = parameter.name, .value = .zero });
            } else {
                const tensor = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = parameter.shape } }, .{});
                errdefer cb.free(tensor);
                try self.owned.append(a, tensor);
                // Ownership has moved to self.owned before the next fallible call.
                self.incoming.append(a, .{ .name = parameter.name, .value = .{ .tensor = tensor } }) catch |err| {
                    _ = self.owned.pop();
                    return err;
                };
            }
        }
        return self;
    }

    fn deinit(self: *Gradients) void {
        for (self.owned.items) |tensor| self.backend.free(tensor);
        self.owned.deinit(self.allocator);
        self.incoming.deinit(self.allocator);
        self.* = undefined;
    }
};

fn expectScalarTransfers(trainer: *const controller.Trainer, before: metal_tensor.MemoryStats) !void {
    const after = metal_tensor.memoryStatsSnapshot();
    // Scalar norm/finite summaries use strict dedicated readback, never a
    // generic host mirror or a downloaded gradient/parameter array.
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(before.to_host_calls, after.to_host_calls);
    const receipt = trainer.last_device_receipt orelse return error.MissingDeviceOptimizerReceipt;
    try std.testing.expect(receipt.selected_slots > 0);
    try std.testing.expectEqual(@as(usize, 0), receipt.scalar_upload_bytes % 4);
    try std.testing.expectEqual(@as(usize, 0), receipt.scalar_download_bytes % 12);
    try std.testing.expect(receipt.scalar_upload_bytes + receipt.scalar_download_bytes <= receipt.selected_slots * 144 + 64);
    try std.testing.expect(!trainer.host_mirrors_current);
}

fn expectFlush(trainer: *controller.Trainer, expected: Flush, result: controller.Result, index: usize) !void {
    try std.testing.expect(result.optimizer_stepped);
    try std.testing.expectEqual(expected.after_microbatch, result.identity.microbatch_step);
    try std.testing.expectEqual(@as(u64, @intCast(index + 1)), result.identity.optimizer_step);
    try std.testing.expectEqual(@as(u32, 0), result.accumulated_microbatches);
    // These are exactly the existing CPU fixture's tolerances.
    try std.testing.expectApproxEqAbs(expected.grad_norm, result.grad_norm, 5e-7);
    try trainer.ensureHostState(null); // Explicit diagnostic boundary only.
    try std.testing.expect(trainer.host_mirrors_current);
    for (trainer.owner.regular_params.items) |slot| {
        const want = expected.parameters.map.get(slot.name) orelse return error.InvalidOptimizerFixture;
        const state = trainer.owner.optimizer_state.param_states.get(slot.name).?;
        try std.testing.expectEqual(want.step, state.step_count);
        try std.testing.expectEqual(want.step, slot.adam_step_count);
        for (slot.grad_accum) |value| try std.testing.expectEqual(@as(f32, 0), value);
        for (want.weight, slot.weights) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 3e-7);
        for (want.exp_avg, state.m) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 1e-7 + @abs(value) * 1e-5);
        for (want.exp_avg_sq, state.v) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 1e-10 + @abs(value) * 2e-5);
    }
    for (trainer.present) |present| try std.testing.expect(!present);
}

const Accept = struct {
    fn apply(_: ?*const anyopaque, _: controller.Identity, _: u32) !void {}
};

fn replay(a: Allocator, cb: *const ops.ComputeBackend, fixture: Fixture, restore_partial: bool, path: []const u8) ![32]u8 {
    var trainer = try create(a, cb, fixture);
    defer trainer.deinit();
    var flush_index: usize = 0;
    for (fixture.microbatches) |microbatch| {
        var gradients = try Gradients.init(a, cb, fixture, microbatch);
        defer gradients.deinit();
        const before = metal_tensor.memoryStatsSnapshot();
        var result = try trainer.submitResident(trainer.identity(), 1, gradients.incoming.items, null);
        try expectScalarTransfers(&trainer, before);
        if (restore_partial and !result.optimizer_stepped) {
            try trainer.save(path, run_identity, null);
            try std.testing.expect(trainer.host_mirrors_current);
            const state_before = try trainer.stateFingerprint(run_identity, null);
            var restored = try create(a, cb, fixture);
            var owned = true;
            errdefer if (owned) restored.deinit();
            try restored.restoreValidated(path, run_identity, null, .{ .context = null, .validate = Accept.apply, .expected_state_sha256 = state_before });
            try std.testing.expect(restored.host_mirrors_current);
            try std.testing.expectEqual(trainer.identity(), restored.identity());
            try std.testing.expectEqual(trainer.owner.accum_count, restored.owner.accum_count);
            try std.testing.expectEqualSlices(bool, trainer.present, restored.present);
            const state_after = try restored.stateFingerprint(run_identity, null);
            try std.testing.expectEqualSlices(u8, &state_before, &state_after);
            const receipt = restored.last_restore_receipt orelse return error.MissingTrainingRestoreReceipt;
            try std.testing.expectEqualSlices(u8, &state_before, &receipt.state_sha256);
            const snapshot = try @import("../runtime/file_snapshot.zig").digest(std.testing.io, std.Io.Dir.cwd(), path, mib, null);
            try std.testing.expectEqual(snapshot.size_bytes, receipt.checkpoint.size_bytes);
            try std.testing.expectEqualSlices(u8, &snapshot.sha256, &receipt.checkpoint.sha256);
            for (trainer.owner.regular_params.items, restored.owner.regular_params.items) |previous, resumed| {
                try std.testing.expect(resumed.device != null);
                try std.testing.expectEqualSlices(f32, previous.weights, resumed.weights);
                try std.testing.expectEqualSlices(f32, previous.grad_accum, resumed.grad_accum);
                const old_state = trainer.owner.optimizer_state.param_states.get(previous.name).?;
                const new_state = restored.owner.optimizer_state.param_states.get(resumed.name).?;
                try std.testing.expectEqualSlices(f32, old_state.m, new_state.m);
                try std.testing.expectEqualSlices(f32, old_state.v, new_state.v);
            }
            trainer.deinit();
            trainer = restored;
            owned = false;
        }
        if (microbatch.flush and !result.optimizer_stepped) {
            const before_flush = metal_tensor.memoryStatsSnapshot();
            result = try trainer.flush(trainer.identity(), null);
            try expectScalarTransfers(&trainer, before_flush);
        }
        if (microbatch.flush) {
            try expectFlush(&trainer, fixture.flushes[flush_index], result, flush_index);
            flush_index += 1;
        } else try std.testing.expect(!result.optimizer_stepped);
    }
    try std.testing.expectEqual(fixture.flushes.len, flush_index);
    return trainer.stateFingerprint(run_identity, null);
}

test "seeded gradient trainer resident Metal matches pinned Torch AdamW and exact durable partial resume" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try load(a);
    defer fixture.deinit();
    var device = try device_fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/resident.safetensors", .{temporary.sub_path});
    defer a.free(path);
    const uninterrupted = try replay(a, &cb, fixture.value, false, path);
    const resumed = try replay(a, &cb, fixture.value, true, path);
    try std.testing.expectEqualSlices(u8, &uninterrupted, &resumed);
}

const Epoch = struct {
    identity: controller.Identity,
    accumulation: u32,
    present: [3]bool,
    tensors: [12]ops.CT,

    fn capture(trainer: *const controller.Trainer) Epoch {
        var self = Epoch{ .identity = trainer.identity(), .accumulation = trainer.owner.accum_count, .present = trainer.present[0..3].*, .tensors = undefined };
        for (trainer.owner.regular_params.items, 0..) |slot, i| {
            const device = slot.device.?;
            @memcpy(self.tensors[i * 4 ..][0..4], &[_]ops.CT{ device.weight, device.grad_accum, device.m, device.v });
        }
        return self;
    }

    fn expectUnchanged(self: Epoch, trainer: *const controller.Trainer) !void {
        const current = capture(trainer);
        try std.testing.expectEqual(self.identity, current.identity);
        try std.testing.expectEqual(self.accumulation, current.accumulation);
        try std.testing.expectEqualSlices(bool, &self.present, &current.present);
        try std.testing.expectEqualSlices(ops.CT, &self.tensors, &current.tensors);
    }
};

const Cancel = struct {
    fn apply(_: ?*anyopaque) !void {
        return error.Cancelled;
    }
};

test "seeded gradient trainer resident Metal failures preserve authoritative epoch and mirror freshness" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try load(a);
    defer fixture.deinit();
    var device = try device_fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var trainer = try create(a, &cb, fixture.value);
    defer trainer.deinit();
    var first = try Gradients.init(a, &cb, fixture.value, fixture.value.microbatches[0]);
    defer first.deinit();
    const initial = trainer.identity();
    _ = try trainer.submitResident(initial, 1, first.incoming.items, null);
    try std.testing.expect(!trainer.host_mirrors_current);
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/partial.safetensors", .{temporary.sub_path});
    defer a.free(path);
    try trainer.save(path, run_identity, null);
    const saved = try trainer.stateFingerprint(run_identity, null);
    var second = try Gradients.init(a, &cb, fixture.value, fixture.value.microbatches[1]);
    defer second.deinit();
    const first_step = try trainer.submitResident(trainer.identity(), 1, second.incoming.items, null);
    try std.testing.expect(!trainer.host_mirrors_current);
    const epoch = Epoch.capture(&trainer);
    try std.testing.expectError(error.TrainingTapeIdentityMismatch, trainer.submitResident(initial, 1, &.{}, null));
    try std.testing.expectError(error.Cancelled, trainer.submitResident(trainer.identity(), 1, first.incoming.items, .{ .check_fn = Cancel.apply }));
    try std.testing.expectError(error.TrainingStateFingerprintMismatch, trainer.restore(path, @splat(18), null));
    try std.testing.expectError(error.TrainingRestoreStateMismatch, trainer.restoreValidated(path, run_identity, null, .{ .context = null, .validate = Accept.apply, .expected_state_sha256 = @splat(0) }));
    try std.testing.expectError(error.Cancelled, trainer.restore(path, run_identity, .{ .check_fn = Cancel.apply }));
    try epoch.expectUnchanged(&trainer);
    try std.testing.expect(!trainer.host_mirrors_current);

    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    _ = try builder.parameter("encoder.weight", ml.Shape.init(.f32, &.{2}));
    {
        var bindings = try trainer.bind(&graph, null);
        defer bindings.deinit();
        try std.testing.expectEqual(@as(usize, 1), bindings.inputs.len);
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.submitResident(trainer.identity(), 1, first.incoming.items, null));
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.flush(trainer.identity(), null));
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.save(path, run_identity, null));
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.restore(path, run_identity, null));
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.ensureHostState(null));
        try epoch.expectUnchanged(&trainer);
    }

    const limits = trainer.limits;
    trainer.limits.max_transaction_bytes = 1;
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, trainer.ensureHostState(null));
    trainer.limits = limits;
    try std.testing.expect(!trainer.host_mirrors_current);
    try epoch.expectUnchanged(&trainer);

    // Cancel only after a real device-to-host copy has changed the first stale
    // mirror. Partial mirrors must never become certified or mutate the GPU.
    const MirrorCancel = struct {
        trainer: *controller.Trainer,
        old: f32,
        observed_partial_copy: bool = false,
        fn apply(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.trainer.owner.regular_params.items[0].weights[0] != self.old) {
                self.observed_partial_copy = true;
                return error.Cancelled;
            }
        }
    };
    var mirror_cancel = MirrorCancel{ .trainer = &trainer, .old = trainer.owner.regular_params.items[0].weights[0] };
    try std.testing.expectError(error.Cancelled, trainer.ensureHostState(.{ .ptr = &mirror_cancel, .check_fn = MirrorCancel.apply }));
    try std.testing.expect(mirror_cancel.observed_partial_copy);
    try std.testing.expect(!trainer.host_mirrors_current);
    try epoch.expectUnchanged(&trainer);
    try trainer.ensureHostState(null);
    try std.testing.expect(trainer.host_mirrors_current);
    try epoch.expectUnchanged(&trainer);
    const complete = try trainer.stateFingerprint(run_identity, null);
    try std.testing.expect(!std.mem.eql(u8, &saved, &complete));
    // All failure paths left the completed first update numerically intact.
    try expectFlush(&trainer, fixture.value.flushes[0], first_step, 0);
    try trainer.restoreValidated(path, run_identity, null, .{ .context = null, .validate = Accept.apply, .expected_state_sha256 = saved });
    try std.testing.expectEqualSlices(u8, &saved, &try trainer.stateFingerprint(run_identity, null));
}
