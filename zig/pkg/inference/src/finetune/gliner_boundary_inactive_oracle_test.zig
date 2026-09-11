// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Independent pinned Trainer._backward_one/AdamW control oracle. Source VJPs
//! are inputs here; complete native forward/backward is tested separately.
//! Explicit diagnostic mirror reads never occur inside a measured update.
const std = @import("std");
const controller = @import("seeded_gradient_trainer.zig");
const step = @import("gliner_boundary_train_step.zig");
const helper = @import("gliner_boundary_train_step_test.zig");
const parity = @import("../architectures/gliner_boundary_parity_test.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const device = @import("../graph/resident_training_fixture.zig");
const metal = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
pub const Names = std.json.ArrayHashMap(?[]const u8);
const Parameter = struct { name: []const u8, shape: []const i32 };
const ParameterState = struct { weight: []const u8, state_present: bool, step: u32, exp_avg: ?[]const u8, exp_avg_sq: ?[]const u8 };
pub const Flush = struct {
    after_microbatch: u64,
    microbatches: u32,
    partial_renormalization: f32,
    global_step: u64,
    scheduler_last_epoch: u64,
    grad_norm: f64,
    parameters: std.json.ArrayHashMap(ParameterState),
};
pub const Microbatch = struct {
    case: []const u8,
    index: u64,
    global_step_before: u64,
    window_after_backward: u32,
    model_loss: f32,
    model_loss_requires_grad: bool,
    reported_loss: f32,
    fallback: bool,
    gradients_scaled: Names,
    gradients_unscaled: Names,
    accumulated_gradients: Names,
    inputs: Names,
    losses: std.json.ArrayHashMap(f32),
};
pub const Profile = struct {
    id: []const u8,
    parameters: []const Parameter,
    initial: Names,
    sequence: []const []const u8,
    targets: []const []const u8,
    flush_after: []const u64,
    microbatches: []const Microbatch,
    flushes: []const Flush,
    frozen_parameters_unchanged: bool,
    fresh_owner_mid_window_resume_exact: ?bool,
};
pub const Optimizer = struct { accumulation_steps: u32, max_grad_norm: f32, betas: [2]f32, eps: f32, task_lr: f32, weight_decay: f32 };
const Fixture = struct { version: u32, qualification: bool, scope: []const u8, source_commit: []const u8, optimizer: Optimizer, profiles: []const Profile };

pub fn expectDigest(bytes: []const u8, hex: []const u8) !void {
    // Bundle digests are canonical 64-byte lowercase hexadecimal. Controller
    // run identities, independently, use raw 32-byte SHA256 values.
    try std.testing.expectEqualStrings(hex, &bundle.Digest.of(bytes).sha256);
}

pub fn optionalTensor(fixture: *const parity.TensorFixture, names: Names, name: []const u8) !?[]const f32 {
    const entry = names.map.get(name) orelse return error.InvalidInactiveTrainingFixture;
    return if (entry) |key| try fixture.floats(key) else null;
}

fn values(fixture: *const parity.TensorFixture, names: Names, name: []const u8) ![]const f32 {
    return try optionalTensor(fixture, names, name) orelse error.InvalidInactiveTrainingFixture;
}

fn create(a: Allocator, cb: *const ops.ComputeBackend, fixture: *const parity.TensorFixture, profile: Profile, optimizer: Optimizer, execution: controller.Execution) !controller.Trainer {
    const parameters = try a.alloc(controller.Parameter, profile.parameters.len);
    defer a.free(parameters);
    for (profile.parameters, parameters) |source, *target| target.* = .{ .name = source.name, .dimensions = source.shape, .values = try values(fixture, profile.initial, source.name), .group = 0 };
    return controller.Trainer.init(a, cb, parameters, .{
        .execution = execution,
        .groups = &.{.{ .optimizer = .{ .beta1 = optimizer.betas[0], .beta2 = optimizer.betas[1], .eps = optimizer.eps, .weight_decay = optimizer.weight_decay }, .schedule = .{ .constant = optimizer.task_lr } }},
        .grad_accum_steps = optimizer.accumulation_steps,
        .max_grad_norm = optimizer.max_grad_norm,
        .limits = .{ .max_state_bytes = 8 * mib, .max_transaction_bytes = 16 * mib, .max_checkpoint_header_bytes = mib, .max_checkpoint_header_heap_bytes = 8 * mib },
    });
}

const Gradients = struct {
    allocator: Allocator,
    cb: *const ops.ComputeBackend,
    host: std.ArrayListUnmanaged(controller.Gradient) = .empty,
    resident: std.ArrayListUnmanaged(controller.ResidentGradient) = .empty,
    owned: std.ArrayListUnmanaged(ops.CT) = .empty,

    fn init(a: Allocator, cb: *const ops.ComputeBackend, fixture: *const parity.TensorFixture, profile: Profile, micro: Microbatch, execution: controller.Execution) !Gradients {
        var self = Gradients{ .allocator = a, .cb = cb };
        errdefer self.deinit();
        const inactive = std.mem.eql(u8, micro.case, "inactive");
        const only_classifier = std.mem.endsWith(u8, profile.id, ".classifier_only");
        const fallback = inactive and only_classifier;
        try std.testing.expectEqual(fallback, micro.fallback);
        try std.testing.expectEqual(!fallback, micro.model_loss_requires_grad);
        try std.testing.expect(micro.model_loss > 0);
        try std.testing.expectEqual(if (fallback) @as(f32, 0) else micro.model_loss, micro.reported_loss);
        for (profile.parameters) |parameter| {
            const gradient = try optionalTensor(fixture, micro.gradients_unscaled, parameter.name);
            const scaled = try optionalTensor(fixture, micro.gradients_scaled, parameter.name);
            const canonical = step.canonicalParameterName(parameter.name);
            const absent = inactive and !only_classifier and std.mem.startsWith(u8, canonical, "classifier.");
            try std.testing.expectEqual(absent, gradient == null);
            try std.testing.expectEqual(absent, scaled == null);
            if (gradient) |input| {
                try std.testing.expectEqual(input.len, scaled.?.len);
                for (input, scaled.?) |unscaled, before| try std.testing.expectEqual(unscaled, before * 2);
                const touched = step.isTouchParameter(parameter.name, helper.config().head);
                if (inactive and (only_classifier or touched)) for (input) |value| try std.testing.expectEqual(@as(f32, 0), value);
                try self.host.append(a, .{ .name = parameter.name, .values = input });
                if (execution == .resident_metal) {
                    const zero = for (input) |value| {
                        if (value != 0) break false;
                    } else true;
                    if (zero) {
                        try self.resident.append(a, .{ .name = parameter.name, .value = .zero });
                    } else {
                        const tensor = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = input, .shape = parameter.shape } }, .{});
                        self.owned.append(a, tensor) catch |err| {
                            cb.free(tensor);
                            return err;
                        };
                        try self.resident.append(a, .{ .name = parameter.name, .value = .{ .tensor = tensor } });
                    }
                }
            }
        }
        return self;
    }

    fn deinit(self: *Gradients) void {
        for (self.owned.items) |tensor| self.cb.free(tensor);
        self.owned.deinit(self.allocator);
        self.resident.deinit(self.allocator);
        self.host.deinit(self.allocator);
        self.* = undefined;
    }
};

fn scalarOnly(owner: *const controller.Trainer, before: metal_tensor.MemoryStats) !void {
    if (owner.execution == .native) return;
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(before.to_host_calls, after.to_host_calls);
    const receipt = owner.last_device_receipt orelse return error.MissingDeviceOptimizerReceipt;
    try std.testing.expect(receipt.selected_slots > 0);
    try std.testing.expect(receipt.scalar_upload_bytes + receipt.scalar_download_bytes <= receipt.selected_slots * 144 + 64);
    try std.testing.expect(!owner.host_mirrors_current);
}

fn expectFlush(owner: *controller.Trainer, fixture: *const parity.TensorFixture, expected: Flush, result: controller.Result) !void {
    try std.testing.expect(result.optimizer_stepped);
    try std.testing.expectEqual(expected.after_microbatch, result.identity.microbatch_step);
    try std.testing.expectEqual(expected.global_step, result.identity.optimizer_step);
    try std.testing.expectEqual(expected.global_step, expected.scheduler_last_epoch);
    try std.testing.expectEqual(@as(u32, 0), result.accumulated_microbatches);
    // Same optimizer tolerances used by the original CPU/Metal Torch fixture.
    try std.testing.expectApproxEqAbs(expected.grad_norm, result.grad_norm, 5e-7);
    try owner.ensureHostState(null);
    for (owner.owner.regular_params.items, owner.present) |slot, present| {
        try std.testing.expect(!present);
        const want = expected.parameters.map.get(slot.name) orelse return error.InvalidInactiveTrainingFixture;
        const state = owner.owner.optimizer_state.param_states.get(slot.name).?;
        try std.testing.expectEqual(want.step, slot.adam_step_count);
        try std.testing.expectEqual(want.step, state.step_count);
        try std.testing.expectEqual(want.state_present, want.step > 0);
        try parity.expectFloats(try fixture.floats(want.weight), slot.weights, 3e-7, 0);
        if (want.exp_avg) |name| try parity.expectFloats(try fixture.floats(name), state.m, 1e-7, 1e-5) else for (state.m) |value| try std.testing.expectEqual(@as(f32, 0), value);
        if (want.exp_avg_sq) |name| try parity.expectFloats(try fixture.floats(name), state.v, 1e-10, 2e-5) else for (state.v) |value| try std.testing.expectEqual(@as(f32, 0), value);
        for (slot.grad_accum) |value| try std.testing.expectEqual(@as(f32, 0), value);
    }
}

fn replay(a: Allocator, cb: *const ops.ComputeBackend, fixture: *const parity.TensorFixture, profile: Profile, optimizer: Optimizer, execution: controller.Execution, restore_partial: bool, path: []const u8) ![32]u8 {
    var identity: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(profile.id, &identity, .{});
    var owner = try create(a, cb, fixture, profile, optimizer, execution);
    defer owner.deinit();
    var flush_index: usize = 0;
    for (profile.microbatches) |micro| {
        try std.testing.expectEqual(micro.global_step_before, owner.identity().optimizer_step);
        try std.testing.expectEqual(micro.index, owner.identity().microbatch_step);
        var gradients = try Gradients.init(a, cb, fixture, profile, micro, execution);
        defer gradients.deinit();
        const before = metal_tensor.memoryStatsSnapshot();
        var result = if (execution == .native)
            try owner.submit(owner.identity(), micro.reported_loss, gradients.host.items, null)
        else
            try owner.submitResident(owner.identity(), micro.reported_loss, gradients.resident.items, null);
        try scalarOnly(&owner, before);
        try std.testing.expectEqual(micro.reported_loss, result.loss.?);
        const flush = std.mem.indexOfScalar(u64, profile.flush_after, micro.index + 1) != null;
        if (!result.optimizer_stepped) {
            try std.testing.expectEqual(micro.window_after_backward, owner.owner.accum_count);
            try owner.ensureHostState(null); // Explicit fixture diagnostic.
            for (owner.owner.regular_params.items, owner.present) |slot, present| {
                const want = try optionalTensor(fixture, micro.accumulated_gradients, slot.name);
                try std.testing.expectEqual(want != null, present);
                if (want) |gradient| try parity.expectFloats(gradient, slot.grad_accum, 1e-7, 1e-5) else for (slot.grad_accum) |value| try std.testing.expectEqual(@as(f32, 0), value);
            }
            if (restore_partial) {
                const state = try owner.stateFingerprint(identity, null);
                try owner.save(path, identity, null);
                var restored = try create(a, cb, fixture, profile, optimizer, execution);
                var owned = true;
                errdefer if (owned) restored.deinit();
                try restored.restore(path, identity, null);
                try std.testing.expectEqual(state, try restored.stateFingerprint(identity, null));
                owner.deinit();
                owner = restored;
                owned = false;
            }
            if (flush) {
                const before_flush = metal_tensor.memoryStatsSnapshot();
                result = try owner.flush(owner.identity(), null);
                try scalarOnly(&owner, before_flush);
            }
        }
        if (flush) {
            const expected = profile.flushes[flush_index];
            try std.testing.expectEqual(micro.window_after_backward, expected.microbatches);
            try std.testing.expectEqual(@as(f32, @floatFromInt(optimizer.accumulation_steps)) / @as(f32, @floatFromInt(expected.microbatches)), expected.partial_renormalization);
            try expectFlush(&owner, fixture, expected, result);
            flush_index += 1;
        } else try std.testing.expect(!result.optimizer_stepped);
    }
    try std.testing.expectEqual(profile.flushes.len, flush_index);
    return owner.stateFingerprint(identity, null);
}

fn exercise(a: Allocator, cb: *const ops.ComputeBackend, execution: controller.Execution) !void {
    const bytes = try parity.fixtureBytes(a, "training_inactive_adapters/capture.json");
    defer a.free(bytes);
    if (bytes.len > mib) return error.InvalidInactiveTrainingFixture;
    try expectDigest(bytes, "f7bb532ad38f54c105b44e703f0af8bb837b02a7ec1850679b2da17b2ecdbfc0");
    const parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    const source = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), source.version);
    try std.testing.expect(!source.qualification);
    try std.testing.expectEqualStrings("gliner25_inactive_adapter_training/v1", source.scope);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", source.source_commit);
    try std.testing.expectEqual(@as(usize, 8), source.profiles.len);
    var tensors = try parity.TensorFixture.init(a, "training_inactive_adapters/tensors.safetensors");
    defer tensors.deinit();
    try expectDigest(tensors.reader.file_bytes, "4607cc7ff24066eb53e18936d95b20b17b5fc4e0281a23c565b357d8b60f1f2f");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/inactive-oracle.safetensors", .{temporary.sub_path});
    defer a.free(path);
    for (source.profiles) |profile| {
        try std.testing.expect(profile.frozen_parameters_unchanged);
        if (profile.fresh_owner_mid_window_resume_exact) |matches| try std.testing.expect(matches);
        const uninterrupted = replay(a, cb, &tensors, profile, source.optimizer, execution, false, path) catch |err| {
            std.debug.print("inactive source profile {s}, uninterrupted: {s}\n", .{ profile.id, @errorName(err) });
            return err;
        };
        const resumed = replay(a, cb, &tensors, profile, source.optimizer, execution, true, path) catch |err| {
            std.debug.print("inactive source profile {s}, resumed: {s}\n", .{ profile.id, @errorName(err) });
            return err;
        };
        try std.testing.expectEqual(uninterrupted, resumed);
    }
}

test "boundary inactive oracle CPU pinned source zero None accumulation AdamW and durable resume" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    try exercise(a, &cb, .native);
}

test "boundary inactive oracle Metal pinned source zero None accumulation AdamW and durable resume" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try device.Device.init(a);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    try exercise(a, &cb, .resident_metal);
}
