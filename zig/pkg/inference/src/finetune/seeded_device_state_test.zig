// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const state = @import("seeded_device_state.zig");
const real = @import("real_autodiff_trainer.zig");
const ops = @import("../ops/ops.zig");
const ml = @import("ml").graph;
const DType = @import("../backends/tensor.zig").DType;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Owner = real.RealAutodiffTrainer;

/// An executable ownership/failure contract. Only the strict resident hooks
/// exist: a generic upload, clone or host conversion cannot pass these tests.
const Fake = struct {
    allocator: Allocator,
    vtable: ops.ComputeBackend.VTable = undefined,
    backend_kind: ops.BackendKind = .metal,
    live: usize = 0,
    uploads: usize = 0,
    downloads: usize = 0,
    max_download_elements: usize = 0,
    const Tensor = struct { allocator: Allocator, owner: *Fake, values: []f32, shape: ml.Shape };

    fn init(a: Allocator) Fake {
        var result = Fake{ .allocator = a };
        result.vtable.backendKind = kind;
        result.vtable.freeTensor = free;
        result.vtable.tensorDType = dtype;
        result.vtable.residentTrainingPrimitive = primitive;
        result.vtable.residentTrainingInstruction = instruction;
        result.vtable.glinerBoundaryDownload = download;
        return result;
    }
    fn cb(self: *Fake) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &self.vtable };
    }
    fn from(raw: *anyopaque) *Fake {
        return @ptrCast(@alignCast(raw));
    }
    fn get(self: *Fake, value: ops.CT) !*Tensor {
        const tensor: *Tensor = @ptrCast(@alignCast(value));
        if (tensor.owner != self) return error.ForeignResidentTrainingTensor;
        return tensor;
    }
    fn make(self: *Fake, values: []const f32, shape: ml.Shape) !ops.CT {
        const data = try self.allocator.dupe(f32, values);
        errdefer self.allocator.free(data);
        const tensor = try self.allocator.create(Tensor);
        tensor.* = .{ .allocator = self.allocator, .owner = self, .values = data, .shape = shape };
        self.live += 1;
        return tensor;
    }
    fn kind(raw: *anyopaque) ops.BackendKind {
        return from(raw).backend_kind;
    }
    fn free(raw: *anyopaque, value: ops.CT) void {
        const self = from(raw);
        const tensor = self.get(value) catch unreachable;
        const a = tensor.allocator;
        a.free(tensor.values);
        a.destroy(tensor);
        self.live -= 1;
    }
    fn dtype(raw: *anyopaque, value: ops.CT) anyerror!DType {
        return @enumFromInt(@intFromEnum((try from(raw).get(value)).shape.dtype));
    }
    fn shapeOf(dims: []const i32) ml.Shape {
        var result = ml.Shape{ .dtype = .f32, .rank_ = @intCast(dims.len) };
        for (dims, 0..) |dim, i| result.dims[i] = dim;
        return result;
    }
    fn primitive(raw: *anyopaque, request: *const ops.resident_training.Request, primitive_limits: ops.resident_training.Limits, control: ?Control) anyerror!ops.CT {
        if (control) |active| try active.check();
        const self = from(raw);
        return switch (request.*) {
            .upload_f32 => |input| blk: {
                if (try ops.resident_training.shapeElements(i32, input.shape, primitive_limits) != input.values.len) return error.InvalidResidentTrainingShape;
                self.uploads += 1;
                break :blk try self.make(input.values, shapeOf(input.shape));
            },
            .reshape => |input| blk: {
                const tensor = try self.get(input.input);
                if (try ops.resident_training.shapeElements(i32, input.shape, primitive_limits) != tensor.values.len) return error.InvalidResidentTrainingShape;
                break :blk try self.make(tensor.values, shapeOf(input.shape));
            },
            else => error.UnexpectedDeviceStatePrimitive,
        };
    }
    fn instruction(raw: *anyopaque, inst: *const ops.resident_program.Instruction, inputs: []const ops.CT, instruction_limits: ops.resident_program.Limits, control: ?Control) anyerror!ops.CT {
        if (control) |active| try active.check();
        const self = from(raw);
        _ = try inst.validate(instruction_limits);
        if (inst.op != .slice or inputs.len != 1 or inst.inputs[0].rank_ != 1) return error.UnexpectedDeviceStateInstruction;
        const tensor = try self.get(inputs[0]);
        if (!tensor.shape.eq(inst.inputs[0])) return error.InvalidResidentProgramShape;
        const attrs = inst.op.slice;
        const start: usize = @intCast(attrs.starts[0]);
        const end: usize = @intCast(attrs.limits[0]);
        return self.make(tensor.values[start..end], inst.output);
    }
    fn download(raw: *anyopaque, value: ops.CT, destination: []f32) anyerror!void {
        const self = from(raw);
        const tensor = try self.get(value);
        if (tensor.values.len != destination.len) return error.InvalidResidentTrainingShape;
        self.downloads += 1;
        self.max_download_elements = @max(self.max_download_elements, destination.len);
        @memcpy(destination, tensor.values);
    }
};

fn tinyOwner(a: Allocator, cb: *const ops.ComputeBackend) !Owner {
    var owner = try Owner.init(a, cb, .{ .lora = .{ .rank = 1, .target_patterns = &.{} } });
    errdefer owner.deinit();
    owner.step_count = 5;
    owner.optimizer_step_count = 2;
    owner.optimizer_state.step_count = 2;
    owner.accum_count = 1;
    for ([_][]const i32{ &.{ 3, 3 }, &.{3}, &.{1} }, 0..) |shape, index| {
        const elements = try ops.resident_training.shapeElements(i32, shape, .{});
        const name = try std.fmt.allocPrint(a, "slot.{d}", .{index});
        errdefer a.free(name);
        const weights = try a.alloc(f32, elements);
        errdefer a.free(weights);
        const accum = try a.alloc(f32, elements);
        errdefer a.free(accum);
        const dims = try a.dupe(i32, shape);
        errdefer a.free(dims);
        const moments = try owner.optimizer_state.getOrCreate(name, elements, true);
        const step: u32 = @intCast(index);
        moments.step_count = step;
        for (0..elements) |i| {
            weights[i] = sample(index, 0, i);
            accum[i] = sample(index, 1, i);
            moments.m[i] = sample(index, 2, i);
            moments.v[i] = sample(index, 3, i);
        }
        const slot = Owner.ParamSlot{ .name = name, .weights = weights, .grad_accum = accum, .dims = dims, .node_id = ml.null_node, .adam_step_count = step };
        if (index == 0) try owner.lora_params.append(a, slot) else try owner.regular_params.append(a, slot);
    }
    return owner;
}
fn slotAt(owner: *Owner, index: usize) *Owner.ParamSlot {
    return if (index == 0) &owner.lora_params.items[0] else &owner.regular_params.items[index - 1];
}
fn sample(slot: usize, field: usize, element: usize) f32 {
    const value: f32 = @as(f32, @floatFromInt(slot * 16 + element + 1)) / 8;
    return switch (field) {
        0 => value,
        1 => -value / 2,
        2 => value / 4,
        3 => value / 8,
        else => unreachable,
    };
}
fn expectHost(owner: *Owner) !void {
    for (0..3) |i| {
        const slot = slotAt(owner, i);
        const moments = owner.optimizer_state.param_states.get(slot.name).?;
        for ([_][]const f32{ slot.weights, slot.grad_accum, moments.m, moments.v }, 0..) |values, field| for (values, 0..) |value, j| try std.testing.expectEqual(sample(i, field, j), value);
        try std.testing.expectEqual(@as(u32, @intCast(i)), slot.adam_step_count);
        try std.testing.expectEqual(slot.adam_step_count, moments.step_count);
    }
    try std.testing.expectEqual(@as(u64, 5), owner.step_count);
    try std.testing.expectEqual(@as(u64, 2), owner.optimizer_step_count);
    try std.testing.expectEqual(@as(u32, 2), owner.optimizer_state.step_count);
    try std.testing.expectEqual(@as(u32, 1), owner.accum_count);
}
fn fillHost(owner: *Owner, value: f32) void {
    for (0..3) |i| {
        const slot = slotAt(owner, i);
        const moments = owner.optimizer_state.param_states.get(slot.name).?;
        for ([_][]f32{ slot.weights, slot.grad_accum, moments.m, moments.v }) |values| @memset(values, value);
    }
}
fn expectUnpublished(owner: *Owner) !void {
    try std.testing.expectEqual(@as(usize, 0), owner.device_trainable_bytes);
    for (0..3) |i| try std.testing.expect(slotAt(owner, i).device == null);
}
const limits = state.Limits{ .snapshot = .{ .chunk_bytes = 16, .max_scratch_bytes = 32 } };
const Epoch = struct {
    owner: *const Owner,
    fake: ?*const Fake = null,
    fail_download: ?usize = null,
    fn guard(self: *Epoch) state.EpochGuard {
        return .{ .context = self, .expected = self.owner.step_count, .validate = validate };
    }
    fn validate(raw: ?*const anyopaque, expected: u64) !void {
        const self: *const Epoch = @ptrCast(@alignCast(raw.?));
        if (self.owner.step_count != expected) return error.TrainingStateIdentityMismatch;
        if (self.fail_download) |at| if (self.fake.?.downloads >= at) return error.TrainingStateIdentityMismatch;
    }
};

test "seeded device state admission counts sole-owner mirrors staging and every slot" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    const cb = fake.cb();
    var owner = try tinyOwner(a, &cb);
    defer owner.deinit();
    const admitted = try state.estimate(&owner, limits);
    try std.testing.expectEqual(@as(usize, 3), admitted.slots);
    try std.testing.expectEqual(@as(usize, 208), admitted.host_payload_bytes);
    try std.testing.expectEqual(@as(usize, 208), admitted.device_payload_bytes);
    try std.testing.expectEqual(@as(usize, 36), admitted.upload_staging_bytes);
    try std.testing.expectEqual(@as(usize, 32), admitted.snapshot_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 416 + 3 * @sizeOf(Owner.DeviceOptimizerSlot) + 36), admitted.initialize_upper_bound_bytes);
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, state.estimate(&owner, .{ .max_device_payload_bytes = 207 }));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, state.initializeFromHost(&owner, .{ .max_total_bytes = admitted.initialize_upper_bound_bytes - 1 }, null));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, state.initializeFromHost(&owner, .{ .max_upload_staging_bytes = 35 }, null));
    const original_name = owner.regular_params.items[0].name;
    owner.regular_params.items[0].name = owner.lora_params.items[0].name;
    try std.testing.expectError(error.InvalidSeededDeviceState, state.estimate(&owner, limits));
    owner.regular_params.items[0].name = original_name;
    owner.regular_params.items[0].adam_step_count += 1;
    try std.testing.expectError(error.InvalidSeededDeviceState, state.estimate(&owner, limits));
    owner.regular_params.items[0].adam_step_count -= 1;
    try expectUnpublished(&owner);
    try std.testing.expectEqual(@as(usize, 0), fake.uploads);
}

test "seeded device state rejects global optimizer counter mismatch before any transfer" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    const cb = fake.cb();
    var owner = try tinyOwner(a, &cb);
    defer owner.deinit();
    var epoch = Epoch{ .owner = &owner };
    for ([_]u32{ 0, 1, 3, std.math.maxInt(u32) }) |wrong| {
        owner.optimizer_state.step_count = wrong;
        try std.testing.expectError(error.InvalidSeededDeviceState, state.estimate(&owner, limits));
        try std.testing.expectError(error.InvalidSeededDeviceState, state.initializeFromHost(&owner, limits, null));
        try std.testing.expectError(error.InvalidSeededDeviceState, state.readMirrors(&owner, epoch.guard(), limits, null));
        try expectUnpublished(&owner);
    }
    try std.testing.expectEqual(@as(usize, 0), fake.uploads);
    try std.testing.expectEqual(@as(usize, 0), fake.downloads);
    owner.optimizer_state.step_count = 2;
    _ = try state.estimate(&owner, limits);
    try expectHost(&owner);
}

test "seeded device state combines controls and cleans every partially staged slot" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    var cb = fake.cb();
    var owner = try tinyOwner(a, &cb);
    defer owner.deinit();
    const Trigger = struct {
        fake: *const Fake,
        after: usize,
        fn control(self: *@This()) Control {
            return .{ .ptr = self, .check_fn = check };
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.fake.uploads >= self.after) return error.Cancelled;
        }
    };
    var trigger = Trigger{ .fake = &fake, .after = 5 };
    cb.execution_control = trigger.control();
    // A request control cannot replace the backend's independent cancellation.
    try std.testing.expectError(error.Cancelled, state.initializeFromHost(&owner, limits, .{}));
    try std.testing.expectEqual(@as(usize, 5), fake.uploads);
    try std.testing.expectEqual(@as(usize, 0), fake.live);
    try expectUnpublished(&owner);
    cb.execution_control = .{};
    trigger.after = fake.uploads + 2;
    try std.testing.expectError(error.Cancelled, state.initializeFromHost(&owner, limits, trigger.control()));
    try std.testing.expectEqual(@as(usize, 0), fake.live);
    try expectUnpublished(&owner);
    const moments = owner.optimizer_state.param_states.get(owner.lora_params.items[0].name).?;
    moments.v[0] = -1;
    try std.testing.expectError(error.NonFiniteTrainingUpdate, state.initializeFromHost(&owner, limits, null));
    moments.v[0] = sample(0, 3, 0);
    owner.regular_params.items[1].weights[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteTrainingUpdate, state.initializeFromHost(&owner, limits, null));
    owner.regular_params.items[1].weights[0] = sample(2, 0, 0);
    try expectHost(&owner);
    try expectUnpublished(&owner);
}

fn initializeAllocationFailures(a: Allocator) !void {
    const stable = std.testing.allocator;
    var fake = Fake.init(a);
    const cb = fake.cb();
    var owner = try tinyOwner(stable, &cb);
    defer owner.deinit();
    owner.allocator = a;
    defer owner.allocator = stable;
    _ = state.initializeFromHost(&owner, limits, null) catch |err| {
        try expectUnpublished(&owner);
        try std.testing.expectEqual(@as(usize, 0), fake.live);
        try expectHost(&owner);
        return err;
    };
    try std.testing.expectEqual(@as(usize, 12), fake.live);
    try expectHost(&owner);
}
test "seeded device state initialization remains atomic at every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, initializeAllocationFailures, .{});
}

test "seeded device state readback validates epoch and recovers uncertified mirrors in place" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    const cb = fake.cb();
    var owner = try tinyOwner(a, &cb);
    defer owner.deinit();
    _ = try state.initializeFromHost(&owner, limits, null);
    const original_pointer = owner.lora_params.items[0].weights.ptr;
    var epoch = Epoch{ .owner = &owner, .fake = &fake };
    const guard = epoch.guard();
    fillHost(&owner, -999);
    owner.step_count += 1;
    try std.testing.expectError(error.TrainingStateIdentityMismatch, state.readMirrors(&owner, guard, limits, null));
    try std.testing.expectEqual(@as(usize, 0), fake.downloads);
    owner.step_count -= 1;
    epoch.fail_download = 2;
    try std.testing.expectError(error.TrainingStateIdentityMismatch, state.readMirrors(&owner, guard, limits, null));
    try std.testing.expectEqual(@as(usize, 12), fake.live);
    try std.testing.expectEqual(sample(0, 0, 0), owner.lora_params.items[0].weights[0]);
    try std.testing.expectEqual(@as(f32, -999), owner.lora_params.items[0].weights[8]);
    epoch.fail_download = null;
    const copied = try state.readMirrors(&owner, guard, limits, null);
    try std.testing.expectEqual(@as(usize, 208), copied.download_bytes);
    try std.testing.expectEqual(@as(usize, 20), copied.download_chunks);
    try std.testing.expectEqual(@as(usize, 4), fake.max_download_elements);
    try std.testing.expectEqual(original_pointer, owner.lora_params.items[0].weights.ptr);
    try expectHost(&owner);
    // Corrupt only a device second moment. Readback must not certify it.
    const device_v = try fake.get(owner.regular_params.items[1].device.?.v);
    device_v.values[0] = -1;
    try std.testing.expectError(error.NonFiniteTrainingUpdate, state.readMirrors(&owner, guard, limits, null));
    device_v.values[0] = sample(2, 3, 0);
    _ = try state.readMirrors(&owner, guard, limits, null);
    try expectHost(&owner);
}

test "seeded device state Metal initializes and snapshots existing mirrors without generic transfers" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try @import("../graph/resident_training_fixture.zig").Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var owner = try tinyOwner(a, &cb);
    defer owner.deinit();
    const mt = @import("../backends/metal_tensor.zig");
    const before = mt.memoryStatsSnapshot();
    const uploaded = try state.initializeFromHost(&owner, limits, null);
    try std.testing.expectEqual(@as(usize, 208), uploaded.upload_bytes);
    try std.testing.expectEqual(@as(usize, 12), uploaded.upload_tensors);
    try std.testing.expectError(error.DeviceOptimizerAlreadyInitialized, state.initializeFromHost(&owner, limits, null));
    const original_pointer = owner.lora_params.items[0].weights.ptr;
    fillHost(&owner, -999);
    var epoch = Epoch{ .owner = &owner };
    const copied = try state.readMirrors(&owner, epoch.guard(), limits, null);
    try std.testing.expectEqual(@as(usize, 208), copied.download_bytes);
    try std.testing.expectEqual(@as(usize, 20), copied.download_chunks);
    try std.testing.expectEqual(original_pointer, owner.lora_params.items[0].weights.ptr);
    try expectHost(&owner);
    const after = mt.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
}
