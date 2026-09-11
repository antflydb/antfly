// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict resident state initialization and checkpoint mirror readback. Every
//! published tensor belongs exclusively to RealAutodiffTrainer.ParamSlot;
//! this module owns no persistent optimizer copy and publishes no freshness.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const real = @import("real_autodiff_trainer.zig");
const snapshot = @import("seeded_device_snapshot.zig");
const optimizers = @import("ml").graph.optimizers;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Owner = real.RealAutodiffTrainer;
const Slot = Owner.ParamSlot;
const Device = Owner.DeviceOptimizerSlot;
const gib = 1024 * 1024 * 1024;

pub const Limits = struct {
    max_slots: usize = 4096,
    max_host_payload_bytes: usize = 4 * gib,
    max_device_payload_bytes: usize = 4 * gib,
    max_total_bytes: usize = 8 * gib,
    max_host_metadata_bytes: usize = 16 * 1024 * 1024,
    max_upload_staging_bytes: usize = gib,
    snapshot: snapshot.Limits = .{},
};
pub const Admission = struct {
    slots: usize,
    host_payload_bytes: usize,
    device_payload_bytes: usize,
    host_metadata_bytes: usize,
    upload_staging_bytes: usize,
    snapshot_scratch_bytes: usize,
    initialize_upper_bound_bytes: usize,
    readback_upper_bound_bytes: usize,
};
pub const Receipt = struct {
    admission: Admission,
    upload_bytes: usize = 0,
    upload_tensors: usize = 0,
    download_bytes: usize = 0,
    download_chunks: usize = 0,
};
/// The enclosing controller holds an exclusive immutable epoch lease. Its
/// validator also checks full optimizer/microbatch/accumulation identity and
/// active bindings. This callback is checked throughout chunked readback.
pub const EpochGuard = struct {
    context: ?*const anyopaque,
    expected: u64,
    validate: *const fn (?*const anyopaque, u64) anyerror!void,
    pub fn check(self: EpochGuard) !void {
        try self.validate(self.context, self.expected);
    }
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn count(owner: *const Owner) !usize {
    return add(owner.lora_params.items.len, owner.regular_params.items.len);
}
fn at(owner: *const Owner, index: usize) *const Slot {
    return if (index < owner.lora_params.items.len) &owner.lora_params.items[index] else &owner.regular_params.items[index - owner.lora_params.items.len];
}
fn mutableAt(owner: *Owner, index: usize) *Slot {
    return if (index < owner.lora_params.items.len) &owner.lora_params.items[index] else &owner.regular_params.items[index - owner.lora_params.items.len];
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn state(owner: *const Owner, slot: *const Slot) !optimizers.ParamState {
    const value = owner.optimizer_state.param_states.get(slot.name) orelse return error.MissingOptimizerState;
    if (value.m.len != slot.weights.len or value.v.len != slot.weights.len or value.z != null or
        value.step_count != slot.adam_step_count or value.step_count > owner.optimizer_step_count)
        return error.InvalidSeededDeviceState;
    return value;
}

/// Allocation-free phase estimates include the already owned four host
/// mirrors, four resident payloads, temporary publication metadata, and the
/// largest synchronous upload or readback staging. Backend/driver overhead is
/// admitted by the enclosing backend allocator and runtime reservation.
pub fn estimate(owner: *const Owner, limits: Limits) !Admission {
    const slots = try count(owner);
    if (slots == 0 or slots > limits.max_slots or limits.max_slots > 4096 or
        owner.config.execution_engine != .interpreter or owner.graph_state != null or owner.compiled_session != null or owner.inactive_graphs.items.len != 0 or
        owner.optimizer_state.param_states.count() != slots or owner.optimizer_state.step_count != owner.optimizer_step_count)
        return error.InvalidSeededDeviceState;
    var payload: usize = 0;
    var largest: usize = 0;
    var read_scratch: usize = 0;
    for (0..slots) |i| {
        const slot = at(owner, i);
        if (slot.name.len == 0 or slot.name.len > 1024) return error.InvalidSeededDeviceState;
        for (0..i) |j| if (std.mem.eql(u8, at(owner, j).name, slot.name)) return error.InvalidSeededDeviceState;
        const elements = try ops.resident_training.shapeElements(i32, slot.dims, limits.snapshot.primitive);
        if (elements != slot.weights.len or slot.grad_accum.len != elements) return error.InvalidSeededDeviceState;
        _ = try state(owner, slot);
        const bytes = try mul(elements, 4);
        payload = try add(payload, try mul(bytes, 4));
        largest = @max(largest, bytes);
        read_scratch = @max(read_scratch, (try snapshot.admission(elements, limits.snapshot)).scratch_upper_bound_bytes);
    }
    const metadata = try mul(slots, @sizeOf(Device));
    if (payload > limits.max_host_payload_bytes or payload > limits.max_device_payload_bytes or metadata > limits.max_host_metadata_bytes)
        return error.TrainingOptimizerLimitExceeded;
    const base = try add(try mul(payload, 2), metadata);
    return .{ .slots = slots, .host_payload_bytes = payload, .device_payload_bytes = payload, .host_metadata_bytes = metadata, .upload_staging_bytes = largest, .snapshot_scratch_bytes = read_scratch, .initialize_upper_bound_bytes = try add(base, largest), .readback_upper_bound_bytes = try add(base, read_scratch) };
}

const Stamp = struct {
    microbatch: u64,
    optimizer: u64,
    optimizer_state: u32,
    accumulation: u32,
    regular: usize,
    lora: usize,
    pub fn of(owner: *const Owner) Stamp {
        return .{ .microbatch = owner.step_count, .optimizer = owner.optimizer_step_count, .optimizer_state = owner.optimizer_state.step_count, .accumulation = owner.accum_count, .regular = owner.regular_params.items.len, .lora = owner.lora_params.items.len };
    }
    pub fn validate(self: Stamp, owner: *const Owner) !void {
        if (!std.meta.eql(self, of(owner))) return error.TrainingStateIdentityMismatch;
    }
};
fn strictBackend(backend: *const ops.ComputeBackend) !void {
    if (backend.kind() != .metal or backend.vtable.residentTrainingPrimitive == null or
        backend.vtable.residentTrainingInstruction == null or backend.vtable.glinerBoundaryDownload == null)
        return error.UnsupportedSeededTrainingBackend;
}
fn validateHost(values: []const f32, nonnegative: bool, control: ?Control) !void {
    for (values, 0..) |value, i| {
        if (i % 4096 == 0) try check(control);
        if (!std.math.isFinite(value) or (nonnegative and value < 0)) return error.NonFiniteTrainingUpdate;
    }
}
fn freeDevice(cb: *const ops.ComputeBackend, value: Device) void {
    cb.free(value.weight);
    cb.free(value.grad_accum);
    cb.free(value.m);
    cb.free(value.v);
}
fn uploadSlot(cb: *const ops.ComputeBackend, slot: *const Slot, moments: optimizers.ParamState, limits: Limits) !Device {
    const weight = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = slot.weights, .shape = slot.dims } }, limits.snapshot.primitive);
    errdefer cb.free(weight);
    const accum = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = slot.grad_accum, .shape = slot.dims } }, limits.snapshot.primitive);
    errdefer cb.free(accum);
    const m = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = moments.m, .shape = slot.dims } }, limits.snapshot.primitive);
    errdefer cb.free(m);
    const v = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = moments.v, .shape = slot.dims } }, limits.snapshot.primitive);
    return .{ .weight = weight, .grad_accum = accum, .m = m, .v = v };
}

const InitControl = struct {
    owner: *const Owner,
    stamp: Stamp,
    primary: ?Control,
    secondary: ?Control,
    fn control(self: *InitControl) Control {
        return .{ .ptr = self, .check_fn = validate };
    }
    fn validate(raw: ?*anyopaque) !void {
        const self: *InitControl = @ptrCast(@alignCast(raw orelse return error.TrainingStateIdentityMismatch));
        try check(self.primary);
        try check(self.secondary);
        try self.stamp.validate(self.owner);
    }
};

/// Initialize a fresh or fully restored host owner. All input mirrors must be
/// immutable until this returns. Any cancellation, callback or allocation
/// failure frees every staged handle and publishes no partial device state.
pub fn initializeFromHost(owner: *Owner, limits: Limits, control: ?Control) !Receipt {
    const admitted = try estimate(owner, limits);
    if (admitted.initialize_upper_bound_bytes > limits.max_total_bytes or admitted.upload_staging_bytes > limits.max_upload_staging_bytes)
        return error.TrainingOptimizerLimitExceeded;
    try strictBackend(owner.compute_backend);
    const stamp = Stamp.of(owner);
    var bridge = InitControl{ .owner = owner, .stamp = stamp, .primary = owner.compute_backend.execution_control, .secondary = control };
    const active = bridge.control();
    try active.check();
    if (owner.device_trainable_bytes != 0) return error.DeviceOptimizerAlreadyInitialized;
    for (0..admitted.slots) |i| {
        const slot = at(owner, i);
        if (slot.device != null) return error.DeviceOptimizerAlreadyInitialized;
        const moments = try state(owner, slot);
        try validateHost(slot.weights, false, active);
        try validateHost(slot.grad_accum, false, active);
        try validateHost(moments.m, false, active);
        try validateHost(moments.v, true, active);
    }
    var cb = owner.compute_backend.*;
    cb.execution_control = active;
    const staged = try owner.allocator.alloc(Device, admitted.slots);
    defer owner.allocator.free(staged);
    var initialized: usize = 0;
    errdefer for (staged[0..initialized]) |value| freeDevice(&cb, value);
    for (staged, 0..) |*device, i| {
        try active.check();
        const slot = at(owner, i);
        device.* = try uploadSlot(&cb, slot, try state(owner, slot), limits);
        initialized += 1;
    }
    try active.check();
    for (0..admitted.slots) |i| if (at(owner, i).device != null) return error.DeviceOptimizerAlreadyInitialized;
    // No fallible operation follows the first ownership publication.
    for (staged, 0..) |device, i| mutableAt(owner, i).device = device;
    owner.device_trainable_bytes = admitted.device_payload_bytes;
    return .{ .admission = admitted, .upload_bytes = admitted.device_payload_bytes, .upload_tensors = admitted.slots * 4 };
}

const ReadControl = struct {
    owner: *const Owner,
    stamp: Stamp,
    epoch: EpochGuard,
    primary: ?Control,
    secondary: ?Control,
    fn control(self: *ReadControl) Control {
        return .{ .ptr = self, .check_fn = validate };
    }
    fn validate(raw: ?*anyopaque) !void {
        const self: *ReadControl = @ptrCast(@alignCast(raw orelse return error.TrainingStateIdentityMismatch));
        try check(self.primary);
        try check(self.secondary);
        try self.stamp.validate(self.owner);
        try self.epoch.check();
    }
};

/// This is a checkpoint/export boundary, never a training-step operation.
/// Destination mirrors can be partially written on error. The caller keeps
/// them uncertified until the receipt is returned and its own epoch lease is
/// revalidated; this helper never updates a freshness flag or optimizer count.
pub fn readMirrors(owner: *Owner, guard: EpochGuard, limits: Limits, control: ?Control) !Receipt {
    const admitted = try estimate(owner, limits);
    if (admitted.readback_upper_bound_bytes > limits.max_total_bytes) return error.TrainingOptimizerLimitExceeded;
    try strictBackend(owner.compute_backend);
    if (owner.device_trainable_bytes != admitted.device_payload_bytes) return error.InvalidSeededDeviceState;
    var bridge = ReadControl{ .owner = owner, .stamp = Stamp.of(owner), .epoch = guard, .primary = owner.compute_backend.execution_control, .secondary = control };
    const active = bridge.control();
    try active.check();
    for (0..admitted.slots) |i| if (at(owner, i).device == null) return error.DeviceOptimizerNotInitialized;
    var receipt = Receipt{ .admission = admitted };
    for (0..admitted.slots) |i| {
        try active.check();
        const slot = mutableAt(owner, i);
        const device = slot.device.?;
        const moments = try state(owner, slot);
        const sources = [_]ops.CT{ device.weight, device.grad_accum, device.m, device.v };
        const destinations = [_][]f32{ slot.weights, slot.grad_accum, moments.m, moments.v };
        for (sources, destinations, 0..) |source, destination, field| {
            const copied = try snapshot.readInto(owner.compute_backend, source, destination, limits.snapshot, active);
            receipt.download_bytes = try add(receipt.download_bytes, copied.download_bytes);
            receipt.download_chunks = try add(receipt.download_chunks, copied.chunks);
            if (field == 3) try validateHost(destination, true, active);
        }
    }
    try active.check();
    return receipt;
}
