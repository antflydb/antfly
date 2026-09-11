// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transactional resident AdamW preparation. The caller remains the sole
//! optimizer owner: this module never changes borrowed tensors or counters.
//! Successful replacements are independent buffers, ready for a no-fail
//! ownership swap. A failed transaction leaves the old epoch usable.
const std = @import("std");
const ml = @import("ml").graph;
const optimizers = ml.optimizers;
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;
const CT = ops.CT;
const mib = 1024 * 1024;
const batch_capacity = 256;

pub const Identity = struct { optimizer_step: u64, microbatch_step: u64 };
pub const Group = struct { optimizer: optimizers.AdamWConfig = .{}, schedule: optimizers.LearningRateSchedule };
pub const Slot = struct {
    weight: CT,
    grad_accum: CT,
    m: CT,
    v: CT,
    shape: []const i32,
    group: usize,
    adam_step: u32,
    present: bool,
};
pub const State = struct {
    identity: Identity,
    accumulated_microbatches: u32,
    grad_accum_steps: u32,
    max_grad_norm: f32 = 1,
    slots: []const Slot,
    groups: []const Group,
};
/// An omitted slot is absent (Python grad=None). An explicit zero participates
/// in accumulation and AdamW, including weight decay and its parameter counter.
pub const Gradient = struct { slot: usize, value: union(enum) { zero, tensor: CT } };
pub const Request = struct {
    expected: Identity,
    action: enum { submit, flush },
    loss: ?f32 = null,
    gradients: []const Gradient = &.{},
};
pub const Limits = struct {
    max_slots: usize = 4096,
    max_groups: usize = 64,
    /// Additional physical tensor payloads, norm partials, and scalar staging.
    /// The caller separately admits the existing weights, moments, gradients,
    /// retained graph, and backend/driver allocation overhead.
    max_device_bytes: usize = 4 * 1024 * mib,
    max_host_metadata_bytes: usize = 64 * mib,
    max_scalar_transfer_bytes: usize = 4 * mib,
    primitive: ops.resident_training.Limits = .{},
    max_norm_partial_bytes: usize = 64 * mib,
};
pub const Estimate = struct {
    selected_slots: usize,
    optimizer_stepped: bool,
    pending_tensor_bytes: usize,
    temporary_tensor_bytes: usize,
    reduction_scratch_bytes: usize,
    device_upper_bound_bytes: usize,
    host_metadata_upper_bound_bytes: usize,
    scalar_transfer_upper_bound_bytes: usize,
    /// Conservative scalar arithmetic/element visits, including all finite
    /// reductions, copy/accumulation/AdamW passes and host metadata scans.
    /// This is an admission bound, not measured GPU work or elapsed time.
    total_work: u64,
};
pub const Receipt = struct {
    identity: Identity,
    accumulated_microbatches: u32,
    optimizer_stepped: bool,
    grad_norm: f64,
    loss: ?f32,
    selected_slots: usize,
    scalar_upload_bytes: usize = 0,
    scalar_download_bytes: usize = 0,
};
pub const Replacement = struct {
    slot: usize,
    /// Present for every replacement, including a cleared accumulator after
    /// an optimizer update. The other three buffers exist only on an update.
    grad_accum: ?CT,
    weight: ?CT = null,
    m: ?CT = null,
    v: ?CT = null,
    adam_step: u32,
    present: bool,
};
pub const Pending = struct {
    backing: Allocator,
    budget: Budget,
    backend: ops.ComputeBackend,
    replacements: []Replacement,
    estimate: Estimate,
    receipt: Receipt,

    /// Transfer one replacement by its position in replacements. This cannot
    /// fail; the owner validates its epoch and completes all fallible work
    /// before starting the swap. Untaken buffers are freed by deinit.
    pub fn take(self: *Pending, index: usize) Replacement {
        const value = self.replacements[index];
        std.debug.assert(value.grad_accum != null);
        self.replacements[index].grad_accum = null;
        self.replacements[index].weight = null;
        self.replacements[index].m = null;
        self.replacements[index].v = null;
        return value;
    }

    /// The backend/runtime must outlive Pending and every taken replacement.
    pub fn deinit(self: *Pending) void {
        for (self.replacements) |value| {
            if (value.grad_accum) |tensor| self.backend.free(tensor);
            if (value.weight) |tensor| self.backend.free(tensor);
            if (value.m) |tensor| self.backend.free(tensor);
            if (value.v) |tensor| self.backend.free(tensor);
        }
        self.budget.allocator().free(self.replacements);
        std.debug.assert(self.budget.live == 0);
        const a = self.backing;
        a.destroy(self);
    }
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn addWork(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn mulWork(a: u64, b: u64) !u64 {
    return std.math.mul(u64, a, b) catch error.TrainingOptimizerLimitExceeded;
}
fn workUpperBound(slot_count: u64, group_count: u64, selected_count: u64, selected_elements: u64, norm_chunks: u64, stepped: bool) !u64 {
    // prepare can scan five live inputs (weight, accumulation, m, v, incoming
    // gradient), separately prove an absent accumulator is zero, then measure
    // the new accumulator. A flush also scans the three updated arrays and
    // proves AdamW cleared its gradient: seven or eleven norms per slot.
    // Always charge the incoming tensor case, even for a supplied .zero, so
    // an all-zero prospective flush safely admits any later gradient values.
    const norm_passes: u64 = if (stepped) 11 else 7;
    // norm_chunks reads each element twice (finite/max then scaled square),
    // and norm_merge reads each three-scalar chunk summary twice. The fixed
    // chunk allowance includes all three 32-lane reductions, lane setup and
    // summary merge work; the per-tensor allowance includes the final three
    // reductions, scalar readback validation and empty/tail lanes.
    const norm_elements = try mulWork(selected_elements, 16);
    const norm_reductions = try addWork(try mulWork(norm_chunks, 1024), try mulWork(selected_count, 2048));
    const norms = try mulWork(try addWork(norm_elements, norm_reductions), norm_passes);
    // Zero broadcast plus independent copy, division, addition and optional
    // partial-window renormalization. A flush additionally clips, snapshots
    // weight/m/v and performs AdamW including its four stores/gradient clear.
    const tensors = try mulWork(selected_elements, if (stepped) 80 else 24);
    // Duplicate/incoming lookup is quadratic in the bounded slot count; group
    // batching is slots*groups. Include shape/owner checks, schedules, scalar
    // uploads, <=256-item batch metadata and final replacement bookkeeping.
    const lookups = try mulWork(try mulWork(slot_count, try addWork(slot_count, group_count)), 16);
    const metadata = try mulWork(try addWork(try addWork(slot_count, group_count), 1), 4096);
    return addWork(try addWork(norms, tensors), try addWork(lookups, metadata));
}
fn elements(slot: Slot, limits: Limits) !usize {
    return ops.resident_training.shapeElements(i32, slot.shape, limits.primitive);
}
fn incoming(request: Request, slot: usize) ?Gradient {
    for (request.gradients) |gradient| if (gradient.slot == slot) return gradient;
    return null;
}
fn selected(state: State, request: Request, slot: usize) bool {
    return state.slots[slot].present or incoming(request, slot) != null;
}
fn nextAccumulation(state: State, request: Request) u32 {
    return state.accumulated_microbatches + @as(u32, @intFromBool(request.action == .submit));
}
fn validateGroup(group: Group) !void {
    const o = group.optimizer;
    if (!std.math.isFinite(o.beta1) or o.beta1 < 0 or o.beta1 >= 1 or
        !std.math.isFinite(o.beta2) or o.beta2 < 0 or o.beta2 >= 1 or
        !std.math.isFinite(o.eps) or o.eps <= 0 or
        !std.math.isFinite(o.weight_decay) or o.weight_decay < 0)
        return error.InvalidOptimizerGroup;
    switch (group.schedule) {
        .constant => |rate| if (!std.math.isFinite(rate) or rate < 0) return error.InvalidOptimizerGroup,
        inline else => |schedule| {
            if (schedule.total_steps == 0) return error.InvalidOptimizerGroup;
            inline for (@typeInfo(@TypeOf(schedule)).@"struct".fields) |field| {
                if (field.type == f32) {
                    const value = @field(schedule, field.name);
                    if (!std.math.isFinite(value) or value < 0) return error.InvalidOptimizerGroup;
                }
            }
            if (@hasField(@TypeOf(schedule), "warmup_steps") and schedule.warmup_steps > schedule.total_steps)
                return error.InvalidOptimizerGroup;
        },
    }
}

/// Allocation-free metadata admission. Payload ownership, physical F32 shapes
/// and finiteness are verified by prepare before any AdamW mutation. The bound
/// includes old/new accumulation temporaries, all pending state, 1,024-element
/// norm partials and both private/shared scalar summary buffers.
pub fn estimate(state: State, request: Request, limits: Limits) !Estimate {
    if (limits.max_slots == 0 or limits.max_slots > 4096 or limits.max_groups == 0 or limits.max_groups > 256 or
        limits.max_device_bytes == 0 or limits.max_host_metadata_bytes <= @sizeOf(Pending) or
        limits.max_scalar_transfer_bytes == 0 or limits.max_norm_partial_bytes == 0 or limits.max_norm_partial_bytes > 64 * mib)
        return error.InvalidDeviceTransactionLimits;
    if (state.slots.len == 0 or state.slots.len > limits.max_slots or state.groups.len == 0 or state.groups.len > limits.max_groups or
        state.grad_accum_steps == 0 or state.grad_accum_steps > 65536 or state.accumulated_microbatches >= state.grad_accum_steps or
        !std.math.isFinite(state.max_grad_norm) or state.max_grad_norm < 0 or
        state.identity.microbatch_step < state.accumulated_microbatches)
        return error.InvalidDeviceOptimizerState;
    if (!std.meta.eql(state.identity, request.expected)) return error.TrainingTapeIdentityMismatch;
    if (state.identity.optimizer_step >= std.math.maxInt(u32) or state.identity.microbatch_step == std.math.maxInt(u64))
        return error.TrainingEpochOverflow;
    switch (request.action) {
        .submit => if (request.loss == null or !std.math.isFinite(request.loss.?)) return error.NonFiniteTrainingUpdate,
        .flush => if (request.loss != null or request.gradients.len != 0) return error.InvalidTrainingGradient,
    }
    if (request.gradients.len > state.slots.len) return error.InvalidTrainingGradient;
    for (request.gradients, 0..) |gradient, i| {
        if (gradient.slot >= state.slots.len) return error.InvalidTrainingGradient;
        for (request.gradients[0..i]) |prior| if (prior.slot == gradient.slot) return error.DuplicateTrainingGradient;
    }
    for (state.groups) |group| try validateGroup(group);
    const accumulated = nextAccumulation(state, request);
    const stepped = accumulated > 0 and (request.action == .flush or accumulated == state.grad_accum_steps);
    var count: usize = 0;
    var payload: usize = 0;
    var largest: usize = 0;
    var partials: usize = 0;
    var largest_partial: usize = 0;
    var selected_elements: usize = 0;
    var norm_chunks: usize = 0;
    for (state.slots, 0..) |slot, i| {
        const n = try elements(slot, limits);
        if (slot.group >= state.groups.len or slot.adam_step > state.identity.optimizer_step or
            (state.accumulated_microbatches == 0 and slot.present)) return error.InvalidDeviceOptimizerState;
        if (!selected(state, request, i)) continue;
        if (stepped and slot.adam_step == std.math.maxInt(u32)) return error.TrainingEpochOverflow;
        const rate = state.groups[slot.group].schedule.lr(@intCast(state.identity.optimizer_step));
        if (!std.math.isFinite(rate) or rate < 0) return error.InvalidOptimizerGroup;
        count += 1;
        selected_elements = try add(selected_elements, n);
        const bytes = try mul(n, 4);
        payload = try add(payload, bytes);
        largest = @max(largest, bytes);
        const chunks = try std.math.divCeil(usize, n, 1024);
        norm_chunks = try add(norm_chunks, chunks);
        const partial = try mul(chunks, 12);
        partials = try add(partials, partial);
        largest_partial = @max(largest_partial, partial);
    }
    const norm_count = @min(batch_capacity, try mul(count, 5));
    const norm_partials = @min(try mul(partials, 5), try mul(largest_partial, norm_count));
    if (norm_partials > limits.max_norm_partial_bytes) return error.TrainingOptimizerLimitExceeded;
    const pending_bytes = try mul(payload, if (stepped) 4 else 1);
    const temporaries = try add(try mul(largest, 2), if (count == 0) 0 else 64);
    const reduction = try add(norm_partials, try mul(norm_count, 24));
    const device = try add(pending_bytes, try add(temporaries, reduction));
    // Besides our explicitly bounded owner/slices, reserve backend CT/shape
    // descriptors and the legacy <=256 AdamW metadata stack. No page-allocator
    // fallback is reached by that batch size (11 arrays, 80 bytes per item).
    const metadata = try add(@sizeOf(Pending), try add(256 * 1024, try add(try mul(count, @sizeOf(Replacement) + 4096), batch_capacity * (@sizeOf(ops.resident_training.NormInput) + @sizeOf(ops.TrainingAdamWBatchInput)))));
    const transfers = try add(try mul(count, 144), if (count == 0) 0 else 64);
    if (device > limits.max_device_bytes or metadata > limits.max_host_metadata_bytes or transfers > limits.max_scalar_transfer_bytes)
        return error.TrainingOptimizerLimitExceeded;
    return .{ .selected_slots = count, .optimizer_stepped = stepped, .pending_tensor_bytes = pending_bytes, .temporary_tensor_bytes = temporaries, .reduction_scratch_bytes = reduction, .device_upper_bound_bytes = device, .host_metadata_upper_bound_bytes = metadata, .scalar_transfer_upper_bound_bytes = transfers, .total_work = try workUpperBound(state.slots.len, state.groups.len, count, selected_elements, norm_chunks, stepped) };
}

test "seeded device transaction work admission covers tensor gradients partial flush and reduction tails without dispatch" {
    // Opaque handles deliberately point to a byte, not a backend tensor. Only
    // immutable metadata is consulted; this must not allocate or dereference
    // even a single tensor while admitting a future optimizer transaction.
    var opaque_byte: u8 = 0;
    const handle: CT = &opaque_byte;
    var slots = [_]Slot{
        .{ .weight = handle, .grad_accum = handle, .m = handle, .v = handle, .shape = &.{1}, .group = 0, .adam_step = 0, .present = false },
        .{ .weight = handle, .grad_accum = handle, .m = handle, .v = handle, .shape = &.{1024}, .group = 0, .adam_step = 0, .present = false },
    };
    var state = State{ .identity = .{ .optimizer_step = 0, .microbatch_step = 1 }, .accumulated_microbatches = 1, .grad_accum_steps = 2, .slots = &slots, .groups = &.{.{ .schedule = .{ .constant = 0.1 } }} };
    const zeros = [_]Gradient{ .{ .slot = 0, .value = .zero }, .{ .slot = 1, .value = .zero } };
    const tensors = [_]Gradient{ .{ .slot = 0, .value = .{ .tensor = handle } }, .{ .slot = 1, .value = .{ .tensor = handle } } };
    var request = Request{ .expected = state.identity, .action = .submit, .loss = 0, .gradients = &zeros };
    const full = try estimate(state, request, .{});
    try std.testing.expect(full.optimizer_stepped);
    try std.testing.expect(full.total_work > 11 * 2 * 1025);
    request.gradients = &tensors;
    try std.testing.expectEqual(full.total_work, (try estimate(state, request, .{})).total_work);

    // The extra element requires a second norm chunk, including all padded
    // reduction lanes. It cannot be costed as only one more element visit.
    slots[1].shape = &.{1025};
    const tail = try estimate(state, request, .{});
    try std.testing.expect(tail.total_work - full.total_work > full.total_work / 1025);
    slots[1].shape = &.{1024};
    state.grad_accum_steps = 4;
    const accumulation = try estimate(state, request, .{});
    try std.testing.expect(!accumulation.optimizer_stepped);
    try std.testing.expect(accumulation.total_work < full.total_work);
    for (&slots) |*slot| slot.present = true;
    const partial = try estimate(state, .{ .expected = state.identity, .action = .flush }, .{});
    try std.testing.expect(partial.optimizer_stepped);
    try std.testing.expectEqual(full.total_work, partial.total_work);

    for (&slots) |*slot| slot.present = false;
    const absent = try estimate(state, .{ .expected = state.identity, .action = .submit, .loss = 0 }, .{});
    try std.testing.expectEqual(@as(usize, 0), absent.selected_slots);
    try std.testing.expect(absent.total_work > 0 and absent.total_work < accumulation.total_work);
    // Growing an unused tensor changes neither device work nor grad=None.
    slots[1].shape = &.{67_108_865};
    try std.testing.expectEqual(absent.total_work, (try estimate(state, .{ .expected = state.identity, .action = .submit, .loss = 0 }, .{})).total_work);
    request.gradients = &zeros;
    const wide = try estimate(state, request, .{});
    try std.testing.expect(wide.total_work > std.math.maxInt(u32));
    try std.testing.expectEqual(@as(u8, 0), opaque_byte);
}

test "seeded device transaction work admission rejects integer overflow" {
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, workUpperBound(std.math.maxInt(u64), 1, 1, 1, 1, true));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, workUpperBound(1, 1, 1, std.math.maxInt(u64), 1, true));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, workUpperBound(1, 1, 1, 1, std.math.maxInt(u64), true));
}

fn shapeOf(slot: Slot) ml.Shape {
    var result = ml.Shape{ .dtype = .f32, .rank_ = @intCast(slot.shape.len) };
    for (slot.shape, 0..) |dim, i| result.dims[i] = dim;
    return result;
}
fn instructionLimits(limits: Limits) ops.resident_program.Limits {
    return .{ .primitive = limits.primitive, .max_scratch_bytes = limits.max_device_bytes };
}
fn validateTensor(cb: *const ops.ComputeBackend, tensor: CT, shape: ml.Shape, limits: Limits) !void {
    const instruction = ops.resident_program.Instruction{
        .op = .{ .reshape = .{ .new_shape = shape } },
        .output = shape,
        .inputs = .{ shape, .{}, .{}, .{} },
        .num_inputs = 1,
    };
    // The strict reshape validates owner, exact dtype/shape, physical storage
    // and the absence of an external command frame; its lease is short-lived.
    const lease = try cb.residentTrainingInstruction(&instruction, &.{tensor}, instructionLimits(limits));
    cb.free(lease);
}
fn scalar(pending: *Pending, value: f32, limits: Limits) !CT {
    const result = try pending.backend.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{value}, .shape = &.{} } }, limits.primitive);
    pending.receipt.scalar_upload_bytes += 4;
    return result;
}
fn zero(pending: *Pending, shape: ml.Shape, scalar_zero: CT, limits: Limits) !CT {
    const instruction = ops.resident_program.Instruction{
        .op = .{ .broadcast_in_dim = .{ .target_shape = shape, .num_axes = 0 } },
        .output = shape,
        .inputs = .{ ml.Shape.init(.f32, &.{}), .{}, .{}, .{} },
        .num_inputs = 1,
    };
    const broadcast = try pending.backend.residentTrainingInstruction(&instruction, &.{scalar_zero}, instructionLimits(limits));
    defer pending.backend.free(broadcast);
    // Scalar broadcast can be a view for a scalar parameter. Ensure every
    // optimizer accumulator owns a physically independent mutable buffer.
    var dims: [8]i32 = undefined;
    for (shape.dims[0..shape.rank_], 0..) |dim, i| dims[i] = @intCast(dim);
    return pending.backend.residentTrainingPrimitive(&.{ .snapshot = .{ .input = broadcast, .shape = dims[0..shape.rank_] } }, limits.primitive);
}
fn binary(pending: *Pending, code: ml.OpCode, lhs: CT, lhs_shape: ml.Shape, rhs: CT, rhs_shape: ml.Shape, limits: Limits) !CT {
    const instruction = ops.resident_program.Instruction{ .op = code, .output = lhs_shape, .inputs = .{ lhs_shape, rhs_shape, .{}, .{} }, .num_inputs = 2 };
    return pending.backend.residentTrainingInstruction(&instruction, &.{ lhs, rhs }, instructionLimits(limits));
}
fn norm(pending: *Pending, values: []const ops.resident_training.NormInput, limits: Limits) !f64 {
    if (values.len == 0) return 0;
    var count: usize = 0;
    for (values) |value| count = try add(count, value.elem_count);
    const summary = try pending.backend.residentTrainingNorm(values, .{
        .primitive = limits.primitive,
        .max_tensors = batch_capacity,
        .max_total_elements = count,
        .max_partial_bytes = limits.max_norm_partial_bytes,
    });
    pending.receipt.scalar_download_bytes += summary.download_bytes;
    if (!summary.finite or !std.math.isFinite(summary.sum_squares) or summary.sum_squares < 0 or
        summary.tensor_count != values.len or summary.download_bytes != values.len * 12)
        return error.NonFiniteTrainingUpdate;
    return summary.sum_squares;
}
fn appendNorm(pending: *Pending, buffer: []ops.resident_training.NormInput, used: *usize, tensor: CT, count: usize, limits: Limits) !void {
    buffer[used.*] = .{ .tensor = tensor, .elem_count = count };
    used.* += 1;
    if (used.* == buffer.len) {
        _ = try norm(pending, buffer, limits);
        used.* = 0;
    }
}

const CombinedControl = struct {
    original: ?Control,
    request: ?Control,
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.original) |original| try original.check();
        if (self.request) |request| try request.check();
    }
    fn control(self: *@This()) Control {
        var result = self.request orelse self.original orelse Control{};
        if (self.original) |original| {
            if (result.io == null) result.io = original.io;
            if (result.hard_cancellation == null) result.hard_cancellation = original.hard_cancellation;
            if (result.progress == null) result.progress = original.progress;
            if (original.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        // A callback-only replacement loses the process interruption boundary
        // while private buffers are submitted or synchronously drained.
        result.ptr = self;
        result.check_fn = check;
        return result;
    }
};

test "seeded device transaction control preserves worker carrier both cancellation sources and earliest deadline" {
    const execution = @import("../execution_control.zig");
    const Probe = struct {
        calls: usize = 0,
        cancelled: bool = false,
        failure: ?anyerror = null,
        monitor: ?execution.MonitorControl = null,
        arms: usize = 0,
        disarms: usize = 0,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.failure) |err| return err;
        }
        fn isCancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled;
        }
        fn arm(raw: *anyopaque, observed: execution.MonitorControl) anyerror!u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.monitor = observed;
            self.arms += 1;
            return 7;
        }
        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 7 and self.monitor != null);
            self.monitor = null;
            self.disarms += 1;
        }
    };
    var original = Probe{};
    var requested = Probe{};
    const future = std.math.maxInt(u64);
    var base = Control{
        .io = std.testing.io,
        .deadline_ns = future - 2,
        .ptr = &original,
        .check_fn = Probe.check,
        .cancellation = .{ .ptr = &original, .is_cancelled_fn = Probe.isCancelled },
        .hard_cancellation = .{ .ptr = &original, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm },
    };
    var request = Control{
        .deadline_ns = future - 1,
        .ptr = &requested,
        .check_fn = Probe.check,
        .cancellation = .{ .ptr = &requested, .is_cancelled_fn = Probe.isCancelled },
    };
    var combined = CombinedControl{ .original = base, .request = request };
    var active = combined.control();
    try std.testing.expectEqual(std.testing.io, active.io.?);
    try std.testing.expectEqual(future - 2, active.deadline_ns.?);
    {
        // This exercises the real process-required guard/monitor contract
        // without a process, thread, device, timer or tensor allocation.
        var guard = try active.enterUninterruptible(.process_required);
        defer guard.deinit();
        const monitor = original.monitor.?;
        try std.testing.expectEqual(future - 2, monitor.deadline_ns.?);
        try monitor.check();
        try std.testing.expect(original.calls > 0 and requested.calls > 0);
        original.failure = error.OriginalControlStopped;
        try std.testing.expectError(error.OriginalControlStopped, monitor.check());
        original.failure = null;
        requested.failure = error.RequestControlStopped;
        try std.testing.expectError(error.RequestControlStopped, monitor.check());
        requested.failure = null;
        original.cancelled = true;
        try std.testing.expectError(error.Cancelled, monitor.check());
        original.cancelled = false;
        requested.cancelled = true;
        try std.testing.expectError(error.Cancelled, monitor.check());
        requested.cancelled = false;
    }
    try std.testing.expectEqual(@as(usize, 1), original.arms);
    try std.testing.expectEqual(@as(usize, 1), original.disarms);

    // Explicit request carriers take precedence; a missing carrier above
    // inherited the original owner. Never discard the earlier deadline.
    request.io = .{ .userdata = &requested, .vtable = std.testing.io.vtable };
    request.hard_cancellation = .{ .ptr = &requested, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm };
    request.deadline_ns = future - 3;
    combined.request = request;
    active = combined.control();
    try std.testing.expectEqual(request.io.?, active.io.?);
    try std.testing.expectEqual(future - 3, active.deadline_ns.?);
    {
        var guard = try active.enterUninterruptible(.process_required);
        defer guard.deinit();
        try requested.monitor.?.check();
    }
    try std.testing.expectEqual(@as(usize, 1), requested.arms);
    try std.testing.expectEqual(@as(usize, 1), requested.disarms);
    base.deadline_ns = 0;
    combined.original = base;
    active = combined.control();
    try std.testing.expectEqual(@as(u64, 0), active.deadline_ns.?);
    try std.testing.expectError(error.Timeout, active.check());
    combined.original = null;
    combined.request = .{};
    try std.testing.expectError(error.ProcessIsolationRequired, combined.control().enterUninterruptible(.process_required));
}

/// Prepare independent resident replacements without modifying any borrowed
/// state. Metal's AdamWMany ABI outside an external frame calls
/// finish_command_buffer (metal_kernels.m), which submits and waits before
/// returning, including on device failure. Every batch is <=256 items and is
/// followed by a cancellation check. trainingSynchronize is intentionally not
/// used: its Metal vtable entry is optional and cannot prove completion.
pub fn prepare(a: Allocator, cb: *const ops.ComputeBackend, state: State, request: Request, limits: Limits, control: ?Control) !*Pending {
    var combined = CombinedControl{ .original = cb.execution_control, .request = control };
    try CombinedControl.check(&combined);
    const admission = try estimate(state, request, limits);
    if (cb.kind() != .metal or cb.vtable.residentTrainingPrimitive == null or cb.vtable.residentTrainingInstruction == null or
        cb.vtable.residentTrainingNorm == null or cb.vtable.trainingAdamWManyF32 == null)
        return error.UnsupportedDeviceOptimizerBackend;
    const pending = try a.create(Pending);
    pending.* = .{ .backing = a, .budget = .{ .backing = a, .limit = limits.max_host_metadata_bytes - @sizeOf(Pending) }, .backend = cb.*, .replacements = &.{}, .estimate = admission, .receipt = .{ .identity = .{
        .optimizer_step = state.identity.optimizer_step + @intFromBool(admission.optimizer_stepped),
        .microbatch_step = state.identity.microbatch_step + @intFromBool(request.action == .submit),
    }, .accumulated_microbatches = if (admission.optimizer_stepped) 0 else nextAccumulation(state, request), .optimizer_stepped = admission.optimizer_stepped, .grad_norm = 0, .loss = request.loss, .selected_slots = admission.selected_slots } };
    pending.backend.execution_control = combined.control();
    errdefer pending.deinit();
    const scratch = pending.budget.allocator();
    pending.replacements = try scratch.alloc(Replacement, admission.selected_slots);
    for (pending.replacements) |*replacement| replacement.* = .{ .slot = 0, .grad_accum = null, .adam_step = 0, .present = false };
    const norms = try scratch.alloc(ops.resident_training.NormInput, batch_capacity);
    defer scratch.free(norms);
    const adam = try scratch.alloc(ops.TrainingAdamWBatchInput, batch_capacity);
    defer scratch.free(adam);

    // Complete physical shape/ownership checks for all participants before
    // allocating their replacement payloads or encoding optimizer work.
    var norm_used: usize = 0;
    var selected_index: usize = 0;
    for (state.slots, 0..) |slot, i| {
        if (!selected(state, request, i)) continue;
        const shape = shapeOf(slot);
        const count = try elements(slot, limits);
        for ([_]CT{ slot.weight, slot.grad_accum, slot.m, slot.v }) |tensor| {
            try validateTensor(&pending.backend, tensor, shape, limits);
            try appendNorm(pending, norms, &norm_used, tensor, count, limits);
        }
        if (!slot.present and try norm(pending, &.{.{ .tensor = slot.grad_accum, .elem_count = count }}, limits) != 0)
            return error.InvalidDeviceOptimizerState;
        if (incoming(request, i)) |gradient| if (gradient.value == .tensor) {
            try validateTensor(&pending.backend, gradient.value.tensor, shape, limits);
            try appendNorm(pending, norms, &norm_used, gradient.value.tensor, count, limits);
        };
        pending.replacements[selected_index] = .{ .slot = i, .grad_accum = null, .adam_step = slot.adam_step + @as(u32, @intFromBool(admission.optimizer_stepped)), .present = !admission.optimizer_stepped };
        selected_index += 1;
    }
    _ = try norm(pending, norms[0..norm_used], limits);
    if (admission.selected_slots != 0) {
        const scalar_shape = ml.Shape.init(.f32, &.{});
        const divisor: f32 = @floatFromInt(state.grad_accum_steps);
        const renormalize: f32 = if (admission.optimizer_stepped and nextAccumulation(state, request) < state.grad_accum_steps)
            divisor / @as(f32, @floatFromInt(nextAccumulation(state, request)))
        else
            1;
        const divisor_tensor = try scalar(pending, divisor, limits);
        defer pending.backend.free(divisor_tensor);
        const renormalize_tensor = try scalar(pending, renormalize, limits);
        defer pending.backend.free(renormalize_tensor);
        const zero_tensor = try scalar(pending, 0, limits);
        defer pending.backend.free(zero_tensor);
        for (pending.replacements) |*replacement| {
            const slot = state.slots[replacement.slot];
            const shape = shapeOf(slot);
            replacement.grad_accum = if (slot.present)
                try pending.backend.residentTrainingPrimitive(&.{ .snapshot = .{ .input = slot.grad_accum, .shape = slot.shape } }, limits.primitive)
            else
                try zero(pending, shape, zero_tensor, limits);
            if (incoming(request, replacement.slot)) |gradient| if (gradient.value == .tensor) {
                const scaled = try binary(pending, .div, gradient.value.tensor, shape, divisor_tensor, scalar_shape, limits);
                defer pending.backend.free(scaled);
                const next = try binary(pending, .add, replacement.grad_accum.?, shape, scaled, shape, limits);
                pending.backend.free(replacement.grad_accum.?);
                replacement.grad_accum = next;
            };
            if (renormalize != 1) {
                const next = try binary(pending, .mul, replacement.grad_accum.?, shape, renormalize_tensor, scalar_shape, limits);
                pending.backend.free(replacement.grad_accum.?);
                replacement.grad_accum = next;
            }
        }
        var sum_squares: f64 = 0;
        norm_used = 0;
        for (pending.replacements) |replacement| {
            norms[norm_used] = .{ .tensor = replacement.grad_accum.?, .elem_count = try elements(state.slots[replacement.slot], limits) };
            norm_used += 1;
            if (norm_used == norms.len) {
                sum_squares += try norm(pending, norms, limits);
                norm_used = 0;
            }
        }
        sum_squares += try norm(pending, norms[0..norm_used], limits);
        if (!std.math.isFinite(sum_squares)) return error.NonFiniteTrainingUpdate;
        pending.receipt.grad_norm = @sqrt(sum_squares);
        if (admission.optimizer_stepped) {
            const clip: f32 = if (state.max_grad_norm > 0 and pending.receipt.grad_norm > state.max_grad_norm)
                @floatCast(state.max_grad_norm / (pending.receipt.grad_norm + 1e-6))
            else
                1;
            const clip_tensor = try scalar(pending, clip, limits);
            defer pending.backend.free(clip_tensor);
            // Snapshot every participating weight/moment before the first
            // mutating batch. No source-state CT is passed to AdamWMany.
            for (pending.replacements) |*replacement| {
                const slot = state.slots[replacement.slot];
                if (clip != 1) {
                    const next = try binary(pending, .mul, replacement.grad_accum.?, shapeOf(slot), clip_tensor, scalar_shape, limits);
                    pending.backend.free(replacement.grad_accum.?);
                    replacement.grad_accum = next;
                }
                replacement.weight = try pending.backend.residentTrainingPrimitive(&.{ .snapshot = .{ .input = slot.weight, .shape = slot.shape } }, limits.primitive);
                replacement.m = try pending.backend.residentTrainingPrimitive(&.{ .snapshot = .{ .input = slot.m, .shape = slot.shape } }, limits.primitive);
                replacement.v = try pending.backend.residentTrainingPrimitive(&.{ .snapshot = .{ .input = slot.v, .shape = slot.shape } }, limits.primitive);
            }
            for (state.groups, 0..) |group, group_id| {
                var used: usize = 0;
                for (pending.replacements) |replacement| {
                    const slot = state.slots[replacement.slot];
                    if (slot.group != group_id) continue;
                    const t: f32 = @floatFromInt(replacement.adam_step);
                    adam[used] = .{ .weight = replacement.weight.?, .grad = replacement.grad_accum.?, .m = replacement.m.?, .v = replacement.v.?, .elem_count = try elements(slot, limits), .bias_correction1 = 1 - std.math.pow(f32, group.optimizer.beta1, t), .bias_correction2 = 1 - std.math.pow(f32, group.optimizer.beta2, t) };
                    used += 1;
                    if (used == adam.len) {
                        try adamBatch(pending, adam, group, state.identity.optimizer_step);
                        used = 0;
                    }
                }
                try adamBatch(pending, adam[0..used], group, state.identity.optimizer_step);
            }
            norm_used = 0;
            for (pending.replacements) |replacement| {
                const count = try elements(state.slots[replacement.slot], limits);
                for ([_]CT{ replacement.weight.?, replacement.m.?, replacement.v.? }) |tensor|
                    try appendNorm(pending, norms, &norm_used, tensor, count, limits);
            }
            _ = try norm(pending, norms[0..norm_used], limits);
            // AdamW's ABI clears its private gradient buffer. Prove this
            // invariant before exposing it as the next accumulation buffer.
            norm_used = 0;
            for (pending.replacements) |replacement| {
                norms[norm_used] = .{ .tensor = replacement.grad_accum.?, .elem_count = try elements(state.slots[replacement.slot], limits) };
                norm_used += 1;
                if (norm_used == norms.len) {
                    if (try norm(pending, norms, limits) != 0) return error.InvalidDeviceOptimizerResult;
                    norm_used = 0;
                }
            }
            if (try norm(pending, norms[0..norm_used], limits) != 0) return error.InvalidDeviceOptimizerResult;
        }
    }
    if (pending.receipt.scalar_upload_bytes + pending.receipt.scalar_download_bytes > admission.scalar_transfer_upper_bound_bytes)
        return error.InvalidDeviceOptimizerResult;
    try pending.backend.checkExecutionControl();
    pending.backend.execution_control = null;
    return pending;
}

fn adamBatch(pending: *Pending, values: []const ops.TrainingAdamWBatchInput, group: Group, step: u64) !void {
    if (values.len == 0) return;
    std.debug.assert(values.len <= batch_capacity);
    try pending.backend.checkExecutionControl();
    try pending.backend.trainingAdamWManyF32(values, .{
        .lr = group.schedule.lr(@intCast(step)),
        .beta1 = group.optimizer.beta1,
        .beta2 = group.optimizer.beta2,
        .eps = group.optimizer.eps,
        .weight_decay = group.optimizer.weight_decay,
        .grad_scale = 1,
    });
    try pending.backend.checkExecutionControl();
}
