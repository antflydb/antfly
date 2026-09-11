// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Host optimizer integration for explicit, independently computed VJPs.
//! RealAutodiffTrainer remains the sole owner of parameter slots and Adam
//! state. Every accumulation/update is staged and validated before publication.
const std = @import("std");
const ml = @import("ml").graph;
const optimizers = ml.optimizers;
const real = @import("real_autodiff_trainer.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const safetensors = @import("../models/safetensors.zig");
const checkpoint = @import("safetensors_checkpoint.zig");
const compat = @import("../io/compat.zig");
const Allocator = std.mem.Allocator;
const snapshot_file = @import("../runtime/file_snapshot.zig");
const device_transaction = @import("seeded_device_transaction.zig");
const device_state = @import("seeded_device_state.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;

pub const Group = struct { optimizer: optimizers.AdamWConfig = .{}, schedule: optimizers.LearningRateSchedule };
pub const Parameter = struct { name: []const u8, values: []const f32, dimensions: []const i32, group: usize };
pub const Gradient = struct { name: []const u8, values: []const f32 };
pub const ResidentGradient = struct { name: []const u8, value: union(enum) { zero, tensor: ops.CT } };
pub const Execution = enum { native, resident_metal };
pub const Identity = struct { optimizer_step: u64, microbatch_step: u64 };
pub const RestoreValidation = struct {
    context: ?*const anyopaque,
    validate: *const fn (?*const anyopaque, Identity, u32) anyerror!void,
    expected_state_sha256: ?[32]u8 = null,
};
pub const RestoreReceipt = struct {
    version: u32 = 1,
    /// Digest of the validated owned state actually published, independent of
    /// checkpoint path, JSON ordering, and unconsumed extension tensors.
    state_sha256: [32]u8,
    identity: Identity,
    accumulated_microbatches: u32,
    checkpoint: snapshot_file.Digest,
};
pub const Limits = struct {
    max_parameters: usize = 4096,
    max_groups: usize = 64,
    max_parameter_name_bytes: usize = 1024,
    max_state_bytes: usize = 4 * 1024 * 1024 * 1024,
    max_transaction_bytes: usize = 4 * 1024 * 1024 * 1024,
    max_checkpoint_header_bytes: usize = 4 * 1024 * 1024,
    max_checkpoint_header_heap_bytes: usize = 64 * 1024 * 1024,
};
pub const Config = struct {
    groups: []const Group,
    grad_accum_steps: u32 = 1,
    max_grad_norm: f32 = 1,
    limits: Limits = .{},
    execution: Execution = .native,
};
pub const Result = struct { loss: ?f32, optimizer_stepped: bool, grad_norm: f64, identity: Identity, accumulated_microbatches: u32 };

pub const NativeUpdateAdmission = struct {
    parameters: usize,
    elements: usize,
    metadata_bytes: usize,
    payload_bytes: usize,
    /// Additional allocator-visible host bytes, excluding the existing owner
    /// state and caller-owned incoming gradient/readback arrays.
    host_upper_bound_bytes: usize,
    /// Conservative scalar-element/byte visits, including bounded schedule
    /// work and worst-case name lookup. This is not elapsed CPU time.
    total_work: u64,
};

const NativePending = struct {
    gradient: []f32,
    weights: ?[]f32 = null,
    m: ?[]f32 = null,
    v: ?[]f32 = null,

    fn deinit(self: NativePending, a: Allocator) void {
        if (self.v) |values| a.free(values);
        if (self.m) |values| a.free(values);
        if (self.weights) |values| a.free(values);
        a.free(self.gradient);
    }
};

fn nativeMetadataBytes(count: usize) !usize {
    return std.math.mul(usize, count, @sizeOf(?[]const f32) + @sizeOf(?NativePending)) catch error.TrainingOptimizerLimitExceeded;
}

fn nativeUpdateEstimate(count: usize, elements: usize, name_bytes: usize, limit: usize) !NativeUpdateAdmission {
    const metadata = try nativeMetadataBytes(count);
    // Every slot can enter with an explicit zero, or remain present from an
    // earlier microbatch. A full/partial flush owns gradient + weights + m + v
    // simultaneously. Direct reclaiming allocations introduce no arena slack.
    const payload = std.math.mul(usize, elements, 4 * @sizeOf(f32)) catch return error.TrainingOptimizerLimitExceeded;
    const total = std.math.add(usize, metadata, payload) catch return error.TrainingOptimizerLimitExceeded;
    if (total > limit) return error.TrainingOptimizerLimitExceeded;
    const element_work = std.math.mul(u64, elements, 64) catch return error.TrainingOptimizerLimitExceeded;
    // Incoming lookup scans at most count names per supplied slot. Also cover
    // optimizer-state hash/probe bytes and per-slot schedule/commit bookkeeping.
    const lookup_work = std.math.mul(u64, count, std.math.add(u64, name_bytes, count) catch return error.TrainingOptimizerLimitExceeded) catch return error.TrainingOptimizerLimitExceeded;
    const name_work = std.math.mul(u64, lookup_work, 4) catch return error.TrainingOptimizerLimitExceeded;
    const scalar_work = std.math.mul(u64, count, 1024) catch return error.TrainingOptimizerLimitExceeded;
    const work = std.math.add(u64, element_work, std.math.add(u64, name_work, scalar_work) catch return error.TrainingOptimizerLimitExceeded) catch return error.TrainingOptimizerLimitExceeded;
    return .{ .parameters = count, .elements = elements, .metadata_bytes = metadata, .payload_bytes = payload, .host_upper_bound_bytes = total, .total_work = work };
}

fn nativeAllocationFailed(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
    const last: *?Budget.AllocationFailure = @ptrCast(@alignCast(raw.?));
    last.* = failure;
}

fn finite(values: []const f32) !void {
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteTrainingUpdate;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
const CombinedControl = struct {
    primary: ?Control,
    secondary: ?Control,
    fn checkBoth(raw: ?*anyopaque) !void {
        const self: *CombinedControl = @ptrCast(@alignCast(raw.?));
        try check(self.primary);
        try check(self.secondary);
    }
    fn control(self: *@This()) Control {
        var result = self.secondary orelse self.primary orelse Control{};
        if (self.primary) |primary| {
            if (result.io == null) result.io = primary.io;
            if (result.hard_cancellation == null) result.hard_cancellation = primary.hard_cancellation;
            if (result.progress == null) result.progress = primary.progress;
            if (primary.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        // Both full controls remain borrowed until every synchronous binding
        // operation/guard completes. Retain their worker/IO carrier as well as
        // callbacks: process-required primitives must still arm the watchdog.
        result.ptr = self;
        result.check_fn = checkBoth;
        return result;
    }
};

test "seeded gradient trainer binding control preserves worker carrier both cancellation sources and earliest deadline" {
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
    var combined = CombinedControl{ .primary = base, .secondary = request };
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
    combined.secondary = request;
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
    combined.primary = base;
    active = combined.control();
    try std.testing.expectEqual(@as(u64, 0), active.deadline_ns.?);
    try std.testing.expectError(error.Timeout, active.check());
    combined.primary = null;
    combined.secondary = .{};
    try std.testing.expectError(error.ProcessIsolationRequired, combined.control().enterUninterruptible(.process_required));
}

fn validateSchedule(schedule: optimizers.LearningRateSchedule) !void {
    switch (schedule) {
        .constant => |value| if (!std.math.isFinite(value) or value < 0) return error.InvalidOptimizerGroup,
        inline else => |value| {
            if (value.total_steps == 0) return error.InvalidOptimizerGroup;
            inline for (@typeInfo(@TypeOf(value)).@"struct".fields) |field| {
                if (field.type == f32) {
                    const number = @field(value, field.name);
                    if (!std.math.isFinite(number) or number < 0) return error.InvalidOptimizerGroup;
                }
            }
            if (@hasField(@TypeOf(value), "warmup_steps") and value.warmup_steps > value.total_steps) return error.InvalidOptimizerGroup;
        },
    }
}

pub const Trainer = struct {
    owner: real.RealAutodiffTrainer,
    groups: []Group,
    group_ids: []usize,
    present: []bool,
    limits: Limits,
    active_binding: bool = false,
    last_restore_receipt: ?RestoreReceipt = null,
    execution: Execution = .native,
    /// The device epoch is authoritative in resident mode. Host mirrors are
    /// certified only after a complete bounded checkpoint/export readback.
    host_mirrors_current: bool = true,
    last_device_receipt: ?device_transaction.Receipt = null,

    pub fn init(a: Allocator, cb: *const ops.ComputeBackend, parameters: []const Parameter, config: Config) !Trainer {
        var result = try initOwnedHost(a, cb, parameters, config);
        errdefer result.deinit();
        if (config.execution == .resident_metal) try result.initializeDevice(cb.execution_control);
        return result;
    }

    fn initOwnedHost(a: Allocator, cb: *const ops.ComputeBackend, parameters: []const Parameter, config: Config) !Trainer {
        if ((config.execution == .native and cb.kind() != .native) or (config.execution == .resident_metal and cb.kind() != .metal)) return error.UnsupportedSeededTrainingBackend;
        if (parameters.len == 0 or parameters.len > config.limits.max_parameters or config.groups.len == 0 or config.groups.len > config.limits.max_groups or config.grad_accum_steps == 0 or config.grad_accum_steps > 65536 or !std.math.isFinite(config.max_grad_norm) or config.max_grad_norm < 0) return error.InvalidSeededTrainerConfig;
        for (config.groups) |group| {
            const o = group.optimizer;
            if (!std.math.isFinite(o.beta1) or o.beta1 < 0 or o.beta1 >= 1 or !std.math.isFinite(o.beta2) or o.beta2 < 0 or o.beta2 >= 1 or !std.math.isFinite(o.eps) or o.eps <= 0 or !std.math.isFinite(o.weight_decay) or o.weight_decay < 0) return error.InvalidOptimizerGroup;
            try validateSchedule(group.schedule);
        }
        var bytes: usize = 0;
        var largest_upload: usize = 0;
        for (parameters, 0..) |parameter, i| {
            if (parameter.name.len == 0 or parameter.name.len > config.limits.max_parameter_name_bytes or parameter.dimensions.len > 8 or parameter.group >= config.groups.len) return error.InvalidTrainingParameter;
            for (parameter.name) |byte| if (byte < 32 or byte == 127) return error.InvalidTrainingParameter;
            for (parameters[0..i]) |prior| if (std.mem.eql(u8, prior.name, parameter.name)) return error.DuplicateTrainingParameter;
            var elements: usize = 1;
            for (parameter.dimensions) |dim| {
                if (dim <= 0) return error.InvalidTrainingParameter;
                elements = std.math.mul(usize, elements, @intCast(dim)) catch return error.TrainingOptimizerLimitExceeded;
            }
            if (elements != parameter.values.len) return error.InvalidTrainingParameter;
            try finite(parameter.values);
            bytes = std.math.add(usize, bytes, std.math.mul(usize, elements, @as(usize, if (config.execution == .resident_metal) 8 else 4) * @sizeOf(f32)) catch return error.TrainingOptimizerLimitExceeded) catch return error.TrainingOptimizerLimitExceeded;
            largest_upload = @max(largest_upload, std.math.mul(usize, elements, @sizeOf(f32)) catch return error.TrainingOptimizerLimitExceeded);
            bytes = std.math.add(usize, bytes, parameter.name.len * 2 + parameter.dimensions.len * @sizeOf(i32) + 1024) catch return error.TrainingOptimizerLimitExceeded;
        }
        if (config.execution == .resident_metal) bytes = std.math.add(usize, bytes, @max(largest_upload, 2 * 1024 * 1024)) catch return error.TrainingOptimizerLimitExceeded;
        if (bytes > config.limits.max_state_bytes) return error.TrainingOptimizerLimitExceeded;
        var owner = try real.RealAutodiffTrainer.init(a, cb, .{ .lora = .{ .rank = 1, .target_patterns = &.{} }, .optimizer = config.groups[0].optimizer, .lr_schedule = config.groups[0].schedule, .grad_accum_steps = config.grad_accum_steps, .max_grad_norm = config.max_grad_norm });
        errdefer owner.deinit();
        for (parameters) |parameter| {
            const name = try a.dupe(u8, parameter.name);
            errdefer a.free(name);
            const weights = try a.dupe(f32, parameter.values);
            errdefer a.free(weights);
            const accum = try a.alloc(f32, weights.len);
            errdefer a.free(accum);
            @memset(accum, 0);
            const dims = try a.dupe(i32, parameter.dimensions);
            errdefer a.free(dims);
            _ = try owner.optimizer_state.getOrCreate(name, weights.len, true);
            try owner.regular_params.append(a, .{ .name = name, .weights = weights, .grad_accum = accum, .dims = dims, .node_id = ml.null_node });
        }
        const groups = try a.dupe(Group, config.groups);
        errdefer a.free(groups);
        const group_ids = try a.alloc(usize, parameters.len);
        errdefer a.free(group_ids);
        for (parameters, group_ids) |parameter, *id| id.* = parameter.group;
        const present = try a.alloc(bool, parameters.len);
        @memset(present, false);
        return .{ .owner = owner, .groups = groups, .group_ids = group_ids, .present = present, .limits = config.limits, .execution = config.execution };
    }

    pub fn deinit(self: *Trainer) void {
        std.debug.assert(!self.active_binding);
        const a = self.owner.allocator;
        a.free(self.groups);
        a.free(self.group_ids);
        a.free(self.present);
        self.owner.deinit();
        self.* = undefined;
    }

    pub fn identity(self: *const Trainer) Identity {
        return .{ .optimizer_step = self.owner.optimizer_step_count, .microbatch_step = self.owner.step_count };
    }

    /// Atomically persist parameters, moments, presence, and an unfinished
    /// accumulation window through the existing durable checkpoint owner.
    pub fn save(self: *Trainer, path: []const u8, run: [32]u8, control: ?Control) !void {
        try check(control);
        if (self.active_binding) return error.TrainingTapeStillLive;
        try self.ensureHostState(control);
        const a = self.owner.allocator;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var tensors = std.ArrayListUnmanaged(checkpoint.NamedTensor).empty;
        const counters = [_]f32{ 1, @floatFromInt(self.owner.accum_count), @floatFromInt(self.owner.config.grad_accum_steps) };
        try tensors.append(scratch, .{ .name = "__extension.seeded.counters", .data = &counters, .shape = &.{3} });
        const presence = try scratch.alloc(f32, self.present.len);
        for (self.present, presence) |present, *value| value.* = if (present) 1 else 0;
        try tensors.append(scratch, .{ .name = "__extension.seeded.presence", .data = presence, .shape = try scratch.dupe(usize, &.{presence.len}) });
        for (self.owner.regular_params.items, self.present, 0..) |slot, present, i| {
            const state = self.owner.optimizer_state.param_states.get(slot.name) orelse return error.InvalidOptimizerState;
            try finite(slot.weights);
            try finite(slot.grad_accum);
            try finite(state.m);
            try finite(state.v);
            if (state.step_count != slot.adam_step_count or state.step_count > self.owner.optimizer_step_count) return error.InvalidOptimizerState;
            if (self.owner.accum_count == 0 and present) return error.InvalidTrainingAccumulation;
            if (!present) for (slot.grad_accum) |value| if (value != 0) return error.InvalidTrainingAccumulation;
            const name = try std.fmt.allocPrint(scratch, "__extension.seeded.gradient.{d}", .{i});
            try tensors.append(scratch, .{ .name = name, .data = slot.grad_accum, .shape = try scratch.dupe(usize, &.{slot.grad_accum.len}) });
            try check(control);
        }
        const digest = try self.fingerprint(run);
        try check(control);
        try self.owner.saveTrainingStateWithExtensions(path, &digest, null, .{ .tensors = tensors.items, .accumulation_count = self.owner.accum_count, .execution_control = control, .use_synchronized_host_state = self.execution == .resident_metal });
    }

    /// Restore into a fresh staged owner and swap only after every field has
    /// passed validation. A corrupt checkpoint cannot half-restore live state.
    pub fn restore(self: *Trainer, path: []const u8, run: [32]u8, control: ?Control) !void {
        return self.restoreValidated(path, run, control, null);
    }

    /// A managed run may impose additional cursor invariants before the staged
    /// owner is published. Rejection leaves the prior live state intact.
    pub fn restoreValidated(self: *Trainer, path: []const u8, run: [32]u8, control: ?Control, validation: ?RestoreValidation) !void {
        try check(control);
        if (self.active_binding) return error.TrainingTapeStillLive;
        const a = self.owner.allocator;
        const parameters = try a.alloc(Parameter, self.owner.regular_params.items.len);
        defer a.free(parameters);
        var staging_bytes: usize = 0;
        var largest_upload: usize = 0;
        for (self.owner.regular_params.items, self.group_ids, parameters) |slot, group, *parameter| {
            parameter.* = .{ .name = slot.name, .values = slot.weights, .dimensions = slot.dims, .group = group };
            staging_bytes = std.math.add(usize, staging_bytes, std.math.mul(usize, slot.weights.len, @as(usize, if (self.execution == .resident_metal) 8 else 4) * @sizeOf(f32)) catch return error.TrainingOptimizerLimitExceeded) catch return error.TrainingOptimizerLimitExceeded;
            largest_upload = @max(largest_upload, std.math.mul(usize, slot.weights.len, @sizeOf(f32)) catch return error.TrainingOptimizerLimitExceeded);
            staging_bytes = std.math.add(usize, staging_bytes, slot.name.len * 2 + slot.dims.len * @sizeOf(i32) + 1024) catch return error.TrainingOptimizerLimitExceeded;
        }
        if (self.execution == .resident_metal) staging_bytes = std.math.add(usize, staging_bytes, @max(largest_upload, 2 * 1024 * 1024)) catch return error.TrainingOptimizerLimitExceeded;
        const reserved = std.math.add(usize, staging_bytes, self.limits.max_checkpoint_header_heap_bytes) catch return error.TrainingOptimizerLimitExceeded;
        if (reserved >= self.limits.max_transaction_bytes) return error.TrainingOptimizerLimitExceeded;
        const file = try snapshot_file.openRegular(compat.io(), compat.cwd(), path, control);
        defer file.close(compat.io());
        var prefix: [8]u8 = undefined;
        if (try file.readPositionalAll(compat.io(), &prefix, 0) != prefix.len) return error.FileTooSmall;
        if (std.mem.readInt(u64, &prefix, .little) > self.limits.max_checkpoint_header_bytes) return error.TrainingOptimizerLimitExceeded;
        const snapshot = snapshot_file.readOpened(a, compat.io(), file, self.limits.max_transaction_bytes - reserved, control) catch |err| {
            if (err == error.SnapshotLimitExceeded) return error.TrainingOptimizerLimitExceeded;
            return err;
        };
        defer a.free(snapshot);
        var snapshot_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var snapshot_offset: usize = 0;
        while (snapshot_offset < snapshot.len) {
            try check(control);
            const end = @min(snapshot.len, snapshot_offset +| (256 * 1024));
            snapshot_hash.update(snapshot[snapshot_offset..end]);
            snapshot_offset = end;
        }
        var header_budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = self.limits.max_checkpoint_header_heap_bytes };
        var reader = safetensors.MMapReader.fromBorrowedBytesLimited(header_budget.allocator(), snapshot, self.limits.max_checkpoint_header_bytes) catch |err| {
            if (err == error.FileTooLarge or err == error.HeaderTooLarge or header_budget.denied) return error.TrainingOptimizerLimitExceeded;
            return err;
        };
        defer reader.deinit();
        try check(control);
        var staged = try initOwnedHost(a, self.owner.compute_backend, parameters, .{ .groups = self.groups, .grad_accum_steps = self.owner.config.grad_accum_steps, .max_grad_norm = self.owner.config.max_grad_norm, .limits = self.limits, .execution = self.execution });
        errdefer staged.deinit();
        staged.owner.optimizer_state.deinit();
        staged.owner.optimizer_state = optimizers.OptimizerState.init(a);
        const digest = try self.fingerprint(run);
        try staged.owner.loadTrainingStateFromReader(&reader, &digest);
        var counters = try reader.readTensor("__extension.seeded.counters");
        defer counters.deinit();
        if (counters.dtype != .f32 or counters.asFloat32().len != 3) return error.InvalidTrainingAccumulation;
        const c = counters.asFloat32();
        try finite(c);
        if (c[0] != 1 or c[1] < 0 or c[1] != @floor(c[1]) or c[1] >= @as(f32, @floatFromInt(staged.owner.config.grad_accum_steps)) or c[2] != @as(f32, @floatFromInt(staged.owner.config.grad_accum_steps))) return error.InvalidTrainingAccumulation;
        staged.owner.accum_count = @intFromFloat(c[1]);
        if (staged.owner.accum_count > staged.owner.step_count) return error.InvalidTrainingAccumulation;
        var presence = try reader.readTensor("__extension.seeded.presence");
        defer presence.deinit();
        if (presence.dtype != .f32 or presence.asFloat32().len != staged.present.len) return error.InvalidTrainingAccumulation;
        for (staged.owner.regular_params.items, staged.present, presence.asFloat32(), 0..) |slot, *present, value, i| {
            if (value != 0 and value != 1) return error.InvalidTrainingAccumulation;
            present.* = value == 1;
            if (staged.owner.accum_count == 0 and present.*) return error.InvalidTrainingAccumulation;
            const name = try std.fmt.allocPrint(a, "__extension.seeded.gradient.{d}", .{i});
            defer a.free(name);
            var gradient = try reader.readTensor(name);
            defer gradient.deinit();
            if (gradient.dtype != .f32 or gradient.asFloat32().len != slot.grad_accum.len) return error.TrainingBindingShapeMismatch;
            try finite(gradient.asFloat32());
            if (!present.*) for (gradient.asFloat32()) |element| if (element != 0) return error.InvalidTrainingAccumulation;
            @memcpy(slot.grad_accum, gradient.asFloat32());
            const state = staged.owner.optimizer_state.param_states.get(slot.name).?;
            if (state.step_count != slot.adam_step_count or state.step_count > staged.owner.optimizer_step_count) return error.InvalidOptimizerState;
            try finite(slot.weights);
            try finite(state.m);
            try finite(state.v);
            for (state.v) |element| if (element < 0) return error.InvalidOptimizerState;
            try check(control);
        }
        if (validation) |v| try v.validate(v.context, staged.identity(), staged.owner.accum_count);
        const state_digest = try staged.stateFingerprint(run, control);
        if (validation) |v| if (v.expected_state_sha256) |expected| if (!std.mem.eql(u8, &expected, &state_digest)) return error.TrainingRestoreStateMismatch;
        staged.last_restore_receipt = .{ .state_sha256 = state_digest, .identity = staged.identity(), .accumulated_microbatches = staged.owner.accum_count, .checkpoint = .{ .size_bytes = snapshot.len, .sha256 = snapshot_hash.finalResult() } };
        if (self.execution == .resident_metal) try staged.initializeDevice(control);
        try check(control);
        self.deinit();
        self.* = staged;
    }

    /// Owned CT bindings must outlive the retained tape. Their lease prevents
    /// optimizer/checkpoint mutation until both forward and backward finish.
    pub fn bind(self: *Trainer, graph: *const ml.Graph, control: ?Control) !Bindings {
        try check(control);
        if (self.active_binding) return error.TrainingTapeStillLive;
        const a = self.owner.allocator;
        const cb = self.owner.compute_backend;
        var combined = CombinedControl{ .primary = cb.execution_control, .secondary = control };
        try CombinedControl.checkBoth(&combined);
        var inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        errdefer {
            for (inputs.items) |input| cb.free(input.value);
            inputs.deinit(a);
        }
        for (graph.parameters.items) |id| {
            const name = graph.parameterName(graph.node(id));
            for (self.owner.regular_params.items) |slot| {
                if (!std.mem.eql(u8, slot.name, name)) continue;
                const shape = graph.node(id).output_shape;
                if (shape.dtype != .f32 or shape.rank() != slot.dims.len) return error.TrainingBindingShapeMismatch;
                for (slot.dims, shape.dims[0..shape.rank()]) |actual, expected| if (actual != expected) return error.TrainingBindingShapeMismatch;
                var controlled = cb.*;
                controlled.execution_control = combined.control();
                const value = if (self.execution == .resident_metal)
                    try controlled.residentTrainingPrimitive(&.{ .reshape = .{ .input = (slot.device orelse return error.DeviceOptimizerNotInitialized).weight, .shape = slot.dims } }, .{})
                else
                    try controlled.fromFloat32Shape(slot.weights, slot.dims);
                errdefer cb.free(value);
                try inputs.append(a, .{ .node_id = id, .value = value });
                break;
            }
            try CombinedControl.checkBoth(&combined);
        }
        const owned = try inputs.toOwnedSlice(a);
        self.active_binding = true;
        return .{ .trainer = self, .inputs = owned };
    }

    pub fn submit(self: *Trainer, expected: Identity, loss: f32, gradients: []const Gradient, control: ?Control) !Result {
        if (self.execution != .native) return error.UnsupportedSeededTrainingBackend;
        if (!std.math.isFinite(loss)) return error.NonFiniteTrainingUpdate;
        return self.update(expected, loss, gradients, false, control);
    }

    /// Renormalizes a partial final window to the actual microbatch count,
    /// matching GLiNER2.5's _renormalize_partial_accumulation. Parameters absent
    /// for the entire window retain grad=None, including their weight decay.
    pub fn flush(self: *Trainer, expected: Identity, control: ?Control) !Result {
        if (self.execution == .resident_metal) return self.updateResident(expected, null, &.{}, true, control);
        return self.update(expected, null, &.{}, true, control);
    }

    /// The caller releases every retained tape/binding lease before submitting
    /// these still-owned resident gradients. No gradient payload is downloaded.
    pub fn submitResident(self: *Trainer, expected: Identity, loss: f32, gradients: []const ResidentGradient, control: ?Control) !Result {
        if (!std.math.isFinite(loss)) return error.NonFiniteTrainingUpdate;
        return self.updateResident(expected, loss, gradients, false, control);
    }

    /// Conservative all-slot flush admission for the enclosing run. This is
    /// metadata-only and does not touch accumulation, counters, or device data.
    pub fn nativeUpdateAdmission(self: *const Trainer) !NativeUpdateAdmission {
        if (self.execution != .native) return error.UnsupportedSeededTrainingBackend;
        const slots = self.owner.regular_params.items;
        if (slots.len == 0 or slots.len > self.limits.max_parameters or self.present.len != slots.len or self.group_ids.len != slots.len)
            return error.InvalidOptimizerState;
        var elements: usize = 0;
        var name_bytes: usize = 0;
        for (slots, self.group_ids) |slot, group| {
            if (slot.weights.len == 0 or slot.grad_accum.len != slot.weights.len or group >= self.groups.len or
                slot.name.len == 0 or slot.name.len > self.limits.max_parameter_name_bytes) return error.InvalidOptimizerState;
            const state = self.owner.optimizer_state.param_states.get(slot.name) orelse return error.InvalidOptimizerState;
            if (state.m.len != slot.weights.len or state.v.len != slot.weights.len or state.step_count != slot.adam_step_count)
                return error.InvalidOptimizerState;
            elements = std.math.add(usize, elements, slot.weights.len) catch return error.TrainingOptimizerLimitExceeded;
            name_bytes = std.math.add(usize, name_bytes, slot.name.len) catch return error.TrainingOptimizerLimitExceeded;
        }
        return nativeUpdateEstimate(slots.len, elements, name_bytes, self.limits.max_transaction_bytes);
    }

    /// Includes every device slot for a prospective complete flush, even when
    /// the current microbatch or accumulation window uses only a subset.
    pub fn residentUpdateAdmission(self: *const Trainer, a: Allocator) !device_transaction.Estimate {
        if (self.execution != .resident_metal) return error.UnsupportedSeededTrainingBackend;
        const parameters = self.owner.regular_params.items;
        const slots = try a.alloc(device_transaction.Slot, parameters.len);
        defer a.free(slots);
        const groups = try a.alloc(device_transaction.Group, self.groups.len);
        defer a.free(groups);
        const gradients = try a.alloc(device_transaction.Gradient, parameters.len);
        defer a.free(gradients);
        for (self.groups, groups) |group, *out| out.* = .{ .optimizer = group.optimizer, .schedule = group.schedule };
        for (parameters, slots, self.group_ids, gradients, 0..) |slot, *out, group, *gradient, index| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            out.* = .{ .weight = device.weight, .grad_accum = device.grad_accum, .m = device.m, .v = device.v, .shape = slot.dims, .group = group, .adam_step = slot.adam_step_count, .present = false };
            gradient.* = .{ .slot = index, .value = .zero };
        }
        const prospective = device_transaction.Identity{ .optimizer_step = self.owner.optimizer_step_count, .microbatch_step = @max(self.owner.step_count, self.owner.config.grad_accum_steps) };
        return device_transaction.estimate(.{ .identity = prospective, .accumulated_microbatches = self.owner.config.grad_accum_steps - 1, .grad_accum_steps = self.owner.config.grad_accum_steps, .max_grad_norm = self.owner.config.max_grad_norm, .slots = slots, .groups = groups }, .{ .expected = prospective, .action = .submit, .loss = 0, .gradients = gradients }, try residentTransactionLimits(self.limits));
    }

    fn updateResident(self: *Trainer, expected: Identity, loss: ?f32, gradients: []const ResidentGradient, flush_only: bool, control: ?Control) !Result {
        try check(control);
        if (self.execution != .resident_metal) return error.UnsupportedSeededTrainingBackend;
        if (self.active_binding) return error.TrainingTapeStillLive;
        if (!std.meta.eql(expected, self.identity())) return error.TrainingTapeIdentityMismatch;
        const parameters = self.owner.regular_params.items;
        if (gradients.len > parameters.len) return error.InvalidTrainingGradient;
        const transaction_limits = try residentTransactionLimits(self.limits);
        var metadata = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = self.owner.allocator, .limit = transaction_limits.max_host_metadata_bytes };
        var arena = std.heap.ArenaAllocator.init(metadata.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        const slots = try a.alloc(device_transaction.Slot, parameters.len);
        const groups = try a.alloc(device_transaction.Group, self.groups.len);
        const incoming = try a.alloc(device_transaction.Gradient, gradients.len);
        for (self.groups, groups) |group, *out| out.* = .{ .optimizer = group.optimizer, .schedule = group.schedule };
        for (parameters, slots, self.group_ids, self.present) |slot, *out, group, present| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            const state = self.owner.optimizer_state.param_states.get(slot.name) orelse return error.InvalidOptimizerState;
            if (state.step_count != slot.adam_step_count or state.m.len != slot.weights.len or state.v.len != slot.weights.len) return error.InvalidOptimizerState;
            out.* = .{ .weight = device.weight, .grad_accum = device.grad_accum, .m = device.m, .v = device.v, .shape = slot.dims, .group = group, .adam_step = slot.adam_step_count, .present = present };
        }
        for (gradients, incoming) |gradient, *out| {
            const slot = for (parameters, 0..) |parameter, index| {
                if (std.mem.eql(u8, parameter.name, gradient.name)) break index;
            } else return error.InvalidTrainingGradient;
            out.* = .{ .slot = slot, .value = switch (gradient.value) {
                .zero => .zero,
                .tensor => |tensor| .{ .tensor = tensor },
            } };
        }
        const pending = try device_transaction.prepare(self.owner.allocator, self.owner.compute_backend, .{
            .identity = .{ .optimizer_step = expected.optimizer_step, .microbatch_step = expected.microbatch_step },
            .accumulated_microbatches = self.owner.accum_count,
            .grad_accum_steps = self.owner.config.grad_accum_steps,
            .max_grad_norm = self.owner.config.max_grad_norm,
            .slots = slots,
            .groups = groups,
        }, .{ .expected = .{ .optimizer_step = expected.optimizer_step, .microbatch_step = expected.microbatch_step }, .action = if (flush_only) .flush else .submit, .loss = loss, .gradients = incoming }, transaction_limits, control);
        defer pending.deinit();
        try check(control);
        if (!std.meta.eql(expected, self.identity()) or self.active_binding) return error.TrainingTapeIdentityMismatch;
        // All work is drained and finite. Publication performs only ownership
        // transfers, scalar stores and infallible destruction of the old CTs.
        for (0..pending.replacements.len) |index| {
            const next = pending.take(index);
            const slot = &parameters[next.slot];
            const device = &slot.device.?;
            const cb = self.owner.compute_backend;
            cb.free(device.grad_accum);
            device.grad_accum = next.grad_accum.?;
            if (next.weight) |weight| {
                cb.free(device.weight);
                cb.free(device.m);
                cb.free(device.v);
                device.weight = weight;
                device.m = next.m.?;
                device.v = next.v.?;
            }
            slot.adam_step_count = next.adam_step;
            self.owner.optimizer_state.param_states.getPtr(slot.name).?.step_count = next.adam_step;
            self.present[next.slot] = next.present;
        }
        const receipt = pending.receipt;
        self.owner.step_count = receipt.identity.microbatch_step;
        self.owner.optimizer_step_count = receipt.identity.optimizer_step;
        self.owner.optimizer_state.step_count = @intCast(receipt.identity.optimizer_step);
        self.owner.accum_count = receipt.accumulated_microbatches;
        if (pending.replacements.len != 0) self.host_mirrors_current = false;
        self.last_device_receipt = receipt;
        return .{ .loss = receipt.loss, .optimizer_stepped = receipt.optimizer_stepped, .grad_norm = receipt.grad_norm, .identity = self.identity(), .accumulated_microbatches = receipt.accumulated_microbatches };
    }

    fn deviceLimits(self: *const Trainer) device_state.Limits {
        return .{
            .max_slots = self.limits.max_parameters,
            .max_host_payload_bytes = self.limits.max_state_bytes,
            .max_device_payload_bytes = self.limits.max_state_bytes,
            .max_total_bytes = self.limits.max_state_bytes,
            .max_host_metadata_bytes = @min(16 * 1024 * 1024, self.limits.max_transaction_bytes),
            .max_upload_staging_bytes = @min(1024 * 1024 * 1024, self.limits.max_transaction_bytes),
            .snapshot = .{ .max_scratch_bytes = @min(2 * 1024 * 1024, self.limits.max_transaction_bytes) },
        };
    }

    fn initializeDevice(self: *Trainer, control: ?Control) !void {
        _ = try device_state.initializeFromHost(&self.owner, self.deviceLimits(), control);
        self.host_mirrors_current = true;
    }

    /// Managed callers hold their run's busy lock. A failed readback leaves
    /// mirrors uncertified, and cannot affect the authoritative device state.
    pub fn ensureHostState(self: *Trainer, control: ?Control) !void {
        try check(control);
        if (self.active_binding) return error.TrainingTapeStillLive;
        if (self.execution == .native or self.host_mirrors_current) return;
        const Epoch = struct {
            trainer: *Trainer,
            identity: Identity,
            accumulated: u32,
            fn validate(raw: ?*const anyopaque, expected: u64) !void {
                const epoch: *const @This() = @ptrCast(@alignCast(raw.?));
                if (expected != epoch.identity.microbatch_step or !std.meta.eql(epoch.identity, epoch.trainer.identity()) or epoch.accumulated != epoch.trainer.owner.accum_count or epoch.trainer.active_binding) return error.TrainingTapeIdentityMismatch;
            }
        };
        var epoch = Epoch{ .trainer = self, .identity = self.identity(), .accumulated = self.owner.accum_count };
        _ = try device_state.readMirrors(&self.owner, .{ .context = &epoch, .expected = epoch.identity.microbatch_step, .validate = Epoch.validate }, self.deviceLimits(), control);
        try Epoch.validate(&epoch, epoch.identity.microbatch_step);
        try check(control);
        self.host_mirrors_current = true;
    }

    fn update(self: *Trainer, expected: Identity, loss: ?f32, gradients: []const Gradient, flush_only: bool, control: ?Control) !Result {
        var failure: ?Budget.AllocationFailure = null;
        var budget = Budget{ .backing = self.owner.allocator, .limit = self.limits.max_transaction_bytes, .failure_context = &failure, .allocation_failed = nativeAllocationFailed };
        defer std.debug.assert(budget.live == 0);
        return self.updateNative(expected, loss, gradients, flush_only, control, budget.allocator()) catch |err| {
            if (err == error.OutOfMemory) if (failure) |value| {
                if (value.kind == .declared_limit) return error.TrainingOptimizerLimitExceeded;
            };
            return err;
        };
    }

    fn updateNative(self: *Trainer, expected: Identity, loss: ?f32, gradients: []const Gradient, flush_only: bool, control: ?Control, scratch: Allocator) !Result {
        try check(control);
        if (self.active_binding) return error.TrainingTapeStillLive;
        if (!std.meta.eql(expected, self.identity())) return error.TrainingTapeIdentityMismatch;
        if (self.owner.optimizer_step_count >= std.math.maxInt(u32) or self.owner.step_count == std.math.maxInt(u64)) return error.TrainingEpochOverflow;
        const slots = self.owner.regular_params.items;
        if (gradients.len > slots.len) return error.InvalidTrainingGradient;
        const metadata_bytes = try nativeMetadataBytes(slots.len);
        if (metadata_bytes > self.limits.max_transaction_bytes) return error.TrainingOptimizerLimitExceeded;
        const incoming = try scratch.alloc(?[]const f32, slots.len);
        defer scratch.free(incoming);
        @memset(incoming, null);
        for (gradients) |gradient| {
            var found = false;
            for (slots, incoming) |slot, *value| if (std.mem.eql(u8, slot.name, gradient.name)) {
                if (value.* != null) return error.DuplicateTrainingGradient;
                if (gradient.values.len != slot.weights.len) return error.TrainingBindingShapeMismatch;
                try finite(gradient.values);
                value.* = gradient.values;
                found = true;
                break;
            };
            if (!found) return error.InvalidTrainingGradient;
        }
        const accumulated = self.owner.accum_count + @as(u32, if (flush_only) 0 else 1);
        const stepped = accumulated > 0 and (flush_only or accumulated == self.owner.config.grad_accum_steps);
        if (accumulated > self.owner.config.grad_accum_steps) return error.InvalidTrainingAccumulation;
        var bytes: usize = 0;
        for (slots, self.present, incoming) |slot, present, new| if (present or new != null) {
            const size = std.math.mul(usize, slot.weights.len, @sizeOf(f32) * @as(usize, if (stepped) 4 else 1)) catch return error.TrainingOptimizerLimitExceeded;
            bytes = std.math.add(usize, bytes, size) catch return error.TrainingOptimizerLimitExceeded;
        };
        const transaction_bytes = std.math.add(usize, metadata_bytes, bytes) catch return error.TrainingOptimizerLimitExceeded;
        if (transaction_bytes > self.limits.max_transaction_bytes) return error.TrainingOptimizerLimitExceeded;
        const pending = try scratch.alloc(?NativePending, slots.len);
        @memset(pending, null);
        defer {
            for (pending) |maybe| if (maybe) |value| value.deinit(scratch);
            scratch.free(pending);
        }
        var norm_square: f64 = 0;
        const divisor: f32 = @floatFromInt(self.owner.config.grad_accum_steps);
        const renormalize: f32 = if (stepped and accumulated < self.owner.config.grad_accum_steps) divisor / @as(f32, @floatFromInt(accumulated)) else 1;
        for (slots, self.present, incoming, pending) |slot, present, new, *out| {
            if (!present and new == null) continue;
            const next = try scratch.dupe(f32, slot.grad_accum);
            // Publish ownership immediately, before any validation or later
            // allocation can fail. The real parameter state is untouched.
            out.* = .{ .gradient = next };
            if (new) |values| for (next, values) |*old, value| {
                old.* += value / divisor;
            };
            if (renormalize != 1) for (next) |*value| {
                value.* *= renormalize;
            };
            try finite(next);
            for (next) |value| norm_square += @as(f64, value) * value;
            try check(control);
        }
        if (!std.math.isFinite(norm_square)) return error.NonFiniteTrainingUpdate;
        const norm = @sqrt(norm_square);
        const clip: f32 = if (self.owner.config.max_grad_norm > 0 and norm > self.owner.config.max_grad_norm) @floatCast(self.owner.config.max_grad_norm / (norm + 1e-6)) else 1;
        if (stepped) for (slots, pending, self.group_ids) |slot, *maybe, group_id| {
            const proposed = if (maybe.*) |*value| value else continue;
            const state = self.owner.optimizer_state.param_states.getPtr(slot.name) orelse return error.InvalidOptimizerState;
            if (state.step_count != slot.adam_step_count or state.step_count == std.math.maxInt(u32) or state.m.len != slot.weights.len or state.v.len != slot.weights.len) return error.InvalidOptimizerState;
            try finite(slot.weights);
            try finite(state.m);
            try finite(state.v);
            proposed.weights = try scratch.dupe(f32, slot.weights);
            proposed.m = try scratch.dupe(f32, state.m);
            proposed.v = try scratch.dupe(f32, state.v);
            for (proposed.gradient) |*value| value.* *= clip;
            const group = self.groups[group_id];
            const rate = group.schedule.lr(@intCast(self.owner.optimizer_step_count));
            if (!std.math.isFinite(rate) or rate < 0) return error.InvalidOptimizerGroup;
            optimizers.stepSlices(.{ .adamw = group.optimizer }, state.step_count + 1, rate, proposed.weights.?, proposed.gradient, proposed.m.?, proposed.v.?);
            try finite(proposed.weights.?);
            try finite(proposed.m.?);
            try finite(proposed.v.?);
            try check(control);
        };
        try check(control);
        // Commit contains no allocations, callbacks, or fallible operations.
        // A process-level hard stop is recovered from a durable checkpoint.
        for (slots, pending, self.present) |*slot, maybe, *present| {
            if (maybe) |proposed| {
                if (stepped) {
                    const state = self.owner.optimizer_state.param_states.getPtr(slot.name).?;
                    @memcpy(slot.weights, proposed.weights.?);
                    @memcpy(state.m, proposed.m.?);
                    @memcpy(state.v, proposed.v.?);
                    state.step_count += 1;
                    slot.adam_step_count += 1;
                } else @memcpy(slot.grad_accum, proposed.gradient);
                present.* = !stepped;
            }
            if (stepped) {
                @memset(slot.grad_accum, 0);
                present.* = false;
            }
        }
        if (!flush_only) self.owner.step_count += 1;
        self.owner.accum_count = if (stepped) 0 else accumulated;
        if (stepped) {
            self.owner.optimizer_step_count += 1;
            self.owner.optimizer_state.step_count = @intCast(self.owner.optimizer_step_count);
        }
        return .{ .loss = loss, .optimizer_stepped = stepped, .grad_norm = norm, .identity = self.identity(), .accumulated_microbatches = self.owner.accum_count };
    }

    /// Bind the optimizer contract to a higher-level dataset/model/RNG digest.
    /// Checkpoint callers must use this digest, not the input digest alone.
    pub fn fingerprint(self: *const Trainer, run: [32]u8) ![32]u8 {
        const a = self.owner.allocator;
        const settings = try std.json.Stringify.valueAlloc(a, .{ .groups = self.groups, .grad_accum_steps = self.owner.config.grad_accum_steps, .max_grad_norm = self.owner.config.max_grad_norm, .partial_window = "actual_microbatches" }, .{});
        defer a.free(settings);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.seeded-gradient-trainer.v1");
        if (self.execution == .resident_metal) hash.update("\x00resident_f32_optimizer_v1\x00");
        hash.update(&run);
        hash.update(settings);
        for (self.owner.regular_params.items, self.group_ids) |slot, group| {
            const entry = try std.json.Stringify.valueAlloc(a, .{ .name = slot.name, .dimensions = slot.dims, .group = group }, .{});
            defer a.free(entry);
            hash.update(entry);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }

    pub fn stateFingerprint(self: *Trainer, run: [32]u8, control: ?Control) ![32]u8 {
        if (self.active_binding) return error.TrainingTapeStillLive;
        try check(control);
        try self.ensureHostState(control);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.seeded-gradient-state.v1\x00");
        hash.update(&try self.fingerprint(run));
        hashInteger(&hash, self.owner.optimizer_step_count);
        hashInteger(&hash, self.owner.step_count);
        hashInteger(&hash, self.owner.accum_count);
        hashInteger(&hash, self.owner.regular_params.items.len);
        for (self.owner.regular_params.items, self.present) |slot, present| {
            try check(control);
            const state = self.owner.optimizer_state.param_states.get(slot.name) orelse return error.InvalidOptimizerState;
            hashInteger(&hash, slot.name.len);
            hash.update(slot.name);
            hashInteger(&hash, slot.dims.len);
            for (slot.dims) |dim| hashInteger(&hash, @intCast(dim));
            hashInteger(&hash, slot.adam_step_count);
            hashInteger(&hash, state.step_count);
            hashInteger(&hash, @intFromBool(present));
            for ([_][]const f32{ slot.weights, state.m, state.v, slot.grad_accum }) |values| {
                hashInteger(&hash, values.len);
                var buffer: [4096]u8 = undefined;
                var offset: usize = 0;
                while (offset < values.len) {
                    try check(control);
                    const end = @min(values.len, offset + 1024);
                    for (values[offset..end], 0..) |value, index| std.mem.writeInt(u32, buffer[index * 4 ..][0..4], @bitCast(value), .little);
                    hash.update(buffer[0 .. (end - offset) * 4]);
                    offset = end;
                }
            }
        }
        return hash.finalResult();
    }
};

fn residentTransactionLimits(limits: Limits) !device_transaction.Limits {
    // Keep usize arithmetic: @min's inferred result type can narrow to u27
    // for the 64 MiB cap, which cannot represent both metadata reservations.
    const metadata_bytes: usize = @min(64 * 1024 * 1024, limits.max_transaction_bytes / 4);
    if (metadata_bytes < 1024) return error.TrainingOptimizerLimitExceeded;
    return .{
        .max_slots = limits.max_parameters,
        .max_groups = limits.max_groups,
        .max_device_bytes = limits.max_transaction_bytes - metadata_bytes * 2,
        .max_host_metadata_bytes = metadata_bytes,
    };
}

test "seeded gradient trainer resident transaction admission preserves full width budget arithmetic" {
    for ([_]usize{ 0, 1, 4095 }) |bytes| try std.testing.expectError(error.TrainingOptimizerLimitExceeded, residentTransactionLimits(.{ .max_transaction_bytes = bytes }));
    for ([_]usize{ 4096, 128 * 1024 * 1024, 256 * 1024 * 1024, 4 * 1024 * 1024 * 1024, std.math.maxInt(usize) }) |bytes| {
        const limits = try residentTransactionLimits(.{ .max_transaction_bytes = bytes });
        try std.testing.expect(limits.max_host_metadata_bytes <= 64 * 1024 * 1024);
        try std.testing.expectEqual(bytes, limits.max_device_bytes + 2 * limits.max_host_metadata_bytes);
    }
}

test "seeded gradient trainer native transaction admission checks full-width totals without allocating" {
    const exact = try nativeUpdateEstimate(3, 7, 41, std.math.maxInt(usize));
    try std.testing.expectEqual(@as(usize, 7 * 4 * @sizeOf(f32)), exact.payload_bytes);
    try std.testing.expect(exact.metadata_bytes > 0);
    try std.testing.expect(exact.total_work >= 64 * exact.elements);
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, nativeUpdateEstimate(3, 7, 41, exact.host_upper_bound_bytes - 1));
    try std.testing.expectEqual(exact, try nativeUpdateEstimate(3, 7, 41, exact.host_upper_bound_bytes));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, nativeUpdateEstimate(std.math.maxInt(usize), 1, 1, std.math.maxInt(usize)));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, nativeUpdateEstimate(1, std.math.maxInt(usize), 1, std.math.maxInt(usize)));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, nativeUpdateEstimate(1, 1, std.math.maxInt(usize), std.math.maxInt(usize)));
}

fn expectedNativeUpdateFailure(result: anyerror!Result, expected: anyerror) !void {
    if (result) |_| return error.ExpectedNativeUpdateFailure else |err| {
        // Preserve independently injected allocation failures for the sweep.
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(expected, err);
    }
}

fn exerciseNativeAdmission(a: Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    // Observe the backing allocator's actual live bytes. There are no test
    // overrides of the transaction allocator or its production allocation path.
    var observed = Budget{ .backing = a, .limit = 4 * 1024 * 1024 };
    defer std.debug.assert(observed.live == 0);
    var trainer = try Trainer.init(observed.allocator(), &cb, &.{
        .{ .name = "encoder.weight", .values = &.{ 1, 2 }, .dimensions = &.{2}, .group = 0 },
        .{ .name = "head.bias", .values = &.{3}, .dimensions = &.{1}, .group = 0 },
        .{ .name = "conditional.weight", .values = &.{4}, .dimensions = &.{1}, .group = 0 },
    }, .{ .groups = &.{.{ .schedule = .{ .constant = 0.1 } }}, .grad_accum_steps = 2 });
    defer trainer.deinit();
    const admitted = try trainer.nativeUpdateAdmission();
    try std.testing.expectEqual(@as(usize, 3), admitted.parameters);
    try std.testing.expectEqual(@as(usize, 4), admitted.elements);
    trainer.limits.max_transaction_bytes = admitted.host_upper_bound_bytes;
    const baseline = observed.live;
    const all_zero = [_]Gradient{
        .{ .name = "encoder.weight", .values = &.{ 0, 0 } },
        .{ .name = "head.bias", .values = &.{0} },
        .{ .name = "conditional.weight", .values = &.{0} },
    };
    observed.peak = baseline;
    const accumulated = try trainer.submit(trainer.identity(), 0, &all_zero, null);
    try std.testing.expect(!accumulated.optimizer_stepped);
    try std.testing.expectEqual(baseline, observed.live);
    try std.testing.expectEqual(admitted.metadata_bytes + admitted.payload_bytes / 4, observed.peak - baseline);
    observed.peak = baseline;
    const full = try trainer.submit(trainer.identity(), 0, &all_zero, null);
    try std.testing.expect(full.optimizer_stepped);
    try std.testing.expectEqual(baseline, observed.live);
    try std.testing.expectEqual(admitted.host_upper_bound_bytes, observed.peak - baseline);
    try std.testing.expectEqual(admitted, try trainer.nativeUpdateAdmission());
    for (trainer.owner.regular_params.items) |slot| try std.testing.expectEqual(@as(u32, 1), slot.adam_step_count);

    // A subset accumulation followed by a partial flush fits the same bound
    // and preserves absent slots rather than turning them into explicit zeros.
    observed.peak = baseline;
    _ = try trainer.submit(trainer.identity(), 1, &.{.{ .name = "head.bias", .values = &.{2} }}, null);
    const partial = try trainer.flush(trainer.identity(), null);
    try std.testing.expect(partial.optimizer_stepped);
    try std.testing.expectEqual(baseline, observed.live);
    try std.testing.expect(observed.peak - baseline <= admitted.host_upper_bound_bytes);
    try std.testing.expectEqual(@as(u32, 1), trainer.owner.regular_params.items[0].adam_step_count);
    try std.testing.expectEqual(@as(u32, 2), trainer.owner.regular_params.items[1].adam_step_count);
    try std.testing.expectEqual(@as(u32, 1), trainer.owner.regular_params.items[2].adam_step_count);

    _ = try trainer.submit(trainer.identity(), 0, &all_zero, null);
    const before_identity = trainer.identity();
    const before = try trainer.stateFingerprint(@splat(97), null);
    trainer.limits.max_transaction_bytes = admitted.host_upper_bound_bytes - 1;
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, trainer.nativeUpdateAdmission());
    try expectedNativeUpdateFailure(trainer.submit(before_identity, 0, &all_zero, null), error.TrainingOptimizerLimitExceeded);
    try std.testing.expectEqual(before_identity, trainer.identity());
    try std.testing.expectEqual(before, try trainer.stateFingerprint(@splat(97), null));
    try std.testing.expectEqual(baseline, observed.live);

    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 3) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    trainer.limits.max_transaction_bytes = admitted.host_upper_bound_bytes;
    try expectedNativeUpdateFailure(trainer.submit(before_identity, 0, &all_zero, .{ .ptr = &cancel, .check_fn = Cancel.check }), error.Cancelled);
    try std.testing.expectEqual(@as(usize, 3), cancel.checks);
    try std.testing.expectEqual(before_identity, trainer.identity());
    try std.testing.expectEqual(before, try trainer.stateFingerprint(@splat(97), null));
    try std.testing.expectEqual(baseline, observed.live);
    observed.peak = baseline;
    const retried = try trainer.submit(before_identity, 0, &all_zero, null);
    try std.testing.expect(retried.optimizer_stepped);
    try std.testing.expectEqual(baseline, observed.live);
    try std.testing.expectEqual(admitted.host_upper_bound_bytes, observed.peak - baseline);
}

test "seeded gradient trainer native transaction admission bounds measured accumulation flush and atomic retry" {
    try exerciseNativeAdmission(std.testing.allocator);
}

test "seeded gradient trainer native transaction admission ownership survives every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseNativeAdmission, .{});
}

fn hashInteger(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}

pub const Bindings = struct {
    trainer: *Trainer,
    inputs: []interpreter.RuntimeInput,
    pub fn deinit(self: *Bindings) void {
        for (self.inputs) |input| self.trainer.owner.compute_backend.free(input.value);
        self.trainer.owner.allocator.free(self.inputs);
        self.trainer.active_binding = false;
        self.* = undefined;
    }
};

fn exerciseTrainer(a: Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var trainer = try Trainer.init(a, &cb, &.{
        .{ .name = "encoder.weight", .values = &.{ 1, 2 }, .dimensions = &.{2}, .group = 0 },
        .{ .name = "head.bias", .values = &.{3}, .dimensions = &.{1}, .group = 1 },
        .{ .name = "absent_head", .values = &.{5}, .dimensions = &.{1}, .group = 1 },
    }, .{ .groups = &.{
        .{ .optimizer = .{ .beta1 = 0, .beta2 = 0, .eps = 1, .weight_decay = 0.2 }, .schedule = .{ .constant = 0.1 } },
        .{ .optimizer = .{ .beta1 = 0, .beta2 = 0, .eps = 1, .weight_decay = 0.4 }, .schedule = .{ .constant = 0.03 } },
    }, .grad_accum_steps = 2, .max_grad_norm = 0 });
    defer trainer.deinit();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    _ = try builder.parameter("encoder.weight", ml.Shape.init(.f32, &.{2}));
    {
        var bindings = try trainer.bind(&graph, null);
        defer bindings.deinit();
        try std.testing.expectEqual(@as(usize, 1), bindings.inputs.len);
        try std.testing.expectError(error.TrainingTapeStillLive, trainer.submit(trainer.identity(), 1, &.{}, null));
    }
    const fingerprint1 = try trainer.fingerprint(@splat(1));
    const fingerprint2 = try trainer.fingerprint(@splat(2));
    try std.testing.expect(!std.mem.eql(u8, &fingerprint1, &fingerprint2));
    const initial = trainer.identity();
    const first = try trainer.submit(initial, 2, &.{ .{ .name = "encoder.weight", .values = &.{ 2, -2 } }, .{ .name = "head.bias", .values = &.{0} } }, null);
    try std.testing.expect(!first.optimizer_stepped);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, trainer.owner.regular_params.items[0].weights);
    const before = trainer.identity();
    try std.testing.expectError(error.TrainingTapeIdentityMismatch, trainer.submit(initial, 1, &.{}, null));
    if (trainer.submit(before, 1, &.{ .{ .name = "encoder.weight", .values = &.{ 0, 2 } }, .{ .name = "head.bias", .values = &.{std.math.nan(f32)} } }, null)) |_| {
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(before, trainer.identity());
        try std.testing.expectEqualSlices(f32, &.{ 1, -1 }, trainer.owner.regular_params.items[0].grad_accum);
        if (err != error.NonFiniteTrainingUpdate) return err;
    }
    const second = trainer.submit(before, 1, &.{.{ .name = "encoder.weight", .values = &.{ 0, 2 } }}, null) catch |err| {
        try std.testing.expectEqual(before, trainer.identity());
        try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, trainer.owner.regular_params.items[0].weights);
        try std.testing.expectEqualSlices(f32, &.{ 1, -1 }, trainer.owner.regular_params.items[0].grad_accum);
        return err;
    };
    try std.testing.expect(second.optimizer_stepped);
    try std.testing.expectApproxEqAbs(@as(f32, 0.93), trainer.owner.regular_params.items[0].weights[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.96), trainer.owner.regular_params.items[0].weights[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.964), trainer.owner.regular_params.items[1].weights[0], 1e-6);
    try std.testing.expectEqual(@as(f32, 5), trainer.owner.regular_params.items[2].weights[0]);
    try std.testing.expectEqual(@as(u32, 1), trainer.owner.regular_params.items[1].adam_step_count);
    try std.testing.expectEqual(@as(u32, 0), trainer.owner.regular_params.items[2].adam_step_count);
    _ = try trainer.submit(trainer.identity(), 0.5, &.{.{ .name = "encoder.weight", .values = &.{ 2, 0 } }}, null);
    const partial = try trainer.flush(trainer.identity(), null);
    try std.testing.expect(partial.optimizer_stepped);
    const moments = trainer.owner.optimizer_state.param_states.get("encoder.weight").?;
    try std.testing.expectEqualSlices(f32, &.{ 2, 0 }, moments.m);
    try std.testing.expectEqual(@as(u32, 2), moments.step_count);
    try std.testing.expectEqual(@as(u32, 1), trainer.owner.regular_params.items[1].adam_step_count);
    try std.testing.expectEqual(@as(u64, 3), trainer.identity().microbatch_step);
    try std.testing.expectEqual(@as(u64, 2), trainer.identity().optimizer_step);
}

test "seeded gradient trainer preserves groups absent gradients and partial window normalization" {
    try exerciseTrainer(std.testing.allocator);
}

test "seeded gradient trainer allocation failures preserve parameter and moment ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseTrainer, .{});
}

const restore_parameters = [_]Parameter{.{ .name = "restore.weight", .values = &.{ 1, 2 }, .dimensions = &.{2}, .group = 0 }};
const restore_groups = [_]Group{.{ .schedule = .{ .constant = 0.1 } }};

fn exerciseRestore(a: Allocator, path: []const u8) !void {
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var trainer = try Trainer.init(a, &cb, &restore_parameters, .{ .groups = &restore_groups });
    defer trainer.deinit();
    _ = try trainer.submit(trainer.identity(), 1, &.{.{ .name = "restore.weight", .values = &.{ 2, -1 } }}, null);
    const before = trainer.identity();
    const weights: [2]f32 = trainer.owner.regular_params.items[0].weights[0..2].*;
    const state = trainer.owner.optimizer_state.param_states.get("restore.weight").?;
    const m: [2]f32 = state.m[0..2].*;
    const v: [2]f32 = state.v[0..2].*;
    trainer.restore(path, @splat(67), null) catch |err| {
        try std.testing.expectEqual(before, trainer.identity());
        try std.testing.expectEqualSlices(f32, &weights, trainer.owner.regular_params.items[0].weights);
        const preserved = trainer.owner.optimizer_state.param_states.get("restore.weight").?;
        try std.testing.expectEqualSlices(f32, &m, preserved.m);
        try std.testing.expectEqualSlices(f32, &v, preserved.v);
        return err;
    };
    try std.testing.expectEqual(@as(u64, 0), trainer.identity().optimizer_step);
    try std.testing.expectEqualSlices(f32, restore_parameters[0].values, trainer.owner.regular_params.items[0].weights);
}

test "seeded gradient trainer restore admits header heap and file before parsing and remains atomic under allocation failure" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var trainer = try Trainer.init(a, &cb, &restore_parameters, .{ .groups = &restore_groups });
    defer trainer.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/restore.safetensors", .{temporary.sub_path});
    defer a.free(path);
    try trainer.save(path, @splat(67), null);
    const saved_digest = try trainer.stateFingerprint(@splat(67), null);
    _ = try trainer.submit(trainer.identity(), 1, &.{.{ .name = "restore.weight", .values = &.{ 2, -1 } }}, null);
    const before = trainer.identity();
    const weights: [2]f32 = trainer.owner.regular_params.items[0].weights[0..2].*;
    for ([_]Limits{ .{ .max_checkpoint_header_bytes = 1 }, .{ .max_checkpoint_header_heap_bytes = 1 }, .{ .max_transaction_bytes = 1 } }) |limits| {
        trainer.limits = limits;
        try std.testing.expectError(error.TrainingOptimizerLimitExceeded, trainer.restore(path, @splat(67), null));
        try std.testing.expectEqual(before, trainer.identity());
        try std.testing.expectEqualSlices(f32, &weights, trainer.owner.regular_params.items[0].weights);
    }
    trainer.limits = .{};
    const Accept = struct {
        fn apply(_: ?*const anyopaque, _: Identity, _: u32) !void {}
    };
    try std.testing.expectError(error.TrainingRestoreStateMismatch, trainer.restoreValidated(path, @splat(67), null, .{ .context = null, .validate = Accept.apply, .expected_state_sha256 = @splat(0) }));
    try std.testing.expectEqual(before, trainer.identity());
    try std.testing.expectEqualSlices(f32, &weights, trainer.owner.regular_params.items[0].weights);
    try std.testing.expect(!std.mem.eql(u8, &saved_digest, &try trainer.stateFingerprint(@splat(67), null)));
    const Reject = struct {
        fn apply(_: ?*const anyopaque, _: Identity, _: u32) !void {
            return error.InvalidBoundaryTrainingProgress;
        }
    };
    try std.testing.expectError(error.InvalidBoundaryTrainingProgress, trainer.restoreValidated(path, @splat(67), null, .{ .context = null, .validate = Reject.apply }));
    try std.testing.expectEqual(before, trainer.identity());
    try std.testing.expectEqualSlices(f32, &weights, trainer.owner.regular_params.items[0].weights);
    const Canceller = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.Cancelled;
        }
    };
    var canceller = Canceller{};
    try std.testing.expectError(error.Cancelled, trainer.restore(path, @splat(67), .{ .ptr = &canceller, .check_fn = Canceller.check }));
    try std.testing.expectEqual(before, trainer.identity());
    try std.testing.expectEqualSlices(f32, &weights, trainer.owner.regular_params.items[0].weights);
    try exerciseRestore(a, path);
    try std.testing.checkAllAllocationFailures(a, exerciseRestore, .{path});
}

test "seeded gradient trainer matches pinned Torch AdamW groups clipping moments and partial flush" {
    const Fixture = struct {
        optimizer: struct { betas: [2]f32, eps: f32, gradient_accumulation_steps: u32, max_grad_norm: f32 },
        parameters: []const struct { name: []const u8, shape: []const i32, initial: []const f32, lr: f32, weight_decay: f32 },
        microbatches: []const struct { gradients: std.json.ArrayHashMap(?[]const f32), flush: bool },
        flushes: []const struct {
            after_microbatch: u64,
            grad_norm: f64,
            parameters: std.json.ArrayHashMap(struct { weight: []const f32, step: u32, exp_avg: []const f32, exp_avg_sq: []const f32 }),
        },
    };
    const a = std.testing.allocator;
    const fixture_bytes = try @import("../util/c_file.zig").readFileMax(a, "testdata/gliner25/training_adamw.json", 1024 * 1024);
    defer a.free(fixture_bytes);
    var fixture = try std.json.parseFromSlice(Fixture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const f = fixture.value;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const parameters = try a.alloc(Parameter, f.parameters.len);
    defer a.free(parameters);
    const groups = try a.alloc(Group, f.parameters.len);
    defer a.free(groups);
    for (f.parameters, parameters, groups, 0..) |parameter, *p, *group, i| {
        p.* = .{ .name = parameter.name, .dimensions = parameter.shape, .values = parameter.initial, .group = i };
        group.* = .{ .optimizer = .{ .beta1 = f.optimizer.betas[0], .beta2 = f.optimizer.betas[1], .eps = f.optimizer.eps, .weight_decay = parameter.weight_decay }, .schedule = .{ .constant = parameter.lr } };
    }
    var trainer = try Trainer.init(a, &cb, parameters, .{ .groups = groups, .grad_accum_steps = f.optimizer.gradient_accumulation_steps, .max_grad_norm = f.optimizer.max_grad_norm });
    defer trainer.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/seeded.safetensors", .{temporary.sub_path});
    defer a.free(path);
    var flush_index: usize = 0;
    for (f.microbatches) |microbatch| {
        var gradients = std.ArrayListUnmanaged(Gradient).empty;
        defer gradients.deinit(a);
        var entries = microbatch.gradients.map.iterator();
        while (entries.next()) |entry| if (entry.value_ptr.*) |values| try gradients.append(a, .{ .name = entry.key_ptr.*, .values = values });
        var result = try trainer.submit(trainer.identity(), 1, gradients.items, null);
        if (!result.optimizer_stepped) {
            try trainer.save(path, @splat(17), null);
            var restored = try Trainer.init(a, &cb, parameters, .{ .groups = groups, .grad_accum_steps = f.optimizer.gradient_accumulation_steps, .max_grad_norm = f.optimizer.max_grad_norm });
            var owned = true;
            errdefer if (owned) restored.deinit();
            try std.testing.expectError(error.TrainingStateFingerprintMismatch, restored.restore(path, @splat(18), null));
            try std.testing.expectEqual(@as(u64, 0), restored.identity().microbatch_step);
            try restored.restore(path, @splat(17), null);
            try std.testing.expectEqual(trainer.identity(), restored.identity());
            try std.testing.expectEqual(trainer.owner.accum_count, restored.owner.accum_count);
            try std.testing.expectEqualSlices(bool, trainer.present, restored.present);
            for (trainer.owner.regular_params.items, restored.owner.regular_params.items) |previous, resumed| {
                try std.testing.expectEqualSlices(f32, previous.weights, resumed.weights);
                try std.testing.expectEqualSlices(f32, previous.grad_accum, resumed.grad_accum);
            }
            trainer.deinit();
            trainer = restored;
            owned = false;
        }
        if (microbatch.flush and !result.optimizer_stepped) result = try trainer.flush(trainer.identity(), null);
        if (!microbatch.flush) {
            try std.testing.expect(!result.optimizer_stepped);
            continue;
        }
        const expected = f.flushes[flush_index];
        flush_index += 1;
        try std.testing.expect(result.optimizer_stepped);
        try std.testing.expectEqual(expected.after_microbatch, result.identity.microbatch_step);
        try std.testing.expectApproxEqAbs(expected.grad_norm, result.grad_norm, 5e-7);
        for (trainer.owner.regular_params.items) |slot| {
            const want = expected.parameters.map.get(slot.name).?;
            const state = trainer.owner.optimizer_state.param_states.get(slot.name).?;
            try std.testing.expectEqual(want.step, state.step_count);
            try std.testing.expectEqual(want.step, slot.adam_step_count);
            for (want.weight, slot.weights) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 3e-7);
            for (want.exp_avg, state.m) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 1e-7 + @abs(value) * 1e-5);
            for (want.exp_avg_sq, state.v) |value, actual| try std.testing.expectApproxEqAbs(value, actual, 1e-10 + @abs(value) * 2e-5);
        }
    }
    try std.testing.expectEqual(f.flushes.len, flush_index);
}

test "seeded gradient trainer checkpoint preserves all Adam counter bits beyond f32 precision" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const parameters = [_]Parameter{.{ .name = "conditional.weight", .dimensions = &.{2}, .values = &.{ 1.25, -0.75 }, .group = 0 }};
    const config = Config{ .groups = &.{.{ .schedule = .{ .constant = 0.001 } }} };
    var trainer = try Trainer.init(a, &cb, &parameters, config);
    defer trainer.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/counter.safetensors", .{temporary.sub_path});
    defer a.free(path);
    for ([_]u32{ 0, 16777216, 16777217, 0x7fffffff, std.math.maxInt(u32) }) |step| {
        trainer.owner.optimizer_step_count = step;
        trainer.owner.optimizer_state.step_count = step;
        trainer.owner.step_count = @as(u64, step) * 2;
        trainer.owner.regular_params.items[0].adam_step_count = step;
        const state = trainer.owner.optimizer_state.param_states.getPtr("conditional.weight").?;
        state.step_count = step;
        @memcpy(state.m, &[_]f32{ 0.25, -0.125 });
        @memcpy(state.v, &[_]f32{ 0.5, 0.75 });
        try trainer.save(path, @splat(51), null);
        var restored = try Trainer.init(a, &cb, &parameters, config);
        defer restored.deinit();
        try restored.restore(path, @splat(51), null);
        try std.testing.expectEqual(trainer.identity(), restored.identity());
        try std.testing.expectEqual(step, restored.owner.regular_params.items[0].adam_step_count);
        const actual = restored.owner.optimizer_state.param_states.get("conditional.weight").?;
        try std.testing.expectEqual(step, actual.step_count);
        try std.testing.expectEqualSlices(f32, state.m, actual.m);
        try std.testing.expectEqualSlices(f32, state.v, actual.v);
        if (step == std.math.maxInt(u32)) {
            try std.testing.expectError(error.TrainingEpochOverflow, restored.submit(restored.identity(), 1, &.{.{ .name = "conditional.weight", .values = &.{ 0, 0 } }}, null));
            try std.testing.expectEqual(trainer.identity(), restored.identity());
            try std.testing.expectEqualSlices(f32, parameters[0].values, restored.owner.regular_params.items[0].weights);
        }
    }
}

test "seeded gradient trainer cancelled checkpoint publication preserves the previous durable state" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const parameters = [_]Parameter{.{ .name = "classifier.bias", .dimensions = &.{1}, .values = &.{0.5}, .group = 0 }};
    var trainer = try Trainer.init(a, &cb, &parameters, .{ .groups = &.{.{ .schedule = .{ .constant = 0.001 } }} });
    defer trainer.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/checkpoint.safetensors", .{temporary.sub_path});
    defer a.free(path);
    try trainer.save(path, @splat(59), null);
    const before = try @import("../util/c_file.zig").readFileMax(a, path, 1024 * 1024);
    defer a.free(before);
    try temporary.dir.writeFile(compat.io(), .{ .sub_path = "checkpoint.safetensors.tmp", .data = "unrelated old staging file" });
    _ = try trainer.submit(trainer.identity(), 1, &.{.{ .name = "classifier.bias", .values = &.{0.1} }}, null);
    const Cancel = struct {
        directory: std.Io.Dir,
        observed_staging: bool = false,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            var entries = self.directory.iterate();
            while (try entries.next(compat.io())) |entry| {
                if (std.mem.startsWith(u8, entry.name, "checkpoint.safetensors.") and std.mem.endsWith(u8, entry.name, ".tmp") and !std.mem.eql(u8, entry.name, "checkpoint.safetensors.tmp")) {
                    self.observed_staging = true;
                    return error.Cancelled;
                }
            }
        }
    };
    var dir = try temporary.dir.openDir(compat.io(), ".", .{ .iterate = true });
    defer dir.close(compat.io());
    var cancel = Cancel{ .directory = dir };
    try std.testing.expectError(error.Cancelled, trainer.save(path, @splat(59), .{ .ptr = &cancel, .check_fn = Cancel.check }));
    try std.testing.expect(cancel.observed_staging);
    const after = try @import("../util/c_file.zig").readFileMax(a, path, 1024 * 1024);
    defer a.free(after);
    try std.testing.expectEqualSlices(u8, before, after);
    try std.testing.expectEqual(@as(u64, 1), trainer.identity().optimizer_step);
    var entries = dir.iterate();
    var files: usize = 0;
    while (try entries.next(compat.io())) |_| files += 1;
    try std.testing.expectEqual(@as(usize, 2), files);
    try trainer.restore(path, @splat(59), null);
    try std.testing.expectEqual(@as(u64, 0), trainer.identity().optimizer_step);
    try std.testing.expectEqual(@as(f32, 0.5), trainer.owner.regular_params.items[0].weights[0]);
}
