// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Shared forward-pass batching. Pipelines retain their task semantics; only
//! explicitly row-independent stages may install this dispatcher. Dynamic
//! leading input/output axes and identical non-batch geometry are required.
const std = @import("std");
const platform = @import("antfly_platform");
const session_mod = @import("../backends/session.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const micro = @import("executor_microbatch.zig");

pub const Dispatch = struct {
    ptr: *anyopaque,
    task: micro.Task,
    run_resident_fn: ?*const fn (*anyopaque, micro.Task, std.mem.Allocator, session_mod.Session, ?*session_mod.RunPermit, ?*std.atomic.Mutex, []const Tensor, ?Control) anyerror!?session_mod.ResidentOutputs = null,
    run_fn: *const fn (*anyopaque, micro.Task, std.mem.Allocator, session_mod.Session, ?*session_mod.RunPermit, ?*std.atomic.Mutex, []const Tensor, ?Control) anyerror![]Tensor,

    pub fn run(self: Dispatch, allocator: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control) ![]Tensor {
        return self.run_fn(self.ptr, self.task, allocator, session, permit, gate, inputs, control);
    }
    pub fn supportsResident(self: Dispatch) bool {
        return self.run_resident_fn != null;
    }

    pub fn runResident(self: Dispatch, allocator: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control) !?session_mod.ResidentOutputs {
        const execute_resident = self.run_resident_fn orelse return null;
        return execute_resident(self.ptr, self.task, allocator, session, permit, gate, inputs, control);
    }
};

/// Invocation-local metadata. Physical outputs/provider/model/quota are shared
/// by ResidentOutputs; row children own only their independently retained CTs.
const ResidentBatch = struct {
    operation_scope: @import("../execution/backend_operation.zig").Scope,
    allocator: std.mem.Allocator,
    outputs: *session_mod.ResidentOutputs,
    columns: []Column,
    total: usize,
    const Column = struct { shape: []i64, flat: @import("../ops/ops.zig").CT, width: usize };

    fn reshape(backend: *const @import("../ops/ops.zig").ComputeBackend, value: @import("../ops/ops.zig").CT, shape: []const i64) !@import("../ops/ops.zig").CT {
        switch (backend.kind()) {
            // Native's canonical reshape materializes logical strided views;
            // CUDA preserves owning/physical storage with retained metadata.
            .native, .cuda => return backend.primReshape(value, shape),
            .metal => {
                var dimensions: [8]i32 = undefined;
                if (shape.len > dimensions.len) return error.UnsupportedResidentRowViews;
                for (shape, 0..) |dim, i| dimensions[i] = std.math.cast(i32, dim) orelse return error.UnsupportedResidentRowViews;
                // Never permit the general primitive's hidden host fallback.
                return (try backend.cloneTensorShape(value, dimensions[0..shape.len])) orelse error.UnsupportedResidentRowViews;
            },
            else => return error.UnsupportedResidentRowViews,
        }
    }

    fn init(allocator: std.mem.Allocator, outputs: *session_mod.ResidentOutputs, session: session_mod.Session, total: usize) !ResidentBatch {
        if (total == 0 or outputs.outputs.len == 0 or outputs.outputs.len != session.outputInfo().len or outputs.backend.vtable.sliceRows2D == null) return error.InvalidFusedOutputShape;
        var operation_scope: @import("../execution/backend_operation.zig").Scope = undefined;
        try @import("../execution/backend_operation.zig").enterInto(outputs.backend, null, outputs.backend.operationToken(), .legacy_unknown_frame, &operation_scope);
        errdefer operation_scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        const columns = try allocator.alloc(Column, outputs.outputs.len);
        var built: usize = 0;
        errdefer {
            for (columns[0..built]) |column| {
                outputs.backend.free(column.flat);
                allocator.free(column.shape);
            }
            allocator.free(columns);
        }
        for (outputs.outputs, session.outputInfo(), columns) |value, info, *column| {
            if (info.dtype != .f32 or try outputs.backend.tensorDType(value) != .f32) return error.InvalidFusedOutputShape;
            const shape = try outputs.backend.tensorShape(value, allocator);
            errdefer allocator.free(shape);
            if (shape.len == 0 or shape.len > 8 or shape[0] != total) return error.InvalidFusedOutputShape;
            var width: usize = 1;
            for (shape[1..]) |dim| {
                if (dim <= 0) return error.InvalidFusedOutputShape;
                width = std.math.mul(usize, width, std.math.cast(usize, dim) orelse return error.InvalidFusedOutputShape) catch return error.ResourceLimitExceeded;
            }
            if (session.vtable.independentBatchRows == null) {
                if (info.shape.len != shape.len) return error.InvalidFusedOutputShape;
                for (shape[1..], info.shape[1..]) |actual, declared| if (declared > 0 and actual != declared) return error.InvalidFusedOutputShape;
            }
            const flat = try reshape(outputs.backend, value, &.{ @intCast(total), std.math.cast(i64, width) orelse return error.ResourceLimitExceeded });
            column.* = .{ .shape = shape, .flat = flat, .width = width };
            built += 1;
        }
        return .{ .allocator = allocator, .outputs = outputs, .columns = columns, .total = total, .operation_scope = operation_scope };
    }

    fn deinit(self: *ResidentBatch) void {
        // Flat/view CT metadata is retired under the SAME explicit provider
        // scope as construction; no nested gate acquisition or network wait.
        defer self.operation_scope.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        for (self.columns) |column| {
            self.outputs.backend.free(column.flat);
            self.allocator.free(column.shape);
        }
        self.allocator.free(self.columns);
    }

    fn rows(self: *ResidentBatch, start: usize, count: usize) !session_mod.ResidentOutputs {
        if (start > self.total or count > self.total - start) return error.InvalidFusedOutputShape;
        const cb = self.outputs.backend;
        const views = try self.allocator.alloc(@import("../ops/ops.zig").CT, self.columns.len);
        var built: usize = 0;
        errdefer {
            for (views[0..built]) |view| cb.free(view);
            self.allocator.free(views);
        }
        for (self.columns, views) |column, *view| {
            const slice = try cb.vtable.sliceRows2D.?(cb.ptr, column.flat, start, count, column.width);
            defer cb.free(slice);
            var shape: [8]i64 = undefined;
            @memcpy(shape[0..column.shape.len], column.shape);
            shape[0] = std.math.cast(i64, count) orelse return error.ResourceLimitExceeded;
            view.* = try reshape(cb, slice, shape[0..column.shape.len]);
            built += 1;
        }
        // Failure leaves the newly created CTs/vector owned by this function.
        return self.outputs.ownViewsOf(self.allocator, views);
    }
};

const Ticket = struct {
    session: session_mod.Session,
    permit: ?*session_mod.RunPermit,
    gate: *std.atomic.Mutex,
    inputs: []const Tensor,
    control: ?Control,
    rows: usize,
};

test "tensor microbatch retained cross output does not pin obsolete columns" {
    const memory = @import("../runtime/tier/memory.zig");
    const alloc = std.testing.allocator;
    var controller = memory.AdmissionController{};
    const session = session_mod.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    const outputs = try alloc.alloc(Tensor, 2);
    outputs[0] = try Tensor.initFloat32(alloc, "logits", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    outputs[1] = try Tensor.initFloat32(alloc, "present.0.encoder.key", &.{ 2, 2 }, &.{ 5, 6, 7, 8 });
    const retained = try session_mod.retainedOutputAmounts(outputs, false);
    const owner = try alloc.create(SharedOutputs);
    const columns = try alloc.alloc(SharedOutputs.Column, 2);
    owner.* = .{ .allocator = alloc, .outputs = outputs, .columns = columns, .remaining = .init(2), .permit = try session.admitHostPreprocess(retained.host_scratch_bytes) };
    owner.credits = try session_mod.OutputCredits.init(alloc, outputs, false, true);
    for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
    const first = try owner.rows(0, 1, 2);
    const second = try owner.rows(1, 1, 2);
    defer alloc.free(first);
    defer alloc.free(second);
    SharedOutputs.release(owner);
    first[0].deinit();
    second[0].deinit();
    try std.testing.expectEqual(retained.host_scratch_bytes / 2, controller.snapshot().host_scratch_bytes);
    try std.testing.expect(!try session.compactExclusiveRows(alloc, second[1..], 0, null));
    first[1].deinit();
    try std.testing.expectEqual(retained.host_scratch_bytes / 2, controller.snapshot().host_scratch_bytes);
    try std.testing.expect(try session.compactExclusiveRows(alloc, second[1..], 0, null));
    try std.testing.expectEqual(@as(usize, 24), controller.snapshot().hostTotalBytes());
    try std.testing.expectEqual(second[1].data.len, second[1].shared_storage.?.len);
    try std.testing.expectEqualSlices(f32, &.{ 7, 8 }, second[1].asFloat32());
    second[1].deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

/// Names and data remain valid through the last consumer, not merely the last
/// submitter. Slices share a batch allocation but never share mutable rows.
const SharedOutputs = struct {
    allocator: std.mem.Allocator,
    outputs: []Tensor,
    permit: ?session_mod.RunPermit,
    columns: []Column,
    remaining: std.atomic.Value(usize),
    credits: ?session_mod.OutputCredits = null,
    mutex: std.atomic.Mutex = .unlocked,

    const Column = struct {
        owner: *SharedOutputs,
        index: usize,
        refs: std.atomic.Value(usize) = .init(1),

        fn isExclusive(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The producer's root reference prevents exclusivity until all
            // views have been published. No new views can be created afterward.
            return self.refs.load(.acquire) == 1;
        }

        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const owner = self.owner;
            // A persistent cross-cache must not pin obsolete logits/self-cache.
            platform.sync.lockYielding(&owner.mutex);
            const output = &owner.outputs[self.index];
            if (owner.permit) |*permit| if (permit.lease) |*lease| {
                output.deinit();
                owner.credits.?.release(self.index, lease);
            } else output.deinit() else output.deinit();
            owner.mutex.unlock();
            if (owner.remaining.fetchSub(1, .acq_rel) != 1) return;
            if (owner.permit) |*permit| permit.deinit();
            if (owner.credits) |*credits| credits.deinit(owner.allocator);
            owner.allocator.free(owner.outputs);
            owner.allocator.free(owner.columns);
            owner.allocator.destroy(owner);
        }
    };

    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const columns = self.columns;
        for (columns) |*column| Column.release(column);
    }

    fn rows(self: *@This(), start: usize, count: usize, total: usize) ![]Tensor {
        const output = try self.allocator.alloc(Tensor, self.outputs.len);
        var initialized: usize = 0;
        errdefer {
            for (output[0..initialized]) |*tensor| tensor.deinit();
            self.allocator.free(output);
        }
        for (self.outputs, output, self.columns) |tensor, *view, *column| {
            const shape = try self.allocator.dupe(i64, tensor.shape);
            if (shape[0] != 0) shape[0] = @intCast(count);
            const stride = tensor.data.len / total;
            view.* = .{
                .data = tensor.data[start * stride ..][0 .. count * stride],
                .dtype = tensor.dtype,
                .shape = shape,
                .name = tensor.name,
                .allocator = self.allocator,
                .owns_data = false,
                .owns_shape = true,
                .lifetime = .{ .context = column, .release = Column.release, .is_exclusive = Column.isExclusive },
                .shared_storage = tensor.data,
                .admitted_storage_domain = if (self.permit) |permit| if (permit.lease) |lease| lease.controller else null else null,
            };
            _ = column.refs.fetchAdd(1, .monotonic);
            initialized += 1;
        }
        return output;
    }
};

fn destroy(allocator: std.mem.Allocator, tensors: []Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn validTensor(tensor: Tensor) bool {
    var elements: usize = 1;
    for (tensor.shape) |dim| {
        if (dim < 0) return false;
        elements = std.math.mul(usize, elements, @intCast(dim)) catch return false;
    }
    return (std.math.mul(usize, elements, tensor.dtype.byteSize()) catch return false) == tensor.data.len;
}

/// Dynamic shape metadata alone does not establish semantic independence.
/// Installation by a row-independent pipeline stage is the explicit opt-in.
pub fn eligible(session: session_mod.Session, inputs: []const Tensor) bool {
    if (inputs.len == 0) return false;
    if (inputs[0].shape.len == 0 or inputs[0].shape[0] <= 0) return false;
    for (inputs, 0..) |input, index| {
        if (!validTensor(input)) return false;
        if (session.isBroadcastInput(input.name)) {
            if (input.data.len == 0 or input.data.len > 8) return false;
        } else if (input.shape.len == 0 or input.shape[0] != inputs[0].shape[0]) return false;
        for (inputs[0..index]) |previous| if (std.mem.eql(u8, previous.name, input.name)) return false;
    }
    if (session.vtable.independentBatchRows) |qualified| return qualified(session.ptr, inputs);
    const declared = session.inputInfo();
    const outputs = session.outputInfo();
    if (declared.len != inputs.len or outputs.len == 0) return false;
    for (inputs) |input| {
        var matched = false;
        for (declared) |info| {
            if (!std.mem.eql(u8, info.name, input.name)) continue;
            if (info.dtype != input.dtype or info.shape.len != input.shape.len) return false;
            if (session.isBroadcastInput(input.name)) {
                if (!std.mem.eql(i64, info.shape, input.shape)) return false;
            } else {
                if (info.shape[0] > 0) return false;
                for (info.shape[1..], input.shape[1..]) |expected, actual| if (expected > 0 and expected != actual) return false;
            }
            matched = true;
            break;
        }
        if (!matched) return false;
    }
    for (outputs) |info| {
        if (info.shape.len == 0 or info.shape[0] > 0) return false;
        if (session.vtable.runGeometry == null and session.output_geometry == null and session.cached_decoder_geometry == null) {
            // The legacy text-stage opt-in preserves a 2-D token axis only.
            // Audio/image transforms and unresolved feature widths require a
            // concrete stage contract before their outputs may be fused.
            for (info.shape[1..], 1..) |dim, axis| {
                if (dim <= 0 and !(axis == 1 and inputs[0].shape.len == 2)) return false;
            }
        }
    }
    return true;
}

pub fn run(broker: *micro.Broker, allocator: std.mem.Allocator, io: std.Io, task: micro.Task, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: *std.atomic.Mutex, inputs: []const Tensor, control: ?Control, deadline: ?std.Io.Clock.Timestamp, wait_us: u64) ![]Tensor {
    return runMode(.host, broker, allocator, io, task, session, permit, gate, inputs, control, deadline, wait_us);
}

pub fn runResident(broker: *micro.Broker, allocator: std.mem.Allocator, io: std.Io, task: micro.Task, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: *std.atomic.Mutex, inputs: []const Tensor, control: ?Control, deadline: ?std.Io.Clock.Timestamp, wait_us: u64) !session_mod.ResidentOutputs {
    return runMode(.resident, broker, allocator, io, task, session, permit, gate, inputs, control, deadline, wait_us);
}

const OutputPlacement = enum { host, resident };
fn OutputType(comptime placement: OutputPlacement) type {
    return if (placement == .host) []Tensor else session_mod.ResidentOutputs;
}
fn destroyMode(comptime placement: OutputPlacement, allocator: std.mem.Allocator, value: OutputType(placement)) void {
    if (comptime placement == .host) destroy(allocator, value) else {
        var owned = value;
        owned.deinit();
    }
}

fn runMode(comptime placement: OutputPlacement, broker: *micro.Broker, allocator: std.mem.Allocator, io: std.Io, task: micro.Task, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: *std.atomic.Mutex, inputs: []const Tensor, control: ?Control, deadline: ?std.Io.Clock.Timestamp, wait_us: u64) !OutputType(placement) {
    if (control) |active| try active.check();
    // A resident result owns its execution lease. Keep a reusable supplied
    // permit in yielded input-residency mode even on the direct path.
    if (comptime placement == .resident) if (permit) |borrowed| {
        if (!try borrowed.yieldExecution() and borrowed.lease != null) return error.MissingRunRequest;
    };
    if (!eligible(session, inputs)) return directMode(placement, allocator, .{ .session = session, .permit = permit, .gate = gate, .inputs = inputs, .control = control, .rows = 1 });
    const alloc = std.heap.smp_allocator;
    var signature = std.Io.Writer.Allocating.init(allocator);
    defer signature.deinit();
    try signature.writer.print("{s};", .{@tagName(placement)});
    try signature.writer.print("{d}:{d};", .{ @intFromPtr(session.vtable), @intFromPtr(gate) });
    try std.json.Stringify.value(session.output_geometry, .{}, &signature.writer);
    try std.json.Stringify.value(session.cached_decoder_geometry, .{}, &signature.writer);
    try std.json.Stringify.value(session.broadcast_inputs, .{}, &signature.writer);
    try std.json.Stringify.value(if (control) |active| active.native_call_policy else @import("../execution_control.zig").NativeCallPolicy.require_supervision, .{}, &signature.writer);
    // A fused native call must stay inside the same supervising boundary.
    // Never inherit just one caller's deadline/cancellation for its peers.
    if (control) |active| if (active.hard_cancellation) |boundary| {
        try signature.writer.print("hard:{d}:{d}:{d};", .{ @intFromPtr(boundary.ptr), @intFromPtr(boundary.arm_fn), @intFromPtr(boundary.disarm_fn) });
    };
    if (session.run_admission) |admission| try std.json.Stringify.value(.{
        .controller = @intFromPtr(admission.controller),
        .backend = admission.backend_class,
        .limits = admission.limits,
        .workspace = admission.static_workspace_bytes,
        .workspace_reserved = admission.backend_workspace_reserved,
        .profile = admission.model_profile,
        .check_live_memory = admission.check_live_memory,
    }, .{}, &signature.writer);
    var bytes: usize = 0;
    for (inputs) |tensor| {
        bytes = try std.math.add(usize, bytes, tensor.data.len);
        const broadcast = session.isBroadcastInput(tensor.name);
        try std.json.Stringify.value(.{ .name = tensor.name, .dtype = tensor.dtype, .shape = if (broadcast) tensor.shape else tensor.shape[1..], .value = if (broadcast) tensor.data else &.{} }, .{}, &signature.writer);
    }
    const ticket = Ticket{ .session = session, .permit = permit, .gate = gate, .inputs = inputs, .control = control, .rows = @intCast(inputs[0].shape[0]) };
    // Requests already larger than the coalescing window keep their existing
    // native batch. Do not turn a valid caller batch into a broker size error.
    if (ticket.rows > 64 or bytes > 64 * 1024 * 1024) return directMode(placement, allocator, ticket);
    if (permit) |borrowed| {
        if (!try borrowed.yieldExecution()) return directMode(placement, allocator, ticket);
    }
    const Probe = struct {
        fn canceled(raw: *const anyopaque) bool {
            const active: *const Control = @ptrCast(@alignCast(raw));
            active.check() catch return true;
            return false;
        }
    };
    const Executor = struct {
        fn call(raw: *anyopaque, items: []const micro.ExecuteItem) void {
            executeMode(placement, raw, items);
        }
    };
    const results = try broker.submitBatchControlled(Ticket, OutputType(placement), io, alloc, .{
        .model = "tensor-forward",
        .generation = @intFromPtr(session.ptr),
        .task = task,
        .schema = signature.written(),
        .resource_class = if (session.backend().usesGpuHostedSession()) .gpu else .cpu,
    }, .{ .mode = .native, .preferred_items = 8, .max_items = 8, .max_tokens = 64, .max_bytes = 64 * 1024 * 1024, .max_wait_us = wait_us }, &.{.{ .bytes = bytes, .tokens = ticket.rows }}, &.{.{}}, deadline, if (control) |*active| .{ .ptr = active, .is_cancelled_fn = Probe.canceled } else .{}, &.{ticket}, broker, Executor.call);
    var owns_result = true;
    defer {
        if (owns_result) for (results) |result| switch (result.result) {
            .value => |value| destroyMode(placement, alloc, value),
            .item_error => {},
        };
        alloc.free(results);
    }
    switch (results[0].result) {
        .item_error => |err| return err.cause,
        .value => |value| {
            if (control) |active| try active.check();
            // Move tensor ownership after joining; only the small container
            // uses the caller allocator. No copies of encoder hidden states.
            if (comptime placement == .host) {
                const output = try allocator.dupe(Tensor, value);
                alloc.free(value);
                owns_result = false;
                return output;
            } else {
                owns_result = false;
                return value;
            }
        },
    }
}

fn execute(raw: *anyopaque, items: []const micro.ExecuteItem) void {
    executeMode(.host, raw, items);
}
fn executeGroup(items: []const micro.ExecuteItem) !void {
    return executeGroupMode(.host, items);
}

fn directMode(comptime placement: OutputPlacement, allocator: std.mem.Allocator, ticket: Ticket) !OutputType(placement) {
    const control = ticket.control orelse Control{};
    // A caller's preprocessing/input owner remains alive independently of the
    // new exact execution. Release idle workspace before queueing ready work.
    if (ticket.permit) |permit| _ = try permit.yieldExecution();
    var cohort_waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    try ticket.session.selectReadyCohort(ticket.gate, control, &cohort_waiter);
    defer ticket.session.leaveSelectedCohort(&cohort_waiter);
    var cleanup_owner: @import("../execution/cleanup_owner.zig").Owner = .{};
    defer cleanup_owner.finishAfterUnlock();
    if (comptime placement == .resident)
        cleanup_owner = try @import("../execution/cleanup_owner.zig").Owner.beginResident(ticket.session);
    // A request-less explicit permit retains its original custom callback and
    // quota policy. Only request-owning permits split off fresh execution.
    var owned_execution: ?session_mod.RunPermit = if (ticket.permit) |permit|
        if (permit.request != null) try permit.acquireExecutionInputsBeforeExecution(.{ .host = ticket.inputs }, allocator, ticket.control) else null
    else
        try ticket.session.admitInputsBeforeExecution(ticket.inputs, allocator, ticket.control);
    defer if (owned_execution) |*execution| execution.deinitWithCleanup(.{ .boundary = cleanup_owner.epoch() });
    const execution = if (owned_execution) |*owned| owned else ticket.permit.?;
    if (comptime placement == .resident) try execution.prepareResidentOutputRetirement();
    try execution.preparePhysicalStorage(allocator);
    if (comptime placement == .host)
        cleanup_owner = try @import("../execution/cleanup_owner.zig").Owner.beginAdmitted(ticket.session);
    if (cleanup_owner.epoch()) |epoch| try execution.bindPhysicalCleanupEpoch(epoch);
    // Exact prepared resources and admission exist before the actual gate.
    try ticket.session.acquireSelectedCohort(control, &cohort_waiter);
    defer ticket.session.releaseSelectedCohortGate(&cohort_waiter);
    if (comptime placement == .resident) {
        var outputs = (try execution.runResidentWithControl(ticket.inputs, allocator, ticket.control)) orelse return error.UnexpectedResidentStageDecline;
        errdefer outputs.deinitWithBoundary(cleanup_owner.epoch());
        _ = try cleanup_owner.retainThroughBackend(outputs.backend);
        if (owned_execution != null and outputs.resource_lease == null) {
            outputs.resource_lease = execution.lease;
            execution.lease = null;
        }
        ticket.session.releaseSelectedCohortGate(&cohort_waiter);
        execution.retireExecutionWithCleanup(.{ .boundary = cleanup_owner.epoch() });
        return outputs;
    } else {
        const outputs = try execution.runWithControl(ticket.inputs, allocator, ticket.control);
        errdefer destroy(allocator, outputs);
        ticket.session.releaseSelectedCohortGate(&cohort_waiter);
        execution.retireExecutionWithCleanup(.{ .boundary = cleanup_owner.epoch() });
        // Output-quota reduction and ownership publication are producer
        // boundaries too; perform them after numerical/model unlock.
        return if (owned_execution != null) execution.attachHostOutputs(outputs, allocator) else outputs;
    }
}

fn executeMode(comptime placement: OutputPlacement, _: *anyopaque, items: []const micro.ExecuteItem) void {
    // Regroup rows from the same immutable encoder allocation in source order.
    // Result slots travel with tickets, so request arrival order is irrelevant.
    var ordered: [8]micro.ExecuteItem = undefined;
    std.debug.assert(items.len <= ordered.len);
    @memcpy(ordered[0..items.len], items);
    std.mem.sort(micro.ExecuteItem, ordered[0..items.len], {}, struct {
        fn key(item: micro.ExecuteItem) usize {
            var largest: usize = 0;
            var address: usize = 0;
            for (item.payloadAs(Ticket).inputs) |tensor| if (tensor.shared_storage != null and tensor.data.len > largest) {
                largest = tensor.data.len;
                address = @intFromPtr(tensor.data.ptr);
            };
            return address;
        }
        fn less(_: void, a: micro.ExecuteItem, b: micro.ExecuteItem) bool {
            return key(a) < key(b);
        }
    }.less);
    executeGroupMode(placement, ordered[0..items.len]) catch |err| {
        for (items) |item| if (!item.slot.completed) item.slot.fail(err);
    };
}

/// Only explicit allocation provenance allows joining rows. Pointer adjacency
/// alone is insufficient: unrelated allocations may happen to be neighbors.
fn sharedColumn(items: []const micro.ExecuteItem, column: usize) ?[]u8 {
    const first = items[0].payloadAs(Ticket).inputs[column];
    const storage = first.shared_storage orelse return null;
    const base = @intFromPtr(storage.ptr);
    const start = @intFromPtr(first.data.ptr);
    if (start < base or start - base > storage.len) return null;
    var offset = start - base;
    for (items) |item| {
        const tensor = item.payloadAs(Ticket).inputs[column];
        const backing = tensor.shared_storage orelse return null;
        if (backing.ptr != storage.ptr or backing.len != storage.len or @intFromPtr(tensor.data.ptr) != base + offset) return null;
        if (tensor.data.len > storage.len - offset) return null;
        offset += tensor.data.len;
    }
    return storage[start - base .. offset];
}

/// Capacity subdivision only happens before a forward. Already completed
/// subgroups are never replayed if a later subgroup fails.
fn subdivideMode(comptime placement: OutputPlacement, items: []const micro.ExecuteItem) void {
    std.debug.assert(items.len > 1);
    const split = items.len / 2;
    for ([_][]const micro.ExecuteItem{ items[0..split], items[split..] }) |part| {
        executeGroupMode(placement, part) catch |err| {
            for (part) |item| if (!item.slot.completed) item.slot.fail(err);
        };
    }
}

fn executeGroupMode(comptime placement: OutputPlacement, items: []const micro.ExecuteItem) !void {
    if (items.len == 0) return;
    if (items.len == 1) {
        const output = try directMode(placement, std.heap.smp_allocator, items[0].payloadAs(Ticket).*);
        items[0].slot.physical_group_key = @intFromPtr(items[0].slot);
        items[0].slot.setValue(OutputType(placement), output, if (items[0].payloadAs(Ticket).rows > 1) .native_batch else .serial);
        return;
    }
    const alloc = std.heap.smp_allocator;
    const first = items[0].payloadAs(Ticket);
    var rows: usize = 0;
    var bytes: usize = 0;
    var unadmitted_input_bytes: usize = 0;
    var packed_input_bytes: usize = 0;
    for (items) |item| {
        rows += item.payloadAs(Ticket).rows;
        for (item.payloadAs(Ticket).inputs) |tensor| {
            bytes = try std.math.add(usize, bytes, tensor.data.len);
        }
    }
    var request = try first.session.planRun(first.inputs, rows);
    request.input_bytes = bytes;
    request.pre_admitted_host_bytes = 0;
    for (first.inputs, 0..) |_, column| {
        const borrowing = sharedColumn(items, column) != null;
        if (!borrowing and !first.session.isBroadcastInput(first.inputs[column].name)) {
            packed_input_bytes = try std.math.add(usize, packed_input_bytes, try std.math.mul(usize, first.inputs[column].data.len / first.rows, rows));
        }
        for (items) |item| {
            const ticket = item.payloadAs(Ticket);
            const input = ticket.inputs[column];
            const covered = if (ticket.permit != null) input.data.len else try first.session.inputResidencyCredit(&.{input});
            if (borrowing) {
                // The logical model input is existing storage, not a new packed
                // allocation. Uncovered bytes are charged by input_bytes itself.
                request.pre_admitted_host_bytes = try std.math.add(usize, request.pre_admitted_host_bytes, covered);
            } else {
                unadmitted_input_bytes = try std.math.add(usize, unadmitted_input_bytes, input.data.len - covered);
            }
        }
    }
    // input_bytes covers the new packed allocation. Originals with a live
    // caller permit are already charged; retain only the uncovered originals.
    request.host_preprocess_bytes = unadmitted_input_bytes;
    var group = micro.ExecutionControl{ .items = items };
    var all_grace = true;
    var all_in_process = true;
    var group_grace_ns: ?u64 = null;
    for (items) |item| {
        const item_control = item.payloadAs(Ticket).control orelse {
            all_grace = false;
            all_in_process = false;
            continue;
        };
        if (item_control.native_call_policy != .in_process) all_in_process = false;
        if (item_control.cancellation_grace_ns) |grace_ns| {
            group_grace_ns = if (group_grace_ns) |current| @min(current, grace_ns) else grace_ns;
        } else {
            all_grace = false;
        }
    }
    const control = Control{
        .ptr = &group,
        .check_fn = micro.ExecutionControl.check,
        .io = items[0].control.io,
        // The aggregate check keeps the forward alive while any caller is
        // live. A scalar deadline from one caller would override that check.
        .deadline_ns = null,
        .native_call_policy = if (all_in_process) .in_process else .require_supervision,
        .hard_cancellation = if (first.control) |active| active.hard_cancellation else null,
        .cancellation_grace_ns = if (all_grace) group_grace_ns else null,
    };
    // One FIFO ready turn bounds resource admission, while suppliers can
    // reenter the original model gate. No numerical/provider scope is active.
    var cohort_waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    try first.session.selectReadyCohort(first.gate, control, &cohort_waiter);
    var cohort_owned = true;
    defer if (cohort_owned) first.session.leaveSelectedCohort(&cohort_waiter);
    var cleanup_owner: @import("../execution/cleanup_owner.zig").Owner = .{};
    defer cleanup_owner.finishAfterUnlock();
    if (comptime placement == .resident)
        cleanup_owner = try @import("../execution/cleanup_owner.zig").Owner.beginResident(first.session);
    const preparation_bytes = try std.math.add(usize, packed_input_bytes, unadmitted_input_bytes);
    var preparation = first.session.admitHostPreprocess(preparation_bytes) catch |err| switch (err) {
        error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => {
            first.session.leaveSelectedCohort(&cohort_waiter);
            cohort_owned = false;
            return subdivideMode(placement, items);
        },
    };
    var preparation_open = true;
    defer if (preparation_open) preparation.deinit();
    // The exact owned tensors below replace this separately admitted host
    // phase without double-charging their payload or uncovered source rows.
    request.pre_admitted_host_bytes = try std.math.add(usize, request.pre_admitted_host_bytes, preparation_bytes);
    const batch_inputs = try alloc.alloc(Tensor, first.inputs.len);
    var owns_inputs = true;
    var initialized: usize = 0;
    defer if (owns_inputs) {
        for (batch_inputs[0..initialized]) |*tensor| tensor.deinit();
        alloc.free(batch_inputs);
    };
    for (first.inputs, 0..) |input, column| {
        if (first.session.isBroadcastInput(input.name)) {
            batch_inputs[column] = input.borrowedView(input.name);
            initialized += 1;
            continue;
        }
        const shape = try alloc.dupe(i64, input.shape);
        errdefer alloc.free(shape);
        shape[0] = @intCast(rows);
        const shared = sharedColumn(items, column);
        const data = shared orelse try alloc.alloc(u8, input.data.len / first.rows * rows);
        var offset: usize = 0;
        if (shared == null) for (items) |item| {
            const source = item.payloadAs(Ticket).inputs[column].data;
            @memcpy(data[offset..][0..source.len], source);
            offset += source.len;
        };
        batch_inputs[column] = .{ .data = data, .shape = shape, .dtype = input.dtype, .name = input.name, .allocator = alloc, .owns_data = shared == null, .owns_shape = true };
        initialized += 1;
    }
    var permit = first.session.admitShapeRequestBeforeExecution(.{ .tensors = batch_inputs }, request, alloc, control) catch |err| switch (err) {
        error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => {
            destroy(alloc, batch_inputs);
            owns_inputs = false;
            preparation.deinit();
            preparation_open = false;
            first.session.leaveSelectedCohort(&cohort_waiter);
            cohort_owned = false;
            return subdivideMode(placement, items);
        },
        else => return err,
    };
    var owns_permit = true;
    defer if (owns_permit) permit.deinitWithCleanup(.{ .boundary = cleanup_owner.epoch() });
    if (comptime placement == .resident) try permit.prepareResidentOutputRetirement();
    try permit.preparePhysicalStorage(alloc);
    if (comptime placement == .host)
        cleanup_owner = try @import("../execution/cleanup_owner.zig").Owner.beginAdmitted(first.session);
    if (cleanup_owner.epoch()) |epoch| try permit.bindPhysicalCleanupEpoch(epoch);
    // The lexical cleanup owner binds prepared resources before this gate
    // and seals only after its eventual numerical unlock.
    try first.session.acquireSelectedCohort(control, &cohort_waiter);
    var locked = true;
    defer if (locked) first.session.releaseSelectedCohortGate(&cohort_waiter);
    if (comptime placement == .resident) {
        var outputs = (try permit.runResidentWithControl(batch_inputs, alloc, control)) orelse return error.UnexpectedResidentStageDecline;
        defer outputs.deinitWithBoundary(cleanup_owner.epoch());
        _ = try cleanup_owner.retainThroughBackend(outputs.backend);
        var batch = try ResidentBatch.init(alloc, &outputs, first.session, rows);
        var batch_open = true;
        defer if (batch_open) batch.deinit();
        const PreparedRow = union(enum) { empty, value: session_mod.ResidentOutputs, item_error: anyerror };
        var prepared: [8]PreparedRow = @splat(.empty);
        std.debug.assert(items.len <= prepared.len);
        defer for (prepared[0..items.len]) |*row| switch (row.*) {
            .value => |*value| value.deinitWithBoundary(cleanup_owner.epoch()),
            else => {},
        };
        for (batch_inputs) |*tensor| tensor.deinit();
        alloc.free(batch_inputs);
        owns_inputs = false;
        // Provider caches and escaped physical spans may survive the forward.
        // Share the complete reservation, rather than trim by logical bytes.
        if (outputs.resource_lease == null) {
            outputs.resource_lease = permit.lease;
            permit.lease = null;
        }
        var offset: usize = 0;
        for (items, prepared[0..items.len]) |item, *row| {
            const count = item.payloadAs(Ticket).rows;
            const start = offset;
            offset += count;
            item.control.check() catch |err| {
                row.* = .{ .item_error = err };
                continue;
            };
            const views = batch.rows(start, count) catch |err| {
                row.* = .{ .item_error = err };
                continue;
            };
            row.* = .{ .value = views };
        }
        batch.deinit();
        batch_open = false;
        first.session.releaseSelectedCohortGate(&cohort_waiter);
        locked = false;
        permit.retireExecutionWithCleanup(.{ .boundary = cleanup_owner.epoch() });
        // Publication/cancellation probes can enter the actual provider now.
        // A receiver never wakes while this actor owns its numerical lane.
        for (items, prepared[0..items.len]) |item, *row| {
            item.control.check() catch |err| {
                if (row.* == .value) row.value.deinitWithBoundary(cleanup_owner.epoch());
                row.* = .empty;
                item.slot.fail(err);
                continue;
            };
            switch (row.*) {
                .value => |value| {
                    item.slot.physical_group_key = @intFromPtr(items[0].slot);
                    item.slot.setValue(OutputType(placement), value, .native_batch);
                },
                .item_error => |err| item.slot.fail(err),
                .empty => unreachable,
            }
            row.* = .empty;
        }
        return;
    }
    const outputs = try permit.runWithControl(batch_inputs, alloc, control);
    var owns_outputs = true;
    defer if (owns_outputs) destroy(alloc, outputs);
    if (outputs.len == 0 or outputs.len != first.session.outputInfo().len) return error.InvalidFusedOutputShape;
    for (outputs, first.session.outputInfo()) |tensor, info| {
        const empty_cross_cache = first.session.cached_decoder_geometry != null and
            std.mem.startsWith(u8, tensor.name, "present.") and std.mem.indexOf(u8, tensor.name, ".encoder.") != null and
            tensor.data.len == 0 and tensor.shape.len == 4 and tensor.shape[0] == 0;
        if (!validTensor(tensor) or tensor.dtype != info.dtype or tensor.shape.len == 0 or (tensor.shape[0] != rows and !empty_cross_cache)) return error.InvalidFusedOutputShape;
        // Native multi-entry sessions qualify concrete stages independently
        // of their summary metadata. Graph contracts must match exactly.
        if (first.session.vtable.independentBatchRows == null) {
            if (tensor.shape.len != info.shape.len) return error.InvalidFusedOutputShape;
            if (!empty_cross_cache) for (tensor.shape[1..], info.shape[1..]) |actual, declared| if (declared > 0 and actual != declared) return error.InvalidFusedOutputShape;
        }
    }
    // No backend scratch or input packing survives this invocation. Keep only
    // the actual materialized outputs admitted across downstream decode steps.
    for (batch_inputs) |*tensor| tensor.deinit();
    alloc.free(batch_inputs);
    owns_inputs = false;
    first.session.releaseSelectedCohortGate(&cohort_waiter);
    locked = false;
    permit.retireExecutionWithCleanup(.{ .boundary = cleanup_owner.epoch() });
    try permit.retainOutputs(outputs);
    const owner = try alloc.create(SharedOutputs);
    errdefer alloc.destroy(owner);
    const columns = try alloc.alloc(SharedOutputs.Column, outputs.len);
    errdefer alloc.free(columns);
    const credits = if (permit.lease) |lease| try session_mod.OutputCredits.init(alloc, outputs, lease.amounts.host_kv_bytes > 0, true) else null;
    owner.* = .{ .allocator = alloc, .outputs = outputs, .permit = permit, .columns = columns, .remaining = .init(outputs.len), .credits = credits };
    for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
    owns_outputs = false;
    owns_permit = false;
    defer SharedOutputs.release(owner);
    var offset: usize = 0;
    for (items) |item| {
        const count = item.payloadAs(Ticket).rows;
        const start = offset;
        offset += count;
        item.control.check() catch |err| {
            item.slot.fail(err);
            continue;
        };
        const views = owner.rows(start, count, rows) catch |err| {
            item.slot.fail(err);
            continue;
        };
        item.slot.physical_group_key = @intFromPtr(items[0].slot);
        item.slot.setValue(OutputType(placement), views, .native_batch);
    }
}

const TestSession = struct {
    expanded: bool = false,
    last_input: usize = 0,
    calls: std.atomic.Value(usize) = .init(0),
    invalid_output: bool = false,
    cancel_after_forward: ?*std.atomic.Value(bool) = null,
    const Info = @import("../backends/tensor.zig").TensorInfo;
    const vtable = session_mod.Session.VTable{
        .run = forward,
        .runWithControl = controlled,
        .inputInfo = info,
        .outputInfo = info,
        .backend = backend,
        .close = close,
    };
    fn info(_: *anyopaque) []const Info {
        return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ -1, -1 } }};
    }
    fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    fn controlled(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: Control) ![]Tensor {
        try control.check();
        return forward(raw, inputs, allocator);
    }
    fn forward(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        _ = self.calls.fetchAdd(1, .monotonic);
        self.last_input = @intFromPtr(inputs[0].data.ptr);
        if (self.expanded) {
            const batch: usize = @intCast(inputs[0].shape[0]);
            const values = try allocator.alloc(f32, batch * 64);
            defer allocator.free(values);
            @memset(values, 2);
            const outputs = try allocator.alloc(Tensor, 1);
            errdefer allocator.free(outputs);
            outputs[0] = try Tensor.initFloat32(allocator, "values", &.{ @intCast(batch), 64 }, values);
            return outputs;
        }
        const values = try allocator.dupe(f32, inputs[0].asFloat32());
        defer allocator.free(values);
        for (values) |*value| value.* *= 2;
        var tensor = try Tensor.initFloat32(allocator, "values", if (self.invalid_output) &.{ 1, 1 } else inputs[0].shape, values);
        errdefer tensor.deinit();
        const outputs = try allocator.alloc(Tensor, 1);
        outputs[0] = tensor;
        if (self.cancel_after_forward) |flag| flag.store(true, .release);
        return outputs;
    }
};

const TestSubmit = struct {
    broker: *micro.Broker,
    session: session_mod.Session,
    gate: *std.atomic.Mutex,
    inputs: []const Tensor,
    task: micro.Task = .rerank,
    control: ?Control = null,
    output: ?[]Tensor = null,
    output_allocator: std.mem.Allocator = std.testing.allocator,
    err: ?anyerror = null,
    permit: ?*session_mod.RunPermit = null,

    fn submit(self: *@This()) std.Io.Cancelable!void {
        self.output = run(self.broker, std.testing.allocator, std.testing.io, self.task, self.session, self.permit, self.gate, self.inputs, self.control, null, 500_000) catch |err| {
            self.err = err;
            return;
        };
    }
    pub fn deinit(self: *@This()) void {
        if (self.output) |output| destroy(self.output_allocator, output);
        self.output = null;
    }
};

test "tensor microbatch executor deterministically fuses a complete window and retains outputs" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .output_allocator = std.heap.smp_allocator };
    defer for (&callers) |*caller| caller.deinit();
    // Supply a known complete window at the executor boundary. Whether eight
    // independent callers arrive before a coalescing timer expires is not a
    // correctness contract (nor a reliable assertion on a loaded CI host).
    var tickets: [8]Ticket = undefined;
    var outputs: [8][]Tensor = @splat(&.{});
    var slots: [8]micro.ResultSlot = undefined;
    var items: [8]micro.ExecuteItem = undefined;
    for (&tickets, &outputs, &slots, &items) |*ticket, *output, *slot, *item| {
        ticket.* = .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{input}, .control = null, .rows = 2 };
        try std.testing.expect(eligible(session, ticket.inputs));
        slot.* = .{ .output = @ptrCast(output) };
        item.* = .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = ticket, .slot = slot };
    }
    execute(&broker, &items);
    for (&callers, outputs, slots) |*caller, output, slot| {
        if (slot.completed and slot.err == null) caller.output = output;
        caller.err = slot.err;
    }
    for (slots) |slot| {
        try std.testing.expect(slot.completed);
        if (slot.err) |err| return err;
        try std.testing.expectEqual(micro.Execution.native_batch, slot.execution);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, caller.output.?[0].asFloat32());
        try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, caller.output.?[0].shape);
        try std.testing.expectEqual(callers[0].output.?[0].lifetime.?.context, caller.output.?[0].lifetime.?.context);
    }
    for (callers[0..7]) |*caller| caller.deinit();
    try std.testing.expectEqual(@as(usize, 8 * input.data.len + 2 * @sizeOf(i64)), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, callers[7].output.?[0].asFloat32());
    callers[7].deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch keeps a live caller after a peer deadline expires" {
    var fake = TestSession{};
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable };
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var tickets = [_]Ticket{
        .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{input}, .control = .{ .deadline_ns = 0 }, .rows = 1 },
        .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{input}, .control = .{ .deadline_ns = std.math.maxInt(u64) }, .rows = 1 },
    };
    var outputs: [2][]Tensor = @splat(&.{});
    var slots = [_]micro.ResultSlot{
        .{ .output = @ptrCast(&outputs[0]) },
        .{ .output = @ptrCast(&outputs[1]) },
    };
    defer for (slots, outputs) |slot, output| {
        if (slot.completed and slot.err == null) destroy(std.heap.smp_allocator, output);
    };
    var items = [_]micro.ExecuteItem{
        .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[0], .slot = &slots[0], .control = .{ .io = std.testing.io, .deadline = std.Io.Clock.Timestamp.now(std.testing.io, .awake) } },
        .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[1], .slot = &slots[1], .control = .{ .io = std.testing.io } },
    };
    try executeGroup(&items);
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    try std.testing.expectEqual(error.DeadlineExceeded, slots[0].err.?);
    try std.testing.expect(slots[1].completed and slots[1].err == null);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, outputs[1][0].asFloat32());
}

test "tensor microbatch broadcasts scalar controls and separates unequal values" {
    const Probe = struct {
        fn info(_: *anyopaque) []const TestSession.Info {
            return &.{
                .{ .name = "values", .dtype = .f32, .shape = &.{ -1, -1 } },
                .{ .name = "branch", .dtype = .bool_, .shape = &.{} },
            };
        }
        fn forward(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
            try std.testing.expectEqual(@as(usize, 0), inputs[1].shape.len);
            try std.testing.expectEqual(@as(usize, 1), inputs[1].data.len);
            return TestSession.forward(raw, inputs, allocator);
        }
    };
    var fake = TestSession{};
    var vtable = TestSession.vtable;
    vtable.inputInfo = Probe.info;
    vtable.run = Probe.forward;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .broadcast_inputs = &.{"branch"} };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate = std.atomic.Mutex.unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var branch_data = [_]u8{ 0, 1 };
    var inputs: [2][2]Tensor = undefined;
    for (&inputs, 0..) |*values, i| values.* = .{ input, .{ .name = "branch", .dtype = .bool_, .shape = &.{}, .data = branch_data[i..][0..1], .allocator = std.testing.allocator, .owns_shape = false, .owns_data = false } };
    var callers: [4]TestSubmit = undefined;
    for (&callers, 0..) |*caller, i| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &inputs[i % 2] };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, caller.output.?[0].asFloat32());
    }
    try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
}

test "tensor microbatch rejoins shared encoder rows without repacking and restores request order" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    var session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 2000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var inputs: [8]Tensor = undefined;
    var first: [8]TestSubmit = undefined;
    for (&inputs, &first, 0..) |*input, *caller, i| {
        input.* = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ @floatFromInt(i + 1), 1 });
        caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = input[0..1] };
    }
    defer for (&inputs) |*input| input.deinit();
    defer for (&first) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&first) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (first) |caller| if (caller.err) |err| return err;
    const backing = first[0].output.?[0].shared_storage.?;
    // Retained source (80) + physical execution (896) fits; fictitious
    // duplicate originals/packing would exceed 1000 and force singleton calls.
    session.run_admission.?.limits.host_limit_bytes = 1000;
    try std.testing.expectEqual(@as(usize, 8), try session.inputResidencyCredit(first[0].output.?));
    var other_controller = memory.AdmissionController{};
    var other_session = session;
    other_session.run_admission.?.controller = &other_controller;
    try std.testing.expectEqual(@as(usize, 0), try other_session.inputResidencyCredit(first[0].output.?));
    var renamed: [8]Tensor = undefined;
    var second: [8]TestSubmit = undefined;
    for (&renamed, &second, 0..) |*input, *caller, i| {
        input.* = first[7 - i].output.?[0].borrowedView("values");
        caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = input[0..1] };
    }
    defer for (&second) |*caller| caller.deinit();
    for (&second) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
    try std.testing.expectEqual(@intFromPtr(backing.ptr), fake.last_input);
    for (second, 0..) |caller, i| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ @floatFromInt(4 * (8 - i)), 4 }, caller.output.?[0].asFloat32());
    }
    for (&second) |*caller| caller.deinit();
    for (&first) |*caller| caller.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch yields idle workspace and permits remain reusable" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 8500 },
        .static_workspace_bytes = 1024,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var permits: [8]session_mod.RunPermit = undefined;
    var count: usize = 0;
    defer for (permits[0..count]) |*permit| permit.deinit();
    for (&permits) |*permit| {
        permit.* = try session.admit(try session_mod.RunRequest.fromTensors(&.{input}));
        count += 1;
    }
    var callers: [8]TestSubmit = undefined;
    for (&callers, &permits) |*caller, *permit| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .permit = permit };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (callers) |caller| if (caller.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    for (&callers) |*caller| caller.deinit();
    try std.testing.expectEqual(@as(usize, 8 * input.data.len), controller.snapshot().host_scratch_bytes);
    const reused = try permits[0].run(&.{input}, std.testing.allocator);
    destroy(std.testing.allocator, reused);
    try std.testing.expectEqual(@as(usize, 8 * input.data.len), controller.snapshot().host_scratch_bytes);
    for (&permits) |*permit| permit.deinit();
    count = 0;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch admits expanded GPU outputs before forwarding" {
    const memory = @import("../runtime/tier/memory.zig");
    const Geometry = struct {
        fn plan(_: *anyopaque, _: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
            return .{ .sequence = 2, .output_bytes = batch * 64 * 4 + 16 };
        }
    };
    var controller = memory.AdmissionController{};
    var fake = TestSession{ .expanded = true };
    var vtable = TestSession.vtable;
    vtable.runGeometry = Geometry.plan;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{ .host_limit_bytes = 5000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqual(@as(usize, 64), caller.output.?[0].asFloat32().len);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 8 * 64 * 4 + 16), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    for (&callers) |*caller| caller.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch reusable permit expands native output geometry" {
    const memory = @import("../runtime/tier/memory.zig");
    const Geometry = struct {
        fn plan(_: *anyopaque, _: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
            return .{ .sequence = 2, .output_bytes = batch * 64 * 4 + 16 };
        }
    };
    var controller = memory.AdmissionController{};
    var fake = TestSession{ .expanded = true };
    var vtable = TestSession.vtable;
    vtable.runGeometry = Geometry.plan;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{ .host_limit_bytes = 5000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var larger = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer larger.deinit();
    var permit = try session.admit(try session_mod.RunRequest.fromTensors(&.{larger}));
    defer permit.deinit();
    const small_outputs = try permit.run(&.{input}, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 64 * 4 + 16), permit.request.?.output_bytes.?);
    destroy(std.testing.allocator, small_outputs);
    const large_outputs = try permit.run(&.{larger}, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2 * 64 * 4 + 16), permit.request.?.output_bytes.?);
    try std.testing.expectEqual(@as(usize, 128), large_outputs[0].asFloat32().len);
    try std.testing.expectEqual(@as(usize, larger.data.len + 2 * 64 * 4 + 16), controller.snapshot().host_scratch_bytes);
    destroy(std.testing.allocator, large_outputs);
    permit.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch rejects ambiguous input layouts and fixed batch graphs" {
    var fake = TestSession{};
    var vtable = TestSession.vtable;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable };
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    try std.testing.expect(eligible(session, &.{input}));
    try std.testing.expect(!eligible(session, &.{ input, input }));
    var bad = input.borrowedView("values");
    bad.shape = &.{ 2, 2 };
    try std.testing.expect(!eligible(session, &.{bad}));
    const Fixed = struct {
        fn info(_: *anyopaque) []const TestSession.Info {
            return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ 1, 2 } }};
        }
        fn veto(_: *anyopaque, _: []const Tensor) bool {
            return false;
        }
    };
    vtable.outputInfo = Fixed.info;
    try std.testing.expect(!eligible(session, &.{input}));
    vtable.outputInfo = TestSession.info;
    vtable.independentBatchRows = Fixed.veto;
    try std.testing.expect(!eligible(session, &.{input}));
}

test "tensor microbatch isolates tasks and fails malformed fused output without replay" {
    for ([_]bool{ false, true }) |malformed| {
        var fake = TestSession{ .invalid_output = malformed };
        const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable };
        var broker = micro.Broker.init(std.testing.allocator);
        defer broker.deinit();
        var gate: std.atomic.Mutex = .unlocked;
        var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
        defer input.deinit();
        var callers: [8]TestSubmit = undefined;
        for (&callers, 0..) |*caller, i| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .task = if (!malformed and i >= 4) .extract else .rerank };
        defer for (&callers) |*caller| caller.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(std.testing.io);
        for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
        try group.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, if (malformed) 1 else 2), fake.calls.load(.monotonic));
        for (callers) |caller| {
            if (malformed) try std.testing.expectEqual(error.InvalidFusedOutputShape, caller.err.?) else if (caller.err) |err| return err;
        }
    }
}

test "tensor microbatch row-view ownership unwinds every allocation failure" {
    const Probe = struct {
        fn create(allocator: std.mem.Allocator) !*SharedOutputs {
            const owner = try allocator.create(SharedOutputs);
            errdefer allocator.destroy(owner);
            const outputs = try allocator.alloc(Tensor, 2);
            errdefer allocator.free(outputs);
            outputs[0] = try Tensor.initFloat32(allocator, "a", &.{ 2, 1 }, &.{ 1, 2 });
            errdefer outputs[0].deinit();
            outputs[1] = try Tensor.initFloat32(allocator, "b", &.{ 2, 1 }, &.{ 3, 4 });
            errdefer outputs[1].deinit();
            const columns = try allocator.alloc(SharedOutputs.Column, outputs.len);
            owner.* = .{ .allocator = allocator, .outputs = outputs, .permit = null, .columns = columns, .remaining = .init(outputs.len) };
            for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
            return owner;
        }
        fn check(allocator: std.mem.Allocator) !void {
            const owner = try create(allocator);
            defer SharedOutputs.release(owner);
            const view = try owner.rows(1, 1, 2);
            defer destroy(owner.allocator, view);
            std.debug.assert(view[0].asFloat32()[0] == 2);
            std.debug.assert(view[1].asFloat32()[0] == 4);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.check, .{});
}

test "tensor microbatch admission subdivision happens before any fused forward" {
    for ([_]bool{ false, true }) |permanent| {
        const memory = @import("../runtime/tier/memory.zig");
        var controller = memory.AdmissionController{};
        if (!permanent) controller.configureForcedRunDenialsForTesting(1);
        var fake = TestSession{};
        const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
            .controller = &controller,
            .backend_class = .cpu,
            .limits = if (permanent) .{ .host_limit_bytes = 800 } else .{},
            .static_workspace_bytes = 1,
            .check_live_memory = false,
        } };
        var broker = micro.Broker.init(std.testing.allocator);
        defer broker.deinit();
        var gate: std.atomic.Mutex = .unlocked;
        var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
        defer input.deinit();
        var callers: [8]TestSubmit = undefined;
        for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
        defer for (&callers) |*caller| caller.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(std.testing.io);
        for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
        try group.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
        for (&callers) |*caller| {
            if (caller.err) |err| return err;
            try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, caller.output.?[0].asFloat32());
            caller.deinit();
        }
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        try std.testing.expectEqual(@as(u64, 8), broker.snapshot(std.testing.io).native_items);
        try std.testing.expectEqual(@as(u64, 2), broker.snapshot(std.testing.io).native_batches);
    }
}

test "tensor microbatch canceled consumer does not discard healthy fused rows" {
    var canceled = std.atomic.Value(bool).init(false);
    const Probe = struct {
        fn check(raw: ?*anyopaque) bool {
            const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return flag.load(.acquire);
        }
    };
    var fake = TestSession{ .cancel_after_forward = &canceled };
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
    callers[0].control = .{ .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = Probe.check } };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    const canceled_error = callers[0].err orelse return error.ExpectedCancellation;
    try std.testing.expect(canceled_error == error.Canceled or canceled_error == error.Cancelled);
    for (callers[1..]) |caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, caller.output.?[0].asFloat32());
    }
}

const ResidentProbe = struct {
    backend: @import("../ops/ops.zig").ComputeBackend,
    gate: *std.atomic.Mutex,
    calls: usize = 0,
    host_calls: usize = 0,
    closed: bool = false,
    cleanup_charged: bool = false,
    controller: ?*@import("../runtime/tier/memory.zig").AdmissionController = null,
    malformed: bool = false,
    transpose: bool = false,
    fail_host: bool = false,
    cancel_after_forward: ?*std.atomic.Value(bool) = null,
    const vtable = session_mod.Session.VTable{
        .run = host,
        .runWithControl = hostControlled,
        .runResident = resident,
        .runResidentWithControl = residentControlled,
        .inputInfo = info,
        .outputInfo = info,
        .backend = kind,
        .close = close,
    };
    fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
        return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ -1, 2 } }};
    }
    fn kind(raw: *anyopaque) @import("../backends/backends.zig").BackendType {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return switch (self.backend.kind()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            else => unreachable,
        };
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.closed = true;
    }
    fn cleanup(raw: *anyopaque, _: std.mem.Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.controller) |controller| self.cleanup_charged = controller.snapshot().hostTotalBytes() + controller.snapshot().backendTotalBytes() > 0;
    }
    fn resident(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) !?session_mod.ResidentOutputs {
        return residentImpl(raw, inputs, allocator, null);
    }
    fn residentControlled(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: Control) !?session_mod.ResidentOutputs {
        return residentImpl(raw, inputs, allocator, control);
    }
    fn residentImpl(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?Control) !?session_mod.ResidentOutputs {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (control) |active| try active.check();
        if (self.gate.tryLock()) {
            self.gate.unlock();
            return error.ResidentDispatchMissingGate;
        }
        self.calls += 1;
        const batch = inputs[0].shape[0];
        const source = blk: {
            const host_input = try self.backend.fromFloat32Shape(inputs[0].asFloat32(), &.{ @intCast(batch), 2 });
            errdefer self.backend.free(host_input);
            if (try self.backend.ensureDeviceResident(host_input)) |device| {
                self.backend.free(host_input);
                break :blk device;
            }
            break :blk host_input;
        };
        defer self.backend.free(source);
        const doubled = try self.backend.add(source, source);
        var doubled_open = true;
        defer if (doubled_open) self.backend.free(doubled);
        const value = if (self.transpose) blk: {
            const reshaped = try self.backend.primReshape(doubled, &.{ 2, batch });
            defer self.backend.free(reshaped);
            break :blk try self.backend.primTranspose(reshaped, &.{ 1, 0 }, &.{ 2, batch });
        } else if (self.malformed) try self.backend.primReshape(doubled, &.{ 1, batch * 2 }) else blk: {
            doubled_open = false;
            break :blk doubled;
        };
        errdefer self.backend.free(value);
        const outputs = try allocator.alloc(@import("../ops/ops.zig").CT, 1);
        outputs[0] = value;
        if (self.cancel_after_forward) |flag| flag.store(true, .release);
        return .{ .outputs = outputs, .backend = &self.backend, .allocator = allocator, .backend_owner = self, .deinit_backend_owner = cleanup };
    }
    fn host(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        return hostImpl(raw, inputs, allocator, null);
    }
    fn hostControlled(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: Control) ![]Tensor {
        return hostImpl(raw, inputs, allocator, control);
    }
    fn hostImpl(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?Control) ![]Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.host_calls += 1;
        if (self.fail_host) return error.ResidentDispatchUsedHost;
        var result = (try residentImpl(raw, inputs, allocator, control)).?;
        defer result.deinit();
        const values = try self.backend.toFloat32(result.outputs[0], allocator);
        defer allocator.free(values);
        const outputs = try allocator.alloc(Tensor, 1);
        errdefer allocator.free(outputs);
        outputs[0] = try Tensor.initFloat32(allocator, "values", inputs[0].shape, values);
        return outputs;
    }
};

fn checkResidentCohort(cb: @import("../ops/ops.zig").ComputeBackend, _: @import("../ops/ops.zig").ComputeBackend) !void {
    const a = std.testing.allocator;
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var probe = ResidentProbe{ .backend = cb, .gate = &gate, .controller = &controller, .fail_host = true };
    var lifetime = session_mod.Session.ModelLifetime{ .owner = &probe, .destroy_fn = ResidentProbe.close };
    const session = session_mod.Session{ .ptr = &probe, .vtable = &ResidentProbe.vtable, .model_lifetime = &lifetime, .run_admission = .{ .controller = &controller, .backend_class = if (cb.kind() == .native) .cpu else .gpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    var session_open = true;
    defer if (session_open) session.close();
    var first = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer first.deinit();
    var second = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 5, 6, 7, 8 });
    defer second.deinit();
    var tickets = [_]Ticket{ .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{first}, .control = .{ .native_call_policy = .in_process }, .rows = 2 }, .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{second}, .control = .{ .native_call_policy = .in_process }, .rows = 2 } };
    var outputs: [2]session_mod.ResidentOutputs = undefined;
    var slots = [_]micro.ResultSlot{ .{ .output = &outputs[0] }, .{ .output = &outputs[1] } };
    var items = [_]micro.ExecuteItem{ .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[0], .slot = &slots[0], .control = .{ .io = std.testing.io } }, .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[1], .slot = &slots[1], .control = .{ .io = std.testing.io } } };
    var open = [_]bool{ false, false };
    defer for (&outputs, open) |*output, live| {
        if (live) output.deinit();
    };
    const before_forward = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
    try executeGroupMode(.resident, &items);
    try before_forward.expectUnchanged(cb);
    for (&slots, &open) |*slot, *live| {
        try std.testing.expect(slot.completed);
        if (slot.err) |err| return err;
        live.* = true;
        try std.testing.expectEqual(micro.Execution.native_batch, slot.execution);
    }
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), probe.host_calls);
    try std.testing.expect(outputs[0].backend_owner == outputs[1].backend_owner);
    try std.testing.expect(outputs[0].backend == outputs[1].backend);
    const bytes_before = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
    const shape = try outputs[0].backend.tensorShape(outputs[0].outputs[0], a);
    defer a.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);
    try bytes_before.expectUnchanged(cb);
    session.close();
    session_open = false;
    try std.testing.expect(!probe.closed);
    outputs[0].deinit();
    open[0] = false;
    try std.testing.expect(!probe.closed);
    try std.testing.expect(controller.snapshot().hostTotalBytes() + controller.snapshot().backendTotalBytes() > 0);
    const remaining = try outputs[1].backend.toFloat32(outputs[1].outputs[0], a);
    defer a.free(remaining);
    try std.testing.expectEqualSlices(f32, &.{ 10, 12, 14, 16 }, remaining);
    outputs[1].deinit();
    open[1] = false;
    try std.testing.expect(probe.closed and probe.cleanup_charged);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "resident tensor microbatch actual Native cohort shares full quota and model through sibling retirement" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkResidentCohort);
}

test "resident tensor microbatch actual Metal cohort shares full quota and model through sibling retirement" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(helper.noWeights, checkResidentCohort);
}

fn checkResidentFaults(cb: @import("../ops/ops.zig").ComputeBackend, _: @import("../ops/ops.zig").ComputeBackend) !void {
    const a = std.testing.allocator;
    for (0..3) |scenario| {
        var canceled = std.atomic.Value(bool).init(false);
        var gate: std.atomic.Mutex = .unlocked;
        var probe = ResidentProbe{ .backend = cb, .gate = &gate, .malformed = scenario == 0, .transpose = scenario == 2, .cancel_after_forward = if (scenario == 1) &canceled else null, .fail_host = true };
        const session = session_mod.Session{ .ptr = &probe, .vtable = &ResidentProbe.vtable };
        var first = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
        defer first.deinit();
        var second = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 5, 6, 7, 8 });
        defer second.deinit();
        var tickets = [_]Ticket{ .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{first}, .control = .{}, .rows = 2 }, .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{second}, .control = .{}, .rows = 2 } };
        var outputs: [2]session_mod.ResidentOutputs = undefined;
        var slots = [_]micro.ResultSlot{ .{ .output = &outputs[0] }, .{ .output = &outputs[1] } };
        var items = [_]micro.ExecuteItem{ .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[0], .slot = &slots[0], .control = .{ .io = std.testing.io, .cancel_requested = if (scenario == 1) &canceled else null } }, .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[1], .slot = &slots[1], .control = .{ .io = std.testing.io } } };
        var broker = micro.Broker.init(a);
        defer broker.deinit();
        executeMode(.resident, &broker, &items);
        defer for (&outputs, slots) |*output, slot| {
            if (slot.completed and slot.err == null) output.deinit();
        };
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectEqual(@as(usize, 0), probe.host_calls);
        if (scenario == 0) {
            // A malformed physical result is an error after one submitted
            // forward, never permission to retry a serial/host route.
            for (slots) |slot| try std.testing.expectEqual(error.InvalidFusedOutputShape, slot.err.?);
        } else {
            if (scenario == 1) try std.testing.expectEqual(error.Canceled, slots[0].err.?) else try std.testing.expect(slots[0].err == null);
            try std.testing.expect(slots[1].completed and slots[1].err == null);
            const values = try outputs[1].backend.toFloat32(outputs[1].outputs[0], a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, if (scenario == 2) &.{ 6, 14, 8, 16 } else &.{ 10, 12, 14, 16 }, values);
            if (scenario == 2) {
                const first_values = try outputs[0].backend.toFloat32(outputs[0].outputs[0], a);
                defer a.free(first_values);
                try std.testing.expectEqualSlices(f32, &.{ 2, 10, 4, 12 }, first_values);
            }
        }
    }
}

test "resident tensor microbatch fail closed after malformed forward preserve canceled sibling and noncontiguous Native rows" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkResidentFaults);
}

test "resident tensor microbatch row view and complete quota promotion unwind every allocation failure" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const native = @import("../ops/native_compute.zig");
            var weights = native.WeightStore{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty };
            defer weights.deinitOwned();
            var compute = native.NativeCompute.init(allocator, &weights, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            const value = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 4, 2 });
            var value_open = true;
            defer if (value_open) cb.free(value);
            const vector = try allocator.alloc(@import("../ops/ops.zig").CT, 1);
            vector[0] = value;
            value_open = false;
            var outputs = session_mod.ResidentOutputs{ .outputs = vector, .backend = &cb, .allocator = allocator };
            defer outputs.deinit();
            var gate: std.atomic.Mutex = .unlocked;
            var model = ResidentProbe{ .backend = cb, .gate = &gate };
            const session = session_mod.Session{ .ptr = &model, .vtable = &ResidentProbe.vtable };
            var batch = try ResidentBatch.init(allocator, &outputs, session, 4);
            defer batch.deinit();
            var first = try batch.rows(0, 2);
            defer first.deinit();
            var second = try batch.rows(2, 2);
            defer second.deinit();
            const values = try second.backend.toFloat32(second.outputs[0], allocator);
            defer allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8 }, values);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

fn checkResidentBrokerPlacement(cb: @import("../ops/ops.zig").ComputeBackend, _: @import("../ops/ops.zig").ComputeBackend) !void {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var broker = micro.Broker.init(a);
    defer broker.deinit();
    var lane: std.Io.Mutex = .init;
    broker.execution_mutex = &lane;
    var gate: std.atomic.Mutex = .unlocked;
    var probe = ResidentProbe{ .backend = cb, .gate = &gate };
    const session = session_mod.Session{ .ptr = &probe, .vtable = &ResidentProbe.vtable };
    var input = try Tensor.initFloat32(a, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var canceled = std.atomic.Value(bool).init(false);
    const Submit = struct {
        broker: *micro.Broker,
        session: session_mod.Session,
        gate: *std.atomic.Mutex,
        inputs: []const Tensor,
        placement: OutputPlacement,
        canceled: *std.atomic.Value(bool),
        err: ?anyerror = null,
        fn isCanceled(raw: ?*anyopaque) bool {
            const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return flag.load(.acquire);
        }
        fn call(self: *@This()) void {
            const control = Control{ .io = std.testing.io, .native_call_policy = .in_process, .cancellation = .{ .ptr = self.canceled, .is_cancelled_fn = isCanceled } };
            switch (self.placement) {
                .host => {
                    const value = run(self.broker, std.testing.allocator, std.testing.io, .transcribe, self.session, null, self.gate, self.inputs, control, null, 0) catch |err| {
                        self.err = err;
                        return;
                    };
                    destroy(std.testing.allocator, value);
                },
                .resident => {
                    var value = runResident(self.broker, std.testing.allocator, std.testing.io, .transcribe, self.session, null, self.gate, self.inputs, control, null, 0) catch |err| {
                        self.err = err;
                        return;
                    };
                    value.deinit();
                },
            }
        }
    };
    var calls = [_]Submit{ .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .placement = .host, .canceled = &canceled }, .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .placement = .resident, .canceled = &canceled } };
    lane.lockUncancelable(io);
    var locked = true;
    var group = std.Io.Group.init;
    defer {
        canceled.store(true, .release);
        if (locked) lane.unlock(io);
        group.cancel(io);
    }
    for (&calls) |*call| try group.concurrent(io, Submit.call, .{call});
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    var groups: usize = 0;
    var has_host = false;
    var has_resident = false;
    while (true) {
        var queued: usize = 0;
        groups = 0;
        has_host = false;
        has_resident = false;
        for (&broker.shards) |*shard| {
            shard.mutex.lockUncancelable(io);
            for (shard.groups.items) |cohort| {
                queued += cohort.tickets.items.len;
                groups += 1;
                has_host = has_host or std.mem.startsWith(u8, cohort.key.schema, "host;");
                has_resident = has_resident or std.mem.startsWith(u8, cohort.key.schema, "resident;");
            }
            shard.mutex.unlock(io);
        }
        if (queued == 2) break;
        if (platform.time.monotonicNs() >= deadline) return error.ResidentPlacementQueueTimeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // Cancel before opening the lane, so a regressed mixed typed key can fail
    // this assertion without writing one output representation into the other.
    canceled.store(true, .release);
    lane.unlock(io);
    locked = false;
    try group.await(io);
    try std.testing.expectEqual(@as(usize, 2), groups);
    try std.testing.expect(has_host and has_resident);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    for (calls) |call| {
        const err = call.err orelse return error.ExpectedPlacementCancellation;
        try std.testing.expect(err == error.Canceled or err == error.Cancelled);
    }
}

test "resident tensor microbatch actual Broker separates host and resident typed cohorts before submission" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkResidentBrokerPlacement);
}

fn checkResidentReusablePermit(cb: @import("../ops/ops.zig").ComputeBackend, _: @import("../ops/ops.zig").ComputeBackend) !void {
    const a = std.testing.allocator;
    const memory = @import("../runtime/tier/memory.zig");
    const Geometry = struct {
        fn plan(_: *anyopaque, _: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
            return .{ .sequence = 2, .output_bytes = batch * 2 * @sizeOf(f32) };
        }
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var broker = micro.Broker.init(a);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var probe = ResidentProbe{ .backend = cb, .gate = &gate, .controller = &controller, .fail_host = true };
    var table = ResidentProbe.vtable;
    table.runGeometry = Geometry.plan;
    const session = session_mod.Session{ .ptr = &probe, .vtable = &table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    var small = try Tensor.initFloat32(a, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer small.deinit();
    var large = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer large.deinit();
    var permit = try session.admit(try session.planRun(&.{large}, null));
    var permit_open = true;
    defer if (permit_open) permit.deinit();
    for ([_]Tensor{ small, large, small }) |input| {
        var output = try runResident(&broker, a, std.testing.io, .transcribe, session, &permit, &gate, &.{input}, null, null, 0);
        defer output.deinit();
        try std.testing.expect(permit.execution_yielded);
        try std.testing.expect(output.resource_lease != null);
        const values = try output.backend.toFloat32(output.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqual(input.asFloat32().len, values.len);
        for (input.asFloat32(), values) |before, after| try std.testing.expectEqual(before * 2, after);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), probe.host_calls);
    permit.deinit();
    permit_open = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "resident tensor microbatch reusable supplied permit reacquires execution through changing windows" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkResidentReusablePermit);
}

test "resident cohort view metadata keeps one actual shared Native provider scope" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const operation = @import("../execution/backend_operation.zig");
    const a = std.testing.allocator;
    const shared = try @import("../backends/compute_context.zig").SharedBackendContext.init(a, .native, null);
    defer shared.release();
    var cb = shared.computeBackend();
    const values = try a.alloc(@import("../ops/ops.zig").CT, 1);
    values[0] = cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 }) catch |err| {
        a.free(values);
        return err;
    };
    var outputs = session_mod.ResidentOutputs{ .outputs = values, .backend = &cb, .allocator = a };
    defer outputs.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var probe = ResidentProbe{ .backend = cb, .gate = &gate };
    const session = session_mod.Session{ .ptr = &probe, .vtable = &ResidentProbe.vtable };
    var blocked = try operation.enter(cb, null, null, .managed);
    defer blocked.deinitChecked() catch @panic("shared provider blocker cleanup");
    try std.testing.expectError(error.QueueFull, ResidentBatch.init(a, &outputs, session, 2));
    try blocked.finish();
    var batch = try ResidentBatch.init(a, &outputs, session, 2);
    var batch_open = true;
    defer if (batch_open) batch.deinit();
    try std.testing.expectError(error.QueueFull, operation.enter(cb, null, null, .managed));
    var first = try batch.rows(0, 1);
    defer first.deinit();
    var second = try batch.rows(1, 1);
    defer second.deinit();
    const first_values = try cb.toFloat32(first.outputs[0], a);
    defer a.free(first_values);
    const second_values = try cb.toFloat32(second.outputs[0], a);
    defer a.free(second_values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, first_values);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, second_values);
    batch.deinit();
    batch_open = false;
    var available = try operation.enter(cb, null, null, .managed);
    try available.finish();
}

test "resident serving cohort releases actual provider before publishing receiver slots" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const operation = @import("../execution/backend_operation.zig");
    const a = std.testing.allocator;
    const shared = try @import("../backends/compute_context.zig").SharedBackendContext.init(a, .native, null);
    defer shared.release();
    const cb = shared.computeBackend();
    var gate: std.atomic.Mutex = .unlocked;
    var probe = ResidentProbe{ .backend = cb, .gate = &gate, .fail_host = true };
    const session = session_mod.Session{ .ptr = &probe, .vtable = &ResidentProbe.vtable };
    const Publication = struct {
        probe: *ResidentProbe,
        checked: usize = 0,
        unavailable: bool = false,
        fn canceled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (self.probe.calls == 0) return false;
            // The existing model-lane publication boundary distinguishes the
            // receiver check from pre-forward/per-item construction checks.
            if (!self.probe.gate.tryLock()) return false;
            self.probe.gate.unlock();
            self.checked += 1;
            var receiver = operation.enter(self.probe.backend, null, null, .managed) catch {
                self.unavailable = true;
                return false;
            };
            receiver.finish() catch {
                self.unavailable = true;
            };
            return false;
        }
    };
    var publication = Publication{ .probe = &probe };
    var first = try Tensor.initFloat32(a, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer first.deinit();
    var second = try Tensor.initFloat32(a, "values", &.{ 1, 2 }, &.{ 3, 4 });
    defer second.deinit();
    var tickets = [_]Ticket{
        .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{first}, .control = .{ .native_call_policy = .in_process }, .rows = 1 },
        .{ .session = session, .permit = null, .gate = &gate, .inputs = &.{second}, .control = .{ .native_call_policy = .in_process }, .rows = 1 },
    };
    var outputs: [2]session_mod.ResidentOutputs = undefined;
    var slots = [_]micro.ResultSlot{ .{ .output = &outputs[0] }, .{ .output = &outputs[1] } };
    defer for (&slots, &outputs) |*slot, *output| {
        if (slot.completed and slot.err == null) output.deinit();
    };
    var items = [_]micro.ExecuteItem{
        .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[0], .slot = &slots[0], .control = .{ .cancellation = .{ .ptr = &publication, .is_cancelled_fn = Publication.canceled } } },
        .{ .allocator = std.heap.smp_allocator, .identity = .{}, .payload = &tickets[1], .slot = &slots[1], .control = .{ .cancellation = .{ .ptr = &publication, .is_cancelled_fn = Publication.canceled } } },
    };
    try executeGroupMode(.resident, &items);
    try std.testing.expect(publication.checked >= 2);
    try std.testing.expect(!publication.unavailable);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var receiver = try operation.enter(cb, null, null, .managed);
    defer receiver.deinitChecked() catch @panic("receiver cleanup failed");
    for (&slots, &outputs, 0..) |*slot, *output, index| {
        try std.testing.expect(slot.completed);
        if (slot.err) |err| return err;
        try std.testing.expectEqual(micro.Execution.native_batch, slot.execution);
        const actual = try cb.toFloat32(output.outputs[0], a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, if (index == 0) &.{ 2, 4 } else &.{ 6, 8 }, actual);
    }
    try receiver.finish();
}
