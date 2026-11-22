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

//! Server-independent synchronous tensor-stage execution. The server broker
//! supplies only optional scheduling callbacks; admission, source lanes and
//! output custody belong to this execution owner for both serial and batch paths.
const std = @import("std");
const platform = @import("antfly_platform");
const session_mod = @import("../backends/session.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Task = enum {
    read,
    generate,
    embed,
    rerank,
    chunk,
    extract,
    rewrite,
    transcribe,
};

pub const Dispatch = struct {
    ptr: *anyopaque,
    task: Task,
    run_stage_fn: ?*const fn (*anyopaque, Task, std.mem.Allocator, session_mod.Session, []const session_mod.StageInput, ?Control) anyerror!session_mod.ResidentOutputs = null,
    run_resident_fn: ?*const fn (*anyopaque, Task, std.mem.Allocator, session_mod.Session, ?*session_mod.RunPermit, ?*std.atomic.Mutex, []const Tensor, ?Control) anyerror!?session_mod.ResidentOutputs = null,
    run_fn: *const fn (*anyopaque, Task, std.mem.Allocator, session_mod.Session, ?*session_mod.RunPermit, ?*std.atomic.Mutex, []const Tensor, ?Control) anyerror![]Tensor,

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

pub fn validTensor(tensor: Tensor) bool {
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

/// One stage entry for task-installed scheduling and the serial Source owner.
/// A custom dispatcher is authoritative; never replay it on a different route.
pub fn hasDispatcher(dispatch: anytype) bool {
    if (comptime @TypeOf(dispatch) == @TypeOf(null)) return false;
    if (comptime @typeInfo(@TypeOf(dispatch)) == .optional) return dispatch != null;
    return true;
}

pub fn stageDispatcher(dispatch: anytype) ?Dispatch {
    if (comptime @TypeOf(dispatch) == @TypeOf(null)) return null;
    const selected = if (comptime @typeInfo(@TypeOf(dispatch)) == .optional) dispatch orelse return null else dispatch;
    if (comptime @TypeOf(selected) != Dispatch) return null;
    return if (selected.run_stage_fn != null) selected else null;
}

pub fn run(allocator: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control, dispatch: anytype) ![]Tensor {
    if (comptime @TypeOf(dispatch) != @TypeOf(null)) {
        if (comptime @typeInfo(@TypeOf(dispatch)) == .optional) {
            if (dispatch) |selected| return selected.run(allocator, session, permit, gate, inputs, control);
        } else return dispatch.run(allocator, session, permit, gate, inputs, control);
    }
    if (permit) |owned| {
        if (owned.session.ptr != session.ptr or owned.session.vtable != session.vtable) return error.InvalidStagePermit;
        // A prepaid legacy stage may have an explicit task-supplied guard.
        // Do not infer another guard from Session: prepared permits already
        // retain their own source/cohort contract. Dispatchers own their gate.
        if (gate) |mutex| {
            if (control) |active| try active.lock(mutex) else platform.sync.lockYielding(mutex);
        }
        defer if (gate) |mutex| mutex.unlock();
        return owned.runWithControl(inputs, allocator, control);
    }
    return runHost(allocator, session, gate orelse session.execution_gate, inputs, control);
}

pub fn runSerial(allocator: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, inputs: []const Tensor, control: ?Control) ![]Tensor {
    return run(allocator, session, permit, null, inputs, control, null);
}

/// Fresh host dispatch shares the same Source owner as the task pipelines.
/// Borrow only the current CPU source scratch; KV and foreign resource owners
/// execute independently and transfer their final container after unlock.
pub fn runHost(allocator: std.mem.Allocator, session: session_mod.Session, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control) ![]Tensor {
    var request = try session.planRun(inputs, null);
    const workspace = @import("../backends/admitted_allocator.zig").AdmittedAllocator.fromAllocator(allocator);
    var borrowed: ?*@import("../backends/admitted_allocator.zig").AdmittedAllocator = null;
    const previous = if (workspace) |owner| owner.session else null;
    const previous_context = if (workspace) |owner| owner.admission_context else null;
    defer if (borrowed) |owner| {
        owner.session = previous;
        owner.admission_context = previous_context;
    };
    if (workspace) |owner| if (session.backend() == .native and !session.preparesInvocation() and try owner.canBorrowRun(session, request)) {
        const bound = owner.bindSession(session.withAdmissionControl(control));
        if (bound) |_| borrowed = owner else |err| switch (err) {
            error.ForeignAdmissionResourceOwner, error.InvalidAdmissionLeaseReduction => {},
        }
    };
    if (borrowed != null) {
        const admission = session.run_admission.?;
        const original = try admission.planRequest(request, session.outputInfo());
        // The original opaque work floor stays intact; this fresh Source also
        // pays for its own allocator RunScope custody node before numerical entry.
        const Owner = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
        // The run's ordinary IO estimate covers data payloads, but not the
        // actual Tensor container, owned shape arrays and allocation headers.
        // Declare those producers before entering the numerical lane too.
        const infos = session.outputInfo();
        var metadata = try Owner.allocationHostBytes(try std.math.mul(usize, infos.len, @sizeOf(Tensor)), .of(Tensor));
        for (infos) |info| {
            metadata = try std.math.add(usize, metadata, try Owner.allocationHostBytes(try std.math.mul(usize, info.shape.len, @sizeOf(i64)), .of(i64)));
            metadata = try std.math.add(usize, metadata, Owner.allocationOverhead(.of(u64)));
        }
        try Owner.declareRunHostAllocations(session, &request, try std.math.add(usize, original.protected.host_scratch_bytes, metadata), true);
    }
    if (workspace != null and borrowed == null) try workspace.?.trim();
    const work = if (workspace != null and borrowed == null) platform.allocator.concurrentFallback() else allocator;
    var source: session_mod.PreparedSource = .{};
    defer source.deinit();
    try source.initShapesBeforeExecutionInto(session, session, gate, .{ .tensors = inputs }, request, borrowed, .host, work, control);
    try source.bindInputsBeforeExecution(.{ .host = inputs }, .host);
    try source.enter();
    const outputs = try source.runHost(inputs);
    if (workspace != null and borrowed == null) {
        errdefer {
            for (outputs) |*output| output.deinit();
            work.free(outputs);
        }
        const result = try allocator.dupe(Tensor, outputs);
        // A refused optional shrink retains its old reservation; outputs
        // have succeeded and must not be replayed or lose their custody.
        workspace.?.trim() catch {};
        work.free(outputs);
        return result;
    }
    return outputs;
}

test "tensor stage dispatch preserves explicit scheduling and never replays a provider failure" {
    const a = std.testing.allocator;
    const Probe = struct {
        serial_calls: usize = 0,
        dispatch_calls: usize = 0,
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        fn serial(raw: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.serial_calls += 1;
            return error.UnexpectedSerialReplay;
        }
        const table = session_mod.Session.VTable{ .run = serial, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    const Dispatcher = struct {
        probe: *Probe,
        permit: *session_mod.RunPermit,
        gate: *std.atomic.Mutex,
        fn run(self: *@This(), _: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control) ![]Tensor {
            try std.testing.expect(session.ptr == @as(*anyopaque, @ptrCast(self.probe)) and permit == self.permit and gate == self.gate);
            try std.testing.expect(inputs.len == 0 and control != null);
            try std.testing.expect(self.gate.tryLock());
            self.gate.unlock();
            self.probe.dispatch_calls += 1;
            return error.InjectedStageFailure;
        }
    };
    var probe = Probe{};
    const session = session_mod.Session{ .ptr = &probe, .vtable = &Probe.table };
    var permit = session_mod.RunPermit{ .session = session, .lease = null };
    defer permit.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var dispatcher = Dispatcher{ .probe = &probe, .permit = &permit, .gate = &gate };
    try std.testing.expectError(error.InjectedStageFailure, run(a, session, &permit, &gate, &.{}, .{}, &dispatcher));
    try std.testing.expectError(error.InjectedStageFailure, run(a, session, &permit, &gate, &.{}, .{}, @as(?*Dispatcher, &dispatcher)));
    try std.testing.expectEqual(@as(usize, 2), probe.dispatch_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.serial_calls);
    var foreign = Probe{};
    const other = session_mod.Session{ .ptr = &foreign, .vtable = &Probe.table };
    try std.testing.expectError(error.InvalidStagePermit, runSerial(a, other, &permit, &.{}, null));
    try std.testing.expectEqual(@as(usize, 0), foreign.serial_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.serial_calls);
}

test "prepaid tensor stages own explicit guards and release them on failure and cancellation" {
    const Probe = struct {
        gate: std.atomic.Mutex = .unlocked,
        calls: usize = 0,
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        fn cancelled(_: ?*anyopaque) bool {
            return true;
        }
        fn execute(raw: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.gate.tryLock()) {
                self.gate.unlock();
                return error.MissingStageGuard;
            }
            self.calls += 1;
            return error.InjectedStageFailure;
        }
        const table = session_mod.Session.VTable{ .run = execute, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    var probe = Probe{};
    const session = session_mod.Session{ .ptr = &probe, .vtable = &Probe.table };
    var permit = session_mod.RunPermit{ .session = session, .lease = null };
    defer permit.deinit();
    try std.testing.expectError(error.InjectedStageFailure, run(std.testing.allocator, session, &permit, &probe.gate, &.{}, null, null));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(probe.gate.tryLock());
    defer probe.gate.unlock();
    // Cancellation while waiting must neither enter the backend nor unlock
    // another owner's guard. The owning caller remains responsible for it.
    const control = Control{ .cancellation = .{ .is_cancelled_fn = Probe.cancelled } };
    try std.testing.expectError(error.Cancelled, run(std.testing.allocator, session, &permit, &probe.gate, &.{}, control, null));
    try std.testing.expect(!probe.gate.tryLock());
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
