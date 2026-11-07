// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A pending interval retains only its publication owner and value metadata.
//! Each phase borrows the existing pending resource's backend synchronously.
//! No submitting-thread registry, backend or compute pointer escapes a phase.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const session_mod = @import("../backends/session.zig");
const diagnostics = @import("diagnostics.zig");
const invocation = @import("invocation_observation.zig");
var next_identity: std.atomic.Value(u64) = .init(1);

pub const Source = struct {
    publisher: *diagnostics.Diagnostics,
    model: *session_mod.Session.ModelLifetime,
    model_allocator: ?std.mem.Allocator = null,
    request_id: ?u64 = null,
    pub fn fromSession(session: ?session_mod.Session, request_id: ?u64) ?Source {
        const owner = session orelse return null;
        const binding = owner.invocation_diagnostics orelse return null;
        const model = owner.model_lifetime orelse return null;
        return .{ .publisher = binding.diagnostics, .model = model, .model_allocator = binding.model_allocator, .request_id = request_id };
    }
};

pub const Span = struct {
    source: ?Source = null,
    identity: u64 = 0,
    sequence: u64 = 0,
    last_failure: ?diagnostics.Text = null,
    pub fn begin(source: ?Source) Span {
        const selected = source orelse return .{};
        if (!selected.publisher.isEnabled()) return .{};
        _ = selected.model.retain();
        return .{ .source = selected, .identity = next_identity.fetchAdd(1, .monotonic) };
    }
    pub fn deinit(self: *Span) void {
        const source = self.source orelse return;
        self.source = null;
        source.model.release();
    }
    pub inline fn call(self: *Span, backend: *const ops.ComputeBackend, phase: diagnostics.PendingInterval.Phase, completion: diagnostics.PendingInterval.Completion, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        return self.callOperation(backend, .decode_step, phase, completion, callback, args);
    }
    pub inline fn callOperation(self: *Span, backend: *const ops.ComputeBackend, operation: diagnostics.Operation, phase: diagnostics.PendingInterval.Phase, completion: diagnostics.PendingInterval.Completion, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        return self.callOperationClassified(backend, operation, phase, completion, classifyPhase, callback, args);
    }
    pub inline fn callOperationClassified(self: *Span, backend: *const ops.ComputeBackend, operation: diagnostics.Operation, phase: diagnostics.PendingInterval.Phase, completion: diagnostics.PendingInterval.Completion, comptime classify: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        const source = self.source orelse return @call(.auto, callback, args);
        if (!source.publisher.isEnabled()) return @call(.auto, callback, args);
        return self.observed(source, backend, operation, phase, completion, classify, callback, args);
    }
    noinline fn observed(self: *Span, source: Source, backend: *const ops.ComputeBackend, operation: diagnostics.Operation, phase: diagnostics.PendingInterval.Phase, completion: diagnostics.PendingInterval.Completion, comptime classify: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        self.sequence +|= 1;
        var interval = diagnostics.PendingInterval{ .identity = self.identity, .request_id = source.request_id, .sequence = self.sequence, .phase = phase, .prior_failure = self.last_failure };
        const binding = invocation.Binding{ .diagnostics = source.publisher, .operation = operation, .capture_programs = true, .model_allocator = source.model_allocator, .stable_backend = backend.*, .context_runtime_scope = .borrowed_execution_backend_context, .pending_interval = &interval };
        return invocation.callClassified(binding, classify, phaseCall, .{ &interval, &self.last_failure, completion, callback, args });
    }
};
fn phaseCall(interval: *diagnostics.PendingInterval, last_failure: *?diagnostics.Text, completion: diagnostics.PendingInterval.Completion, comptime callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const result = @call(.auto, callback, args) catch |err| {
        last_failure.* = diagnostics.Text.init(@errorName(err));
        interval.prior_failure = last_failure.*;
        return err;
    };
    const available = if (comptime @typeInfo(@TypeOf(result)) == .optional) result != null else if (comptime @TypeOf(result) == bool) result else true;
    if (available) interval.completion = completion;
    return result;
}

/// Failure before acquisition has no authority to sample the real backend.
pub fn acquisitionFailure(source: ?Source, failure: anyerror) void {
    const selected = source orelse return;
    if (!selected.publisher.isEnabled()) return;
    selected.publisher.publish(.{
        .scope = .session_numerical_dispatch,
        .operation = .decode_step,
        .outcome = .failed,
        .error_name = diagnostics.Text.init(@errorName(failure)),
        .pending_interval = .{ .identity = next_identity.fetchAdd(1, .monotonic), .request_id = selected.request_id, .sequence = 1, .phase = .acquire, .prior_failure = diagnostics.Text.init(@errorName(failure)) },
    });
}

fn classifyPhase(value: anytype) invocation.ResultClassification {
    const available = if (comptime @typeInfo(@TypeOf(value)) == .optional) value != null else if (comptime @TypeOf(value) == bool) value else true;
    return .{ .unsupported = !available };
}
