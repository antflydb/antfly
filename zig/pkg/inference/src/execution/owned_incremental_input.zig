// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Complete encoder/source lifetime across repeated incremental decode attempts.
//! This owner controls both final siblings; constructor presence alone never
//! qualifies a generic ResidentOutputs source for detached payload retirement.
const std = @import("std");
const stages = @import("encoder_decoder_runtime.zig");
const decoder = @import("decoder_stage.zig");
const Session = @import("../backends/session.zig").Session;
const Sink = @import("retirement_sink.zig");

pub const Cleanup = union(enum) {
    unqualified,
    /// Explicit caller contract: this task is not invoked while holding an
    /// enclosing provider/model lane. This guard starts before encoder work and
    /// ends only after every decoder plus both source roots have been retired.
    task: struct { ref: Sink.Ref, guard: Sink.Guard, deadline_ns: ?u64, background_supervised: bool },
    /// A real enclosing owner retains/seals this epoch after its actual unlock.
    /// The input owner cannot create, finish, or substitute the caller's epoch.
    outer: Sink.Epoch,

    /// The caller explicitly owns a lane-free task. Only the execution owner
    /// chooses whether its actual encoder/receiver pair needs a detached root.
    /// An ineligible pair keeps its original alias/borrowed-provider lifetime.
    pub fn beginForPairWithoutEnclosingLanes(encoder: Session, destination: Session, control: ?@import("../execution_control.zig").InferenceExecutionControl) !Cleanup {
        const backend = encoder.backend();
        if (backend != .cuda and !(backend == .metal and control != null)) return .unqualified;
        if (destination.backend() != backend or (encoder.ptr == destination.ptr and encoder.vtable == destination.vtable)) return .unqualified;
        const receiver = @import("architecture_session.zig").IncrementalDecoderAdapter.qualify(encoder, destination) orelse return .unqualified;
        if (receiver.same_model_owner) return .unqualified;
        const supervised_async = if (control) |active| active.native_call_policy == .require_supervision and active.deadline_ns != null else false;
        if (receiver.backend != .metal and !(receiver.backend == .cuda and supervised_async)) return .unqualified;
        return beginTaskWithoutEnclosingLanes(control);
    }
    pub fn beginTaskWithoutEnclosingLanes(control: ?@import("../execution_control.zig").InferenceExecutionControl) !Cleanup {
        var ref = try Sink.Ref.create();
        errdefer ref.release();
        const deadline = if (control) |active| if (active.deadline_ns) |end| end +| (active.cancellation_grace_ns orelse 0) else null else null;
        return .{ .task = .{ .ref = ref, .guard = try ref.enter(), .deadline_ns = deadline, .background_supervised = if (control) |active| active.native_call_policy == .require_supervision and deadline != null else false } };
    }
    pub fn borrowedEpoch(self: *const Cleanup) !?Sink.Epoch {
        return switch (self.*) {
            .unqualified => null,
            .task => |*value| try value.guard.retainEpoch(),
            .outer => |value| try value.retain(),
        };
    }
    pub fn finish(self: *Cleanup) void {
        switch (self.*) {
            .unqualified => {},
            .task => |*value| {
                value.guard.finish();
                value.ref.close() catch unreachable;
                value.ref.release();
            },
            .outer => |*epoch| epoch.release(),
        }
        self.* = .unqualified;
    }
};

/// The task owns both siblings and declares no enclosing model/provider lane.
/// Only a foreign Metal receiver benefits from releasing its original Engine;
/// same-model decode deliberately keeps its existing alias/borrowed-provider path.
fn promoteForeignMetalSource(encoder: *stages.EncoderOutputs, destination: Session, cleanup: *Cleanup) !void {
    if (cleanup.* != .task) return;
    const resident = switch (encoder.*) {
        .host => return,
        .resident => |*value| value,
    };
    const receiver = resident.incremental_receiver orelse return;
    if (receiver.destination_ptr != destination.ptr or receiver.destination_vtable != destination.vtable) return error.InvalidIncrementalReceiver;
    if (receiver.backend != .metal or receiver.same_model_owner) return;
    const values = &resident.values;
    if (values.resource_hold != null or values.backend.execution_control == null) return;
    const operation = values.backend.operationToken() orelse return;
    // Existing provider publication owns its exact B. A raw Engine may bind
    // only this task's explicit, pre-encoder epoch, never a newly invented B.
    var epoch = try operation.retainCleanupEpoch();
    defer if (epoch) |*value| value.release();
    if (epoch == null) {
        epoch = try cleanup.borrowedEpoch();
        if (epoch == null) return;
        if (!try values.backend.bindCleanupEpoch(&epoch.?)) return;
    }
    // Completion and original Engine/provider retirement occur here, before
    // retainIncrementalSource selects a CT/backend and before receiver admission.
    _ = try values.detachDenseF32PhysicalPayload(.{ .boundary = &epoch.?, .operation = operation });
}

pub const OwnedIncrementalInput = struct {
    encoder: stages.EncoderOutputs,
    source: ?decoder.RetainedEncoderSource,
    allocator: std.mem.Allocator,
    cleanup: Cleanup,

    /// Consumes encoder and cleanup only on success. The original holder stays
    /// valid on failed resource promotion, preserving construction error unwind.
    pub fn take(allocator: std.mem.Allocator, encoder: *stages.EncoderOutputs, destination: Session, cleanup: *Cleanup) !OwnedIncrementalInput {
        try promoteForeignMetalSource(encoder, destination, cleanup);
        var source = try decoder.retainIncrementalSource(allocator, encoder, destination);
        if (source) |*value| switch (cleanup.*) {
            .task => |task| {
                value.cleanup_root_qualified = true;
                value.cleanup_root_deadline_ns = task.deadline_ns;
                value.cleanup_root_background_supervised = task.background_supervised;
            },
            // External epochs still forward correctly at root cleanup; raw
            // external lanes do not opt into detached receiver promotion.
            .unqualified, .outer => {},
        };
        const result = OwnedIncrementalInput{ .encoder = encoder.*, .source = source, .allocator = allocator, .cleanup = cleanup.* };
        encoder.* = undefined;
        cleanup.* = .unqualified;
        return result;
    }

    pub fn deinit(self: *OwnedIncrementalInput) void {
        var epoch = self.cleanup.borrowedEpoch() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        // This actual root epoch covers BOTH siblings. Decoder source clones
        // must be gone before this destructor; per-attempt private clones use
        // separate decoder epochs and cannot retain this entire task's quota.
        switch (self.cleanup) {
            // The public raw destructor preserves its historical outside-
            // execution contract. This selects raw APIs explicitly, rather
            // than reinterpreting a missing boundary as current authority.
            .unqualified => {
                if (self.source) |*source| source.deinit();
                self.source = null;
                self.encoder.deinit(self.allocator);
            },
            .task, .outer => {
                const boundary = if (epoch) |*value| value else @import("antfly_platform").inference_process_supervisor.restartWorker();
                if (self.source) |*source| source.deinitWithBoundary(boundary);
                self.source = null;
                self.encoder.deinitWithBoundary(self.allocator, boundary);
            },
        }
        // Internal backend/model CloseScopes have ended before sealing. An
        // explicitly enclosing owner's epoch remains unsealed until its unlock.
        if (epoch) |*value| value.release();
        self.cleanup.finish();
        self.* = undefined;
    }
};
