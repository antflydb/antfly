// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit host/device cuts for the mixed-task training step. Parameter
//! uploads belong to the enclosing model owner; this module counts the step's
//! typed inputs, detached proposal views, loss logits and finite summaries.
//! The Step never passes encoder hidden states or parameter gradients to this
//! transfer interface.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const seeded = @import("../graph/seeded_training.zig");
const resident = @import("../ops/resident_training_ops.zig");
const Values = @import("gliner_boundary_encoder_graph.zig").Values;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Limits = struct {
    max_upload_bytes: usize = 256 * 1024 * 1024,
    max_readback_bytes: usize = 128 * 1024 * 1024,
};
pub const Readback = enum { proposal_logits, proposal_features, loss_logits, loss_gradients, finite_control };
pub const Counts = struct {
    upload_bytes: usize = 0,
    proposal_logits: usize = 0,
    proposal_features: usize = 0,
    loss_logits: usize = 0,
    loss_gradients: usize = 0,
    finite_control: usize = 0,

    pub fn readbackBytes(self: Counts) !usize {
        return add(try add(self.proposal_logits, self.proposal_features), try add(try add(self.loss_logits, self.loss_gradients), self.finite_control));
    }
};
pub const Admission = struct {
    upper: Counts = .{},
    /// Reusable host-to-device staging in addition to the retained inputs.
    largest_upload_bytes: usize = 0,
    /// Conservatively includes retained Session buffers and step upload
    /// staging; this is device allocation accounting, not process RSS.
    device_upper_bound_bytes: usize = 0,
    loss_device_bytes: usize = 0,

    pub fn upload(self: *Admission, shape: ml.Shape) !void {
        const bytes = try shapeBytes(shape);
        self.upper.upload_bytes = try add(self.upper.upload_bytes, bytes);
        self.largest_upload_bytes = @max(self.largest_upload_bytes, bytes);
    }
    pub fn readback(self: *Admission, kind: Readback, bytes: usize) !void {
        switch (kind) {
            inline else => |tag| @field(self.upper, @tagName(tag)) = try add(@field(self.upper, @tagName(tag)), bytes),
        }
    }
    pub fn validate(self: Admission, limits: Limits) !void {
        if (limits.max_upload_bytes == 0 or limits.max_readback_bytes == 0 or
            self.upper.upload_bytes > limits.max_upload_bytes or
            try self.upper.readbackBytes() > limits.max_readback_bytes)
            return error.BoundaryTrainingTransferLimitExceeded;
    }
};
pub const Diagnostics = struct {
    bytes: Counts = .{},
    upload_calls: usize = 0,
    readback_calls: usize = 0,
};

pub fn shapeBytes(shape: ml.Shape) !usize {
    if (shape.dtype != .f32 and shape.dtype != .i32) return error.InvalidBoundaryTrainingTransfer;
    const bytes = try seeded.shapeBytes(shape);
    if (bytes == 0) return error.InvalidBoundaryTrainingTransfer;
    return bytes;
}
fn add(left: usize, right: usize) !usize {
    return std.math.add(usize, left, right) catch error.BoundaryTrainingTransferLimitExceeded;
}

pub const IO = struct {
    allocator: Allocator,
    cb: *const ops.ComputeBackend,
    execution: seeded.Execution,
    primitive: resident.Limits,
    admission: Admission,
    control: ?Control,
    diagnostics: Diagnostics = .{},

    fn check(self: *const IO) !void {
        if (self.control) |control| try control.check();
    }
    fn lossDiagnostics(self: *const IO, uploads: usize, readback: usize, largest: usize, calls: usize) !Diagnostics {
        try self.check();
        var next = self.diagnostics;
        next.bytes.upload_bytes = try add(next.bytes.upload_bytes, uploads);
        next.bytes.loss_gradients = try add(next.bytes.loss_gradients, readback);
        if (next.bytes.upload_bytes > self.admission.upper.upload_bytes or
            next.bytes.loss_gradients > self.admission.upper.loss_gradients or largest > self.admission.largest_upload_bytes)
            return error.BoundaryTrainingTransferLimitExceeded;
        next.upload_calls = try add(next.upload_calls, calls);
        next.readback_calls = try add(next.readback_calls, @intFromBool(readback != 0));
        return next;
    }

    pub fn elementwiseLossGradient(raw: *anyopaque, request: *const ops.elementwise_loss_math.Request) anyerror!void {
        const self: *IO = @ptrCast(@alignCast(raw));
        try request.validate();
        const bytes = try std.math.mul(usize, request.logits.len, 4);
        const next = try self.lossDiagnostics(try std.math.mul(usize, bytes, 3), bytes, bytes, if (bytes == 0) 0 else 3);
        const apply = self.cb.vtable.elementwiseLossGradient orelse return error.UnsupportedElementwiseLossMath;
        try apply(self.cb.ptr, request);
        self.diagnostics = next;
        try self.check();
    }

    pub fn consistencyLossGradient(raw: *anyopaque, request: *const ops.consistency_loss_math.Request) anyerror!void {
        const self: *IO = @ptrCast(@alignCast(raw));
        try request.validate();
        const plan = try request.plan();
        var next = try self.lossDiagnostics(plan.upload_bytes, plan.readback_bytes, plan.largest_upload_bytes, 14);
        next.readback_calls = try add(next.readback_calls, 2);
        const apply = self.cb.vtable.consistencyLossGradient orelse return error.UnsupportedConsistencyLossMath;
        try apply(self.cb.ptr, request);
        self.diagnostics = next;
        try self.check();
    }

    pub fn listwiseLossGradient(raw: *anyopaque, request: *const ops.listwise_loss_math.Request) anyerror!void {
        const self: *IO = @ptrCast(@alignCast(raw));
        try request.validate();
        const empty = request.logits.len == 0;
        const input_count = try add(try std.math.mul(usize, request.logits.len, 2), try add(request.maxima.len, request.seeds.len));
        const uploads = if (empty) 0 else try std.math.mul(usize, input_count, 4);
        const readback = if (empty) 0 else try std.math.mul(usize, request.gradient.len, 4);
        const largest = if (empty) 0 else try std.math.mul(usize, @max(request.logits.len, request.maxima.len), 4);
        const next = try self.lossDiagnostics(uploads, readback, largest, if (empty) 0 else 4);
        const apply = self.cb.vtable.listwiseLossGradient orelse return error.UnsupportedListwiseLossMath;
        try apply(self.cb.ptr, request);
        self.diagnostics = next;
        try self.check();
    }

    pub fn recordLossGradient(raw: *anyopaque, request: *const ops.record_loss_math.Request) anyerror!void {
        const self: *IO = @ptrCast(@alignCast(raw));
        try request.validate();
        const count = try add(request.logits.len, request.seeds.len);
        const uploads = try std.math.mul(usize, count, 8);
        const readback_bytes = try std.math.mul(usize, count, 4);
        const largest = try std.math.mul(usize, request.logits.len, 4);
        var next = try self.lossDiagnostics(uploads, readback_bytes, largest, if (count == 0) 0 else 4);
        if (readback_bytes != 0) next.readback_calls = try add(next.readback_calls, 1);
        const apply = self.cb.vtable.recordLossGradient orelse return error.UnsupportedRecordLossMath;
        try apply(self.cb.ptr, request);
        self.diagnostics = next;
        try self.check();
    }

    pub fn upload(self: *IO, shape: ml.Shape, values: Values) !ops.CT {
        try self.check();
        var dimensions: [8]i32 = undefined;
        if (shape.rank_ > dimensions.len) return error.InvalidBoundaryTrainingTransfer;
        for (shape.dims[0..shape.rank_], dimensions[0..shape.rank_]) |dim, *out| out.* = std.math.cast(i32, dim) orelse return error.InvalidBoundaryTrainingTransfer;
        const dims = dimensions[0..shape.rank_];
        const bytes = try shapeBytes(shape);
        switch (values) {
            .f32 => |data| if (shape.dtype != .f32 or data.len != bytes / 4) return error.InvalidBoundaryTrainingTransfer,
            .i32 => |data| if (shape.dtype != .i32 or data.len != bytes / 4) return error.InvalidBoundaryTrainingTransfer,
        }
        const next = try add(self.diagnostics.bytes.upload_bytes, bytes);
        if (self.execution != .native and (next > self.admission.upper.upload_bytes or bytes > self.admission.largest_upload_bytes))
            return error.BoundaryTrainingTransferLimitExceeded;
        const tensor = if (self.execution != .native) switch (values) {
            .f32 => |data| try self.cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = data, .shape = dims } }, self.primitive),
            .i32 => |data| try self.cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = data, .shape = dims } }, self.primitive),
        } else switch (values) {
            .f32 => |data| try self.cb.fromFloat32Shape(data, dims),
            .i32 => |data| (try self.cb.fromInt32Shape(data, dims)) orelse return error.UnsupportedTrainingIntegerBackend,
        };
        errdefer self.cb.free(tensor);
        try self.check();
        if (self.execution != .native) {
            self.diagnostics.bytes.upload_bytes = next;
            self.diagnostics.upload_calls += 1;
        }
        return tensor;
    }
    pub fn download(self: *IO, tensor: ops.CT, shape: ml.Shape, kind: Readback) ![]f32 {
        if (shape.dtype != .f32 or kind == .finite_control) return error.InvalidBoundaryTrainingTransfer;
        try self.check();
        if (self.execution == .native) return self.cb.toFloat32(tensor, self.allocator);
        const bytes = try shapeBytes(shape);
        var next = self.diagnostics.bytes;
        switch (kind) {
            inline else => |tag| {
                const value = try add(@field(next, @tagName(tag)), bytes);
                if (value > @field(self.admission.upper, @tagName(tag))) return error.BoundaryTrainingTransferLimitExceeded;
                @field(next, @tagName(tag)) = value;
            },
        }
        const output = try self.allocator.alloc(f32, bytes / 4);
        errdefer self.allocator.free(output);
        try self.cb.glinerBoundaryDownload(tensor, output);
        try self.check();
        self.diagnostics.bytes = next;
        self.diagnostics.readback_calls += 1;
        return output;
    }
    pub fn recordControl(self: *IO, bytes: usize) !void {
        if (bytes > self.admission.upper.finite_control) return error.BoundaryTrainingTransferLimitExceeded;
        self.diagnostics.bytes.finite_control = bytes;
    }
};

test "boundary training transfer admission separates bounded head cuts and rejects overflow" {
    var plan = Admission{};
    try plan.upload(ml.Shape.init(.i32, &.{ 2, 7 }));
    try plan.upload(ml.Shape.init(.f32, &.{ 2, 3 }));
    try plan.readback(.proposal_logits, 72);
    try plan.readback(.proposal_features, 112);
    try plan.readback(.loss_logits, 24);
    try plan.readback(.finite_control, 12);
    try std.testing.expectEqual(@as(usize, 80), plan.upper.upload_bytes);
    try std.testing.expectEqual(@as(usize, 56), plan.largest_upload_bytes);
    try std.testing.expectEqual(@as(usize, 220), try plan.upper.readbackBytes());
    try plan.validate(.{ .max_upload_bytes = 80, .max_readback_bytes = 220 });
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, plan.validate(.{ .max_upload_bytes = 79 }));
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, plan.validate(.{ .max_readback_bytes = 219 }));
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, plan.readback(.loss_logits, std.math.maxInt(usize)));
    try std.testing.expectError(error.InvalidBoundaryTrainingTransfer, plan.upload(ml.Shape.init(.i64, &.{2})));
}

test "loss callbacks enforce transfer admission before backend dispatch" {
    const Probe = struct {
        calls: usize = 0,
        fn binary(raw: *anyopaque, request: *const ops.elementwise_loss_math.Request) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            @memset(request.cotangents, 0);
        }
        fn listwise(raw: *anyopaque, request: *const ops.listwise_loss_math.Request) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            @memset(request.gradient, 0);
        }
        fn record(raw: *anyopaque, request: *const ops.record_loss_math.Request) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            @memset(request.gradient, 0);
            @memset(request.losses, 0);
        }
    };
    var probe = Probe{};
    // Only these three callbacks are reachable through the adapter under test.
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.elementwiseLossGradient = Probe.binary;
    vtable.listwiseLossGradient = Probe.listwise;
    vtable.recordLossGradient = Probe.record;
    const cb = ops.ComputeBackend{ .ptr = &probe, .vtable = &vtable };
    var io = IO{ .allocator = std.testing.allocator, .cb = &cb, .execution = .resident_cuda, .primitive = .{}, .admission = .{ .upper = .{ .upload_bytes = 52, .loss_gradients = 16 }, .largest_upload_bytes = 8 }, .control = null };
    var grad = [_]f32{ 1, 1 };
    const binary = ops.elementwise_loss_math.Request{ .logits = &.{ 0, 0 }, .targets = &.{ 1, 0 }, .cotangents = &grad, .settings = .{}, .max_elements = 2 };
    io.admission.upper.loss_gradients = 7;
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, IO.elementwiseLossGradient(&io, &binary));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    io.admission.upper.loss_gradients = 16;
    try IO.elementwiseLossGradient(&io, &binary);
    const listwise = ops.listwise_loss_math.Request{ .logits = &.{ 0, 0 }, .masks = &.{ 3, 1 }, .maxima = &.{ 0, 0 }, .seeds = &.{1}, .gradient = &grad, .batch = 1, .queries = 1, .candidates = 2, .max_elements = 2 };
    try IO.listwiseLossGradient(&io, &listwise);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expectEqual(@as(usize, 52), io.diagnostics.bytes.upload_bytes);
    try std.testing.expectEqual(@as(usize, 16), io.diagnostics.bytes.loss_gradients);
    try std.testing.expectEqual(@as(usize, 7), io.diagnostics.upload_calls);
    try std.testing.expectEqual(@as(usize, 2), io.diagnostics.readback_calls);
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, IO.elementwiseLossGradient(&io, &binary));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    var scalar: [1]f32 = undefined;
    const record = ops.record_loss_math.Request{ .logits = &.{ 0, 0 }, .masks = &.{ 3, 1 }, .target_columns = &.{0}, .seeds = &.{1}, .gradient = &grad, .losses = &scalar, .width = 2, .max_elements = 2 };
    io.admission.upper.upload_bytes = 76;
    io.admission.upper.loss_gradients = 27;
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, IO.recordLossGradient(&io, &record));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    io.admission.upper.loss_gradients = 28;
    try IO.recordLossGradient(&io, &record);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 76), io.diagnostics.bytes.upload_bytes);
    try std.testing.expectEqual(@as(usize, 28), io.diagnostics.bytes.loss_gradients);
    try std.testing.expectEqual(@as(usize, 11), io.diagnostics.upload_calls);
    try std.testing.expectEqual(@as(usize, 4), io.diagnostics.readback_calls);
}

test "boundary training consistency transfer rejects overrun before dispatch" {
    const Probe = struct {
        calls: usize = 0,
        fn apply(raw: *anyopaque, _: *const ops.consistency_loss_math.Request) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    const a = std.testing.allocator;
    var groups = try ops.consistency_loss_math.grouping.build(a, &.{ 0, 1 }, 2, .{}, null);
    defer groups.deinit();
    var grad: [2]f32 = undefined;
    const request = ops.consistency_loss_math.Request{ .pairs = &.{ 0, 0 }, .margins = .{ &.{ 0, 0 }, &.{ 0, 0 } }, .valid = &.{ 1, 1 }, .indices = .{ &.{ 0, 1 }, &.{ 0, 1 } }, .keep = .{ &.{ 1, 1 }, &.{ 1, 1 } }, .groups = .{ groups, groups }, .counts = .{ 2, 2 }, .weight = 0.1, .gradients = .{ &grad, &grad, &grad }, .max_elements = 2 };
    const plan = try request.plan();
    var probe = Probe{};
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.consistencyLossGradient = Probe.apply;
    const cb = ops.ComputeBackend{ .ptr = &probe, .vtable = &vtable };
    var io = IO{ .allocator = a, .cb = &cb, .execution = .resident_cuda, .primitive = .{}, .admission = .{ .upper = .{ .upload_bytes = plan.upload_bytes, .loss_gradients = plan.readback_bytes - 1 }, .largest_upload_bytes = plan.largest_upload_bytes }, .control = null };
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, IO.consistencyLossGradient(&io, &request));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    io.admission.upper.loss_gradients = plan.readback_bytes;
    try IO.consistencyLossGradient(&io, &request);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(plan.upload_bytes, io.diagnostics.bytes.upload_bytes);
    try std.testing.expectEqual(plan.readback_bytes, io.diagnostics.bytes.loss_gradients);
    try std.testing.expectEqual(@as(usize, 14), io.diagnostics.upload_calls);
    try std.testing.expectEqual(@as(usize, 3), io.diagnostics.readback_calls);
    try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, IO.consistencyLossGradient(&io, &request));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
