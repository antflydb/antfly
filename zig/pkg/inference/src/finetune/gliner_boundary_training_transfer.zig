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
pub const Readback = enum { proposal_logits, proposal_features, loss_logits, finite_control };
pub const Counts = struct {
    upload_bytes: usize = 0,
    proposal_logits: usize = 0,
    proposal_features: usize = 0,
    loss_logits: usize = 0,
    finite_control: usize = 0,

    pub fn readbackBytes(self: Counts) !usize {
        return add(try add(self.proposal_logits, self.proposal_features), try add(self.loss_logits, self.finite_control));
    }
};
pub const Admission = struct {
    upper: Counts = .{},
    /// Reusable host-to-device staging in addition to the retained inputs.
    largest_upload_bytes: usize = 0,
    /// Conservatively includes retained Session buffers and step upload
    /// staging; this is device allocation accounting, not process RSS.
    device_upper_bound_bytes: usize = 0,

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
        if (self.execution == .resident_metal and (next > self.admission.upper.upload_bytes or bytes > self.admission.largest_upload_bytes))
            return error.BoundaryTrainingTransferLimitExceeded;
        const tensor = if (self.execution == .resident_metal) switch (values) {
            .f32 => |data| try self.cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = data, .shape = dims } }, self.primitive),
            .i32 => |data| try self.cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = data, .shape = dims } }, self.primitive),
        } else switch (values) {
            .f32 => |data| try self.cb.fromFloat32Shape(data, dims),
            .i32 => |data| (try self.cb.fromInt32Shape(data, dims)) orelse return error.UnsupportedTrainingIntegerBackend,
        };
        errdefer self.cb.free(tensor);
        try self.check();
        if (self.execution == .resident_metal) {
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
