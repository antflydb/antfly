// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned, non-donated decoder buffers. A step publishes all state only
//! after synchronization, output validation and successful logits allocation.
const std = @import("std");
const pjrt = @import("pjrt").pjrt;
const Executor = @import("pjrt_executor.zig").PjrtExecutor;
const ops = @import("../ops/ops.zig");

pub const State = struct {
    allocator: std.mem.Allocator,
    buffers: []pjrt.Buffer = &.{},
    position: usize = 0,
    capacity: usize,
    admitted_bytes: usize = std.math.maxInt(usize),
    client: ?*pjrt.Client = null,

    pub fn deinit(self: *State) void {
        self.reset();
        self.* = undefined;
    }
    pub fn reset(self: *State) void {
        for (self.buffers) |*buffer| buffer.deinit();
        self.allocator.free(self.buffers);
        self.buffers = &.{};
        self.position = 0;
        self.client = null;
    }
    fn initialize(self: *State, executor: *const Executor) !void {
        if (self.buffers.len != 0) return;
        const start = executor.resident_output_start orelse return error.NotResidentDecoder;
        var bytes: usize = 0;
        for (executor.output_shapes[start..]) |shape| {
            var size: usize = @sizeOf(f32);
            for (shape) |dim| size = try std.math.mul(usize, size, std.math.cast(usize, dim) orelse return error.InvalidResidentState);
            bytes = try std.math.add(usize, bytes, size);
        }
        if (bytes > self.admitted_bytes) return error.UnadmittedResidentState;
        const buffers = try self.allocator.alloc(pjrt.Buffer, executor.output_shapes.len - start + 1);
        var initialized: usize = 0;
        errdefer {
            for (buffers[0..initialized]) |*buffer| buffer.deinit();
            self.allocator.free(buffers);
        }
        buffers[0] = try executor.client.bufferFromHostFloat32(&.{0}, &.{});
        initialized += 1;
        for (executor.output_shapes[start..], buffers[1..]) |shape, *buffer| {
            var elements: usize = 1;
            for (shape) |dim| elements = try std.math.mul(usize, elements, std.math.cast(usize, dim) orelse return error.InvalidResidentState);
            const zeros = try self.allocator.alloc(f32, elements);
            defer self.allocator.free(zeros);
            @memset(zeros, 0);
            buffer.* = try executor.client.bufferFromHostFloat32(zeros, shape);
            initialized += 1;
        }
        self.buffers = buffers;
        self.client = executor.client;
    }

    pub fn run(self: *State, executor: *Executor, backend: *const ops.ComputeBackend, values: []?ops.CT, rows: usize, valid_rows: usize, a: std.mem.Allocator, control: ?@import("../execution_control.zig").InferenceExecutionControl) ![]f32 {
        if (rows == 0 or rows != executor.resident_query_rows or executor.resident_capacity != self.capacity or self.position > self.capacity or rows > self.capacity - self.position) return error.InvalidResidentDecodePosition;
        if (valid_rows == 0 or valid_rows > rows or (valid_rows != rows and !executor.resident_dynamic_rows)) return error.InvalidResidentDecodePosition;
        if (executor.resident_prefill_only and self.position != 0) return error.InvalidResidentDecodePosition;
        if (self.client) |client| if (client != executor.client) return error.ForeignCompiledProvider;
        const start = executor.resident_output_start orelse return error.NotResidentDecoder;
        if (start != 1) return error.InvalidResidentDecoderOutput;
        if (control) |active| try active.check();
        try self.initialize(executor);
        if (self.buffers.len != executor.output_shapes.len - start + 1) return error.InvalidResidentState;
        const position: f32 = @floatFromInt(self.position);
        var cursor = try executor.client.bufferFromHostFloat32(&.{position}, &.{});
        defer cursor.deinit();
        // The scalar is also transactional. Never replace the last committed
        // position until execution and readback have succeeded.
        const old_cursor = self.buffers[0];
        self.buffers[0] = cursor;
        defer self.buffers[0] = old_cursor;
        var outputs = try executor.executeWithResidentState(values, backend, self.buffers, a);
        defer {
            for (outputs) |*buffer| buffer.deinit();
            a.free(outputs);
        }
        if (outputs.len != executor.output_shapes.len) return error.InvalidResidentState;
        for (outputs, executor.output_shapes) |*buffer, shape| if (!try buffer.matchesFloat32Shape(shape)) return error.InvalidResidentState;
        const logits = try outputs[0].toFloat32WithShape(a, executor.output_shapes[0]);
        errdefer a.free(logits);
        if (control) |active| try active.check();
        // All fallible work is complete. Swap whole bank generations; the old
        // bank retires with outputs and cannot be observed by another request.
        for (self.buffers[1..], outputs[start..]) |*destination, *source| std.mem.swap(pjrt.Buffer, destination, source);
        self.position += valid_rows;
        return logits;
    }
};
