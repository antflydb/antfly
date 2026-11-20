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

//! Request-owned, non-donated decoder buffers. Prefill produces the first bank;
//! decode replaces it only after synchronization, validation and logits readback.
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

    pub fn run(self: *State, executor: *Executor, backend: *const ops.ComputeBackend, values: []?ops.CT, rows: usize, valid_rows: usize, a: std.mem.Allocator, control: ?@import("../execution_control.zig").InferenceExecutionControl) ![]f32 {
        if (rows == 0 or rows != executor.resident_query_rows or executor.resident_capacity != self.capacity or self.position > self.capacity or rows > self.capacity - self.position) return error.InvalidResidentDecodePosition;
        if (valid_rows == 0 or valid_rows > rows or (valid_rows != rows and !executor.resident_dynamic_rows)) return error.InvalidResidentDecodePosition;
        if (executor.resident_prefill_only and self.position != 0) return error.InvalidResidentDecodePosition;
        if (executor.resident_initial != (self.buffers.len == 0)) return error.InvalidResidentState;
        if (self.client) |client| if (client != executor.client) return error.ForeignCompiledProvider;
        const start = executor.resident_output_start orelse return error.NotResidentDecoder;
        if (start != 1) return error.InvalidResidentDecoderOutput;
        if (control) |active| try active.check();
        const banks = executor.output_shapes.len - start;
        var first_headers: ?[]pjrt.Buffer = null;
        defer if (first_headers) |headers| self.allocator.free(headers);
        if (executor.resident_initial) {
            var bytes: usize = 0;
            for (executor.output_shapes[start..]) |shape| {
                var size: usize = @sizeOf(f32);
                for (shape) |dim| size = try std.math.mul(usize, size, std.math.cast(usize, dim) orelse return error.InvalidResidentState);
                bytes = try std.math.add(usize, bytes, size);
            }
            if (bytes > self.admitted_bytes) return error.UnadmittedResidentState;
            first_headers = try self.allocator.alloc(pjrt.Buffer, banks);
        } else if (self.buffers.len != banks) return error.InvalidResidentState;

        // Initial prefill has no past operands or host zero-bank uploads.
        // A decode cursor and borrowed old bank remain private to this step.
        const arguments = try a.alloc(pjrt.Buffer, if (executor.resident_initial) 0 else banks + 1);
        defer a.free(arguments);
        var cursor: ?pjrt.Buffer = null;
        defer if (cursor) |*buffer| buffer.deinit();
        if (!executor.resident_initial) {
            const position: f32 = @floatFromInt(self.position);
            cursor = try executor.client.bufferFromHostFloat32(&.{position}, &.{});
            arguments[0] = cursor.?;
            @memcpy(arguments[1..], self.buffers);
        }
        var outputs = try executor.executeWithResidentState(values, backend, arguments, a);
        var moved_first_bank = false;
        defer {
            for (outputs[0..if (moved_first_bank) start else outputs.len]) |*buffer| buffer.deinit();
            a.free(outputs);
        }
        if (outputs.len != executor.output_shapes.len) return error.InvalidResidentState;
        for (outputs, executor.output_shapes) |*buffer, shape| if (!try buffer.matchesFloat32Shape(shape)) return error.InvalidResidentState;
        const logits = try outputs[0].toFloat32WithShape(a, executor.output_shapes[0]);
        errdefer a.free(logits);
        // Reject invalid arithmetic before publishing any cache generation.
        for (logits) |value| if (!std.math.isFinite(value)) return error.InvalidResidentDecoderOutput;
        if (control) |active| try active.check();
        // All fallible work is complete. Publish a complete bank generation;
        // the first bank moves directly from prefill, later old banks retire.
        if (first_headers) |headers| {
            @memcpy(headers, outputs[start..]);
            self.buffers = headers;
            first_headers = null;
            moved_first_bank = true;
        } else {
            for (self.buffers, outputs[start..]) |*destination, *source| std.mem.swap(pjrt.Buffer, destination, source);
        }
        self.client = executor.client;
        self.position += valid_rows;
        return logits;
    }
};
