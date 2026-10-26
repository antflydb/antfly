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

//! Independently owned CUDA staging for attention_match_v1. This is not a
//! serving binding: bias/coverage-aware dispatch must be qualified separately.
//! Caller holds the CUDA context and source cache exclusively through upload.
const std = @import("std");
const half_exact = @import("half_exact.zig");
const context_mod = @import("context.zig");
const buffer_mod = @import("buffer.zig");
const checkpoint = @import("../../runtime/kv/attention_match_checkpoint.zig");
const packed_storage = checkpoint.packed_storage;
const rotated = @import("../../runtime/kv/turboquant_rotated.zig");
const Control = @import("../../runtime/kv/attention_match.zig").Control;

pub const Precision = enum { f16, f32 };
pub const Head = struct {
    layer: u32 = 0,
    head: u32 = 0,
    window: u32 = 0,
    dim: u32 = 0,
    rows: usize = 0,
    capacity: usize = 0,
    start: usize = 0,
    logical_length: u64 = 0,
    compacted_until: u64 = 0,
    /// Earliest position recoverable without restoring evicted local rows.
    rollback_floor: u64 = 0,
    compressed_rows: usize = 0,
    span_count: usize = 0,
    encoded_keys: buffer_mod.DeviceBuffer = .{},
    encoded_values: buffer_mod.DeviceBuffer = .{},
    spans: buffer_mod.DeviceBuffer = .{},
    keys: buffer_mod.DeviceBuffer = .{},
    values: buffer_mod.DeviceBuffer = .{},
    bias: buffer_mod.DeviceBuffer = .{},
    positions: buffer_mod.DeviceBuffer = .{},
    coverage_ends: buffer_mod.DeviceBuffer = .{},
};
pub const DeviceState = struct {
    allocator: std.mem.Allocator,
    context: *context_mod.CudaContext,
    precision: Precision,
    heads: []Head = &.{},
    logical_length: u64,
    initial_logical_length: u64,
    allocated_bytes: usize = 0,

    /// Independently owned shadow for repeat-query collection. The caller holds
    /// the source exclusively at a completed forward boundary. Only live rows
    /// are copied; ring order and fitted bias/coverage remain exact.
    pub fn clone(self: *DeviceState, allocator: std.mem.Allocator, max_bytes: usize, control: Control) !DeviceState {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (self.heads.len == 0 or self.heads.len > 8192) return error.InvalidAttentionShape;
        var bytes: usize = 0;
        for (self.heads) |head| {
            if (head.logical_length != self.logical_length or head.rows == 0 or head.rows > head.capacity or head.start >= head.capacity or head.capacity > 262144 or head.dim == 0 or head.dim > 1024) return error.InvalidAttentionShape;
            try validateCompressed(head);
            const width = try mul(head.dim, if (self.precision == .f16) 2 else 4);
            if (head.keys.len < try mul(head.capacity - head.compressed_rows, width) or head.values.len < try mul(head.capacity - head.compressed_rows, width) or head.bias.len < try mul(head.capacity, 4) or head.positions.len < try mul(head.capacity, 8) or head.coverage_ends.len < try mul(head.capacity, 8)) return error.InvalidAttentionShape;
            bytes = try add(bytes, try deviceBytes(head, head.rows, self.precision));
        }
        if (try add(self.context.device_allocations.snapshot().live, bytes) > max_bytes) return error.CudaMemoryLimitExceeded;
        var result: DeviceState = .{ .allocator = allocator, .context = self.context, .precision = self.precision, .logical_length = self.logical_length, .initial_logical_length = self.initial_logical_length };
        errdefer result.deinit();
        result.heads = try allocator.alloc(Head, self.heads.len);
        @memset(result.heads, .{});
        for (self.heads, result.heads) |source, *dest| {
            try control.check();
            dest.* = .{ .layer = source.layer, .head = source.head, .window = source.window, .dim = source.dim, .rows = source.rows, .capacity = source.rows, .logical_length = source.logical_length, .compacted_until = source.compacted_until, .rollback_floor = source.rollback_floor, .compressed_rows = source.compressed_rows, .span_count = source.span_count };
            const width = try mul(source.dim, if (self.precision == .f16) 2 else 4);
            dest.keys = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(source.rows - source.compressed_rows, width));
            dest.values = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(source.rows - source.compressed_rows, width));
            dest.bias = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(source.rows, 4));
            dest.positions = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(source.rows, 8));
            dest.coverage_ends = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(source.rows, 8));
            try cloneCompressed(self.context, dest, source);
            try copyRing(self.context, dest.keys, source.keys, exactHead(source), width);
            try copyRing(self.context, dest.values, source.values, exactHead(source), width);
            try copyRing(self.context, dest.bias, source.bias, source, 4);
            try copyRing(self.context, dest.positions, source.positions, source, 8);
            try copyRing(self.context, dest.coverage_ends, source.coverage_ends, source, 8);
        }
        try result.synchronize();
        try control.check();
        result.allocated_bytes = bytes;
        return result;
    }

    pub fn layer(self: *DeviceState, layer_index: u32) ![]Head {
        for (self.heads, 0..) |head, start| {
            if (head.layer != layer_index) continue;
            var end = start + 1;
            while (end < self.heads.len and self.heads[end].layer == layer_index) : (end += 1) {}
            return self.heads[start..end];
        }
        return error.UnsupportedKvSnapshotLayout;
    }

    /// Grow without changing any logical row. The old allocation remains owned
    /// until the complete linearized replacement has been copied and fenced.
    pub fn grow(self: *DeviceState, head: *Head, capacity: usize, max_bytes: usize, control: Control) !void {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (capacity <= head.capacity) return;
        if (head.rows == 0 or head.rows > head.capacity or head.start >= head.capacity or capacity > 262144)
            return error.InvalidAttentionShape;
        const row_bytes = try mul(head.dim, if (self.precision == .f16) 2 else 4);
        try validateCompressed(head.*);
        // Compressed buffers remain immutable and transfer only after the fence.
        const bytes = try add(try mul(capacity - head.compressed_rows, 2 * row_bytes), try mul(capacity, 20));
        if (try add(self.context.device_allocations.snapshot().live, bytes) > max_bytes) return error.CudaMemoryLimitExceeded;
        var replacement = head.*;
        replacement.encoded_keys = .{};
        replacement.encoded_values = .{};
        replacement.spans = .{};
        replacement.keys = .{};
        replacement.values = .{};
        replacement.bias = .{};
        replacement.positions = .{};
        replacement.coverage_ends = .{};
        replacement.capacity = capacity;
        replacement.start = 0;
        errdefer {
            self.context.synchronize() catch {};
            freeHead(self.context, &replacement);
        }
        replacement.keys = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(capacity - head.compressed_rows, row_bytes));
        replacement.values = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(capacity - head.compressed_rows, row_bytes));
        replacement.bias = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(capacity, 4));
        replacement.positions = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(capacity, 8));
        replacement.coverage_ends = try buffer_mod.DeviceBuffer.alloc(self.context, try mul(capacity, 8));
        try copyRing(self.context, replacement.keys, head.keys, exactHead(head.*), row_bytes);
        try copyRing(self.context, replacement.values, head.values, exactHead(head.*), row_bytes);
        try copyRing(self.context, replacement.bias, head.bias, head.*, 4);
        try copyRing(self.context, replacement.positions, head.positions, head.*, 8);
        try copyRing(self.context, replacement.coverage_ends, head.coverage_ends, head.*, 8);
        try self.context.synchronize();
        try control.check();
        const old_bytes = (head.capacity - head.compressed_rows) * 2 * row_bytes + head.capacity * 20;
        replacement.encoded_keys = head.encoded_keys;
        replacement.encoded_values = head.encoded_values;
        replacement.spans = head.spans;
        head.encoded_keys = .{};
        head.encoded_values = .{};
        head.spans = .{};
        freeHead(self.context, head);
        head.* = replacement;
        self.allocated_bytes = self.allocated_bytes - old_bytes + bytes;
    }

    /// Call after every consumer of this donor has completed the forward pass.
    /// Earlier queries in a multi-token append may need rows outside the final
    /// local window. Trimming before the next donor append is also safe.
    pub fn retainLocalWindow(self: *DeviceState, layer_index: u32) !void {
        try self.synchronize();
        for (try self.layer(layer_index)) |*head| {
            if (head.window == 0 or head.rows <= head.window) continue;
            const drop = head.rows - head.window;
            head.start = (head.start + drop) % head.capacity;
            head.rows -= drop;
            head.rollback_floor = head.logical_length;
        }
    }

    /// Roll back only the most recent exact append, before local history is
    /// evicted. CUDA token lookahead needs this when its pending token is EOS.
    /// All heads are validated and fenced before any row count is changed.
    pub fn truncate(self: *DeviceState, retained: u64, control: Control) !void {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (retained < self.initial_logical_length or retained > self.logical_length or self.heads.len == 0) return error.UnsupportedAttentionMatchRollback;
        const removed: usize = @intCast(self.logical_length - retained);
        if (removed == 0) return;
        for (self.heads) |head| {
            if (head.logical_length != self.logical_length or retained < head.rollback_floor or head.rows <= removed or head.rows > head.capacity or head.start >= head.capacity or
                (head.window > 0 and head.rows - removed < @min(head.window, retained))) return error.UnsupportedAttentionMatchRollback;
        }
        try self.synchronize();
        try control.check();
        for (self.heads) |*head| {
            head.rows -= removed;
            head.logical_length = retained;
        }
        self.logical_length = retained;
    }

    /// Read the authoritative device rows after append, rather than exporting
    /// the stale host candidate used for installation. Metadata supplies trusted
    /// identities, current tokens/logits and the unchanged model donor mapping.
    /// The owner holds the cache exclusively through readback and validation.
    pub fn download(self: *DeviceState, allocator: std.mem.Allocator, metadata: checkpoint.State, limits: checkpoint.Limits, max_host_bytes: usize, control: Control) !checkpoint.Owned {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (metadata.logical_length != self.logical_length or metadata.tokens.len != self.logical_length or
            metadata.logical_length > limits.max_tokens or metadata.logits.len == 0 or metadata.logits.len > limits.max_vocab or
            metadata.layer_donors.len == 0 or metadata.layer_donors.len > limits.max_layers or metadata.donors.len == 0 or
            metadata.donors.len > metadata.layer_donors.len) return error.InvalidKvCheckpoint;
        var required = try mul(try add(try add(metadata.tokens.len, metadata.logits.len), metadata.layer_donors.len), 4);
        required = try add(required, try mul(metadata.donors.len, @sizeOf(checkpoint.Donor)));
        var count: usize = 0;
        for (metadata.donors) |donor| {
            const heads = try self.layer(donor.layer);
            if (heads.len != donor.heads.len or heads.len > limits.max_kv_heads) return error.InvalidKvCheckpoint;
            count = try add(count, heads.len);
            required = try add(required, try mul(heads.len, @sizeOf(checkpoint.Head)));
            for (heads, 0..) |stored, i| {
                if (stored.rows > stored.capacity or stored.start >= stored.capacity) return error.InvalidKvCheckpoint;
                const head = snapshotHead(stored);
                if (head.head != i or head.window != donor.window or head.logical_length != self.logical_length or
                    head.dim == 0 or head.dim > limits.max_head_dim or head.rows == 0 or head.rows > head.capacity or
                    head.capacity > 262144 or head.start >= head.capacity or (head.window > 0 and head.rows != @min(head.window, self.logical_length)))
                    return error.InvalidKvCheckpoint;
                try validateCompressed(head);
                required = try add(required, try add(compressedBytes(head), try mul(head.rows, try add(20, try mul(head.dim, 8)))));
            }
        }
        if (count != self.heads.len or required > limits.max_state_bytes) return error.KvCheckpointLimit;
        const staging_count: usize = if (self.precision == .f16) 128 * 1024 else 0;
        if (try add(required, staging_count * 2) > max_host_bytes) return error.CompactionWorkspaceLimit;
        var result = try checkpoint.cloneMetadata(allocator, metadata);
        errdefer result.deinit();
        const staging = try allocator.alloc(f16, staging_count);
        defer allocator.free(staging);
        for (result.state.donors) |donor| {
            for (try self.layer(donor.layer), @constCast(donor.heads)) |stored, *head| {
                const source = snapshotHead(stored);
                try control.check();
                head.dim = source.dim;
                head.compacted_until = source.compacted_until;
                head.positions = try allocator.alloc(u64, source.rows);
                head.coverage_ends = try allocator.alloc(u64, source.rows);
                head.bias = try allocator.alloc(f32, source.rows);
                head.keys = try allocator.alloc(f32, try mul(source.rows, source.dim));
                head.values = try allocator.alloc(f32, head.keys.len);
                try self.readRing(source.positions, source, 8, std.mem.sliceAsBytes(@constCast(head.positions)), control);
                try self.readRing(source.coverage_ends, source, 8, std.mem.sliceAsBytes(@constCast(head.coverage_ends)), control);
                try self.readRing(source.bias, source, 4, std.mem.sliceAsBytes(@constCast(head.bias)), control);
                if (source.compressed_rows == 0) {
                    try self.readValues(source.keys, source, @constCast(head.keys), staging, control);
                    try self.readValues(source.values, source, @constCast(head.values), staging, control);
                } else {
                    try self.downloadCompressed(allocator, source, head, staging, control);
                }
            }
        }
        try control.check();
        try result.state.validateWithControl(limits, control);
        return result;
    }

    fn downloadCompressed(self: *DeviceState, allocator: std.mem.Allocator, source: Head, head: *checkpoint.Head, staging: []f16, control: Control) !void {
        head.compressed.version = rotated.version;
        head.compressed.spans = try allocator.alloc(packed_storage.Span, source.span_count);
        head.compressed.keys = try allocator.alloc(u8, source.encoded_keys.len);
        head.compressed.values = try allocator.alloc(u8, source.encoded_values.len);
        // Each copy is fenced before its destination can unwind.
        try self.readRaw(source.spans, std.mem.sliceAsBytes(@constCast(head.compressed.spans)), control);
        try self.readRaw(source.encoded_keys, @constCast(head.compressed.keys), control);
        try self.readRaw(source.encoded_values, @constCast(head.compressed.values), control);
        try head.compressed.validate(source.rows, source.dim);
        if (head.compressed.rows() != source.compressed_rows) return error.InvalidKvCheckpoint;
        var row: usize = 0;
        while (row < source.rows) {
            try control.check();
            switch (head.compressed.index(row)) {
                .compressed => |index| {
                    try head.compressed.decodeRow(source.dim, index, @constCast(head.keys[row * source.dim ..][0..source.dim]), @constCast(head.values[row * source.dim ..][0..source.dim]));
                    row += 1;
                },
                .exact => |index| {
                    const first = row;
                    while (row < source.rows and head.compressed.index(row) == .exact) : (row += 1) {}
                    var part = exactHead(source);
                    part.start = index;
                    part.rows = row - first;
                    try self.readValues(source.keys, part, @constCast(head.keys[first * source.dim .. row * source.dim]), staging, control);
                    try self.readValues(source.values, part, @constCast(head.values[first * source.dim .. row * source.dim]), staging, control);
                },
            }
        }
    }

    fn readRaw(self: *DeviceState, source: buffer_mod.DeviceBuffer, output: []u8, control: Control) !void {
        errdefer self.context.synchronize() catch {};
        var offset: usize = 0;
        while (offset < output.len) {
            try control.check();
            const count = @min(output.len - offset, 256 * 1024);
            const part: buffer_mod.DeviceBuffer = .{ .ptr = source.ptr + offset, .len = count };
            try part.copyToHost(self.context, output[offset..][0..count]);
            try self.synchronize();
            offset += count;
        }
    }

    fn readRing(self: *DeviceState, buffer: buffer_mod.DeviceBuffer, head: Head, width: usize, output: []u8, control: Control) !void {
        const ring_bytes = try mul(head.capacity, width);
        if (output.len != try mul(head.rows, width) or buffer.len < ring_bytes) return error.InvalidKvCheckpoint;
        errdefer self.context.synchronize() catch {};
        var offset: usize = 0;
        while (offset < output.len) {
            try control.check();
            const physical = (head.start * width + offset) % ring_bytes;
            const count = @min(@min(output.len - offset, ring_bytes - physical), 256 * 1024);
            const source: buffer_mod.DeviceBuffer = .{ .ptr = buffer.ptr + physical, .len = count };
            try source.copyToHost(self.context, output[offset..][0..count]);
            try self.synchronize();
            offset += count;
        }
    }

    fn readValues(self: *DeviceState, buffer: buffer_mod.DeviceBuffer, head: Head, output: []f32, staging: []f16, control: Control) !void {
        if (self.precision == .f32) return self.readRing(buffer, head, head.dim * 4, std.mem.sliceAsBytes(output), control);
        const capacity = try mul(head.capacity, head.dim);
        if (buffer.len < try mul(capacity, 2) or output.len != try mul(head.rows, head.dim)) return error.InvalidKvCheckpoint;
        errdefer self.context.synchronize() catch {};
        var offset: usize = 0;
        while (offset < output.len) {
            try control.check();
            const physical = (head.start * head.dim + offset) % capacity;
            const count = @min(@min(output.len - offset, capacity - physical), staging.len);
            const source: buffer_mod.DeviceBuffer = .{ .ptr = buffer.ptr + physical * 2, .len = count * 2 };
            try source.copyToHost(self.context, std.mem.sliceAsBytes(staging[0..count]));
            try self.synchronize();
            for (output[offset..][0..count], staging[0..count]) |*out, value| out.* = half_exact.decode(value);
            offset += count;
        }
    }

    pub fn synchronize(self: *DeviceState) !void {
        try self.context.synchronize();
    }
    pub fn deinit(self: *DeviceState) void {
        // Pending async metadata uploads may still borrow host checkpoint data.
        self.context.synchronize() catch {};
        for (self.heads) |*head| {
            freeHead(self.context, head);
        }
        self.allocator.free(self.heads);
        self.* = undefined;
    }
};
fn snapshotHead(head: Head) Head {
    var result = head;
    if (head.capacity > 0 and head.window > 0 and head.rows > head.window) {
        result.start = (head.start + head.rows - head.window) % head.capacity;
        result.rows = head.window;
    }
    return result;
}
fn freeHead(context: *context_mod.CudaContext, head: *Head) void {
    head.encoded_keys.free(context);
    head.encoded_values.free(context);
    head.spans.free(context);
    head.keys.free(context);
    head.values.free(context);
    head.bias.free(context);
    head.positions.free(context);
    head.coverage_ends.free(context);
}
fn copyRing(context: *context_mod.CudaContext, dst: buffer_mod.DeviceBuffer, src: buffer_mod.DeviceBuffer, head: Head, width: usize) !void {
    const first = @min(head.rows, head.capacity - head.start);
    const part: buffer_mod.DeviceBuffer = .{ .ptr = src.ptr + head.start * width, .len = first * width };
    try dst.copyFromDevice(context, part, part.len);
    if (first < head.rows) {
        const tail: buffer_mod.DeviceBuffer = .{ .ptr = dst.ptr + first * width, .len = (head.rows - first) * width };
        try tail.copyFromDevice(context, src, tail.len);
    }
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CudaMemoryLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CudaMemoryLimitExceeded;
}

/// CPU-only storage preflight, also used before committing HTTP response headers.
/// No CUDA allocation or silent f16 rounding is allowed before this succeeds.
pub fn allocationBytesWithControl(state: checkpoint.State, precision: Precision, limits: checkpoint.Limits, control: Control) !usize {
    try state.validateWithControl(limits, control);
    if (try state.residentBytes() > limits.max_state_bytes) return error.KvCheckpointLimit;
    var bytes: usize = 0;
    for (state.donors) |donor| for (donor.heads) |head| {
        try control.check();
        const exact_elements = try mul(head.positions.len - head.compressed.rows(), head.dim);
        bytes = try add(bytes, try add(head.compressed.bytes(), try add(try mul(exact_elements, if (precision == .f16) 4 else 8), try mul(head.positions.len, 20))));
        if (precision == .f16) {
            for ([_][]const f32{ head.keys, head.values }) |values| {
                var runs = head.compressed.exactRuns(head.positions.len);
                while (runs.next()) |run| {
                    var offset = run.first * head.dim;
                    const limit = offset + run.rows * head.dim;
                    while (offset < limit) {
                        try control.check();
                        const end = offset + @min(@as(usize, 4096), limit - offset);
                        for (values[offset..end]) |value| {
                            if (!half_exact.isRepresentable(value)) return error.KvStoragePrecisionMismatch;
                        }
                        offset = end;
                    }
                }
            }
        }
    };
    try control.check();
    return bytes;
}

pub const Backend = struct {
    allocator: std.mem.Allocator,
    context: *context_mod.CudaContext,
    precision: Precision,
    limits: checkpoint.Limits = .{},

    pub fn allocationBytes(self: Backend, state: checkpoint.State) !usize {
        return allocationBytesWithControl(state, self.precision, self.limits, .{});
    }

    /// max_bytes bounds total live context allocation, including model weights,
    /// original cache and other resident workspace, not only the new buffers.
    pub fn upload(self: Backend, state: checkpoint.State, max_bytes: usize, control: Control) !DeviceState {
        try control.check();
        try self.context.makeCurrent();
        if (self.context.debug_graph_capture_active or try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        const bytes = try allocationBytesWithControl(state, self.precision, self.limits, control);
        if (try add(self.context.device_allocations.snapshot().live, bytes) > max_bytes) return error.CudaMemoryLimitExceeded;
        var count: usize = 0;
        for (state.donors) |donor| count = try add(count, donor.heads.len);
        var result: DeviceState = .{ .allocator = self.allocator, .context = self.context, .precision = self.precision, .logical_length = state.logical_length, .initial_logical_length = state.logical_length };
        errdefer result.deinit();
        result.heads = try self.allocator.alloc(Head, count);
        @memset(result.heads, .{});
        // Reuse a bounded conversion slab for the entire upload. The previous
        // 8 KiB stack slab forced thousands of stream fences on real models.
        const staging = try self.allocator.alloc(f16, if (self.precision == .f16) 128 * 1024 else 0);
        defer self.allocator.free(staging);
        var index: usize = 0;
        for (state.donors) |donor| for (donor.heads, 0..) |source, head_index| {
            try control.check();
            const head = &result.heads[index];
            index += 1;
            head.layer = donor.layer;
            head.head = @intCast(head_index);
            head.window = donor.window;
            head.dim = source.dim;
            head.rows = source.positions.len;
            head.capacity = head.rows;
            head.logical_length = state.logical_length;
            head.rollback_floor = state.logical_length;
            head.compacted_until = source.compacted_until;
            head.compressed_rows = source.compressed.rows();
            head.span_count = source.compressed.spans.len;
            const kv_bytes = try mul(try mul(head.rows - head.compressed_rows, head.dim), if (self.precision == .f16) 2 else 4);
            head.keys = try buffer_mod.DeviceBuffer.alloc(self.context, kv_bytes);
            head.values = try buffer_mod.DeviceBuffer.alloc(self.context, kv_bytes);
            head.bias = try buffer_mod.DeviceBuffer.alloc(self.context, source.bias.len * 4);
            head.positions = try buffer_mod.DeviceBuffer.alloc(self.context, source.positions.len * 8);
            head.coverage_ends = try buffer_mod.DeviceBuffer.alloc(self.context, source.coverage_ends.len * 8);
            head.encoded_keys = try buffer_mod.DeviceBuffer.alloc(self.context, source.compressed.keys.len);
            head.encoded_values = try buffer_mod.DeviceBuffer.alloc(self.context, source.compressed.values.len);
            head.spans = try buffer_mod.DeviceBuffer.alloc(self.context, source.compressed.spans.len * @sizeOf(packed_storage.Span));
            try head.encoded_keys.copyFromHost(self.context, source.compressed.keys);
            try head.encoded_values.copyFromHost(self.context, source.compressed.values);
            try head.spans.copyFromHost(self.context, std.mem.sliceAsBytes(source.compressed.spans));
            try self.uploadExact(head.keys, source.keys, source, staging, control);
            try self.uploadExact(head.values, source.values, source, staging, control);
            try head.bias.copyFromHost(self.context, std.mem.sliceAsBytes(source.bias));
            try head.positions.copyFromHost(self.context, std.mem.sliceAsBytes(source.positions));
            try head.coverage_ends.copyFromHost(self.context, std.mem.sliceAsBytes(source.coverage_ends));
        };
        try result.synchronize();
        try control.check();
        result.allocated_bytes = bytes;
        return result;
    }

    fn uploadExact(self: Backend, buffer: buffer_mod.DeviceBuffer, values: []const f32, source: checkpoint.Head, staging: []f16, control: Control) !void {
        // Keep the slab alive on failures as well as successful uploads. Pack
        // across exact-run boundaries so fragmented retention does not force a
        // small transfer and stream fence for each run.
        errdefer self.context.synchronize() catch {};
        var runs = source.compressed.exactRuns(source.positions.len);
        var staged: usize = 0;
        var uploaded: usize = 0;
        while (runs.next()) |run| {
            try control.check();
            const exact = values[run.first * source.dim ..][0 .. run.rows * source.dim];
            if (self.precision == .f32) {
                const dest: buffer_mod.DeviceBuffer = .{ .ptr = buffer.ptr + run.offset * source.dim * 4, .len = exact.len * 4 };
                try dest.copyFromHost(self.context, std.mem.sliceAsBytes(exact));
                continue;
            }
            var offset: usize = 0;
            while (offset < exact.len) {
                try control.check();
                const count = @min(staging.len - staged, exact.len - offset);
                for (staging[staged..][0..count], exact[offset..][0..count]) |*out, value| out.* = half_exact.encode(value);
                staged += count;
                offset += count;
                if (staged == staging.len) {
                    try self.uploadStaged(buffer, staging, uploaded, control);
                    uploaded += staged;
                    staged = 0;
                }
            }
        }
        if (staged > 0) try self.uploadStaged(buffer, staging[0..staged], uploaded, control);
    }

    fn uploadStaged(self: Backend, buffer: buffer_mod.DeviceBuffer, staging: []const f16, offset: usize, control: Control) !void {
        try control.check();
        const target: buffer_mod.DeviceBuffer = .{ .ptr = buffer.ptr + offset * 2, .len = staging.len * 2 };
        try target.copyFromHost(self.context, std.mem.sliceAsBytes(staging));
        try self.context.synchronize();
    }
};

test "attention match CUDA storage preflight checks precision before device allocation" {
    const state: checkpoint.State = .{
        .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
        .logical_length = 2,
        .tokens = &.{ 0, 1 },
        .logits = &.{ 0, 1 },
        .layer_donors = &.{0},
        .donors = &.{.{ .layer = 0, .window = 2, .heads = &.{.{
            .dim = 1,
            .compacted_until = 0,
            .positions = &.{ 0, 1 },
            .coverage_ends = &.{ 1, 2 },
            .keys = &.{ 0, 1 },
            .values = &.{ -2, 3 },
            .bias = &.{ 0, 0 },
        }} }},
    };
    try std.testing.expectEqual(@as(usize, 48), try allocationBytesWithControl(state, .f16, .{}, .{}));
    try std.testing.expectEqual(@as(usize, 56), try allocationBytesWithControl(state, .f32, .{}, .{}));
    var changed = state;
    var donor = state.donors[0];
    var head = donor.heads[0];
    head.keys = &.{ 0, 0.1 };
    donor.heads = (&head)[0..1];
    changed.donors = (&donor)[0..1];
    try std.testing.expectError(error.KvStoragePrecisionMismatch, allocationBytesWithControl(changed, .f16, .{}, .{}));
    try std.testing.expectEqual(@as(usize, 56), try allocationBytesWithControl(changed, .f32, .{}, .{}));
    head.keys = &.{ 0, 1 };
    head.values = &.{ -2, 0.1 };
    try std.testing.expectError(error.KvStoragePrecisionMismatch, allocationBytesWithControl(changed, .f16, .{}, .{}));
    try std.testing.expectError(error.KvCheckpointLimit, allocationBytesWithControl(state, .f16, .{ .max_state_bytes = 1 }, .{}));
}

test "attention match CUDA storage preflight cancellation covers large arrays" {
    const count = 5000;
    const allocator = std.testing.allocator;
    const positions = try allocator.alloc(u64, count);
    defer allocator.free(positions);
    const ends = try allocator.alloc(u64, count);
    defer allocator.free(ends);
    const tokens = try allocator.alloc(u32, count);
    defer allocator.free(tokens);
    const values = try allocator.alloc(f32, count);
    defer allocator.free(values);
    for (positions, ends, tokens, values, 0..) |*position, *end, *token, *value, i| {
        position.* = i;
        end.* = i + 1;
        token.* = @intCast(i % 2);
        value.* = 0;
    }
    const state: checkpoint.State = .{
        .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
        .logical_length = count,
        .tokens = tokens,
        .logits = &.{ 0, 1 },
        .layer_donors = &.{0},
        .donors = &.{.{ .layer = 0, .window = count, .heads = &.{.{
            .dim = 1,
            .compacted_until = 0,
            .positions = positions,
            .coverage_ends = ends,
            .keys = values,
            .values = values,
            .bias = values,
        }} }},
    };
    const Cancel = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const call = self.calls;
            self.calls += 1;
            if (call == self.fail_at) return error.Cancelled;
        }
        fn control(self: *@This()) Control {
            return .{ .context = self, .check_fn = check };
        }
    };
    var completed: Cancel = .{};
    try std.testing.expectEqual(@as(usize, count * 24), try allocationBytesWithControl(state, .f16, .{}, completed.control()));
    try std.testing.expect(completed.calls > 4);
    for (0..completed.calls) |fail_at| {
        var cancel: Cancel = .{ .fail_at = fail_at };
        try std.testing.expectError(error.Cancelled, allocationBytesWithControl(state, .f16, .{}, cancel.control()));
    }
}

fn exactHead(head: Head) Head {
    var result = head;
    result.rows -= head.compressed_rows;
    result.capacity -= head.compressed_rows;
    return result;
}

fn compressedBytes(head: Head) usize {
    return head.encoded_keys.len + head.encoded_values.len + head.spans.len;
}

fn deviceBytes(head: Head, capacity: usize, precision: Precision) !usize {
    return add(compressedBytes(head), try add(try mul(capacity, 20), try mul(try mul(capacity - head.compressed_rows, head.dim), if (precision == .f16) 4 else 8)));
}

pub fn validateCompressed(head: Head) !void {
    if (head.compressed_rows == 0) {
        if (head.span_count != 0 or compressedBytes(head) != 0) return error.InvalidAttentionShape;
        return;
    }
    if (head.compressed_rows > head.rows or head.rows > head.capacity or head.span_count == 0 or head.span_count > head.compressed_rows or
        head.start != 0 or head.window != 0 or !rotated.supported(head.dim) or
        head.encoded_keys.ptr == 0 or head.encoded_values.ptr == 0 or head.spans.ptr == 0 or
        head.encoded_keys.len != head.compressed_rows * rotated.rowBytes(head.dim) or
        head.encoded_values.len != head.compressed_rows * packed_storage.valueBytes(head.dim) or
        head.spans.len != head.span_count * @sizeOf(packed_storage.Span)) return error.InvalidAttentionShape;
}

fn cloneCompressed(context: *context_mod.CudaContext, dest: *Head, source: Head) !void {
    dest.encoded_keys = try buffer_mod.DeviceBuffer.alloc(context, source.encoded_keys.len);
    dest.encoded_values = try buffer_mod.DeviceBuffer.alloc(context, source.encoded_values.len);
    dest.spans = try buffer_mod.DeviceBuffer.alloc(context, source.spans.len);
    try dest.encoded_keys.copyFromDevice(context, source.encoded_keys, source.encoded_keys.len);
    try dest.encoded_values.copyFromDevice(context, source.encoded_values, source.encoded_values.len);
    try dest.spans.copyFromDevice(context, source.spans, source.spans.len);
}
