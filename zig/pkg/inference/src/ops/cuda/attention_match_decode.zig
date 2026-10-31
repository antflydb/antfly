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

//! Context-owned ragged attention dispatch. The serving owner must serialize
//! mutation, dispatch and retirement; creating this runner does not install KV.
const std = @import("std");
const checkpoint = @import("../../runtime/kv/attention_match_checkpoint.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const driver = @import("driver.zig");
const storage = @import("attention_match_storage.zig");
const Control = @import("../../runtime/kv/attention_match.zig").Control;

const Descriptor = extern struct {
    keys: u64,
    values: u64,
    bias: u64,
    positions: u64,
    coverage_ends: u64,
    rows: u64,
    capacity: u64,
    start: u64,
    encoded_keys: u64,
    encoded_values: u64,
    spans: u64,
    compressed_rows: u64,
    span_count: u64,
};
comptime {
    if (@sizeOf(Descriptor) != 104 or @offsetOf(Descriptor, "rows") != 40) @compileError("CUDA head ABI mismatch");
}

pub const Request = struct {
    heads: []const storage.Head,
    precision: storage.Precision,
    queries: Buffer,
    output: Buffer,
    log_mass: ?Buffer = null,
    query_rows: usize,
    query_heads: usize,
    first_query_position: u64,
    scale: f32,
    softcap: f32 = 0,
};

const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("artifacts/attention_match_decode.fatbin"), .{
    .partial_f16 = "attention_match_partial_f16",
    .partial_f32 = "attention_match_partial_f32",
    .append_f16 = "attention_match_append_f16",
    .append_f32 = "attention_match_append_f32",
    .merge = "attention_match_merge",
    .rotated_f16 = "attention_match_rotated_f16",
    .rotated_f32 = "attention_match_rotated_f32",
    .rotate_queries = "attention_match_rotate_queries",
});

pub const Runner = struct {
    context: *Context,
    program: Program,
    rotated_queries: Buffer = .{},
    descriptors: Buffer = .{},
    partials: Buffer = .{},
    status: Buffer = .{},
    max_device_bytes: usize,
    max_scratch_bytes: usize = 64 * 1024 * 1024,

    pub fn init(context: *Context, max_device_bytes: usize) !Runner {
        try context.makeCurrent();
        if (try context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        var program: Program = .{};
        try program.initCurrent(&context.driver);
        return .{ .context = context, .program = program, .max_device_bytes = max_device_bytes };
    }

    pub fn deinit(self: *Runner) void {
        self.context.makeCurrent() catch {};
        self.context.synchronize() catch {};
        self.descriptors.free(self.context);
        self.partials.free(self.context);
        self.status.free(self.context);
        self.rotated_queries.free(self.context);
        self.program.deinitCurrent(&self.context.driver) catch return;
        self.* = undefined;
    }

    fn reserve(self: *Runner, buffer: *Buffer, required: usize) !void {
        if (buffer.len >= required) return;
        const live = self.context.device_allocations.snapshot().live;
        if (required > self.max_device_bytes or live > self.max_device_bytes - required) return error.CudaMemoryLimitExceeded;
        // Admit the overlap before allocation; allocation failure leaves the
        // previous workspace usable. Every dispatch is fenced before return.
        const replacement = try Buffer.alloc(self.context, required);
        buffer.free(self.context);
        buffer.* = replacement;
    }

    /// Append post-RoPE token-major f32 K/V to one donor. Writes only reserved
    /// tail rows; finite-value validation, fence and cancellation precede
    /// publication. Existing rows remain semantically unchanged on failure.
    /// A forward pass must append each donor exactly once and finish attention
    /// before trimming its local window. Do not export a partially run pass.
    pub fn append(self: *Runner, state: *storage.DeviceState, layer: u32, keys: Buffer, values: Buffer, rows: usize, first_position: u64, control: Control) !void {
        try control.check();
        if (state.context != self.context) return error.InvalidCudaState;
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (rows == 0 or rows > 512 or first_position != state.logical_length or first_position < state.initial_logical_length)
            return error.InvalidAttentionShape;
        const end = std.math.add(u64, first_position, rows) catch return error.InvalidAttentionShape;
        // Resume admission bounds the prompt suffix to 8192 tokens. Decode
        // appends share this runner but have an independent output allowance;
        // both phases are bounded by the admitted logical context ceiling.
        if (end > checkpoint.max_context_tokens) return error.CompactionAppendLimit;
        const heads = try state.layer(layer);
        if (heads.len == 0 or heads.len > 64 or heads[0].dim == 0 or heads[0].dim > 1024) return error.InvalidAttentionShape;
        const dim: usize = heads[0].dim;
        const count = rows * heads.len * dim;
        if (keys.ptr == 0 or values.ptr == 0 or keys.len < count * 4 or values.len < count * 4) return error.InvalidAttentionShape;
        for (heads, 0..) |head, i| {
            try storage.validateCompressed(head);
            if (head.head != i or head.dim != dim or head.logical_length != first_position or head.rows > head.capacity or
                head.capacity == 0 or head.capacity > 262144 or head.start >= head.capacity or head.window > 131072)
                return error.InvalidAttentionShape;
            for ([_]Buffer{ head.keys, head.values, head.bias, head.positions, head.coverage_ends, head.encoded_keys, head.encoded_values, head.spans }) |source|
                if (overlaps(keys, source) or overlaps(values, source)) return error.InvalidAttentionShape;
        }
        var descriptors: [64]Descriptor = undefined;
        for (heads, 0..) |*head, i| {
            const required = head.rows + rows;
            const capacity = if (head.window == 0) ((required + 255) / 256) * 256 else @max(required, @as(usize, head.window) + 512);
            try state.grow(head, capacity, self.max_device_bytes, control);
            descriptors[i] = .{ .keys = head.keys.ptr, .values = head.values.ptr, .bias = head.bias.ptr, .positions = head.positions.ptr, .coverage_ends = head.coverage_ends.ptr, .rows = head.rows, .capacity = head.capacity, .start = head.start, .encoded_keys = head.encoded_keys.ptr, .encoded_values = head.encoded_values.ptr, .spans = head.spans.ptr, .compressed_rows = head.compressed_rows, .span_count = head.span_count };
        }
        try self.reserve(&self.descriptors, heads.len * @sizeOf(Descriptor));
        try self.reserve(&self.status, @sizeOf(c_int));
        var status: c_int = 0;
        errdefer self.context.synchronize() catch {};
        try self.descriptors.copyFromHost(self.context, std.mem.sliceAsBytes(descriptors[0..heads.len]));
        try self.status.copyFromHost(self.context, std.mem.asBytes(&status));
        try self.launch(if (state.precision == .f16) self.program.function(.append_f16) else self.program.function(.append_f32), (count + 255) / 256, 1, .{ keys.ptr, values.ptr, self.descriptors.ptr, int(heads.len), int(dim), int(rows), first_position, self.status.ptr });
        try self.status.copyToHost(self.context, std.mem.asBytes(&status));
        try self.context.synchronize();
        if (status != 0) return error.NonFiniteAttention;
        try control.check();
        for (heads) |*head| {
            head.rollback_floor = first_position;
            head.rows += rows;
            head.logical_length = end;
        }
        for (state.heads) |head| if (head.logical_length != end) return;
        state.logical_length = end;
    }

    /// Synchronous, bounded dispatch. No source buffers are mutated. A failed
    /// request may have partially written its private output, which is invalid.
    pub fn attend(self: *Runner, request: Request, control: Control) !void {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (request.heads.len == 0 or request.heads.len > 64 or request.query_heads == 0 or request.query_heads > 256 or
            request.query_heads % request.heads.len != 0 or request.query_rows == 0 or request.query_rows > checkpoint.max_context_tokens or
            !std.math.isFinite(request.scale) or request.scale <= 0 or !std.math.isFinite(request.softcap) or request.softcap < 0)
            return error.InvalidAttentionShape;
        _ = std.math.add(u64, request.first_query_position, request.query_rows - 1) catch return error.InvalidAttentionShape;
        const dim: usize = request.heads[0].dim;
        if (dim == 0 or dim > 1024) return error.InvalidAttentionShape;
        const count = request.query_rows * request.query_heads * dim;
        if (request.queries.ptr == 0 or request.output.ptr == 0 or request.queries.len < count * 4 or request.output.len < count * 4)
            return error.InvalidAttentionShape;
        if (overlaps(request.queries, request.output)) return error.InvalidAttentionShape;
        if (request.log_mass) |mass| if (mass.ptr == 0 or mass.len < request.query_rows * request.query_heads * 4 or
            overlaps(mass, request.queries) or overlaps(mass, request.output)) return error.InvalidAttentionShape;
        var descriptors: [64]Descriptor = undefined;
        var max_rows: usize = 0;
        var compressed = false;
        for (request.heads, 0..) |head, i| {
            if (head.dim != dim or head.layer != request.heads[0].layer or head.window != request.heads[0].window or
                head.head != i or head.rows == 0 or head.rows > head.capacity or head.capacity > 262144 or
                head.start >= head.capacity or head.bias.ptr == 0 or
                head.positions.ptr == 0 or head.coverage_ends.ptr == 0)
                return error.InvalidAttentionShape;
            try storage.validateCompressed(head);
            compressed = compressed or head.compressed_rows != 0;
            const kv_bytes = (head.capacity - head.compressed_rows) * dim * @as(usize, if (request.precision == .f16) 2 else 4);
            if (kv_bytes != 0 and (head.keys.ptr == 0 or head.values.ptr == 0)) return error.InvalidAttentionShape;
            if (head.keys.len < kv_bytes or head.values.len < kv_bytes or head.bias.len < head.capacity * 4 or
                head.positions.len < head.capacity * 8 or head.coverage_ends.len < head.capacity * 8) return error.InvalidAttentionShape;
            for ([_]Buffer{ head.keys, head.values, head.bias, head.positions, head.coverage_ends, head.encoded_keys, head.encoded_values, head.spans }) |source| {
                if (overlaps(request.output, source)) return error.InvalidAttentionShape;
                if (request.log_mass) |mass| if (overlaps(mass, source)) return error.InvalidAttentionShape;
            }
            max_rows = @max(max_rows, head.rows);
            descriptors[i] = .{ .keys = head.keys.ptr, .values = head.values.ptr, .bias = head.bias.ptr, .positions = head.positions.ptr, .coverage_ends = head.coverage_ends.ptr, .rows = head.rows, .capacity = head.capacity, .start = head.start, .encoded_keys = head.encoded_keys.ptr, .encoded_values = head.encoded_values.ptr, .spans = head.spans.ptr, .compressed_rows = head.compressed_rows, .span_count = head.span_count };
        }
        const splits = (max_rows + 255) / 256;
        const per_row = request.query_heads * splits * (dim + 2) * 4;
        const rotation_per_row = if (compressed) request.query_heads * dim * 4 else 0;
        const chunk_rows = @min(@as(usize, 16), self.max_scratch_bytes / (per_row + rotation_per_row));
        if (chunk_rows == 0) return error.CompactionWorkspaceLimit;
        try self.reserve(&self.descriptors, @sizeOf(Descriptor) * request.heads.len);
        try self.reserve(&self.partials, @min(request.query_rows, chunk_rows) * per_row);
        if (compressed) try self.reserve(&self.rotated_queries, @min(request.query_rows, chunk_rows) * rotation_per_row);
        // Keep stack descriptors alive on all paths, including launch/cancel
        // failures after an asynchronous copy was queued.
        errdefer self.context.synchronize() catch {};
        try self.descriptors.copyFromHost(self.context, std.mem.sliceAsBytes(descriptors[0..request.heads.len]));
        var offset: usize = 0;
        while (offset < request.query_rows) {
            try control.check();
            const rows = @min(chunk_rows, request.query_rows - offset);
            const q = request.queries.ptr + offset * request.query_heads * dim * 4;
            const out = request.output.ptr + offset * request.query_heads * dim * 4;
            const mass = if (request.log_mass) |buffer| buffer.ptr + offset * request.query_heads * 4 else @as(usize, 0);
            const position = request.first_query_position + offset;
            if (compressed) {
                try self.launch(self.program.function(.rotate_queries), rows * request.query_heads, 1, .{ q, self.rotated_queries.ptr, int(dim) });
                try self.launch(if (request.precision == .f16) self.program.function(.rotated_f16) else self.program.function(.rotated_f32), splits, rows * request.query_heads, .{ q, self.rotated_queries.ptr, self.descriptors.ptr, self.partials.ptr, int(request.query_heads), int(request.heads.len), int(dim), int(splits), position, request.scale, request.softcap, @as(u64, request.heads[0].window) });
            } else {
                try self.launch(if (request.precision == .f16) self.program.function(.partial_f16) else self.program.function(.partial_f32), splits, rows * request.query_heads, .{ q, self.descriptors.ptr, self.partials.ptr, int(request.query_heads), int(request.heads.len), int(dim), int(splits), position, request.scale, request.softcap, @as(u64, request.heads[0].window) });
            }
            try self.launch(self.program.function(.merge), rows * request.query_heads, 1, .{ self.partials.ptr, out, mass, int(dim), int(splits) });
            // Bound cancellation latency and retire scratch before reuse.
            try self.context.synchronize();
            offset += rows;
        }
        try control.check();
    }

    fn launch(self: *Runner, function: driver.CUfunction, x: usize, y: usize, args: anytype) !void {
        const names = @typeInfo(@TypeOf(args)).@"struct".field_names;
        const Types = comptime blk: {
            var types: [names.len]type = undefined;
            for (names, 0..) |name, i| types[i] = @FieldType(@TypeOf(args), name);
            break :blk types;
        };
        var values: @Tuple(&Types) = undefined;
        var params: [names.len]?*anyopaque = undefined;
        inline for (0..names.len) |i| {
            values[i] = args[i];
            params[i] = @ptrCast(&values[i]);
        }
        try self.context.driver.check(self.context.driver.fns.cuLaunchKernel(function, @intCast(x), @intCast(y), 1, 256, 1, 1, 0, self.context.stream, &params, null));
    }
};

fn int(value: usize) c_int {
    return @intCast(value);
}

fn overlaps(a: Buffer, b: Buffer) bool {
    // Difference comparison avoids wrapping a pointer plus its buffer length.
    return if (a.ptr >= b.ptr) a.ptr - b.ptr < b.len else b.ptr - a.ptr < a.len;
}
