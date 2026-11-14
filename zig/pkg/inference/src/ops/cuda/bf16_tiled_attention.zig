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

//! Caller-owned tiled BF16 attention. Physical source rows remain present in
//! Q/K/V and output; only keys outside the logical prefix are masked. Every
//! workspace region belongs to the admitted invocation/arena, not a lazy pool.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const Blas = @import("cublaslt.zig").CublasLt;
const Module = @import("embedding_gemma2.zig").Module;
const SourceRows = @import("../kernel_geometry.zig").SourceGemmRows;

pub const tile_rows: usize = 1024;

pub const Region = struct {
    offset: usize,
    bytes: usize,
};

pub const WorkspaceLayout = struct {
    scores: Region,
    probabilities: Region,
    library: Region,
    bytes: usize,

    fn append(cursor: *usize, bytes: usize) !Region {
        const offset = try aligned(cursor.*);
        cursor.* = try std.math.add(usize, offset, bytes);
        return .{ .offset = offset, .bytes = bytes };
    }

    pub fn resolve(sequence: usize, heads: usize, library_bytes: usize) !WorkspaceLayout {
        if (sequence == 0 or sequence > 8192 or heads == 0 or heads > 64) return error.InvalidShape;
        const scores = try std.math.mul(usize, try std.math.mul(usize, heads, tile_rows), sequence);
        var cursor: usize = 0;
        const score_region = try append(&cursor, try std.math.mul(usize, scores, @sizeOf(f32)));
        const probability_region = try append(&cursor, try std.math.mul(usize, scores, @sizeOf(u16)));
        const library_region = try append(&cursor, library_bytes);
        return .{ .scores = score_region, .probabilities = probability_region, .library = library_region, .bytes = try aligned(cursor) };
    }
};

fn aligned(bytes: usize) !usize {
    return (try std.math.add(usize, bytes, 255)) & ~@as(usize, 255);
}

fn region(buffer: Buffer, span: Region) Buffer {
    if (span.bytes == 0) return .{};
    return .{ .ptr = buffer.ptr + span.offset, .len = span.bytes };
}

fn checkedBuffer(buffer: Buffer, bytes: usize, alignment: usize) !void {
    if (bytes == 0 or buffer.ptr == 0 or buffer.ptr % alignment != 0 or buffer.len < bytes) return error.InvalidShape;
    _ = try std.math.add(usize, buffer.ptr, bytes);
}

fn disjoint(a: Buffer, a_bytes: usize, b: Buffer, b_bytes: usize) !void {
    if (a_bytes == 0 or b_bytes == 0) return;
    const a_end = try std.math.add(usize, a.ptr, a_bytes);
    const b_end = try std.math.add(usize, b.ptr, b_bytes);
    if (a.ptr < b_end and b.ptr < a_end) return error.InvalidShape;
}

pub const Plan = struct {
    batch: usize,
    source: SourceRows,
    heads: usize,
    head_dim: usize,
    hidden: usize,
    image_bytes: usize,
    tensor_bytes: usize,
    score_stride: usize,
    workspace: WorkspaceLayout,

    pub fn resolve(batch: usize, source: SourceRows, heads: usize, head_dim: usize, library_bytes: usize) !Plan {
        try source.validate();
        if (batch == 0 or head_dim != 64) return error.InvalidShape;
        const workspace = try WorkspaceLayout.resolve(source.physical, heads, library_bytes);
        const hidden = try std.math.mul(usize, heads, head_dim);
        const image_bytes = try std.math.mul(usize, try std.math.mul(usize, source.physical, hidden), @sizeOf(u16));
        return .{ .batch = batch, .source = source, .heads = heads, .head_dim = head_dim, .hidden = hidden, .image_bytes = image_bytes, .tensor_bytes = try std.math.mul(usize, batch, image_bytes), .score_stride = try std.math.mul(usize, tile_rows, source.physical), .workspace = workspace };
    }

    fn validate(self: Plan, output: Buffer, q: Buffer, k: Buffer, v: Buffer, scratch: Buffer) !void {
        // Re-resolve public value metadata before any context/library access.
        const expected = try resolve(self.batch, self.source, self.heads, self.head_dim, self.workspace.library.bytes);
        if (!std.meta.eql(self, expected)) return error.InvalidShape;
        try checkedBuffer(output, self.tensor_bytes, 2);
        try checkedBuffer(q, self.tensor_bytes, 2);
        try checkedBuffer(k, self.tensor_bytes, 2);
        try checkedBuffer(v, self.tensor_bytes, 2);
        try checkedBuffer(scratch, self.workspace.bytes, 256);
        inline for (.{ q, k, v }) |input| {
            try disjoint(output, self.tensor_bytes, input, self.tensor_bytes);
            try disjoint(scratch, self.workspace.bytes, input, self.tensor_bytes);
        }
        try disjoint(output, self.tensor_bytes, scratch, self.workspace.bytes);
    }

    /// Metadata-only eligibility: false means ordinary warm execution must
    /// populate the exact immutable full/tail plans before graph capture.
    pub fn captureReady(self: Plan, ctx: *Context, blas: *Blas, output: Buffer, q: Buffer, k: Buffer, v: Buffer, scratch: Buffer) !bool {
        try self.validate(output, q, k, v, scratch);
        const scores = region(scratch, self.workspace.scores);
        const probabilities = region(scratch, self.workspace.probabilities);
        const library = region(scratch, self.workspace.library);
        // Plan alignment is batch/address-specific, so include every image.
        for (0..self.batch) |image| {
            const image_offset = image * self.image_bytes;
            const qi: Buffer = .{ .ptr = q.ptr + image_offset, .len = self.image_bytes };
            const ki: Buffer = .{ .ptr = k.ptr + image_offset, .len = self.image_bytes };
            const vi: Buffer = .{ .ptr = v.ptr + image_offset, .len = self.image_bytes };
            const oi: Buffer = .{ .ptr = output.ptr + image_offset, .len = self.image_bytes };
            var query: usize = 0;
            while (query < self.source.physical) : (query += tile_rows) {
                const rows = @min(tile_rows, self.source.physical - query);
                const offset = query * self.hidden * @sizeOf(u16);
                const q_view: Buffer = .{ .ptr = qi.ptr + offset, .len = qi.len - offset };
                const out_view: Buffer = .{ .ptr = oi.ptr + offset, .len = oi.len - offset };
                if (!try blas.hasBf16AttentionPlan(ctx, scores, q_view, ki, library, rows, self.source.physical, self.head_dim, self.hidden, self.hidden, self.source.physical, self.heads, self.head_dim, self.head_dim, self.score_stride, false, true, true)) return false;
                if (!try blas.hasBf16AttentionPlan(ctx, out_view, probabilities, vi, library, rows, self.head_dim, self.source.physical, self.source.physical, self.hidden, self.hidden, self.heads, self.score_stride, self.head_dim, self.head_dim, false, false, false)) return false;
            }
        }
        return true;
    }

    /// No device/host allocations in this body. Outside capture the existing
    /// bounded library cache may learn immutable plans. During capture every
    /// exact full/tail QK and PV key is checked before the first submission.
    pub fn run(self: Plan, ctx: *Context, blas: *Blas, module: *Module, output: Buffer, q: Buffer, k: Buffer, v: Buffer, scratch: Buffer) !void {
        try self.validate(output, q, k, v, scratch);
        const scores = region(scratch, self.workspace.scores);
        const probabilities = region(scratch, self.workspace.probabilities);
        const library = region(scratch, self.workspace.library);
        if (try ctx.streamCaptureActive()) {
            if (!try self.captureReady(ctx, blas, output, q, k, v, scratch)) return error.CublasLtUnsupported;
        }
        for (0..self.batch) |image| {
            const image_offset = image * self.image_bytes;
            const qi: Buffer = .{ .ptr = q.ptr + image_offset, .len = self.image_bytes };
            const ki: Buffer = .{ .ptr = k.ptr + image_offset, .len = self.image_bytes };
            const vi: Buffer = .{ .ptr = v.ptr + image_offset, .len = self.image_bytes };
            const oi: Buffer = .{ .ptr = output.ptr + image_offset, .len = self.image_bytes };
            var query: usize = 0;
            while (query < self.source.physical) : (query += tile_rows) {
                const rows = @min(tile_rows, self.source.physical - query);
                const offset = query * self.hidden * @sizeOf(u16);
                const q_view: Buffer = .{ .ptr = qi.ptr + offset, .len = qi.len - offset };
                const out_view: Buffer = .{ .ptr = oi.ptr + offset, .len = oi.len - offset };
                // Q is already multiplied by sqrt(64), matching the retained
                // old vendor route; alpha restores source scale-one logits.
                try blas.matmulEmbeddingGemma2Attention(ctx, scores, q_view, ki, library, rows, self.source.physical, self.head_dim, self.hidden, self.hidden, self.source.physical, self.heads, self.head_dim, self.head_dim, self.score_stride, false, true, true, 1.0 / 8.0);
                try module.launchSourceAttentionSoftmax(ctx, probabilities, scores, @intCast(self.source.physical), @intCast(self.source.logical), @intCast(query), @intCast(rows), @intCast(self.heads), @intCast(tile_rows));
                try blas.matmulEmbeddingGemma2Attention(ctx, out_view, probabilities, vi, library, rows, self.head_dim, self.source.physical, self.source.physical, self.hidden, self.hidden, self.heads, self.score_stride, self.head_dim, self.head_dim, false, false, false, 1.0);
            }
        }
    }
};

test "tiled BF16 attention sizes full source rows and reuses workspace across batch" {
    const one = try Plan.resolve(1, .{ .logical = 2340, .physical = 2520 }, 12, 64, 0);
    const eight = try Plan.resolve(8, .{ .logical = 2340, .physical = 2520 }, 12, 64, 0);
    try std.testing.expectEqual(@as(usize, 185794560), one.workspace.bytes);
    try std.testing.expectEqual(one.workspace.bytes, eight.workspace.bytes);
    try std.testing.expectEqual(one.tensor_bytes * 8, eight.tensor_bytes);
    try std.testing.expectEqual(@as(usize, 3870720), one.image_bytes);
    const with_library = try Plan.resolve(1, one.source, 12, 64, 32 * 1024 * 1024);
    try std.testing.expectEqual(one.workspace.bytes + 32 * 1024 * 1024, with_library.workspace.bytes);
    try std.testing.expectEqual(@as(usize, 0), with_library.workspace.library.offset % 256);
    try std.testing.expectError(error.InvalidShape, Plan.resolve(1, .{ .logical = 2521, .physical = 2520 }, 12, 64, 0));
    try std.testing.expectError(error.InvalidShape, Plan.resolve(1, .{ .logical = 1, .physical = 8193 }, 12, 64, 0));
    try std.testing.expectError(error.InvalidShape, Plan.resolve(1, .{ .logical = 1, .physical = 1 }, 65, 64, 0));
    try std.testing.expectError(error.InvalidShape, Plan.resolve(1, .{ .logical = 1, .physical = 1 }, 12, 32, 0));
    try std.testing.expectError(error.Overflow, WorkspaceLayout.resolve(3136, 12, std.math.maxInt(usize)));
}

test "tiled BF16 attention rejects malformed buffers before context access" {
    const plan = try Plan.resolve(1, .{ .logical = 1, .physical = 2 }, 1, 64, 0);
    const q: Buffer = .{ .ptr = 0x1000, .len = plan.tensor_bytes };
    const k: Buffer = .{ .ptr = 0x2000, .len = plan.tensor_bytes };
    const v: Buffer = .{ .ptr = 0x3000, .len = plan.tensor_bytes };
    const out: Buffer = .{ .ptr = 0x4000, .len = plan.tensor_bytes };
    const scratch: Buffer = .{ .ptr = 0x10000, .len = plan.workspace.bytes };
    try plan.validate(out, q, k, v, scratch);
    try std.testing.expectError(error.InvalidShape, plan.validate(q, q, k, v, scratch));
    var malformed = plan;
    malformed.score_stride += 1;
    try std.testing.expectError(error.InvalidShape, malformed.run(undefined, undefined, undefined, out, q, k, v, scratch));
    var short = q;
    short.len -= 1;
    try std.testing.expectError(error.InvalidShape, plan.run(undefined, undefined, undefined, out, short, k, v, scratch));
}
