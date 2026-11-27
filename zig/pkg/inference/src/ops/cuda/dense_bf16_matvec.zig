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

const std = @import("std");
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.dense_bf16_matvec).image;
}, catalog.dense_bf16_matvec_entries);
const threads = 128;
const tile_values = 256;
const tiles_per_block = threads / 32;
const Result = enum { bf16, f32 };
const shortlist_width = 2304;
const shortlist_warps = 8;
const shortlist_threads = shortlist_warps * 32;
const norm_bytes = 16;

const ShortlistPointers = extern struct { low: u64, bounds: u64 };

pub const Shortlist = struct {
    storage: DeviceBuffer = .{},
    rows: u32 = 0,

    pub fn deinit(self: *Shortlist, ctx: *Context) void {
        self.storage.free(ctx);
        self.* = .{};
    }

    fn pointers(self: Shortlist) ShortlistPointers {
        const elements = @as(usize, self.rows) * shortlist_width;
        const base = self.storage.ptr;
        return .{ .low = base, .bounds = base + elements / 2 };
    }

    fn blocks(self: Shortlist) u32 {
        return (self.rows - 1) / shortlist_warps + 1;
    }

    pub fn workspaceBytes(self: Shortlist) usize {
        if (self.rows == 0) return 0;
        return self.partialsOffset() + @as(usize, self.blocks()) * 8;
    }

    fn partialsOffset(self: Shortlist) usize {
        return std.mem.alignForward(usize, shortlist_width + norm_bytes + 8 + @as(usize, self.rows) * 4, 8);
    }
};

const PackedPointers = extern struct {
    low: u64,
    high: u64,
    tiles: u64,
    spill: u64,
};

const InputSegments = extern struct {
    first: u64,
    second: u64,
    third: u64,
    first_end: u32,
    second_end: u32,
};

pub const Packed = struct {
    storage: DeviceBuffer = .{},
    k: u32 = 0,
    n: u32 = 0,
    lanes_per_row: u32 = 32,

    pub fn deinit(self: *Packed, ctx: *Context) void {
        self.storage.free(ctx);
        self.* = .{};
    }

    fn spillOffset(elements: usize) usize {
        return std.mem.alignForward(usize, elements + elements / 2 + elements / tile_values * 4, 16);
    }

    fn pointers(self: Packed, first_row: u32) PackedPointers {
        const elements = @as(usize, self.k) * self.n;
        const offset = @as(usize, self.k) * first_row;
        const base = self.storage.ptr;
        return .{
            .low = base + offset,
            .high = base + elements + offset / 2,
            .tiles = base + elements + elements / 2 + offset / tile_values * 4,
            .spill = base + spillOffset(elements),
        };
    }
};

const PackedSlice = struct { matrix: Packed, first_row: u32 };
const Weight = union(enum) { raw: DeviceBuffer, encoded: PackedSlice };

fn supported(ctx: *Context) bool {
    return ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1;
}

pub const Module = struct {
    program: Program = .{},

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
    }

    /// The packed BF16 matrix remains the source for all final scores.
    pub fn tryPrepareShortlist(self: *Module, ctx: *Context, raw: DeviceBuffer, exact: Packed) !Shortlist {
        if (!supported(ctx) or exact.storage.ptr == 0 or exact.k != shortlist_width or exact.lanes_per_row != 32) return .{};
        const n = exact.n;
        if (n < 1024) return .{};
        const elements = @as(usize, n) * shortlist_width;
        try buffer.validateRegions(1, .{raw}, .{elements * 2}, .{16}, 1);
        var shortlist: Shortlist = .{
            .storage = DeviceBuffer.allocWithPurpose(ctx, elements / 2 + @as(usize, n) * norm_bytes, .model_cache) catch |err| switch (err) {
                error.CudaMemoryLimitExceeded => return .{},
                else => return err,
            },
            .rows = n,
        };
        errdefer shortlist.deinit(ctx);
        var flag = try DeviceBuffer.allocWithPurpose(ctx, 4, .command_workspace);
        defer flag.free(ctx);
        var host = try buffer.HostBuffer.alloc(ctx, 4);
        defer host.free(ctx);
        errdefer ctx.synchronize() catch {};
        @memset(host.bytes(), 0);
        try flag.copyFromHost(ctx, host.constBytes());
        try self.program.init(ctx);
        var raw_address = raw.ptr;
        var weights = shortlist.pointers();
        var args = [_]?*anyopaque{ &raw_address, &weights, &flag.ptr };
        try self.program.launch(ctx, .pack_shortlist, n, 1, threads, &args);
        try flag.copyToHost(ctx, host.bytes());
        try ctx.synchronize();
        if (std.mem.readInt(u32, host.constBytes()[0..4], .little) != 0) shortlist.deinit(ctx);
        return shortlist;
    }

    pub fn runGreedy(self: *Module, ctx: *Context, input: DeviceBuffer, exact: Packed, shortlist: Shortlist, scratch: DeviceBuffer, result: []u8, cap: f32) !void {
        if (!supported(ctx) or exact.k != shortlist_width or exact.lanes_per_row != 32 or shortlist.rows == 0 or
            shortlist.rows != exact.n or result.len != 4 or !std.math.isFinite(cap) or cap < 0x1p-16 or cap > 0x1p16)
            return error.InvalidShape;
        try buffer.validateRegions(4, .{ input, exact.storage, shortlist.storage, scratch }, .{
            shortlist_width * 2, exact.storage.len, shortlist.storage.len, shortlist.workspaceBytes(),
        }, .{ 16, 16, 16, 16 }, 3);
        try self.program.init(ctx);
        var n = shortlist.rows;
        var blocks = shortlist.blocks();
        var refine_blocks = (n - 1) / shortlist_threads + 1;
        var limit = cap;
        var exact_weights = exact.pointers(0);
        var coarse_weights = shortlist.pointers();
        var activation = input.ptr;
        var quantized = scratch.ptr;
        var norms = quantized + shortlist_width;
        var cutoff = norms + norm_bytes;
        var index = cutoff + 4;
        var upper = index + 4;
        var partials = scratch.ptr + shortlist.partialsOffset();
        var quantize_args = [_]?*anyopaque{ &activation, &quantized, &norms };
        try self.program.launch(ctx, .quantize_shortlist, 1, 1, threads, &quantize_args);
        var bound_args = [_]?*anyopaque{ &upper, &partials, &quantized, &norms, &coarse_weights, &n };
        try self.program.launch(ctx, .shortlist_bounds, blocks, 1, shortlist_threads, &bound_args);
        var seed_args = [_]?*anyopaque{ &cutoff, &partials, &blocks, &activation, &exact_weights, &limit };
        try self.program.launch(ctx, .shortlist_seed, 1, 1, shortlist_threads, &seed_args);
        var refine_args = [_]?*anyopaque{ &partials, &activation, &exact_weights, &upper, &cutoff, &n, &limit };
        try self.program.launch(ctx, .shortlist_refine, refine_blocks, 1, shortlist_threads, &refine_args);
        var index_args = [_]?*anyopaque{ &index, &partials, &refine_blocks };
        try self.program.launch(ctx, .greedy_index, 1, 1, shortlist_threads, &index_args);
        const chosen: DeviceBuffer = .{ .ptr = index, .len = 4, .borrowed = true };
        try chosen.copyToHost(ctx, result);
    }

    /// Repack immutable row-major BF16 weights. The source remains available
    /// to cuBLAS and other customers of the raw layout.
    pub fn tryPack(self: *Module, ctx: *Context, raw: DeviceBuffer, k: u32, n: u32, result: Result) !Packed {
        if (!supported(ctx) or (k != 2304 and k != 3072) or n < 1024) return .{};
        var lanes: u32 = if (result == .f32) 32 else if (k == 2304) 8 else 16;
        if (n % (32 / lanes) != 0) return .{};
        const elements = @as(usize, k) * n;
        const raw_bytes = elements * 2;
        if (raw.ptr == 0 or raw.ptr % 16 != 0 or raw.len < raw_bytes) return error.InvalidShape;
        var tiles = std.math.cast(u32, elements / tile_values) orelse return error.InvalidShape;
        const blocks = (tiles - 1) / tiles_per_block + 1;
        var host = try buffer.HostBuffer.alloc(ctx, 4);
        defer host.free(ctx);
        var count = try DeviceBuffer.allocWithPurpose(ctx, 4, .command_workspace);
        defer count.free(ctx);
        errdefer ctx.synchronize() catch {};
        try self.program.init(ctx);
        @memset(host.bytes(), 0);
        try count.copyFromHost(ctx, host.constBytes());
        var pointers = [_]u64{ raw.ptr, count.ptr };
        var count_args = [_]?*anyopaque{ &pointers[0], &tiles, &pointers[1] };
        try self.program.launch(ctx, .count_spills, blocks, 1, threads, &count_args);
        try count.copyToHost(ctx, host.bytes());
        try ctx.synchronize();
        var spill_count = std.mem.readInt(u32, host.constBytes()[0..4], .little);
        if (spill_count > tiles) return error.InvalidCudaState;
        const packed_bytes = Packed.spillOffset(elements) + @as(usize, spill_count) * tile_values * 2;
        if (packed_bytes >= raw_bytes - raw_bytes / 16) return .{};
        var matrix: Packed = .{
            .storage = DeviceBuffer.allocWithPurpose(ctx, packed_bytes, .model_cache) catch |err| switch (err) {
                error.CudaMemoryLimitExceeded => return .{},
                else => return err,
            },
            .k = k,
            .n = n,
            .lanes_per_row = lanes,
        };
        errdefer matrix.deinit(ctx);
        errdefer ctx.synchronize() catch {};
        @memset(host.bytes(), 0);
        try count.copyFromHost(ctx, host.constBytes());
        var encoded = matrix.pointers(0);
        var row_vectors = k / 8;
        var pack_args = [_]?*anyopaque{ &pointers[0], &encoded, &tiles, &pointers[1], &spill_count, &row_vectors, &lanes };
        try self.program.launch(ctx, .pack, blocks, 1, threads, &pack_args);
        try count.copyToHost(ctx, host.bytes());
        try ctx.synchronize();
        if (std.mem.readInt(u32, host.constBytes()[0..4], .little) != spill_count) return error.InvalidCudaState;
        return matrix;
    }

    pub fn tryRun(self: *Module, ctx: *Context, dst: DeviceBuffer, input: DeviceBuffer, weight: DeviceBuffer, k: u32, n: u32, result: Result) !bool {
        return self.run(ctx, dst, input, .{ .raw = weight }, k, n, result);
    }

    pub fn runThreeF32x64(self: *Module, ctx: *Context, dst: DeviceBuffer, input: DeviceBuffer, weights: [3]DeviceBuffer, n: u32) !void {
        if (!supported(ctx) or n == 0) return error.InvalidShape;
        const row_bytes = 64 * 2;
        const weight_bytes = @as(usize, n) * row_bytes;
        try buffer.validateRegions(5, .{ input, weights[0], weights[1], weights[2], dst }, .{
            row_bytes * 3, weight_bytes, weight_bytes, weight_bytes, @as(usize, n) * 12,
        }, .{ 16, 16, 16, 16, 4 }, 4);
        try self.program.init(ctx);
        var addresses = [_]u64{ dst.ptr, input.ptr };
        var weight_ptrs = [_]u64{ weights[0].ptr, weights[1].ptr, weights[2].ptr };
        var count = n;
        const columns = threads / 4;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &weight_ptrs, &count };
        try self.program.launch(ctx, .three_f32_64, (n - 1) / columns + 1, 3, threads, &args);
    }

    pub fn tryRunPacked(self: *Module, ctx: *Context, dst: DeviceBuffer, input: DeviceBuffer, matrix: Packed, first_row: u32, n: u32, result: Result) !bool {
        if (matrix.storage.ptr == 0) return false;
        if (first_row > matrix.n or n > matrix.n - first_row) return error.InvalidShape;
        const expected_lanes: u32 = if (result == .f32) 32 else if (matrix.k == 2304) 8 else 16;
        if (matrix.lanes_per_row != expected_lanes or first_row % (32 / expected_lanes) != 0) return false;
        return self.run(ctx, dst, input, .{ .encoded = .{ .matrix = matrix, .first_row = first_row } }, matrix.k, n, result);
    }

    pub fn tryRunPackedBatch(self: *Module, ctx: *Context, dst: DeviceBuffer, input: DeviceBuffer, matrix: Packed, rows: u32) !bool {
        if (!supported(ctx) or matrix.storage.ptr == 0 or matrix.k != 3072 or matrix.lanes_per_row != 16 or matrix.n < 1024 or rows < 2 or rows > 4)
            return false;
        try buffer.validateRegions(3, .{ input, matrix.storage, dst }, .{
            @as(usize, rows) * matrix.k * 2, matrix.storage.len, @as(usize, rows) * matrix.n * 2,
        }, .{ 16, 16, 2 }, 2);
        try self.program.init(ctx);
        var output_address = dst.ptr;
        var input_address = input.ptr;
        var encoded = matrix.pointers(0);
        var n = matrix.n;
        var count = rows;
        const columns = threads / 16;
        var args = [_]?*anyopaque{ &output_address, &input_address, &encoded, &n, &count };
        try self.program.launch(ctx, .packed_batch_bf16_3072, (n - 1) / columns + 1, 1, threads, &args);
        return true;
    }

    pub fn tryRunSegmented(self: *Module, ctx: *Context, dst: DeviceBuffer, inputs: [3]DeviceBuffer, matrix: Packed, output_widths: [3]u32) !bool {
        if (!supported(ctx) or matrix.storage.ptr == 0 or matrix.k != 2304 or matrix.lanes_per_row != 8) return false;
        const second_end = try std.math.add(u32, output_widths[0], output_widths[1]);
        var n = try std.math.add(u32, second_end, output_widths[2]);
        if (n != matrix.n or output_widths[0] == 0 or output_widths[1] == 0 or output_widths[2] == 0) return error.InvalidShape;
        const input_bytes = @as(usize, matrix.k) * 2;
        try buffer.validateRegions(5, inputs ++ .{ matrix.storage, dst }, .{
            input_bytes, input_bytes, input_bytes, matrix.storage.len, @as(usize, n) * 2,
        }, .{ 16, 16, 16, 16, 2 }, 4);
        try self.program.init(ctx);
        var address = dst.ptr;
        var encoded = matrix.pointers(0);
        var segments: InputSegments = .{
            .first = inputs[0].ptr,
            .second = inputs[1].ptr,
            .third = inputs[2].ptr,
            .first_end = output_widths[0],
            .second_end = second_end,
        };
        const columns = threads / 8;
        var args = [_]?*anyopaque{ &address, &segments, &encoded, &n };
        try self.program.launch(ctx, .segmented_bf16_2304, (n - 1) / columns + 1, 1, threads, &args);
        return true;
    }

    fn run(self: *Module, ctx: *Context, dst: DeviceBuffer, input: DeviceBuffer, weight: Weight, k: u32, n: u32, result: Result) !bool {
        if (!supported(ctx) or n < 1024 or input.ptr % 16 != 0) return false;
        const entry: Program.Entry = switch (result) {
            .f32 => if (k == 64 and weight == .raw) .f32_64 else if (k == 2304 and n >= 32768)
                if (weight == .encoded) .packed_f32_2304 else .f32_2304
            else
                return false,
            .bf16 => switch (k) {
                2304 => if (weight == .encoded) .packed_bf16_2304 else .bf16_2304,
                3072 => if (weight == .encoded) .packed_bf16_3072 else .bf16_3072,
                else => return false,
            },
        };
        const columns: u32 = switch (entry) {
            .f32_2304, .packed_f32_2304 => threads / 32,
            .f32_64 => threads / 4,
            .bf16_2304, .packed_bf16_2304 => threads / 8,
            .bf16_3072, .packed_bf16_3072 => threads / 16,
            else => unreachable,
        };
        const input_bytes = @as(usize, k) * 2;
        const output_bytes: usize = if (result == .bf16) 2 else 4;
        if (dst.ptr % output_bytes != 0) return error.InvalidShape;
        const backing, const bytes = switch (weight) {
            .raw => |raw| .{ raw, @as(usize, n) * input_bytes },
            .encoded => |slice| .{ slice.matrix.storage, slice.matrix.storage.len },
        };
        if (backing.ptr % 16 != 0) return false;
        try buffer.validateRegions(3, .{ input, backing, dst }, .{ input_bytes, bytes, @as(usize, n) * output_bytes }, .{ 16, 16, 2 }, 2);
        try self.program.init(ctx);
        var addresses = [_]u64{ dst.ptr, input.ptr, backing.ptr };
        var encoded: PackedPointers = if (weight == .encoded) weight.encoded.matrix.pointers(weight.encoded.first_row) else undefined;
        const argument: *anyopaque = if (weight == .encoded) &encoded else &addresses[2];
        var count = n;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], argument, &count };
        try self.program.launch(ctx, entry, (n - 1) / columns + 1, 1, threads, &args);
        return true;
    }
};
