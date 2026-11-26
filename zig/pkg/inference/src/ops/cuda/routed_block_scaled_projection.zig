// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Projection for already-quantized, routed activation rows. Packed weights
//! and block scales retain their ordinary expert/column-major row layout.
const std = @import("std");
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.routed_block_scaled_projection).image;
}, catalog.routed_block_scaled_projection_entries);

const Matvec = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.routed_block_scaled_matvec).image;
}, catalog.routed_block_scaled_matvec_entries);

pub const Format = enum { nvfp4, mxfp8 };
pub const ActivationPrecision = enum { block_scaled, bf16 };
const small_grouped_routes = 128;
const medium_grouped_routes = 512;
const wide_grouped_routes = 4096;
const max_grouped_routes = 32_768;
const min_grouped_input_rows = 16;
const min_grouped_bf16_ffn_rows = 8;
const routes_per_group = 8;
const routes_per_wide_group = 32;
const routes_per_large_group = 64;
const route_group_threads = 256;
const wide_route_group_threads = 128;

fn groupBytes(routes: u32) usize {
    return (@as(usize, routes) + 1) * @sizeOf(u32);
}

pub const Request = struct {
    use_pipelined_projection: bool = false,
    format: Format,
    activation: DeviceBuffer,
    activation_block_scale: DeviceBuffer,
    weight: DeviceBuffer,
    weight_block_scale: DeviceBuffer,
    expert_ids: DeviceBuffer,
    /// F32 product of activation and expert outer scales per routed row.
    /// MXFP8 requires only its per-block power-of-two scales.
    row_outer_scale: DeviceBuffer = .{},
    output: DeviceBuffer,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    expert_count: u32,
    interleaved: bool = false,
};

pub const QuantizeRequest = struct {
    format: Format,
    input: DeviceBuffer,
    expert_ids: DeviceBuffer = .{},
    weight_amax: DeviceBuffer = .{},
    weight_denominator: DeviceBuffer = .{},
    activation: DeviceBuffer,
    activation_block_scale: DeviceBuffer,
    row_outer_scale: DeviceBuffer = .{},
    input_rows: u32,
    repeat: u32,
    width: u32,
    expert_count: u32 = 0,
};

pub const PackedWeights = struct {
    values: DeviceBuffer,
    scales: DeviceBuffer,
    amax: DeviceBuffer = .{},
    denominator: DeviceBuffer = .{},
    interleaved: bool = false,
};

pub const Bf16Request = struct {
    format: Format,
    input: DeviceBuffer,
    weights: PackedWeights,
    expert_ids: DeviceBuffer,
    output: DeviceBuffer,
    input_rows: u32,
    repeat: u32,
    in_dim: u32,
    out_dim: u32,
    expert_count: u32,
};

pub const FfnRequest = struct {
    use_pipelined_projection: bool = false,
    format: Format,
    input: DeviceBuffer,
    logits: DeviceBuffer,
    router_bias: DeviceBuffer,
    gate_up: PackedWeights,
    down: PackedWeights,
    output: DeviceBuffer,
    scratch: DeviceBuffer,
    rows: u32,
    hidden: u32,
    intermediate: u32,
    experts: u32,
    topk: u32,
    route_scale: f32,
    gate_cap: f32,
    up_cap: f32,
    activation_precision: ActivationPrecision = .block_scaled,
};

const threads = 128;
const columns_per_block = threads / 32;

fn validate(r: Request) !void {
    const block_values: u32 = if (r.format == .nvfp4) 16 else 32;
    if (r.rows == 0 or r.rows > 65535 or r.in_dim == 0 or r.in_dim % block_values != 0 or
        r.out_dim == 0 or r.expert_count == 0) return error.InvalidShape;
    const elements_per_row: usize = if (r.format == .nvfp4) r.in_dim / 2 else r.in_dim;
    const scales_per_row: usize = r.in_dim / block_values;
    const weight_rows = try std.math.mul(usize, r.expert_count, r.out_dim);
    const output_values = try std.math.mul(usize, r.rows, r.out_dim);
    const lengths = [7]usize{
        try std.math.mul(usize, r.rows, elements_per_row),
        try std.math.mul(usize, r.rows, scales_per_row),
        try std.math.mul(usize, weight_rows, elements_per_row),
        try std.math.mul(usize, weight_rows, scales_per_row),
        try std.math.mul(usize, r.rows, @sizeOf(u32)),
        if (r.format == .nvfp4) try std.math.mul(usize, r.rows, @sizeOf(f32)) else 0,
        try std.math.mul(usize, output_values, @sizeOf(u16)),
    };
    try buffer.validateRegions(7, .{ r.activation, r.activation_block_scale, r.weight, r.weight_block_scale, r.expert_ids, r.row_outer_scale, r.output }, lengths, .{ 1, 1, 1, 1, 4, 4, 2 }, 6);
}

fn sm120(ctx: *const Context) bool {
    return ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1;
}

fn supportsMatvec(ctx: *const Context, r: Request) bool {
    const fp4 = r.format == .nvfp4;
    if (!sm120(ctx) or r.out_dim % 16 != 0 or
        r.in_dim % @as(u32, if (fp4) 64 else 32) != 0) return false;
    if (r.interleaved and r.weight.ptr % 16 != 0) return false;
    const packed_addresses = r.activation.ptr | r.weight.ptr;
    const scales = if (fp4) r.activation_block_scale.ptr | r.weight_block_scale.ptr else 0;
    return (packed_addresses | scales) % @alignOf(u32) == 0;
}

const Span = struct {
    offset: usize,
    len: usize,

    fn allocate(cursor: *usize, len: usize) !Span {
        const offset = (try std.math.add(usize, cursor.*, 15)) & ~@as(usize, 15);
        cursor.* = try std.math.add(usize, offset, len);
        return .{ .offset = offset, .len = len };
    }

    fn view(self: Span, parent: DeviceBuffer) DeviceBuffer {
        if (self.len == 0) return .{};
        return .{ .ptr = parent.ptr + self.offset, .len = self.len, .borrowed = true };
    }
};

const FfnLayout = struct {
    routed_rows: u32,
    ids: Span,
    route_weights: Span,
    group_count: Span,
    groups: Span,
    group_width: u32,
    group_metadata: Span,
    encoded: Span,
    block_scale: Span,
    outer_scale: Span,
    gate_up: Span,
    activated: Span,
    down: Span,
    bytes: usize,

    fn plan(r: FfnRequest) !FfnLayout {
        const fp4 = r.format == .nvfp4;
        const group: u32 = if (fp4) 16 else 32;
        if (r.rows == 0 or r.hidden == 0 or r.intermediate == 0 or
            r.hidden > 65536 or r.intermediate > 65536 or r.hidden % group != 0 or r.intermediate % group != 0 or
            r.experts == 0 or r.experts > 256 or r.topk == 0 or r.topk > @min(8, r.experts))
            return error.InvalidShape;
        const routed_rows = try std.math.mul(u32, r.rows, r.topk);
        if (routed_rows > 65535) return error.InvalidShape;
        const largest = try std.math.mul(usize, routed_rows, @max(r.hidden, r.intermediate));
        const expert_intermediate = try std.math.mul(usize, routed_rows, r.intermediate);
        const expert_hidden = try std.math.mul(usize, routed_rows, r.hidden);
        const group_rows = r.rows >= min_grouped_bf16_ffn_rows and routed_rows <= max_grouped_routes;
        const width: u32 = if (routed_rows <= medium_grouped_routes) routes_per_group else if (routed_rows <= wide_grouped_routes) routes_per_wide_group else routes_per_large_group;
        // The extra bucket collects invalid expert IDs and preserves zero output.
        // Each nonempty bucket contributes at most width - 1 padding slots.
        const buckets = @min(routed_rows, r.experts + 1);
        const max_groups = (routed_rows + (width - 1) * buckets) / width;
        var cursor: usize = 0;
        var result: FfnLayout = .{
            .routed_rows = routed_rows,
            .ids = try Span.allocate(&cursor, @as(usize, routed_rows) * 4),
            .route_weights = try Span.allocate(&cursor, @as(usize, routed_rows) * 4),
            .group_count = try Span.allocate(&cursor, if (group_rows) @sizeOf(u32) else 0),
            .groups = try Span.allocate(&cursor, if (group_rows) @as(usize, max_groups) * groupBytes(width) else 0),
            .group_width = width,
            .group_metadata = try Span.allocate(&cursor, if (group_rows and width == routes_per_large_group) 2 * @as(usize, r.experts + 1) * @sizeOf(u32) else 0),
            .encoded = try Span.allocate(&cursor, if (fp4) largest / 2 else largest),
            .block_scale = try Span.allocate(&cursor, largest / group),
            .outer_scale = try Span.allocate(&cursor, if (fp4) @as(usize, routed_rows) * 4 else 0),
            .gate_up = try Span.allocate(&cursor, try std.math.mul(usize, expert_intermediate, 4)),
            .activated = try Span.allocate(&cursor, try std.math.mul(usize, expert_intermediate, 2)),
            .down = try Span.allocate(&cursor, try std.math.mul(usize, expert_hidden, 2)),
            .bytes = undefined,
        };
        result.bytes = cursor;
        return result;
    }
};

pub fn ffnScratchBytes(r: FfnRequest) !usize {
    return (try FfnLayout.plan(r)).bytes;
}

const GroupedRoutes = struct {
    table: DeviceBuffer = .{},
    count: DeviceBuffer = .{},
    width: u32 = routes_per_group,
};

pub const Module = struct {
    program: Program = .{},
    matvec: Matvec = .{},
    use_single_route: bool = false,

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.matvec.deinit(ctx);
        self.program.deinit(ctx);
    }

    /// Repeat each BF16 token for the selected-route layout. For NVFP4,
    /// publish the combined activation/expert outer scale beside each row.
    pub fn quantize(self: *Module, ctx: *Context, r: QuantizeRequest) !void {
        const fp4 = r.format == .nvfp4;
        const group: u32 = if (fp4) 16 else 32;
        if (r.input_rows == 0 or r.repeat == 0 or r.width == 0 or r.width > 65536 or
            r.width % group != 0 or (fp4 and r.expert_count == 0)) return error.InvalidShape;
        const output_rows = try std.math.mul(u32, r.input_rows, r.repeat);
        if (output_rows > 65535) return error.InvalidShape;
        const input_values = try std.math.mul(usize, r.input_rows, r.width);
        const output_values = try std.math.mul(usize, output_rows, r.width);
        const lengths = [7]usize{
            try std.math.mul(usize, input_values, 2),
            if (fp4) @as(usize, output_rows) * 4 else 0,
            if (fp4) try std.math.mul(usize, r.expert_count, 4) else 0,
            if (fp4) 4 else 0,
            if (fp4) output_values / 2 else output_values,
            output_values / group,
            if (fp4) @as(usize, output_rows) * 4 else 0,
        };
        try buffer.validateRegions(7, .{ r.input, r.expert_ids, r.weight_amax, r.weight_denominator, r.activation, r.activation_block_scale, r.row_outer_scale }, lengths, .{ 2, 4, 4, 4, 1, 1, 4 }, 4);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        var addresses = [7]u64{
            r.activation.ptr,         r.activation_block_scale.ptr, r.row_outer_scale.ptr,
            r.input.ptr,              r.expert_ids.ptr,             r.weight_amax.ptr,
            r.weight_denominator.ptr,
        };
        var width = r.width;
        var repeat = r.repeat;
        var experts = r.expert_count;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &addresses[5], &addresses[6], &width, &repeat, &experts };
        if (fp4 and sm120(ctx)) {
            try self.matvec.init(ctx);
            const share = !self.use_single_route and r.repeat > 1 and r.activation.ptr % @alignOf(u64) == 0;
            try self.matvec.launch(ctx, if (share) .quantize_nvfp4_routes else .quantize_nvfp4, if (share) r.input_rows else output_rows, 1, 256, &args);
        } else if (!fp4 and !self.use_single_route and sm120(ctx) and r.input_rows >= min_grouped_input_rows and r.activation.ptr % @alignOf(u32) == 0) {
            try self.matvec.init(ctx);
            try self.matvec.launch(ctx, .quantize_mxfp8_routes, r.input_rows, 1, threads, &args);
        } else {
            try self.program.init(ctx);
            try self.program.launch(ctx, if (fp4) .quantize_nvfp4 else .quantize_mxfp8, output_rows, 1, 256, &args);
        }
    }

    /// Reorder owned packed checkpoint tensors into contiguous Tensor Core
    /// fragments once at residency time. Both buffers retain their byte size.
    pub fn interleaveWeights(self: *Module, ctx: *Context, weights: *PackedWeights, format: Format, k: u32, n: u32, experts: u32) !void {
        const group: u32 = if (format == .nvfp4) 64 else 32;
        if (weights.interleaved or !sm120(ctx) or k == 0 or k % group != 0 or n == 0 or n % 16 != 0 or experts == 0) return;
        if (weights.values.borrowed or weights.scales.borrowed) return error.InvalidShape;
        const groups = try std.math.mul(u32, n / 16, experts);
        const tile_count: usize = try std.math.mul(usize, groups, k / group);
        var scale_width: u32 = if (format == .nvfp4) 4 else 1;
        const value_bytes = try std.math.mul(usize, tile_count, 512);
        const scale_bytes = try std.math.mul(usize, tile_count, @as(usize, 16) * scale_width);
        if (format == .nvfp4 and weights.scales.ptr % 4 != 0) return error.InvalidShape;
        try buffer.validateRegions(2, .{ weights.values, weights.scales }, .{ value_bytes, scale_bytes }, .{ 4, 1 }, 2);
        var values = try DeviceBuffer.allocWithPurpose(ctx, value_bytes, .model_cache);
        errdefer values.free(ctx);
        var scales = try DeviceBuffer.allocWithPurpose(ctx, scale_bytes, .model_cache);
        errdefer scales.free(ctx);
        try self.matvec.init(ctx);
        var addresses = [_]u64{ values.ptr, scales.ptr, weights.values.ptr, weights.scales.ptr };
        var tiles = k / group;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &tiles, &scale_width };
        try self.matvec.launch(ctx, .pack, groups, 1, 256, &args);
        try ctx.synchronize();
        weights.values.free(ctx);
        weights.scales.free(ctx);
        weights.values = values;
        weights.scales = scales;
        weights.interleaved = true;
    }

    pub fn run(self: *Module, ctx: *Context, r: Request) !void {
        return self.projectBlockScaled(ctx, r, .{});
    }

    fn projectBlockScaled(self: *Module, ctx: *Context, r: Request, grouped: GroupedRoutes) !void {
        try validate(r);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        const routed = if (grouped.table.ptr != 0) grouped.table else r.expert_ids;
        var addresses = [7]u64{
            r.output.ptr,             r.activation.ptr, r.activation_block_scale.ptr, r.weight.ptr,
            r.weight_block_scale.ptr, routed.ptr,       r.row_outer_scale.ptr,
        };
        var k = r.in_dim;
        var n = r.out_dim;
        var experts = r.expert_count;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &addresses[5], &addresses[6], &k, &n, &experts };
        if (r.interleaved) {
            if (!supportsMatvec(ctx, r)) return error.InvalidShape;
            try self.matvec.init(ctx);
            if (grouped.table.ptr == 0) {
                try self.matvec.launch(ctx, if (r.format == .nvfp4) .nvfp4 else .mxfp8, r.out_dim / 16, r.rows, threads, &args);
            } else {
                const fp4 = r.format == .nvfp4;
                const pipelined = r.use_pipelined_projection and fp4 and grouped.width == routes_per_large_group and
                    r.rows >= 8192 and r.out_dim % 64 == 0 and (r.activation.ptr | r.weight.ptr | r.output.ptr) % 16 == 0;
                const more_columns = grouped.width == routes_per_large_group and r.out_dim % 32 == 0;
                const entry: Matvec.Entry = if (pipelined) .nvfp4_routes64_cols64_pipelined else switch (grouped.width) {
                    routes_per_group => if (fp4) .nvfp4_routes8 else .mxfp8_routes8,
                    routes_per_wide_group => if (fp4) .nvfp4_routes32 else .mxfp8_routes32,
                    routes_per_large_group => if (more_columns)
                        (if (fp4) .nvfp4_routes64_cols32 else .mxfp8_routes64_cols32)
                    else
                        (if (fp4) .nvfp4_routes64 else .mxfp8_routes64),
                    else => unreachable,
                };
                var count = grouped.count.ptr;
                var grouped_args = args ++ [_]?*anyopaque{&count};
                const columns: u32 = if (pipelined) 64 else if (more_columns) 32 else 16;
                const block_threads: u32 = if (pipelined) 256 else threads;
                try self.matvec.launch(ctx, entry, r.out_dim / columns, @intCast(grouped.table.len / groupBytes(grouped.width)), block_threads, &grouped_args);
            }
            return;
        }
        try self.program.init(ctx);
        const columns = (r.out_dim - 1) / columns_per_block + 1;
        try self.program.launch(ctx, switch (r.format) {
            .nvfp4 => .nvfp4,
            .mxfp8 => .mxfp8,
        }, columns, r.rows, threads, &args);
    }

    /// Multiply BF16 activations by weights expanded from packed checkpoint
    /// fragments to BF16 at the Tensor Core input, with F32 accumulation.
    pub fn runBf16(self: *Module, ctx: *Context, r: Bf16Request) !void {
        try self.projectBf16(ctx, r, .{});
    }

    fn projectBf16(self: *Module, ctx: *Context, r: Bf16Request, grouped: GroupedRoutes) !void {
        const fp4 = r.format == .nvfp4;
        const tile_width: u32 = if (fp4) 64 else 32;
        const scale_group: u32 = if (fp4) 16 else 32;
        if (!sm120(ctx)) return error.UnsupportedCudaDevice;
        if (!r.weights.interleaved or r.input_rows == 0 or r.repeat == 0 or r.expert_count == 0 or
            r.in_dim == 0 or r.in_dim % tile_width != 0 or r.out_dim == 0 or r.out_dim % 16 != 0)
            return error.InvalidShape;
        const rows = try std.math.mul(u32, r.input_rows, r.repeat);
        if (rows > 65535) return error.InvalidShape;
        const weight_rows = try std.math.mul(usize, r.expert_count, r.out_dim);
        const input_elements = try std.math.mul(usize, r.input_rows, r.in_dim);
        const output_elements = try std.math.mul(usize, rows, r.out_dim);
        const lengths = [7]usize{
            try std.math.mul(usize, input_elements, 2),
            try std.math.mul(usize, weight_rows, if (fp4) r.in_dim / 2 else r.in_dim),
            try std.math.mul(usize, weight_rows, r.in_dim / scale_group),
            @as(usize, rows) * 4,
            if (fp4) try std.math.mul(usize, r.expert_count, 4) else 0,
            if (fp4) 4 else 0,
            try std.math.mul(usize, output_elements, 2),
        };
        try buffer.validateRegions(7, .{ r.input, r.weights.values, r.weights.scales, r.expert_ids, r.weights.amax, r.weights.denominator, r.output }, lengths, .{ 4, 16, 1, 4, 4, 4, 2 }, 6);
        if (fp4 and r.weights.scales.ptr % 4 != 0) return error.InvalidShape;
        try self.matvec.init(ctx);
        const share_weights = grouped.table.ptr != 0;
        const routing = if (share_weights) grouped.table.ptr else r.expert_ids.ptr;
        var ptrs = [_]u64{ r.output.ptr, r.input.ptr, r.weights.values.ptr, r.weights.scales.ptr, routing, r.weights.amax.ptr, r.weights.denominator.ptr };
        var k = r.in_dim;
        var n = r.out_dim;
        var experts = r.expert_count;
        var repeat = r.repeat;
        var count = grouped.count.ptr;
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &ptrs[4], &ptrs[5], &ptrs[6], &k, &n, &experts, &repeat, &count };
        if (share_weights) {
            const entry: Matvec.Entry = switch (grouped.width) {
                routes_per_group => if (fp4) .nvfp4_bf16_grouped else .mxfp8_bf16_grouped,
                routes_per_wide_group => if (fp4) .nvfp4_bf16_routes32 else .mxfp8_bf16_routes32,
                routes_per_large_group => if (fp4) .nvfp4_bf16_routes64 else .mxfp8_bf16_routes64,
                else => unreachable,
            };
            try self.matvec.launch(ctx, entry, r.out_dim / 16, @intCast(grouped.table.len / groupBytes(grouped.width)), threads, &args);
        } else {
            try self.matvec.launch(ctx, if (fp4) .nvfp4_bf16 else .mxfp8_bf16, r.out_dim / 16, rows, threads, args[0..11]);
        }
    }

    fn groupRoutes(self: *Module, ctx: *Context, r: FfnRequest, layout: FfnLayout, ids: DeviceBuffer) !GroupedRoutes {
        if (self.use_single_route or !r.gate_up.interleaved or !r.down.interleaved or layout.groups.len == 0 or
            (r.activation_precision != .bf16 and r.rows < min_grouped_input_rows) or
            ctx.info.compute_major != 12 or ctx.info.compute_minor != 1) return .{};
        const groups: GroupedRoutes = .{ .table = layout.groups.view(r.scratch), .count = layout.group_count.view(r.scratch), .width = layout.group_width };
        try self.matvec.init(ctx);
        var ptrs = [_]u64{ groups.table.ptr, groups.count.ptr, ids.ptr };
        var rows = layout.routed_rows;
        var experts = r.experts;
        if (groups.width == routes_per_large_group) {
            const metadata = layout.group_metadata.view(r.scratch);
            var prefixes = metadata.ptr;
            var cursors = metadata.ptr + metadata.len / 2;
            var count_args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &prefixes, &cursors, &ptrs[2], &rows, &experts };
            try self.matvec.launch(ctx, .group_rows64_count, 1, 1, route_group_threads, &count_args);
            var scatter_args = [_]?*anyopaque{ &ptrs[0], &prefixes, &cursors, &ptrs[2], &rows, &experts };
            try self.matvec.launch(ctx, .group_rows64_scatter, (rows - 1) / route_group_threads + 1, 1, route_group_threads, &scatter_args);
            return groups;
        }
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &rows, &experts };
        const entry: Matvec.Entry = if (rows <= small_grouped_routes)
            .group_rows
        else if (rows <= medium_grouped_routes)
            .group_rows_512
        else
            .group_rows32_4096;
        const group_threads: u32 = if (groups.width == routes_per_wide_group) wide_route_group_threads else route_group_threads;
        try self.matvec.launch(ctx, entry, 1, 1, group_threads, &args);
        return groups;
    }

    fn projectFfn(self: *Module, ctx: *Context, precision: ActivationPrecision, projection: Request, input: QuantizeRequest, weights: PackedWeights, grouped: GroupedRoutes) !void {
        if (precision == .bf16) {
            try self.projectBf16(ctx, .{
                .format = projection.format,
                .input = input.input,
                .weights = weights,
                .expert_ids = projection.expert_ids,
                .output = projection.output,
                .input_rows = input.input_rows,
                .repeat = input.repeat,
                .in_dim = projection.in_dim,
                .out_dim = projection.out_dim,
                .expert_count = projection.expert_count,
            }, grouped);
        } else {
            try self.quantize(ctx, input);
            try self.projectBlockScaled(ctx, projection, grouped);
        }
    }

    /// Routed FFN for checkpoint-native packed weights. Router
    /// logits are supplied in F32 by the caller's BF16 projection.
    pub fn runFfn(self: *Module, ctx: *Context, r: FfnRequest) !void {
        const layout = try FfnLayout.plan(r);
        if (!std.math.isFinite(r.route_scale) or r.route_scale <= 0 or
            !std.math.isFinite(r.gate_cap) or r.gate_cap <= 0 or
            !std.math.isFinite(r.up_cap) or r.up_cap <= 0) return error.InvalidShape;
        const row_bytes = @as(usize, r.rows) * r.hidden * 2;
        const fp4 = r.format == .nvfp4;
        const columns = @as(usize, r.experts) * r.hidden * r.intermediate;
        const scale_group: usize = if (fp4) 16 else 32;
        const amax_bytes: usize = if (fp4) @as(usize, r.experts) * 4 else 0;
        const denominator_bytes: usize = if (fp4) 4 else 0;
        try buffer.validateRegions(13, .{
            r.input,       r.logits,      r.router_bias, r.gate_up.values,   r.gate_up.scales, r.gate_up.amax, r.gate_up.denominator,
            r.down.values, r.down.scales, r.down.amax,   r.down.denominator, r.output,         r.scratch,
        }, .{
            row_bytes,                         @as(usize, r.rows) * r.experts * 4, @as(usize, r.experts) * 4,
            if (fp4) columns else 2 * columns, 2 * columns / scale_group,          amax_bytes,
            denominator_bytes,                 if (fp4) columns / 2 else columns,  columns / scale_group,
            amax_bytes,                        denominator_bytes,                  row_bytes,
            layout.bytes,
        }, .{ 2, 4, 4, 1, 1, 4, 4, 1, 1, 4, 4, 2, 16 }, 11);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        try self.program.init(ctx);

        const ids = layout.ids.view(r.scratch);
        const route_weights = layout.route_weights.view(r.scratch);
        const encoded = layout.encoded.view(r.scratch);
        const block_scale = layout.block_scale.view(r.scratch);
        const outer_scale = layout.outer_scale.view(r.scratch);
        const gate_up = layout.gate_up.view(r.scratch);
        const activated = layout.activated.view(r.scratch);
        const down = layout.down.view(r.scratch);
        var route_ptrs = [4]u64{ ids.ptr, route_weights.ptr, r.logits.ptr, r.router_bias.ptr };
        var experts = r.experts;
        var topk = r.topk;
        var route_scale = r.route_scale;
        var args = [_]?*anyopaque{ &route_ptrs[0], &route_ptrs[1], &route_ptrs[2], &route_ptrs[3], &experts, &topk, &route_scale };
        const route_threads: u32 = if (r.experts == 128 and ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1) 32 else 256;
        try self.program.launch(ctx, .route, r.rows, 1, route_threads, &args);
        const grouped = try self.groupRoutes(ctx, r, layout, ids);

        var quantize_request: QuantizeRequest = .{
            .format = r.format,
            .input = r.input,
            .expert_ids = ids,
            .weight_amax = r.gate_up.amax,
            .weight_denominator = r.gate_up.denominator,
            .activation = encoded,
            .activation_block_scale = block_scale,
            .row_outer_scale = outer_scale,
            .input_rows = r.rows,
            .repeat = r.topk,
            .width = r.hidden,
            .expert_count = r.experts,
        };
        var projection: Request = .{
            .use_pipelined_projection = r.use_pipelined_projection,
            .format = r.format,
            .activation = encoded,
            .activation_block_scale = block_scale,
            .weight = r.gate_up.values,
            .interleaved = r.gate_up.interleaved,
            .weight_block_scale = r.gate_up.scales,
            .expert_ids = ids,
            .row_outer_scale = outer_scale,
            .output = gate_up,
            .rows = layout.routed_rows,
            .in_dim = r.hidden,
            .out_dim = r.intermediate * 2,
            .expert_count = r.experts,
        };
        try self.projectFfn(ctx, r.activation_precision, projection, quantize_request, r.gate_up, grouped);
        var activation_ptrs = [3]u64{ activated.ptr, gate_up.ptr, route_weights.ptr };
        var intermediate = r.intermediate;
        var routed_rows = layout.routed_rows;
        var gate_cap = r.gate_cap;
        var up_cap = r.up_cap;
        const fuse = !self.use_single_route and r.activation_precision == .block_scaled and sm120(ctx) and
            r.rows >= min_grouped_input_rows and r.intermediate == 1152;
        if (fuse) {
            try self.matvec.init(ctx);
            var ptrs = [_]u64{ encoded.ptr, block_scale.ptr, outer_scale.ptr, gate_up.ptr, route_weights.ptr, ids.ptr, r.down.amax.ptr, r.down.denominator.ptr };
            var fused_args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &ptrs[4], &ptrs[5], &ptrs[6], &ptrs[7], &experts, &gate_cap, &up_cap };
            try self.matvec.launch(ctx, if (fp4) .activate_nvfp4_1152 else .activate_mxfp8_1152, layout.routed_rows, 1, if (fp4) 256 else threads, &fused_args);
        } else {
            var activation_args = [_]?*anyopaque{ &activation_ptrs[0], &activation_ptrs[1], &activation_ptrs[2], &intermediate, &routed_rows, &gate_cap, &up_cap };
            try self.program.launch(ctx, .activate, (r.intermediate - 1) / threads + 1, layout.routed_rows, threads, &activation_args);
        }

        quantize_request.input = activated;
        quantize_request.weight_amax = r.down.amax;
        quantize_request.weight_denominator = r.down.denominator;
        quantize_request.input_rows = layout.routed_rows;
        quantize_request.repeat = 1;
        quantize_request.width = r.intermediate;
        projection.interleaved = r.down.interleaved;
        projection.weight = r.down.values;
        projection.weight_block_scale = r.down.scales;
        projection.output = down;
        projection.in_dim = r.intermediate;
        projection.out_dim = r.hidden;
        if (fuse) try self.projectBlockScaled(ctx, projection, grouped) else try self.projectFfn(ctx, r.activation_precision, projection, quantize_request, r.down, grouped);
        var output = r.output.ptr;
        var routed_output = down.ptr;
        var hidden = r.hidden;
        var rows = r.rows;
        var combine_args = [_]?*anyopaque{ &output, &routed_output, &hidden, &rows, &topk };
        try self.program.launch(ctx, .combine, (r.hidden - 1) / threads + 1, r.rows, threads, &combine_args);
    }
};
