// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native launch-boundary tests: no CUDA installation or device is required.
const std = @import("std");
const driver = @import("../driver.zig");
const context = @import("../context.zig");
const Buffer = @import("../buffer.zig").DeviceBuffer;
const Kind = enum { pair, qkv, argmax_last, argmax_rows };
const Case = struct { adapter: []const u8, kernel: []const u8, kind: Kind, values: usize, bytes: usize, columns: usize, threads: usize, packed_input: bool = false, rows_kernel: ?[]const u8 = null };
const cases = [_]Case{
    .{ .adapter = "launchLinearQ8_0ArgmaxTile4F32", .kernel = "linear_q8_0_argmax_stage1_tile4", .kind = .argmax_last, .values = 32, .bytes = 34, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4KArgmaxTile4F32", .kernel = "linear_q4_k_argmax_stage1_tile4", .kind = .argmax_last, .values = 256, .bytes = 144, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ8_0ArgmaxRowsTile4F32", .kernel = "linear_q8_0_argmax_rows_stage1_tile4", .kind = .argmax_rows, .values = 32, .bytes = 34, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0ArgmaxRowsTile4F32", .kernel = "linear_q4_0_argmax_rows_stage1_tile4", .kind = .argmax_rows, .values = 32, .bytes = 18, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0ArgmaxRowsTile16F32", .kernel = "linear_q4_0_argmax_rows_stage1_tile16", .kind = .argmax_rows, .values = 32, .bytes = 18, .columns = 16, .threads = 256 },
    .{ .adapter = "launchLinearQ4KArgmaxRowsTile4F32", .kernel = "linear_q4_k_argmax_rows_stage1_tile4", .kind = .argmax_rows, .values = 256, .bytes = 144, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ6KArgmaxRowsTile4F32", .kernel = "linear_q6_k_argmax_rows_stage1_tile4", .kind = .argmax_rows, .values = 256, .bytes = 210, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ6KArgmaxRowsTile8F32", .kernel = "linear_q6_k_argmax_rows_stage1_tile8", .kind = .argmax_rows, .values = 256, .bytes = 210, .columns = 8, .threads = 256 },
    .{ .adapter = "launchLinearQ6KArgmaxRowsTile16F32", .kernel = "linear_q6_k_argmax_rows_stage1_tile16", .kind = .argmax_rows, .values = 256, .bytes = 210, .columns = 16, .threads = 256 },
    .{ .adapter = "launchLinearQ8_0PairNoBiasTile4F32", .kernel = "linear_q8_0_pair_nobias_f32_tile4", .kind = .pair, .values = 32, .bytes = 34, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0PairNoBiasTile4F32", .kernel = "linear_q4_0_pair_nobias_f32_tile4", .kind = .pair, .values = 32, .bytes = 18, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0PairNoBiasTile4W4F32", .kernel = "linear_q4_0_pair_nobias_f32_tile4_w4", .kind = .pair, .values = 32, .bytes = 18, .columns = 4, .threads = 128 },
    .{ .adapter = "launchLinearQ4_0PairNoBiasQ8_1Tile4F32", .kernel = "linear_q4_0_pair_nobias_q8_1_f32_tile4", .kind = .pair, .values = 32, .bytes = 18, .columns = 4, .threads = 128, .packed_input = true },
    .{ .adapter = "launchLinearQ4_0PairNoBiasQ8_1Tile4W8F32", .kernel = "linear_q4_0_pair_nobias_q8_1_f32_tile4_w8", .kind = .pair, .values = 32, .bytes = 18, .columns = 4, .threads = 256, .packed_input = true },
    .{ .adapter = "launchLinearQ4_0PairNoBiasQ8_1Tile8F32", .kernel = "linear_q4_0_pair_nobias_q8_1_f32_tile8", .kind = .pair, .values = 32, .bytes = 18, .columns = 8, .threads = 128, .packed_input = true },
    .{ .adapter = "launchLinearQ4_0PairNoBiasTile8F32", .kernel = "linear_q4_0_pair_nobias_f32_tile8", .kind = .pair, .values = 32, .bytes = 18, .columns = 8, .threads = 256 },
    .{ .adapter = "launchLinearQ4KPairNoBiasTile4F32", .kernel = "linear_q4_k_pair_nobias_f32_tile4", .kind = .pair, .values = 256, .bytes = 144, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4KQkvNoBiasTiledF32", .kernel = "linear_q4_k_qkv_nobias_f32_tiled", .kind = .qkv, .values = 256, .bytes = 144, .columns = 0, .threads = 256 },
    .{ .adapter = "launchLinearQ8_0QkvNoBiasTile4F32", .kernel = "linear_q8_0_qkv_nobias_f32_tile4", .kind = .qkv, .values = 32, .bytes = 34, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasTile4F32", .kernel = "linear_q4_0_qkv_nobias_f32_tile4", .kind = .qkv, .values = 32, .bytes = 18, .columns = 4, .threads = 256 },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasTile4W4F32", .kernel = "linear_q4_0_qkv_nobias_f32_tile4_w4", .kind = .qkv, .values = 32, .bytes = 18, .columns = 4, .threads = 128 },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasQ8_1Tile4F32", .kernel = "linear_q4_0_qkv_nobias_q8_1_f32_tile4", .kind = .qkv, .values = 32, .bytes = 18, .columns = 4, .threads = 128, .packed_input = true },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasQ8_1Tile8F32", .kernel = "linear_q4_0_qkv_nobias_q8_1_f32_tile8", .kind = .qkv, .values = 32, .bytes = 18, .columns = 8, .threads = 128, .packed_input = true, .rows_kernel = "linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4" },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasQ8_1Tile8W8F32", .kernel = "linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8", .kind = .qkv, .values = 32, .bytes = 18, .columns = 8, .threads = 256, .packed_input = true },
    .{ .adapter = "launchLinearQ4_0QkvNoBiasTile8F32", .kernel = "linear_q4_0_qkv_nobias_f32_tile8", .kind = .qkv, .values = 32, .bytes = 18, .columns = 8, .threads = 256 },
};
const Record = struct { function: usize, grid: [3]u32, block: [3]u32, shared: u32, stream: usize, arguments: [11]u64 = @splat(0) };
const Trace = struct { records: [2]Record = undefined, count: usize = 0, currents: usize = 0, pointer_count: usize, scalar_count: usize };
threadlocal var active: *Trace = undefined;
fn setCurrent(_: driver.CUcontext) callconv(.c) driver.CUresult {
    active.currents += 1;
    return 0;
}
fn launch(function: driver.CUfunction, gx: c_uint, gy: c_uint, gz: c_uint, bx: c_uint, by: c_uint, bz: c_uint, shared: c_uint, stream: driver.CUstream, params: ?[*]?*anyopaque, extra: ?[*]?*anyopaque) callconv(.c) driver.CUresult {
    std.debug.assert(extra == null and active.count < active.records.len);
    const id = @intFromPtr(function.?);
    var record = Record{ .function = id, .grid = .{ gx, gy, gz }, .block = .{ bx, by, bz }, .shared = shared, .stream = @intFromPtr(stream.?) };
    const pointers = if (id >= 9000) 3 else active.pointer_count;
    const scalars = if (id == 9001) 1 else if (id == 9002) 2 else active.scalar_count;
    for (0..pointers + scalars) |i| record.arguments[i] = if (i < pointers) @as(*const u64, @ptrCast(@alignCast(params.?[i].?))).* else @as(*const u32, @ptrCast(@alignCast(params.?[i].?))).*;
    active.records[active.count] = record;
    active.count += 1;
    return 0;
}
const Request = struct {
    rows: usize,
    k: usize = 256,
    n: usize = 17,
    kv: usize = 7,
    suppress: usize = 2,
    a: Buffer,
    b: Buffer,
    c: Buffer,
    input: Buffer,
    wa: Buffer,
    wb: Buffer,
    wc: Buffer,
    partial: Buffer,
    indices: Buffer,
    suppression: Buffer,
};
fn request(comptime c: Case, rows: usize) Request {
    const col_tiles = if (c.columns == 0) 0 else (17 + c.columns - 1) / c.columns;
    const partials = if (c.kind == .argmax_rows) rows * col_tiles else col_tiles;
    return .{
        .rows = rows,
        .a = .{ .ptr = 0x1000, .len = rows * 17 * 4 },
        .b = .{ .ptr = 0x2000, .len = rows * (if (c.kind == .qkv) @as(usize, 7) else 17) * 4 },
        .c = .{ .ptr = 0x3000, .len = rows * 7 * 4 },
        .input = .{ .ptr = 0x4000, .len = rows * (if (c.packed_input) @as(usize, 8 * 36) else 256 * 4) },
        .wa = .{ .ptr = 0x5000, .len = 17 * (256 / c.values) * c.bytes },
        .wb = .{ .ptr = 0x6000, .len = (if (c.kind == .qkv) @as(usize, 7) else 17) * (256 / c.values) * c.bytes },
        .wc = .{ .ptr = 0x7000, .len = 7 * (256 / c.values) * c.bytes },
        .partial = .{ .ptr = 0x8000, .len = partials * 4 },
        .indices = .{ .ptr = 0x9000, .len = partials * 4 },
        .suppression = .{ .ptr = 0xa000, .len = 8 },
    };
}
fn invoke(comptime K: type, comptime Impl: type, comptime c: Case, module: *K, ctx: *context.CudaContext, r: Request) driver.Error!void {
    const fnc = @field(Impl, c.adapter);
    switch (c.kind) {
        .pair => try fnc(module, ctx, r.a, r.b, r.input, r.wa, r.wb, r.rows, r.k, r.n),
        .qkv => try fnc(module, ctx, r.a, r.b, r.c, r.input, r.wa, r.wb, r.wc, r.rows, r.k, r.n, r.kv),
        .argmax_last, .argmax_rows => try fnc(module, ctx, r.a, r.partial, r.indices, r.input, r.wa, r.suppression, r.rows, r.k, r.n, r.suppress),
    }
}
fn checkWire(comptime c: Case, r: Request, trace: Trace, id: usize, row_variant: bool) !void {
    const first = trace.records[0];
    try std.testing.expectEqual(id, first.function);
    try std.testing.expectEqual([3]u32{ @intCast(c.threads), 1, 1 }, first.block);
    try std.testing.expectEqual(@as(u32, 0), first.shared);
    try std.testing.expectEqual(@as(usize, 0xb000), first.stream);
    const tiles = if (c.columns == 0) 0 else (r.n + c.columns - 1) / c.columns;
    var expected: [11]u64 = @splat(0);
    const grid: usize = switch (c.kind) {
        .pair => blk: {
            expected[0..8].* = .{ r.a.ptr, r.b.ptr, r.input.ptr, r.wa.ptr, r.wb.ptr, r.rows, r.k, r.n };
            break :blk r.rows * tiles * 2;
        },
        .qkv => blk: {
            expected = .{ r.a.ptr, r.b.ptr, r.c.ptr, r.input.ptr, r.wa.ptr, r.wb.ptr, r.wc.ptr, r.rows, r.k, r.n, r.kv };
            if (c.columns == 0) break :blk r.rows * (r.n + 2 * r.kv);
            const per_row = tiles + 2 * ((r.kv + c.columns - 1) / c.columns);
            break :blk (if (row_variant) (r.rows + 3) / 4 else r.rows) * per_row;
        },
        .argmax_last, .argmax_rows => blk: {
            expected[0..9].* = .{ r.partial.ptr, r.indices.ptr, r.input.ptr, r.wa.ptr, r.suppression.ptr, r.rows, r.k, r.n, r.suppress };
            break :blk (if (c.kind == .argmax_rows) r.rows else 1) * tiles;
        },
    };
    try std.testing.expectEqual([3]u32{ @intCast(grid), 1, 1 }, first.grid);
    try std.testing.expectEqual(expected, first.arguments);
    if (c.kind == .argmax_last or c.kind == .argmax_rows) {
        const reduce = trace.records[1];
        var want: [11]u64 = @splat(0);
        want[0..3].* = .{ r.a.ptr, r.partial.ptr, r.indices.ptr };
        if (c.kind == .argmax_rows) {
            want[3] = r.rows;
            want[4] = tiles;
        } else want[3] = tiles;
        try std.testing.expectEqual(want, reduce.arguments);
        try std.testing.expectEqual([3]u32{ @intCast(if (c.kind == .argmax_rows) r.rows else 1), 1, 1 }, reduce.grid);
        try std.testing.expectEqual([3]u32{ 256, 1, 1 }, reduce.block);
        try std.testing.expectEqual(@as(u32, 0), reduce.shared);
        try std.testing.expectEqual(@as(usize, 0xb000), reduce.stream);
        try std.testing.expectEqual(@as(usize, if (c.kind == .argmax_rows) 9002 else 9001), reduce.function);
    }
}
pub fn run(comptime K: type, comptime Before: type) !void {
    @setEvalBranchQuota(100000);
    var checked: usize = 0;
    inline for (cases, 0..) |c, index| {
        const is_argmax = c.kind == .argmax_last or c.kind == .argmax_rows;
        for ([_]usize{ 1, 3, 5 }) |rows| for ([_]bool{ false, true }) |row_available| {
            if (c.rows_kernel == null and row_available) continue;
            for (0..20) |mode| {
                var r = request(c, rows);
                var expected_error: ?driver.Error = null;
                switch (mode) {
                    0 => {},
                    1 => {
                        r.rows = 0;
                        if (is_argmax) expected_error = error.InvalidCudaState;
                    },
                    2 => {
                        r.n = 0;
                        r.kv = 0;
                        if (is_argmax) expected_error = error.InvalidCudaState;
                    },
                    3 => {
                        r.k = 0;
                        expected_error = error.InvalidCudaState;
                    },
                    4 => {
                        r.k = 255;
                        expected_error = error.InvalidCudaState;
                    },
                    5...14 => {
                        const fields = .{ "a", "b", "c", "input", "wa", "wb", "wc", "partial", "indices", "suppression" };
                        inline for (fields, 0..) |field, i| if (mode == 5 + i) {
                            const needed = if (is_argmax) (i == 0 or i == 3 or i == 4 or i >= 7) else if (c.kind == .pair) (i == 0 or i == 1 or i == 3 or i == 4 or i == 5) else i <= 6;
                            if (needed) {
                                if (i == 0 and is_argmax) @field(r, field).len = (if (c.kind == .argmax_rows) rows else 1) * 4 - 1 else @field(r, field).len -= 1;
                                expected_error = error.InvalidCudaState;
                            }
                        };
                    },
                    15, 16 => {
                        if (mode == 16) r.k = 0;
                        expected_error = if (c.rows_kernel != null) (if (mode == 16) error.InvalidCudaState else if (row_available and rows > 1) null else error.CudaKernelUnavailable) else error.CudaKernelUnavailable;
                    },
                    17, 18 => {
                        if (!is_argmax) continue;
                        if (mode == 18) r.k = 0;
                        expected_error = error.CudaKernelUnavailable;
                    },
                    19 => {
                        r.rows = @as(usize, std.math.maxInt(u32)) + 1;
                        inline for (.{ "a", "b", "c", "input", "wa", "wb", "wc", "partial", "indices", "suppression" }) |field| @field(r, field).len = std.math.maxInt(usize);
                        expected_error = error.InvalidCudaState;
                    },
                    else => unreachable,
                }
                var previous: Trace = undefined;
                inline for (.{ K, Before }, 0..) |Impl, side| {
                    if (comptime side == 1 and K == Before) continue;
                    var module: K = .{};
                    @field(module, c.kernel) = if (mode == 15 or mode == 16) null else @ptrFromInt(index + 1);
                    if (c.rows_kernel) |field| @field(module, field) = if (row_available) @ptrFromInt(index + 1001) else null;
                    module.argmax_reduce_pairs_f32 = if (mode == 17 or mode == 18) null else @ptrFromInt(9001);
                    module.argmax_reduce_rows_pairs_f32 = if (mode == 17 or mode == 18) null else @ptrFromInt(9002);
                    var trace = Trace{ .pointer_count = if (c.kind == .qkv) 7 else 5, .scalar_count = if (c.kind == .pair) 3 else 4 };
                    active = &trace;
                    var ctx: context.CudaContext = undefined;
                    ctx.ctx = @ptrFromInt(0xc000);
                    ctx.stream = @ptrFromInt(0xb000);
                    ctx.stats = .{};
                    ctx.driver.fns.cuCtxSetCurrent = setCurrent;
                    ctx.driver.fns.cuLaunchKernel = launch;
                    const actual_error: ?driver.Error = if (invoke(K, Impl, c, &module, &ctx, r)) |_| null else |err| err;
                    if (actual_error != expected_error) std.debug.print("CUDA projection case={s} rows={d} optional={} mode={d} expected={any} actual={any}\n", .{ c.adapter, rows, row_available, mode, expected_error, actual_error });
                    try std.testing.expectEqual(expected_error, actual_error);
                    const wanted_count: usize = if (expected_error != null or r.rows == 0 or r.n == 0) 0 else if (is_argmax) 2 else 1;
                    try std.testing.expectEqual(wanted_count, trace.count);
                    try std.testing.expectEqual(wanted_count, trace.currents);
                    try std.testing.expectEqual(wanted_count, ctx.stats.kernel_launches);
                    if (wanted_count != 0) try checkWire(c, r, trace, if (c.rows_kernel != null and row_available and rows > 1) index + 1001 else index + 1, c.rows_kernel != null and row_available and rows > 1);
                    if (comptime side == 0) previous = trace else {
                        try std.testing.expectEqual(previous.count, trace.count);
                        try std.testing.expectEqualDeep(previous.records[0..previous.count], trace.records[0..trace.count]);
                    }
                }
                checked += 1;
            }
        };
    }
    std.debug.print("CUDA projection native launch contracts: {d} cases, 25 adapters, original comparison={}\n", .{ checked, K != Before });
}

// GPU fixtures use uniform, independently encoded +1 ternary/integer codes.
// At K=256 their exact dot product is sign*(row+1)*(column+1)*plane.
const Fixture = struct {
    r: Request,
    owners: [10]Buffer = @splat(.{}),
    weights: [3]?[]u8 = @splat(null),
    input: ?[]u8 = null,
    excluded: [2]i32 = .{ 0, 0 },

    fn init(comptime c: Case, ctx: *context.CudaContext, rows: usize, k: usize, n: usize, kv: usize) !Fixture {
        var self = Fixture{ .r = request(c, rows) };
        errdefer {
            ctx.synchronize() catch {};
            self.deinit(ctx);
        }
        self.r.k = k;
        self.r.n = n;
        self.r.kv = kv;
        const output_bytes = (rows * n + 4) * 4;
        for (0..3) |plane| self.owners[plane] = try Buffer.alloc(ctx, output_bytes);
        self.owners[3] = try Buffer.alloc(ctx, rows * (if (c.packed_input) (k / 32) * 36 else k * 4));
        for (0..3) |plane| {
            const columns = if (c.kind == .qkv and plane != 0) kv else n;
            const bytes = try std.testing.allocator.alloc(u8, columns * (k / c.values) * c.bytes);
            self.weights[plane] = bytes;
            @memset(bytes, 0);
            for (0..columns) |column| for (0..k / c.values) |block| {
                const off = (column * (k / c.values) + block) * c.bytes;
                const scale: f16 = @floatCast(@as(f32, @floatFromInt((column + 1) * (plane + 1))) / 32);
                if (c.bytes == 210) {
                    @memset(bytes[off..][0..128], 0x11);
                    @memset(bytes[off + 128 ..][0..64], 0xaa);
                    @memset(bytes[off + 192 ..][0..16], 1);
                    std.mem.writeInt(u16, bytes[off + 208 ..][0..2], @bitCast(scale), .little);
                } else {
                    std.mem.writeInt(u16, bytes[off..][0..2], @bitCast(scale), .little);
                    if (c.bytes == 18) @memset(bytes[off + 2 ..][0..16], 0x99);
                    if (c.bytes == 34) @memset(bytes[off + 2 ..][0..32], 1);
                    if (c.bytes == 144) {
                        @memset(bytes[off + 4 ..][0..12], 1);
                        @memset(bytes[off + 16 ..][0..128], 0x11);
                    }
                }
            };
            self.owners[4 + plane] = try Buffer.alloc(ctx, bytes.len);
            try self.owners[4 + plane].copyFromHost(ctx, bytes);
        }
        const tiles = (n + (if (c.columns == 0) @as(usize, 1) else c.columns) - 1) / (if (c.columns == 0) @as(usize, 1) else c.columns);
        for (7..9) |i| self.owners[i] = try Buffer.alloc(ctx, rows * tiles * 4);
        self.owners[9] = try Buffer.alloc(ctx, 8);
        self.input = try std.testing.allocator.alloc(u8, self.owners[3].len);
        const a_count = if (c.kind == .argmax_last) 1 else if (c.kind == .argmax_rows) rows else rows * n;
        self.r.a = .{ .ptr = self.owners[0].ptr + 8, .len = a_count * 4 };
        self.r.b = .{ .ptr = self.owners[1].ptr + 8, .len = rows * (if (c.kind == .qkv) kv else n) * 4 };
        self.r.c = .{ .ptr = self.owners[2].ptr + 8, .len = rows * kv * 4 };
        self.r.input = self.owners[3];
        self.r.wa = self.owners[4];
        self.r.wb = self.owners[5];
        self.r.wc = self.owners[6];
        self.r.partial = self.owners[7];
        self.r.indices = self.owners[8];
        self.r.suppression = self.owners[9];
        try self.update(c, ctx, 0);
        try ctx.synchronize();
        return self;
    }
    fn update(self: *Fixture, comptime c: Case, ctx: *context.CudaContext, generation: usize) !void {
        const bytes = self.input.?;
        for (0..self.r.rows) |row| {
            const negative = (row + generation) % 2 != 0;
            const scale: f32 = @as(f32, @floatFromInt(row + 1)) / 8;
            if (c.packed_input) {
                for (0..self.r.k / 32) |block| {
                    const off = (row * (self.r.k / 32) + block) * 36;
                    std.mem.writeInt(u16, bytes[off..][0..2], @bitCast(@as(f16, @floatCast(scale))), .little);
                    std.mem.writeInt(u16, bytes[off + 2 ..][0..2], @bitCast(@as(f16, @floatCast(scale * 32 * (if (negative) @as(f32, -1) else 1)))), .little);
                    @memset(bytes[off + 4 ..][0..32], if (negative) 0xff else 1);
                }
            } else for (0..self.r.k) |column| std.mem.writeInt(u32, bytes[(row * self.r.k + column) * 4 ..][0..4], @bitCast(scale * (if (negative) @as(f32, -1) else 1)), .little);
        }
        self.excluded = if (generation % 2 == 0) .{ 0, @intCast(self.r.n - 1) } else .{ 1, @intCast(self.r.n - 2) };
        try self.r.input.copyFromHost(ctx, bytes);
        try self.r.suppression.copyFromHost(ctx, std.mem.sliceAsBytes(&self.excluded));
    }
    fn reset(self: *Fixture, comptime K: type, module: *K, ctx: *context.CudaContext) !void {
        for (self.owners[0..3]) |owner| try module.launchFillF32(ctx, owner, owner.len / 4, -777);
    }
    fn check(self: *Fixture, comptime c: Case, ctx: *context.CudaContext, generation: usize) !void {
        const all = try std.testing.allocator.alloc(f32, 3 * (self.r.rows * self.r.n + 4));
        defer std.testing.allocator.free(all);
        const capacity = self.r.rows * self.r.n + 4;
        for (0..3) |plane| try self.owners[plane].copyToHost(ctx, std.mem.sliceAsBytes(all[plane * capacity ..][0..capacity]));
        try ctx.synchronize();
        for (0..3) |plane| {
            const columns = if (c.kind == .qkv and plane != 0) self.r.kv else self.r.n;
            const argmax = c.kind == .argmax_last or c.kind == .argmax_rows;
            const used: usize = if (argmax) (if (plane != 0) 0 else if (c.kind == .argmax_rows) self.r.rows else 1) else if (plane == 0 or c.kind == .qkv or (c.kind == .pair and plane == 1)) self.r.rows * columns else 0;
            for (all[plane * capacity ..][0..capacity], 0..) |actual, i| {
                if (i < 2 or i >= used + 2) {
                    try std.testing.expectEqual(@as(f32, -777), actual);
                    continue;
                }
                if (argmax) {
                    const row = if (c.kind == .argmax_last) self.r.rows - 1 else i - 2;
                    const negative = (row + generation) % 2 != 0;
                    var winner: usize = if (negative) 0 else self.r.n - 1;
                    while (winner == @as(usize, @intCast(self.excluded[0])) or winner == @as(usize, @intCast(self.excluded[1]))) {
                        if (negative) winner += 1 else winner -= 1;
                    }
                    try std.testing.expectEqual(@as(u32, @intCast(winner)), @as(u32, @bitCast(actual)));
                } else {
                    const row = (i - 2) / columns;
                    const column = (i - 2) % columns;
                    const expected = @as(f32, @floatFromInt((row + 1) * (column + 1) * (plane + 1))) * (if ((row + generation) % 2 != 0) @as(f32, -1) else 1);
                    try std.testing.expectEqual(expected, actual);
                }
            }
        }
    }
    fn checkReadOnly(self: *Fixture, ctx: *context.CudaContext) !void {
        for (0..5) |i| {
            const expected = if (i == 0) self.input.? else if (i == 4) std.mem.sliceAsBytes(&self.excluded) else self.weights[i - 1].?;
            const source = if (i == 0) self.r.input else if (i == 4) self.r.suppression else self.owners[i + 3];
            const actual = try std.testing.allocator.alloc(u8, expected.len);
            defer std.testing.allocator.free(actual);
            try source.copyToHost(ctx, actual);
            try ctx.synchronize();
            try std.testing.expectEqualSlices(u8, expected, actual);
        }
    }
    fn deinit(self: *Fixture, ctx: *context.CudaContext) void {
        for (&self.owners) |*owner| owner.free(ctx);
        for (self.weights) |bytes| if (bytes) |raw| std.testing.allocator.free(raw);
        if (self.input) |raw| std.testing.allocator.free(raw);
    }
};

pub fn device(comptime K: type, comptime Before: type) !void {
    @setEvalBranchQuota(100000);
    var ctx = try context.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try K.load(&ctx);
    defer module.unload(&ctx);
    const graph_mod = @import("../graph.zig");
    var checked: usize = 0;
    inline for (cases) |c| {
        const row_kernel = if (c.rows_kernel) |field| @field(module, field) else null;
        defer if (c.rows_kernel) |field| {
            @field(module, field) = row_kernel;
        };
        for ([_]bool{ false, true }) |fallback| {
            if (fallback and c.rows_kernel == null) continue;
            if (c.rows_kernel) |field| @field(module, field) = if (fallback) null else row_kernel;
            for ([_]usize{ 17, 32 }) |n| for ([_]usize{ 1, 2, 5 }) |rows| {
                errdefer std.debug.print("CUDA projection device case={s} rows={d} n={d} fallback={}\n", .{ c.adapter, rows, n, fallback });
                var fixture = try Fixture.init(c, &ctx, rows, 256, n, if (n == 17) 7 else 16);
                defer fixture.deinit(&ctx);
                inline for (.{ Before, K }, 0..) |Impl, side| {
                    if (comptime side == 0 and Before == K) continue;
                    try fixture.reset(K, &module, &ctx);
                    const count = ctx.stats.kernel_launches;
                    try invoke(K, Impl, c, &module, &ctx, fixture.r);
                    try std.testing.expectEqual(@as(usize, if (c.kind == .argmax_last or c.kind == .argmax_rows) 2 else 1), ctx.stats.kernel_launches - count);
                    try fixture.check(c, &ctx, 0);
                }
                try graph_mod.beginCapture(&ctx);
                var capturing = true;
                errdefer if (capturing) {
                    const abandoned = ctx.endStreamCapture() catch null;
                    if (abandoned != null) ctx.destroyGraph(abandoned);
                };
                try invoke(K, K, c, &module, &ctx, fixture.r);
                var graph = try graph_mod.endCapture(&ctx);
                capturing = false;
                defer graph.deinit(&ctx);
                for (1..3) |generation| {
                    try fixture.update(c, &ctx, generation);
                    try fixture.reset(K, &module, &ctx);
                    try graph.launch(&ctx);
                    try fixture.check(c, &ctx, generation);
                }
                try fixture.checkReadOnly(&ctx);
                checked += 1;
            };
        }
    }
    std.debug.print("CUDA projection device oracle/guards/read-only/replay: {d} profiles; two mutable-operand graph replays each; original comparison={}\n", .{ checked, Before != K });
}

fn duration(comptime K: type, comptime Impl: type, comptime c: Case, module: *K, ctx: *context.CudaContext, r: Request) !u64 {
    const io = @import("../../../io/compat.zig").testingIo();
    try ctx.synchronize();
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    for (0..32) |_| try invoke(K, Impl, c, module, ctx, r);
    const elapsed = std.Io.Clock.awake.now(io).nanoseconds - start;
    try ctx.synchronize();
    return @intCast(elapsed);
}

// Qualification-only caller supplies the frozen original implementations.
// Measure host submission, with synchronization outside each measured arm.
pub fn benchmark(comptime K: type, comptime Before: type) !void {
    @setEvalBranchQuota(100000);
    if (!@import("antfly_platform").env.getenvBoolDefault("ANTFLY_CUDA_PROJECTION_BENCH", false)) return;
    comptime std.debug.assert(K != Before);
    var ctx = try context.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try K.load(&ctx);
    defer module.unload(&ctx);
    inline for (cases, 0..) |c, index| for ([_]usize{ 1, 8 }) |rows| {
        var fixture = try Fixture.init(c, &ctx, rows, 1536, 1025, 257);
        defer fixture.deinit(&ctx);
        for (0..3) |order| {
            for (0..8) |_| {
                _ = try duration(K, Before, c, &module, &ctx, fixture.r);
                _ = try duration(K, K, c, &module, &ctx, fixture.r);
            }
            for (0..64) |sample| {
                const first = if (order == 2) try duration(K, K, c, &module, &ctx, fixture.r) else try duration(K, Before, c, &module, &ctx, fixture.r);
                const second = if (order == 1) try duration(K, K, c, &module, &ctx, fixture.r) else try duration(K, Before, c, &module, &ctx, fixture.r);
                std.debug.print("CUDA_PROJECTION_TIMING {d} {d} {d} {d} {d} {d}\n", .{ index, rows, order, sample, first, second });
            }
        }
    };
}
