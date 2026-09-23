// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Laya-only CPU contractions. Preserve the portable oracle's FP64
//! accumulation while using system BLAS for concrete matrix/batched products.
const std = @import("std");
const options = @import("build_options");
const ops = @import("../../ops/ops.zig");
const native = @import("../../ops/native_compute.zig");
const CT = ops.CT;

extern "c" fn cblas_dgemm(layout: c_int, transa: c_int, transb: c_int, m: c_int, n: c_int, k: c_int, alpha: f64, a: [*]const f64, lda: c_int, b: [*]const f64, ldb: c_int, beta: f64, c_out: [*]f64, ldc: c_int) void;

/// Storage must outlive every use of cb. Other backends keep their vtable.
pub fn install(cb: *ops.ComputeBackend, storage: *ops.ComputeBackend.VTable) void {
    if (comptime !options.enable_system_blas) return;
    if (cb.kind() != .native) return;
    storage.* = cb.vtable.*;
    storage.dotGeneralOp = dot;
    cb.vtable = storage;
}

fn dot(ctx: *anyopaque, lhs: CT, rhs: CT, ls: []const i64, rs: []const i64, lc_axes: []const u8, rc_axes: []const u8, lb: []const u8, rb: []const u8) anyerror!CT {
    const compute: *native.NativeCompute = @ptrCast(@alignCast(ctx));
    const cb = compute.computeBackend(); // The original portable vtable.
    const eligible = blk: {
        if (comptime !options.enable_system_blas) break :blk false;
        if (ls.len != rs.len or (ls.len != 2 and ls.len != 3) or lc_axes.len != 1 or rc_axes.len != 1) break :blk false;
        for (ls) |dim| if (dim <= 0 or dim > std.math.maxInt(c_int)) break :blk false;
        for (rs) |dim| if (dim <= 0 or dim > std.math.maxInt(c_int)) break :blk false;
        if (ls.len == 2) {
            if (lb.len != 0 or rb.len != 0 or lc_axes[0] > 1 or rc_axes[0] > 1) break :blk false;
        } else {
            if (!std.mem.eql(u8, lb, &.{0}) or !std.mem.eql(u8, rb, &.{0}) or ls[0] != rs[0] or
                lc_axes[0] < 1 or lc_axes[0] > 2 or rc_axes[0] < 1 or rc_axes[0] > 2) break :blk false;
        }
        break :blk ls[lc_axes[0]] == rs[rc_axes[0]];
    };
    if (!eligible) return cb.primDotGeneral(lhs, rhs, ls, rs, lc_axes, rc_axes, lb, rb);
    const a = compute.allocator;
    const rank = ls.len;
    const lc = lc_axes[0];
    const rc = rc_axes[0];
    const free_sum: usize = if (rank == 2) 1 else 3;
    const m: usize = @intCast(ls[free_sum - lc]);
    const n: usize = @intCast(rs[free_sum - rc]);
    const k: usize = @intCast(ls[lc]);
    const batches: usize = if (rank == 3) @intCast(ls[0]) else 1;
    const mk = std.math.mul(usize, m, k) catch return error.ShapeMismatch;
    const kn = std.math.mul(usize, k, n) catch return error.ShapeMismatch;
    const mn = std.math.mul(usize, m, n) catch return error.ShapeMismatch;
    const left_count = std.math.mul(usize, batches, mk) catch return error.ShapeMismatch;
    const right_count = std.math.mul(usize, batches, kn) catch return error.ShapeMismatch;
    const output_count = std.math.mul(usize, batches, mn) catch return error.ShapeMismatch;
    // Materialize logical order through the public backend API, including
    // transpose/slice views. Never reinterpret a strided buffer as contiguous.
    const left = try cb.toFloat32(lhs, a);
    defer a.free(left);
    const right = try cb.toFloat32(rhs, a);
    defer a.free(right);
    if (left.len != left_count or right.len != right_count)
        return cb.primDotGeneral(lhs, rhs, ls, rs, lc_axes, rc_axes, lb, rb);
    const da = try a.alloc(f64, left_count);
    defer a.free(da);
    const db = try a.alloc(f64, right_count);
    defer a.free(db);
    const dc = try a.alloc(f64, mn);
    defer a.free(dc);
    const out = try a.alloc(f32, output_count);
    defer a.free(out);
    for (da, left) |*dst, value| dst.* = value;
    for (db, right) |*dst, value| dst.* = value;
    const ta = lc != rank - 1;
    const tb = rc == rank - 1;
    for (0..batches) |batch| {
        cblas_dgemm(101, if (ta) 112 else 111, if (tb) 112 else 111, @intCast(m), @intCast(n), @intCast(k), 1, da[batch * mk ..].ptr, @intCast(if (ta) m else k), db[batch * kn ..].ptr, @intCast(if (tb) k else n), 0, dc.ptr, @intCast(n));
        for (out[batch * mn ..][0..mn], dc) |*dst, value| dst.* = @floatCast(value);
    }
    var shape: [3]i32 = .{ @intCast(batches), @intCast(m), @intCast(n) };
    return cb.fromFloat32Shape(out, if (rank == 3) &shape else shape[1..]);
}

test "laya CPU BLAS preserves FP64 contractions for transpose views and batches" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const reference = compute.computeBackend();
    var actual = reference;
    var table: ops.ComputeBackend.VTable = undefined;
    install(&actual, &table);
    for ([_]bool{ false, true }) |batched| {
        const x = try reference.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, if (batched) &.{ 2, 2, 3 } else &.{ 4, 3 });
        defer reference.free(x);
        const y = try reference.fromFloat32Shape(&.{ -1, 2, 3, 0, 2, 1, 4, 5, -1, 2, 3, 0 }, if (batched) &.{ 2, 3, 2 } else &.{ 3, 4 });
        defer reference.free(y);
        const xs: []const i64 = if (batched) &.{ 2, 2, 3 } else &.{ 4, 3 };
        const ys: []const i64 = if (batched) &.{ 2, 3, 2 } else &.{ 3, 4 };
        for ([_]bool{ false, true }) |tx| for ([_]bool{ false, true }) |ty| {
            const perm: []const u8 = if (batched) &.{ 0, 2, 1 } else &.{ 1, 0 };
            const xt = if (tx) try reference.primTranspose(x, perm, xs) else null;
            defer if (xt) |v| reference.free(v);
            const yt = if (ty) try reference.primTranspose(y, perm, ys) else null;
            defer if (yt) |v| reference.free(v);
            var lx: [3]i64 = undefined;
            var ry: [3]i64 = undefined;
            @memcpy(lx[0..xs.len], xs);
            @memcpy(ry[0..ys.len], ys);
            if (tx) std.mem.swap(i64, &lx[xs.len - 1], &lx[xs.len - 2]);
            if (ty) std.mem.swap(i64, &ry[ys.len - 1], &ry[ys.len - 2]);
            const lc: u8 = @intCast(if (tx) xs.len - 2 else xs.len - 1);
            const rc: u8 = @intCast(if (ty) ys.len - 1 else ys.len - 2);
            const batch_axes: []const u8 = if (batched) &.{0} else &.{};
            const want = try reference.primDotGeneral(xt orelse x, yt orelse y, lx[0..xs.len], ry[0..ys.len], &.{lc}, &.{rc}, batch_axes, batch_axes);
            defer reference.free(want);
            const got = try actual.primDotGeneral(xt orelse x, yt orelse y, lx[0..xs.len], ry[0..ys.len], &.{lc}, &.{rc}, batch_axes, batch_axes);
            defer reference.free(got);
            const w = try reference.toFloat32(want, a);
            defer a.free(w);
            const g = try actual.toFloat32(got, a);
            defer a.free(g);
            try std.testing.expectEqualSlices(f32, w, g);
        };
    }
}
