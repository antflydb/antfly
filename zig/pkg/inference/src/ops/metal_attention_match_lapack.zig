// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! FP64 Householder least squares with the reference solver's scratch bound.
const std = @import("std");
const solver = @import("../runtime/kv/attention_match.zig");
const builtin = @import("builtin");

// LP64 CBLAS_NEW_LAPACK declarations from the macOS 13.3+ SDK.
extern "c" fn @"cblas_dgemv$NEWLAPACK"(c_int, c_int, c_int, c_int, f64, [*]const f64, c_int, [*]const f64, c_int, f64, [*]f64, c_int) void;
extern "c" fn @"cblas_ddot$NEWLAPACK"(c_int, [*]const f64, c_int, [*]const f64, c_int) f64;
extern "c" fn @"cblas_daxpy$NEWLAPACK"(c_int, f64, [*]const f64, c_int, [*]f64, c_int) void;
extern "c" fn @"cblas_dnrm2$NEWLAPACK"(c_int, [*]const f64, c_int) f64;

/// FP64 residual OMP with two-pass reorthogonalization. Acceleration changes
/// dot products and matrix/vector evaluation only. Both active-set fallback
/// and final mass acceptance use the common native direct NNLS/KKT solver.
/// P is already validated by fitMatrices. Scratch stays below the native
/// selector bound: the transposed Q-by-N copy is replaced by N scores/flags;
/// the single-query case needs only flags. No source KV or held data enters.
pub fn select(_: ?*anyopaque, allocator: std.mem.Allocator, p: []const f64, importance: []const f64, q: usize, n: usize, indices: []usize, weights: []f64, cfg: solver.Config) !usize {
    if (comptime !supported) return error.UnsupportedMetalFp64Selection;
    const m = indices.len;
    if (q == 0 or n == 0 or m == 0 or m > n or weights.len != m or importance.len != n or
        p.len != try std.math.mul(usize, q, n)) return error.InvalidAttentionShape;
    const nq = std.math.cast(c_int, q) orelse return error.InvalidAttentionShape;
    const nn = std.math.cast(c_int, n) orelse return error.InvalidAttentionShape;
    _ = std.math.cast(c_int, m) orelse return error.InvalidAttentionShape;
    try cfg.control.check();
    for (importance) |norm| if (!std.math.isFinite(norm) or norm < 0) return error.NonFiniteAttention;
    const scores = try allocator.alloc(f64, if (q > 1) n else 0);
    defer allocator.free(scores);
    const used_storage = try allocator.alloc(bool, std.mem.alignForward(usize, n, @alignOf(f64)));
    defer allocator.free(used_storage);
    const used = used_storage[0..n];
    const basis = try allocator.alloc(f64, try std.math.mul(usize, q, m));
    defer allocator.free(basis);
    const residual = try allocator.alloc(f64, q);
    defer allocator.free(residual);
    const norms = try allocator.alloc(f64, m);
    defer allocator.free(norms);
    const orth = try allocator.alloc(f64, try std.math.mul(usize, q, m));
    defer allocator.free(orth);
    const triangular = try allocator.alloc(f64, try std.math.mul(usize, m, m));
    defer allocator.free(triangular);
    const projected = try allocator.alloc(f64, m);
    defer allocator.free(projected);
    const trial = try allocator.alloc(f64, m);
    defer allocator.free(trial);
    const column_norms = try allocator.alloc(f64, m);
    defer allocator.free(column_norms);
    @memset(used, false);
    @memset(triangular, 0);
    @memset(weights, 0);
    @memset(residual, 1);
    var qr_valid = true;
    var active_solves: usize = 0;
    for (0..m) |selected| {
        try cfg.control.check();
        if (q > 1) @"cblas_dgemv$NEWLAPACK"(101, 112, nq, nn, 1, p.ptr, nn, residual.ptr, 1, 0, scores.ptr, 1);
        var best: ?usize = null;
        var best_score: f64 = -std.math.inf(f64);
        for (0..n) |column| {
            const dot = if (q == 1) p[column] * residual[0] else scores[column];
            const norm = importance[column];
            if (used[column]) continue;
            const score = if (norm > 0) dot / @sqrt(norm) else 0;
            if (!std.math.isFinite(score)) return error.NonFiniteAttention;
            if (best == null or score > best_score) {
                best = column;
                best_score = score;
            }
        }
        indices[selected] = best orelse return error.InvalidFitConfig;
        used[indices[selected]] = true;
        const count = selected + 1;
        const nc: c_int = @intCast(count);
        for (0..q) |i| for (indices[0..count], 0..) |index, j| {
            basis[i * count + j] = p[i * n + index];
        };
        weights[selected] = 0;
        column_norms[selected] = @sqrt(importance[indices[selected]]);
        if (qr_valid and column_norms[selected] > 0) {
            const column = orth[selected * q ..][0..q];
            for (column, 0..) |*value, i| value.* = p[i * n + indices[selected]] / column_norms[selected];
            for (0..2) |_| for (0..selected) |j| {
                const prior = orth[j * q ..][0..q];
                const dot = @"cblas_ddot$NEWLAPACK"(nq, column.ptr, 1, prior.ptr, 1);
                triangular[j * m + selected] += dot;
                @"cblas_daxpy$NEWLAPACK"(nq, -dot, prior.ptr, 1, column.ptr, 1);
            };
            const norm = @"cblas_dnrm2$NEWLAPACK"(nq, column.ptr, 1);
            qr_valid = std.math.isFinite(norm) and norm > 1e-12 and count <= q;
            if (qr_valid) {
                triangular[selected * m + selected] = norm;
                projected[selected] = 0;
                for (column) |*value| {
                    value.* /= norm;
                    projected[selected] += value.*;
                }
            }
        } else qr_valid = false;
        var feasible = qr_valid;
        if (qr_valid) {
            var j = count;
            while (j > 0) {
                j -= 1;
                var value = projected[j];
                for (j + 1..count) |k| value -= triangular[j * m + k] * trial[k];
                trial[j] = value / triangular[j * m + j];
            }
            for (trial[0..count], column_norms[0..count]) |*weight, norm| {
                weight.* /= norm;
                if (!std.math.isFinite(weight.*) or weight.* < 0) feasible = false;
            }
        }
        if (feasible) {
            @memset(residual, 1);
            @"cblas_dgemv$NEWLAPACK"(101, 111, nq, nc, -1, basis.ptr, nc, trial.ptr, 1, 1, residual.ptr, 1);
            @"cblas_dgemv$NEWLAPACK"(101, 112, nq, nc, 1, basis.ptr, nc, residual.ptr, 1, 0, norms.ptr, 1);
            var violation: f64 = 0;
            for (0..count) |j| {
                const gradient = norms[j] / column_norms[j];
                if (!std.math.isFinite(gradient)) {
                    feasible = false;
                    break;
                }
                violation = @max(violation, if (trial[j] > 0) @abs(gradient) else @max(0, gradient));
            }
            feasible = feasible and violation <= cfg.nnls_tolerance * @sqrt(@as(f64, @floatFromInt(q)));
        }
        if (feasible) @memcpy(weights[0..count], trial[0..count]) else active_solves += try solver.fitMassActive(allocator, basis[0 .. q * count], q, count, weights[0..count], residual, norms[0..count], cfg, true);
    }
    return active_solves;
}

pub const supported = if (builtin.os.tag == .macos)
    builtin.os.version_range.semver.min.order(.{ .major = 13, .minor = 3, .patch = 0 }) != .lt
else
    false;

// SDK lapack_version.h binds the LP64 API to these macOS 13.3+ symbols.
extern "c" fn @"dgelqf$NEWLAPACK"(*const c_int, *const c_int, [*]f64, *const c_int, [*]f64, [*]f64, *const c_int, *c_int) void;
extern "c" fn @"dormlq$NEWLAPACK"(*const u8, *const u8, *const c_int, *const c_int, *const c_int, [*]f64, *const c_int, [*]const f64, [*]f64, *const c_int, [*]f64, *const c_int, *c_int) void;

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidAttentionShape;
}

pub fn values(_: ?*anyopaque, allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, control: solver.Control, out: []f64) !void {
    if (comptime !supported) return error.UnsupportedMetalFp64Values;
    try control.check();
    if (q == 0 or m == 0 or d == 0 or !std.math.isFinite(ridge) or ridge < 0 or
        a.len != try mul(q, m) or y.len != try mul(q, d) or out.len != try mul(m, d))
        return error.InvalidAttentionShape;
    for (a) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
    for (y) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
    const rows = try std.math.add(usize, q, m);
    const nr = std.math.cast(c_int, rows) orelse return error.InvalidAttentionShape;
    const nm = std.math.cast(c_int, m) orelse return error.InvalidAttentionShape;
    const nd = std.math.cast(c_int, d) orelse return error.InvalidAttentionShape;
    const qr = try allocator.alloc(f64, try mul(rows, m));
    defer allocator.free(qr);
    const rhs = try allocator.alloc(f64, try mul(rows, d));
    defer allocator.free(rhs);
    const tau = try allocator.alloc(f64, m);
    defer allocator.free(tau);
    @memset(qr, 0);
    @memset(rhs, 0);
    @memcpy(qr[0..a.len], a);
    @memcpy(rhs[0..y.len], y);
    for (0..m) |j| qr[(q + j) * m + j] = @sqrt(ridge);
    const side: u8 = 'R';
    const trans: u8 = 'T';
    const query: c_int = -1;
    var info: c_int = 0;
    var lq_query: f64 = 0;
    var apply_query: f64 = 0;
    @"dgelqf$NEWLAPACK"(&nm, &nr, qr.ptr, &nm, tau.ptr, @ptrCast(&lq_query), &query, &info);
    if (info != 0) return error.AttentionMatchLapackFailure;
    @"dormlq$NEWLAPACK"(&side, &trans, &nd, &nr, &nm, qr.ptr, &nm, tau.ptr, rhs.ptr, &nd, @ptrCast(&apply_query), &query, &info);
    if (info != 0 or !std.math.isFinite(lq_query) or !std.math.isFinite(apply_query) or
        lq_query < 1 or apply_query < 1) return error.AttentionMatchLapackFailure;
    // Reference QR uses rows + max(m,d) elements for v and dots. Preserve
    // that exact bound across tau + work, even if LAPACK prefers more work.
    const cap = try std.math.add(usize, q, @max(m, d));
    const count: usize = @intFromFloat(@min(@as(f64, @floatFromInt(cap)), @max(lq_query, apply_query)));
    if (count < @max(m, d)) return error.AttentionMatchLapackFailure;
    const lwork = std.math.cast(c_int, count) orelse return error.InvalidAttentionShape;
    const work = try allocator.alloc(f64, count);
    defer allocator.free(work);
    try control.check();
    // Row-major A is column-major A^T. Its LQ factorization represents A's
    // Householder QR without an extra transpose or normal equations.
    @"dgelqf$NEWLAPACK"(&nm, &nr, qr.ptr, &nm, tau.ptr, work.ptr, &lwork, &info);
    if (info != 0) return error.AttentionMatchLapackFailure;
    try control.check();
    // Y^T Q^T is the row-major representation of QY, the transformed RHS.
    @"dormlq$NEWLAPACK"(&side, &trans, &nd, &nr, &nm, qr.ptr, &nm, tau.ptr, rhs.ptr, &nd, work.ptr, &lwork, &info);
    if (info != 0) return error.AttentionMatchLapackFailure;
    try control.check();
    var j = m;
    while (j > 0) {
        j -= 1;
        try control.check();
        const diagonal = qr[j * m + j];
        if (!std.math.isFinite(diagonal) or diagonal == 0) return error.SingularValueFit;
        for (0..d) |col| {
            var value = rhs[j * d + col];
            for (j + 1..m) |k| value -= qr[j * m + k] * out[k * d + col];
            out[j * d + col] = value / diagonal;
            if (!std.math.isFinite(out[j * d + col])) return error.NonFiniteAttention;
        }
    }
}
