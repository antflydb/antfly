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

//! attention_match_v1 numerical reference. Inputs are actual post-RoPE queries
//! and keys, grouped by KV head across ALL consumers of a shared donor. This
//! module deliberately has no device dependencies. FP64 is the CUDA oracle,
//! not a qualified serving implementation. Bias is added AFTER logit softcap.
const std = @import("std");

pub const Head = struct {
    keys: []const f64,
    values: []const f64,
    /// Null means exact rows, each with unit attention mass.
    bias: ?[]const f64 = null,
    rows: usize,
    dim: usize,
    scale: f64,
    softcap: f64 = 0,

    pub fn validate(self: Head) !void {
        if (self.rows == 0 or self.dim == 0 or !std.math.isFinite(self.scale) or self.scale <= 0 or
            !std.math.isFinite(self.softcap) or self.softcap < 0) return error.InvalidAttentionShape;
        const count = try mul(self.rows, self.dim);
        if (self.keys.len != count or self.values.len != count) return error.InvalidAttentionShape;
        try finite(self.keys);
        try finite(self.values);
        if (self.bias) |b| {
            if (b.len != self.rows) return error.InvalidAttentionShape;
            try finite(b);
        }
    }
};

pub const Control = struct {
    context: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    pub fn check(self: Control) !void {
        if (self.check_fn) |check_fn| try check_fn(self.context);
    }
};

/// Optional numerical acceleration. Acceptance, source ownership, storage
/// rounding and the final direct NNLS check remain in the native engine.
/// Every callback is synchronous and returns logical scratch ownership to its
/// backend. A backend may retain reusable
/// device capacity until deinit only while keeping it charged against the live
/// context ledger. GPU implementations admit new device memory against that
/// ledger. Host scratch beyond the reference bound must be declared.
pub const Backend = struct {
    validation_batch: enum { rows_64, rows_256 } = .rows_64,
    context: ?*anyopaque = null,
    workspace_fn: *const fn (?*anyopaque, usize, usize, usize, usize) anyerror!usize,
    attend_fn: ?*const fn (?*anyopaque, std.mem.Allocator, Head, []const f64, []f64, ?[]f64, ?[]f64, Control) anyerror!void = null,
    /// Validation consumes only attention output and log mass. Backends may
    /// retain probabilities on device instead of downloading unused matrices.
    summary_fn: ?*const fn (?*anyopaque, std.mem.Allocator, Head, []const f64, []f64, []f64, Control) anyerror!void = null,
    values_fn: ?*const fn (?*anyopaque, std.mem.Allocator, []const f64, []const f64, usize, usize, usize, f64, Control, []f64) anyerror!void = null,
    select_fn: ?*const fn (?*anyopaque, std.mem.Allocator, []const f64, []const f64, usize, usize, []usize, []f64, Config) anyerror!usize = null,
};

pub const Config = struct {
    backend: ?Backend = null,
    /// Reports held-out errors even when a candidate is rejected.
    metrics: ?*Metrics = null,
    target_rows: usize,
    selection: enum { rms, residual_omp } = .rms,
    mass_solver: enum { coordinate, active_set } = .coordinate,
    max_workspace_bytes: usize = 512 * 1024 * 1024,
    max_sweeps: usize = 4096,
    nnls_tolerance: f64 = 1e-8,
    ridge: f64 = 1e-8,
    min_weight: f64 = 1e-12,
    max_relative_mass_error: f64 = 0.05,
    max_relative_output_rmse: f64 = 0.05,
    control: Control = .{},

    fn validate(self: Config, rows: usize) !void {
        if (self.target_rows == 0 or self.target_rows > rows or self.max_sweeps == 0) return error.InvalidFitConfig;
        for ([_]f64{ self.nnls_tolerance, self.ridge, self.min_weight }) |x| {
            if (!std.math.isFinite(x) or x <= 0) return error.InvalidFitConfig;
        }
        for ([_]f64{ self.max_relative_mass_error, self.max_relative_output_rmse }) |x| {
            if (!std.math.isFinite(x) or x < 0) return error.InvalidFitConfig;
        }
    }
};

pub const Metrics = struct {
    /// True only after held-out attention metrics were actually evaluated.
    /// Matrix-only device adapters populate solver counters but do not own a
    /// held-out acceptance decision.
    quality_evaluated: bool = false,
    max_relative_mass_error: f64 = 0,
    relative_output_rmse: f64 = 0,
    nnls_sweeps: usize = 0,
    selection_steps: usize = 0,
    selection_active_solves: usize = 0,
};

pub const Result = struct {
    allocator: std.mem.Allocator,
    keys: []f64,
    values: []f64,
    bias: []f64,
    source_indices: []usize,
    metrics: Metrics = .{},

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
        self.allocator.free(self.bias);
        self.allocator.free(self.source_indices);
        self.* = undefined;
    }

    pub fn head(self: Result, source: Head) Head {
        return .{ .keys = self.keys, .values = self.values, .bias = self.bias, .rows = self.bias.len, .dim = source.dim, .scale = source.scale, .softcap = source.softcap };
    }
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidAttentionShape;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidAttentionShape;
}
fn finite(xs: []const f64) !void {
    for (xs) |x| if (!std.math.isFinite(x)) return error.NonFiniteAttention;
}

/// Conservative bound on simultaneous scratch AND result allocations, excluding
/// caller-owned source/query buffers. Admission must also count those buffers and
/// the original device cache until replacement has committed.
pub fn workspaceBytes(n: usize, m: usize, q: usize, d: usize) !usize {
    var count = try mul(try mul(q, n), 2); // source probabilities and selection transpose
    count = try add(count, try mul(try mul(q, m), 5)); // basis, fitted probabilities, QR
    count = try add(count, try mul(try mul(q, d), 2)); // targets and QR RHS
    count = try add(count, try mul(try mul(m, m), 2)); // ridge and incremental QR rows
    count = try add(count, try mul(try mul(m, d), 4)); // QR RHS, result K/V, spare
    count = try add(count, try mul(try add(try add(n, m), try add(q, d)), 16));
    return mul(count, @sizeOf(f64));
}

pub fn fitWorkspaceBytes(n: usize, m: usize, q: usize, d: usize, heldout: usize, cfg: Config) !usize {
    const batch = validationBatchSize(heldout, cfg);
    const base = try workspaceBytes(n, m, @max(q, batch), d);
    const validation = try validationBytes(n, m, d, heldout, cfg);
    const extra = if (cfg.backend) |backend| try backend.workspace_fn(backend.context, n, m, @max(q, batch), d) else 0;
    return add(try add(base, validation), extra);
}

fn validationBatchSize(queries: usize, cfg: Config) usize {
    return @min(queries, if (cfg.backend != null and cfg.backend.?.attend_fn != null) (switch (cfg.backend.?.validation_batch) {
        .rows_64 => @as(usize, 64),
        .rows_256 => 256,
    }) else 1);
}

pub fn validationBytes(n: usize, m: usize, d: usize, queries: usize, cfg: Config) !usize {
    const batch = validationBatchSize(queries, cfg);
    const probability_rows = if (cfg.backend != null and cfg.backend.?.summary_fn != null) 0 else try add(n, m);
    const buffers = try mul(try mul(batch, try add(probability_rows, try add(try mul(d, 2), 2))), 8);
    const extra = if (cfg.backend) |backend| try backend.workspace_fn(backend.context, @max(n, m), 1, batch, d) else 0;
    return add(buffers, extra);
}

/// Caller-owned row-major buffers; never materializes all held-out queries in
/// one allocation. Control is checked before and after accelerated operations.
pub fn attendBatch(allocator: std.mem.Allocator, h: Head, queries: []const f64, probabilities: []f64, targets: ?[]f64, masses: ?[]f64, cfg: Config) !void {
    try cfg.control.check();
    if (h.dim == 0 or h.rows == 0 or queries.len == 0 or queries.len % h.dim != 0) return error.InvalidAttentionShape;
    const q = queries.len / h.dim;
    if (probabilities.len != try mul(q, h.rows)) return error.InvalidAttentionShape;
    if (targets) |out| if (out.len != queries.len) return error.InvalidAttentionShape;
    if (masses) |out| if (out.len != q) return error.InvalidAttentionShape;
    if (cfg.backend) |backend| if (backend.attend_fn) |call| {
        try call(backend.context, allocator, h, queries, probabilities, targets, masses, cfg.control);
        try finite(probabilities);
        if (targets) |out| try finite(out);
        if (masses) |out| try finite(out);
        try cfg.control.check();
        return;
    };
    for (0..q) |i| {
        try cfg.control.check();
        const mass = try attend(h, queries[i * h.dim ..][0..h.dim], probabilities[i * h.rows ..][0..h.rows], if (targets) |out| out[i * h.dim ..][0..h.dim] else null);
        if (masses) |out| out[i] = mass;
    }
}

pub fn attendSummary(allocator: std.mem.Allocator, head: Head, queries: []const f64, targets: []f64, masses: []f64, cfg: Config) !void {
    try cfg.control.check();
    if (head.dim == 0 or head.rows == 0 or queries.len == 0 or queries.len % head.dim != 0 or
        targets.len != queries.len or masses.len != queries.len / head.dim) return error.InvalidAttentionShape;
    const backend = cfg.backend orelse return error.UnsupportedAttentionSummary;
    const call = backend.summary_fn orelse return error.UnsupportedAttentionSummary;
    try call(backend.context, allocator, head, queries, targets, masses, cfg.control);
    try finite(targets);
    try finite(masses);
    try cfg.control.check();
}

pub fn validationMetrics(allocator: std.mem.Allocator, source: Head, compact: Head, queries: []const f64, cfg: Config) !Metrics {
    if (source.dim == 0 or source.dim != compact.dim or queries.len == 0 or queries.len % source.dim != 0) return error.InvalidAttentionShape;
    const d = source.dim;
    const count = queries.len / d;
    const batch = validationBatchSize(count, cfg);
    const summary = cfg.backend != null and cfg.backend.?.summary_fn != null;
    const p = try allocator.alloc(f64, if (summary) 0 else try mul(batch, source.rows));
    defer allocator.free(p);
    const ph = try allocator.alloc(f64, if (summary) 0 else try mul(batch, compact.rows));
    defer allocator.free(ph);
    const y = try allocator.alloc(f64, try mul(batch, d));
    defer allocator.free(y);
    const yh = try allocator.alloc(f64, try mul(batch, d));
    defer allocator.free(yh);
    const mass = try allocator.alloc(f64, batch);
    defer allocator.free(mass);
    const fitted_mass = try allocator.alloc(f64, batch);
    defer allocator.free(fitted_mass);
    var result: Metrics = .{};
    var error_norm: f64 = 0;
    var target_norm: f64 = 0;
    var offset: usize = 0;
    while (offset < count) {
        const take = @min(batch, count - offset);
        const qs = queries[offset * d ..][0 .. take * d];
        if (summary) {
            try attendSummary(allocator, source, qs, y[0 .. take * d], mass[0..take], cfg);
            try attendSummary(allocator, compact, qs, yh[0 .. take * d], fitted_mass[0..take], cfg);
        } else {
            try attendBatch(allocator, source, qs, p[0 .. take * source.rows], y[0 .. take * d], mass[0..take], cfg);
            try attendBatch(allocator, compact, qs, ph[0 .. take * compact.rows], yh[0 .. take * d], fitted_mass[0..take], cfg);
        }
        for (mass[0..take], fitted_mass[0..take]) |a, b| {
            const relative = @abs(@exp(b - a) - 1);
            if (!std.math.isFinite(relative)) return error.CompactionFitRejected;
            result.max_relative_mass_error = @max(result.max_relative_mass_error, relative);
        }
        for (y[0 .. take * d], yh[0 .. take * d]) |a, b| {
            error_norm = std.math.hypot(error_norm, a - b);
            target_norm = std.math.hypot(target_norm, a);
        }
        offset += take;
    }
    result.relative_output_rmse = error_norm / @max(target_norm, 1e-12);
    return result;
}

fn solveValues(allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, cfg: Config, out: []f64) !void {
    try cfg.control.check();
    if (cfg.backend) |backend| if (backend.values_fn) |call| {
        try call(backend.context, allocator, a, y, q, m, d, ridge, cfg.control, out);
        try finite(out);
        try cfg.control.check();
        return;
    };
    try fitValues(allocator, a, y, q, m, d, ridge, cfg.control, out);
}

fn logit(h: Head, query: []const f64, row: usize) !f64 {
    var x: f64 = 0;
    for (query, h.keys[row * h.dim ..][0..h.dim]) |q, k| x += q * k;
    x *= h.scale;
    if (!std.math.isFinite(x)) return error.NonFiniteAttention;
    if (h.softcap > 0) x = h.softcap * std.math.tanh(x / h.softcap);
    x += if (h.bias) |b| b[row] else 0;
    if (!std.math.isFinite(x)) return error.NonFiniteAttention;
    return x;
}

/// Returns log(sum(exp(logits))) and optionally the normalized attention output.
/// Callers must validate the head first; output and scratch cannot alias inputs.
pub fn attend(h: Head, query: []const f64, probabilities: []f64, output: ?[]f64) !f64 {
    if (query.len != h.dim or probabilities.len != h.rows or h.rows == 0) return error.InvalidAttentionShape;
    if (output) |y| if (y.len != h.dim) return error.InvalidAttentionShape;
    var maximum: f64 = -std.math.inf(f64);
    for (probabilities, 0..) |*p, i| {
        p.* = try logit(h, query, i);
        maximum = @max(maximum, p.*);
    }
    var sum: f64 = 0;
    for (probabilities) |*p| {
        p.* = @exp(p.* - maximum);
        sum += p.*;
    }
    for (probabilities) |*p| p.* /= sum;
    if (output) |y| {
        @memset(y, 0);
        for (probabilities, 0..) |p, i| {
            for (y, h.values[i * h.dim ..][0..h.dim]) |*v, original| v.* += p * original;
        }
        try finite(y);
    }
    const mass = maximum + @log(sum);
    if (!std.math.isFinite(mass)) return error.NonFiniteAttention;
    return mass;
}

/// Coordinate descent NNLS, with a projected-gradient KKT convergence check.
/// A is normalized by each source row's partition function, so the target is 1.
fn fitMass(a: []const f64, q: usize, m: usize, weights: []f64, residual: []f64, norms: []f64, cfg: Config) !usize {
    @memset(weights, 0);
    @memset(residual, 1);
    @memset(norms, 0);
    for (0..q) |i| for (0..m) |j| {
        norms[j] += a[i * m + j] * a[i * m + j];
    };
    for (0..cfg.max_sweeps) |sweep| {
        try cfg.control.check();
        for (0..m) |j| {
            if (norms[j] == 0) continue;
            var gradient: f64 = 0;
            for (0..q) |i| gradient += a[i * m + j] * residual[i];
            const next = @max(0, weights[j] + gradient / norms[j]);
            const delta = next - weights[j];
            if (!std.math.isFinite(next)) return error.NonFiniteAttention;
            weights[j] = next;
            for (0..q) |i| residual[i] -= delta * a[i * m + j];
        }
        var violation: f64 = 0;
        for (0..m) |j| {
            if (norms[j] == 0) continue;
            var gradient: f64 = 0;
            for (0..q) |i| gradient += a[i * m + j] * residual[i];
            const projected = if (weights[j] > 0) @abs(gradient) else @max(0, gradient);
            violation = @max(violation, projected / @sqrt(norms[j]));
        }
        if (violation <= cfg.nnls_tolerance * @sqrt(@as(f64, @floatFromInt(q)))) return sweep + 1;
    }
    return error.MassFitDidNotConverge;
}

// Lawson-Hanson active-set solve using scaled Householder QR, with the same
// direct residual/KKT check as coordinate descent. No normal equations.
pub fn fitMassActive(allocator: std.mem.Allocator, a: []const f64, q: usize, m: usize, weights: []f64, residual: []f64, norms: []f64, cfg: Config, warm: bool) !usize {
    // Keep allocation lengths aligned to the subsequent f64 workspace. The
    // fixed-buffer allocator cannot reclaim alignment gaps on free; repeated
    // active-set solves must release every byte before the next selection step.
    const active_storage = try allocator.alloc(bool, std.mem.alignForward(usize, m, @alignOf(f64)));
    defer allocator.free(active_storage);
    const active = active_storage[0..m];
    const indices = try allocator.alloc(usize, m);
    defer allocator.free(indices);
    const matrix = try allocator.alloc(f64, try mul(q, m));
    defer allocator.free(matrix);
    const rhs = try allocator.alloc(f64, q);
    defer allocator.free(rhs);
    const solution = try allocator.alloc(f64, m);
    defer allocator.free(solution);
    const trial = try allocator.alloc(f64, m);
    defer allocator.free(trial);
    @memset(rhs, 1);
    @memset(norms, 0);
    if (!warm) @memset(weights, 0);
    for (weights, active) |w, *flag| {
        if (!std.math.isFinite(w) or w < 0) return error.NonFiniteAttention;
        flag.* = w > 0;
    }
    for (0..q) |i| for (0..m) |j| {
        norms[j] += a[i * m + j] * a[i * m + j];
    };
    for (norms, weights, active) |*norm, *weight, *flag| {
        if (!std.math.isFinite(norm.*)) return error.NonFiniteAttention;
        norm.* = @sqrt(norm.*);
        if (norm.* == 0) {
            weight.* = 0;
            flag.* = false;
        }
    }
    const tolerance = cfg.nnls_tolerance * @sqrt(@as(f64, @floatFromInt(q)));
    var solves: usize = 0;
    while (true) {
        try cfg.control.check();
        @memset(residual, 1);
        for (0..q) |i| for (0..m) |j| {
            residual[i] -= a[i * m + j] * weights[j];
        };
        try finite(residual);
        var violation: f64 = 0;
        var best: ?usize = null;
        var best_gradient: f64 = tolerance;
        for (0..m) |j| {
            if (norms[j] == 0) continue;
            var gradient: f64 = 0;
            for (0..q) |i| gradient += a[i * m + j] * residual[i];
            gradient /= norms[j];
            if (!std.math.isFinite(gradient)) return error.NonFiniteAttention;
            violation = @max(violation, if (weights[j] > 0) @abs(gradient) else @max(0, gradient));
            if (!active[j] and gradient > best_gradient) {
                best = j;
                best_gradient = gradient;
            }
        }
        if (violation <= tolerance) return @max(solves, 1);
        if (solves >= cfg.max_sweeps) return error.MassFitDidNotConverge;
        if (best) |j| active[j] = true;
        while (true) {
            try cfg.control.check();
            if (solves >= cfg.max_sweeps) return error.MassFitDidNotConverge;
            var count: usize = 0;
            for (active, 0..) |flag, j| if (flag) {
                indices[count] = j;
                count += 1;
            };
            if (count == 0 or count > q) return error.SingularValueFit;
            for (0..q) |i| for (indices[0..count], 0..) |j, c| {
                matrix[i * count + c] = a[i * m + j] / norms[j];
            };
            // The active columns are added only for a positive projected
            // gradient, preserving independence to the KKT tolerance.
            try solveValues(allocator, matrix[0 .. q * count], rhs, q, count, 1, 0, cfg, solution[0..count]);
            solves += 1;
            @memset(trial, 0);
            var feasible = true;
            for (indices[0..count], solution[0..count]) |j, z| {
                trial[j] = z / norms[j];
                if (!std.math.isFinite(trial[j])) return error.NonFiniteAttention;
                if (trial[j] <= 0) feasible = false;
            }
            if (feasible) {
                @memcpy(weights, trial);
                break;
            }
            var alpha: f64 = 1;
            var drop: ?usize = null;
            for (indices[0..count]) |j| if (trial[j] <= 0) {
                const fraction = if (weights[j] == 0) 0 else weights[j] / (weights[j] - trial[j]);
                if (drop == null or fraction < alpha) {
                    alpha = fraction;
                    drop = j;
                }
            };
            if (!std.math.isFinite(alpha) or alpha < 0 or alpha > 1) return error.NonFiniteAttention;
            for (weights, 0..) |*w, j| {
                w.* += alpha * (trial[j] - w.*);
                if (w.* <= 0 or (drop != null and j == drop.?)) {
                    w.* = 0;
                    active[j] = false;
                }
            }
        }
    }
}

/// Householder QR of ridge-augmented [A; sqrt(lambda) I], avoiding normal
/// equations (which square the condition number). RHS columns are transformed
/// together, and one scratch vector is reused for each reflector.
fn fitValues(allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, control: Control, out: []f64) !void {
    const rows = try add(q, m);
    const qr = try allocator.alloc(f64, try mul(rows, m));
    defer allocator.free(qr);
    const rhs = try allocator.alloc(f64, try mul(rows, d));
    defer allocator.free(rhs);
    const v = try allocator.alloc(f64, rows);
    defer allocator.free(v);
    const dots = try allocator.alloc(f64, @max(m, d));
    defer allocator.free(dots);
    @memset(qr, 0);
    @memset(rhs, 0);
    @memcpy(qr[0..a.len], a);
    @memcpy(rhs[0..y.len], y);
    for (0..m) |j| qr[(q + j) * m + j] = @sqrt(ridge);
    for (0..m) |j| {
        try control.check();
        var norm: f64 = 0;
        for (j..rows) |i| norm = std.math.hypot(norm, qr[i * m + j]);
        if (!std.math.isFinite(norm) or norm == 0) return error.SingularValueFit;
        const alpha = if (qr[j * m + j] >= 0) -norm else norm;
        for (j..rows) |i| v[i] = qr[i * m + j];
        v[j] -= alpha;
        var vn: f64 = 0;
        for (v[j..]) |x| vn = std.math.hypot(vn, x);
        for (v[j..]) |*x| x.* /= vn;
        @memset(dots[0..m], 0);
        for (j..rows) |i| for (j..m) |col| {
            dots[col] += v[i] * qr[i * m + col];
        };
        for (j..rows) |i| for (j..m) |col| {
            qr[i * m + col] -= 2 * v[i] * dots[col];
        };
        @memset(dots[0..d], 0);
        for (j..rows) |i| for (0..d) |col| {
            dots[col] += v[i] * rhs[i * d + col];
        };
        for (j..rows) |i| for (0..d) |col| {
            rhs[i * d + col] -= 2 * v[i] * dots[col];
        };
    }
    var j = m;
    while (j > 0) {
        j -= 1;
        for (0..d) |col| {
            var x = rhs[j * d + col];
            for (j + 1..m) |k| x -= qr[j * m + k] * out[k * d + col];
            out[j * d + col] = x / qr[j * m + j];
        }
    }
    try finite(out);
}

/// Device adapters may supply only derived attention matrices. The original
/// source K/V and captured queries remain device-resident. Rows of P must be
/// normalized probabilities for the immutable source; Y is its attention output.
pub const MatrixInput = struct {
    probabilities: []const f64,
    targets: []const f64,
    bias: ?[]const f64 = null,
    rows: usize,
    dim: usize,
    query_rows: usize,

    fn validate(self: MatrixInput) !void {
        if (self.rows == 0 or self.dim == 0 or self.query_rows == 0 or
            self.probabilities.len != try mul(self.query_rows, self.rows) or
            self.targets.len != try mul(self.query_rows, self.dim)) return error.InvalidAttentionShape;
        try finite(self.targets);
        if (self.bias) |bias| {
            if (bias.len != self.rows) return error.InvalidAttentionShape;
            try finite(bias);
        }
        for (0..self.query_rows) |q| {
            var sum: f64 = 0;
            for (self.probabilities[q * self.rows ..][0..self.rows]) |p| {
                if (!std.math.isFinite(p) or p < 0 or p > 1) return error.InvalidAttentionProbabilities;
                sum += p;
            }
            // Allow normalization roundoff from an explicitly FP32 device
            // frontend; the native OMP/NNLS/QR solve itself remains FP64.
            if (@abs(sum - 1) > 1e-5) return error.InvalidAttentionProbabilities;
        }
    }
};

/// Unvalidated private staging. This object has no publication authority:
/// acceptance must evaluate rounded K/V and f32 bias against the full source.
pub const MatrixResult = struct {
    allocator: std.mem.Allocator,
    values: []f64,
    bias: []f64,
    source_indices: []usize,
    metrics: Metrics = .{},

    pub fn deinit(self: *MatrixResult) void {
        self.allocator.free(self.values);
        self.allocator.free(self.bias);
        self.allocator.free(self.source_indices);
        self.* = undefined;
    }
};

/// Conservative simultaneous host scratch + private result bound. Caller-owned
/// P/Y, device buffers, and the live source must be admitted separately.
pub fn matrixWorkspaceBytes(n: usize, m: usize, q: usize, d: usize) !usize {
    return workspaceBytes(n, m, q, d);
}

pub fn fitMatrices(allocator: std.mem.Allocator, input: MatrixInput, cfg: Config) !MatrixResult {
    if (cfg.metrics) |metrics| metrics.* = .{};
    try cfg.control.check();
    try input.validate();
    try cfg.validate(input.rows);
    // Keeping every row is an exact device copy, never a fitted substitute.
    if (cfg.target_rows == input.rows) return error.ExactAttentionCopyRequired;
    const m = cfg.target_rows;
    const workspace = try matrixWorkspaceBytes(input.rows, m, input.query_rows, input.dim);
    if (workspace > cfg.max_workspace_bytes) return error.CompactionWorkspaceLimit;
    const result_bytes = try add(try mul(try mul(m, input.dim), @sizeOf(f64)), try mul(m, @sizeOf(f64) + @sizeOf(usize)));
    const scratch_buffer = try allocator.alignedAlloc(u8, .of(f64), workspace - result_bytes);
    defer allocator.free(scratch_buffer);
    var fixed = std.heap.FixedBufferAllocator.init(scratch_buffer);
    const values = try allocator.alloc(f64, try mul(m, input.dim));
    errdefer allocator.free(values);
    const bias = try allocator.alloc(f64, m);
    errdefer allocator.free(bias);
    const indices = try allocator.alloc(usize, m);
    errdefer allocator.free(indices);
    var result = MatrixResult{ .allocator = allocator, .values = values, .bias = bias, .source_indices = indices };
    defer if (cfg.metrics) |metrics| {
        metrics.* = result.metrics;
    };
    try solveMatrices(&fixed, input, cfg, values, bias, indices, &result.metrics);
    try cfg.control.check();
    return result;
}

fn solveMatrices(fixed: *std.heap.FixedBufferAllocator, input: MatrixInput, cfg: Config, values: []f64, bias: []f64, indices: []usize, metrics: *Metrics) !void {
    const scratch = fixed.allocator();
    const n = input.rows;
    const m = cfg.target_rows;
    const q = input.query_rows;
    const d = input.dim;
    const probabilities = input.probabilities;
    const targets = input.targets;
    const importance = try scratch.alloc(f64, n);
    @memset(importance, 0);
    for (0..q) |i| {
        const ps = probabilities[i * n ..][0..n];
        for (importance, ps) |*score, p| score.* += p * p; // RMS ranking
    }
    const order = try scratch.alloc(usize, n);
    for (order, 0..) |*idx, i| idx.* = i;
    std.mem.sort(usize, order, importance, struct {
        fn less(scores: []const f64, a: usize, b: usize) bool {
            return scores[a] > scores[b] or (scores[a] == scores[b] and a < b);
        }
    }.less);
    if (cfg.selection == .rms) {
        @memcpy(indices, order[0..m]);
    } else if (cfg.backend != null and cfg.backend.?.select_fn != null) {
        const selection_mark = fixed.end_index;
        const selected_weights = try scratch.alloc(f64, m);
        const backend = cfg.backend.?;
        metrics.selection_active_solves = try backend.select_fn.?(backend.context, scratch, probabilities, importance, q, n, indices, selected_weights, cfg);
        try cfg.control.check();
        // Validate accelerator output before using it for indexing. The final
        // native NNLS solve below verifies direct KKT convergence independently.
        for (indices, selected_weights, 0..) |idx, weight, i| {
            if (idx >= n or std.mem.indexOfScalar(usize, indices[0..i], idx) != null or !std.math.isFinite(weight) or weight < 0) return error.InvalidSelectionResult;
            importance[idx] = weight;
        }
        metrics.selection_steps = m;
        fixed.end_index = selection_mark;
    } else {
        const selection_mark = fixed.end_index;
        {
            const columns = try scratch.alloc(f64, q * n);
            for (0..q) |i| for (0..n) |j| {
                columns[j * q + i] = probabilities[i * n + j];
            };
            const basis = try scratch.alloc(f64, q * m);
            const weights = try scratch.alloc(f64, m);
            const residual = try scratch.alloc(f64, q);
            const norms = try scratch.alloc(f64, m);
            const orth = try scratch.alloc(f64, q * m);
            const triangular = try scratch.alloc(f64, m * m);
            const projected = try scratch.alloc(f64, m);
            const trial = try scratch.alloc(f64, m);
            const column_norms = try scratch.alloc(f64, m);
            @memset(triangular, 0);
            var qr_valid = true;
            @memset(weights, 0);
            @memset(residual, 1);
            for (0..m) |selected| {
                try cfg.control.check();
                var best: ?usize = null;
                var best_score: f64 = -std.math.inf(f64);
                for (0..n) |column| {
                    if (std.mem.indexOfScalar(usize, indices[0..selected], column) != null) continue;
                    var dot: f64 = 0;
                    for (columns[column * q ..][0..q], residual) |x, r| dot += x * r;
                    const norm = importance[column];
                    const score = if (norm > 0) dot / @sqrt(norm) else 0;
                    if (best == null or score > best_score) {
                        best = column;
                        best_score = score;
                    }
                }
                indices[selected] = best orelse return error.InvalidFitConfig;
                const count = selected + 1;
                for (0..q) |i| for (indices[0..count], 0..) |idx, j| {
                    basis[i * count + j] = probabilities[i * n + idx];
                };
                weights[selected] = 0;
                column_norms[selected] = @sqrt(importance[indices[selected]]);
                if (qr_valid and column_norms[selected] > 0) {
                    const column = orth[selected * q ..][0..q];
                    for (column, 0..) |*v, i| v.* = probabilities[i * n + indices[selected]] / column_norms[selected];
                    // Twice-reorthogonalized MGS updates the QR without forming
                    // normal equations. Every accepted solution still passes
                    // the direct NNLS residual/KKT check below.
                    for (0..2) |_| for (0..selected) |j| {
                        const prior = orth[j * q ..][0..q];
                        var dot: f64 = 0;
                        for (column, prior) |v, u| dot += v * u;
                        triangular[j * m + selected] += dot;
                        for (column, prior) |*v, u| v.* -= dot * u;
                    };
                    var norm: f64 = 0;
                    for (column) |v| norm = std.math.hypot(norm, v);
                    qr_valid = std.math.isFinite(norm) and norm > 1e-12 and count <= q;
                    if (qr_valid) {
                        triangular[selected * m + selected] = norm;
                        projected[selected] = 0;
                        for (column) |*v| {
                            v.* /= norm;
                            projected[selected] += v.*;
                        }
                    }
                } else qr_valid = false;
                var feasible = qr_valid;
                if (qr_valid) {
                    var j = count;
                    while (j > 0) {
                        j -= 1;
                        var z = projected[j];
                        for (j + 1..count) |k| z -= triangular[j * m + k] * trial[k];
                        trial[j] = z / triangular[j * m + j];
                    }
                    for (trial[0..count], column_norms[0..count]) |*w, norm| {
                        w.* /= norm;
                        if (!std.math.isFinite(w.*) or w.* < 0) feasible = false;
                    }
                }
                if (feasible) {
                    @memset(residual, 1);
                    for (0..q) |i| for (0..count) |j| {
                        residual[i] -= basis[i * count + j] * trial[j];
                    };
                    var violation: f64 = 0;
                    @memset(norms[0..count], 0);
                    for (0..q) |i| for (0..count) |j| {
                        norms[j] += basis[i * count + j] * residual[i];
                    };
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
                if (feasible) @memcpy(weights[0..count], trial[0..count]) else metrics.selection_active_solves += try fitMassActive(scratch, basis[0 .. q * count], q, count, weights[0..count], residual, norms[0..count], cfg, true);
                metrics.selection_steps += 1;
            }
            for (indices, weights) |idx, w| importance[idx] = w;
        }
        fixed.end_index = selection_mark;
    }
    std.mem.sort(usize, indices, {}, std.sort.asc(usize));
    const basis = try scratch.alloc(f64, try mul(q, m));
    for (indices, 0..) |idx, j| {
        for (0..q) |i| basis[i * m + j] = probabilities[i * n + idx];
    }
    const weights = try scratch.alloc(f64, m);
    const residual = try scratch.alloc(f64, q);
    const norms = try scratch.alloc(f64, m);
    if (cfg.selection == .residual_omp) for (indices, weights) |idx, *w| {
        w.* = importance[idx];
    };
    metrics.nnls_sweeps = if (cfg.mass_solver == .active_set)
        try fitMassActive(scratch, basis, q, m, weights, residual, norms, cfg, cfg.selection == .residual_omp)
    else
        try fitMass(basis, q, m, weights, residual, norms, cfg);
    for (indices, bias, weights) |idx, *b, w| b.* = (if (input.bias) |bs| bs[idx] else @as(f64, 0)) + @log(@max(w, cfg.min_weight));
    try finite(bias);
    const fitted = try scratch.alloc(f64, try mul(q, m));
    // Reweight the original normalized probabilities. This is algebraically
    // identical to softmax over the selected keys plus their new log mass,
    // including any bias already represented by the source. No source KV or
    // query readback is needed by a device adapter.
    for (0..q) |i| {
        var sum: f64 = 0;
        for (indices, 0..) |idx, j| {
            const weight = @max(weights[j], cfg.min_weight);
            fitted[i * m + j] = probabilities[i * n + idx] * weight;
            sum += fitted[i * m + j];
        }
        if (!std.math.isFinite(sum) or sum <= 0) return error.NonFiniteAttention;
        for (fitted[i * m ..][0..m]) |*p| p.* /= sum;
    }
    try solveValues(scratch, fitted, targets, q, m, d, cfg.ridge, cfg, values);
}

/// Fit a single donor KV head. Held-out queries are mandatory, and callers must
/// collect independent queries from every consumer/GQA head. No source mutation
/// occurs, including on OOM, poor fit, or convergence failure.
pub fn fit(allocator: std.mem.Allocator, source: Head, train: []const f64, heldout: []const f64, cfg: Config) !Result {
    if (cfg.metrics) |metrics| metrics.* = .{};
    try cfg.control.check();
    try source.validate();
    try cfg.validate(source.rows);
    if (train.len == 0 or heldout.len == 0 or train.len % source.dim != 0 or heldout.len % source.dim != 0)
        return error.InvalidAttentionShape;
    try finite(train);
    try finite(heldout);
    const n = source.rows;
    const m = cfg.target_rows;
    const d = source.dim;
    const q = train.len / d;
    const result_bytes = try add(try mul(try mul(m, d), 2 * @sizeOf(f64)), try mul(m, @sizeOf(f64) + @sizeOf(usize)));
    const workspace = if (m == n) result_bytes else try fitWorkspaceBytes(n, m, q, d, heldout.len / d, cfg);
    if (workspace > cfg.max_workspace_bytes) return error.CompactionWorkspaceLimit;
    const scratch_buffer = try allocator.alignedAlloc(u8, .of(f64), workspace - result_bytes);
    defer allocator.free(scratch_buffer);
    var fixed = std.heap.FixedBufferAllocator.init(scratch_buffer);
    const scratch = fixed.allocator();
    const keys = try allocator.alloc(f64, try mul(m, d));
    errdefer allocator.free(keys);
    const values = try allocator.alloc(f64, try mul(m, d));
    errdefer allocator.free(values);
    const bias = try allocator.alloc(f64, m);
    errdefer allocator.free(bias);
    const indices = try allocator.alloc(usize, m);
    errdefer allocator.free(indices);
    var result: Result = .{ .allocator = allocator, .keys = keys, .values = values, .bias = bias, .source_indices = indices };
    defer {
        if (cfg.metrics) |metrics| metrics.* = result.metrics;
    }
    if (m == n) {
        @memcpy(keys, source.keys);
        @memcpy(values, source.values);
        for (bias, indices, 0..) |*b, *idx, i| {
            b.* = if (source.bias) |bs| bs[i] else 0;
            idx.* = i;
        }
        return result;
    }
    const probabilities = try scratch.alloc(f64, try mul(q, n));
    const targets = try scratch.alloc(f64, try mul(q, d));
    try attendBatch(scratch, source, train, probabilities, targets, null, cfg);
    try solveMatrices(&fixed, .{ .probabilities = probabilities, .targets = targets, .bias = source.bias, .rows = n, .dim = d, .query_rows = q }, cfg, values, bias, indices, &result.metrics);
    for (indices, 0..) |idx, j| @memcpy(keys[j * d ..][0..d], source.keys[idx * d ..][0..d]);
    const compact = result.head(source);
    const validation = try validationMetrics(scratch, source, compact, heldout, cfg);
    result.metrics.max_relative_mass_error = validation.max_relative_mass_error;
    result.metrics.relative_output_rmse = validation.relative_output_rmse;
    result.metrics.quality_evaluated = true;
    if (!std.math.isFinite(result.metrics.max_relative_mass_error) or
        !std.math.isFinite(result.metrics.relative_output_rmse) or
        result.metrics.max_relative_mass_error > cfg.max_relative_mass_error or
        result.metrics.relative_output_rmse > cfg.max_relative_output_rmse) return error.CompactionFitRejected;
    return result;
}

test "attention_match matrix adapter matches native source fit without source KV" {
    const h: Head = .{ .keys = &.{ -1, 0, 0.5, 1 }, .values = &.{ 3, 2, 1, 4 }, .bias = &.{ 0.1, -0.2, 0.3, 0 }, .rows = 4, .dim = 1, .scale = 0.5, .softcap = 0.7 };
    const queries = [_]f64{ -2, -1, 0, 1, 2, 3 };
    var p: [queries.len * 4]f64 = undefined;
    var y: [queries.len]f64 = undefined;
    const cfg: Config = .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .max_relative_mass_error = 10, .max_relative_output_rmse = 10 };
    try attendBatch(std.testing.allocator, h, &queries, &p, &y, null, cfg);
    var matrix = try fitMatrices(std.testing.allocator, .{ .probabilities = &p, .targets = &y, .bias = h.bias, .rows = h.rows, .dim = h.dim, .query_rows = queries.len }, cfg);
    defer matrix.deinit();
    var native = try fit(std.testing.allocator, h, &queries, &.{ -0.5, 0.5 }, cfg);
    defer native.deinit();
    try std.testing.expectEqualSlices(usize, native.source_indices, matrix.source_indices);
    try std.testing.expectEqualSlices(f64, native.bias, matrix.bias);
    try std.testing.expectEqualSlices(f64, native.values, matrix.values);
    try std.testing.expect(!matrix.metrics.quality_evaluated);
    try std.testing.expect(native.metrics.quality_evaluated);
}

test "attention_match matrix input rejects invalid probabilities and enforces scratch admission" {
    const input: MatrixInput = .{ .probabilities = &.{ 0.5, 0.5, 0.25, 0.75 }, .targets = &.{ 1, 2 }, .rows = 2, .dim = 1, .query_rows = 2 };
    var cfg: Config = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set };
    cfg.max_workspace_bytes = (try matrixWorkspaceBytes(2, 1, 2, 1)) - 1;
    try std.testing.expectError(error.CompactionWorkspaceLimit, fitMatrices(std.testing.failing_allocator, input, cfg));
    var invalid = input;
    invalid.probabilities = &.{ 0.5, 0.4, 0.25, 0.75 };
    try std.testing.expectError(error.InvalidAttentionProbabilities, fitMatrices(std.testing.failing_allocator, invalid, cfg));
}

test "attention_match matrix fitting frees every failed allocation" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var r = try fitMatrices(a, .{ .probabilities = &.{ 0.5, 0.5, 0.5, 0.5 }, .targets = &.{ 3, 3 }, .rows = 2, .dim = 1, .query_rows = 2 }, .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set });
            defer r.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "attention_match identity preserves fitted bias and values exactly" {
    const h: Head = .{ .keys = &.{ 0, 1 }, .values = &.{ 3, 4 }, .bias = &.{ 2, -1 }, .rows = 2, .dim = 1, .scale = 1 };
    var r = try fit(std.testing.allocator, h, &.{0}, &.{1}, .{ .target_rows = 2 });
    defer r.deinit();
    try std.testing.expectEqualSlices(f64, h.keys, r.keys);
    try std.testing.expectEqualSlices(f64, h.values, r.values);
    try std.testing.expectEqualSlices(f64, h.bias.?, r.bias);
}

test "attention_match duplicate keys preserve mass alongside a future exact token" {
    const h: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
    var r = try fit(std.testing.allocator, h, &.{ 0, 1 }, &.{ -2, 3 }, .{ .target_rows = 1 });
    defer r.deinit();
    try std.testing.expectApproxEqAbs(@log(@as(f64, 4)), r.bias[0], 1e-10);
    const full: Head = .{ .keys = &.{ 0, 0, 0, 0, 1 }, .values = &.{ 2, 4, 6, 8, 10 }, .rows = 5, .dim = 1, .scale = 1 };
    const compact: Head = .{ .keys = &.{ 0, 1 }, .values = &.{ r.values[0], 10 }, .bias = &.{ r.bias[0], 0 }, .rows = 2, .dim = 1, .scale = 1 };
    var full_p: [5]f64 = undefined;
    var compact_p: [2]f64 = undefined;
    var full_y: [1]f64 = undefined;
    var compact_y: [1]f64 = undefined;
    const a = try attend(full, &.{0.7}, &full_p, &full_y);
    const b = try attend(compact, &.{0.7}, &compact_p, &compact_y);
    try std.testing.expectApproxEqAbs(a, b, 1e-10);
    try std.testing.expectApproxEqAbs(full_y[0], compact_y[0], 1e-7);
}

test "attention_match repeated compaction incorporates existing extreme biases" {
    const h: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 2, 2, 2 }, .bias = &.{ 1000, 1000, 1000, 1000 }, .rows = 4, .dim = 1, .scale = 1 };
    var first = try fit(std.testing.allocator, h, &.{ 0, 1 }, &.{2}, .{ .target_rows = 2 });
    defer first.deinit();
    var second = try fit(std.testing.allocator, first.head(h), &.{ 0, 1 }, &.{2}, .{ .target_rows = 1 });
    defer second.deinit();
    try std.testing.expectApproxEqAbs(1000 + @log(@as(f64, 4)), second.bias[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2), second.values[0], 1e-7);
}

test "attention_match rejects unseen-query mass loss and invalid inputs" {
    const h: Head = .{ .keys = &.{ -1, 1 }, .values = &.{ 1, 1 }, .rows = 2, .dim = 1, .scale = 1 };
    try std.testing.expectError(error.CompactionFitRejected, fit(std.testing.allocator, h, &.{0}, &.{10}, .{ .target_rows = 1 }));
    try std.testing.expectError(error.CompactionWorkspaceLimit, fit(std.testing.allocator, h, &.{0}, &.{1}, .{ .target_rows = 1, .max_workspace_bytes = 1 }));
    try std.testing.expectError(error.NonFiniteAttention, fit(std.testing.allocator, h, &.{std.math.nan(f64)}, &.{1}, .{ .target_rows = 1 }));
    try std.testing.expectError(error.InvalidAttentionShape, workspaceBytes(std.math.maxInt(usize), 2, 2, 2));
}

test "attention_match rejected fit retains diagnostics and resets them on early failure" {
    const h: Head = .{ .keys = &.{ -1, 1 }, .values = &.{ 1, 1 }, .rows = 2, .dim = 1, .scale = 1 };
    var metrics: Metrics = .{};
    try std.testing.expectError(error.CompactionFitRejected, fit(std.testing.allocator, h, &.{0}, &.{10}, .{ .target_rows = 1, .metrics = &metrics }));
    try std.testing.expect(metrics.max_relative_mass_error > 0.99);
    try std.testing.expect(metrics.quality_evaluated);
    try std.testing.expect(metrics.nnls_sweeps > 0);
    try std.testing.expect(metrics.relative_output_rmse < 0.05);
    try std.testing.expectError(error.CompactionWorkspaceLimit, fit(std.testing.allocator, h, &.{0}, &.{10}, .{ .target_rows = 1, .max_workspace_bytes = 1, .metrics = &metrics }));
    try std.testing.expectEqualDeep(Metrics{}, metrics);
}

fn allocationFailure(allocator: std.mem.Allocator) !void {
    const h: Head = .{ .keys = &.{ 0, 0, 0 }, .values = &.{ 1, 2, 3 }, .rows = 3, .dim = 1, .scale = 1 };
    var r = try fit(allocator, h, &.{ 0, 1 }, &.{2}, .{ .target_rows = 1 });
    defer r.deinit();
}
test "attention_match allocation failures leave source intact and leak no memory" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}

test "attention_match QR recovers multiple value columns without normal equations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var result: [4]f64 = undefined;
    // A * [[2, -1], [4, 3]] = Y. A has full column rank.
    try fitValues(arena.allocator(), &.{ 1, 0, 0, 1, 0.5, 0.5 }, &.{ 2, -1, 4, 3, 3, 1 }, 3, 2, 2, 1e-12, .{}, &result);
    for (result, [_]f64{ 2, -1, 4, 3 }) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-10);
    // Duplicate columns are rank deficient without ridge but must stay finite.
    try fitValues(arena.allocator(), &.{ 0.5, 0.5, 0.5, 0.5 }, &.{ 3, 1, 3, 1 }, 2, 2, 2, 1e-8, .{}, &result);
    try std.testing.expectApproxEqAbs(@as(f64, 3), (result[0] + result[2]) / 2, 1e-7);
    try std.testing.expectApproxEqAbs(@as(f64, 1), (result[1] + result[3]) / 2, 1e-7);
}

test "attention_match cancellation interrupts fitting and unwinds allocations" {
    const Check = struct {
        calls: usize = 0,
        fn check(ctx: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.calls += 1;
            if (self.calls >= 4) return error.Cancelled;
        }
    };
    var check: Check = .{};
    const h: Head = .{ .keys = &.{ 0, 0, 0 }, .values = &.{ 1, 2, 3 }, .rows = 3, .dim = 1, .scale = 1 };
    try std.testing.expectError(error.Cancelled, fit(std.testing.allocator, h, &.{ 0, 1 }, &.{2}, .{
        .target_rows = 1,
        .control = .{ .context = &check, .check_fn = Check.check },
    }));
    try std.testing.expectEqual(@as(usize, 4), check.calls);
}

test "attention_match biases are added after softcap" {
    const h: Head = .{ .keys = &.{10}, .values = &.{3}, .bias = &.{1000}, .rows = 1, .dim = 1, .scale = 1, .softcap = 2 };
    var p: [1]f64 = undefined;
    const mass = try attend(h, &.{1}, &p, null);
    try std.testing.expectApproxEqAbs(1000 + 2 * std.math.tanh(@as(f64, 5)), mass, 1e-10);
}

test "attention_match NNLS satisfies nonnegative optimum and rejects unconverged fit" {
    var weights: [2]f64 = undefined;
    var residual: [2]f64 = undefined;
    var norms: [2]f64 = undefined;
    const matrix = [_]f64{ 1, 2, 2, 3 };
    // Unconstrained LS gives [-1, 1]; NNLS gives [0, 5/13]. Clipping
    // unconstrained LS to zero would NOT solve this constrained problem.
    _ = try fitMass(&matrix, 2, 2, &weights, &residual, &norms, .{ .target_rows = 2 });
    try std.testing.expectApproxEqAbs(@as(f64, 0), weights[0], 1e-7);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0 / 13.0), weights[1], 1e-7);
    try std.testing.expectError(error.MassFitDidNotConverge, fitMass(&matrix, 2, 2, &weights, &residual, &norms, .{
        .target_rows = 2,
        .max_sweeps = 1,
    }));
}

test "attention_match active NNLS enforces boundary optimum and ignores duplicate zero columns" {
    var w: [3]f64 = undefined;
    var r: [2]f64 = undefined;
    var norms: [3]f64 = undefined;
    const a = [_]f64{ 0.1, 0.2, 0, 0.5, 0.9, 0 };
    _ = try fitMassActive(std.testing.allocator, &a, 2, 3, &w, &r, &norms, .{ .target_rows = 1 }, false);
    try std.testing.expectEqual(@as(f64, 0), w[0]);
    try std.testing.expectApproxEqAbs(1.1 / 0.85, w[1], 1e-12);
    try std.testing.expectEqual(@as(f64, 0), w[2]);
    try std.testing.expect(a[0] * r[0] + a[3] * r[1] <= 0);
    const duplicate = [_]f64{ 0.5, 0.5, 0, 0.5, 0.5, 0 };
    _ = try fitMassActive(std.testing.allocator, &duplicate, 2, 3, &w, &r, &norms, .{ .target_rows = 1 }, false);
    try std.testing.expectApproxEqAbs(2, w[0] + w[1], 1e-12);
    try std.testing.expectEqual(@as(f64, 0), w[2]);
}

test "attention_match active NNLS bounds work and rejects nonfinite warm state" {
    var w: [2]f64 = undefined;
    var r: [2]f64 = undefined;
    var norms: [2]f64 = undefined;
    const a = [_]f64{ 1, 0, 0, 1 };
    try std.testing.expectError(error.MassFitDidNotConverge, fitMassActive(std.testing.allocator, &a, 2, 2, &w, &r, &norms, .{ .target_rows = 2, .max_sweeps = 1 }, false));
    _ = try fitMassActive(std.testing.allocator, &a, 2, 2, &w, &r, &norms, .{ .target_rows = 2 }, true);
    try std.testing.expectApproxEqAbs(1, w[0], 1e-12);
    try std.testing.expectApproxEqAbs(1, w[1], 1e-12);
    w[0] = std.math.nan(f64);
    try std.testing.expectError(error.NonFiniteAttention, fitMassActive(std.testing.allocator, &a, 2, 2, &w, &r, &norms, .{ .target_rows = 2 }, true));
}

fn activeAllocationFailure(allocator: std.mem.Allocator) !void {
    const h: Head = .{ .keys = &.{ -1, 0, 0.2, 1 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
    var result = try fit(allocator, h, &.{ -0.5, 0, 0.5, 1 }, &.{0.25}, .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .max_relative_mass_error = 1, .max_relative_output_rmse = 1 });
    defer result.deinit();
}
test "attention_match residual selection active solve unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, activeAllocationFailure, .{});
}

test "attention_match residual QR handles dependent columns and more retained rows than queries" {
    const h: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 2, 2, 2 }, .rows = 4, .dim = 1, .scale = 1 };
    var r = try fit(std.testing.allocator, h, &.{ 0, 1 }, &.{ -1, 2 }, .{ .target_rows = 3, .selection = .residual_omp, .mass_solver = .active_set });
    defer r.deinit();
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, r.source_indices);
    try std.testing.expectEqual(@as(usize, 3), r.metrics.selection_steps);
    try std.testing.expect(r.metrics.selection_active_solves > 0);
    try std.testing.expect(r.metrics.max_relative_mass_error < 1e-8);
    try std.testing.expect(r.metrics.relative_output_rmse < 1e-8);
}

test "attention_match repeated active solves return fixed workspace to its mark" {
    var memory: [4096]u8 align(8) = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&memory);
    var w: [3]f64 = undefined;
    var r: [2]f64 = undefined;
    var norms: [3]f64 = undefined;
    const a = [_]f64{ 0.1, 0.2, 0, 0.5, 0.9, 0 };
    for (0..32) |_| {
        _ = try fitMassActive(fixed.allocator(), &a, 2, 3, &w, &r, &norms, .{ .target_rows = 1 }, false);
        try std.testing.expectEqual(@as(usize, 0), fixed.end_index);
    }
}

const TestBackend = struct {
    calls: usize = 0,
    largest_batch: usize = 0,
    extra: usize = 0,
    invalid_indices: bool = false,
    nonfinite: bool = false,
    fn workspace(raw: ?*anyopaque, _: usize, _: usize, _: usize, _: usize) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        return self.extra;
    }
    fn attention(raw: ?*anyopaque, allocator: std.mem.Allocator, h: Head, queries: []const f64, p: []f64, y: ?[]f64, masses: ?[]f64, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        self.largest_batch = @max(self.largest_batch, queries.len / h.dim);
        try attendBatch(allocator, h, queries, p, y, masses, .{ .target_rows = 1, .control = control });
        if (self.nonfinite) p[0] = std.math.nan(f64);
    }
    fn values(_: ?*anyopaque, allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, control: Control, out: []f64) !void {
        try fitValues(allocator, a, y, q, m, d, ridge, control, out);
    }
    fn select(raw: ?*anyopaque, _: std.mem.Allocator, _: []const f64, _: []const f64, _: usize, n: usize, indices: []usize, weights: []f64, _: Config) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        for (indices, weights, 0..) |*idx, *weight, i| {
            idx.* = if (self.invalid_indices) n else i;
            weight.* = 0; // Native final NNLS must repair this unfit warm state.
        }
        return 0;
    }
    fn backend(self: *@This()) Backend {
        return .{ .context = self, .workspace_fn = workspace, .attend_fn = attention, .values_fn = values, .select_fn = select };
    }
};

fn backendAllocationFailure(allocator: std.mem.Allocator) !void {
    var backend: TestBackend = .{};
    const source: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
    const heldout: [131]f64 = @splat(1);
    var result = try fit(allocator, source, &.{0}, &heldout, .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .backend = backend.backend() });
    defer result.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 5), result.values[0], 1e-6);
    try std.testing.expectEqual(@as(usize, 64), backend.largest_batch);
    try std.testing.expectEqual(@as(usize, 7), backend.calls);
}

test "attention_match backend bounds heldout batches independently of training count and unwinds failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, backendAllocationFailure, .{});
}

test "attention_match backend cannot bypass shape finite or workspace gates" {
    var backend: TestBackend = .{ .invalid_indices = true };
    const source: Head = .{ .keys = &.{ 0, 0 }, .values = &.{ 2, 4 }, .rows = 2, .dim = 1, .scale = 1 };
    var cfg: Config = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set, .backend = backend.backend() };
    try std.testing.expectError(error.InvalidSelectionResult, fit(std.testing.allocator, source, &.{0}, &.{1}, cfg));
    backend.invalid_indices = false;
    backend.nonfinite = true;
    try std.testing.expectError(error.NonFiniteAttention, fit(std.testing.allocator, source, &.{0}, &.{1}, cfg));
    backend.nonfinite = false;
    backend.extra = cfg.max_workspace_bytes;
    backend.calls = 0;
    try std.testing.expectError(error.CompactionWorkspaceLimit, fit(std.testing.allocator, source, &.{0}, &.{1}, cfg));
    try std.testing.expectEqual(@as(usize, 0), backend.calls);
    cfg.max_workspace_bytes = 0;
    try std.testing.expectError(error.CompactionWorkspaceLimit, fit(std.testing.allocator, source, &.{0}, &.{1}, cfg));
}

fn backend256AllocationFailure(allocator: std.mem.Allocator) !void {
    var backend: TestBackend = .{};
    var implementation = backend.backend();
    implementation.validation_batch = .rows_256;
    const source: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
    const heldout: [515]f64 = @splat(1);
    var result = try fit(allocator, source, &.{0}, &heldout, .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .backend = implementation });
    defer result.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 5), result.values[0], 1e-6);
    try std.testing.expectEqual(@as(usize, 256), backend.largest_batch);
    try std.testing.expectEqual(@as(usize, 7), backend.calls);
}
test "attention_match bounded 256-row validation handles tails and unwinds allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, backend256AllocationFailure, .{});
}

const SummaryTestBackend = struct {
    fn summary(raw: ?*anyopaque, allocator: std.mem.Allocator, h: Head, queries: []const f64, y: []f64, masses: []f64, control: Control) !void {
        const state: *TestBackend = @ptrCast(@alignCast(raw.?));
        const p = try allocator.alloc(f64, queries.len / h.dim * h.rows);
        defer allocator.free(p);
        try TestBackend.attention(raw, allocator, h, queries, p, y, masses, control);
        if (state.nonfinite) masses[0] = std.math.nan(f64);
    }
};
fn summaryAllocationFailure(allocator: std.mem.Allocator) !void {
    var backend: TestBackend = .{};
    var implementation = backend.backend();
    implementation.validation_batch = .rows_256;
    implementation.summary_fn = SummaryTestBackend.summary;
    const source: Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
    const compact: Head = .{ .keys = &.{ 0, 0 }, .values = &.{ 5, 5 }, .bias = &.{ @log(@as(f64, 2)), @log(@as(f64, 2)) }, .rows = 2, .dim = 1, .scale = 1 };
    const queries: [515]f64 = @splat(1);
    const cfg: Config = .{ .target_rows = 2, .backend = implementation };
    const actual = try validationMetrics(allocator, source, compact, &queries, cfg);
    try std.testing.expectEqual(@as(usize, 256), backend.largest_batch);
    try std.testing.expectEqual(@as(usize, 6), backend.calls);
    var reference_cfg = cfg;
    reference_cfg.backend.?.summary_fn = null;
    const expected = try validationMetrics(allocator, source, compact, &queries, reference_cfg);
    try std.testing.expectEqual(expected.max_relative_mass_error, actual.max_relative_mass_error);
    try std.testing.expectEqual(expected.relative_output_rmse, actual.relative_output_rmse);
    try std.testing.expectEqual(@as(usize, 256 * 6 * 8), try validationBytes(4, 2, 1, queries.len, reference_cfg) - try validationBytes(4, 2, 1, queries.len, cfg));
}
test "attention_match output-only validation preserves metrics and unwinds allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, summaryAllocationFailure, .{});
}

test "attention_match output-only validation rejects nonfinite accelerated summaries" {
    var backend: TestBackend = .{ .nonfinite = true };
    var implementation = backend.backend();
    implementation.summary_fn = SummaryTestBackend.summary;
    const h: Head = .{ .keys = &.{0}, .values = &.{2}, .rows = 1, .dim = 1, .scale = 1 };
    var targets: [1]f64 = undefined;
    var masses: [1]f64 = undefined;
    try std.testing.expectError(error.NonFiniteAttention, attendSummary(std.testing.allocator, h, &.{1}, &targets, &masses, .{ .target_rows = 1, .backend = implementation }));
}
