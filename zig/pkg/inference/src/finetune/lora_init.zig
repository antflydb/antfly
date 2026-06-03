// Copyright 2026 Antfly, Inc.
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

// LoRA adapter initialization: PiSSA and LoftQ variants.
//
// Pure-Zig linear algebra (no LAPACK): randomized SVD built on a Jacobi
// eigensolver over the Gram matrix. See PiSSA (Meng et al., 2024) and LoftQ
// (Li et al., 2023) for algorithm background.

const std = @import("std");

pub const Matrix = struct {
    rows: usize,
    cols: usize,
    data: []f32,
};

pub const PiSSAResult = struct {
    a: []f32,
    b: []f32,
    residual: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PiSSAResult) void {
        self.allocator.free(self.a);
        self.allocator.free(self.b);
        self.allocator.free(self.residual);
        self.* = undefined;
    }
};

pub const QuantizeFn = *const fn (allocator: std.mem.Allocator, w: []const f32) anyerror![]f32;

pub const LoftQResult = PiSSAResult;
pub const EVAResult = PiSSAResult;
pub const LoRAGAResult = PiSSAResult;

fn matmul(
    out: []f32,
    a: []const f32,
    b: []const f32,
    m: usize,
    k: usize,
    n: usize,
) void {
    @memset(out, 0.0);
    var i: usize = 0;
    while (i < m) : (i += 1) {
        var p: usize = 0;
        while (p < k) : (p += 1) {
            const aip = a[i * k + p];
            if (aip == 0.0) continue;
            var j: usize = 0;
            while (j < n) : (j += 1) {
                out[i * n + j] += aip * b[p * n + j];
            }
        }
    }
}

fn matmulTN(
    out: []f32,
    a: []const f32,
    b: []const f32,
    m: usize,
    k: usize,
    n: usize,
) void {
    // out[m,n] = a^T[m,k] @ b[k,n] where a is stored as [k,m]
    @memset(out, 0.0);
    var p: usize = 0;
    while (p < k) : (p += 1) {
        var i: usize = 0;
        while (i < m) : (i += 1) {
            const aip = a[p * m + i];
            if (aip == 0.0) continue;
            var j: usize = 0;
            while (j < n) : (j += 1) {
                out[i * n + j] += aip * b[p * n + j];
            }
        }
    }
}

fn matmulNT(
    out: []f32,
    a: []const f32,
    b: []const f32,
    m: usize,
    k: usize,
    n: usize,
) void {
    // out[m,n] = a[m,k] @ b^T[k,n] where b is stored as [n,k]
    @memset(out, 0.0);
    var i: usize = 0;
    while (i < m) : (i += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) {
            var s: f32 = 0.0;
            var p: usize = 0;
            while (p < k) : (p += 1) {
                s += a[i * k + p] * b[j * k + p];
            }
            out[i * n + j] = s;
        }
    }
}

fn modifiedGramSchmidt(q: []f32, rows: usize, cols: usize) void {
    // q is [rows, cols], orthonormalize columns in place.
    // If a column is numerically rank-deficient, zero it (no spurious unit vectors).
    var j: usize = 0;
    while (j < cols) : (j += 1) {
        // original norm for the relative tolerance
        var orig_norm: f32 = 0.0;
        {
            var i: usize = 0;
            while (i < rows) : (i += 1) {
                const v = q[i * cols + j];
                orig_norm += v * v;
            }
            orig_norm = @sqrt(orig_norm);
        }
        // subtract projections on previous columns (twice for numerical stability)
        var pass: u32 = 0;
        while (pass < 2) : (pass += 1) {
            var p: usize = 0;
            while (p < j) : (p += 1) {
                var dot: f32 = 0.0;
                var i: usize = 0;
                while (i < rows) : (i += 1) dot += q[i * cols + p] * q[i * cols + j];
                i = 0;
                while (i < rows) : (i += 1) q[i * cols + j] -= dot * q[i * cols + p];
            }
        }
        var norm: f32 = 0.0;
        var i: usize = 0;
        while (i < rows) : (i += 1) {
            const v = q[i * cols + j];
            norm += v * v;
        }
        norm = @sqrt(norm);
        const tol = 1e-6 * (if (orig_norm > 0.0) orig_norm else 1.0);
        if (norm > tol) {
            const inv = 1.0 / norm;
            i = 0;
            while (i < rows) : (i += 1) q[i * cols + j] *= inv;
        } else {
            i = 0;
            while (i < rows) : (i += 1) q[i * cols + j] = 0.0;
        }
    }
}

fn gaussian(rng: *std.Random.DefaultPrng) f32 {
    const r = rng.random();
    var ua = r.float(f32);
    const ub = r.float(f32);
    if (ua < 1e-30) ua = 1e-30;
    const mag = @sqrt(-2.0 * @log(ua));
    return mag * @cos(2.0 * std.math.pi * ub);
}

fn fillGaussian(buf: []f32, seed: u64) void {
    var rng = std.Random.DefaultPrng.init(seed);
    for (buf) |*x| x.* = gaussian(&rng);
}

/// Classical Jacobi eigendecomposition of a symmetric n×n matrix.
/// `a` is n*n row-major, overwritten with diagonal eigenvalues (off-diagonal ~0).
/// `v` (n*n) receives eigenvectors as columns.
fn jacobiEigen(a: []f32, v: []f32, n: usize) void {
    // initialize v = I
    @memset(v, 0.0);
    var i: usize = 0;
    while (i < n) : (i += 1) v[i * n + i] = 1.0;

    const max_sweeps: usize = 100;
    var sweep: usize = 0;
    while (sweep < max_sweeps) : (sweep += 1) {
        var off: f64 = 0.0;
        var diag: f64 = 0.0;
        i = 0;
        while (i < n) : (i += 1) {
            var j: usize = 0;
            while (j < n) : (j += 1) {
                const x: f64 = @floatCast(a[i * n + j]);
                if (i == j) diag += x * x else off += x * x;
            }
        }
        if (off <= 1e-18 * diag + 1e-30) break;

        var p: usize = 0;
        while (p < n - 1) : (p += 1) {
            var q: usize = p + 1;
            while (q < n) : (q += 1) {
                const apq = a[p * n + q];
                if (@abs(apq) < 1e-20) continue;
                const app = a[p * n + p];
                const aqq = a[q * n + q];
                const theta = (aqq - app) / (2.0 * apq);
                var t: f32 = undefined;
                if (theta >= 0.0) {
                    t = 1.0 / (theta + @sqrt(1.0 + theta * theta));
                } else {
                    t = 1.0 / (theta - @sqrt(1.0 + theta * theta));
                }
                const c = 1.0 / @sqrt(1.0 + t * t);
                const s = t * c;

                a[p * n + p] = app - t * apq;
                a[q * n + q] = aqq + t * apq;
                a[p * n + q] = 0.0;
                a[q * n + p] = 0.0;

                var r: usize = 0;
                while (r < n) : (r += 1) {
                    if (r != p and r != q) {
                        const arp = a[r * n + p];
                        const arq = a[r * n + q];
                        a[r * n + p] = c * arp - s * arq;
                        a[p * n + r] = a[r * n + p];
                        a[r * n + q] = s * arp + c * arq;
                        a[q * n + r] = a[r * n + q];
                    }
                    const vrp = v[r * n + p];
                    const vrq = v[r * n + q];
                    v[r * n + p] = c * vrp - s * vrq;
                    v[r * n + q] = s * vrp + c * vrq;
                }
            }
        }
    }
}

/// Truncated SVD of W [m,n] via randomized range-finding.
/// Allocates and returns U [m, rank], S [rank], VT [rank, n].
/// Caller owns all three buffers.
fn truncatedSvd(
    allocator: std.mem.Allocator,
    w: []const f32,
    m: usize,
    n: usize,
    rank: usize,
    power_iters: u32,
    seed: u64,
) !struct { u: []f32, s: []f32, vt: []f32 } {
    const oversample: usize = 5;
    var k = rank + oversample;
    if (k > n) k = n;
    if (k > m) k = m;
    std.debug.assert(rank <= k);

    // Omega: [n, k]
    const omega = try allocator.alloc(f32, n * k);
    defer allocator.free(omega);
    fillGaussian(omega, seed);

    // Y = W @ Omega : [m, k]
    const y = try allocator.alloc(f32, m * k);
    defer allocator.free(y);
    matmul(y, w, omega, m, n, k);

    // Power iterations: Y = W @ (W^T @ Y), orthonormalize between.
    var power: u32 = 0;
    while (power < power_iters) : (power += 1) {
        modifiedGramSchmidt(y, m, k);
        // z = W^T @ Y : [n, k]
        const z = try allocator.alloc(f32, n * k);
        defer allocator.free(z);
        matmulTN(z, w, y, n, m, k);
        modifiedGramSchmidt(z, n, k);
        // Y = W @ z : [m, k]
        matmul(y, w, z, m, n, k);
    }
    modifiedGramSchmidt(y, m, k);
    // Q = y : [m, k]

    // B_small = Q^T @ W : [k, n]
    const b_small = try allocator.alloc(f32, k * n);
    defer allocator.free(b_small);
    matmulTN(b_small, y, w, k, m, n);

    // Form M = B_small @ B_small^T : [k, k]
    const mm = try allocator.alloc(f32, k * k);
    defer allocator.free(mm);
    var i: usize = 0;
    while (i < k) : (i += 1) {
        var j: usize = 0;
        while (j < k) : (j += 1) {
            var s: f32 = 0.0;
            var p: usize = 0;
            while (p < n) : (p += 1) s += b_small[i * n + p] * b_small[j * n + p];
            mm[i * k + j] = s;
        }
    }

    // Eigendecompose M : eigvals are squares of singular values of B_small.
    const eigvec = try allocator.alloc(f32, k * k);
    defer allocator.free(eigvec);
    jacobiEigen(mm, eigvec, k);

    // Extract eigenvalues and sort descending.
    var idx = try allocator.alloc(usize, k);
    defer allocator.free(idx);
    var eigvals = try allocator.alloc(f32, k);
    defer allocator.free(eigvals);
    i = 0;
    while (i < k) : (i += 1) {
        idx[i] = i;
        eigvals[i] = mm[i * k + i];
    }
    // simple selection sort on idx by eigvals desc
    i = 0;
    while (i < k) : (i += 1) {
        var best = i;
        var j: usize = i + 1;
        while (j < k) : (j += 1) {
            if (eigvals[idx[j]] > eigvals[idx[best]]) best = j;
        }
        const tmp = idx[i];
        idx[i] = idx[best];
        idx[best] = tmp;
    }

    // Build output S, U_small (= selected eigenvectors as columns of [k, rank]),
    // and V_small_T = (1/s) * U_small^T @ B_small (shape [rank, n]).
    const s_out = try allocator.alloc(f32, rank);
    errdefer allocator.free(s_out);

    // u_small as [k, rank]
    const u_small = try allocator.alloc(f32, k * rank);
    defer allocator.free(u_small);

    var r: usize = 0;
    while (r < rank) : (r += 1) {
        const ev = eigvals[idx[r]];
        const sv: f32 = if (ev > 0.0) @sqrt(ev) else 0.0;
        s_out[r] = sv;
        var row: usize = 0;
        while (row < k) : (row += 1) {
            u_small[row * rank + r] = eigvec[row * k + idx[r]];
        }
    }

    // vt_out = diag(1/s) @ u_small^T @ b_small : [rank, n]
    const vt_out = try allocator.alloc(f32, rank * n);
    errdefer allocator.free(vt_out);
    // temp = u_small^T @ b_small : [rank, n]
    matmulTN(vt_out, u_small, b_small, rank, k, n);
    r = 0;
    while (r < rank) : (r += 1) {
        const sv = s_out[r];
        const inv: f32 = if (sv > 1e-20) 1.0 / sv else 0.0;
        var j: usize = 0;
        while (j < n) : (j += 1) vt_out[r * n + j] *= inv;
    }

    // U = Q @ u_small : [m, rank]
    const u_out = try allocator.alloc(f32, m * rank);
    errdefer allocator.free(u_out);
    matmul(u_out, y, u_small, m, k, rank);

    return .{ .u = u_out, .s = s_out, .vt = vt_out };
}

pub fn pissaInit(
    allocator: std.mem.Allocator,
    w: []const f32,
    out_features: usize,
    in_features: usize,
    rank: usize,
    power_iters: u32,
    seed: u64,
) !PiSSAResult {
    std.debug.assert(w.len == out_features * in_features);
    std.debug.assert(rank > 0);
    std.debug.assert(rank <= @min(out_features, in_features));

    const svd = try truncatedSvd(allocator, w, out_features, in_features, rank, power_iters, seed);
    defer allocator.free(svd.u);
    defer allocator.free(svd.s);
    defer allocator.free(svd.vt);

    // A = sqrt(S) * V^T : [rank, in_features]
    const a = try allocator.alloc(f32, rank * in_features);
    errdefer allocator.free(a);
    // B = U * sqrt(S) : [out_features, rank]
    const b = try allocator.alloc(f32, out_features * rank);
    errdefer allocator.free(b);

    var r: usize = 0;
    while (r < rank) : (r += 1) {
        const sv = svd.s[r];
        const sqrt_s: f32 = if (sv > 0.0) @sqrt(sv) else 0.0;
        var j: usize = 0;
        while (j < in_features) : (j += 1) {
            a[r * in_features + j] = sqrt_s * svd.vt[r * in_features + j];
        }
        var i: usize = 0;
        while (i < out_features) : (i += 1) {
            b[i * rank + r] = sqrt_s * svd.u[i * rank + r];
        }
    }

    // residual = W - B @ A
    const residual = try allocator.alloc(f32, out_features * in_features);
    errdefer allocator.free(residual);
    matmul(residual, b, a, out_features, rank, in_features);
    var idx: usize = 0;
    while (idx < residual.len) : (idx += 1) {
        residual[idx] = w[idx] - residual[idx];
    }

    return .{
        .a = a,
        .b = b,
        .residual = residual,
        .allocator = allocator,
    };
}

pub fn loftqInit(
    allocator: std.mem.Allocator,
    w: []const f32,
    out_features: usize,
    in_features: usize,
    rank: usize,
    num_iter: u32,
    power_iters: u32,
    quantize: QuantizeFn,
    seed: u64,
) !LoftQResult {
    std.debug.assert(w.len == out_features * in_features);
    std.debug.assert(rank > 0);
    std.debug.assert(rank <= @min(out_features, in_features));
    std.debug.assert(num_iter >= 1);

    const total = out_features * in_features;

    // Seed A,B from a PiSSA pass on W so that with identity quantize the loop
    // is already at its fixed point (matches PiSSA). With real quantization
    // the subsequent iterations refine around the quantization error.
    const init_svd = try truncatedSvd(allocator, w, out_features, in_features, rank, power_iters, seed);
    defer allocator.free(init_svd.u);
    defer allocator.free(init_svd.s);
    defer allocator.free(init_svd.vt);

    var a = try allocator.alloc(f32, rank * in_features);
    errdefer allocator.free(a);
    var b = try allocator.alloc(f32, out_features * rank);
    errdefer allocator.free(b);
    {
        var r: usize = 0;
        while (r < rank) : (r += 1) {
            const sv = init_svd.s[r];
            const sqrt_s: f32 = if (sv > 0.0) @sqrt(sv) else 0.0;
            var j: usize = 0;
            while (j < in_features) : (j += 1) {
                a[r * in_features + j] = sqrt_s * init_svd.vt[r * in_features + j];
            }
            var ii: usize = 0;
            while (ii < out_features) : (ii += 1) {
                b[ii * rank + r] = sqrt_s * init_svd.u[ii * rank + r];
            }
        }
    }

    var w_q: []f32 = try allocator.alloc(f32, total);
    errdefer allocator.free(w_q);
    @memset(w_q, 0.0);

    const ba = try allocator.alloc(f32, total);
    defer allocator.free(ba);
    const residual = try allocator.alloc(f32, total);
    defer allocator.free(residual);

    var it: u32 = 0;
    while (it < num_iter) : (it += 1) {
        // W_res = W - B@A
        matmul(ba, b, a, out_features, rank, in_features);
        var i: usize = 0;
        while (i < total) : (i += 1) residual[i] = w[i] - ba[i];

        // W_q = quantize(W_res)
        const new_wq = try quantize(allocator, residual);
        allocator.free(w_q);
        w_q = new_wq;

        // diff = W - W_q
        i = 0;
        while (i < total) : (i += 1) residual[i] = w[i] - w_q[i];

        // truncated SVD of diff
        const svd = try truncatedSvd(allocator, residual, out_features, in_features, rank, power_iters, seed +% (it + 1));
        defer allocator.free(svd.u);
        defer allocator.free(svd.s);
        defer allocator.free(svd.vt);

        var r: usize = 0;
        while (r < rank) : (r += 1) {
            const sv = svd.s[r];
            const sqrt_s: f32 = if (sv > 0.0) @sqrt(sv) else 0.0;
            var j: usize = 0;
            while (j < in_features) : (j += 1) {
                a[r * in_features + j] = sqrt_s * svd.vt[r * in_features + j];
            }
            var ii: usize = 0;
            while (ii < out_features) : (ii += 1) {
                b[ii * rank + r] = sqrt_s * svd.u[ii * rank + r];
            }
        }
    }

    return .{
        .a = a,
        .b = b,
        .residual = w_q,
        .allocator = allocator,
    };
}

/// EVA initialization from an activation covariance matrix X^T X.
///
/// `activation_covariance` is [in_features, in_features] row-major. EVA uses
/// the leading activation principal directions for LoRA A and leaves LoRA B at
/// zero so the initialized adapter is function-preserving while A is data-aware.
pub fn evaInit(
    allocator: std.mem.Allocator,
    activation_covariance: []const f32,
    out_features: usize,
    in_features: usize,
    rank: usize,
    power_iters: u32,
    seed: u64,
) !EVAResult {
    std.debug.assert(activation_covariance.len == in_features * in_features);
    std.debug.assert(rank > 0);
    std.debug.assert(rank <= in_features);

    const svd = try truncatedSvd(allocator, activation_covariance, in_features, in_features, rank, power_iters, seed);
    defer allocator.free(svd.u);
    defer allocator.free(svd.s);
    defer allocator.free(svd.vt);

    const a = try allocator.alloc(f32, rank * in_features);
    errdefer allocator.free(a);
    const b = try allocator.alloc(f32, out_features * rank);
    errdefer allocator.free(b);
    @memset(b, 0.0);

    var r: usize = 0;
    while (r < rank) : (r += 1) {
        var j: usize = 0;
        while (j < in_features) : (j += 1) {
            a[r * in_features + j] = svd.u[j * rank + r];
        }
    }

    const residual = try allocator.alloc(f32, out_features * in_features);
    errdefer allocator.free(residual);
    @memset(residual, 0.0);

    return .{
        .a = a,
        .b = b,
        .residual = residual,
        .allocator = allocator,
    };
}

/// LoRA-GA initialization from a dense gradient snapshot dL/dW.
///
/// The initialized low-rank update approximates `-step_scale * gradient`,
/// aligning the adapter direction with the first gradient signal instead of
/// using weight-only SVD or quantization-error proxies.
pub fn loraGaInit(
    allocator: std.mem.Allocator,
    gradient: []const f32,
    out_features: usize,
    in_features: usize,
    rank: usize,
    step_scale: f32,
    power_iters: u32,
    seed: u64,
) !LoRAGAResult {
    std.debug.assert(gradient.len == out_features * in_features);
    std.debug.assert(rank > 0);
    std.debug.assert(rank <= @min(out_features, in_features));

    var result = try pissaInit(allocator, gradient, out_features, in_features, rank, power_iters, seed);
    errdefer result.deinit();
    for (result.b) |*value| value.* *= -step_scale;
    for (result.residual) |*value| value.* *= -step_scale;
    return result;
}

// ---------------- tests ----------------

fn frobenius(a: []const f32) f32 {
    var s: f64 = 0.0;
    for (a) |x| s += @as(f64, x) * @as(f64, x);
    return @floatCast(@sqrt(s));
}

fn frobeniusDiff(a: []const f32, b: []const f32) f32 {
    var s: f64 = 0.0;
    for (a, 0..) |x, i| {
        const d = @as(f64, x) - @as(f64, b[i]);
        s += d * d;
    }
    return @floatCast(@sqrt(s));
}

test "pissaInit identity reconstructs exactly" {
    const allocator = std.testing.allocator;
    const n: usize = 4;
    const w = try allocator.alloc(f32, n * n);
    defer allocator.free(w);
    @memset(w, 0.0);
    var i: usize = 0;
    while (i < n) : (i += 1) w[i * n + i] = 1.0;

    var res = try pissaInit(allocator, w, n, n, n, 3, 42);
    defer res.deinit();

    // B @ A should equal I
    const ba = try allocator.alloc(f32, n * n);
    defer allocator.free(ba);
    matmul(ba, res.b, res.a, n, n, n);
    try std.testing.expect(frobeniusDiff(ba, w) < 1e-4);
    try std.testing.expect(frobenius(res.residual) < 1e-4);
}

test "pissaInit rank-1 outer product" {
    const allocator = std.testing.allocator;
    const m: usize = 5;
    const n: usize = 4;
    const u = [_]f32{ 1.0, 2.0, -1.5, 0.5, 3.0 };
    const v = [_]f32{ 0.7, -1.2, 2.1, 0.3 };
    const w = try allocator.alloc(f32, m * n);
    defer allocator.free(w);
    var i: usize = 0;
    while (i < m) : (i += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) {
            w[i * n + j] = u[i] * v[j];
        }
    }

    var res = try pissaInit(allocator, w, m, n, 1, 4, 7);
    defer res.deinit();

    const wf = frobenius(w);
    const rf = frobenius(res.residual);
    try std.testing.expect(rf / wf < 1e-4);
}

test "pissaInit random W near optimal rank-r error" {
    const allocator = std.testing.allocator;
    const m: usize = 12;
    const n: usize = 10;
    const rank: usize = 3;

    const w = try allocator.alloc(f32, m * n);
    defer allocator.free(w);
    var rng = std.Random.DefaultPrng.init(123);
    for (w) |*x| x.* = gaussian(&rng);

    // Compute "true" optimal rank-r error via a high-quality SVD
    // (more power iters, same randomized routine but converged).
    const ref_svd = try truncatedSvd(allocator, w, m, n, rank, 20, 999);
    defer allocator.free(ref_svd.u);
    defer allocator.free(ref_svd.s);
    defer allocator.free(ref_svd.vt);

    // Reconstruct ref = U diag(S) V^T
    const ref = try allocator.alloc(f32, m * n);
    defer allocator.free(ref);
    @memset(ref, 0.0);
    var i: usize = 0;
    while (i < m) : (i += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) {
            var s: f32 = 0.0;
            var r: usize = 0;
            while (r < rank) : (r += 1) {
                s += ref_svd.u[i * rank + r] * ref_svd.s[r] * ref_svd.vt[r * n + j];
            }
            ref[i * n + j] = s;
        }
    }
    const optimal_err = frobeniusDiff(w, ref);
    const wf = frobenius(w);

    var res = try pissaInit(allocator, w, m, n, rank, 3, 321);
    defer res.deinit();
    const rf = frobenius(res.residual);

    // Randomized SVD is approximate; allow slack.
    try std.testing.expect(rf <= optimal_err + 0.1 * wf);
}

fn identityQuantize(allocator: std.mem.Allocator, w: []const f32) anyerror![]f32 {
    const out = try allocator.alloc(f32, w.len);
    @memcpy(out, w);
    return out;
}

test "loftqInit with identity quantize matches pissa" {
    const allocator = std.testing.allocator;
    const m: usize = 6;
    const n: usize = 5;
    const rank: usize = 2;

    const w = try allocator.alloc(f32, m * n);
    defer allocator.free(w);
    var rng = std.Random.DefaultPrng.init(555);
    for (w) |*x| x.* = gaussian(&rng);

    // With identity quantize, W_q = W_res = W - B@A, then diff = W - W_q = B@A,
    // so SVD(diff) reproduces the current B@A and iteration converges in one step.
    var pissa = try pissaInit(allocator, w, m, n, rank, 4, 77);
    defer pissa.deinit();

    var lq = try loftqInit(allocator, w, m, n, rank, 2, 4, identityQuantize, 77);
    defer lq.deinit();

    // Compare B@A products (A and B are only defined up to sign/basis changes,
    // but the product is unique).
    const pissa_ba = try allocator.alloc(f32, m * n);
    defer allocator.free(pissa_ba);
    const lq_ba = try allocator.alloc(f32, m * n);
    defer allocator.free(lq_ba);
    matmul(pissa_ba, pissa.b, pissa.a, m, rank, n);
    matmul(lq_ba, lq.b, lq.a, m, rank, n);

    const diff = frobeniusDiff(pissa_ba, lq_ba);
    const ref = frobenius(pissa_ba);
    try std.testing.expect(diff / (ref + 1e-12) < 1e-3);
}

test "evaInit uses activation principal direction and preserves function" {
    const allocator = std.testing.allocator;
    const cov = [_]f32{
        4.0, 0.0, 0.0,
        0.0, 1.0, 0.0,
        0.0, 0.0, 0.5,
    };
    var res = try evaInit(allocator, &cov, 2, 3, 1, 4, 99);
    defer res.deinit();

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), @abs(res.a[0]), 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), res.a[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), res.a[2], 1e-4);
    for (res.b) |value| try std.testing.expectEqual(@as(f32, 0.0), value);
}

test "loraGaInit approximates negative gradient direction" {
    const allocator = std.testing.allocator;
    const grad = [_]f32{
        1.0, 0.0,
        0.0, 0.0,
    };
    var res = try loraGaInit(allocator, &grad, 2, 2, 1, 0.25, 4, 123);
    defer res.deinit();

    const ba = try allocator.alloc(f32, 4);
    defer allocator.free(ba);
    matmul(ba, res.b, res.a, 2, 1, 2);
    try std.testing.expectApproxEqAbs(@as(f32, -0.25), ba[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), ba[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), ba[2], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), ba[3], 1e-4);
}
