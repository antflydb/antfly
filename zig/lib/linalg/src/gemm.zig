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

//! SGEMM kernel core (no threading, no Io).
//!
//! Houses the comptime-selected register tile, the per-row `*AddSlice`
//! workers shared by the threaded entry points in `mod.zig`, and the
//! single-threaded `*Sequential` public entry points used by inner-loop
//! callers (most notably `attention.zig`).
//!
//! The split exists because:
//!   - `attention.zig` wants to call SGEMM from inside a per-(batch, head)
//!     loop, and `mod.zig` re-exports `attention` -- importing `mod.zig`
//!     from `attention.zig` would form a cycle that's fragile under future
//!     refactors.  Routing through this file keeps the dependency
//!     direction clean: `mod.zig -> gemm.zig`, `attention.zig -> gemm.zig`.
//!   - The threaded dispatchers in `mod.zig` (Sync via the futex pool, Io
//!     via `std.Io.Group.async`) can stay in one place without dragging
//!     the inner kernels along.
//!
//! Adding a new SGEMM shape: drop the kernel body here, expose a
//! `*Sequential` wrapper, and add a `*Sync` / `*` (Io) wrapper in `mod.zig`
//! using the shared `pickWorkers` / `dispatchJobs` machinery.

const std = @import("std");
const builtin = @import("builtin");
const primitives = @import("primitives.zig");

const vec_len = primitives.vec_len;

/// Register-tile dimensions for the inner SGEMM kernels.  Both
/// `sgemmTransBAddSlice` and `sgemmAddSlice` consume MR×NR vector
/// accumulators kept live across the K-loop; bigger tiles fill more of the
/// FMA pipeline (4-cycle latency on x86, ≥ 8 in-flight FMAs needed to
/// saturate two FMA pipes), at the cost of more vector registers.
///
/// We size the tile from the target's vector register file:
///
///   x86_64 + AVX-512F  : 32 zmm registers, vec_len=16  ->  MR=6 NR=2
///                         12 acc + ample headroom for B/A loads + temps.
///                         NR stays at 2 so the cached B panel is
///                         2 * K * 4 bytes -- still fits in 32KB L1d at
///                         CLIP-B (K=768, 6KB) and the largest BERT MLP
///                         shape we care about (K=3072, 24KB).
///   x86_64 + AVX2      : 16 ymm registers, vec_len=8   ->  MR=6 NR=2
///                         12 of 16 ymm.  Same NR=2 panel rule.
///   aarch64 (NEON)     : 32 q-registers, vec_len=8 (lowers to 2 q-regs
///                         each) -> MR=6 NR=2 = 24 q-regs of 32, leaves
///                         8 q-regs for loads/temps.
///   wasm / fallback    : MR=4 NR=2.  Conservative; matches the previous
///                         hand-tuned default.
///
/// The kernels do their own M-tail (rows < MR) and N-tail (cols < NR*V)
/// scalar fallback so any tile choice is correct, just possibly suboptimal.
pub const SgemmTile = struct {
    MR: usize,
    NR: usize,
};

pub const sgemm_tile: SgemmTile = blk: {
    if (builtin.cpu.arch == .x86_64) {
        if (std.Target.x86.featureSetHas(builtin.cpu.features, .avx512f))
            break :blk .{ .MR = 6, .NR = 2 };
        if (std.Target.x86.featureSetHas(builtin.cpu.features, .avx2))
            break :blk .{ .MR = 6, .NR = 2 };
    }
    if (builtin.cpu.arch == .aarch64) break :blk .{ .MR = 6, .NR = 2 };
    break :blk .{ .MR = 4, .NR = 2 };
};

/// Multiply C in-place by beta.  Inline because it sits at the top of every
/// public SGEMM entry point and beta=1.0 (the common case for GEMV
/// accumulation) short-circuits to nothing.
pub inline fn applyBeta(c_out: []f32, beta: f32) void {
    if (beta == 1.0) return;
    if (beta == 0.0) {
        @memset(c_out, 0);
        return;
    }
    const V = vec_len;
    const beta_v: @Vector(V, f32) = @splat(beta);
    var idx: usize = 0;
    while (idx + V <= c_out.len) : (idx += V) {
        const cv: @Vector(V, f32) = c_out[idx..][0..V].*;
        c_out[idx..][0..V].* = cv * beta_v;
    }
    while (idx < c_out.len) : (idx += 1) c_out[idx] *= beta;
}

// --- sgemm: C = alpha * A @ B + beta * C ----------------------------------

// Per-tile MR x (NR_VECS * V) micro-kernel.  Builds a tile of C in registers,
// then adds to C in one pass.  A_block is MR rows of A; lda = k.  B_block is
// the full panel of B starting at column j_base; ldb = n.
fn matmulRegisterTile(
    comptime MR: usize,
    comptime NR_VECS: usize,
    k: usize,
    alpha: f32,
    a: [*]const f32,
    lda: usize,
    b: [*]const f32,
    ldb: usize,
    c: [*]f32,
    ldc: usize,
) void {
    const V = vec_len;
    const Vec = @Vector(V, f32);
    const zero: Vec = @splat(0.0);

    var acc: [MR][NR_VECS]Vec = undefined;
    inline for (0..MR) |r| {
        inline for (0..NR_VECS) |c_idx| acc[r][c_idx] = zero;
    }

    var l: usize = 0;
    while (l < k) : (l += 1) {
        var bv: [NR_VECS]Vec = undefined;
        inline for (0..NR_VECS) |c_idx| {
            bv[c_idx] = b[l * ldb + c_idx * V ..][0..V].*;
        }
        inline for (0..MR) |r| {
            const av: Vec = @splat(alpha * a[r * lda + l]);
            inline for (0..NR_VECS) |c_idx| {
                acc[r][c_idx] = @mulAdd(Vec, av, bv[c_idx], acc[r][c_idx]);
            }
        }
    }

    inline for (0..MR) |r| {
        inline for (0..NR_VECS) |c_idx| {
            const cur: Vec = c[r * ldc + c_idx * V ..][0..V].*;
            c[r * ldc + c_idx * V ..][0..V].* = cur + acc[r][c_idx];
        }
    }
}

/// Per-worker compute slice for sgemm: process rows [m_start, m_end) and ADD
/// alpha * A[m_start:m_end] @ B into C.  Caller has already pre-scaled C by
/// beta.  Uses a Kc-blocked outer loop so a Kc-slice of B fits in L1.
pub fn sgemmAddSlice(
    m_start: usize,
    m_end: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    c_out: []f32,
) void {
    const V = vec_len;
    const MR_MAIN = sgemm_tile.MR;
    const NR_VECS_MAIN = sgemm_tile.NR;
    const NR_MAIN = NR_VECS_MAIN * V;
    const KC: usize = if (V >= 16) 128 else 256;

    var i: usize = m_start;
    const m_main_end = m_start + ((m_end - m_start) / MR_MAIN) * MR_MAIN;
    while (i < m_main_end) : (i += MR_MAIN) {
        var j: usize = 0;
        while (j + NR_MAIN <= n) : (j += NR_MAIN) {
            var kc: usize = 0;
            while (kc < k) {
                const kc_len = @min(KC, k - kc);
                matmulRegisterTile(
                    MR_MAIN,
                    NR_VECS_MAIN,
                    kc_len,
                    alpha,
                    a.ptr + i * k + kc,
                    k,
                    b.ptr + kc * n + j,
                    n,
                    c_out.ptr + i * n + j,
                    n,
                );
                kc += kc_len;
            }
        }
        // Tail in N: fall back to per-row vectorized loop.
        while (j < n) {
            const j_remaining = n - j;
            if (j_remaining >= V) {
                inline for (0..MR_MAIN) |r| {
                    var acc: @Vector(V, f32) = @splat(0.0);
                    var l: usize = 0;
                    while (l < k) : (l += 1) {
                        const av: @Vector(V, f32) = @splat(alpha * a[(i + r) * k + l]);
                        const bv: @Vector(V, f32) = b[l * n + j ..][0..V].*;
                        acc += av * bv;
                    }
                    const cv: @Vector(V, f32) = c_out[(i + r) * n + j ..][0..V].*;
                    c_out[(i + r) * n + j ..][0..V].* = cv + acc;
                }
                j += V;
            } else {
                inline for (0..MR_MAIN) |r| {
                    for (0..j_remaining) |jj| {
                        var sum: f32 = 0.0;
                        for (0..k) |l| {
                            sum += a[(i + r) * k + l] * b[l * n + j + jj];
                        }
                        c_out[(i + r) * n + j + jj] += alpha * sum;
                    }
                }
                j = n;
            }
        }
    }

    // Tail in M: rows that don't make a full MR block.  Row-at-a-time,
    // vectorized in N.
    while (i < m_end) : (i += 1) {
        const a_row = a[i * k ..];
        const c_row = c_out[i * n ..];
        for (0..k) |l| {
            const a_val: @Vector(V, f32) = @splat(alpha * a_row[l]);
            const b_row = b[l * n ..];
            var j: usize = 0;
            while (j + V <= n) : (j += V) {
                const bv: @Vector(V, f32) = b_row[j..][0..V].*;
                const cv: @Vector(V, f32) = c_row[j..][0..V].*;
                c_row[j..][0..V].* = cv + a_val * bv;
            }
            while (j < n) : (j += 1) {
                c_row[j] += a_val[0] * b_row[j];
            }
        }
    }
}

// --- sgemmTransB: C = alpha * A @ B^T + beta * C --------------------------
//
// One generic kernel parametrized by `BT` (the storage type of B) covers
// both the f32 and f16 weight paths.  The two differ only in how a single
// B vector is loaded: f32 reads directly, f16 loads `@Vector(V, f16)` and
// `@floatCast`s to f32 on-chip via F16C / AVX-512 FP16 / NEON fcvtl, so we
// don't pay an up-front f32 weight materialization.  Comptime branching
// resolves the difference at compile time -- the two `pub` AddSlice
// wrappers below produce identical codegen to hand-written copies.

inline fn loadBVecAsF32(comptime BT: type, comptime V: usize, b: []const BT, offset: usize) @Vector(V, f32) {
    if (BT == f32) return b[offset..][0..V].*;
    const bh: @Vector(V, BT) = b[offset..][0..V].*;
    return @floatCast(bh);
}

inline fn loadBScalarAsF32(comptime BT: type, b: []const BT, offset: usize) f32 {
    if (BT == f32) return b[offset];
    return @floatCast(b[offset]);
}

fn sgemmTransBAddSliceGeneric(
    comptime BT: type,
    m_start: usize,
    m_end: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const BT,
    c_out: []f32,
) void {
    const V = vec_len;
    const Vec = @Vector(V, f32);
    const MR = sgemm_tile.MR;
    const NR = sgemm_tile.NR;

    const m_main = m_start + ((m_end - m_start) / MR) * MR;
    var j: usize = 0;
    while (j + NR <= n) : (j += NR) {
        var i: usize = m_start;
        while (i < m_main) : (i += MR) {
            var acc: [MR][NR]Vec = undefined;
            inline for (0..MR) |r| inline for (0..NR) |c| {
                acc[r][c] = @splat(0.0);
            };

            var l: usize = 0;
            while (l + V <= k) : (l += V) {
                var av: [MR]Vec = undefined;
                inline for (0..MR) |r| {
                    av[r] = a[(i + r) * k + l ..][0..V].*;
                }
                var bv: [NR]Vec = undefined;
                inline for (0..NR) |c| {
                    bv[c] = loadBVecAsF32(BT, V, b, (j + c) * k + l);
                }
                inline for (0..MR) |r| {
                    inline for (0..NR) |c| {
                        acc[r][c] = @mulAdd(Vec, av[r], bv[c], acc[r][c]);
                    }
                }
            }

            var sum: [MR][NR]f32 = undefined;
            inline for (0..MR) |r| inline for (0..NR) |c| {
                sum[r][c] = @reduce(.Add, acc[r][c]);
            };
            while (l < k) : (l += 1) {
                inline for (0..MR) |r| {
                    const a_val = a[(i + r) * k + l];
                    inline for (0..NR) |c| {
                        sum[r][c] += a_val * loadBScalarAsF32(BT, b, (j + c) * k + l);
                    }
                }
            }

            inline for (0..MR) |r| inline for (0..NR) |c| {
                c_out[(i + r) * n + j + c] += alpha * sum[r][c];
            };
        }

        // M-tail: rows that don't fill an MR tile within this j-block.
        while (i < m_end) : (i += 1) {
            const a_row = a[i * k ..][0..k];
            inline for (0..NR) |c| {
                const b_off = (j + c) * k;
                var acc: Vec = @splat(0.0);
                var l: usize = 0;
                while (l + V <= k) : (l += V) {
                    const av: Vec = a_row[l..][0..V].*;
                    const bv = loadBVecAsF32(BT, V, b, b_off + l);
                    acc = @mulAdd(Vec, av, bv, acc);
                }
                var sum: f32 = @reduce(.Add, acc);
                while (l < k) : (l += 1) sum += a_row[l] * loadBScalarAsF32(BT, b, b_off + l);
                c_out[i * n + j + c] += alpha * sum;
            }
        }
    }
    // N-tail: leftover columns at the end of n.
    while (j < n) : (j += 1) {
        const b_off = j * k;
        var irow: usize = m_start;
        while (irow < m_end) : (irow += 1) {
            const a_row = a[irow * k ..][0..k];
            var acc: Vec = @splat(0.0);
            var l: usize = 0;
            while (l + V <= k) : (l += V) {
                const av: Vec = a_row[l..][0..V].*;
                const bv = loadBVecAsF32(BT, V, b, b_off + l);
                acc = @mulAdd(Vec, av, bv, acc);
            }
            var sum: f32 = @reduce(.Add, acc);
            while (l < k) : (l += 1) sum += a_row[l] * loadBScalarAsF32(BT, b, b_off + l);
            c_out[irow * n + j] += alpha * sum;
        }
    }
}

/// Per-worker compute slice for sgemmTransB (f32 weights).  B is row-major
/// [n, k] so the NR-row B panel is contiguous; we iterate j-outer so the
/// panel stays L1-resident across all m rows.
pub fn sgemmTransBAddSlice(
    m_start: usize,
    m_end: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    c_out: []f32,
) void {
    sgemmTransBAddSliceGeneric(f32, m_start, m_end, n, k, alpha, a, b, c_out);
}

/// Per-worker compute slice for sgemmTransBF16Weights.  Loads f16 B vectors
/// and `@floatCast`s on chip so we don't pay the up-front f32 weight
/// materialization.
pub fn sgemmTransBF16AddSlice(
    m_start: usize,
    m_end: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f16,
    c_out: []f32,
) void {
    sgemmTransBAddSliceGeneric(f16, m_start, m_end, n, k, alpha, a, b, c_out);
}

// --- Public single-threaded entry points ----------------------------------
//
// Threaded variants live in `mod.zig` and reuse the AddSlice workers above.

/// Pure Zig SGEMM with A transposed: C = alpha * A^T @ B + beta * C.
/// Single-threaded; no AddSlice / worker decomposition (KV compaction is
/// the only caller and sequential execution is fine there).
pub fn sgemmTransA(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    const V = vec_len;

    if (beta == 0.0) {
        @memset(c_out[0 .. m * n], 0.0);
    } else if (beta != 1.0) {
        const beta_v: @Vector(V, f32) = @splat(beta);
        var idx: usize = 0;
        while (idx + V <= m * n) : (idx += V) {
            const cv: @Vector(V, f32) = c_out[idx..][0..V].*;
            c_out[idx..][0..V].* = cv * beta_v;
        }
        while (idx < m * n) : (idx += 1) {
            c_out[idx] *= beta;
        }
    }

    for (0..k) |l| {
        const b_row = b[l * n ..];
        for (0..m) |i| {
            const a_val: @Vector(V, f32) = @splat(alpha * a[l * m + i]);
            const c_row = c_out[i * n ..];
            var j: usize = 0;
            while (j + V <= n) : (j += V) {
                const bv: @Vector(V, f32) = b_row[j..][0..V].*;
                const cv: @Vector(V, f32) = c_row[j..][0..V].*;
                c_row[j..][0..V].* = cv + a_val * bv;
            }
            while (j < n) : (j += 1) {
                c_row[j] += a_val[0] * b_row[j];
            }
        }
    }
}

/// Single-threaded sgemmTransB.  Same arithmetic as `mod.sgemmTransBSync`
/// but always runs on the calling thread — no worker dispatch, no per-call
/// thread pool overhead.  Use for matmuls that are already inside a
/// coarse-grained parallel loop (per-head attention, per-window Swin
/// attention) where threading the matmul itself would oversubscribe.
pub fn sgemmTransBSequential(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    sgemmTransBAddSlice(0, m, n, k, alpha, a, b, c_out);
}

/// Single-threaded sgemm (B not transposed).  See `sgemmTransBSequential`.
pub fn sgemmSequential(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    sgemmAddSlice(0, m, n, k, alpha, a, b, c_out);
}
