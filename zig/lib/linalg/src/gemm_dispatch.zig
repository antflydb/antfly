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

//! Dispatch above the arithmetic kernels and below threading. Attention's
//! sequential calls use the same selection without nested worker pools.
const core = @import("gemm.zig");
const builtin = @import("builtin");
pub const x86 = @import("x86.zig");
pub const SgemmTile = core.SgemmTile;
pub const sgemm_tile = core.sgemm_tile;
pub const applyBeta = core.applyBeta;
/// Identity is chosen at the actual authored terminal dispatch branch. The
/// shim/core's inner vector tiles and CPU instructions are not enumerated.
pub const SelectedBody = enum { portable_sgemm, x86_avx2_sgemm_shim, portable_transb_f32, x86_avx2_transb_f32_shim, portable_transb_f16, x86_avx2_transb_f16_shim };
pub const sgemmTransA = core.sgemmTransA;

extern fn antfly_x86_sgemm(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void;

pub fn sgemmAddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) void {
    _ = sgemmAddSliceSelected(m_start, m_end, n, k, alpha, a, b, c);
}
pub fn sgemmAddSliceSelected(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) SelectedBody {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return .x86_avx2_sgemm_shim;
        }
    }
    core.sgemmAddSlice(m_start, m_end, n, k, alpha, a, b, c);
    return .portable_sgemm;
}

extern fn antfly_x86_sgemm_transb(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void;

pub fn sgemmTransBAddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) void {
    _ = sgemmTransBAddSliceSelected(m_start, m_end, n, k, alpha, a, b, c);
}
pub fn sgemmTransBAddSliceSelected(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) SelectedBody {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm_transb(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return .x86_avx2_transb_f32_shim;
        }
    }
    core.sgemmTransBAddSlice(m_start, m_end, n, k, alpha, a, b, c);
    return .portable_transb_f32;
}

extern fn antfly_x86_sgemm_transb_f16(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f16, c: [*]f32) callconv(.c) void;

pub fn sgemmTransBF16AddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f16, c: []f32) void {
    _ = sgemmTransBF16AddSliceSelected(m_start, m_end, n, k, alpha, a, b, c);
}
pub fn sgemmTransBF16AddSliceSelected(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f16, c: []f32) SelectedBody {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm_transb_f16(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return .x86_avx2_transb_f16_shim;
        }
    }
    core.sgemmTransBF16AddSlice(m_start, m_end, n, k, alpha, a, b, c);
    return .portable_transb_f16;
}

// Native hot entries keep their alignment as linked backend code changes.
// WebAssembly rejects explicit function alignment and uses the same core math.
pub const sgemmSequential = if (builtin.cpu.arch.isWasm()) core.sgemmSequential else sgemmSequentialAligned;
pub const sgemmTransBSequential = if (builtin.cpu.arch.isWasm()) core.sgemmTransBSequential else sgemmTransBSequentialAligned;

fn sgemmSequentialAligned(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32) align(64) void {
    if (comptime !x86.enabled) return core.sgemmSequential(m, n, k, alpha, a, b, beta, c);
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    sgemmAddSlice(0, m, n, k, alpha, a, b, c);
}

fn sgemmTransBSequentialAligned(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32) align(64) void {
    if (comptime !x86.enabled) return core.sgemmTransBSequential(m, n, k, alpha, a, b, beta, c);
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    sgemmTransBAddSlice(0, m, n, k, alpha, a, b, c);
}

pub const GemmObservation = struct {
    /// selected-body enum order is declared at the actual dispatch boundary.
    successful_leaf_returns: [6]u64 = @splat(0),
    dispatched_workers: u64 = 0,
    entered_workers: u64 = 0,
    returned_workers: u64 = 0,
    opaque_vendor_returns: u64 = 0,
    unobserved_callback_returns: u64 = 0,
    pub fn merge(self: *GemmObservation, value: GemmObservation) void {
        for (&self.successful_leaf_returns, value.successful_leaf_returns) |*count, observed| count.* +|= observed;
        self.dispatched_workers +|= value.dispatched_workers;
        self.entered_workers +|= value.entered_workers;
        self.returned_workers +|= value.returned_workers;
        self.opaque_vendor_returns +|= value.opaque_vendor_returns;
        self.unobserved_callback_returns +|= value.unobserved_callback_returns;
    }
};
/// Single-threaded VALUE entries select at the same authored leaf dispatcher.
/// All arithmetic remains in existing applyBeta/core/AVX bodies; these entries
/// neither introduce workers nor infer worker counts from a sequential call.
pub fn sgemmSequentialValue(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32, result: *GemmObservation) void {
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    const selected = sgemmAddSliceSelected(0, m, n, k, alpha, a, b, c);
    result.successful_leaf_returns[@backingInt(selected)] +|= 1;
}
pub fn sgemmTransBSequentialValue(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32, result: *GemmObservation) void {
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    const selected = sgemmTransBAddSliceSelected(0, m, n, k, alpha, a, b, c);
    result.successful_leaf_returns[@backingInt(selected)] +|= 1;
}
