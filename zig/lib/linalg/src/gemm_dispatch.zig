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
pub const x86 = @import("x86.zig");
pub const SgemmTile = core.SgemmTile;
pub const sgemm_tile = core.sgemm_tile;
pub const applyBeta = core.applyBeta;
pub const sgemmTransA = core.sgemmTransA;

extern fn antfly_x86_sgemm(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void;

pub fn sgemmAddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) void {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return;
        }
    }
    core.sgemmAddSlice(m_start, m_end, n, k, alpha, a, b, c);
}

extern fn antfly_x86_sgemm_transb(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void;

pub fn sgemmTransBAddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, c: []f32) void {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm_transb(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return;
        }
    }
    core.sgemmTransBAddSlice(m_start, m_end, n, k, alpha, a, b, c);
}

extern fn antfly_x86_sgemm_transb_f16(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f16, c: [*]f32) callconv(.c) void;

pub fn sgemmTransBF16AddSlice(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f16, c: []f32) void {
    if (comptime x86.enabled) {
        if (x86.selected() == .avx2) {
            antfly_x86_sgemm_transb_f16(m_start, m_end, n, k, alpha, a.ptr, b.ptr, c.ptr);
            return;
        }
    }
    core.sgemmTransBF16AddSlice(m_start, m_end, n, k, alpha, a, b, c);
}

pub fn sgemmSequential(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32) void {
    if (comptime !x86.enabled) return core.sgemmSequential(m, n, k, alpha, a, b, beta, c);
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    sgemmAddSlice(0, m, n, k, alpha, a, b, c);
}

pub fn sgemmTransBSequential(m: usize, n: usize, k: usize, alpha: f32, a: []const f32, b: []const f32, beta: f32, c: []f32) void {
    if (comptime !x86.enabled) return core.sgemmTransBSequential(m, n, k, alpha, a, b, beta, c);
    if (m == 0 or n == 0) return;
    applyBeta(c[0 .. m * n], beta);
    if (k == 0) return;
    sgemmTransBAddSlice(0, m, n, k, alpha, a, b, c);
}
