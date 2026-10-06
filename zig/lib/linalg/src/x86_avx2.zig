// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Separately compiled AVX2/FMA/F16C object; only scalar/pointer ABI crosses targets.
const core = @import("gemm.zig");

fn antfly_x86_sgemm(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void {
    core.sgemmAddSlice(m_start, m_end, n, k, alpha, a[0 .. m_end * k], b[0 .. k * n], c[0 .. m_end * n]);
}

fn antfly_x86_sgemm_transb(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f32, c: [*]f32) callconv(.c) void {
    core.sgemmTransBAddSlice(m_start, m_end, n, k, alpha, a[0 .. m_end * k], b[0 .. n * k], c[0 .. m_end * n]);
}

fn antfly_x86_sgemm_transb_f16(m_start: usize, m_end: usize, n: usize, k: usize, alpha: f32, a: [*]const f32, b: [*]const f16, c: [*]f32) callconv(.c) void {
    core.sgemmTransBF16AddSlice(m_start, m_end, n, k, alpha, a[0 .. m_end * k], b[0 .. n * k], c[0 .. m_end * n]);
}

comptime {
    @export(&antfly_x86_sgemm, .{ .name = "antfly_x86_sgemm", .visibility = .hidden });
    @export(&antfly_x86_sgemm_transb, .{ .name = "antfly_x86_sgemm_transb", .visibility = .hidden });
    @export(&antfly_x86_sgemm_transb_f16, .{ .name = "antfly_x86_sgemm_transb_f16", .visibility = .hidden });
}
