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
