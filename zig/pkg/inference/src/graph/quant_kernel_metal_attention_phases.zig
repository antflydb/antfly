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

//! Native attention phases shared by dense and paged compatibility schedules.
//! Emit before native compilation: original storage, operations and barriers
//! remain byte-identical. Launch admission and K/V acquisition stay with callers.
const std = @import("std");
const gpu = @import("quant_kernel_gpu_operator_renderer.zig");

pub const Owner = struct {
    pub const path = "src/backends/metal_kernels.m";
    pub const begin = "// quant-kernel-codegen:begin Metal attention phase owners";
    pub const end = "// quant-kernel-codegen:end Metal attention phase owners";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        for (phases) |phase| {
            try out.appendSlice(allocator, "#define TERMITE_MSL_ATTENTION_");
            for (phase.name) |c| try out.append(allocator, std.ascii.toUpper(c));
            try out.appendSlice(allocator, " \\\n");
            const quoted = try gpu.quoteMetalIndented(allocator, phase.source, 4);
            defer allocator.free(quoted);
            var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, quoted, "\n"), '\n');
            while (lines.next()) |line| {
                try out.appendSlice(allocator, line);
                try out.appendSlice(allocator, if (lines.peek() != null) " \\\n" else "\n");
            }
        }
        return out.toOwnedSlice(allocator);
    }
};

/// Resolve whole-line native phase references for standalone/runtime templates.
/// Unknown references fail generation rather than shipping an incomplete kernel.
pub fn expand(allocator: std.mem.Allocator, template: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var lines = std.mem.splitScalar(u8, template, '\n');
    while (lines.next()) |line| {
        if (line.len == 0 and lines.peek() == null) break;
        const prefix = "// @attention-phase ";
        if (std.mem.startsWith(u8, line, prefix)) {
            const name = line[prefix.len..];
            for (phases) |phase| {
                if (std.mem.eql(u8, name, phase.name)) {
                    try out.appendSlice(allocator, phase.source);
                    break;
                }
            } else return error.UnknownMetalAttentionPhase;
        } else {
            try out.appendSlice(allocator, line);
            try out.append(allocator, '\n');
        }
    }
    return out.toOwnedSlice(allocator);
}

pub const row32_update =
    \\            if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }
    \\            float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f;
    \\            float m_old; float m_new; float corr; float row_sum;
    \\            antfly_attention_online_row32(sc, sM, j, sp, j * 32u + kk, m_old, m_new, corr, row_sum);
    \\
;

pub const online_init =
    \\    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }
    \\    if (tid < 64u) sdiag[tid] = 0.0f;
    \\
;

pub const accumulator_init =
    \\    simdgroup_float8x8 mo[8];
    \\    for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    \\
;

pub const normalization =
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; }
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u);
    \\
;

const Phase = struct { name: []const u8, source: []const u8 };
const phases = [_]Phase{
    .{ .name = "staging", .source =
    \\    threadgroup half *sq = (threadgroup half *)shmem;
    \\    threadgroup half *skv = sq + 8u * hd;
    \\    threadgroup char *fb = shmem + (8u + 32u) * hd * 2u;
    \\    threadgroup float *ss = (threadgroup float *)fb;
    \\    threadgroup half *sp = (threadgroup half *)(fb + 1024u);
    \\
    },
    .{ .name = "accumulators", .source =
    \\    for (uint i = uint(tid); i < 8u * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * hd + d] * scale) : half(0.0f); }
    \\
    ++ online_init ++
        \\    const uint dslice = uint(sgitg) * (hd / 4u);
        \\
    ++ accumulator_init ++
        \\    const uint d_tiles = hd / 32u;
        \\
    },
    .{ .name = "score_rows", .source =
    \\        simdgroup_store(ms, ss + uint(sgitg) * 8u, 32u, 0, false);
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        for (uint jj = 0u; jj < 2u; ++jj) {
    \\            uint j = uint(sgitg) + jj * 4u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; uint kk = uint(lane); uint ki = kc + kk;
    \\
    },
    .{ .name = "online_weights", .source =
    \\
    ++ row32_update ++
        \\            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }
        \\        }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u);
        \\        for (uint i = 0u; i < d_tiles; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\
    },
    .{ .name = "publish", .source =
    \\
    ++ normalization ++
        \\    threadgroup float *so = (threadgroup float *)skv;
        \\    for (uint dt = 0u; dt < d_tiles; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); simdgroup_store(scaled, so + uint(sgitg) * (8u * (hd / 4u)) + dt * 8u, hd / 4u, 0, false); }
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint i = uint(tid); i < 8u * hd; i += 128u) { uint j = i / hd; uint d = i - j * hd; uint qi = q0 + j; if (qi >= p.q_len) continue; uint sg_of_d = d / (hd / 4u); uint d_in = d - sg_of_d * (hd / 4u); output[qi * q_stride + h * hd + d] = so[sg_of_d * (8u * (hd / 4u)) + j * (hd / 4u) + d_in]; }
        \\
    },
};

test "Metal attention phases reject unknown references and preserve surrounding source" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.UnknownMetalAttentionPhase, expand(a, "// @attention-phase missing\n"));
    const text = try expand(a, "before\n// @attention-phase staging\nafter\n");
    defer a.free(text);
    const expected = try std.mem.concat(a, u8, &.{ "before\n", phases[0].source, "after\n" });
    defer a.free(expected);
    try std.testing.expectEqualStrings(expected, text);
}
