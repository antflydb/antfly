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

//! Simdgroup-matrix quantized matmul (ggml mul_mm port), 64 x 32 output
//! tiles: each K step dequantizes a 64 x 32 weight tile and stages a 32 x 32
//! activation tile as half in threadgroup memory, then four simdgroups each
//! accumulate 4 x 2 8x8 products.
//!
//! Variants: `generic` guards both tile edges (and partial K); `unrolled`
//! adds TERMITE_FOR_UNROLL; `aligned` assumes full tiles and K % 32 == 0
//! (vector activation loads, no guards); `aligned_tail` is the aligned
//! kernel for the last partial row tile.

const msl = @import("quant_metal_msl.zig");
const wrap = @import("quant_metal_wrap.zig");
const fill = msl.fill;

pub const Variant = enum { generic, unrolled, aligned, aligned_tail };

/// Block type and bytes, and how the 16-value half-block index advances
/// (Q8_0 blocks hold two 16-value halves, Q4_0 one interleaved pair).
pub const Format = struct { name: []const u8, bytes: u32, advance: []const u8 };

pub const q4_0 = Format{ .name = "q4_0", .bytes = 18, .advance = "x = x + 1;" };
pub const q8_0 = Format{ .name = "q8_0", .bytes = 34, .advance = "il = (il + 2 < 2) ? il + 2 : il % 2; x = (il < 2) ? x + 1 : x;" };

const direct_store = "device float *C = output + uint(r1) * p.out_dim + uint(r0) + 32u * (uint(sgitg) & 1u) + 16u * (uint(sgitg) >> 1u) * p.out_dim; for (short i = 0; i < 8; ++i) simdgroup_store(mc[i], C + 8u * uint(i % 4) + 8u * p.out_dim * uint(i / 4), p.out_dim, 0, false);";

/// Edge tiles go through threadgroup memory; `copy` is the row copy loop.
fn guardedStore(comptime guard: []const u8, comptime copy: []const u8) []const u8 {
    return fill("if (${g}) { ${s} }\n    else { threadgroup_barrier(mem_flags::mem_threadgroup); threadgroup float *temp_str = ((threadgroup float *)shmem) + 32u * (uint(sgitg) & 1u) + 16u * (uint(sgitg) >> 1u) * uint(NR0); for (short i = 0; i < 8; ++i) simdgroup_store(mc[i], temp_str + 8u * uint(i % 4) + 8u * uint(NR0) * uint(i / 4), uint(NR0), 0, false); threadgroup_barrier(mem_flags::mem_threadgroup); if (sgitg == 0) { for (int j = int(tiitg); j < int(nr1); j += NR1) { device float *D = output + uint(r1 + j) * p.out_dim + uint(r0); threadgroup float *C = ((threadgroup float *)shmem) + uint(j) * uint(NR0); ${c} } } }", .{ .g = guard, .s = direct_store, .c = copy });
}

pub fn kernel(comptime name: []const u8, comptime f: Format, comptime v: Variant) msl.Item {
    const aligned = v == .aligned or v == .aligned_tail;
    const loop = if (v == .generic) "for " else "TERMITE_FOR_UNROLL";
    return msl.msl(msl.signature(name, &(wrap.linear ++ wrap.mm)) ++ fill(
        \\
        \\    weight += p.weight_offset;
        \\    threadgroup half *sa = (threadgroup half *)(shmem); threadgroup half *sb = (threadgroup half *)(shmem + 4096u);
        \\    const short NR0 = 64; const short NR1 = 32; const short NK = 32; const short NL0 = 2; const short NL1 = 4;
        \\    const int r0 = int(tg.x) * NR0; const int r1 = int(tg.y) * NR1;${guard}
        \\    ${rows}
        \\    device const termite_block_${f} *x = (device const termite_block_${f} *)(weight + (uint(r0 + lr0) * p.row_blocks * ${bytes}u)); const short iy = 8 * (short(tiitg) % NL1);
        \\${y}    simdgroup_half8x8 ma[4]; simdgroup_half8x8 mb[2]; simdgroup_float8x8 mc[8];
        \\    for (short i = 0; i < 8; ++i) mc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
        \\    for (uint loop_k = 0u; loop_k < p.in_dim; loop_k += uint(NK)) {
        \\        half4x4 temp_a; termite_dequantize_${f}_4x4_half(x, ${il}, temp_a); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        ${for}(short i = 0; i < 16; ++i) { const short sx = 2 * il0 + i / 8; const short sy = (short(tiitg) / NL0) / 8; const short lx = (short(tiitg) / NL0) % 8; const short ly = i % 8; const short ib = 8 * sx + sy; *(sa + 64 * ib + 8 * ly + lx) = temp_a[i / 4][i % 4]; }
        \\        ${load_b}
        \\        ${advance} threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        threadgroup const half *lsma = sa + 4 * 64 * (uint(sgitg) % 2u); threadgroup const half *lsmb = sb + 2 * 64 * (uint(sgitg) / 2u);
        \\        ${for}(short ik = 0; ik < NK / 8; ++ik) { simdgroup_barrier(mem_flags::mem_none); ${for}(short i = 0; i < 4; ++i) simdgroup_load(ma[i], lsma + 64 * i, 8, 0, false); simdgroup_barrier(mem_flags::mem_none); ${for}(short i = 0; i < 2; ++i) simdgroup_load(mb[i], lsmb + 64 * i, 8, 0, false); simdgroup_barrier(mem_flags::mem_none); ${for}(short i = 0; i < 8; ++i) simdgroup_multiply_accumulate(mc[i], mb[i / 4], ma[i % 4], mc[i]); lsma += 8 * 64; lsmb += 4 * 64; }
        \\    }
        \\    ${store}
        \\}
        \\
    , .{
        .guard = switch (v) {
            .generic, .unrolled => " if (r0 >= int(p.out_dim) || r1 >= int(p.rows)) return;",
            .aligned => "",
            .aligned_tail => " if (r1 >= int(p.rows)) return;",
        },
        .rows = switch (v) {
            .generic, .unrolled => "const short nr0 = short(min(uint(NR0), p.out_dim - uint(r0))); const short nr1 = short(min(uint(NR1), p.rows - uint(r1)));\n    const short lr0 = short(min(int(tiitg) / int(NL0), int(nr0) - 1)); const short lr1 = short(min(int(tiitg) / int(NL1), int(nr1) - 1)); const short il0 = short(tiitg) % NL0; short il = il0;",
            .aligned => "const short lr0 = short(tiitg) / NL0; const short lr1 = short(tiitg) / NL1; const short il0 = short(tiitg) % NL0;",
            .aligned_tail => "const short nr1 = short(min(uint(NR1), p.rows - uint(r1))); const short lr0 = short(tiitg) / NL0; const short lr1 = short(min(int(tiitg) / int(NL1), int(nr1) - 1)); const short il0 = short(tiitg) % NL0;",
        },
        .f = f.name,
        .bytes = f.bytes,
        .y = if (aligned) "    device const float *y = input + uint(r1 + lr1) * p.in_dim + uint(iy);\n" else "",
        .il = if (aligned) "il0" else "il",
        .@"for" = loop,
        .load_b = if (aligned)
            "const short sx = short(tiitg) % NL1; const short sy = (short(tiitg) / NL1) / 8; const short ly = (short(tiitg) / NL1) % 8; const short ib = 4 * sx + sy;\n        *(threadgroup half2x4 *)(sb + 64 * ib + 8 * ly) = (half2x4)(*((device const float2x4 *)y));"
        else
            fill("${for}(short i = 0; i < 8; ++i) { const short sx = short(tiitg) % NL1; const short sy = (short(tiitg) / NL1) / 8; const short lx = i; const short ly = (short(tiitg) / NL1) % 8; const short ib = 4 * sx + sy; uint k = loop_k + uint(iy + i); *(sb + 64 * ib + 8 * ly + lx) = k < p.in_dim ? half(input[uint(r1 + lr1) * p.in_dim + k]) : half(0.0f); }", .{ .@"for" = loop }),
        .advance = if (aligned) f.advance ++ " y += uint(NK);" else f.advance,
        .store = switch (v) {
            .generic, .unrolled => guardedStore("r0 + NR0 <= int(p.out_dim) && r1 + NR1 <= int(p.rows)", "uint i = 0u; for (; i < uint(nr0); ++i) D[i] = C[i];"),
            .aligned => direct_store,
            .aligned_tail => guardedStore("r1 + NR1 <= int(p.rows)", "for (uint i = 0u; i < uint(NR0); ++i) D[i] = C[i];"),
        },
    }));
}

pub const q4_0_kernels = [_]msl.Item{
    kernel("termite_q4_0_linear_mm_sg", q4_0, .generic),
    kernel("termite_q4_0_linear_mm_sg_unrolled", q4_0, .unrolled),
    kernel("termite_q4_0_linear_mm_sg_aligned", q4_0, .aligned),
    kernel("termite_q4_0_linear_mm_sg_aligned_tail", q4_0, .aligned_tail),
};
pub const q8_0_kernel = kernel("termite_q8_0_linear_mm_sg", q8_0, .generic);
