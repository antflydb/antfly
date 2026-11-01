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

//! Emit CUDA recipe expressions plus independent scalar fixtures for a host C++
//! compiler. This checks generated syntax/arithmetic; GPU qualification is separate.
const std = @import("std");
const recipe = @import("graph/quant_recipe.zig");
const math = @import("antfly_tile").native_math;
const reduction = @import("ops/kernel_reduction.zig");
const reference = @import("gguf/testing/quant_reference.zig");
pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const output = args.next() orelse return error.MissingOutputPath;
    var out: std.Io.Writer.Allocating = .init(init.gpa);
    defer out.deinit();
    const w = &out.writer;
    try w.writeAll(
        \\#include <stdint.h>
        \\#include <stdio.h>
        \\#include <string.h>
        \\#include <math.h>
        \\#define __device__
        \\#define __forceinline__ inline
        \\static _Float16 __ushort_as_half(uint16_t bits) { _Float16 value; memcpy(&value, &bits, 2); return value; }
        \\static float __half2float(_Float16 value) { return float(value); }
        \\static float __uint_as_float(uint32_t bits) { float value; memcpy(&value, &bits, 4); return value; }
        \\
    );
    try w.print("{s}\n{s}\n{s}\n", .{ recipe.cuda_half_source, recipe.cuda_word_source, recipe.cuda_float_source });
    inline for (recipe.formats) |format| {
        const source = try recipe.render(init.gpa, comptime recipe.forFormat(format), .cuda, "decode_" ++ @tagName(format));
        defer init.gpa.free(source);
        try w.print("{s}\n", .{source});
    }
    const q6 = comptime recipe.forFormat(.q6_k);
    inline for (.{
        .{ "q6_integer", q6.component(q6.integer_component.?) },
        .{ "q6_scale", q6.component(q6.scale_component.?) },
        .{ "q6_sub_scale", recipe.q6SubblockScale() },
    }) |part| {
        const source = try recipe.render(init.gpa, part[1], .cuda, part[0]);
        defer init.gpa.free(source);
        try w.print("{s}\n", .{source});
    }
    // The host shim validates generated syntax and expression order. Mapping
    // __expf to expf here does not qualify NVIDIA's fast-exp approximation.
    try w.writeAll("#define __expf expf\nstatic float max(float x, float y) { return fmaxf(x,y); }\n");
    inline for (.{ math.metal, math.cuda, math.cuda_fast }, 0..) |target, target_index| {
        inline for (comptime std.meta.tags(math.Activation), 0..) |kind, kind_index| {
            const expression = comptime math.expression(math.activation(kind, .left_associated), target, "x");
            try w.print("static float activation_{d}_{d}(float x) {{ return {s}; }}\n", .{ target_index, kind_index, expression });
        }
    }
    // MSL's float overloads are supplied explicitly for the host compiler.
    // The generated expressions above use the same signatures on device.
    try w.writeAll(comptime math.guardedGeluSource(math.cuda, "static", "guarded_gelu", false) ++ "\n");
    try w.writeAll(
        \\static float activation_reference(int kind, float x) {
        \\    if (kind == 0) return 0.5f*x*(1.0f+tanhf(0.7978845608028654f*(x+0.044715f*x*x*x)));
        \\    if (kind == 1) return 1.0f/(1.0f+expf(-x));
        \\    if (kind == 2) return x/(1.0f+expf(-x));
        \\    if (kind == 3) return x/(1.0f+expf(-1.702f*x));
        \\    float r = fmaxf(x, 0.0f); return kind == 4 ? r : r*r;
        \\}
        \\static float guarded_reference(float x) {
        \\    if (!isfinite(x)) return 0;
        \\    float inner = 0.7978845608028654f*(x+0.044715f*x*x*x);
        \\    if (inner > 10) return x;
        \\    if (inner < -10) return 0;
        \\    float y = 0.5f*x*(1+tanhf(inner)); return isfinite(y) ? y : 0;
        \\}
        \\static float shuffle_inputs[5][32], shuffle_final[32];
        \\static unsigned shuffle_lane;
        \\static float __shfl_down_sync(unsigned, float, unsigned delta) {
        \\    unsigned stage = delta == 16 ? 0 : delta == 8 ? 1 : delta == 4 ? 2 : delta == 2 ? 3 : 4;
        \\    return shuffle_inputs[stage][shuffle_lane + delta < 32 ? shuffle_lane + delta : shuffle_lane];
        \\}
        \\static float __shfl_sync(unsigned, float, unsigned lane) { return shuffle_final[lane]; }
        \\static void prepare_shuffle(int maximum, unsigned trial) {
        \\    for (unsigned lane=0; lane<32; ++lane) shuffle_final[lane] = sinf(float(lane*37+trial*11))*100.0f;
        \\    for (unsigned stage=0,delta=16; delta; ++stage,delta>>=1) {
        \\        memcpy(shuffle_inputs[stage],shuffle_final,sizeof(shuffle_final));
        \\        for (unsigned lane=0; lane<32; ++lane) {
        \\            float other=shuffle_inputs[stage][lane+delta<32 ? lane+delta : lane];
        \\            shuffle_final[lane]=maximum ? fmaxf(shuffle_inputs[stage][lane],other) : shuffle_inputs[stage][lane]+other;
        \\        }
        \\    }
        \\}
        \\
    );
    inline for (.{ reduction.Operation.sum, reduction.Operation.maximum }, 0..) |operation, operation_index| {
        inline for (.{ reduction.Result.lane_zero, reduction.Result.broadcast }, 0..) |result, result_index| {
            const name = std.fmt.comptimePrint("reduce_{d}_{d}", .{ operation_index, result_index });
            try w.writeAll(comptime reduction.cudaWarpSource(name, operation, result, .static_device, "value") ++ "\n");
        }
    }
    try w.writeAll("int main() { unsigned checked = 0;\n");
    inline for (recipe.formats) |format| {
        const r = comptime recipe.forFormat(format);
        for (0..64) |trial| {
            var raw: [r.blockBytes()]u8 = undefined;
            var expected: [r.blockValues()]f32 = undefined;
            @import("graph/testing/quant_blocks.zig").fill(format, trial, &raw);
            try reference.decode(format, &raw, &expected);
            try w.writeAll("{ const unsigned char raw[] = {");
            for (raw) |byte| try w.print("{d},", .{byte});
            try w.writeAll("}; const uint32_t expected[] = {");
            for (expected) |value| try w.print("0x{x}u,", .{@as(u32, @bitCast(value))});
            try w.print("}}; for (int lane = 0; lane < {d}; ++lane) {{ float want = __uint_as_float(expected[lane]); float got = decode_{s}(raw, lane); if (want != got) {{ fprintf(stderr, \"{s} trial {d} lane %d: %.9g != %.9g\\n\", lane, double(got), double(want)); return 1; }} ++checked; }} }}\n", .{ r.blockValues(), @tagName(format), @tagName(format), trial });
        }
    }
    // Obtain integer values and subblock scales from the independent full
    // decoder by setting the other component to one. This also covers zero
    // scales, where dividing a full decoded value could not recover the bits.
    for (0..64) |trial| {
        var raw: [210]u8 = undefined;
        @import("graph/testing/quant_blocks.zig").fill(.q6_k, trial, &raw);
        var unit_scale = raw;
        @memset(unit_scale[192..208], 1);
        unit_scale[208] = 0;
        unit_scale[209] = 0x3c;
        var expected_integer: [256]f32 = undefined;
        try reference.decode(.q6_k, &unit_scale, &expected_integer);
        var unit_value = raw;
        @memset(unit_value[0..128], 0x11);
        @memset(unit_value[128..192], 0xaa);
        var expected_scale: [256]f32 = undefined;
        try reference.decode(.q6_k, &unit_value, &expected_scale);
        try w.writeAll("{ const unsigned char raw[]={");
        for (raw) |byte| try w.print("{d},", .{byte});
        try w.writeAll("}; const int integers[]={");
        for (expected_integer) |value| try w.print("{d},", .{@as(i32, @intFromFloat(value))});
        try w.writeAll("}; const uint32_t scales[]={");
        for (expected_scale) |value| try w.print("0x{x}u,", .{@as(u32, @bitCast(value))});
        try w.writeAll("}; for(int lane=0;lane<256;++lane) { float scale=q6_scale(raw,lane),sub_scale=q6_sub_scale(raw,lane/16); uint32_t bits,sub_bits; memcpy(&bits,&scale,4); memcpy(&sub_bits,&sub_scale,4); if(q6_integer(raw,lane)!=integers[lane] || bits!=scales[lane] || sub_bits!=scales[lane]) { fprintf(stderr, \"Q6 component mismatch\\n\"); return 5; } checked+=3; } }\n");
    }
    inline for (.{ 0, 1, 2 }) |target_index| {
        inline for (0..6) |kind_index| {
            try w.print("for (int i=-4096;i<=4096;++i) {{ float x=float(i)/256.0f; float got=activation_{d}_{d}(x),want=activation_reference({d},x); if (fabsf(got-want)>2e-6f) {{ fprintf(stderr, \"activation {d}/{d} %.9g != %.9g\\n\", double(got),double(want)); return 2; }} ++checked; }}\n", .{ target_index, kind_index, kind_index, target_index, kind_index });
        }
    }
    try w.writeAll("{ const float inputs[]={0.0f,-0.0f,1.0f,-1.0f,10.0f,-10.0f,1e30f,-1e30f,INFINITY,-INFINITY,NAN}; for(float x:inputs) { if(guarded_gelu(x)!=guarded_reference(x)) return 3; ++checked; } }\n");
    inline for (0..2) |operation_index| {
        inline for (0..2) |result_index| {
            try w.print("for(unsigned trial=0;trial<64;++trial) {{ prepare_shuffle({d},trial); for(shuffle_lane=0;shuffle_lane<32;++shuffle_lane) {{ float got=reduce_{d}_{d}(shuffle_inputs[0][shuffle_lane]); float want=shuffle_final[{s}]; if(got!=want) {{ fprintf(stderr, \"reduction mismatch\\n\"); return 4; }} ++checked; }} }}\n", .{ operation_index, operation_index, result_index, if (result_index == 1) "0" else "shuffle_lane" });
        }
    }
    try w.writeAll("printf(\"Kernel recipe host conformance: %u values passed\\n\", checked); return 0; }\n");
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output, .data = out.written() });
}
