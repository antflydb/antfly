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

//! Generate a model-free NVIDIA device test. The CUDA toolkit is needed only
//! for this developer target; inference deployment keeps its driver-only ABI.
const std = @import("std");
const quant = @import("graph/quant_recipe.zig");
const reference = @import("gguf/testing/quant_reference.zig");
const blocks = @import("graph/testing/quant_blocks.zig");
const math = @import("antfly_tile").native_math;
const reduction = @import("ops/kernel_reduction.zig");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const path = args.next() orelse return error.MissingOutputPath;
    var output: std.Io.Writer.Allocating = .init(init.gpa);
    defer output.deinit();
    const w = &output.writer;
    try w.writeAll(
        \\#include <cuda_runtime.h>
        \\#include <cuda_fp16.h>
        \\#include <stdint.h>
        \\#include <stdlib.h>
        \\#include <stdio.h>
        \\#include <string.h>
        \\#include <math.h>
        \\#define CUDA_CHECK(call) do { cudaError_t e=(call); if(e!=cudaSuccess) { fprintf(stderr,"%s: %s\n",#call,cudaGetErrorString(e)); exit(2); } } while(0)
        \\static void *upload(const void *data,size_t bytes) { void *p; CUDA_CHECK(cudaMalloc(&p,bytes)); CUDA_CHECK(cudaMemcpy(p,data,bytes,cudaMemcpyHostToDevice)); return p; }
        \\static unsigned checked=0;
        \\static void check(const char *name,float *device,const uint32_t *expected,size_t count,float tolerance) {
        \\    float *values=(float*)malloc(count*sizeof(float)); if(!values) exit(3);
        \\    CUDA_CHECK(cudaGetLastError()); CUDA_CHECK(cudaMemcpy(values,device,count*sizeof(float),cudaMemcpyDeviceToHost));
        \\    for(size_t i=0;i<count;++i) { float want; memcpy(&want,expected+i,4); float got=values[i];
        \\        if(!isfinite(got) || fabsf(got-want)>tolerance) { fprintf(stderr,"%s[%zu]: %.9g != %.9g\n",name,i,double(got),double(want)); exit(4); }
        \\    } checked+=(unsigned)count; printf("%s: %zu values passed\n",name,count); free(values);
        \\}
        \\
    );
    try w.print("{s}\n{s}\n{s}\n", .{ quant.cuda_half_source, quant.cuda_word_source, quant.cuda_float_source });
    inline for (quant.formats) |format| {
        const r = comptime quant.forFormat(format);
        const name = "decode_" ++ @tagName(format);
        const source = try quant.render(init.gpa, r, .cuda, name);
        defer init.gpa.free(source);
        try w.print("{s}\n__global__ void {s}_kernel(const unsigned char *raw,float *out) {{ unsigned i=blockIdx.x*blockDim.x+threadIdx.x; if(i<{d}) out[i]={s}(raw+(i/{d})*{d},i%{d}); }}\n", .{ source, name, 64 * r.blockValues(), name, r.blockValues(), r.blockBytes(), r.blockValues() });
    }
    inline for (.{ math.cuda, math.cuda_fast }, 0..) |target, target_index| {
        inline for (comptime std.meta.tags(math.Activation), 0..) |kind, kind_index| {
            const expression = comptime math.expression(math.activation(kind, .left_associated), target, "x");
            try w.print("__global__ void activation_{d}_{d}(float *out) {{ unsigned i=blockIdx.x*blockDim.x+threadIdx.x; if(i<8193) {{ float x=(int(i)-4096)/256.0f; out[i]={s}; }} }}\n", .{ target_index, kind_index, expression });
        }
    }
    inline for (.{ reduction.Operation.sum, .maximum }, 0..) |operation, operation_index| {
        inline for (.{ reduction.Result.lane_zero, .broadcast }, 0..) |result, result_index| {
            const name = std.fmt.comptimePrint("reduction_{d}_{d}", .{ operation_index, result_index });
            try w.writeAll(comptime reduction.cudaWarpSource(name, operation, result, .static_device, "value") ++ "\n");
            try w.print("__global__ void {s}_kernel(const float *input,float *out) {{ unsigned i=blockIdx.x*32+threadIdx.x; out[i]={s}(input[i]); }}\n", .{ name, name });
        }
    }
    try w.writeAll("int main() { cudaDeviceProp props; CUDA_CHECK(cudaGetDeviceProperties(&props,0)); printf(\"GPU: %s SM%d%d\\n\",props.name,props.major,props.minor); float *device; CUDA_CHECK(cudaMalloc((void**)&device,8193*sizeof(float)));\n");
    inline for (quant.formats) |format| {
        const r = comptime quant.forFormat(format);
        var raw: [64 * r.blockBytes()]u8 = undefined;
        var expected: [64 * r.blockValues()]f32 = undefined;
        for (0..64) |trial| {
            blocks.fill(format, trial, raw[trial * r.blockBytes() ..][0..r.blockBytes()]);
            try reference.decode(format, raw[trial * r.blockBytes() ..][0..r.blockBytes()], expected[trial * r.blockValues() ..][0..r.blockValues()]);
        }
        // Largest packed case has 16,384 outputs, so use its own allocation.
        try w.writeAll("{ static const unsigned char raw[]={");
        for (raw) |byte| try w.print("{d},", .{byte});
        try w.writeAll("};\n");
        try emitExpected(w, &expected);
        try w.print("unsigned char *gpu_raw=(unsigned char*)upload(raw,sizeof(raw)); float *packed; CUDA_CHECK(cudaMalloc((void**)&packed,{d}*sizeof(float))); decode_{s}_kernel<<<{d},256>>>(gpu_raw,packed); check(\"{s}\",packed,expected,{d},0.0f); CUDA_CHECK(cudaFree(packed)); CUDA_CHECK(cudaFree(gpu_raw)); }}\n", .{ expected.len, @tagName(format), (expected.len + 255) / 256, @tagName(format), expected.len });
    }
    inline for (.{ 0, 1 }) |target_index| {
        inline for (comptime std.meta.tags(math.Activation), 0..) |kind, kind_index| {
            var expected: [8193]f32 = undefined;
            for (&expected, 0..) |*value, i| {
                const x: f64 = @as(f64, @floatFromInt(@as(i32, @intCast(i)) - 4096)) / 256;
                const positive = @max(x, 0);
                value.* = @floatCast(switch (kind) {
                    .gelu => 0.5 * x * (1 + std.math.tanh(@sqrt(2.0 / std.math.pi) * (x + 0.044715 * x * x * x))),
                    .sigmoid => 1 / (1 + @exp(-x)),
                    .silu => x / (1 + @exp(-x)),
                    .quick_gelu => x / (1 + @exp(-1.702 * x)),
                    .relu => positive,
                    .relu_squared => positive * positive,
                });
            }
            try w.writeAll("{\n");
            try emitExpected(w, &expected);
            try w.print("activation_{d}_{d}<<<33,256>>>(device); check(\"activation {d}/{d}\",device,expected,8193,0.00001f); }}\n", .{ target_index, kind_index, target_index, kind_index });
        }
    }
    inline for (.{ reduction.Operation.sum, .maximum }, 0..) |operation, operation_index| {
        inline for (.{ reduction.Result.lane_zero, .broadcast }, 0..) |result, result_index| {
            var values: [64 * 32]f32 = undefined;
            var expected: [64 * 32]f32 = undefined;
            for (0..64) |trial| {
                for (values[trial * 32 ..][0..32], 0..) |*value, lane| value.* = @sin(@as(f32, @floatFromInt(lane * 37 + trial * 11))) * 100;
                expected[trial * 32 ..][0..32].* = reduction.evaluate(operation, result, values[trial * 32 ..][0..32].*);
            }
            try w.writeAll("{ static const uint32_t input_bits[]={");
            for (values) |value| try w.print("0x{x}u,", .{@as(u32, @bitCast(value))});
            try w.writeAll("};\n");
            try emitExpected(w, &expected);
            try w.print("float *gpu_input=(float*)upload(input_bits,sizeof(input_bits)); reduction_{d}_{d}_kernel<<<64,32>>>(gpu_input,device); check(\"reduction {d}/{d}\",device,expected,2048,0.0f); CUDA_CHECK(cudaFree(gpu_input)); }}\n", .{ operation_index, result_index, operation_index, result_index });
        }
    }
    try w.writeAll("CUDA_CHECK(cudaFree(device)); printf(\"CUDA device recipe conformance: %u values passed\\n\",checked); return 0; }\n");
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = output.written() });
}
fn emitExpected(w: *std.Io.Writer, values: []const f32) !void {
    try w.writeAll("static const uint32_t expected[]={");
    for (values) |value| try w.print("0x{x}u,", .{@as(u32, @bitCast(value))});
    try w.writeAll("};\n");
}
