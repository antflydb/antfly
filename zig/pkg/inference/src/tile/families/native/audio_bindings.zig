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

//! Fixed audio geometry and historical ABI bindings; arithmetic is tile-owned.
const tile = @import("antfly_tile");
const audio = tile.native_audio;
const program = @import("../../native_program.zig");
const fmt = @import("std").fmt.comptimePrint;
const cuda_math: tile.fexpr.FExpr.Source = .{ .add = "__fadd_rn", .sub = "__fsub_rn", .mul = "__fmul_rn", .div = "__fdiv_rn" };
const metal_math: tile.fexpr.FExpr.Source = .{ .add = "ADD", .sub = "SUB", .mul = "MUL" };
fn power(comptime source: tile.fexpr.FExpr.Source) []const u8 {
    return audio.power.renderNative(source, .{ .real = "x.x", .imag = "x.y" }, null);
}
pub const metal = .{
    .frontend = program.metalAt("", audio.Frontend.native(.{ .integer = "uint", .wide = "uint", .prefix = "p.", .source = .{ .compact = true }, .sqrt = "sqrt", .log = "log", .normalize = "antfly_channel_norm_relu" })),
    .tiled_convolution = @import("convolution_bindings.zig").metal_audio.tiled_convolution,
    .matrix_coordinates = @import("convolution_bindings.zig").metal_audio.matrix_coordinates,
    .fft_size = fmt("{d}", .{audio.Fft512.size}),
    .fft_samples = fmt("{d}", .{audio.Fft512.frame_samples}),
    .fft_bins = fmt("{d}", .{audio.Fft512.bins}),
    .fft_threads = fmt("{d}", .{audio.Fft512.threads}),
    .fft_index_bits = fmt("{d}", .{audio.Fft512.index_bits}),
    .complex_power = power(.{ .compact = true }),
    .complex_power_separate = power(metal_math),
    .fft_plain = program.metalAt("", audio.Butterfly.native(.{ .twiddles = "tw" }) ++ "\n"),
    .fft_separate = program.metalAt("", audio.Butterfly.native(.{ .twiddles = "plan", .source = metal_math }) ++ "\n"),
    .mel_projection = program.metalAt("", (audio.MelProjection{}).native(.{ .source = metal_math })),
};
const cuda = .{
    .fft_size = metal.fft_size,
    .fft_samples = metal.fft_samples,
    .fft_bins = metal.fft_bins,
    .fft_threads = metal.fft_threads,
    .reverse_shift = fmt("{d}", .{audio.Fft512.reverse_shift}),
    .power = power(cuda_math),
    .fft_plain = audio.Butterfly.native(.{ .twiddles = "tw", .integer = "unsigned", .lane = "t", .constructor = "make_float2", .barrier = "__syncthreads();", .source = cuda_math, .split_components = true }),
    .fft_separate = audio.Butterfly.native(.{ .twiddles = "plan", .integer = "unsigned", .constructor = "make_float2", .barrier = "__syncthreads();", .source = cuda_math }),
    .mel_projection = (audio.MelProjection{}).native(.{ .integer = "unsigned", .frame = "(size_t)frame", .log = "logf", .source = cuda_math }),
};
pub const source_0 = program.render(
    \\extern "C" __global__ void antfly_audio_fft512_f32(const float *input, const float *tw, float *out) {
    \\    __shared__ float2 values[{{fft_size}}];
    \\    unsigned t=threadIdx.x, frame=blockIdx.x;
    \\    for (unsigned i=t;i<{{fft_size}};i+={{fft_threads}}) values[__brev(i)>>{{reverse_shift}}]=make_float2(i<{{fft_samples}} ? input[(size_t)frame*{{fft_samples}}+i] : 0.f,0.f);
    \\    __syncthreads();
    \\{{fft_plain}}
    \\    for (unsigned i=t;i<{{fft_bins}};i+={{fft_threads}}) { float2 x=values[i]; out[(size_t)frame*{{fft_bins}}+i]=sqrtf({{power}}); }
    \\}
    \\
, cuda);
pub const source_1 = program.render(
    \\extern "C" __global__ void antfly_audio_logmel_f32(const float *input,const float *plan,const float *mask,float *out,unsigned hop,float floor) {
    \\unsigned tid=threadIdx.x,frame=blockIdx.x;__shared__ float2 values[{{fft_size}}];__shared__ float mag[{{fft_bins}}];
    \\for(unsigned i=tid;i<{{fft_size}};i+={{fft_threads}}) { unsigned r=__brev(i)>>{{reverse_shift}}; values[r]=make_float2(i<{{fft_samples}} ? input[(size_t)frame*hop+i]*plan[{{fft_size}}+i] : 0.f,0.f); }
    \\__syncthreads();
    \\{{fft_separate}}
    \\bool bad=false;
    \\for(unsigned i=tid;i<{{fft_bins}};i+={{fft_threads}}) { float2 x=values[i];mag[i]=sqrtf({{power}});bad|=!isfinite(mag[i]); }
    \\int nonfinite=__syncthreads_or(bad);
    \\{{mel_projection}}
    \\}
    \\
, cuda);

pub const metal_channel = struct {
    pub const source_0 = program.metal(
        "inline void antfly_channel_norm_relu(device const float *input, device const float *weight, device float *output, uint base, uint stride, uint width, float eps) {\n" ++
            audio.ChannelNorm.native(.{ .integer = "uint", .index = "uint", .source = .{ .compact = true }, .sqrt = "sqrt", .maximum = "max" }) ++ "}\n",
    );
};
pub const cuda_frontend = struct {
    pub const source_0 =
        "__device__ __forceinline__ void antfly_channel_norm_relu_f32(const float *input,const float *weight,float *output,size_t base,unsigned stride,unsigned width,float eps) {\n" ++
        audio.ChannelNorm.native(.{ .integer = "unsigned", .index = "size_t", .source = cuda_math, .sqrt = "sqrtf", .maximum = "fmaxf", .zero_first = true }) ++ "}\n" ++
        "extern \"C\" __global__ void antfly_audio_frontend_f32(const float *input, const float *aux, float *out, unsigned mode, unsigned rows, unsigned width, unsigned hop, float floor, unsigned batch) {\nunsigned i=blockIdx.x*blockDim.x+threadIdx.x;\n" ++
        audio.Frontend.native(.{ .integer = "unsigned", .wide = "size_t", .source = cuda_math, .sqrt = "sqrtf", .log = "logf", .normalize = "antfly_channel_norm_relu_f32" }) ++ "}\n";
};
