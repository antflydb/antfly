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

//! Optional bounded FP16 cuDNN convolutions for Florence's four downsamplers.
const std = @import("std");
const Desc = ?*anyopaque;
const Status = c_int;
const Perf = extern struct {
    algo: c_int,
    status: Status,
    time: f32,
    memory: usize,
    determinism: c_int,
    math_type: c_int,
    reserved: [3]c_int,
};
const Table = struct {
    cudnnCreate: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroy: *const fn (Desc) callconv(.c) Status,
    cudnnSetStream: *const fn (Desc, ?*anyopaque) callconv(.c) Status,
    cudnnCreateTensorDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyTensorDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetTensor4dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnCreateFilterDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyFilterDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetFilter4dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnCreateConvolutionDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyConvolutionDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetConvolution2dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnSetConvolutionMathType: *const fn (Desc, c_int) callconv(.c) Status,
    cudnnGetConvolutionForwardAlgorithm_v7: *const fn (Desc, Desc, Desc, Desc, Desc, c_int, *c_int, [*]Perf) callconv(.c) Status,
    cudnnConvolutionForward: *const fn (Desc, *const f32, Desc, ?*const anyopaque, Desc, ?*const anyopaque, Desc, c_int, ?*anyopaque, usize, *const f32, Desc, ?*anyopaque) callconv(.c) Status,
    cudnnAddTensor: *const fn (Desc, *const f32, Desc, ?*const anyopaque, *const f32, Desc, ?*anyopaque) callconv(.c) Status,
};

pub const Key = struct {
    batch: usize,
    inputs: usize,
    outputs: usize,
    height: usize,
    width: usize,
    kernel: usize,
    stride: usize,
    padding: usize,

    pub fn eligible(k: Key) bool {
        if (k.batch == 0 or k.batch > 64 or k.height == 0 or k.width == 0 or k.height > 4096 or k.width > 4096) return false;
        return (k.inputs == 3 and k.outputs == 128 and k.kernel == 7 and k.stride == 4 and k.padding == 3) or
            ((k.inputs == 128 or k.inputs == 256 or k.inputs == 512) and k.outputs == k.inputs * 2 and k.kernel == 3 and k.stride == 2 and k.padding == 1);
    }
    pub fn outHeight(k: Key) usize {
        return (k.height + 2 * k.padding - k.kernel) / k.stride + 1;
    }
    pub fn outWidth(k: Key) usize {
        return (k.width + 2 * k.padding - k.kernel) / k.stride + 1;
    }
};

pub const Plan = struct {
    key: Key = undefined,
    x: Desc = null,
    y: Desc = null,
    bias: Desc = null,
    weight: Desc = null,
    convolution: Desc = null,
    algorithm: c_int = 0,
    workspace_bytes: usize = 0,

    fn deinit(p: *Plan, f: *const Table) void {
        inline for (.{ "x", "y", "bias" }) |name| {
            if (@field(p, name) != null) _ = f.cudnnDestroyTensorDescriptor(@field(p, name));
        }
        if (p.weight != null) _ = f.cudnnDestroyFilterDescriptor(p.weight);
        if (p.convolution != null) _ = f.cudnnDestroyConvolutionDescriptor(p.convolution);
        p.* = .{};
    }
};

pub const Module = struct {
    lib: std.DynLib,
    f: Table,
    handle: Desc,
    plans: [32]Plan = @splat(.{}),
    count: usize = 0,
    calls: usize = 0,
    pub const workspace_limit = 64 * 1024 * 1024;

    pub fn init(stream: ?*anyopaque) !Module {
        var lib = std.DynLib.open("libcudnn.so.9") catch return error.CudnnUnavailable;
        errdefer lib.close();
        var f: Table = undefined;
        inline for (@typeInfo(Table).@"struct".field_names) |name| {
            @field(f, name) = lib.lookup(@FieldType(Table, name), name) orelse return error.CudnnUnavailable;
        }
        var handle: Desc = null;
        try check(f.cudnnCreate(&handle));
        errdefer _ = f.cudnnDestroy(handle);
        try check(f.cudnnSetStream(handle, stream));
        return .{ .lib = lib, .f = f, .handle = handle };
    }
    pub fn deinit(m: *Module) void {
        for (m.plans[0..m.count]) |*p| p.deinit(&m.f);
        _ = m.f.cudnnDestroy(m.handle);
        m.lib.close();
        m.* = undefined;
    }
    pub fn plan(m: *Module, key: Key) !?*const Plan {
        if (!key.eligible()) return null;
        for (m.plans[0..m.count]) |*p| if (std.meta.eql(key, p.key)) return p;
        if (m.count == m.plans.len) return null;
        var p = Plan{ .key = key };
        errdefer p.deinit(&m.f);
        try check(m.f.cudnnCreateTensorDescriptor(&p.x));
        try check(m.f.cudnnCreateTensorDescriptor(&p.y));
        try check(m.f.cudnnCreateTensorDescriptor(&p.bias));
        try check(m.f.cudnnCreateFilterDescriptor(&p.weight));
        try check(m.f.cudnnCreateConvolutionDescriptor(&p.convolution));
        // NCHW, original FP16 operands, F32 accumulation, cross-correlation.
        try check(m.f.cudnnSetTensor4dDescriptor(p.x, 0, 2, @intCast(key.batch), @intCast(key.inputs), @intCast(key.height), @intCast(key.width)));
        try check(m.f.cudnnSetTensor4dDescriptor(p.y, 0, 2, @intCast(key.batch), @intCast(key.outputs), @intCast(key.outHeight()), @intCast(key.outWidth())));
        try check(m.f.cudnnSetTensor4dDescriptor(p.bias, 0, 2, 1, @intCast(key.outputs), 1, 1));
        try check(m.f.cudnnSetFilter4dDescriptor(p.weight, 2, 0, @intCast(key.outputs), @intCast(key.inputs), @intCast(key.kernel), @intCast(key.kernel)));
        try check(m.f.cudnnSetConvolution2dDescriptor(p.convolution, @intCast(key.padding), @intCast(key.padding), @intCast(key.stride), @intCast(key.stride), 1, 1, 1, 0));
        try check(m.f.cudnnSetConvolutionMathType(p.convolution, 1));
        var candidates: [8]Perf = undefined;
        var count: c_int = 0;
        try check(m.f.cudnnGetConvolutionForwardAlgorithm_v7(m.handle, p.x, p.weight, p.convolution, p.y, candidates.len, &count, &candidates));
        if (count < 0 or count > candidates.len) return error.CudnnFailure;
        var found = false;
        for (candidates[0..@intCast(count)]) |candidate| {
            if (candidate.status != 0 or candidate.memory > workspace_limit or candidate.determinism != 1) continue;
            p.algorithm = candidate.algo;
            p.workspace_bytes = candidate.memory;
            try check(m.f.cudnnSetConvolutionMathType(p.convolution, candidate.math_type));
            found = true;
            break;
        }
        if (!found) {
            p.deinit(&m.f);
            return null;
        }
        m.plans[m.count] = p;
        m.count += 1;
        return &m.plans[m.count - 1];
    }
    pub fn run(m: *Module, p: *const Plan, x: usize, weight: usize, bias: usize, output: usize, workspace: usize) !void {
        const one: f32 = 1;
        const zero: f32 = 0;
        try check(m.f.cudnnConvolutionForward(m.handle, &one, p.x, @ptrFromInt(x), p.weight, @ptrFromInt(weight), p.convolution, p.algorithm, if (p.workspace_bytes == 0) null else @ptrFromInt(workspace), p.workspace_bytes, &zero, p.y, @ptrFromInt(output)));
        try check(m.f.cudnnAddTensor(m.handle, &one, p.bias, @ptrFromInt(bias), &one, p.y, @ptrFromInt(output)));
        m.calls += 1;
    }
};
fn check(status: Status) !void {
    if (status != 0) return error.CudnnFailure;
}

test "LightOnOCR3 CUDA Florence convolution eligibility excludes unrelated and oversized geometry" {
    var key = Key{ .batch = 16, .inputs = 3, .outputs = 128, .height = 768, .width = 768, .kernel = 7, .stride = 4, .padding = 3 };
    try std.testing.expect(key.eligible());
    try std.testing.expectEqual(@as(usize, 192), key.outHeight());
    key.batch = 65;
    try std.testing.expect(!key.eligible());
    key.batch = 16;
    key.stride = 2;
    try std.testing.expect(!key.eligible());
    try std.testing.expectEqual(@as(usize, 48), @sizeOf(Perf));
}
