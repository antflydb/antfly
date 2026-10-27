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

const context = @import("context.zig");
const driver = @import("driver.zig");

pub const Page = extern struct {
    tokens: u32 = 0,
    query_position: u32 = 0,
    block_count: u32 = 0,
    blocks: [64]u32 = @splat(0),
};
pub const Params = extern struct {
    q: driver.CUdeviceptr = 0,
    k: driver.CUdeviceptr = 0,
    v: driver.CUdeviceptr = 0,
    out: driver.CUdeviceptr = 0,
    partials: driver.CUdeviceptr = 0,
    statistics: driver.CUdeviceptr = 0,
    new_k: driver.CUdeviceptr = 0,
    new_v: driver.CUdeviceptr = 0,
    items: u32 = 0,
    heads: u32 = 0,
    kv_heads: u32 = 0,
    chunks: u32 = 0,
    page_size: u32 = 0,
    row_values: u32 = 0,
    capacity: u32 = 0,
    pages: [64]Page = @splat(.{}),
};
comptime {
    if (@sizeOf(Page) != 268 or @sizeOf(Params) != 17248 or @offsetOf(Params, "pages") != 92)
        @compileError("LightOn CUDA attention packet ABI changed");
}

pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [4]driver.CUfunction = @splat(null),
    calls: usize = 0,
    suffix_write_calls: usize = 0,
    chunk128_calls: usize = 0,
    items: usize = 0,

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.* = .{};
        }
    }
    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module != null) return;
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_attention.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [4]driver.CUfunction = @splat(null);
        inline for (.{ "lighton_attention_chunks", "lighton_attention_combine", "lighton_attention_write_suffix", "lighton_attention_chunks128" }, 0..) |name, i|
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[i], module, name));
        self.module = module;
        self.functions = functions;
    }
    pub fn writeSuffix(self: *Module, ctx: *context.CudaContext, params: *Params) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[2], (params.row_values + 127) / 128, params.items, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.suffix_write_calls += 1;
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, params: *Params, chunk128: bool) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[if (chunk128) 3 else 0], params.heads, params.items * params.chunks, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[1], params.items * params.heads, 1, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
        self.chunk128_calls += @intFromBool(chunk128);
        self.items += params.items;
    }
};
