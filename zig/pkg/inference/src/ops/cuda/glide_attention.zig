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
pub const Params = extern struct {
    q: u64,
    k: u64,
    v: u64,
    out: u64,
    ranges: u64,
    queries: u32,
    heads: u32,
    kv_heads: u32,
    scale: f32,
};
pub const DenseParams = extern struct {
    q: u64,
    k: u64,
    v: u64,
    out: u64,
    qp: u64,
    kp: u64,
    vp: u64,
    scores: u64,
    probabilities: u64,
    op: u64,
    query_start: u32,
    key_start: u32,
    queries: u32,
    keys: u32,
    heads: u32,
    kv_heads: u32,
    scale: f32,
    padded_keys: u32,
};
comptime {
    if (@sizeOf(Params) != 56 or @offsetOf(Params, "queries") != 40 or
        @sizeOf(DenseParams) != 112 or @offsetOf(DenseParams, "query_start") != 80)
        @compileError("GLiDE attention CUDA parameter ABI changed");
}
pub const Module = struct {
    module: driver.CUmodule = null,
    function: driver.CUfunction = null,
    dense_functions: [4]driver.CUfunction = @splat(null),
    tensor_function: driver.CUfunction = null,
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch return;
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }
    fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module == null) {
            var module: driver.CUmodule = null;
            const image = @embedFile("glide_attention.ptx") ++ "\x00";
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
            errdefer _ = ctx.driver.fns.cuModuleUnload(module);
            var function: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "glide_attention_f32"));
            var functions: [4]driver.CUfunction = @splat(null);
            for (&functions, [_][:0]const u8{ "glide_pack_kv", "glide_pack_q", "glide_softmax", "glide_unpack_o" }) |*fun, name|
                try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(fun, module, name));
            var tensor: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&tensor, module, "glide_attention_f16"));
            self.tensor_function = tensor;
            self.dense_functions = functions;
            self.module = module;
            self.function = function;
        }
    }
    pub fn dense(self: *Module, ctx: *context.CudaContext, index: usize, parameters: DenseParams) !void {
        try self.init(ctx);
        var p = parameters;
        var args = [_]?*anyopaque{&p};
        const blocks = if (index == 0) ((p.padded_keys + 31) / 32) * 8 * p.kv_heads else p.queries * p.heads;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.dense_functions[index], blocks, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, parameters: Params, tensor: bool) !void {
        try self.init(ctx);
        var p = parameters;
        var args = [_]?*anyopaque{&p};
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(if (tensor) self.tensor_function else self.function, if (tensor) (p.queries + 15) / 16 else (p.queries * p.heads + 3) / 4, if (tensor) p.heads else 1, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
};
