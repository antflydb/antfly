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

// Native export names remain the compatibility ABI of the qualified PTX.
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("lighton_attention.ptx") ++ "\x00", .{
    .chunks_d128 = "lighton_attention_chunks",
    .combine_d128 = "lighton_attention_combine",
    .write_suffix = "lighton_attention_write_suffix",
    .chunks128_d128 = "lighton_attention_chunks128",
});

pub const Module = struct {
    program: Program = .{},
    calls: usize = 0,
    suffix_write_calls: usize = 0,
    chunk128_calls: usize = 0,
    items: usize = 0,

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.program.module != null) {
            self.program.deinit(ctx);
            self.* = .{};
        }
    }
    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }
    pub fn writeSuffix(self: *Module, ctx: *context.CudaContext, params: *Params) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try self.program.launch(ctx, .write_suffix, (params.row_values + 127) / 128, params.items, 128, &args);
        self.suffix_write_calls += 1;
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, params: *Params, chunk128: bool) !void {
        var args = [_]?*anyopaque{@ptrCast(params)};
        try self.program.launch(ctx, if (chunk128) .chunks128_d128 else .chunks_d128, params.heads, params.items * params.chunks, 128, &args);
        try self.program.launch(ctx, .combine_d128, params.items * params.heads, 1, 128, &args);
        self.calls += 1;
        self.chunk128_calls += @intFromBool(chunk128);
        self.items += params.items;
    }
};
