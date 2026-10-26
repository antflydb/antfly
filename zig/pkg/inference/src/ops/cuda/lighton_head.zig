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

/// Static CUDA 13.0 / SM12.1 images. BF16 operands, F32 accumulation and
/// comparisons. The two final ABI pointers are unused Triton scratch slots;
/// generation rejects any image requiring global or profiling scratch.
pub const Module = struct {
    modules: [3]driver.CUmodule = .{ null, null, null },
    functions: [3]driver.CUfunction = .{ null, null, null },

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.functions[2] != null) return;
        var prepared = Module{};
        errdefer prepared.deinit(ctx);
        const images = .{
            @embedFile("lighton_head_08.ptx") ++ "\x00",
            @embedFile("lighton_head_4b.ptx") ++ "\x00",
            @embedFile("lighton_head_finish.ptx") ++ "\x00",
        };
        inline for (images, 0..) |image, i| {
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&prepared.modules[i], image.ptr, 0, null, null));
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&prepared.functions[i], prepared.modules[i], if (i == 2) "lighton_head_finish" else "lighton_head"));
        }
        self.* = prepared;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        ctx.makeCurrent() catch {};
        for (self.modules) |module| if (module != null) {
            _ = ctx.driver.fns.cuModuleUnload(module);
        };
        self.* = .{};
    }

    pub fn run(self: *Module, ctx: *context.CudaContext, pointers: [5]driver.CUdeviceptr, rows: u32, hidden: usize) !void {
        var p = pointers;
        var count = rows;
        var unused: driver.CUdeviceptr = 0;
        var global_scratch: driver.CUdeviceptr = 0;
        var profile_scratch: driver.CUdeviceptr = 0;
        // X, W, unused full-matrix output, tile maxima, tile indices, rows,
        // unused global scratch and unused profiling scratch.
        var args = [_]?*anyopaque{ &p[0], &p[1], &unused, &p[2], &p[3], &count, &global_scratch, &profile_scratch };
        const index: usize = if (hidden == 1024) 0 else 1;
        const tile_rows: u32 = if (index == 0) 64 else 32;
        const shared: u32 = if (index == 0) 24576 else 20480;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[index], (rows + tile_rows - 1) / tile_rows, 1940, 1, 128, 1, 1, shared, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        var finish_args = [_]?*anyopaque{ &p[2], &p[3], &p[4], &global_scratch, &profile_scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[2], rows, 1, 1, 128, 1, 1, 16, ctx.stream, &finish_args, null));
        ctx.noteKernelLaunch();
    }
};
