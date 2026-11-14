// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const context = @import("context.zig");
const driver = @import("driver.zig");
pub const Module = struct {
    module: driver.CUmodule = null,
    function: driver.CUfunction = null,
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch return;
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        try ctx.makeCurrent();
        if (self.module == null) {
            var module: driver.CUmodule = null;
            const image = @embedFile("glide_activation.ptx") ++ "\x00";
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
            errdefer _ = ctx.driver.fns.cuModuleUnload(module);
            var function: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "glide_silu_up_f16"));
            self.module = module;
            self.function = function;
        }
        var p = pointers;
        var n = count;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.function, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
};
