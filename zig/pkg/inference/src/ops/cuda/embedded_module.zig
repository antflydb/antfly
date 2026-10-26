// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transactional ownership for embedded, operation-specific CUDA programs.
//! Entry names are compile-time ABI data. Launches still use direct CUfunctions;
//! this owner adds no runtime dispatch, argument packing or schedule policy.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");

pub fn EmbeddedModule(comptime image: [:0]const u8, comptime entries: anytype) type {
    const fields = @typeInfo(@TypeOf(entries)).@"struct".field_names;
    return struct {
        const Self = @This();
        pub const Entry = std.meta.FieldEnum(@TypeOf(entries));
        module: driver.CUmodule = null,
        functions: [fields.len]driver.CUfunction = @splat(null),

        pub fn function(self: *const Self, entry: Entry) driver.CUfunction {
            return self.functions[@backingInt(entry)];
        }

        /// The qualified families use a 2D grid, a 1D block and no dynamic
        /// shared memory. Keep their driver call and launch accounting together.
        pub inline fn launch(self: *const Self, ctx: *context.CudaContext, entry: Entry, grid_x: u32, grid_y: u32, threads: u32, args: []?*anyopaque) driver.Error!void {
            try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.function(entry), grid_x, grid_y, 1, threads, 1, 1, 0, ctx.stream, args.ptr, null));
            ctx.noteKernelLaunch();
        }

        pub fn init(self: *Self, ctx: *context.CudaContext) !void {
            try ctx.makeCurrent();
            if (self.module != null) return;
            try self.load(&ctx.driver);
        }

        fn load(self: *Self, api: anytype) !void {
            var module: driver.CUmodule = null;
            try api.check(api.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
            errdefer _ = api.fns.cuModuleUnload(module);
            var functions: [fields.len]driver.CUfunction = @splat(null);
            inline for (fields, 0..) |field, i| {
                try api.check(api.fns.cuModuleGetFunction(&functions[i], module, @field(entries, field)));
            }
            // Publish only after every lookup succeeds. A failed lookup leaves
            // a retryable empty owner and cannot expose unloaded function handles.
            self.* = .{ .module = module, .functions = functions };
        }

        pub fn deinit(self: *Self, ctx: *context.CudaContext) void {
            if (self.module != null) {
                ctx.makeCurrent() catch {};
                _ = ctx.driver.fns.cuModuleUnload(self.module);
            }
            self.* = .{};
        }
    };
}

// Exercise rollback without requiring a CUDA device or dynamically loaded driver.
test "embedded CUDA program publishes all entries atomically and retries failed loads" {
    const Mock = struct {
        var lookups: usize = 0;
        var unloads: usize = 0;
        var fail_load: bool = false;
        var fail_lookup: bool = true;
        fns: struct {
            cuModuleLoadDataEx: @TypeOf(&load) = &load,
            cuModuleGetFunction: @TypeOf(&lookup) = &lookup,
            cuModuleUnload: @TypeOf(&unload) = &unload,
        } = .{},
        fn check(_: *@This(), result: c_int) !void {
            if (result != 0) return error.MockDriverFailure;
        }
        fn load(out: *driver.CUmodule, _: ?*const anyopaque, _: c_uint, _: ?[*]driver.CUjit_option, _: ?[*]?*anyopaque) c_int {
            if (fail_load) return 1;
            out.* = @ptrFromInt(0x1000);
            return 0;
        }
        fn lookup(out: *driver.CUfunction, _: driver.CUmodule, _: [*:0]const u8) c_int {
            lookups += 1;
            if (fail_lookup and lookups == 2) return 1;
            out.* = @ptrFromInt(0x2000 + lookups * 16);
            return 0;
        }
        fn unload(_: driver.CUmodule) c_int {
            unloads += 1;
            return 0;
        }
    };
    Mock.lookups = 0;
    Mock.unloads = 0;
    Mock.fail_load = true;
    Mock.fail_lookup = true;
    var api: Mock = .{};
    var program: EmbeddedModule("image", .{ .first = "first", .second = "second" }) = .{};
    try std.testing.expectError(error.MockDriverFailure, program.load(&api));
    try std.testing.expectEqual(@as(usize, 0), Mock.unloads);
    Mock.fail_load = false;
    try std.testing.expectError(error.MockDriverFailure, program.load(&api));
    try std.testing.expect(program.module == null);
    try std.testing.expect(program.function(.first) == null);
    try std.testing.expect(program.function(.second) == null);
    try std.testing.expectEqual(@as(usize, 1), Mock.unloads);
    Mock.fail_lookup = false;
    try program.load(&api);
    try std.testing.expect(program.module != null);
    try std.testing.expect(program.function(.first) != null);
    try std.testing.expect(program.function(.second) != null);
    try std.testing.expect(program.function(.first) != program.function(.second));
    try std.testing.expectEqual(@as(usize, 1), Mock.unloads);
}
