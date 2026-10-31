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

//! Transactional ownership for embedded, operation-specific CUDA programs.
//! Entry names are compile-time ABI data. Launches still use direct CUfunctions;
//! this owner adds no runtime dispatch, argument packing or schedule policy.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");

pub fn EmbeddedModule(comptime image: [:0]const u8, comptime entries: anytype) type {
    return Module(image, entries);
}

/// Architecture selection stays in the caller; the owner shares lookup and retirement.
pub fn SelectedModule(comptime entries: anytype) type {
    return Module(null, entries);
}

fn Module(comptime image: ?[:0]const u8, comptime entries: anytype) type {
    const enum_entries = @TypeOf(entries) == type;
    const fields = if (enum_entries) @typeInfo(entries).@"enum".field_names else @typeInfo(@TypeOf(entries)).@"struct".field_names;
    comptime {
        if (enum_entries) for (fields, 0..) |field, index| {
            if (@backingInt(@field(entries, field)) != index)
                @compileError("CUDA entry enums must use dense declaration order");
        };
    }
    return struct {
        const Self = @This();
        pub const Entry = if (enum_entries) entries else std.meta.FieldEnum(@TypeOf(entries));
        module: driver.CUmodule = null,
        pending_module: driver.CUmodule = null,
        functions: [fields.len]driver.CUfunction = @splat(null),

        pub inline fn function(self: *const Self, entry: Entry) driver.CUfunction {
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
            try self.initCurrent(&ctx.driver);
        }

        /// Initialize a fixed image after the caller selected its context.
        pub fn initCurrent(self: *Self, api: anytype) !void {
            try self.loadCurrent(api, comptime image orelse @compileError("selected images require loadCurrent"));
        }

        /// Caller has selected the context. Success adds no context/driver calls
        /// on cached access; failed retirement is retried before another load.
        pub fn loadCurrent(self: *Self, api: anytype, bytes: []const u8) !void {
            if (self.module != null) return;
            const names: [fields.len][:0]const u8 = comptime blk: {
                var result: [fields.len][:0]const u8 = undefined;
                for (fields, 0..) |field, i| result[i] = if (enum_entries) field else @field(entries, field);
                break :blk result;
            };
            const loaded = try loadImage(api, bytes, names, &self.pending_module);
            self.* = .{ .module = loaded.module, .functions = loaded.functions };
        }

        pub fn deinitChecked(self: *Self, ctx: anytype) !void {
            if (self.module == null and self.pending_module == null) return;
            try ctx.makeCurrent();
            try self.deinitCurrent(&ctx.driver);
        }

        pub fn deinitCurrent(self: *Self, api: anytype) !void {
            try unloadImage(api, &self.module);
            self.functions = @splat(null);
            try unloadImage(api, &self.pending_module);
        }

        pub fn deinit(self: *Self, ctx: *context.CudaContext) void {
            self.deinitChecked(ctx) catch {};
        }
    };
}

/// An operation family may retain several statically compiled images. The
/// descriptor selects image and ABI entry only; launch geometry stays with its
/// tuned schedule. Cached lookups add no driver calls or dynamic ABI packing.
pub const Image = struct { image: [:0]const u8, entry: [:0]const u8 };

/// Resolve a fixed offline profile at compile time; no serving-time catalog lookup.
pub fn profile(comptime spec: anytype) Image {
    return .{ .image = @field(@import("generated/standalone_images.zig").images, spec.file), .entry = spec.entry };
}

pub fn EmbeddedPrograms(comptime descriptors: anytype) type {
    const fields = @typeInfo(@TypeOf(descriptors)).@"struct".field_names;
    const images: [fields.len]Image = comptime blk: {
        var result: [fields.len]Image = undefined;
        for (fields, 0..) |field, i| {
            const descriptor = @field(descriptors, field);
            result[i] = .{ .image = descriptor.image, .entry = descriptor.entry };
        }
        break :blk result;
    };
    return struct {
        const Self = @This();
        pub const Program = std.meta.FieldEnum(@TypeOf(descriptors));
        modules: [fields.len]driver.CUmodule = @splat(null),
        pending_modules: [fields.len]driver.CUmodule = @splat(null),
        functions: [fields.len]driver.CUfunction = @splat(null),

        pub inline fn function(self: *const Self, program: Program) driver.CUfunction {
            return self.functions[@backingInt(program)];
        }

        pub fn init(self: *Self, ctx: *context.CudaContext, program: Program) !void {
            const index = @backingInt(program);
            if (self.modules[index] != null) return;
            try ctx.makeCurrent();
            try self.load(&ctx.driver, program);
        }

        fn load(self: *Self, api: anytype, program: Program) !void {
            const index = @backingInt(program);
            const image = images[index];
            const loaded = try loadImage(api, image.image, [_][:0]const u8{image.entry}, &self.pending_modules[index]);
            self.modules[index] = loaded.module;
            self.functions[index] = loaded.functions[0];
        }

        pub fn deinitChecked(self: *Self, ctx: anytype) !void {
            try ctx.makeCurrent();
            for (&self.modules, &self.pending_modules, &self.functions) |*module, *pending, *handle| {
                try unloadImage(&ctx.driver, module);
                handle.* = null;
                try unloadImage(&ctx.driver, pending);
            }
        }

        pub fn deinit(self: *Self, ctx: *context.CudaContext) void {
            self.deinitChecked(ctx) catch {};
        }
    };
}

fn LoadedImage(comptime count: usize) type {
    return struct { module: driver.CUmodule, functions: [count]driver.CUfunction };
}

fn unloadImage(api: anytype, module: *driver.CUmodule) !void {
    if (module.* == null) return;
    try api.check(api.fns.cuModuleUnload(module.*));
    module.* = null;
}

fn loadImage(api: anytype, image: []const u8, names: anytype, pending: *driver.CUmodule) !LoadedImage(names.len) {
    try unloadImage(api, pending);
    var module: driver.CUmodule = null;
    try api.check(api.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
    errdefer {
        if (api.fns.cuModuleUnload(module) != 0) pending.* = module;
    }
    var functions: [names.len]driver.CUfunction = @splat(null);
    inline for (0..names.len) |i| try api.check(api.fns.cuModuleGetFunction(&functions[i], module, names[i].ptr));
    return .{ .module = module, .functions = functions };
}

// Exercise rollback without requiring a CUDA device or dynamically loaded driver.
test "embedded CUDA program publishes all entries atomically and retries failed loads" {
    const Mock = struct {
        var lookups: usize = 0;
        var unloads: usize = 0;
        var fail_load: bool = false;
        var fail_lookup: bool = true;
        var fail_unload: bool = false;
        var fail_module: driver.CUmodule = null;
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
        fn unload(module: driver.CUmodule) c_int {
            if (fail_unload or (fail_module != null and module == fail_module)) return 1;
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
    try std.testing.expectError(error.MockDriverFailure, program.initCurrent(&api));
    try std.testing.expectEqual(@as(usize, 0), Mock.unloads);
    Mock.fail_load = false;
    try std.testing.expectError(error.MockDriverFailure, program.initCurrent(&api));
    try std.testing.expect(program.module == null);
    try std.testing.expect(program.function(.first) == null);
    try std.testing.expect(program.function(.second) == null);
    try std.testing.expectEqual(@as(usize, 1), Mock.unloads);
    Mock.fail_lookup = false;
    try program.initCurrent(&api);
    try std.testing.expect(program.module != null);
    try std.testing.expect(program.function(.first) != null);
    try std.testing.expect(program.function(.second) != null);
    try std.testing.expect(program.function(.first) != program.function(.second));
    try std.testing.expectEqual(@as(usize, 1), Mock.unloads);
    // A failure in another image must not retire a previously published program.
    Mock.lookups = 0;
    Mock.unloads = 0;
    Mock.fail_load = false;
    Mock.fail_lookup = true;
    var bank: EmbeddedPrograms(.{
        .first = .{ .image = "image-first", .entry = "first" },
        .second = .{ .image = "image-second", .entry = "second" },
    }) = .{};
    try bank.load(&api, .first);
    const first = bank.function(.first);
    try std.testing.expectError(error.MockDriverFailure, bank.load(&api, .second));
    try std.testing.expectEqual(first, bank.function(.first));
    try std.testing.expect(bank.function(.second) == null);
    try std.testing.expect(bank.modules[1] == null);
    try std.testing.expectEqual(@as(usize, 1), Mock.unloads);
    Mock.fail_lookup = false;
    try bank.load(&api, .second);
    try std.testing.expect(bank.function(.second) != null);
    try std.testing.expectEqual(first, bank.function(.first));
    const Current = struct {
        driver: Mock = .{},
        fail: bool = true,
        fn makeCurrent(self: *@This()) !void {
            if (self.fail) return error.MockContextFailure;
        }
    };
    var current: Current = .{};
    try std.testing.expectError(error.MockContextFailure, bank.deinitChecked(&current));
    try std.testing.expectEqual(first, bank.function(.first));
    current.fail = false;
    Mock.fail_unload = true;
    try std.testing.expectError(error.MockDriverFailure, bank.deinitChecked(&current));
    try std.testing.expectEqual(first, bank.function(.first));
    try std.testing.expect(bank.function(.second) != null);
    Mock.fail_unload = false;
    try bank.deinitChecked(&current);
    try std.testing.expect(bank.function(.first) == null and bank.function(.second) == null);
    current.fail = true;
    try std.testing.expectError(error.MockContextFailure, program.deinitChecked(&current));
    try std.testing.expect(program.module != null);
    current.fail = false;
    Mock.fail_unload = true;
    try std.testing.expectError(error.MockDriverFailure, program.deinitChecked(&current));
    try std.testing.expect(program.module != null);
    Mock.fail_unload = false;
    try program.deinitChecked(&current);
    try std.testing.expect(program.module == null);
    var selected: SelectedModule(enum { first, second }) = .{};
    Mock.lookups = 0;
    Mock.fail_lookup = true;
    Mock.fail_unload = true;
    try std.testing.expectError(error.MockDriverFailure, selected.loadCurrent(&api, "selected-image"));
    try std.testing.expect(selected.module == null and selected.pending_module != null);
    try std.testing.expect(selected.function(.first) == null);
    Mock.fail_lookup = false;
    const lookups = Mock.lookups;
    try std.testing.expectError(error.MockDriverFailure, selected.loadCurrent(&api, "selected-image"));
    try std.testing.expectEqual(lookups, Mock.lookups);
    Mock.fail_unload = false;
    try selected.loadCurrent(&api, "selected-image");
    try std.testing.expect(selected.pending_module == null and selected.function(.second) != null);
    const ready_lookups = Mock.lookups;
    try selected.loadCurrent(&api, "selected-image");
    try std.testing.expectEqual(ready_lookups, Mock.lookups);
    try selected.deinitChecked(&current);
    try std.testing.expect(selected.module == null and selected.function(.first) == null);
    // A parent must retain a failed child, even after another child retired.
    var composite: @import("encoder_block_bf16.zig").Module = .{};
    composite.up.programs.modules[0] = @ptrFromInt(0x3000);
    composite.down.programs.pending_modules[0] = @ptrFromInt(0x4000);
    composite.program.module = @ptrFromInt(0x5000);
    Mock.fail_module = @ptrFromInt(0x4000);
    try std.testing.expectError(error.MockDriverFailure, composite.deinitChecked(&current));
    try std.testing.expect(composite.up.programs.modules[0] == null);
    try std.testing.expectEqual(Mock.fail_module, composite.down.programs.pending_modules[0]);
    try std.testing.expect(composite.program.module != null);
    Mock.fail_module = null;
    try composite.deinitChecked(&current);
    try std.testing.expect(composite.down.programs.pending_modules[0] == null and composite.program.module == null);
}
