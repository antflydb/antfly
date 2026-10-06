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

const std = @import("std");

/// Stable runtime allocation. Init adopts resources only after success; the
/// constructor remains responsible for unwinding its own partial internals.
pub fn RuntimeOwner(comptime Runtime: type) type {
    return struct {
        runtime: ?*Runtime = null,
        pub fn create(alloc: std.mem.Allocator, args: anytype) !@This() {
            const runtime = try alloc.create(Runtime);
            errdefer alloc.destroy(runtime);
            runtime.* = try @call(.auto, Runtime.init, args);
            return .{ .runtime = runtime };
        }
        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.runtime) |runtime| {
                runtime.deinit();
                alloc.destroy(runtime);
                self.runtime = null;
            }
        }
    };
}

/// Runtime and stable callback context have distinct retirement steps because
/// another runtime may retain this context during its final durable drain.
/// The coordinator must retire that dependent before deinitContext.
pub fn Bundle(comptime Context: type, comptime Runtime: type) type {
    return ManagedBundle(Context, Runtime, null);
}
/// Context-owned resources transfer only on successful construction. The caller
/// unwinds its source value on error; release runs once after runtime retirement.
pub fn OwningBundle(comptime Context: type, comptime Runtime: type, comptime release: *const fn (*Context) void) type {
    return ManagedBundle(Context, Runtime, release);
}
fn ManagedBundle(comptime Context: type, comptime Runtime: type, comptime release: ?*const fn (*Context) void) type {
    return struct {
        context: ?*Context = null,
        runtime: ?*Runtime = null,
        /// The constructor receives allocator and stable context before args.
        /// It assembles borrowed capabilities; this owner handles allocations.
        pub fn create(alloc: std.mem.Allocator, context: Context, args: anytype, comptime construct: anytype) !@This() {
            const stable = try alloc.create(Context);
            errdefer alloc.destroy(stable);
            stable.* = context;
            const runtime = try alloc.create(Runtime);
            errdefer alloc.destroy(runtime);
            runtime.* = try @call(.auto, construct, .{ alloc, stable } ++ args);
            return .{ .context = stable, .runtime = runtime };
        }
        pub fn deinitRuntime(self: *@This(), alloc: std.mem.Allocator) void {
            var owner = RuntimeOwner(Runtime){ .runtime = self.runtime };
            owner.deinit(alloc);
            self.runtime = null;
        }
        pub fn deinitContext(self: *@This(), alloc: std.mem.Allocator) void {
            std.debug.assert(self.runtime == null);
            if (self.context) |context| {
                if (release) |cleanup| cleanup(context);
                alloc.destroy(context);
            }
            self.context = null;
        }
        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.deinitRuntime(alloc);
            self.deinitContext(alloc);
        }
    };
}

test "local runtime bundle unwinds stable allocations and preserves dependent context lifetime" {
    const Context = struct { value: u64 };
    const Runtime = struct {
        context: *Context,
        fn init(_: std.mem.Allocator, context: *Context) !@This() {
            return .{ .context = context };
        }
        pub fn deinit(self: *@This()) void {
            self.context.value += 1;
        }
    };
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var bundle = try Bundle(Context, Runtime).create(alloc, .{ .value = 7 }, .{}, Runtime.init);
            defer bundle.deinit(alloc);
            try std.testing.expect(bundle.runtime.?.context == bundle.context.?);
            bundle.deinitRuntime(alloc);
            try std.testing.expectEqual(@as(u64, 8), bundle.context.?.value);
            bundle.deinitContext(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "local runtime bundle returns constructor errors without adopting context" {
    const Context = struct { value: u64 };
    const Runtime = struct {
        fn init(_: std.mem.Allocator, _: *Context) !@This() {
            return error.ConstructorFailed;
        }
        pub fn deinit(_: *@This()) void {
            unreachable;
        }
    };
    try std.testing.expectError(error.ConstructorFailed, Bundle(Context, Runtime).create(std.testing.allocator, .{ .value = 1 }, .{}, Runtime.init));
}

test "local owning runtime bundle retires resources exactly once after the runtime" {
    const Context = struct {
        order: *u32,
        fn release(self: *@This()) void {
            std.debug.assert(self.order.* == 1);
            self.order.* = 2;
        }
    };
    const Runtime = struct {
        context: *Context,
        fn init(_: std.mem.Allocator, context: *Context) !@This() {
            return .{ .context = context };
        }
        pub fn deinit(self: *@This()) void {
            self.context.order.* = 1;
        }
    };
    var order: u32 = 0;
    var owner = try OwningBundle(Context, Runtime, Context.release).create(std.testing.allocator, .{ .order = &order }, .{}, Runtime.init);
    owner.deinit(std.testing.allocator);
    owner.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), order);
}

test "local owning runtime bundle leaves source resources with caller on construction failure" {
    const Context = struct {
        alloc: std.mem.Allocator,
        bytes: []u8,
        fn release(self: *@This()) void {
            self.alloc.free(self.bytes);
        }
    };
    const Runtime = struct {
        fn init(_: std.mem.Allocator, _: *Context, refuse: bool) !@This() {
            if (refuse) return error.ConstructorFailed;
            return .{};
        }
        pub fn deinit(_: *@This()) void {}
    };
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var source: Context = .{ .alloc = alloc, .bytes = try alloc.dupe(u8, "owned") };
            errdefer source.release();
            var owner = try OwningBundle(Context, Runtime, Context.release).create(alloc, source, .{false}, Runtime.init);
            defer owner.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    var source: Context = .{ .alloc = std.testing.allocator, .bytes = try std.testing.allocator.dupe(u8, "owned") };
    defer source.release();
    try std.testing.expectError(error.ConstructorFailed, OwningBundle(Context, Runtime, Context.release).create(std.testing.allocator, source, .{true}, Runtime.init));
    try std.testing.expectEqualStrings("owned", source.bytes);
}
