// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata extension lifecycle operations.

const std = @import("std");
const operation = @import("../api/operation.zig");
const extension_domain = @import("../extensions/mod.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        install: *const fn (*anyopaque, std.mem.Allocator, []const u8, extension_domain.InstallExtensionRequest) anyerror!extension_domain.InstalledExtension,
        update: *const fn (*anyopaque, std.mem.Allocator, []const u8, extension_domain.UpdateExtensionRequest) anyerror!extension_domain.InstalledExtension,
        drop: *const fn (*anyopaque, std.mem.Allocator, []const u8, extension_domain.DropExtensionRequest) anyerror!void,
        enable: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!extension_domain.InstalledExtension,
        disable: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!extension_domain.InstalledExtension,
        configure: *const fn (*anyopaque, std.mem.Allocator, []const u8, extension_domain.ConfigureExtensionRequest) anyerror!extension_domain.InstalledExtension,
        restore: *const fn (*anyopaque, std.mem.Allocator, []const extension_domain.InstalledExtension, []const extension_domain.ExtensionMember, []const extension_domain.ExtensionDependency) anyerror!void,
    };
};

pub const Operations = struct {
    source: Source,

    pub fn install(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8, request: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
        try ctx.ensureActive();
        return self.source.vtable.install(self.source.ptr, alloc, name, request);
    }

    pub fn update(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8, request: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        try ctx.ensureActive();
        return self.source.vtable.update(self.source.ptr, alloc, name, request);
    }

    pub fn drop(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8, request: extension_domain.DropExtensionRequest) !void {
        try ctx.ensureActive();
        try self.source.vtable.drop(self.source.ptr, alloc, name, request);
    }

    pub fn enable(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8) !extension_domain.InstalledExtension {
        try ctx.ensureActive();
        return self.source.vtable.enable(self.source.ptr, alloc, name);
    }

    pub fn disable(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8) !extension_domain.InstalledExtension {
        try ctx.ensureActive();
        return self.source.vtable.disable(self.source.ptr, alloc, name);
    }

    pub fn configure(self: Operations, alloc: std.mem.Allocator, ctx: operation.RequestContext, name: []const u8, request: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        try ctx.ensureActive();
        return self.source.vtable.configure(self.source.ptr, alloc, name, request);
    }

    pub fn restore(
        self: Operations,
        alloc: std.mem.Allocator,
        ctx: operation.RequestContext,
        installed: []const extension_domain.InstalledExtension,
        members: []const extension_domain.ExtensionMember,
        dependencies: []const extension_domain.ExtensionDependency,
    ) !void {
        try ctx.ensureActive();
        try self.source.vtable.restore(self.source.ptr, alloc, installed, members, dependencies);
    }
};

test "metadata extension operations stop canceled requests before mutation" {
    const FakeSource = struct {
        calls: usize = 0,

        fn unsupportedInstalled(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
            return error.UnsupportedOperation;
        }
        fn unsupportedUpdated(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
            return error.UnsupportedOperation;
        }
        fn unsupportedDrop(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: extension_domain.DropExtensionRequest) !void {
            return error.UnsupportedOperation;
        }
        fn unsupportedNamed(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !extension_domain.InstalledExtension {
            return error.UnsupportedOperation;
        }
        fn unsupportedConfigure(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
            return error.UnsupportedOperation;
        }
        fn restore(ptr: *anyopaque, _: std.mem.Allocator, _: []const extension_domain.InstalledExtension, _: []const extension_domain.ExtensionMember, _: []const extension_domain.ExtensionDependency) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = .{ .ptr = &source, .vtable = &.{
        .install = FakeSource.unsupportedInstalled,
        .update = FakeSource.unsupportedUpdated,
        .drop = FakeSource.unsupportedDrop,
        .enable = FakeSource.unsupportedNamed,
        .disable = FakeSource.unsupportedNamed,
        .configure = FakeSource.unsupportedConfigure,
        .restore = FakeSource.restore,
    } } };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, operations.restore(std.testing.allocator, .{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }, &.{}, &.{}, &.{}));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}
