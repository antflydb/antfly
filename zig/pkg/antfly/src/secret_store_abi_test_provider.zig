// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled independently so the consumer cannot share this archive's Zig
//! error numbering or std.Io implementation.
const std = @import("std");
const secrets = @import("common/secrets.zig");
const error_abi = @import("runtime_error_abi.zig");

// Tests execute serially. Inject inside the owning archive so the consumer
// must receive cancellation through stable callback status transport.
var open_fault: u8 = 0;
const io_vtable: std.Io.VTable = blk: {
    var result = std.Options.debug_io.vtable.*;
    result.dirOpenFile = openFile;
    break :blk result;
};

fn openFile(userdata: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
    return switch (open_fault) {
        0 => std.Options.debug_io.vtable.dirOpenFile(userdata, dir, path, options),
        1 => error.Canceled,
        2 => error.FileNotFound,
        else => unreachable,
    };
}

fn ownerIo() std.Io {
    return .{ .userdata = std.Options.debug_io.userdata, .vtable = &io_vtable };
}

export fn secret_store_abi_set_open_fault(fault: u8) callconv(.c) void {
    std.debug.assert(fault <= 2);
    open_fault = fault;
}

export fn secret_store_abi_create(
    allocator: *const std.mem.Allocator,
    path: [*]const u8,
    path_len: usize,
    output: *?*secrets.FileStore,
) callconv(.c) error_abi.Status {
    const store = allocator.create(secrets.FileStore) catch |err| return error_abi.statusFromError(err);
    store.* = secrets.FileStore.initWithIo(allocator.*, ownerIo(), path[0..path_len]) catch |err| {
        allocator.destroy(store);
        return error_abi.statusFromError(err);
    };
    output.* = store;
    return .{};
}

export fn secret_store_abi_create_layered(
    allocator: *const std.mem.Allocator,
    primary: [*]const u8,
    primary_len: usize,
    fallback: [*]const u8,
    fallback_len: usize,
    output: *?*secrets.FileStore,
) callconv(.c) error_abi.Status {
    const store = allocator.create(secrets.FileStore) catch |err| return error_abi.statusFromError(err);
    store.* = secrets.FileStore.initLayeredWithIo(allocator.*, ownerIo(), &.{ primary[0..primary_len], fallback[0..fallback_len] }) catch |err| {
        allocator.destroy(store);
        return error_abi.statusFromError(err);
    };
    output.* = store;
    return .{};
}

export fn secret_store_abi_destroy(store: *secrets.FileStore) callconv(.c) void {
    const allocator = store.alloc;
    store.deinit();
    allocator.destroy(store);
}

const secret_contract = @import("common/secret_contract.zig");
const secret_record = @import("common/secret_record.zig");
const FoundationFixture = struct {
    var context: u8 = 0;
    fn resolve(_: *anyopaque, alloc: std.mem.Allocator, scope: []const u8, _: []const u8, _: secret_contract.ReadOptions) !secret_contract.Lookup {
        if (std.mem.eql(u8, scope, "unavailable")) return error.Unavailable;
        return .{ .revision = 9, .value = .{ .revision = 8, .secret = .{ .bytes = try alloc.dupe(u8, "archive-secret") } } };
    }
    fn list(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: secret_contract.ReadOptions) !secret_contract.Listing {
        const key = try alloc.dupe(u8, "token");
        errdefer alloc.free(key);
        const entries = try alloc.alloc(secret_contract.Metadata, 1);
        entries[0] = .{ .key = key, .revision = 8 };
        return .{ .revision = 9, .entries = entries };
    }
    fn refresh(_: *anyopaque, _: []const u8) !secret_contract.Health {
        return .{ .revision = 9, .stale = true, .available = false };
    }
    fn put(_: *anyopaque, _: []const u8, _: []const u8, _: []const u8, expected: secret_contract.ExpectedRevision) !secret_contract.Mutation {
        try expected.check(8);
        return .{ .revision = 10 };
    }
    fn remove(_: *anyopaque, _: []const u8, _: []const u8, expected: secret_contract.ExpectedRevision) !secret_contract.Mutation {
        try expected.check(8);
        return .{ .revision = 10 };
    }
    fn wrap(_: *anyopaque, _: std.mem.Allocator, _: secret_contract.Identity, _: *const secret_record.DataKey) !secret_record.WrappedKey {
        return error.Unavailable;
    }
    fn unwrap(_: *anyopaque, _: secret_contract.Identity, _: []const u8, _: []const u8, _: *secret_record.DataKey) !void {
        return error.Unavailable;
    }
};

export fn secret_foundation_abi_handles(source: *secret_contract.Source, writer: *secret_contract.NativeStore.Writer, keys: *secret_record.KeyProvider) callconv(.c) void {
    source.* = .{ .ptr = &FoundationFixture.context, .vtable = &.{ .resolve = FoundationFixture.resolve, .list_metadata = FoundationFixture.list, .refresh = FoundationFixture.refresh } };
    writer.* = .{ .ptr = &FoundationFixture.context, .vtable = &.{ .put = FoundationFixture.put, .remove_override = FoundationFixture.remove } };
    keys.* = .{ .ptr = &FoundationFixture.context, .vtable = &.{ .wrap = FoundationFixture.wrap, .unwrap = FoundationFixture.unwrap } };
}
