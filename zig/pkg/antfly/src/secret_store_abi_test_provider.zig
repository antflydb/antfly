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
