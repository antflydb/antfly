// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled independently so the consumer cannot share this archive's Zig
//! error numbering or std.Io implementation.
const std = @import("std");
const secrets = @import("common/secrets.zig");
const error_abi = @import("runtime_error_abi.zig");

export fn secret_store_abi_create(
    allocator: *const std.mem.Allocator,
    path: [*]const u8,
    path_len: usize,
    output: *?*secrets.FileStore,
) callconv(.c) error_abi.Status {
    const store = allocator.create(secrets.FileStore) catch |err| return error_abi.statusFromError(err);
    store.* = secrets.FileStore.initWithIo(allocator.*, std.Options.debug_io, path[0..path_len]) catch |err| {
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
