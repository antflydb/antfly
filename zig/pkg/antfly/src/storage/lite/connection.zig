// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const backend = @import("backend.zig");
const db_mod = @import("../db/db.zig");

const Allocator = std.mem.Allocator;

pub const Connection = struct {
    backend: backend.Handle,
    db: db_mod.DB,
    open_mode: db_mod.OpenOptions.OpenMode,

    pub const Options = struct {
        fsync: bool = true,
    };

    pub fn open(allocator: Allocator, path: []const u8, open_mode: db_mod.OpenOptions.OpenMode) !Connection {
        return try openWithOptions(allocator, path, open_mode, .{});
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, open_mode: db_mod.OpenOptions.OpenMode, opts: Options) !Connection {
        var lite_backend = try backend.Handle.open(allocator, path, .{
            .read_only = openModeRequiresReadOnlyBackends(open_mode),
            .no_sync = !opts.fsync,
        });
        errdefer lite_backend.deinit();

        return try openWithBackend(allocator, path, open_mode, &lite_backend);
    }

    /// Opens an existing Lite database or atomically creates a new one. The
    /// exclusive create closes the existence-check race; if another process
    /// wins creation, this process retries the normal writer open and therefore
    /// still respects the single-writer file lock.
    pub fn openOrCreateWithOptions(allocator: Allocator, path: []const u8, opts: Options) !Connection {
        return openWithOptions(allocator, path, .writer, opts) catch |open_err| switch (open_err) {
            error.FileNotFound => createWithOptions(allocator, path, true, opts) catch |create_err| switch (create_err) {
                error.PathAlreadyExists => openWithOptions(allocator, path, .writer, opts),
                else => create_err,
            },
            else => open_err,
        };
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Connection {
        return try createWithOptions(allocator, path, exclusive, .{});
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, exclusive: bool, opts: Options) !Connection {
        var lite_backend = try backend.Handle.createWithOptions(allocator, path, .{
            .exclusive = exclusive,
            .no_sync = !opts.fsync,
        });
        errdefer lite_backend.deinit();

        return try openWithBackend(allocator, path, .writer, &lite_backend);
    }

    pub fn close(self: *Connection) void {
        if (openModeCanWrite(self.open_mode)) {
            self.db.sync(true) catch {};
            self.db.syncIndexes(true) catch {};
        }
        self.db.close();
        self.backend.deinit();
        self.* = undefined;
    }
};

fn openWithBackend(
    allocator: Allocator,
    path: []const u8,
    open_mode: db_mod.OpenOptions.OpenMode,
    lite_backend: *backend.Handle,
) !Connection {
    var opts = db_mod.OpenOptions{
        .open_mode = open_mode,
        .external_derived_checkpoints = false,
    };
    try lite_backend.configureDbOpenOptions(&opts);

    const db = try db_mod.DB.open(allocator, path, opts);

    const moved_backend = lite_backend.*;
    lite_backend.* = undefined;
    return .{
        .backend = moved_backend,
        .db = db,
        .open_mode = open_mode,
    };
}

pub fn openModeRequiresReadOnlyBackends(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return switch (open_mode) {
        .query_readonly, .status_only => true,
        else => false,
    };
}

pub fn openModeCanWrite(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return switch (open_mode) {
        .writer, .writer_no_replay => true,
        else => false,
    };
}

test "lite connection opens readonly backends for readonly db modes" {
    try std.testing.expect(!openModeRequiresReadOnlyBackends(.writer));
    try std.testing.expect(!openModeRequiresReadOnlyBackends(.writer_no_replay));
    try std.testing.expect(openModeRequiresReadOnlyBackends(.query_readonly));
    try std.testing.expect(openModeRequiresReadOnlyBackends(.status_only));
}

test "lite connection write modes sync on close" {
    try std.testing.expect(openModeCanWrite(.writer));
    try std.testing.expect(openModeCanWrite(.writer_no_replay));
    try std.testing.expect(!openModeCanWrite(.query_readonly));
    try std.testing.expect(!openModeCanWrite(.status_only));
}

test "lite connection propagates fsync policy to native file" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/fsync.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var created = try Connection.create(allocator, path, true);
    created.close();

    var connection = try Connection.openWithOptions(allocator, path, .writer, .{ .fsync = false });
    defer connection.close();
    try std.testing.expect(connection.backend.native_docstore.?.file.no_sync);
}

test "lite connection open or create initializes a missing file" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/new.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var created = try Connection.openOrCreateWithOptions(allocator, path, .{ .fsync = false });
    try std.testing.expect(created.backend.native_docstore.?.file.no_sync);
    created.close();

    var reopened = try Connection.openOrCreateWithOptions(allocator, path, .{ .fsync = true });
    defer reopened.close();
    try std.testing.expect(!reopened.backend.native_docstore.?.file.no_sync);
}
