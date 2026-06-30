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
const db_mod = @import("../db/mod.zig");

const Allocator = std.mem.Allocator;

pub const Connection = struct {
    backend: backend.Handle,
    db: db_mod.DB,
    open_mode: db_mod.OpenOptions.OpenMode,

    pub fn open(allocator: Allocator, path: []const u8, open_mode: db_mod.OpenOptions.OpenMode) !Connection {
        var lite_backend = try backend.Handle.open(allocator, path, .{
            .read_only = openModeRequiresReadOnlyBackends(open_mode),
        });
        errdefer lite_backend.deinit();

        return try openWithBackend(allocator, path, open_mode, &lite_backend);
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Connection {
        var lite_backend = try backend.Handle.create(allocator, path, exclusive);
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
