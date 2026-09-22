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
const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
const group_ids = @import("../../common/group_ids.zig");
const full_text_index_defaults = @import("../../common/full_text_index_defaults.zig");
const db_types = @import("../db/types.zig");

const Allocator = std.mem.Allocator;

pub const Connection = struct {
    backend: backend.Handle,
    db: db_mod.DB,
    open_mode: db_mod.OpenOptions.OpenMode,

    pub const Options = struct {
        fsync: bool = true,
        writer_lock_marker: []const u8 = "",
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

        return try openWithBackend(allocator, path, open_mode, &lite_backend, false);
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
            .writer_lock_marker = opts.writer_lock_marker,
        });
        errdefer lite_backend.deinit();

        var connection = try openWithBackend(allocator, path, .writer, &lite_backend, true);
        errdefer connection.close();
        try connection.backend.markEmbeddedArtifact();
        return connection;
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

/// Embedded Lite's root is the future standalone `default` table. Assigning
/// that deterministic identity at file creation avoids an O(live documents)
/// namespace rewrite when the artifact is first served through `/db/v1`.
///
/// This is the single source of truth for the identity every Antfly Lite
/// `.aflite` file is created with, regardless of which surface creates it
/// (the CLI through `Connection`, the C ABI, or the native `embedded`
/// package). All three call `identityOpenOptions` and
/// `provisionDefaultFullTextIndex` below so a file produced by one surface
/// stays fully openable by the others.
pub fn embeddedRootIdentity() db_mod.DocIdentityNamespace {
    const table_name = "default";
    const table_id = std.hash.Wyhash.hash(0x54424c45, table_name);
    const group_id = group_ids.dataGroupIdFromHash(std.hash.Wyhash.hash(0x47525031, table_name));
    return .{
        .table_id = if (table_id == 0) 1 else table_id,
        .shard_id = group_id,
        .range_id = group_id,
    };
}

pub const IdentityOpenOptions = struct {
    identity_namespace: ?db_mod.DocIdentityNamespace,
    prefer_existing_identity_namespace: bool,
};

/// The identity-namespace fields every Lite `OpenOptions` should carry.
/// `create` requests are pinned to `embeddedRootIdentity()` (nothing is
/// stored yet, so there is nothing to defer to); opens of an existing file
/// prefer whatever identity is already durable there -- including a legacy
/// file whose first write persisted the zero-value default namespace before
/// every surface agreed on `embeddedRootIdentity()` -- so a valid `.aflite`
/// file is never rejected outright for its identity.
pub fn identityOpenOptions(create: bool) IdentityOpenOptions {
    return .{
        .identity_namespace = embeddedRootIdentity(),
        .prefer_existing_identity_namespace = !create,
    };
}

/// Provisions the same default full-text index every Antfly table is
/// provisioned with on creation (see `full_text_index_defaults.zig`), so a
/// freshly created Lite database supports text search without a separate
/// `lite index create` step. Must only be called once, immediately after a
/// brand-new database is opened for the first time.
pub fn provisionDefaultFullTextIndex(db: *db_mod.DB) !void {
    try db.addIndex(.{
        .name = full_text_index_defaults.default_full_text_index_name,
        .kind = .full_text,
        .config_json = "{}",
    });
}

fn openWithBackend(
    allocator: Allocator,
    path: []const u8,
    open_mode: db_mod.OpenOptions.OpenMode,
    lite_backend: *backend.Handle,
    create: bool,
) !Connection {
    const identity = identityOpenOptions(create);
    var opts = db_mod.OpenOptions{
        .open_mode = open_mode,
        .external_derived_checkpoints = false,
        .identity_namespace = identity.identity_namespace,
        .prefer_existing_identity_namespace = identity.prefer_existing_identity_namespace,
    };
    try lite_backend.configureDbOpenOptions(&opts);

    var db = try db_mod.DB.open(allocator, path, opts);
    errdefer db.close();

    if (create) {
        try provisionDefaultFullTextIndex(&db);
    }

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

test "lite connection create provisions the default full text index" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/default-index.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var created = try Connection.create(allocator, path, true);
    defer created.close();

    const indexes = try created.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings(full_text_index_defaults.default_full_text_index_name, indexes[0].name);
}

test "lite connection adopts a legacy file whose first write persisted the default namespace" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/legacy-null-identity.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    // Reproduces a Lite file created by a surface that (before this fix)
    // never configured an identity namespace: the first write silently
    // persists the zero-value default namespace as the durable identity.
    {
        var lite_backend = try backend.Handle.createWithOptions(allocator, path, .{ .exclusive = true });
        defer lite_backend.deinit();
        var opts = db_mod.OpenOptions{
            .open_mode = .writer,
            .external_derived_checkpoints = false,
            .identity_namespace = null,
        };
        try lite_backend.configureDbOpenOptions(&opts);
        var db = try db_mod.DB.open(allocator, path, opts);
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:legacy", .value = "{}" }},
            .sync_level = .write,
        });
    }

    // The CLI path must still open it -- and every other surface's writes
    // are visible -- instead of rejecting it with IdentityNamespaceMismatch.
    var reopened = try Connection.open(allocator, path, .query_readonly);
    defer reopened.close();
    var result = (try reopened.db.lookup(allocator, "doc:legacy", .{})) orelse return error.MissingDocument;
    defer result.deinit(allocator);
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
