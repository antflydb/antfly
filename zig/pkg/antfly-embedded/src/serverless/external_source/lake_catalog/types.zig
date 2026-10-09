// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The catalog owns a lake commit; index publication remains a separate step.
const std = @import("std");
pub const Context = @import("../../query/lake_read_context.zig").Context;
/// Providers borrow this callback only for the synchronous object operation.
/// It propagates deadlines as well as cancellation to active network I/O.
pub fn contextCancellation(context: *const Context) @import("objectstore").CancellationToken {
    return .{ .ptr = context, .is_cancelled_fn = struct {
        fn check(raw: *const anyopaque) bool {
            const current: *const Context = @ptrCast(@alignCast(raw));
            current.ensureActive() catch return true;
            return false;
        }
    }.check };
}

pub const max_metadata_bytes = 16 * 1024 * 1024;
pub const max_commit_bytes = 4 * 1024 * 1024;

pub const Config = struct {
    type: enum { managed, rest },
    /// Required for REST. A node-configured external_io/http connection with
    /// lake_catalog_read / lake_catalog_write capabilities; never raw secrets.
    connection: ?[]const u8 = null,
    uri: ?[]const u8 = null,
    namespace: []const []const u8 = &.{},
    name: ?[]const u8 = null,
    warehouse: ?[]const u8 = null,

    pub fn validate(self: Config) !void {
        if (self.type == .managed) {
            if (self.connection != null or self.uri != null or self.namespace.len != 0 or self.name != null or self.warehouse != null) return error.InvalidLakeCatalog;
            return;
        }
        if (self.connection == null or self.connection.?.len == 0 or self.uri == null or self.name == null or self.namespace.len == 0) return error.InvalidLakeCatalog;
        const uri = try std.Uri.parse(self.uri.?);
        if ((!std.mem.eql(u8, uri.scheme, "https") and !std.mem.eql(u8, uri.scheme, "http")) or uri.host == null or uri.user != null or uri.password != null or uri.query != null or uri.fragment != null) return error.InvalidLakeCatalog;
        if (self.name.?.len == 0 or self.namespace.len > 32) return error.InvalidLakeCatalog;
        for (self.namespace) |part| if (part.len == 0 or std.mem.indexOfAny(u8, part, "\x00\x1f") != null) return error.InvalidLakeCatalog;
        if (std.mem.indexOfAny(u8, self.name.?, "\x00\x1f") != null) return error.InvalidLakeCatalog;
    }
};

pub const Table = struct {
    metadata_location: []u8,
    metadata_json: []u8,
    /// Opaque compare-and-swap evidence. Callers must not synthesize it.
    version: ?[]u8 = null,
    record_key: ?[]u8 = null,
    pub fn deinit(self: *Table, a: std.mem.Allocator) void {
        a.free(self.metadata_location);
        a.free(self.metadata_json);
        if (self.version) |v| a.free(v);
        if (self.record_key) |v| a.free(v);
        self.* = undefined;
    }
};

pub const Commit = struct {
    /// Stable across retries/restarts. Reusing it with another payload fails.
    id: []const u8,
    expected_metadata_location: []const u8,
    /// Standard Iceberg REST {requirements: [...], updates: [...]} envelope.
    body: []const u8,
    timestamp_ms: i64,
    pub fn validate(self: Commit) !void {
        if (self.id.len == 0 or self.id.len > 256 or self.expected_metadata_location.len == 0 or self.body.len == 0 or self.body.len > max_commit_bytes or self.timestamp_ms < 0) return error.InvalidLakeCommit;
    }
};
pub const Outcome = enum { committed, not_committed, unknown };

pub fn digestHex(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}
pub fn commitHash(c: Commit) [64]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ c.id, c.expected_metadata_location, c.body }) |v| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, v.len, .little);
        hash.update(&length);
        hash.update(v);
    }
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}
