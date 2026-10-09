// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
pub const ttl_ms: u64 = 300_000;
pub const max_ttl_ms: u64 = std.time.ms_per_hour;
pub const Request = struct {
    id: []const u8,
    table_id: u64,
    expires_ms: u64,
    create: bool = false,
    /// Authenticated original generation identity. Execution may move to a
    /// replacement range, but physical document IDs remain in this namespace.
    origin: ?Namespace = null,
    timeout_ms: ?u64 = null,
    pub fn namespace(self: Request, owner: Namespace) !Namespace {
        if (owner.table_id != self.table_id) return error.CatalogGenerationChanged;
        const original = self.origin orelse owner;
        if (original.table_id != self.table_id or (self.create and !original.eql(owner))) return error.CatalogGenerationChanged;
        return original;
    }
    /// Each original generation gets its own virtual/local cache root when
    /// multiple pre-split ranges execute on the same replacement owner.
    pub fn cacheId(self: Request, a: std.mem.Allocator, owner: Namespace) ![]u8 {
        const original = try self.namespace(owner);
        if (original.eql(owner)) return a.dupe(u8, self.id);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-query-origin-cache-v1");
        hash.update(self.id);
        var number: [8]u8 = undefined;
        for ([_]u64{ original.table_id, original.shard_id, original.range_id }) |value| {
            std.mem.writeInt(u64, &number, value, .big);
            hash.update(&number);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    }
    pub fn forDeadline(self: Request, deadline_ns: ?u64) Request {
        var request = self;
        if (deadline_ns) |deadline| request.timeout_ms = (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms;
        return request;
    }
    pub fn validate(self: Request, now: u64) !void {
        if (self.id.len != 64 or self.table_id == 0) return error.InvalidQueryRequest;
        if (self.origin) |origin| if (origin.table_id != self.table_id) return error.CatalogGenerationChanged;
        for (self.id) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidQueryRequest;
        if (self.expires_ms <= now or self.expires_ms > now +| max_ttl_ms) return error.CatalogGenerationChanged;
    }
};

test "native query origins retain identities across repartition and isolate cache roots" {
    const a = std.testing.allocator;
    const original: Namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 };
    const replacement: Namespace = .{ .table_id = 7, .shard_id = 20, .range_id = 21 };
    const id: [64]u8 = @splat('a');
    var request: Request = .{ .id = &id, .table_id = 7, .expires_ms = 1000, .origin = original };
    try request.validate(1);
    try std.testing.expect((try request.namespace(replacement)).eql(original));
    const moved = try request.cacheId(a, replacement);
    defer a.free(moved);
    const local = try request.cacheId(a, original);
    defer a.free(local);
    try std.testing.expectEqualStrings(&id, local);
    try std.testing.expect(!std.mem.eql(u8, moved, local));
    request.origin = .{ .table_id = 7, .shard_id = 30, .range_id = 31 };
    const other = try request.cacheId(a, replacement);
    defer a.free(other);
    try std.testing.expect(!std.mem.eql(u8, moved, other));
    request.create = true;
    try std.testing.expectError(error.CatalogGenerationChanged, request.namespace(replacement));
    request.create = false;
    request.origin = .{ .table_id = 8 };
    try std.testing.expectError(error.CatalogGenerationChanged, request.validate(1));
    try std.testing.expectError(error.CatalogGenerationChanged, request.namespace(replacement));
}
