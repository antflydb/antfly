// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable HTTP connection identity. This is deliberately separate from a
//! transaction ID: DISCARD ALL owns a connection's idle resources, while an
//! in-flight or uncertain transaction keeps the connection fenced.
const std = @import("std");
const settings = @import("antfly_local_sources").sql_setting_catalog;

pub const prefix = "\x00sql-connection-v1/";
pub const max_record_bytes = 128 << 10;
pub const max_overlay_entries = 128;
pub const ttl_ms: u64 = 60 * 60 * 1000;

pub const State = enum { idle, beginning, active, uncertain };

pub const Record = struct {
    id: [32]u8,
    principal: []const u8,
    owner_node_id: u64,
    expires_at_ms: u64,
    generation: u64 = 1,
    revision: u64 = 1,
    database: []const u8,
    namespace: []const u8,
    state: State = .idle,
    active_txn: ?[16]u8 = null,
    overlay: []const settings.OverlayEntry = &.{},

    pub fn validate(self: Record) !void {
        // Node zero is the standalone-local owner sentinel when no session
        // router is configured. It remains distinct from every routed node.
        if (self.generation == 0 or self.revision == 0 or self.expires_at_ms == 0 or
            self.principal.len > 4096 or self.database.len == 0 or self.database.len > 256 or
            self.namespace.len == 0 or self.namespace.len > 256 or self.overlay.len > max_overlay_entries)
            return error.InvalidSqlConnection;
        if ((self.state != .idle) != (self.active_txn != null))
            return error.InvalidSqlConnection;
    }

    pub fn requireOwner(self: Record, id: []const u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !void {
        try self.validate();
        // TTL retires only idle resources. A live or uncertain transaction
        // must remain reachable long enough to reach a proven disposition.
        if (!std.mem.eql(u8, &self.id, id) or (self.state == .idle and self.expires_at_ms <= now_ms) or
            !std.mem.eql(u8, self.principal, principal)) return error.SqlConnectionNotFound;
        if (self.owner_node_id != owner_node_id) return error.SqlConnectionWrongOwner;
    }
};

pub fn key(id: []const u8) ![prefix.len + 32]u8 {
    if (id.len != 32) return error.SqlConnectionNotFound;
    var result: [prefix.len + 32]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], id);
    return result;
}

pub fn loadTxn(txn: anytype, alloc: std.mem.Allocator, id: []const u8) !std.json.Parsed(Record) {
    const record_key = try key(id);
    const raw = txn.get(&record_key) catch |err| switch (err) {
        error.NotFound => return error.SqlConnectionNotFound,
        else => return err,
    };
    if (raw.len > max_record_bytes) return error.InvalidSqlConnection;
    var parsed = std.json.parseFromSlice(Record, alloc, raw, .{ .allocate = .alloc_always }) catch return error.InvalidSqlConnection;
    errdefer parsed.deinit();
    try parsed.value.validate();
    if (!std.mem.eql(u8, &parsed.value.id, id)) return error.InvalidSqlConnection;
    return parsed;
}

pub fn putTxn(txn: anytype, alloc: std.mem.Allocator, value: Record) !void {
    try value.validate();
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    if (encoded.len > max_record_bytes) return error.SqlProgramLimitExceeded;
    const record_key = try key(&value.id);
    try txn.put(&record_key, encoded);
}

test "HTTP connection record binds principal, owner and transaction fence" {
    const alloc = std.testing.allocator;
    const id: [32]u8 = @splat('a');
    const base: Record = .{ .id = id, .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "main", .namespace = "public" };
    try base.validate();
    try base.requireOwner(&id, "alice", 7, 999);
    var standalone = base;
    standalone.owner_node_id = 0;
    try standalone.requireOwner(&id, "alice", 0, 999);
    try std.testing.expectError(error.SqlConnectionWrongOwner, standalone.requireOwner(&id, "alice", 7, 999));
    try std.testing.expectError(error.SqlConnectionNotFound, base.requireOwner(&id, "bob", 7, 999));
    try std.testing.expectError(error.SqlConnectionWrongOwner, base.requireOwner(&id, "alice", 8, 999));
    try std.testing.expectError(error.SqlConnectionNotFound, base.requireOwner(&id, "alice", 7, 1000));
    var uncertain = base;
    uncertain.state = .uncertain;
    try std.testing.expectError(error.InvalidSqlConnection, uncertain.validate());
    uncertain.active_txn = @splat(1);
    try uncertain.validate();
    try uncertain.requireOwner(&id, "alice", 7, 1000);
    const encoded = try std.json.Stringify.valueAlloc(alloc, uncertain, .{});
    defer alloc.free(encoded);
    var decoded = try std.json.parseFromSlice(Record, alloc, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(State.uncertain, decoded.value.state);
}
