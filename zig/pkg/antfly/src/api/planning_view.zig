// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Query-planning content identity; excludes schemas, leader hints and telemetry
//! that do not contribute to join routing, conservative statistics or identity.
const std = @import("std");
pub fn revision(alloc: std.mem.Allocator, snapshot: anytype) ![32]u8 {
    const Table = struct { table_id: u64, name: []const u8 };
    const tables = try alloc.alloc(Table, snapshot.tables.len);
    defer alloc.free(tables);
    for (snapshot.tables, tables) |table, *out| out.* = .{ .table_id = table.table_id, .name = table.name };
    const Status = struct { group_id: u64, doc_count: u64, disk_bytes: u64, disk_bytes_known: bool, identity_conflict: bool, identity_reassignment_active: bool, identity: @import("../metadata/table_manager.zig").RuntimeDocIdentityStatusReport };
    const statuses = try alloc.alloc(Status, snapshot.merged_group_statuses.len);
    defer alloc.free(statuses);
    for (snapshot.merged_group_statuses, statuses) |status, *out| out.* = .{ .group_id = status.group_id, .doc_count = status.doc_count, .disk_bytes = status.disk_bytes, .disk_bytes_known = status.disk_bytes_known, .identity_conflict = status.doc_identity_namespace_conflict, .identity_reassignment_active = status.doc_identity_reassignment_active, .identity = status.doc_identity };
    const bytes = try std.json.Stringify.valueAlloc(alloc, .{ .version = @as(u8, 1), .group_id = snapshot.status.metadata_group_id, .incarnation = snapshot.status.metadata_incarnation, .tables = tables, .ranges = snapshot.ranges, .statuses = statuses }, .{});
    defer alloc.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}
