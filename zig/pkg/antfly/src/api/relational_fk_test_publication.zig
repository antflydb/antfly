// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Test fixture for an already-accepted child FK generation decision. The
//! production child-owner apply still verifies the source fence, exact schema
//! and catalog digests, and Raft marker before installing the new schema.
const std = @import("std");
const db_mod = @import("../storage/db/db.zig");
const topology = @import("../storage/db/relational_integrity_topology.zig");
const public_schema = @import("../schema/mod.zig");
const runtime_schema = @import("../storage/schema.zig");

pub fn install(alloc: std.mem.Allocator, db: *db_mod.DB, before_json: []const u8, after_json: []const u8, raft_index: u64) !void {
    const before_catalog = (try db.core.getStoreValue(alloc, @import("../storage/db/relational_integrity_catalog.zig").key)) orelse return error.IntegrityCatalogChanged;
    defer alloc.free(before_catalog);
    var before_catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(before_catalog, &before_catalog_digest, .{});
    var before_schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(before_json, &before_schema_digest, .{});
    var after_schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(after_json, &after_schema_digest, .{});

    var parsed = try public_schema.parseValidatedTableSchema(alloc, after_json);
    defer parsed.deinit(alloc);
    const next = try public_schema.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, next);
    var prepared = try db.core.prepareSchemaMetadataPublishedChild(next, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = after_json }});
    defer prepared.deinit();
    var after_catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prepared.integrity_catalog.?.value, &after_catalog_digest, .{});

    const namespace = db.core.identity_namespace;
    const fence: topology.Fence = .{
        .role = .child_generation_source,
        .transition_id = raft_index,
        .attempt = 1,
        .admission_epoch = raft_index,
        .peer_group_id = namespace.shard_id,
        .owner_group_id = namespace.shard_id,
        .namespace = namespace,
        .catalog_digest = before_catalog_digest,
    };
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = fence }, null);
    const command: topology.Command = .{ .action = .install_child_schema, .fence = fence, .child_schema_install = .{
        .schema_json = after_json,
        .before_schema_json_digest = before_schema_digest,
        .schema_json_digest = after_schema_digest,
        .before_catalog_digest = before_catalog_digest,
        .after_catalog_digest = after_catalog_digest,
    } };
    try db.batchRaftReplicatedApply(.{ .relational_topology = command }, .{ .term = 1, .index = raft_index });
}
