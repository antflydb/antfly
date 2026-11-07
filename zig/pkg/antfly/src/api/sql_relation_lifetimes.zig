// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bind SQL syntax to the existing catalog and native owner. There is no
//! second temporary-table row store: names, typed rows, indexes and mutations
//! continue through their ordinary catalog/storage interfaces.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
pub const persistence = local.common_topology_records.RelationPersistence;
const connection = @import("sql_connection_record.zig");
pub const Owner = persistence.Owner;
pub const Descriptor = persistence.Descriptor;

/// This capsule owns connection/fence identity by value and borrows only the
/// process-long API server. Retained cursors never retain a request stack.
pub fn ownerMinter(server: anytype, context: local.api_operation.RequestContext) !?local.common_relation_owner_authority.Minter {
    const owner = context.sql_relation_owner orelse return null;
    try owner.validate();
    try @import("sql_preparation_authority_keys.zig").requireConfiguration(server.cfg);
    const Server = @TypeOf(server);
    const Bridge = struct {
        fn sign(raw: *anyopaque, a: std.mem.Allocator, captured: Owner, scope: local.common_relation_owner_authority.Scope) anyerror![]u8 {
            const api: Server = @ptrCast(@alignCast(raw));
            const key = try @import("sql_preparation_authority_keys.zig").configured(api.cfg);
            return local.common_relation_owner_authority.sign(a, key.secret, key.issuer, captured, scope, @intCast(@divFloor(api.internalAuthRealtimeNs(), std.time.ns_per_s)));
        }
    };
    const command_digest: ?[32]u8 = if (context.sql_command_fence) |fence| digest: {
        try fence.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        fence.hashInto(&hash);
        var bytes: [32]u8 = undefined;
        hash.final(&bytes);
        break :digest bytes;
    } else null;
    return .{ .ptr = server, .sign_fn = Bridge.sign, .owner = owner, .command_fence_digest = command_digest };
}

/// A connection may read ordinary tables without any TEMP authority. Bind a
/// retained signer only after the actual captured relation requires it.
pub fn ownerMinterForRelation(server: anytype, context: local.api_operation.RequestContext, descriptor_value: ?Descriptor) !?local.common_relation_owner_authority.Minter {
    const value = descriptor_value orelse return null;
    try value.validate();
    if (value.kind != .temporary) return null;
    try value.requireConnectionOwner(context.sql_relation_owner);
    return (try ownerMinter(server, context)) orelse error.SqlTemporaryRelationNotOwned;
}

/// Capture process-local authority only after the final participant set is
/// known. The returned outer slice belongs to the caller's scratch arena;
/// row buffers remain borrowed. Ordinary participants take no allocation.
pub fn bindParticipants(a: std.mem.Allocator, context: local.api_operation.RequestContext, definitions: []const local.common_topology_records.TableRecord, tables: []const local.api_distributed_txn_contract.TableCommitRequest) ![]const local.api_distributed_txn_contract.TableCommitRequest {
    var bound: ?[]local.api_distributed_txn_contract.TableCommitRequest = null;
    errdefer if (bound) |owned| a.free(owned);
    for (tables, 0..) |table, index| {
        const definition = for (definitions) |candidate| {
            if (std.mem.eql(u8, candidate.name, table.table_name)) break candidate;
        } else return error.TableNotFound;
        const actual = definition.sql_persistence;
        if (table.sql_persistence) |captured| {
            if (actual == null or !captured.eql(actual.?)) return error.SqlRelationOwnerChanged;
        }
        if (actual) |value| {
            try value.validate();
            try value.requireConnectionOwner(context.sql_relation_owner);
            if (table.sql_persistence == null) {
                if (bound == null) bound = try a.dupe(local.api_distributed_txn_contract.TableCommitRequest, tables);
                bound.?[index].sql_persistence = value;
            }
        }
    }
    if (bound) |owned| return owned;
    return tables;
}

pub fn attachOwnerMinters(server: anytype, a: std.mem.Allocator, context: local.api_operation.RequestContext, tables: []const local.api_distributed_txn_contract.TableCommitRequest) ![]const local.api_distributed_txn_contract.TableCommitRequest {
    var needed = false;
    for (tables) |table| if (table.sql_persistence) |value| {
        try value.validate();
        if (value.kind == .temporary) {
            try value.requireConnectionOwner(context.sql_relation_owner);
            needed = true;
        }
    };
    if (!needed) return tables;
    const minter = (try ownerMinter(server, context)) orelse return error.SqlTemporaryRelationNotOwned;
    const captured = try a.dupe(local.api_distributed_txn_contract.TableCommitRequest, tables);
    for (captured) |*table| if (table.sql_persistence) |value| {
        if (value.kind == .temporary) table.relation_owner_minter = minter;
    };
    return captured;
}

/// The exact transport body must be finalized before delegating user authority.
pub fn signOwnerGrant(server: anytype, a: std.mem.Allocator, context: local.api_operation.RequestContext, value: Descriptor, table_id: u64, group_id: u64, purpose: local.common_relation_owner_authority.Purpose, body: []const u8) !?[]u8 {
    try context.ensureActive();
    try value.validate();
    if (value.kind != .temporary) return null;
    const minter = (try ownerMinter(server, context)) orelse return error.SqlTemporaryRelationNotOwned;
    try value.requireConnectionOwner(minter.owner);
    return minter.mint(a, .{ .purpose = purpose, .table_id = table_id, .group_id = group_id, .descriptor_fingerprint = try value.fingerprint(), .body_digest = local.common_relation_owner_authority.bodyDigest(body) });
}

/// Call only after the ordinary durable connection load authenticated the
/// principal, node, expiry and command fence. An old connection generation
/// cannot lend its table namespace to DISCARD ALL or a replacement process.
pub fn ownerFromConnection(record: connection.Record, execution_instance: [16]u8, node_id: u64) !Owner {
    try record.validate();
    if (record.owner_node_id != node_id) return error.SqlTemporaryRelationNotOwned;
    const wire_epoch = record.wire_epoch orelse return error.InvalidSqlConnection;
    var principal_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(record.principal, &principal_digest, .{});
    const result: Owner = .{
        .issuer_node_id = record.owner_node_id,
        .connection_id = record.id,
        .wire_epoch = wire_epoch,
        .execution_instance = execution_instance,
        .connection_generation = record.generation,
        .principal_digest = principal_digest,
    };
    try result.validate();
    return result;
}

pub fn descriptor(kind: persistence.Kind, on_commit: persistence.OnCommit, node_id: u64, store_id: u64, generation_instance: [16]u8, data_generation: u64, owner: ?Owner) !?Descriptor {
    if (kind == .permanent) {
        if (owner != null or on_commit != .preserve_rows) return error.InvalidSqlRelationPersistence;
        return null;
    }
    const result: Descriptor = .{
        .kind = kind,
        .owner_node_id = node_id,
        .owner_store_id = store_id,
        .generation_instance = generation_instance,
        .data_generation = data_generation,
        .owner = owner,
        .on_commit = on_commit,
    };
    try result.validate();
    return result;
}

/// TEMP may name its own pg_temp schema but cannot create an object in an
/// ordinary schema, even if the current role owns that ordinary schema.
pub fn bindTemporaryTarget(a: std.mem.Allocator, target: domain.Target, qualified: bool, owner: Owner) !domain.Target {
    var buffer: [72]u8 = undefined;
    const temporary = try owner.namespace(&buffer);
    if (qualified and !std.mem.eql(u8, target.namespace, "pg_temp") and
        !std.mem.eql(u8, target.namespace, temporary)) return error.InvalidSqlTemporaryNamespace;
    var result = target;
    result.namespace = try a.dupe(u8, temporary);
    return result;
}

pub fn temporaryNamespace(a: std.mem.Allocator, owner: Owner) ![]u8 {
    var buffer: [72]u8 = undefined;
    return a.dupe(u8, try owner.namespace(&buffer));
}

/// PostgreSQL does not permit a temporary FK to cross a session or refer to
/// an ordinary relation. Permanent referents cannot depend on crash-reset
/// UNLOGGED parents either. UNLOGGED child->permanent parent references use
/// the ordinary FK machinery and are retired before crash-reset publication.
pub const requireForeignKeyClasses = persistence.requireForeignKeyClasses;

test "SQL relation lifetime binding isolates names and rejects qualified ordinary TEMP schemas" {
    const a = std.testing.allocator;
    const record: connection.Record = .{ .id = @splat(1), .wire_epoch = @splat(2), .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "default", .namespace = "public" };
    const owner = try ownerFromConnection(record, @splat(3), 7);
    const target: domain.Target = .{ .table = "rows" };
    const bound = try bindTemporaryTarget(a, target, false, owner);
    defer a.free(bound.namespace);
    try std.testing.expect(std.mem.startsWith(u8, bound.namespace, "pg_temp_"));
    try std.testing.expect(bound.namespace.len <= 63);
    try std.testing.expectEqualStrings(target.table, bound.table);
    try std.testing.expectError(error.InvalidSqlTemporaryNamespace, bindTemporaryTarget(a, target, true, owner));
    const alias = try bindTemporaryTarget(a, .{ .namespace = "pg_temp", .table = "rows" }, true, owner);
    defer a.free(alias.namespace);
    try std.testing.expectEqualStrings(bound.namespace, alias.namespace);
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, ownerFromConnection(record, @splat(3), 9));
}

test "SQL relation lifetime FK classes retain the same session and reset-safe dependency direction" {
    const owner: Owner = .{ .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    const temporary = (try descriptor(.temporary, .preserve_rows, 7, 11, owner.execution_instance, 1, owner)).?;
    const unlogged = (try descriptor(.unlogged, .preserve_rows, 7, 11, @splat(3), 1, null)).?;
    try requireForeignKeyClasses(temporary, temporary);
    try requireForeignKeyClasses(unlogged, null);
    try requireForeignKeyClasses(unlogged, unlogged);
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(null, unlogged));
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(temporary, null));
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(null, temporary));
    var other = temporary;
    other.owner.?.connection_generation += 1;
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(temporary, other));
}

test "SQL relation lifetime public participants bind native catalog descriptors before owner signing" {
    const a = std.testing.allocator;
    const Participant = local.api_distributed_txn_contract.TableCommitRequest;
    const native: Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 9, .generation_instance = @splat(1), .data_generation = 3 };
    const definitions = [_]local.common_topology_records.TableRecord{ .{ .table_id = 11, .name = "native", .sql_persistence = native }, .{ .table_id = 13, .name = "ordinary" } };
    const incoming = [_]Participant{ .{ .table_name = "ordinary" }, .{ .table_name = "native" } };
    const bound = try bindParticipants(a, .{}, &definitions, &incoming);
    defer a.free(bound);
    try std.testing.expect(bound[0].sql_persistence == null);
    try std.testing.expect(bound[1].sql_persistence.?.eql(native));
    try std.testing.expect(incoming[1].sql_persistence == null);
    var replaced = native;
    replaced.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, bindParticipants(a, .{}, &definitions, &.{.{ .table_name = "native", .sql_persistence = replaced }}));
    const ordinary = [_]Participant{.{ .table_name = "ordinary" }};
    try std.testing.expectError(error.TableNotFound, bindParticipants(a, .{}, &definitions, &.{.{ .table_name = "missing" }}));
    const unchanged = try bindParticipants(a, .{}, &definitions, &ordinary);
    try std.testing.expectEqual(ordinary[0..].ptr, unchanged.ptr);
    const owner: Owner = .{ .connection_id = @splat(2), .wire_epoch = @splat(3), .execution_instance = @splat(4), .connection_generation = 1, .principal_digest = @splat(5) };
    var temporary = native;
    temporary.kind = .temporary;
    temporary.owner = owner;
    const temporary_definitions = [_]local.common_topology_records.TableRecord{.{ .table_id = 17, .name = "temporary", .sql_persistence = temporary }};
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, bindParticipants(a, .{}, &temporary_definitions, &.{.{ .table_name = "temporary" }}));
    const owned = try bindParticipants(a, .{ .sql_relation_owner = owner }, &temporary_definitions, &.{.{ .table_name = "temporary" }});
    defer a.free(owned);
    try std.testing.expect(owned[0].sql_persistence.?.eql(temporary));
}
