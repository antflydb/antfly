// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared publisher facts and native Source ownership. A subscription owns one
//! database-level slot and transaction stream; no table is its synthetic anchor.
const std = @import("std");
const domain = @import("domain.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const max_relations = 4096;
pub const max_columns = 1600;
pub const max_encoded_bytes = 4 * 1024 * 1024;
pub const Subscription = struct {
    database_id: u64,
    subscription_id: u64,
    pub fn validate(self: @This()) !void {
        if (self.database_id == 0 or self.subscription_id == 0 or self.subscription_id > domain.max_relation_oid) return error.InvalidLogicalReplicationOrigin;
    }
};
pub const Owner = union(enum) {
    table_source: struct { table_id: u64, source_ordinal: u32 },
    logical_subscription: Subscription,
    pub fn validate(self: Owner) !void {
        switch (self) {
            .table_source => |table| if (table.table_id == 0) return error.InvalidLogicalReplicationOrigin,
            .logical_subscription => |subscription| try subscription.validate(),
        }
    }
    /// Canonical owner address shared by Source slot, birth and influence
    /// directories. A subscription never borrows a synthetic table identity.
    pub fn keyPart(self: Owner, buffer: []u8) ![]const u8 {
        try self.validate();
        return switch (self) {
            .table_source => |value| std.fmt.bufPrint(buffer, "table:{d}:{d}", .{ value.table_id, value.source_ordinal }),
            .logical_subscription => |value| std.fmt.bufPrint(buffer, "subscription:{d}:{d}", .{ value.database_id, value.subscription_id }),
        };
    }
    pub fn eql(self: Owner, other: Owner) bool {
        if (std.meta.activeTag(self) != std.meta.activeTag(other)) return false;
        return switch (self) {
            .table_source => |table| table.table_id == other.table_source.table_id and table.source_ordinal == other.table_source.source_ordinal,
            .logical_subscription => |subscription| subscription.database_id == other.logical_subscription.database_id and subscription.subscription_id == other.logical_subscription.subscription_id,
        };
    }
};
pub const ReplicaIdentity = enum { default, nothing, full, index };
pub const Column = struct {
    name: []const u8,
    type_oid: u32,
    type_modifier: i32,
    key: bool,
};
pub const Relation = struct {
    id: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: ReplicaIdentity,
    columns: []const Column,
    /// Actual effective publisher predicate; null means an unrestricted member.
    row_filter: ?[]const u8 = null,
    pub fn validate(self: Relation, a: A) !void {
        if (self.id == 0 or self.columns.len == 0 or self.columns.len > max_columns) return error.InvalidLogicalReplicationOrigin;
        try domain.validateName(self.namespace);
        try domain.validateName(self.name);
        if (self.row_filter) |predicate| if (predicate.len == 0 or predicate.len > max_encoded_bytes or
            std.mem.indexOfScalar(u8, predicate, 0) != null or !std.unicode.utf8ValidateSlice(predicate)) return error.InvalidLogicalReplicationOrigin;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (self.columns) |column| {
            try domain.validateName(column.name);
            if (column.type_oid == 0 or (try names.getOrPut(a, column.name)).found_existing) return error.InvalidLogicalReplicationOrigin;
        }
    }
    pub fn digest(self: Relation, a: A) !Digest {
        try self.validate(a);
        return digestValue(a, "antfly:logical-replication-relation:v1\x00", self);
    }
};
/// Actual provider cluster/database identity and OID/schema roster from ONE
/// retained provider connection. Empty publication membership is valid.
pub const Observation = struct {
    provider_identity: Digest,
    relations: []const Relation,
    pub fn validate(self: Observation, a: A) !void {
        if (std.mem.allEqual(u8, &self.provider_identity, 0) or self.relations.len > max_relations) return error.InvalidLogicalReplicationOrigin;
        var previous: u32 = 0;
        for (self.relations) |relation| {
            try relation.validate(a);
            if (relation.id <= previous) return error.InvalidLogicalReplicationOrigin;
            previous = relation.id;
        }
    }
    /// Own every roster field independently of a provider result or request arena.
    pub fn clone(self: Observation, a: A) !Observation {
        try self.validate(a);
        const relations = try a.alloc(Relation, self.relations.len);
        var count: usize = 0;
        errdefer {
            for (relations[0..count]) |relation| freeRelation(a, relation);
            a.free(relations);
        }
        for (self.relations, relations) |relation, *out| {
            const namespace = try a.dupe(u8, relation.namespace);
            errdefer a.free(namespace);
            const name = try a.dupe(u8, relation.name);
            errdefer a.free(name);
            const columns = try a.alloc(Column, relation.columns.len);
            var column_count: usize = 0;
            errdefer {
                for (columns[0..column_count]) |column| a.free(column.name);
                a.free(columns);
            }
            for (relation.columns, columns) |column, *owned| {
                owned.* = column;
                owned.name = try a.dupe(u8, column.name);
                column_count += 1;
            }
            out.* = relation;
            out.namespace = namespace;
            out.name = name;
            out.columns = columns;
            out.row_filter = if (relation.row_filter) |predicate| try a.dupe(u8, predicate) else null;
            count += 1;
        }
        return .{ .provider_identity = self.provider_identity, .relations = relations };
    }
    pub fn deinit(self: *Observation, a: A) void {
        for (self.relations) |relation| freeRelation(a, relation);
        a.free(self.relations);
        self.* = undefined;
    }
    pub fn digest(self: Observation, a: A) !Digest {
        try self.validate(a);
        return digestValue(a, "antfly:logical-replication-roster:v1\x00", self);
    }
    pub fn find(self: Observation, id: u32) ?Relation {
        var left: usize = 0;
        var right = self.relations.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (self.relations[mid].id < id) left = mid + 1 else right = mid;
        }
        return if (left < self.relations.len and self.relations[left].id == id) self.relations[left] else null;
    }
};
/// Concrete native birth; proposal-only owner/definition data cannot stand in
/// for this identity. Event generation and upload token remain separate counters.
pub const Identity = struct {
    owner: Owner,
    origin_generation: u64,
    provider_identity: Digest,
    recipe_digest: Digest,
    slot_name: []const u8,
    roster_digest: Digest,
    pub fn validate(self: Identity) !void {
        try self.owner.validate();
        if (self.origin_generation == 0 or self.slot_name.len == 0 or self.slot_name.len > 63 or
            std.mem.allEqual(u8, &self.provider_identity, 0) or std.mem.allEqual(u8, &self.recipe_digest, 0) or std.mem.allEqual(u8, &self.roster_digest, 0)) return error.InvalidLogicalReplicationOrigin;
        for (self.slot_name) |byte| if (!(std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '_')) return error.InvalidLogicalReplicationOrigin;
    }
};
fn freeRelation(a: A, relation: Relation) void {
    a.free(relation.namespace);
    a.free(relation.name);
    if (relation.row_filter) |predicate| a.free(predicate);
    for (relation.columns) |column| a.free(column.name);
    a.free(relation.columns);
}

fn digestValue(a: A, tag: []const u8, value: anytype) !Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > max_encoded_bytes) return error.ProgramLimitExceeded;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(tag);
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

test "logical subscription origin owns a real database and full ordered relation roster" {
    const a = std.testing.allocator;
    const columns = [_]Column{.{ .name = "id", .type_oid = 20, .type_modifier = -1, .key = true }};
    const observation: Observation = .{ .provider_identity = @splat(1), .relations = &.{
        .{ .id = 17, .namespace = "public", .name = "parents", .replica_identity = .default, .columns = &columns },
        .{ .id = 91, .namespace = "public", .name = "children", .replica_identity = .default, .columns = &columns },
    } };
    try observation.validate(a);
    try std.testing.expect(observation.find(17) != null);
    try std.testing.expect(observation.find(90) == null);
    try std.testing.expectEqualStrings("children", observation.find(91).?.name);
    const identity: Identity = .{ .owner = .{ .logical_subscription = .{ .database_id = 3, .subscription_id = 9 } }, .origin_generation = 2, .provider_identity = observation.provider_identity, .recipe_digest = @splat(2), .slot_name = "usage_slot", .roster_digest = try observation.digest(a) };
    try identity.validate();
    var unbound = identity;
    unbound.origin_generation = 0;
    try std.testing.expectError(error.InvalidLogicalReplicationOrigin, unbound.validate());
    try std.testing.expectError(error.InvalidLogicalReplicationOrigin, (Owner{ .logical_subscription = .{ .database_id = 3, .subscription_id = 0 } }).validate());
    var reordered = observation;
    reordered.relations = &.{ observation.relations[1], observation.relations[0] };
    try std.testing.expectError(error.InvalidLogicalReplicationOrigin, reordered.validate(a));
    var changed = observation;
    changed.provider_identity = @splat(3);
    try std.testing.expect(!std.mem.eql(u8, &try changed.digest(a), &identity.roster_digest));
}
