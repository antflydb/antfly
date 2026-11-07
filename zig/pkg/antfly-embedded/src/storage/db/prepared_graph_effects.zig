// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable graph deltas carried by the existing primary-owner row intent.
//! No callback, executable SQL, or independent transaction decision is retained.
const std = @import("std");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const max_encoded_bytes = 16 * 1024 * 1024;
pub const max_operations = 4096;
pub const Index = struct { name: []const u8, generation: u64, definition_digest: [32]u8 };
pub const Effects = struct {
    version: u8 = 1,
    admission: ?Admission = null,
    owner: []const u8,
    indexes: []const Index,
    writes: []const types.GraphEdgeWrite = &.{},
    deletes: []const types.GraphEdgeDelete = &.{},

    pub fn validate(self: Effects, owner_key: []const u8) !void {
        if (self.version != 1 or owner_key.len == 0 or owner_key.len > 4096 or !std.mem.eql(u8, self.owner, owner_key) or
            self.indexes.len == 0 or self.indexes.len > 64 or self.writes.len + self.deletes.len == 0 or self.writes.len + self.deletes.len > max_operations) return error.InvalidPreparedGraphEffects;
        for (self.indexes, 0..) |index, i| {
            if (index.name.len == 0 or index.name.len > 256 or index.generation == 0 or std.mem.allEqual(u8, &index.definition_digest, 0)) return error.InvalidPreparedGraphEffects;
            for (self.indexes[0..i]) |previous| if (std.mem.eql(u8, previous.name, index.name)) return error.InvalidPreparedGraphEffects;
        }
        if (self.admission) |admitted| {
            try admitted.validate();
            if (admitted.indexes.len != self.indexes.len) return error.InvalidPreparedGraphEffects;
            for (self.indexes) |index| try admitted.requireIndex(index.name, index.definition_digest);
        }
        for (self.writes) |write| try self.requireOperation(owner_key, write);
        for (self.deletes) |delete| try self.requireOperation(owner_key, delete);
    }
    fn requireOperation(self: Effects, owner_key: []const u8, operation: anytype) !void {
        // Raw document transforms belong to the primary document that supplied
        // their captured preimage; no attachment can smuggle another owner.
        const owner = if (operation.owner_document.len != 0) operation.owner_document else if (operation.owner.len != 0) operation.owner else operation.source;
        if (!std.mem.eql(u8, owner, owner_key) or operation.source.len == 0 or operation.target.len == 0 or operation.edge_type.len == 0) return error.InvalidPreparedGraphEffects;
        for (self.indexes) |index| if (std.mem.eql(u8, index.name, operation.index_name)) return;
        return error.InvalidPreparedGraphEffects;
    }
    pub fn encode(self: Effects, a: A, owner_key: []const u8) ![]u8 {
        try self.validate(owner_key);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.TransactionTooLarge;
        return bytes;
    }
};
pub fn decode(a: A, bytes: []const u8, owner_key: []const u8) !std.json.Parsed(Effects) {
    if (bytes.len == 0 or bytes.len > max_encoded_bytes) return error.InvalidPreparedGraphEffects;
    var parsed = try std.json.parseFromSlice(Effects, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate(owner_key);
    return parsed;
}

test "prepared graph effects retain native deltas and immutable index binding across cold codec" {
    const a = std.testing.allocator;
    const effects: Effects = .{ .owner = "doc:a", .indexes = &.{.{ .name = "g", .generation = 9, .definition_digest = @splat(2) }}, .writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "cites", .weight = 2.5 }} };
    const bytes = try effects.encode(a, "doc:a");
    defer a.free(bytes);
    var parsed = try decode(a, bytes, "doc:a");
    defer parsed.deinit();
    try std.testing.expectEqualDeep(effects, parsed.value);
    try std.testing.expectError(error.InvalidPreparedGraphEffects, decode(a, bytes, "doc:other"));
    var wrong = effects;
    wrong.indexes = &.{.{ .name = "other", .generation = 9, .definition_digest = @splat(2) }};
    try std.testing.expectError(error.InvalidPreparedGraphEffects, wrong.encode(a, "doc:a"));
    wrong = effects;
    wrong.writes = &.{.{ .index_name = "g", .source = "doc:other", .target = "doc:b", .edge_type = "cites" }};
    try std.testing.expectError(error.InvalidPreparedGraphEffects, wrong.encode(a, "doc:a"));
}

/// Called under the existing native apply/index fence. A retained intent never
/// follows a same-name graph index to another generation or definition.
pub fn requireIndexes(a: A, manager: anytype, indexes: []const Index) !void {
    for (indexes) |expected| {
        const actual = manager.graphIndex(expected.name) orelse return error.PreparedGenerationChanged;
        if (actual.config.coverage_generation != expected.generation) return error.PreparedGenerationChanged;
        const digest = (try @import("graph_retirement_config.zig").fromLoaded(a, &.{actual.config})).?;
        if (!std.mem.eql(u8, &digest, &expected.definition_digest)) return error.PreparedGenerationChanged;
    }
}

pub const Definition = struct { name: []const u8, digest: [32]u8 };
/// Data only. This is accepted on server roots only inside the existing exact
/// final-body preparation grant, after a fresh metadata admission rejoin.
pub const Admission = struct {
    metadata_group_id: u64,
    incarnation: [16]u8,
    source_revision: u64,
    table_id: u64,
    schema_digest: [32]u8,
    indexes: []const Definition,
    data_protocol: u16 = 7,
    pub fn validate(self: @This()) !void {
        if (self.metadata_group_id == 0 or self.table_id == 0 or self.data_protocol != 7 or std.mem.allEqual(u8, &self.incarnation, 0) or
            std.mem.allEqual(u8, &self.schema_digest, 0) or self.indexes.len == 0 or self.indexes.len > 64) return error.InvalidPreparedGraphEffects;
        for (self.indexes, 0..) |index, i| {
            if (index.name.len == 0 or index.name.len > 256 or std.mem.allEqual(u8, &index.digest, 0)) return error.InvalidPreparedGraphEffects;
            if (i != 0 and !std.mem.lessThan(u8, self.indexes[i - 1].name, index.name)) return error.InvalidPreparedGraphEffects;
        }
    }
    pub fn requireIndex(self: @This(), name: []const u8, digest: [32]u8) !void {
        try self.validate();
        for (self.indexes) |index| if (std.mem.eql(u8, index.name, name)) {
            if (!std.mem.eql(u8, &index.digest, &digest)) return error.PreparedGenerationChanged;
            return;
        };
        return error.PreparedGenerationChanged;
    }
};

/// Metadata declaration identity is checked again under the actual native
/// writer/index fence, including ordinary protected atomic batches.
pub fn requireDefinitions(a: A, manager: anytype, proof: Admission) !void {
    try proof.validate();
    for (proof.indexes) |expected| {
        const actual = manager.graphIndex(expected.name) orelse return error.PreparedGenerationChanged;
        const digest = (try @import("graph_retirement_config.zig").fromLoaded(a, &.{actual.config})) orelse return error.PreparedGenerationChanged;
        if (!std.mem.eql(u8, &digest, &expected.digest)) return error.PreparedGenerationChanged;
    }
}
