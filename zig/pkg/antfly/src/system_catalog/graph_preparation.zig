// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Private first-use graph-intent admission. The existing metadata proposal
//! and bounded outcome journal establish reader capability before user work.
//! Read-only captures never raise a floor and hot captures do not propose.
const std = @import("std");
const local = @import("antfly_local_sources");
const snapshot = @import("trigger_snapshot.zig");
const effects = local.storage_db_prepared_graph_effects;
const identity = local.storage_db_graph_retirement_config;
const A = std.mem.Allocator;
pub const outcomes = @import("sequence_outcomes.zig");
pub const max_command_bytes = 64 * 1024;
pub const Prepared = struct { snapshot: snapshot.Snapshot, head: u64 };
pub const Source = struct {
    table_id: u64,
    epoch: @TypeOf(@as(snapshot.Snapshot, undefined).epoch),
    schema_digest: [32]u8,
    indexes: []const effects.Definition,
    pub fn validate(self: @This()) !void {
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.epoch.incarnation, 0) or std.mem.allEqual(u8, &self.schema_digest, 0) or self.indexes.len == 0 or self.indexes.len > 64) return error.InvalidPreparedGraphEffects;
        for (self.indexes, 0..) |index, i| {
            if (!local.api_index_metadata.isValidIndexName(index.name) or std.mem.allEqual(u8, &index.digest, 0)) return error.InvalidPreparedGraphEffects;
            if (i != 0 and !std.mem.lessThan(u8, self.indexes[i - 1].name, index.name)) return error.InvalidPreparedGraphEffects;
        }
    }
    pub fn requireSnapshot(self: @This(), a: A, captured: snapshot.Snapshot) !void {
        try self.validate();
        if (captured.record.table_id != self.table_id or !self.epoch.eql(captured.epoch)) return error.CatalogGenerationChanged;
        const names = try a.alloc([]const u8, self.indexes.len);
        defer a.free(names);
        for (names, self.indexes) |*name, index| name.* = index.name;
        const actual = capture(a, captured, names) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.CatalogGenerationChanged,
        };
        defer {
            for (actual.indexes) |index| a.free(index.name);
            a.free(actual.indexes);
        }
        if (!std.mem.eql(u8, &self.schema_digest, &actual.schema_digest)) return error.CatalogGenerationChanged;
        for (self.indexes, actual.indexes) |expected, current| if (!std.mem.eql(u8, &expected.digest, &current.digest)) return error.CatalogGenerationChanged;
    }
    pub fn admitted(self: @This(), group: u64) !effects.Admission {
        try self.validate();
        const proof: effects.Admission = .{ .metadata_group_id = group, .incarnation = self.epoch.incarnation, .source_revision = self.epoch.revision, .table_id = self.table_id, .schema_digest = self.schema_digest, .indexes = self.indexes };
        try proof.validate();
        return proof;
    }
};
pub const Mutation = struct {
    source: Source,
    admission: outcomes.Admission,
    pub fn validate(self: @This()) !void {
        try self.source.validate();
        try self.admission.validate();
        if (self.admission.command_fence != null) return error.InvalidPreparedGraphEffects;
    }
};
pub const Observation = struct {
    source: Source,
    admission: outcomes.Admission,
    digest: [32]u8,
    pub fn validate(self: @This(), a: A) !void {
        try self.source.validate();
        try (Mutation{ .source = self.source, .admission = self.admission }).validate();
        if (!std.mem.eql(u8, &self.digest, &try commandDigest(a, .{ .source = self.source, .admission = self.admission }))) return error.InvalidPreparedGraphEffects;
    }
};
pub fn commandDigest(a: A, request: Mutation) ![32]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    if (bytes.len > max_command_bytes) return error.CatalogCommandTooLarge;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/graph-preparation-admission/v1\x00");
    hash.update(bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn observation(a: A, request: Mutation) !Observation {
    return .{ .source = request.source, .admission = request.admission, .digest = try commandDigest(a, request) };
}
pub fn capture(a: A, captured: snapshot.Snapshot, names: []const []const u8) !Source {
    if (names.len == 0 or names.len > 64 or captured.source.value != .table or captured.source.value.table.binding.table_id != captured.record.table_id) return error.InvalidPreparedGraphEffects;
    const indexes = try a.alloc(effects.Definition, names.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |index| a.free(index.name);
        a.free(indexes);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, a, captured.record.indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableIndexMetadata;
    for (names, indexes) |name, *index| {
        const config = parsed.value.object.get(name) orelse return error.IndexNotFound;
        if (try local.api_table_index_config.parseIndexKind(config) != .graph) return error.InvalidPreparedGraphEffects;
        var one: std.json.ObjectMap = .empty;
        defer one.deinit(a);
        try one.put(a, name, config);
        const encoded = try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = one }, .{});
        defer a.free(encoded);
        const digest = (try identity.fromMetadata(a, encoded)) orelse return error.InvalidTableIndexMetadata;
        index.* = .{ .name = try a.dupe(u8, name), .digest = digest };
        initialized += 1;
    }
    std.mem.sort(effects.Definition, indexes, {}, struct {
        fn less(_: void, x: effects.Definition, y: effects.Definition) bool {
            return std.mem.lessThan(u8, x.name, y.name);
        }
    }.less);
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(captured.record.schema_json, &schema_digest, .{});
    const source: Source = .{ .table_id = captured.record.table_id, .epoch = captured.epoch, .schema_digest = schema_digest, .indexes = indexes };
    try source.validate();
    return source;
}

test "graph preparation binds nonce source epoch selected index schema and rejects command borrowing" {
    const a = std.testing.allocator;
    const request: Mutation = .{
        .source = .{ .table_id = 7, .epoch = .{ .incarnation = @splat(1), .revision = 9 }, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} },
        .admission = .{ .nonce = @splat(4), .head = 12 },
    };
    try request.validate();
    const observed = try observation(a, request);
    try observed.validate(a);
    const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(encoded);
    var decoded = try std.json.parseFromSlice(Mutation, a, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqualDeep(request, decoded.value);
    try std.testing.expectEqualDeep(observed, try observation(a, decoded.value));
    inline for (.{ "table_id", "schema_digest", "indexes" }) |field| {
        var replaced = request;
        if (comptime std.mem.eql(u8, field, "table_id")) replaced.source.table_id = 8;
        if (comptime std.mem.eql(u8, field, "schema_digest")) replaced.source.schema_digest = @splat(6);
        if (comptime std.mem.eql(u8, field, "indexes")) replaced.source.indexes = &.{.{ .name = "g", .digest = @splat(6) }};
        try std.testing.expect(!std.mem.eql(u8, &observed.digest, &try commandDigest(a, replaced)));
    }
    var mismatched = observed;
    mismatched.source.epoch.revision += 1;
    try std.testing.expectError(error.InvalidPreparedGraphEffects, mismatched.validate(a));
    var unordered = request;
    unordered.source.indexes = &.{ .{ .name = "z", .digest = @splat(1) }, .{ .name = "a", .digest = @splat(1) } };
    try std.testing.expectError(error.InvalidPreparedGraphEffects, unordered.validate());
    const admission = try request.source.admitted(11);
    try std.testing.expectEqual(@as(u16, 7), admission.data_protocol);
    try std.testing.expectEqual(@as(u64, 11), admission.metadata_group_id);
    try admission.requireIndex("g", @splat(3));
    try std.testing.expectError(error.PreparedGenerationChanged, admission.requireIndex("g", @splat(5)));
}
