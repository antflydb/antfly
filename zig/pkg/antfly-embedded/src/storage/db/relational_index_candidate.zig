// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Independently compiled private candidate for regular CREATE UNIQUE INDEX.
//! This validates an index-only schema delta, not publication authority. The
//! controller must separately bind the complete owner cut and name/table locks.
const std = @import("std");
const schema_api = @import("../../schema/mod.zig");
const native_schema = @import("../schema.zig");
const native_index = @import("../relational_index.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const tuples = @import("relational_index_keys.zig");
const predicates = @import("relational_index_predicate.zig");
const catalog = @import("relational_integrity_catalog.zig");
const compiler = @import("../../schema/relational_declarations.zig");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
pub const max_schema_bytes = 2 * 1024 * 1024;

pub const Candidate = struct {
    next: catalog.Update,
    reservation: catalog.Update,

    pub fn deinit(self: *Candidate) void {
        self.reservation.deinit();
        self.next.deinit();
        self.* = undefined;
    }

    /// Owns both encoded catalogs. All validation precedes any store writes.
    /// The predecessor is the actual owner catalog, including retired bindings
    /// and burned generation allocations, not a reconstruction from schema.
    pub fn prepare(alloc: Allocator, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, name: []const u8) !Candidate {
        if (table_id == 0 or name.len == 0 or name.len > 1024 or
            before_json.len > max_schema_bytes or after_json.len > max_schema_bytes or
            predecessor.len > catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
        try validateSchemaDelta(alloc, before_json, after_json, name);
        var before = try schema_api.parseValidatedTableSchema(alloc, before_json);
        defer before.deinit(alloc);
        var after = try schema_api.parseValidatedTableSchema(alloc, after_json);
        defer after.deinit(alloc);
        if (before.storage_mode != .relational or after.storage_mode != .relational or
            before.external_base_source != null or after.external_base_source != null or
            after.version != (std.math.add(u32, before.version, 1) catch return error.InvalidIndexPublicationCandidate))
            return error.InvalidIndexPublicationCandidate;
        try validatePhysicalPair(alloc, after, name);
        var current = try compiler.compileCatalog(alloc, before, table_id, predecessor);
        defer current.deinit();
        if (!std.mem.eql(u8, current.value, predecessor)) return error.IntegrityCatalogChanged;
        var next = try compiler.compileCatalog(alloc, after, table_id, predecessor);
        errdefer next.deinit();
        _ = try catalog.indexAddition(current.catalog, next.catalog, name);
        const reservation = try catalog.prepareIndexReservation(alloc, predecessor, next.catalog, name);
        return .{ .next = next, .reservation = reservation };
    }
};

/// Name equality alone must not authorize building different physical keys or
/// partial membership than the UNIQUE rule being published. Compare independently
/// compiled equality keys/predicates; sort direction and covering columns are
/// presentation/storage choices, not uniqueness semantics.
fn validatePhysicalPair(alloc: Allocator, schema: schema_api.ParsedTableSchema, name: []const u8) !void {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const a = scratch.allocator();
    const indexes = (try schema.relationalIndexDefinitions(a)) orelse return error.InvalidIndexPublicationCandidate;
    const rules = try schema.relationalUniqueDefinitions(a);
    const index = for (indexes) |entry| {
        if (std.mem.eql(u8, entry.name, name)) break entry;
    } else return error.InvalidIndexPublicationCandidate;
    const rule = for (rules) |entry| {
        if (std.mem.eql(u8, entry.name, name)) break entry;
    } else return error.InvalidIndexPublicationCandidate;
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, schema);
    defer native_schema.freeSchema(alloc, runtime);
    var layout = try rows.PhysicalLayout.init(alloc, runtime);
    defer layout.deinit();
    const rule_keys = if (rule.keys.len != 0) rule.keys else blk: {
        const keys = try a.alloc(native_index.RelationalIndexKey, rule.columns.len);
        for (rule.columns, keys) |column, *key| key.* = .{ .column = column };
        break :blk keys;
    };
    var physical = try tuples.TuplePlan.init(alloc, runtime, &layout, index.keys);
    defer physical.deinit();
    var logical = try tuples.TuplePlan.init(alloc, runtime, &layout, rule_keys);
    defer logical.deinit();
    if (physical.keys.len != logical.keys.len) return error.InvalidIndexPublicationCandidate;
    for (0..physical.keys.len) |i| if (!physical.sameEqualityKey(i, logical, i)) return error.InvalidIndexPublicationCandidate;
    const physical_where = try predicateIdentity(alloc, runtime, &layout, index.where);
    const logical_where = try predicateIdentity(alloc, runtime, &layout, rule.where);
    if (!std.mem.eql(u8, &physical_where, &logical_where)) return error.InvalidIndexPublicationCandidate;
}

fn predicateIdentity(alloc: Allocator, schema: native_schema.TableSchema, layout: *const rows.PhysicalLayout, conditions: []const native_index.UniquePredicate) ![32]u8 {
    if (conditions.len == 0) return @splat(0);
    var plan = try predicates.Plan.init(alloc, schema, layout, conditions);
    defer plan.deinit();
    return plan.identity;
}

fn touched(key: []const u8) bool {
    return std.mem.eql(u8, key, "version") or std.mem.eql(u8, key, "relational_indexes") or std.mem.eql(u8, key, "unique_constraints");
}

const equal = @import("../../schema/json_shape.zig").equal;

fn entries(schema: Value, key: []const u8) ![]const Value {
    const value = schema.object.get(key) orelse return &.{};
    if (value != .array) return error.InvalidIndexPublicationCandidate;
    return value.array.items;
}

fn named(value: Value, name: []const u8) bool {
    if (value != .object) return false;
    const field = value.object.get("name") orelse return false;
    return field == .string and std.mem.eql(u8, field.string, name);
}

fn addition(before: []const Value, after: []const Value, name: []const u8) !Value {
    if (after.len != before.len + 1) return error.InvalidIndexPublicationCandidate;
    for (before) |entry| if (named(entry, name)) return error.InvalidIndexPublicationCandidate;
    var old: usize = 0;
    var result: ?Value = null;
    for (after) |entry| {
        if (named(entry, name)) {
            if (result != null) return error.InvalidIndexPublicationCandidate;
            result = entry;
        } else {
            if (old == before.len or !equal(before[old], entry, 0)) return error.InvalidIndexPublicationCandidate;
            old += 1;
        }
    }
    if (old != before.len) return error.InvalidIndexPublicationCandidate;
    return result orelse error.InvalidIndexPublicationCandidate;
}

fn validateSchemaDelta(alloc: Allocator, before_json: []const u8, after_json: []const u8, name: []const u8) !void {
    var before = try std.json.parseFromSlice(Value, alloc, before_json, .{ .max_value_len = max_schema_bytes });
    defer before.deinit();
    var after = try std.json.parseFromSlice(Value, alloc, after_json, .{ .max_value_len = max_schema_bytes });
    defer after.deinit();
    if (before.value != .object or after.value != .object) return error.InvalidIndexPublicationCandidate;
    for ([_]bool{ false, true }) |reverse| {
        const left = if (reverse) after.value else before.value;
        const right = if (reverse) before.value else after.value;
        var it = left.object.iterator();
        while (it.next()) |entry| {
            if (touched(entry.key_ptr.*)) continue;
            const other = right.object.get(entry.key_ptr.*) orelse return error.InvalidIndexPublicationCandidate;
            if (!equal(entry.value_ptr.*, other, 0)) return error.InvalidIndexPublicationCandidate;
        }
    }
    _ = try addition(try entries(before.value, "relational_indexes"), try entries(after.value, "relational_indexes"), name);
    const rule = try addition(try entries(before.value, "unique_constraints"), try entries(after.value, "unique_constraints"), name);
    const origin = rule.object.get("origin") orelse return error.InvalidIndexPublicationCandidate;
    if (origin != .string or !std.mem.eql(u8, origin.string, "index")) return error.InvalidIndexPublicationCandidate;
    // An ordinary SQL index cannot smuggle a PRIMARY/deferred constraint into
    // the candidate. The typed schema compiler checks expression/key coverage.
    for ([_][]const u8{ "primary", "deferrable" }) |key| if (rule.object.get(key)) |value| {
        if (value != .bool or value.bool) return error.InvalidIndexPublicationCandidate;
    };
}

const test_before =
    \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
;

test "relational index candidate owns its native catalogs after request memory is released" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var candidate = blk: {
        var request = std.heap.ArenaAllocator.init(a);
        defer request.deinit();
        const input = request.allocator();
        break :blk try Candidate.prepare(a, 101, try input.dupe(u8, test_before), try input.dupe(u8, test_after), try input.dupe(u8, prior.value), try input.dupe(u8, "email_idx"));
    };
    defer candidate.deinit();
    try std.testing.expectEqualStrings("email_idx", candidate.next.catalog.find(.unique, "email_idx").?.definition.name);
    try std.testing.expectEqualSlices(u8, prior.value, candidate.reservation.expected.?);
    try std.testing.expectEqual(@as(u32, 1), candidate.reservation.catalog.schema_version);
    try std.testing.expectEqual(@as(u32, 2), candidate.next.catalog.schema_version);
    var publication = try catalog.prepareIndexPublication(a, prior.value, candidate.reservation.value, candidate.next.value, "email_idx");
    defer publication.deinit();
    try std.testing.expectEqualSlices(u8, candidate.next.value, publication.value);
}
const test_after =
    \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
;

test "relational index candidate preserves CHECK coverage and reserves without public schema change" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var candidate = try Candidate.prepare(a, 101, test_before, test_after, prior.value, "email_idx");
    defer candidate.deinit();
    try std.testing.expectEqual(@as(u32, 1), candidate.reservation.catalog.schema_version);
    try std.testing.expectEqual(@as(u32, 2), candidate.next.catalog.schema_version);
    try std.testing.expect(candidate.reservation.catalog.find(.unique, "email_idx") == null);
    try std.testing.expectEqualSlices(u8, &prior.catalog.checks_digest, &candidate.next.catalog.checks_digest);
    try std.testing.expectEqual(prior.catalog.next_generation + 1, candidate.reservation.catalog.next_generation);
    var installed = try catalog.prepareIndexPublication(a, prior.value, candidate.reservation.value, candidate.next.value, "email_idx");
    defer installed.deinit();
    try std.testing.expectEqualSlices(u8, candidate.next.value, installed.value);
}

test "relational index candidate rejects unrelated schema changes and incomplete index pairs" {
    const a = std.testing.allocator;
    try validateSchemaDelta(a, test_before, test_after, "email_idx");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const edits = [_]struct { from: []const u8, to: []const u8 }{
        .{ .from = "\"additionalProperties\":false", .to = "\"additionalProperties\":true" },
        .{ .from = "\"op\":\"gt\"", .to = "\"op\":\"gte\"" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"constraint\"" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"index\",\"primary\":true" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"index\",\"deferrable\":true" },
        .{ .from = "\"relational_indexes\"", .to = "\"other_indexes\"" },
        .{ .from = "\"unique_constraints\"", .to = "\"other_constraints\"" },
    };
    for (edits) |edit| {
        const changed = try std.mem.replaceOwned(u8, scratch, test_after, edit.from, edit.to);
        try std.testing.expectError(error.InvalidIndexPublicationCandidate, validateSchemaDelta(a, test_before, changed, "email_idx"));
    }
    // JSON object key order is irrelevant, unlike changes to row semantics.
    try std.testing.expect(equal(try std.json.parseFromSliceLeaky(Value, scratch, "{\"a\":1,\"b\":2}", .{}), try std.json.parseFromSliceLeaky(Value, scratch, "{\"b\":2,\"a\":1}", .{}), 0));
}

test "relational index candidate rejects stale predecessor and mismatched physical keys or predicates" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    const wrong_key = try std.mem.replaceOwned(u8, a, test_after, "\"columns\":[\"email\"]", "\"columns\":[\"id\"]");
    defer a.free(wrong_key);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Candidate.prepare(a, 101, test_before, wrong_key, prior.value, "email_idx"));
    const wrong_where = try std.mem.replaceOwned(u8, a, test_after, "\"origin\":\"index\"", "\"origin\":\"index\",\"where\":[{\"column\":\"id\",\"op\":\"gt\",\"value\":0}]");
    defer a.free(wrong_where);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Candidate.prepare(a, 101, test_before, wrong_where, prior.value, "email_idx"));
    const stale = try std.mem.replaceOwned(u8, a, test_before, "\"version\":1", "\"version\":0");
    defer a.free(stale);
    var stale_schema = try schema_api.parseValidatedTableSchema(a, stale);
    defer stale_schema.deinit(a);
    var stale_catalog = try compiler.compileCatalog(a, stale_schema, 101, null);
    defer stale_catalog.deinit();
    try std.testing.expectError(error.IntegrityCatalogChanged, Candidate.prepare(a, 101, test_before, test_after, stale_catalog.value, "email_idx"));
    try std.testing.expectError(error.IntegrityCatalogIncarnationMismatch, Candidate.prepare(a, 102, test_before, test_after, prior.value, "email_idx"));
}

test "relational index candidate retry preserves the canceled attempt generation burn" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var first = try Candidate.prepare(a, 101, test_before, test_after, prior.value, "email_idx");
    defer first.deinit();
    // Cancellation keeps the reservation and the original public schema. A
    // retry must not resurrect the first private build's keys/unique claims.
    var retry = try Candidate.prepare(a, 101, test_before, test_after, first.reservation.value, "email_idx");
    defer retry.deinit();
    const first_binding = first.next.catalog.find(.unique, "email_idx").?;
    const retry_binding = retry.next.catalog.find(.unique, "email_idx").?;
    try std.testing.expectEqual(first_binding.allocation + 1, retry_binding.allocation);
    try std.testing.expect(!std.mem.eql(u8, &first_binding.generation, &retry_binding.generation));
    try std.testing.expectEqualSlices(u8, &prior.catalog.schema_digest, &retry.reservation.catalog.schema_digest);
    try std.testing.expectEqualSlices(u8, &prior.catalog.checks_digest, &retry.reservation.catalog.checks_digest);
    try std.testing.expect(retry.reservation.catalog.find(.unique, "email_idx") == null);
}
