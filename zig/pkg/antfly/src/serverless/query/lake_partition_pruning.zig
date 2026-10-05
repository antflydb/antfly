// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Inclusive projections of source predicates onto the file's Iceberg spec.
//! Unknown types/transforms/partition values remain residual predicates.
const std = @import("std");
const external = @import("antfly_local_sources").serverless_external_source_types;
const metadata = @import("../external_source/iceberg_metadata.zig");
const Predicate = @import("lake_stream.zig").Predicate;
const datetime = @import("antfly_local_sources").datetime;
pub const Rule = struct { spec: i32, partition: []const u8, source: []const u8, type_name: []const u8, transform: []const u8 };
pub const Rules = struct {
    arena: std.heap.ArenaAllocator,
    items: []const Rule,
    pub fn deinit(self: *Rules) void {
        self.arena.deinit();
    }
};
pub fn parseAlloc(a: std.mem.Allocator, uri: []const u8, bytes: []const u8, snapshot: []const u8, requested: ?[]const u8, fingerprint: []const u8) !Rules {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(std.json.Value, owned, bytes, .{ .allocate = .alloc_always });
    if (root != .object) return error.InvalidIcebergMetadata;
    var plan = try metadata.parseMetadataPlanAlloc(a, uri, bytes, requested);
    defer plan.deinit(a);
    if (!std.mem.eql(u8, plan.current_snapshot_id, snapshot) or !std.mem.eql(u8, plan.schema_fingerprint, fingerprint)) return error.ExternalLakeSnapshotMismatch;
    var items: std.ArrayList(Rule) = .empty;
    if (root.object.get("partition-specs")) |specs| {
        if (specs != .array) return error.InvalidIcebergMetadata;
        for (specs.array.items) |spec| {
            if (spec != .object) return error.InvalidIcebergMetadata;
            const id = spec.object.get("spec-id") orelse return error.InvalidIcebergMetadata;
            const fields = spec.object.get("fields") orelse return error.InvalidIcebergMetadata;
            if (id != .integer or fields != .array) return error.InvalidIcebergMetadata;
            const spec_id = std.math.cast(i32, id.integer) orelse return error.InvalidIcebergMetadata;
            for (fields.array.items) |field| {
                if (field != .object) return error.InvalidIcebergMetadata;
                const source = field.object.get("source-id") orelse continue;
                const name = field.object.get("name") orelse continue;
                const transform = field.object.get("transform") orelse continue;
                if (source != .integer or name != .string or transform != .string) continue;
                const column = for (plan.schema_fields) |column| {
                    if (column.id == source.integer) break column;
                } else continue;
                try items.append(owned, .{ .spec = spec_id, .partition = name.string, .transform = transform.string, .source = try owned.dupe(u8, column.name), .type_name = try owned.dupe(u8, column.type_name) });
            }
        }
    }
    const retained = try items.toOwnedSlice(owned);
    return .{ .arena = arena, .items = retained };
}
fn parameter(transform: []const u8, start: []const u8) ?i64 {
    if (!std.mem.startsWith(u8, transform, start) or !std.mem.endsWith(u8, transform, "]")) return null;
    const n = std.fmt.parseInt(i64, transform[start.len .. transform.len - 1], 10) catch return null;
    return if (n > 0 and n <= std.math.maxInt(i32)) n else null;
}
fn numeric(rule: Rule, value: i64) ?i64 {
    const integral = std.mem.eql(u8, rule.type_name, "int") or std.mem.eql(u8, rule.type_name, "long");
    if (std.mem.eql(u8, rule.transform, "identity")) return if (integral) value else null;
    if (parameter(rule.transform, "truncate[")) |width| {
        if (!integral) return null;
        return std.math.sub(i64, value, @mod(value, width)) catch null;
    }
    const timestamp = std.mem.startsWith(u8, rule.type_name, "timestamp") or std.mem.startsWith(u8, rule.type_name, "timestamptz");
    if (parameter(rule.transform, "bucket[")) |buckets| {
        if (!integral and !timestamp) return null;
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, if (timestamp) @divFloor(value, 1000) else value, .little);
        return @intCast((std.hash.Murmur3_32.hashWithSeed(&bytes, 0) & 0x7fffffff) % @as(u64, @intCast(buckets)));
    }
    const date = std.mem.eql(u8, rule.type_name, "date");
    if (!date and !timestamp) return null;
    const days = if (date) value else @divFloor(value, 86_400_000_000_000);
    if (std.mem.eql(u8, rule.transform, "day")) return days;
    if (std.mem.eql(u8, rule.transform, "hour")) return if (timestamp) @divFloor(value, 3_600_000_000_000) else null;
    const civil = datetime.civilFromDays(days);
    if (std.mem.eql(u8, rule.transform, "year")) return civil.year - 1970;
    if (std.mem.eql(u8, rule.transform, "month")) return (civil.year - 1970) * 12 + @as(i64, civil.month) - 1;
    return null;
}
fn compare(order: std.math.Order, op: Predicate.Op, exact: bool) bool {
    return switch (op) {
        .eq => order == .eq,
        .neq => if (exact) order != .eq else true,
        .lt => if (exact) order == .lt else order != .gt,
        .lte => order != .gt,
        .gt => if (exact) order == .gt else order != .lt,
        .gte => order != .lt,
    };
}
fn prefix(value: []const u8, width: i64) ?[]const u8 {
    var view = std.unicode.Utf8View.init(value) catch return null;
    var iter = view.iterator();
    var count: i64 = 0;
    while (count < width) : (count += 1) {
        if (iter.nextCodepointSlice() == null) break;
    }
    return value[0..iter.i];
}
pub fn mayMatch(rules: []const Rule, file: external.FileEntry, predicates: []const Predicate) bool {
    const spec = file.partition_spec_id orelse return true;
    for (predicates) |predicate| for (rules) |rule| {
        if (rule.spec != spec or !std.mem.eql(u8, rule.source, predicate.column)) continue;
        const stored = for (file.partition_values) |partition| {
            if (std.mem.eql(u8, partition.column_id, rule.partition)) break partition.string_value;
        } else continue;
        const exact = std.mem.eql(u8, rule.transform, "identity");
        const bucket = parameter(rule.transform, "bucket[");
        if (bucket != null and predicate.op != .eq) continue;
        switch (predicate.value) {
            .integer => |value| {
                const transformed = numeric(rule, value) orelse continue;
                const partition = std.fmt.parseInt(i64, stored, 10) catch continue;
                if (!compare(std.math.order(partition, transformed), predicate.op, exact)) return false;
            },
            .bytes => |value| {
                if (!std.mem.eql(u8, rule.type_name, "string")) continue;
                if (bucket) |buckets| {
                    const partition = std.fmt.parseInt(i64, stored, 10) catch continue;
                    const transformed = (std.hash.Murmur3_32.hashWithSeed(value, 0) & 0x7fffffff) % @as(u64, @intCast(buckets));
                    if (partition != transformed) return false;
                } else {
                    const transformed = if (exact) value else if (parameter(rule.transform, "truncate[")) |width| prefix(value, width) orelse continue else continue;
                    if (!compare(std.mem.order(u8, stored, transformed), predicate.op, exact)) return false;
                }
            },
            .boolean => |value| {
                if (!exact or !std.mem.eql(u8, rule.type_name, "boolean")) continue;
                const parsed = if (std.mem.eql(u8, stored, "true")) true else if (std.mem.eql(u8, stored, "false")) false else continue;
                if (!compare(std.math.order(@intFromBool(parsed), @intFromBool(value)), predicate.op, true)) return false;
            },
        }
    };
    return true;
}

test "external lake partition projections preserve inclusive boundaries and mixed specs" {
    const rules = [_]Rule{
        .{ .spec = 7, .partition = "p", .source = "n", .type_name = "long", .transform = "truncate[10]" },
        .{ .spec = 8, .partition = "p", .source = "n", .type_name = "long", .transform = "bucket[16]" },
    };
    var values = [_]external.PartitionValue{.{ .column_id = @constCast("p"), .string_value = @constCast("-10") }};
    var file: external.FileEntry = .{ .file_id = @constCast("f"), .object_uri = @constCast("object://b/k"), .byte_len = 1, .row_count = 1, .row_groups = &.{}, .partition_spec_id = 7, .partition_values = &values };
    try std.testing.expect(mayMatch(&rules, file, &.{.{ .column = "n", .op = .lt, .value = .{ .integer = -1 } }}));
    try std.testing.expect(!mayMatch(&rules, file, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
    try std.testing.expect(mayMatch(&rules, file, &.{.{ .column = "n", .op = .neq, .value = .{ .integer = -1 } }}));
    try std.testing.expectEqual(@as(?i64, -10), numeric(rules[0], -1));
    try std.testing.expectEqual(@as(?i64, 3), numeric(rules[1], 34));
    file.partition_spec_id = 8;
    values[0].string_value = @constCast("3");
    try std.testing.expect(mayMatch(&rules, file, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 34 } }}));
    values[0].string_value = @constCast("4");
    try std.testing.expect(!mayMatch(&rules, file, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 34 } }}));
    file.partition_spec_id = 99;
    try std.testing.expect(mayMatch(&rules, file, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 34 } }}));
    try std.testing.expectEqualStrings("é雪", prefix("é雪a", 2).?);
}
