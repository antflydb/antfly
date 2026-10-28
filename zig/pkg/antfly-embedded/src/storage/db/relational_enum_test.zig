// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Enum columns across native schema epochs, preparation, AROW and tuple keys.
const std = @import("std");
const schema = @import("../schema.zig");
const types = @import("../../system_catalog/named_types.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const mapper = @import("document_mapper.zig");
const keys = @import("relational_index_keys.zig");
const A = std.mem.Allocator;

fn encode(a: A, table: schema.TableSchema, text: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
    defer parsed.deinit();
    return mapper.buildRelationalRowValueForSchemaFromParsedAlloc(a, parsed.value, table);
}

test "SQL named enum native epochs own one definition and reject malformed dependency contracts" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "m", "" });
    defer labels.deinit();
    const records = [_]types.Record{.{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .{ .user = .{ .name = "owner", .instance_id = @splat(5) } } }};
    const columns = [_]schema.RelationalColumn{
        .{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7, .allows_null = true },
        .{ .name = "previous", .path = "previous", .column_type = .enumeration, .named_type_id = 7, .allows_null = true },
    };
    const table: schema.TableSchema = .{ .version = 9, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &columns, .named_types = &records };
    const bytes = try schema.serializeSchema(a, table);
    defer a.free(bytes);
    try std.testing.expectEqual(@as(u32, 39), schema.requiredReaderVersion(table));
    try std.testing.expect(!(schema.ReaderCapabilities{ .semantic_ceiling = 38, .encoding_ceiling = 39 }).supports(schema.readerRequirementsForSchema(table)));
    const decoded = try schema.deserializeSchema(a, bytes);
    defer schema.freeSchema(a, decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.named_types.len);
    try std.testing.expectEqualDeep(table.named_types[0], decoded.named_types[0]);
    try std.testing.expect(decoded.named_types[0].definition.labels.ptr != records[0].definition.labels.ptr);
    const canonical = try schema.serializeSchema(a, decoded);
    defer a.free(canonical);
    try std.testing.expectEqualSlices(u8, bytes, canonical);
    for (0..bytes.len) |length| {
        if (schema.deserializeSchema(a, bytes[0..length])) |unexpected| {
            schema.freeSchema(a, unexpected);
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    var missing = table;
    missing.named_types = &.{};
    try std.testing.expectError(error.InvalidSchema, schema.serializeSchema(a, missing));
    var wrong = columns;
    wrong[0].named_type_id = 99;
    missing = table;
    missing.relational_columns = &wrong;
    try std.testing.expectError(error.InvalidSchema, schema.serializeSchema(a, missing));
    var duplicate = [_]types.Record{ records[0], records[0] };
    duplicate[1].identity = try records[0].identity.remap(8, 2);
    missing = table;
    missing.named_types = &duplicate;
    try std.testing.expectError(error.InvalidSchema, schema.serializeSchema(a, missing));
}

test "SQL named enum native rows preserve symbols logical hashes and index ranks through cold evolution" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "m", "" });
    defer labels.deinit();
    const records = [_]types.Record{.{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled }};
    const columns = [_]schema.RelationalColumn{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7, .allows_null = true }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &columns, .named_types = &records };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    try std.testing.expectEqual(@as(u32, 4), layout.fixed_len);
    try std.testing.expectEqual(@as(u32, 0), layout.variable_count);
    var plan = try keys.TuplePlan.init(a, table, &layout, &.{.{ .column = "state" }});
    defer plan.deinit();
    const z = try encode(a, table, "{\"state\":\"z\"}");
    defer a.free(z);
    const a_row = try encode(a, table, "{\"state\":\"a\"}");
    defer a.free(a_row);
    const empty = try encode(a, table, "{\"state\":\"\"}");
    defer a.free(empty);
    const null_row = try encode(a, table, "{\"state\":null}");
    defer a.free(null_row);
    const absent = try encode(a, table, "{}");
    defer a.free(absent);
    try std.testing.expectError(error.SqlInvalidTextRepresentation, encode(a, table, "{\"state\":\"Z\"}"));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, encode(a, table, "{\"state\":1}"));
    const z_view = try codec.ordinalRowView(z, table, &layout);
    const a_view = try codec.ordinalRowView(a_row, table, &layout);
    const z_key = try plan.encodeAlloc(a, z_view);
    defer a.free(z_key.bytes);
    const a_key = try plan.encodeAlloc(a, a_view);
    defer a.free(a_key.bytes);
    try std.testing.expect(std.mem.order(u8, z_key.bytes, a_key.bytes) == .lt);
    try std.testing.expectEqual(z_key.bytes.len, try plan.prefixLen(z_key.bytes));
    const atom = (try layout.enumView(0).?.input("z")).*;
    var bound = std.ArrayList(u8).empty;
    defer bound.deinit(a);
    _ = try plan.appendValues(a, &bound, &.{.{ .enumeration = &atom }});
    try std.testing.expectEqualSlices(u8, z_key.bytes, bound.items);
    var impostor = atom;
    impostor.origin_id = @splat(4);
    try std.testing.expectError(error.InvalidRelationalIndexBound, plan.appendValues(a, &bound, &.{.{ .enumeration = &impostor }}));
    try std.testing.expectEqualSlices(u8, z_key.bytes, bound.items);
    try std.testing.expect((try codec.findCellByOrdinal(null_row, table, 0)).?.is_null);
    try std.testing.expect(try codec.findCellByOrdinal(absent, table, 0) == null);
    const empty_json = try codec.reconstructOrdinalValueWithLayoutAlloc(a, empty, table, &layout);
    defer a.free(empty_json);
    try std.testing.expectEqualStrings("{\"state\":\"\"}", empty_json);
    // Independent vector sources must produce the same tuple as primary AROW.
    const sources = @import("../rowsource/types.zig");
    const native_batch: sources.ColumnBatch = .{
        .snapshot = .{ .table_id = "states", .snapshot_id = "cold" },
        .row_refs = &.{ .{ .relational_key = "z" }, .{ .relational_key = "a" }, .{ .relational_key = "null" } },
        .columns = &.{.{ .name = "state", .nulls = .{ .bytes = &.{ 0, 0, 1 } }, .values = .{ .i64 = &.{ 1, 2, -1 } } }},
    };
    var native_keys = try plan.bindBatch(a, native_batch);
    defer native_keys.deinit();
    var external_batch = native_batch;
    external_batch.columns = &.{.{ .name = "state", .nulls = .{ .bytes = &.{ 0, 0, 1 } }, .values = .{ .bytes = &.{ "z", "a", "invalid-but-null" } } }};
    var external_keys = try plan.bindExternalBatch(a, external_batch);
    defer external_keys.deinit();
    for (0..3) |row_index| {
        bound.clearRetainingCapacity();
        const native_null = try native_keys.append(a, &bound, row_index);
        const copy = try a.dupe(u8, bound.items);
        defer a.free(copy);
        bound.clearRetainingCapacity();
        try std.testing.expectEqual(native_null, try external_keys.append(a, &bound, row_index));
        try std.testing.expectEqualSlices(u8, copy, bound.items);
        if (row_index == 0) try std.testing.expectEqualSlices(u8, z_key.bytes, copy);
        if (row_index == 1) try std.testing.expectEqualSlices(u8, a_key.bytes, copy);
    }
    // A checksum authenticates bytes, not membership in the declared enum.
    const corrupt = try a.dupe(u8, z);
    defer a.free(corrupt);
    try std.testing.expectEqual(@as(usize, 66), corrupt.len); // 56 header + two bitmaps + four-byte symbol + CRC32.
    for ([_]u32{ 0, 5, std.math.maxInt(u32) }) |symbol| {
        std.mem.writeInt(u32, corrupt[58..62], symbol, .little);
        try codec.setOrdinalSemanticHash(corrupt, try codec.rowSemanticHash(z));
        try std.testing.expectError(error.InvalidSqlEnumSymbol, codec.validateOrdinalWithLayout(corrupt, table, &layout));
        try std.testing.expectError(error.InvalidSqlEnumSymbol, codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(a, corrupt, table, &layout));
    }
    const digest = try codec.rowSemanticHash(z);
    var successor = try types.Owned.add(a, labels.definition, "x", .{ .before = "a" });
    defer successor.deinit();
    var next_records = records;
    next_records[0].definition = successor.definition;
    next_records[0].identity.generation += 1;
    var next = table;
    next.version = 2;
    next.named_types = &next_records;
    const schema_bytes = try schema.serializeSchema(a, next);
    defer a.free(schema_bytes);
    const cold = try schema.deserializeSchema(a, schema_bytes);
    defer schema.freeSchema(a, cold);
    var cold_layout = try codec.PhysicalLayout.init(a, cold);
    defer cold_layout.deinit();
    var cold_plan = try keys.TuplePlan.init(a, cold, &cold_layout, &.{.{ .column = "state" }});
    defer cold_plan.deinit();
    try std.testing.expectEqualDeep(plan.fingerprint, cold_plan.fingerprint);
    var historical_plan = try cold_plan.projectSource(a, table, &layout);
    defer historical_plan.deinit();
    const old_key = try historical_plan.encodeAlloc(a, z_view);
    defer a.free(old_key.bytes);
    try std.testing.expectEqualSlices(u8, z_key.bytes, old_key.bytes);
    const next_z = try encode(a, cold, "{\"state\":\"z\"}");
    defer a.free(next_z);
    try std.testing.expectEqualDeep(digest, try codec.rowSemanticHash(next_z));
    var restored = try codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(a, next_z, cold, &cold_layout);
    defer restored.deinit(a);
    try std.testing.expectEqualStrings("z", restored.root.object.get("state").?.string);
    var remapped_records = next_records;
    remapped_records[0].identity = try next_records[0].identity.remap(77, 5);
    var remapped_columns = columns;
    remapped_columns[0].named_type_id = 77;
    var remapped = next;
    remapped.relational_columns = &remapped_columns;
    remapped.named_types = &remapped_records;
    const remapped_z = try encode(a, remapped, "{\"state\":\"z\"}");
    defer a.free(remapped_z);
    try std.testing.expectEqualDeep(digest, try codec.rowSemanticHash(remapped_z));
    var remapped_layout = try codec.PhysicalLayout.init(a, remapped);
    defer remapped_layout.deinit();
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, cold_plan.projectSource(a, remapped, &remapped_layout));
    var bad_cell = (try z_view.findCell(0)).?;
    bad_cell.value = .{ .i64_val = 0 };
    try std.testing.expectError(error.InvalidSqlEnumSymbol, codec.serializeOrdinal(a, 1, &columns, &.{bad_cell}, @splat(0)));
    bad_cell.value = .{ .i64_val = 100 };
    try std.testing.expectError(error.InvalidSqlEnumSymbol, codec.serializeOrdinal(a, 1, &columns, &.{bad_cell}, @splat(0)));
}

test "SQL named enum native epoch decoding and compiled directories unwind every allocation" {
    const Run = struct {
        fn run(a: A, bytes: []const u8) !void {
            const table = try schema.deserializeSchema(a, bytes);
            defer schema.freeSchema(a, table);
            var layout = try codec.PhysicalLayout.init(a, table);
            defer layout.deinit();
            var plan = try keys.TuplePlan.init(a, table, &layout, &.{.{ .column = "state" }});
            defer plan.deinit();
            var cover = try @import("relational_index_cover.zig").Plan.init(a, table, &layout, &.{}, &.{"state"});
            defer cover.deinit();
            _ = try layout.enumView(0).?.input("a");
        }
    };
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "m" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .{ .role = .{ .name = "enum_owner", .instance_id = @splat(6) } } };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &.{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7 }}, .named_types = &.{record} };
    const bytes = try schema.serializeSchema(a, table);
    defer a.free(bytes);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{bytes});
}

test "SQL named enum native sparse epochs and projected tuple loop avoid per-row allocations" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a", "m", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    var columns: [64]schema.RelationalColumn = undefined;
    for (&columns, 0..) |*column, ordinal| column.* = .{ .name = try std.fmt.allocPrint(a, "c{d}", .{ordinal}), .path = "unused", .column_type = .enumeration, .named_type_id = 7, .allows_null = true };
    defer for (&columns) |column| a.free(column.name);
    for (&columns) |*column| column.path = column.name;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &columns, .named_types = &.{record} };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    try std.testing.expectEqual(@as(usize, 1), layout.enum_views.len);
    const bytes = try encode(a, table, "{\"c63\":\"a\"}");
    defer a.free(bytes);
    const view = try codec.ordinalRowView(bytes, table, &layout);
    var iterator = try codec.canonicalOrdinalCellIteratorWithLayout(bytes, table, &layout);
    try std.testing.expect(iterator.isSparse());
    try std.testing.expectEqual(@as(usize, 1), iterator.presentCount());
    var plan = try keys.TuplePlan.init(a, table, &layout, &.{.{ .column = "c63" }});
    defer plan.deinit();
    var out = std.ArrayList(u8).empty;
    defer out.deinit(a);
    try out.ensureTotalCapacity(a, 128);
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var total: u64 = 0;
    for (0..100_000) |_| {
        const cell = (try view.findCell(63)).?;
        total += @intCast(cell.value.i64_val);
        out.clearRetainingCapacity();
        _ = try plan.append(denied.allocator(), &out, view);
    }
    try std.testing.expectEqual(@as(u64, 200_000), total);
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
    std.debug.print("SQL enum native sparse projection: rows=100000 columns=64 row_bytes={} elapsed_ns={} allocations={} value_slot_bytes={}\n", .{ bytes.len, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started, denied.alloc_index, @sizeOf(@import("../../sql/row_value.zig").Value) });
}

test "SQL named enum covering indexes retain only referenced definitions and fence type identity" {
    const a = std.testing.allocator;
    const Cover = @import("relational_index_cover.zig").Plan;
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const records = [_]types.Record{
        .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled },
        .{ .identity = try types.Identity.init(9, 2, 1, @splat(4)), .definition = labels.definition, .owner = .auth_disabled },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .named_types = &records, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7 },
    } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var cover = try Cover.init(a, table, &layout, &.{.{ .column = "id" }}, &.{"state"});
    defer cover.deinit();
    try std.testing.expectEqual(@as(usize, 1), cover.named_types.len);
    try std.testing.expectEqual(@as(usize, 1), cover.layout.enum_views.len);
    const row = try encode(a, table, "{\"id\":1,\"state\":\"a\"}");
    defer a.free(row);
    const encoded = try cover.encode(a, try codec.ordinalRowView(row, table, &layout));
    defer a.free(encoded);
    const decoded = try cover.decode(encoded);
    const field = (try decoded.findCell(1)).?;
    try std.testing.expectEqual(@as(i64, 2), field.value.i64_val);
    const logical = try codec.ownedJsonValueFromCellAlloc(a, cover.columns[1], field);
    defer a.free(logical.string);
    try std.testing.expectEqualStrings("a", logical.string);
    var source = try cover.projectSource(a, table, &layout);
    defer source.deinit();
    const historical = try cover.encodeSource(a, try codec.ordinalRowView(row, table, &layout), &source);
    defer a.free(historical);
    try std.testing.expectEqualSlices(u8, encoded, historical);
    var impostors = records;
    impostors[0].identity.origin_id = @splat(8);
    var other = table;
    other.named_types = &impostors;
    var other_layout = try codec.PhysicalLayout.init(a, other);
    defer other_layout.deinit();
    var other_cover = try Cover.init(a, other, &other_layout, &.{.{ .column = "id" }}, &.{"state"});
    defer other_cover.deinit();
    try std.testing.expect(!std.mem.eql(u8, &cover.fingerprint, &other_cover.fingerprint));
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, cover.projectSource(a, other, &other_layout));
    try std.testing.expectError(error.InvalidRelationalIndexForwardValue, other_cover.decode(encoded));
}

test "SQL named enum schema pins retain atom directories after producer and initial owner release" {
    const a = std.testing.allocator;
    const registry = @import("schema_registry.zig");
    const epoch = create: {
        var labels = try types.Owned.create(a, &.{ "z", "a" });
        defer labels.deinit();
        const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
        const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .named_types = &.{record}, .relational_columns = &.{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7 }} };
        break :create try registry.Epoch.createCloned(a, table);
    };
    var initial: registry.SchemaView = .{ .epoch = epoch };
    var pinned = initial.clone();
    initial.release();
    defer pinned.release();
    const view = pinned.physicalLayout().enumView(0).?;
    const atom = try view.input("a");
    const encoded = try view.encode(atom);
    try std.testing.expectEqual(atom, try view.decode(&encoded));
    try std.testing.expectEqualStrings("a", try view.output(atom));
    try std.testing.expectEqual(@as(u32, 39), pinned.readerRequirements().semantic_version);
    try std.testing.expectEqual(@as(u32, 39), pinned.readerRequirements().encoding_version);
}
