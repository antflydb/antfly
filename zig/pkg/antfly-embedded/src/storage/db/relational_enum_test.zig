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
    for ([_][]const u8{ "{\"state\":\"z\"}", "{\"state\":\"a\"}", "{\"state\":\"\"}", "{\"state\":null}", "{}" }, [_][]const u8{ z, a_row, empty, null_row, absent }) |json, row| {
        const logical = try @import("document_content_hash.zig").hashRelationalJson(a, json, table);
        try std.testing.expectEqualDeep(try codec.rowSemanticHash(row), logical);
    }
    try std.testing.expectError(error.SqlInvalidTextRepresentation, @import("document_content_hash.zig").hashRelationalJson(a, "{\"state\":\"Z\"}", table));
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
    const domain: types.Record = .{ .identity = try types.Identity.init(9, 2, 1, @splat(4)), .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .named = .{ .id = record.identity.id, .origin_id = record.identity.origin_id, .generation = record.identity.generation } } } } };
    for ([_]bool{ false, true }) |as_domain| {
        for (&columns) |*column| column.named_type_id = if (as_domain) 9 else 7;
        const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &columns, .named_types = if (as_domain) &.{ record, domain } else &.{record} };
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
        std.debug.print("SQL enum native sparse projection: domain={} rows=100000 columns=64 row_bytes={} elapsed_ns={} allocations={} value_slot_bytes={}\n", .{ as_domain, bytes.len, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started, denied.alloc_index, @sizeOf(@import("../../sql/row_value.zig").Value) });
    }
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
    try std.testing.expectEqual(schema.storage_format_version, pinned.readerRequirements().encoding_version);
}

test "SQL named enum FK planners retain exact domains across column names and additive epochs" {
    const a = std.testing.allocator;
    const registry = @import("schema_registry.zig");
    const integrity = @import("../../api/relational_integrity.zig");
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    var evolved = try types.Owned.add(a, labels.definition, "middle", .{ .before = "a" });
    defer evolved.deinit();
    const base: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const child_schema: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .named_types = &.{base}, .relational_columns = &.{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7 }} };
    var child: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, child_schema) };
    defer child.release();
    const parent_unique: @import("../relational_index.zig").UniqueConstraint = .{ .name = "pk", .columns = &.{"parent_state"} };
    const Cases = enum { same, additive, another_type, another_origin, forged_rank, forged_label };
    for (std.enums.values(Cases)) |case| {
        var record = base;
        var forged = [_]types.Label{ labels.definition.labels[0], labels.definition.labels[1] };
        switch (case) {
            .same => {},
            .additive => {
                record.identity.generation = 2;
                record.definition = evolved.definition;
            },
            .another_type => record.identity = try record.identity.remap(8, 2),
            .another_origin => record.identity.origin_id = @splat(8),
            .forged_rank => {
                forged[0].order_key = "\x01";
                record.definition.labels = &forged;
            },
            .forged_label => {
                forged[0].text = "other";
                record.definition.labels = &forged;
            },
        }
        const parent_schema: schema.TableSchema = .{ .version = 2, .storage_mode = .relational, .requires_public_schema = true, .named_types = &.{record}, .relational_columns = &.{.{ .name = "parent_state", .path = "parent_state", .column_type = .enumeration, .named_type_id = record.identity.id }} };
        var parent: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, parent_schema) };
        defer parent.release();
        const binding: integrity.ForeignBinding = .{
            .generation = @splat(1),
            .parent_generation = @splat(2),
            .definition = .{ .name = "fk", .child_columns = &.{"state"}, .parent_table = "parent", .parent_columns = &.{"parent_state"} },
            .parent = parent,
            .parent_unique = parent_unique,
        };
        if (case != .same and case != .additive) {
            try std.testing.expectError(error.ForeignKeyTypeMismatch, integrity.Plan.init(a, "child", child, &.{}, &.{binding}));
            continue;
        }
        var plan = try integrity.Plan.init(a, "child", child, &.{}, &.{binding});
        defer plan.deinit();
        const child_bytes = try encode(a, child_schema, "{\"state\":\"a\"}");
        defer a.free(child_bytes);
        const parent_bytes = try encode(a, parent_schema, "{\"parent_state\":\"a\"}");
        defer a.free(parent_bytes);
        var child_tuple = try keys.TuplePlan.init(a, child.tableSchema().*, child.physicalLayout(), &.{.{ .column = "state" }});
        defer child_tuple.deinit();
        var parent_tuple = try keys.TuplePlan.init(a, parent.tableSchema().*, parent.physicalLayout(), &.{.{ .column = "parent_state" }});
        defer parent_tuple.deinit();
        try std.testing.expect(child_tuple.sameValueDomain(0, parent_tuple, 0));
        const left = try child_tuple.encodeAlloc(a, try codec.ordinalRowView(child_bytes, child.tableSchema().*, child.physicalLayout()));
        defer a.free(left.bytes);
        const right = try parent_tuple.encodeAlloc(a, try codec.ordinalRowView(parent_bytes, parent.tableSchema().*, parent.physicalLayout()));
        defer a.free(right.bytes);
        try std.testing.expectEqualSlices(u8, left.bytes, right.bytes);
    }
}

test "SQL named enum JSON predicates and bounds prepare borrowed atoms and preserve declaration order" {
    const a = std.testing.allocator;
    const predicates = @import("relational_predicate.zig");
    const partial = @import("relational_index_predicate.zig");
    var labels = try types.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .named_types = &.{record}, .relational_columns = &.{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7, .allows_null = true }} };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var tuple = try keys.TuplePlan.init(a, table, &layout, &.{.{ .column = "state" }});
    defer tuple.deinit();
    const literal = try tuple.resolveLiteral(0, .{ .string = "a" });
    try std.testing.expectEqual(try layout.enumView(0).?.input("a"), literal.enumeration);
    var typed = std.ArrayList(u8).empty;
    defer typed.deinit(a);
    _ = try tuple.appendValues(a, &typed, &.{literal});
    var wire = std.ArrayList(u8).empty;
    defer wire.deinit(a);
    _ = try tuple.appendLiteralValues(a, &wire, &.{.{ .string = "a" }});
    try std.testing.expectEqualSlices(u8, typed.items, wire.items);
    const prefix = wire.items.len;
    // The typed encoder remains strict; only the explicit JSON boundary may
    // turn a label into an enum. Failed conversions preserve existing bytes.
    try std.testing.expectError(error.InvalidRelationalIndexBound, tuple.appendValues(a, &wire, &.{.{ .string = "a" }}));
    try std.testing.expectEqual(prefix, wire.items.len);
    try std.testing.expectError(error.InvalidRelationalIndexBound, tuple.appendLiteralValues(a, &wire, &.{.{ .string = "A" }}));
    try std.testing.expectEqual(prefix, wire.items.len);
    var condition = try predicates.Plan.init(a, table, &layout, .{ .column = "state", .op = .lt, .value = .{ .string = "a" } });
    defer condition.deinit();
    var source = try condition.projectSource(a, table, &layout);
    defer source.deinit();
    var member = try partial.Plan.init(a, table, &layout, &.{.{ .field = "state", .op = .lt, .value_json = "\"a\"" }});
    defer member.deinit();
    var partial_source = try member.projectSource(a, table, &layout);
    defer partial_source.deinit();
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(a);
    const inputs = [_][]const u8{ "{\"state\":\"z\"}", "{\"state\":\"a\"}", "{\"state\":\"\"}", "{\"state\":null}", "{}" };
    const truths = [_]predicates.Truth{ .yes, .no, .no, .unknown, .unknown };
    for (inputs, truths) |input, truth| {
        const row_bytes = try encode(a, table, input);
        defer a.free(row_bytes);
        const row = try codec.ordinalRowView(row_bytes, table, &layout);
        try std.testing.expectEqual(truth, try source.evaluate(a, &scratch, row));
        try std.testing.expectEqual(truth == .yes, try partial_source.matches(a, &scratch, row));
    }
    try wire.ensureTotalCapacity(a, 128);
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const execution_alloc = denied.allocator();
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100_000) |_| {
        const atom = try tuple.resolveLiteral(0, .{ .string = "a" });
        wire.clearRetainingCapacity();
        _ = try tuple.appendLiteralValues(execution_alloc, &wire, &.{.{ .enumeration = atom.enumeration }});
    }
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
    std.debug.print("SQL enum JSON bound preparation: iterations=100000 elapsed_ns={} allocations={}\n", .{ std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started, denied.alloc_index });
}

test "SQL named enum text projection excludes row registries and remains stable across additive type epochs" {
    const a = std.testing.allocator;
    var labels = try types.Owned.create(a, &.{ "z", "a" });
    defer labels.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .named_types = &.{record}, .relational_columns = &.{.{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7 }} };
    const projected = try schema.serializeTextProjectionSchema(a, table);
    defer a.free(projected);
    const decoded = try schema.deserializeSchema(a, projected);
    defer schema.freeSchema(a, decoded);
    try std.testing.expectEqual(schema.StorageMode.document, decoded.storage_mode);
    try std.testing.expect(!decoded.requires_public_schema);
    try std.testing.expectEqual(@as(usize, 0), decoded.named_types.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.relational_columns.len);
    var plain = table;
    plain.named_types = &.{};
    plain.relational_columns = &.{};
    const ordinary = try schema.serializeTextProjectionSchema(a, plain);
    defer a.free(ordinary);
    try std.testing.expectEqualSlices(u8, ordinary, projected);
    var next = try types.Owned.add(a, labels.definition, "middle", .{ .before = "a" });
    defer next.deinit();
    var evolved_record = record;
    evolved_record.identity.generation += 1;
    evolved_record.definition = next.definition;
    var evolved = table;
    evolved.named_types = &.{evolved_record};
    const after = try schema.serializeTextProjectionSchema(a, evolved);
    defer a.free(after);
    try std.testing.expectEqualSlices(u8, projected, after);
}

test "SQL domain physical layouts preserve base bytes and retain only ancestor enum directories" {
    const a = std.testing.allocator;
    const domain: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .builtin = .text } } } };
    const column: schema.RelationalColumn = .{ .name = "state", .path = "state", .column_type = .string, .sql_element_type = .text, .named_type_id = 7, .allows_null = true };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &.{column}, .named_types = &.{domain} };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    try std.testing.expectEqual(@as(usize, 0), layout.enum_views.len);
    const cells = [_]codec.Cell{.{ .ordinal = 0, .path = "state", .value_type = .bytes_val, .value = .{ .bytes_val = "queued" } }};
    const bytes = try codec.serializePreparedOrdinalWithLayout(a, 1, table.relational_columns, &cells, @splat(0), &layout);
    defer a.free(bytes);
    var plain_column = column;
    plain_column.named_type_id = null;
    const plain_table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{plain_column} };
    var plain_layout = try codec.PhysicalLayout.init(a, plain_table);
    defer plain_layout.deinit();
    const plain_bytes = try codec.serializePreparedOrdinalWithLayout(a, 1, plain_table.relational_columns, &cells, @splat(0), &plain_layout);
    defer a.free(plain_bytes);
    try std.testing.expectEqualSlices(u8, plain_bytes, bytes);

    var labels = try types.Owned.create(a, &.{ "queued", "done" });
    defer labels.deinit();
    const enumeration: types.Record = .{ .identity = try types.Identity.init(9, 2, 1, @splat(4)), .owner = .auth_disabled, .definition = labels.definition };
    var enum_domain = domain;
    enum_domain.definition.domain.base = .{ .named = .{ .id = 9, .origin_id = enumeration.identity.origin_id, .generation = 1 } };
    const enum_column: schema.RelationalColumn = .{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 7, .allows_null = true };
    const enum_table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &.{enum_column}, .named_types = &.{ enum_domain, enumeration } };
    var enum_layout = try codec.PhysicalLayout.init(a, enum_table);
    defer enum_layout.deinit();
    try std.testing.expectEqual(@as(usize, 1), enum_layout.enum_views.len);
    try std.testing.expectEqual(@as(u64, 9), enum_layout.enumView(0).?.record.identity.id);
    var wrong_leaf = enum_column;
    wrong_leaf.named_type_id = enumeration.identity.id;
    try std.testing.expectError(error.InvalidRelationalRow, enum_layout.enumRecordForColumn(wrong_leaf, 0));
    for ([_]bool{ false, true }) |is_null| {
        const enum_cells = [_]codec.Cell{.{ .ordinal = 0, .path = "state", .value_type = .i64_val, .is_null = is_null, .value = .{ .i64_val = if (is_null) 0 else 1 }, .enum_record = &enum_layout.enumView(0).?.record }};
        const encoded = try codec.serializePreparedOrdinalWithLayout(a, 1, enum_table.relational_columns, &enum_cells, @splat(0), &enum_layout);
        defer a.free(encoded);
        for ([_]bool{ false, true }) |cached| {
            const text = if (cached) try codec.reconstructOrdinalValueWithLayoutAlloc(a, encoded, enum_table, &enum_layout) else try codec.reconstructOrdinalValueAlloc(a, encoded, enum_table);
            defer a.free(text);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
            defer parsed.deinit();
            const state = parsed.value.object.get("state").?;
            if (is_null) try std.testing.expect(state == .null) else try std.testing.expectEqualStrings("queued", state.string);
        }
        var forged_record = enumeration;
        forged_record.identity.origin_id = @splat(6);
        var forged_cells = enum_cells;
        forged_cells[0].enum_record = &forged_record;
        try std.testing.expectError(error.InvalidRelationalRow, codec.serializePreparedOrdinalWithLayout(a, 1, enum_table.relational_columns, &forged_cells, @splat(0), &enum_layout));
    }
    const projected = try @import("relational_index_cover.zig").Plan.init(a, enum_table, &enum_layout, &.{}, &.{"state"});
    var covering = projected;
    defer covering.deinit();
    try std.testing.expectEqual(@as(usize, 2), covering.named_types.len);
    try std.testing.expectEqual(@as(u64, 9), covering.layout.enumView(0).?.record.identity.id);
    var rebound = enum_domain;
    rebound.definition.domain.base.named.origin_id = @splat(6);
    var wrong = enum_table;
    wrong.named_types = &.{ rebound, enumeration };
    try std.testing.expectError(error.InvalidSchema, codec.PhysicalLayout.init(a, wrong));
}

test "SQL domain native schema epochs fence readers retain cold DAGs and bind owned SQL columns" {
    const a = std.testing.allocator;
    const records = [_]types.Record{
        .{ .identity = try types.Identity.init(7, 2, 1, @splat(3)), .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .builtin = .text }, .not_null = true, .checks = &.{.{ .name = "unblocked", .expression =
        \\{"op":"ne","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"blocked"}]}
        }} } } },
        .{ .identity = try types.Identity.init(9, 2, 1, @splat(4)), .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .named = .{ .id = 7, .origin_id = @splat(3), .generation = 1 } } } } },
    };
    const columns = [_]schema.RelationalColumn{.{ .name = "state", .path = "state", .column_type = .string, .sql_element_type = .text, .named_type_id = 9, .allows_null = true }};
    const table: schema.TableSchema = .{ .version = 4, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &columns, .named_types = &records };
    const bytes = try schema.serializeSchema(a, table);
    defer a.free(bytes);
    try std.testing.expectEqual(@as(u32, 41), schema.requiredReaderVersion(table));
    try std.testing.expect(!(schema.ReaderCapabilities{ .semantic_ceiling = 40, .encoding_ceiling = 40 }).supports(schema.readerRequirementsForSchema(table)));
    // Domain semantics remain at 41, while newly serialized schemas use the
    // current envelope even when they do not contain newer expressions.
    try std.testing.expect((schema.ReaderCapabilities{ .semantic_ceiling = 41, .encoding_ceiling = schema.storage_format_version }).supports(schema.readerRequirementsForSchema(table)));
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const bound = build: {
        const decoded = try schema.deserializeSchema(a, bytes);
        defer schema.freeSchema(a, decoded);
        const canonical = try schema.serializeSchema(a, decoded);
        defer a.free(canonical);
        try std.testing.expectEqualSlices(u8, bytes, canonical);
        break :build try @import("../../sql/catalog.zig").relationalColumnsLeaky(region.allocator(), decoded);
    };
    try std.testing.expectEqual(@as(u64, 9), bound[0].domain_view.?.identity.?.id);
    try std.testing.expectEqual(@as(u64, 7), bound[0].domain_view.?.parent.?.identity.?.id);
    try std.testing.expect(bound[0].enum_view == null);
    try std.testing.expectEqualStrings("state", bound[0].name);
    var budget: usize = 1024 * 1024;
    var execution = @import("../../schema/relational_expression.zig").Execution.init(region.allocator(), &budget);
    try std.testing.expect((try bound[0].domain_view.?.validate(&execution, .null, .assignment)) != null);
    var changed = records;
    changed[1].definition.domain.base.named.generation += 1;
    var wrong = table;
    wrong.named_types = &changed;
    try std.testing.expectError(error.InvalidSchema, schema.serializeSchema(a, wrong));
    var wrong_columns = columns;
    wrong_columns[0].sql_element_type = .int32;
    wrong = table;
    wrong.relational_columns = &wrong_columns;
    try std.testing.expectError(error.InvalidSchema, schema.serializeSchema(a, wrong));
    // A v40 envelope has no v42 string-length feature byte. Preserve its
    // actual framing so this probes forbidden domain records, not misalignment.
    const disguised = try a.alloc(u8, bytes.len - 1);
    defer a.free(disguised);
    var registry_bytes: usize = 4;
    for (records) |record| {
        const encoded = try record.encode(a);
        defer a.free(encoded);
        registry_bytes += 4 + encoded.len;
    }
    const flag_offset = bytes.len - registry_bytes - 1;
    try std.testing.expectEqual(@as(u8, 0), bytes[flag_offset]);
    @memcpy(disguised[0..flag_offset], bytes[0..flag_offset]);
    @memcpy(disguised[flag_offset..], bytes[flag_offset + 1 ..]);
    std.mem.writeInt(u32, disguised[4..8], 40, .little);
    try std.testing.expectError(error.UnsupportedVersion, schema.deserializeSchema(a, disguised));
}
