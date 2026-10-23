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

const std = @import("std");
const transform = @import("relational_row_transform.zig");
const mapper = @import("document_mapper.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const alloc = std.testing.allocator;

fn usingRewrite(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    const ddl = @import("../../sql/schema_ddl.zig");
    var create = try compiler.compile(scratch, "CREATE TABLE changes(n bigint, label text, payload text)", .{});
    defer create.deinit();
    const before = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(scratch, create.statement.create_table);
    var candidate = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before, .{});
    var alter = try compiler.compile(scratch, "ALTER TABLE changes ALTER COLUMN n TYPE numeric USING n::numeric + 1", .{});
    defer alter.deinit();
    const rule = (try ddl.prepareColumnTransform(scratch, candidate, alter.statement.catalog_ddl, .{})).?;
    try std.testing.expect(try ddl.apply(scratch, &candidate, alter.statement.catalog_ddl));
    try candidate.object.put(scratch, "version", .{ .integer = 2 });
    const after = try std.json.Stringify.valueAlloc(scratch, candidate, .{});
    const policies: transform.Policies = .{ .expression_schema = before, .column_transforms = &.{rule} };
    var program = try transform.Program.init(a, before, after, policies);
    defer program.deinit();
    var repeated = try transform.Program.initWithTarget(a, before, &program);
    defer repeated.deinit();
    try std.testing.expect(program.transform_set.? == repeated.transform_set.?);
    try std.testing.expectEqualDeep(program.identity, repeated.identity);
    const wide: [16 * 1024]u8 = @splat('x');
    const document = try std.json.Stringify.valueAlloc(scratch, .{ .n = @as(i64, 9007199254740993), .label = "Unchanged", .payload = &wide }, .{});
    var input = try sourceRow(&program, a, document);
    defer input.deinit(a);
    try codec.setOrdinalWriteTimestampNs(input.packed_row, 123);
    var result = try program.transform(a, input.packed_row);
    defer result.deinit(a);
    var again = try repeated.transform(a, input.packed_row);
    defer again.deinit(a);
    try std.testing.expectEqualSlices(u8, result.packed_row, again.packed_row);
    try std.testing.expectEqual(@as(u64, 123), try codec.rowWriteTimestampNs(result.packed_row));
    try std.testing.expectEqual(@as(usize, 1), result.expression_input_cells);
    const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    var logical = try view.materializeRootAlloc(a);
    defer logical.deinit(a);
    const n = logical.root.object.get("n").?;
    try std.testing.expect(n == .number_string);
    try std.testing.expectEqualStrings("9007199254740994", n.number_string);
    try std.testing.expectEqualStrings("Unchanged", logical.root.object.get("label").?.string);
    try std.testing.expectEqualStrings(&wide, logical.root.object.get("payload").?.string);
    const contract = @import("relational_rewrite_contract.zig");
    const programs = @import("relational_rewrite_program.zig");
    var owned = try programs.ProgramSet.init(a, &.{before}, after, policies);
    defer owned.deinit();
    const intent: contract.Intent = .{ .version = 3, .source_schemas = &.{before}, .target_schema = after, .expression_schema = before, .column_transforms = &.{rule}, .program_digest = owned.identity };
    const bytes = try std.json.Stringify.valueAlloc(a, intent, .{});
    defer a.free(bytes);
    var decoded = try std.json.parseFromSlice(contract.Intent, a, bytes, .{ .allocate = .alloc_always });
    defer decoded.deinit();
    var reopened = try programs.ProgramSet.initIntent(a, decoded.value);
    defer reopened.deinit();
    try std.testing.expectEqualDeep(owned.identity, reopened.identity);
    var forged = intent;
    forged.column_transforms = &.{.{ .column = "n", .expression_json = "{\"op\":\"literal\",\"type\":\"numeric\",\"sql_type\":\"numeric\",\"value\":7}" }};
    if (programs.ProgramSet.initIntent(a, forged)) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |err| {
        if (err == error.OutOfMemory or err == error.WriteFailed) return err;
        try std.testing.expectEqual(error.RestoreStagingScopeChanged, err);
    }
}

fn renameRewrite(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    const ddl = @import("../../sql/schema_ddl.zig");
    var create = try compiler.compile(scratch, "CREATE TABLE changes(label text, upper_label text GENERATED ALWAYS AS (upper(label)) STORED,payload text)", .{});
    defer create.deinit();
    const before = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(scratch, create.statement.create_table);
    var candidate = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before, .{});
    var alter = try compiler.compile(scratch, "ALTER TABLE changes RENAME COLUMN label TO state", .{});
    defer alter.deinit();
    const rule = (try ddl.prepareColumnTransform(scratch, candidate, alter.statement.catalog_ddl, .{})).?;
    try std.testing.expect(try ddl.apply(scratch, &candidate, alter.statement.catalog_ddl));
    try candidate.object.put(scratch, "version", .{ .integer = 2 });
    const after = try std.json.Stringify.valueAlloc(scratch, candidate, .{});
    const policies: transform.Policies = .{ .dropped_columns = .allow, .expression_schema = before, .column_transforms = &.{rule} };
    var program = try transform.Program.init(a, before, after, policies);
    defer program.deinit();
    const wide: [16 * 1024]u8 = @splat('x');
    const document = try std.json.Stringify.valueAlloc(scratch, .{ .label = "MiXeD", .upper_label = "MIXED", .payload = &wide }, .{});
    var input = try sourceRow(&program, a, document);
    defer input.deinit(a);
    try codec.setOrdinalWriteTimestampNs(input.packed_row, 123);
    var output = try program.transform(a, input.packed_row);
    defer output.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), output.expression_input_cells);
    try std.testing.expectEqual(@as(u64, 123), try codec.rowWriteTimestampNs(output.packed_row));
    const view = try codec.ordinalRowView(output.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    var logical = try view.materializeRootAlloc(a);
    defer logical.deinit(a);
    try std.testing.expect(logical.root.object.get("label") == null);
    try std.testing.expectEqualStrings("MiXeD", logical.root.object.get("state").?.string);
    try std.testing.expectEqualStrings("MIXED", logical.root.object.get("upper_label").?.string);
    try std.testing.expectEqualStrings(&wide, logical.root.object.get("payload").?.string);
    const programs = @import("relational_rewrite_program.zig");
    const contract = @import("relational_rewrite_contract.zig");
    var owned = try programs.ProgramSet.init(a, &.{before}, after, policies);
    defer owned.deinit();
    const intent: contract.Intent = .{ .version = 3, .allow_column_drops = true, .source_schemas = &.{before}, .target_schema = after, .expression_schema = before, .column_transforms = &.{rule}, .program_digest = owned.identity };
    var reopened = try programs.ProgramSet.initIntent(a, intent);
    defer reopened.deinit();
    try std.testing.expectEqualDeep(owned.identity, reopened.identity);
    var replay = try reopened.programs[0].transform(a, input.packed_row);
    defer replay.deinit(a);
    try std.testing.expectEqualSlices(u8, output.packed_row, replay.packed_row);
}

test "SQL column rename resolves only its operand preserves packed payload and owns durable mapping across allocation faults" {
    try renameRewrite(alloc);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, renameRewrite, .{});
}

test "SQL USING rewrite binds typed operands once preserves unrelated cells and reconstructs exact durable intent" {
    try usingRewrite(alloc);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, usingRewrite, .{});
}

test "SQL USING rewrite shares predecessor binding across historical layouts with absent nullable operands" {
    const programs = @import("relational_rewrite_program.zig");
    const old = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const before = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"},\"label\":{\"type\":\"keyword\",\"nullable\":true}},\"additionalProperties\":false}}}}";
    const after = try std.mem.replaceOwned(u8, alloc, before, "\"version\":2", "\"version\":3");
    defer alloc.free(after);
    const policies: transform.Policies = .{
        .expression_schema = before,
        .column_transforms = &.{.{ .column = "label", .expression_json = "{\"op\":\"coalesce\",\"args\":[{\"op\":\"lower_ascii\",\"args\":[{\"op\":\"column\",\"column\":\"label\"}]},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"absent\"}]}" }},
    };
    var set = try programs.ProgramSet.init(alloc, &.{ before, old }, after, policies);
    defer set.deinit();
    var reversed = try programs.ProgramSet.init(alloc, &.{ old, before }, after, policies);
    defer reversed.deinit();
    try std.testing.expectEqualDeep(set.identity, reversed.identity);
    try std.testing.expect(set.programs[0].transform_set.? == set.programs[1].transform_set.?);
    for (set.programs, [_][]const u8{ "{\"n\":4}", "{\"n\":4,\"label\":\"MiXeD\"}" }, [_][]const u8{ "absent", "mixed" }) |*program, json, expected| {
        var row = try sourceRow(program, alloc, json);
        defer row.deinit(alloc);
        var result = try program.transform(alloc, row.packed_row);
        defer result.deinit(alloc);
        const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
        const cell = (try view.findCell(view.ordinalForName("label").?)).?;
        try std.testing.expect(!cell.is_null);
        try std.testing.expectEqualStrings(expected, cell.value.bytes_val);
    }
}

test "SQL USING rewrite text results preserve builtin identity and nullable schema union arms" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    const ddl = @import("../../sql/schema_ddl.zig");
    var create = try compiler.compile(a, "CREATE TABLE changes(label text)", .{});
    defer create.deinit();
    const typed = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(a, create.statement.create_table);
    const union_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"label\":{\"type\":[\"keyword\",\"null\"]}},\"additionalProperties\":false}}}}";
    for ([_][]const u8{ typed, union_schema }) |before| {
        var candidate = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{});
        var alter = try compiler.compile(a, "ALTER TABLE changes ALTER COLUMN label TYPE text USING lower(label)", .{});
        defer alter.deinit();
        const rule = (try ddl.prepareColumnTransform(a, candidate, alter.statement.catalog_ddl, .{})).?;
        try std.testing.expect(try ddl.apply(a, &candidate, alter.statement.catalog_ddl));
        try candidate.object.put(a, "version", .{ .integer = 2 });
        const after = try std.json.Stringify.valueAlloc(a, candidate, .{});
        var program = try transform.Program.init(alloc, before, after, .{ .expression_schema = before, .column_transforms = &.{rule} });
        defer program.deinit();
        for ([_][]const u8{ "{\"label\":\"MiXeD\"}", "{\"label\":null}" }, [_]?[]const u8{ "mixed", null }) |json, expected| {
            var row = try sourceRow(&program, alloc, json);
            defer row.deinit(alloc);
            var result = try program.transform(alloc, row.packed_row);
            defer result.deinit(alloc);
            const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
            const cell = (try view.findCell(view.ordinalForName("label").?)).?;
            if (expected) |text| {
                try std.testing.expect(!cell.is_null);
                try std.testing.expectEqualStrings(text, cell.value.bytes_val);
            } else try std.testing.expect(cell.is_null);
        }
    }
}

test "SQL USING rewrite mapped blob comparison uses the retained physical ordinal" {
    const source = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"blob\",\"nullable\":true}},\"additionalProperties\":false}}}}";
    const logical = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\",\"nullable\":true},\"b\":{\"type\":\"blob\",\"nullable\":true}},\"additionalProperties\":false}}}}";
    const target = "{\"version\":3,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\",\"nullable\":true},\"b\":{\"type\":\"blob\",\"nullable\":true},\"same\":{\"type\":\"boolean\",\"nullable\":true}},\"additionalProperties\":false}}}}";
    var program = try transform.Program.init(alloc, source, target, .{
        .expression_schema = logical,
        .column_transforms = &.{.{ .column = "same", .expression_json = "{\"op\":\"eq\",\"args\":[{\"op\":\"column\",\"column\":\"b\"},{\"op\":\"literal\",\"type\":\"blob\",\"value\":\"AAE=\"}]}" }},
    });
    defer program.deinit();
    for ([_][]const u8{ "{\"b\":\"AAE=\"}", "{\"b\":null}" }, [_]?bool{ true, null }) |json, expected| {
        var row = try sourceRow(&program, alloc, json);
        defer row.deinit(alloc);
        var result = try program.transform(alloc, row.packed_row);
        defer result.deinit(alloc);
        const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
        const same = (try view.findCell(view.ordinalForName("same").?)).?;
        if (expected) |value| {
            try std.testing.expect(!same.is_null);
            try std.testing.expectEqual(value, same.value.bool_val);
        } else try std.testing.expect(same.is_null);
    }
}

test "relational index system typed rewrite preserves JSON SQL null and blob operand identity" {
    const before =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"b":{"type":"blob","nullable":true},"payload":{"type":"json"}},"additionalProperties":false}}}}
    ;
    const after =
        \\{"version":2,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"coalesce","args":[{"op":"column","column":"b"},{"op":"literal","type":"blob","value":"eA=="}]}},{"column":"same","expression":{"op":"eq","args":[{"op":"column","column":"b"},{"op":"literal","type":"blob","value":"AAE="}]}},{"column":"j","expression":{"op":"literal","type":"json","value":null}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"b":{"type":"blob","nullable":true},"g":{"type":"blob"},"same":{"type":"boolean","nullable":true},"j":{"type":"json","nullable":true},"payload":{"type":"json"}},"additionalProperties":false}}}}
    ;
    var program = try transform.Program.init(alloc, before, after, .{});
    defer program.deinit();
    const Run = struct {
        fn run(a: std.mem.Allocator, owner: *const transform.Program, packed_row: []const u8, expected_blob: []const u8, expected_same: ?bool) !void {
            var result = try owner.transform(a, packed_row);
            defer result.deinit(a);
            const view = try codec.ordinalRowView(result.packed_row, owner.target.tableSchema().*, owner.target.physicalLayout());
            const columns = owner.target.tableSchema().relational_columns;
            const g = (try view.findCell(owner.target.physicalLayout().ordinalForName(columns, "g").?)).?;
            const same = (try view.findCell(owner.target.physicalLayout().ordinalForName(columns, "same").?)).?;
            const j = (try view.findCell(owner.target.physicalLayout().ordinalForName(columns, "j").?)).?;
            const payload = (try view.findCell(owner.target.physicalLayout().ordinalForName(columns, "payload").?)).?;
            try std.testing.expectEqualStrings(expected_blob, g.value.bytes_val);
            if (expected_same) |boolean| {
                try std.testing.expect(!same.is_null);
                try std.testing.expectEqual(boolean, same.value.bool_val);
            } else try std.testing.expect(same.is_null);
            try std.testing.expect(j.is_json and j.is_null);
            try std.testing.expectEqualStrings("{\"scale\":1.20}", payload.value.bytes_val);
            try std.testing.expectEqual(@as(usize, 1), result.expression_input_cells);
        }
    };
    for ([_][]const u8{ "{\"b\":\"AAE=\",\"payload\":{\"scale\":1.20}}", "{\"b\":null,\"payload\":{\"scale\":1.20}}" }, [_][]const u8{ "AAE=", "eA==" }, [_]?bool{ true, null }) |json, expected, same| {
        var source = try sourceRow(&program, alloc, json);
        defer source.deinit(alloc);
        try Run.run(alloc, &program, source.packed_row, expected, same);
        var stable = std.testing.FailingAllocator.init(alloc, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ &program, source.packed_row, expected, same });
    }
}

test "SQL expression DDL rewrite computes stored generated columns and admitted defaults from base cells" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    var create = try compiler.compile(a, "CREATE TABLE exprs (n smallint, label text)", .{});
    defer create.deinit();
    const before = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(a, create.statement.create_table);
    var candidate = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{});
    for ([_][]const u8{
        "ALTER TABLE exprs ADD COLUMN g integer GENERATED ALWAYS AS (CASE WHEN n IS NULL THEN 0 ELSE CAST(n AS integer)+1 END) STORED NOT NULL",
        "ALTER TABLE exprs ADD COLUMN extra integer DEFAULT (4*5) NOT NULL",
    }) |sql| {
        var added = try compiler.compile(a, sql, .{});
        defer added.deinit();
        try std.testing.expect(try @import("../../sql/schema_ddl.zig").apply(a, &candidate, added.statement.catalog_ddl));
    }
    const after = try std.json.Stringify.valueAlloc(a, candidate, .{});
    var program = try transform.Program.init(alloc, before, after, .{ .default_columns = &.{"extra"} });
    defer program.deinit();
    for ([_][]const u8{ "{\"n\":3,\"label\":\"cold\"}", "{\"n\":null,\"label\":\"cold\"}" }, [_]i64{ 4, 0 }) |input, expected| {
        var source = try sourceRow(&program, alloc, input);
        defer source.deinit(alloc);
        var result = try program.transform(alloc, source.packed_row);
        defer result.deinit(alloc);
        const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
        const columns = program.target.tableSchema().relational_columns;
        const generated = (try view.findCell(program.target.physicalLayout().ordinalForName(columns, "g").?)).?;
        const defaulted = (try view.findCell(program.target.physicalLayout().ordinalForName(columns, "extra").?)).?;
        const cold = (try view.findCell(program.target.physicalLayout().ordinalForName(columns, "label").?)).?;
        try std.testing.expectEqual(expected, generated.value.i64_val);
        try std.testing.expectEqual(@as(i64, 20), defaulted.value.i64_val);
        try std.testing.expectEqualStrings("cold", cold.value.bytes_val);
        try std.testing.expectEqual(@as(usize, 1), result.expression_input_cells);
    }
}

test "SQL primary-key rewrite retains a present key across nullable-to-required row mapping" {
    const compiler = @import("../../sql/compiler.zig");
    var create = try compiler.compile(alloc, "CREATE TABLE pk_good (id BIGINT, note TEXT)", .{});
    defer create.deinit();
    const before = try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(alloc, create.statement.create_table);
    defer alloc.free(before);
    var alter = try compiler.compile(alloc, "ALTER TABLE pk_good ADD CONSTRAINT pk_good_key PRIMARY KEY (id)", .{});
    defer alter.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var candidate = try std.json.parseFromSlice(std.json.Value, a, before, .{ .parse_numbers = false });
    defer candidate.deinit();
    try std.testing.expect(try @import("../../sql/schema_ddl.zig").apply(a, &candidate.value, alter.statement.catalog_ddl));
    try candidate.value.object.put(a, "version", .{ .integer = 1 });
    const after = try std.json.Stringify.valueAlloc(alloc, candidate.value, .{});
    defer alloc.free(after);
    var program = try transform.Program.init(alloc, before, after, .{});
    defer program.deinit();
    try std.testing.expectEqual(@as(?usize, 0), program.source.physicalLayout().ordinalForName(program.source.tableSchema().relational_columns, "id"));
    try std.testing.expectEqual(@as(?usize, 0), program.target.physicalLayout().ordinalForName(program.target.tableSchema().relational_columns, "id"));
    var source = try mapper.PreparedRelationalWrite.initFromIntent(alloc, "row-a", "{\"id\":1,\"note\":\"first\"}", program.source.validator(), program.source.tableSchema().*, program.source.physicalLayout(), null);
    defer source.deinit(alloc);
    try source.finalizeMetadata(123);
    const source_view = try codec.ordinalRowView(source.packed_row, program.source.tableSchema().*, program.source.physicalLayout());
    const source_id = (try source_view.findCell(0)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!source_id.is_null);
    var result = try program.transform(alloc, source.packed_row);
    defer result.deinit(alloc);
    const target_view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    const target_id = (try target_view.findCell(0)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!target_id.is_null);
    var null_source = try mapper.PreparedRelationalWrite.initFromIntent(alloc, "row-null", "{\"id\":null,\"note\":\"missing\"}", program.source.validator(), program.source.tableSchema().*, program.source.physicalLayout(), null);
    defer null_source.deinit(alloc);
    try null_source.finalizeMetadata(123);
    try std.testing.expectError(error.InvalidRelationalRow, program.transform(alloc, null_source.packed_row));
}

const source_schema =
    \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"n","expression":{"op":"literal","type":"integer","value":2}}],"generated_columns":[{"column":"g","expression":{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":1}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"n":{"type":["integer","null"]},"g":{"type":"integer"},"wide":{"type":"string"}},"required":["x","g"],"additionalProperties":false}}}}
;
const target_schema =
    \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"n","expression":{"op":"literal","type":"integer","value":7}}],"generated_columns":[{"column":"g","expression":{"op":"multiply","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":3}]}},{"column":"h","expression":{"op":"add","args":[{"op":"column","column":"g"},{"op":"literal","type":"integer","value":1}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"h":{"type":"integer"},"x":{"type":"integer"},"n":{"type":["integer","null"]},"g":{"type":"integer"},"wide":{"type":"string"}},"required":["x","g","h"],"additionalProperties":false}}}}
;

fn sourceRow(program: *const transform.Program, allocator: std.mem.Allocator, json: []const u8) !mapper.PreparedRelationalWrite {
    var row = try mapper.PreparedRelationalWrite.initPreserved(allocator, "row", json, program.source.validator(), program.source.tableSchema().*, program.source.physicalLayout());
    errdefer row.deinit(allocator);
    try row.finalizeMetadata(0);
    return row;
}

test "relational index system rewrite transforms independent same-version schemas and preserves historical absence NULL timestamps" {
    var preserve = try transform.Program.init(alloc, source_schema, target_schema, .{});
    defer preserve.deinit();
    var apply = try transform.Program.init(alloc, source_schema, target_schema, .{ .default_columns = &.{"n"} });
    defer apply.deinit();
    try std.testing.expect(!std.mem.eql(u8, &preserve.identity, &apply.identity));
    try std.testing.expect(!std.mem.eql(u8, &preserve.source_schema_digest, &preserve.target_schema_digest));
    var source = try sourceRow(&preserve, alloc, "{\"x\":4,\"g\":5}");
    defer source.deinit(alloc);
    try codec.setOrdinalWriteTimestampNs(source.packed_row, 123456);
    var result = try preserve.transform(alloc, source.packed_row);
    defer result.deinit(alloc);
    var repeated = try preserve.transform(alloc, source.packed_row);
    defer repeated.deinit(alloc);
    try std.testing.expectEqualSlices(u8, result.packed_row, repeated.packed_row);
    try std.testing.expectEqualSlices(u8, &source.semantic_hash, &result.source_semantic_hash);
    try std.testing.expectEqual(@as(u64, 123456), try codec.rowWriteTimestampNs(result.packed_row));
    const row = try codec.ordinalRowView(result.packed_row, preserve.target.tableSchema().*, preserve.target.physicalLayout());
    try std.testing.expectEqual(@as(i64, 12), (try row.findCell(row.ordinalForName("g").?)).?.value.i64_val);
    try std.testing.expectEqual(@as(i64, 13), (try row.findCell(row.ordinalForName("h").?)).?.value.i64_val);
    try std.testing.expectEqual(@as(?codec.Cell, null), try row.findCell(row.ordinalForName("n").?));
    var filled = try apply.transform(alloc, source.packed_row);
    defer filled.deinit(alloc);
    const filled_row = try codec.ordinalRowView(filled.packed_row, apply.target.tableSchema().*, apply.target.physicalLayout());
    try std.testing.expectEqual(@as(i64, 7), (try filled_row.findCell(filled_row.ordinalForName("n").?)).?.value.i64_val);
    var explicit_null = try sourceRow(&apply, alloc, "{\"x\":4,\"g\":5,\"n\":null}");
    defer explicit_null.deinit(alloc);
    var null_result = try apply.transform(alloc, explicit_null.packed_row);
    defer null_result.deinit(alloc);
    const null_row = try codec.ordinalRowView(null_result.packed_row, apply.target.tableSchema().*, apply.target.physicalLayout());
    try std.testing.expect((try null_row.findCell(null_row.ordinalForName("n").?)).?.is_null);
}

test "relational index system rewrite validates forged historical generated values and semantic hash before recomputing" {
    var program = try transform.Program.init(alloc, source_schema, target_schema, .{});
    defer program.deinit();
    // Encode physically valid but logically forged g without the public writer.
    const forged_json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":4,\"g\":999}", .{ .parse_numbers = false });
    defer forged_json.deinit();
    const forged = try mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, forged_json.value, program.source.tableSchema().*);
    defer alloc.free(forged);
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, program.transform(alloc, forged));
    var valid = try sourceRow(&program, alloc, "{\"x\":4,\"g\":5}");
    defer valid.deinit(alloc);
    const wrong_hash = try alloc.dupe(u8, valid.packed_row);
    defer alloc.free(wrong_hash);
    try codec.setOrdinalSemanticHash(wrong_hash, @splat(0x5a));
    try std.testing.expectError(error.RelationalRowSemanticHashMismatch, program.transform(alloc, wrong_hash));
    // Ordinary restore remains preservation-only, never target computation.
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, mapper.PreparedRelationalWrite.initPreserved(alloc, "row", "{\"x\":4,\"g\":5}", program.target.validator(), program.target.tableSchema().*, program.target.physicalLayout()));
}

const only_x =
    \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"],"additionalProperties":false}}}}
;

test "relational index system rewrite drop policy is explicit and target shape failures are atomic" {
    try std.testing.expectError(error.RelationalRewriteColumnDrop, transform.Program.init(alloc, source_schema, only_x, .{}));
    var dropped = try transform.Program.init(alloc, source_schema, only_x, .{ .dropped_columns = .allow });
    defer dropped.deinit();
    var source = try sourceRow(&dropped, alloc, "{\"x\":4,\"g\":5}");
    defer source.deinit(alloc);
    var result = try dropped.transform(alloc, source.packed_row);
    defer result.deinit(alloc);
    const row = try codec.ordinalRowView(result.packed_row, dropped.target.tableSchema().*, dropped.target.physicalLayout());
    try std.testing.expectEqual(@as(i64, 4), (try row.findCell(0)).?.value.i64_val);
    const type_change =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"number"}},"additionalProperties":false}}}}
    ;
    try std.testing.expectError(error.RelationalRewriteTypeChange, transform.Program.init(alloc, source_schema, type_change, .{ .dropped_columns = .allow }));
}

test "relational index system rewrite projection avoids wide unrelated payload evaluation" {
    var program = try transform.Program.init(alloc, source_schema, target_schema, .{});
    defer program.deinit();
    try std.testing.expect(!program.source.validator().?.restore.full_root);
    try std.testing.expect(!program.target.validator().?.restore.full_root);
    const wide = try alloc.alloc(u8, 512 * 1024);
    defer alloc.free(wide);
    @memset(wide, 'w');
    const document = try std.fmt.allocPrint(alloc, "{{\"x\":4,\"g\":5,\"wide\":\"{s}\"}}", .{wide});
    defer alloc.free(document);
    var source = try sourceRow(&program, alloc, document);
    defer source.deinit(alloc);
    var result = try program.transform(alloc, source.packed_row);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.expression_input_cells);
    const row = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    try std.testing.expectEqualSlices(u8, wide, (try row.findCell(row.ordinalForName("wide").?)).?.value.bytes_val);
}

test "relational index system rewrite preserves NOT VALID history while checking required predicates" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const deferred =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"deferred","column":"n","op":"gte","value":0,"not_valid":true}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"k":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var raw = try std.json.parseFromSliceLeaky(std.json.Value, a, deferred, .{});
    _ = raw.object.orderedRemove("checks");
    const unchecked_json = try std.json.Stringify.valueAlloc(a, raw, .{});
    var unchecked = try transform.Program.init(alloc, unchecked_json, unchecked_json, .{});
    defer unchecked.deinit();
    // This row predates the NOT VALID declaration; public writes cannot create it.
    var old = try sourceRow(&unchecked, alloc, "{\"n\":-1,\"k\":2}");
    defer old.deinit(alloc);
    var mixed = try std.json.parseFromSliceLeaky(std.json.Value, a, deferred, .{});
    mixed.object.getPtr("version").?.* = .{ .integer = 2 };
    try mixed.object.getPtr("checks").?.array.append(try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"required\",\"column\":\"k\",\"op\":\"gte\",\"value\":0}", .{}));
    const mixed_json = try std.json.Stringify.valueAlloc(a, mixed, .{});
    var program = try transform.Program.init(alloc, deferred, mixed_json, .{});
    defer program.deinit();
    var result = try program.transform(alloc, old.packed_row);
    defer result.deinit(alloc);
    const view = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    try std.testing.expectEqual(@as(i64, -1), (try view.findCell(view.ordinalForName("n").?)).?.value.i64_val);
    var logical = try codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(alloc, result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
    defer logical.deinit(alloc);
    try program.target.validator().?.validateTypedStoredRoot(alloc, &logical.root, view);
    try std.testing.expectError(error.RelationalCheckViolation, program.target.validator().?.validateValue(alloc, &logical.root));
    var damaged = try sourceRow(&unchecked, alloc, "{\"n\":-1,\"k\":-2}");
    defer damaged.deinit(alloc);
    try std.testing.expectError(error.RelationalCheckViolation, program.transform(alloc, damaged.packed_row));
    // Explicitly validating the deferred rule must reject its old violation.
    mixed.object.getPtr("checks").?.array.items[0].object.getPtr("not_valid").?.* = .{ .bool = false };
    var validated = try transform.Program.init(alloc, deferred, try std.json.Stringify.valueAlloc(a, mixed, .{}), .{});
    defer validated.deinit();
    try std.testing.expectError(error.RelationalCheckViolation, validated.transform(alloc, old.packed_row));
}

test "relational index system rewrite target overflow CHECK and required failures publish no result" {
    var overflow = try transform.Program.init(alloc, source_schema, target_schema, .{});
    defer overflow.deinit();
    var large = try sourceRow(&overflow, alloc, "{\"x\":4611686018427387903,\"g\":4611686018427387904}");
    defer large.deinit(alloc);
    try std.testing.expectError(error.RelationalExpressionOverflow, overflow.transform(alloc, large.packed_row));
    const checked =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"small","column":"x","op":"lt","value":3}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"],"additionalProperties":false}}}}
    ;
    var check = try transform.Program.init(alloc, source_schema, checked, .{ .dropped_columns = .allow });
    defer check.deinit();
    var source = try sourceRow(&check, alloc, "{\"x\":4,\"g\":5}");
    defer source.deinit(alloc);
    try std.testing.expectError(error.RelationalCheckViolation, check.transform(alloc, source.packed_row));
    const required =
        \\{"version":2,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"n","expression":{"op":"literal","type":"integer","value":7}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"n":{"type":"integer"}},"required":["x","n"],"additionalProperties":false}}}}
    ;
    var missing = try transform.Program.init(alloc, source_schema, required, .{ .dropped_columns = .allow });
    defer missing.deinit();
    try std.testing.expectError(error.InvalidRelationalRow, missing.transform(alloc, source.packed_row));
    var filled = try transform.Program.init(alloc, source_schema, required, .{ .dropped_columns = .allow, .default_columns = &.{"n"} });
    defer filled.deinit();
    var result = try filled.transform(alloc, source.packed_row);
    defer result.deinit(alloc);
    // The source buffer remains valid and untouched after every failed attempt.
    var again = try overflow.transform(alloc, source.packed_row);
    defer again.deinit(alloc);
}

test "relational index system rewrite binds schema identity and enforces bounded input before parsing" {
    var a = try transform.Program.init(alloc, source_schema, target_schema, .{});
    defer a.deinit();
    const formatted = try std.fmt.allocPrint(alloc, "\n {s}\n", .{target_schema});
    defer alloc.free(formatted);
    var b = try transform.Program.init(alloc, source_schema, formatted, .{});
    defer b.deinit();
    try std.testing.expectEqualSlices(u8, &a.identity, &b.identity);
    var reverse = try transform.Program.init(alloc, target_schema, source_schema, .{ .dropped_columns = .allow });
    defer reverse.deinit();
    try std.testing.expect(!std.mem.eql(u8, &a.identity, &reverse.identity));
    const oversized = try alloc.alloc(u8, transform.max_row_bytes + 1);
    defer alloc.free(oversized);
    try std.testing.expectError(error.RelationalRewriteBudgetExceeded, a.transform(alloc, oversized));
    try std.testing.expectError(error.RelationalRewriteBudgetExceeded, transform.Program.init(alloc, oversized, source_schema, .{}));
    try std.testing.expectError(error.RelationalRewriteRequiresRelational, transform.Program.init(alloc, "{\"version\":1}", source_schema, .{}));
}

fn exerciseAllocations(allocator: std.mem.Allocator) !void {
    var program = try transform.Program.init(allocator, source_schema, target_schema, .{});
    defer program.deinit();
    var source = try sourceRow(&program, allocator, "{\"x\":4,\"g\":5}");
    defer source.deinit(allocator);
    var result = try program.transform(allocator, source.packed_row);
    defer result.deinit(allocator);
}

test "relational index system rewrite retains full-root source and target validation when projection is insufficient" {
    const root_limited =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}},"minProperties":2,"additionalProperties":false}}}}
    ;
    const unrestricted =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var source_guard = try transform.Program.init(alloc, root_limited, unrestricted, .{});
    defer source_guard.deinit();
    try std.testing.expect(source_guard.source.validator().?.restore.full_root);
    const malformed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":1}", .{});
    defer malformed.deinit();
    const encoded = try mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, malformed.value, source_guard.source.tableSchema().*);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, source_guard.transform(alloc, encoded));
    var target_guard = try transform.Program.init(alloc, unrestricted, root_limited, .{});
    defer target_guard.deinit();
    try std.testing.expect(target_guard.target.validator().?.restore.full_root);
    try std.testing.expectError(error.InvalidBatchRequest, target_guard.transform(alloc, encoded));
    var valid = try sourceRow(&source_guard, alloc, "{\"x\":1,\"y\":2}");
    defer valid.deinit(alloc);
    var result = try source_guard.transform(alloc, valid.packed_row);
    defer result.deinit(alloc);
}

test "relational index system rewrite allocation failures release both immutable epochs and all row buffers" {
    try std.testing.checkAllAllocationFailures(alloc, exerciseAllocations, .{});
}

// Rows from BOTH layouts predate the added column. The oldest also predates
// an unrelated column whose subsequently changed DEFAULT must not backfill.
test "SQL scoped defaults preserve older absent columns across historical layouts" {
    const Harness = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const oldest = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
            const latest = "{\"version\":3,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"column_defaults\":[{\"column\":\"old_col\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":7}},{\"column\":\"new_col\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":9}}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"},\"old_col\":{\"type\":[\"integer\",\"null\"]},\"new_col\":{\"type\":[\"integer\",\"null\"]}},\"additionalProperties\":false}}}}";
            const newer = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"column_defaults\":[{\"column\":\"old_col\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":7}}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"},\"old_col\":{\"type\":[\"integer\",\"null\"]}},\"additionalProperties\":false}}}}";
            var template = try transform.Program.init(allocator, oldest, latest, .{ .default_columns = &.{"new_col"} });
            var template_open = true;
            defer if (template_open) template.deinit();
            var program = try transform.Program.initWithTarget(allocator, oldest, &template);
            defer program.deinit();
            template.deinit();
            template_open = false;
            var source = try sourceRow(&program, allocator, "{\"x\":1}");
            defer source.deinit(allocator);
            var result = try program.transform(allocator, source.packed_row);
            defer result.deinit(allocator);
            const row = try codec.ordinalRowView(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
            try std.testing.expect(try row.findCell(row.ordinalForName("old_col").?) == null);
            try std.testing.expectEqual(@as(i64, 9), (try row.findCell(row.ordinalForName("new_col").?)).?.value.i64_val);
            var unscoped = try transform.Program.init(allocator, oldest, latest, .{});
            defer unscoped.deinit();
            try std.testing.expect(!std.mem.eql(u8, &program.identity, &unscoped.identity));
            var programs = try @import("relational_rewrite_program.zig").ProgramSet.init(allocator, &.{ oldest, newer }, latest, .{ .default_columns = &.{"new_col"} });
            defer programs.deinit();
            const intent: @import("relational_rewrite_contract.zig").Intent = .{ .source_schemas = &.{ oldest, newer }, .target_schema = latest, .default_columns = &.{"new_col"}, .program_digest = programs.identity };
            const bytes = try std.json.Stringify.valueAlloc(allocator, intent, .{});
            defer allocator.free(bytes);
            var recovered = try std.json.parseFromSlice(@import("relational_rewrite_contract.zig").Intent, allocator, bytes, .{ .allocate = .alloc_always });
            defer recovered.deinit();
            try recovered.value.validate();
            var restored = try transform.Program.init(allocator, oldest, latest, .{ .default_columns = recovered.value.default_columns });
            defer restored.deinit();
            try std.testing.expectEqualSlices(u8, &program.identity, &restored.identity);
            var restored_set = try @import("relational_rewrite_program.zig").ProgramSet.initIntent(allocator, recovered.value);
            defer restored_set.deinit();
            try std.testing.expectEqualSlices(u8, &programs.identity, &restored_set.identity);
            const recent_program = &restored_set.programs[1];
            for ([_][]const u8{ "{\"x\":2}", "{\"x\":2,\"old_col\":null}" }) |document| {
                var recent = try sourceRow(recent_program, allocator, document);
                defer recent.deinit(allocator);
                var rewritten = try recent_program.transform(allocator, recent.packed_row);
                defer rewritten.deinit(allocator);
                const recent_row = try codec.ordinalRowView(rewritten.packed_row, recent_program.target.tableSchema().*, recent_program.target.physicalLayout());
                if (try recent_row.findCell(recent_row.ordinalForName("old_col").?)) |cell| try std.testing.expect(cell.is_null);
                try std.testing.expectEqual(@as(i64, 9), (try recent_row.findCell(recent_row.ordinalForName("new_col").?)).?.value.i64_val);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Harness.run, .{});
}

test "SQL scoped defaults reject unknown duplicate and nondefault columns" {
    try std.testing.expectError(error.InvalidRestoreStagingCommand, transform.Program.init(alloc, source_schema, target_schema, .{ .default_columns = &.{"missing"} }));
    try std.testing.expectError(error.InvalidRestoreStagingCommand, transform.Program.init(alloc, source_schema, target_schema, .{ .default_columns = &.{ "n", "n" } }));
    try std.testing.expectError(error.InvalidRestoreStagingCommand, transform.Program.init(alloc, source_schema, target_schema, .{ .default_columns = &.{"x"} }));
}
