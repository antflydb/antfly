// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const transform = @import("relational_row_transform.zig");
const mapper = @import("document_mapper.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const alloc = std.testing.allocator;

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
    var apply = try transform.Program.init(alloc, source_schema, target_schema, .{ .defaults = .apply_to_absent });
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
    var filled = try transform.Program.init(alloc, source_schema, required, .{ .dropped_columns = .allow, .defaults = .apply_to_absent });
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
