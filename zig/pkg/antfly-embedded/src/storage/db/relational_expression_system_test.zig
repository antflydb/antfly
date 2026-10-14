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
const db_mod = @import("mod.zig");
const rows = @import("relational_rows.zig");
const mapper = @import("document_mapper.zig");
const alloc = std.testing.allocator;

test "relational index system cold generated JSON verification preserves SQL null and shared ownership" {
    const a = std.testing.allocator;
    const expressions = @import("../../schema/relational_expression.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const Execution = expressions.Execution;
    const Set = expressions.Set;
    const Allocator = std.mem.Allocator;
    const max_allocated_bytes = expressions.max_allocated_bytes;
    var validator = try @import("../../schema/mod.zig").CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"literal","type":"json","value":null}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"g":{"type":"json","nullable":true}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    const set = validator.execution.expressions.?;
    var layout = try codec.PhysicalLayout.init(a, set.table);
    defer layout.deinit();
    const ordinal: u32 = @intCast(layout.ordinalForName(set.table.relational_columns, "g").?);
    const sql_null = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &.{
        .{ .ordinal = ordinal, .path = "g", .value_type = .bytes_val, .is_json = true, .is_null = true, .value = .{ .bytes_val = "" } },
    }, @splat(0));
    defer a.free(sql_null);
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(a, &bytes);
    execution.numeric.max_input_bytes = 0;
    // No JSON decoding is required for SQL NULL; a present JSON null is not
    // interchangeable with this generated result.
    try set.verifyRowWithExecution(&execution, try codec.ordinalRowView(sql_null, set.table, &layout));
    const wide: [1024]u8 = @splat('x');
    const text = try std.fmt.allocPrint(a, "\"{s}\"", .{wide});
    defer a.free(text);
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn poll(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.Canceled;
        }
    };
    for ([_][]const u8{ "null", text }) |json| {
        const physical = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &.{
            .{ .ordinal = ordinal, .path = "g", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = json } },
        }, @splat(0));
        defer a.free(physical);
        const row = try codec.ordinalRowView(physical, set.table, &layout);
        bytes = max_allocated_bytes;
        execution = Execution.init(a, &bytes);
        var observed: Control = .{};
        execution.numeric.checkpoint = Control.poll;
        execution.numeric.ptr = &observed;
        try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRowWithExecution(&execution, row));
        for (1..observed.polls + 1) |cut| {
            bytes = max_allocated_bytes;
            execution = Execution.init(a, &bytes);
            var canceled: Control = .{ .fail_at = cut };
            execution.numeric.checkpoint = Control.poll;
            execution.numeric.ptr = &canceled;
            try std.testing.expectError(error.Canceled, set.verifyRowWithExecution(&execution, row));
            execution.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, set.verifyRowWithExecution(&execution, row));
        }
        const Run = struct {
            fn run(fault_alloc: Allocator, owner: *const Set, input: codec.OrdinalRowView) !void {
                owner.verifyRow(fault_alloc, input) catch |err| {
                    if (err == error.InvalidRelationalGeneratedValue) return;
                    return err;
                };
                return error.TestExpectedError;
            }
        };
        try Run.run(a, set, row);
        var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ set, row });
    }
}

test "relational index system public JSON generated expressions retain typed values and null authority" {
    const a = std.testing.allocator;
    const api = @import("../../schema/mod.zig");
    for ([_][]const u8{
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":{\"n\":1.20}}",
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":null,\"json_null\":true}",
        "{\"op\":\"column\",\"column\":\"source\"}",
    }, 0..) |expression, i| {
        const definition = try std.fmt.allocPrint(
            a,
            "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"generated_columns\":[{{\"column\":\"g\",\"expression\":{s}}}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"g\":{{\"type\":\"json\",\"nullable\":true}},\"source\":{{\"type\":\"json\",\"nullable\":true}}}},\"additionalProperties\":false}}}}}}}}",
            .{expression},
        );
        defer a.free(definition);
        var validator = try api.CompiledTableValidator.init(a, definition);
        defer validator.deinit(a);
        const runtime = try api.deriveRuntimeTableSchema(a, validator.schema);
        defer @import("../schema.zig").freeSchema(a, runtime);
        try std.testing.expect(runtime.requires_json_expressions);
        var document = try std.json.parseFromSlice(std.json.Value, a, "{\"source\":{\"n\":1.20}}", .{ .parse_numbers = false });
        defer document.deinit();
        const prepared = try validator.prepareTypedValueWithNulls(document.arena.allocator(), a, &document.value, &.{}, false);
        defer prepared.deinit(document.arena.allocator());
        if (i == 1) {
            try std.testing.expectEqual(std.json.Value.null, document.value.object.get("g").?);
            try std.testing.expectEqual(@as(usize, 1), prepared.fields.len);
            try std.testing.expectEqualStrings("g", prepared.fields[0]);
        } else {
            try std.testing.expectEqualStrings("1.20", document.value.object.get("g").?.object.get("n").?.number_string);
            try std.testing.expectEqual(@as(usize, 0), prepared.fields.len);
        }
        try validator.prepareTypedValue(document.arena.allocator(), a, &document.value, prepared.fields, true);
    }
}

test "relational index system typed document expression preparation retains authoritative root null provenance" {
    const a = std.testing.allocator;
    const expressions = @import("../../schema/relational_expression.zig");
    const operands = @import("../../sql/row_value.zig");
    var validator = try @import("../../schema/mod.zig").CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"literal","type":"json","value":null}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"g":{"type":"json","nullable":true},"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    const set = validator.execution.expressions.?;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var document = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"g\":null,\"payload\":null}", .{});
    var bytes: usize = expressions.max_allocated_bytes;
    var execution = expressions.Execution.init(region.allocator(), &bytes);
    var roots: operands.JsonRow = undefined;
    roots.initRegion(region.allocator(), bytes, &execution.numeric);
    defer roots.deinit();
    try roots.bindAdmission(&bytes);
    var nulls: operands.JsonNulls = undefined;
    nulls.initRegion(region.allocator(), bytes, &execution.numeric);
    defer nulls.deinit();
    try nulls.bindAdmission(&bytes);
    // Both source markers are legitimate root JSON datums. The generated SQL
    // NULL result replaces g's submitted semantics; unrelated payload survives.
    try nulls.import(&.{ "g", "payload" });
    try set.applyTypedJsonWithExecution(&execution, &document, &roots, &nulls);
    try std.testing.expect(!try nulls.contains("g"));
    try std.testing.expect(try nulls.contains("payload"));
    try std.testing.expectEqual(@as(usize, 1), (try nulls.snapshot()).len);
    try std.testing.expectEqual(std.json.Value.null, document.object.get("g").?);
    try std.testing.expectEqual(@as(usize, 0), roots.memory.footprint());
    try std.testing.expectEqual(@as(usize, 0), roots.decodes);
    var other_bytes: usize = expressions.max_allocated_bytes;
    var other = expressions.Execution.init(region.allocator(), &other_bytes);
    try std.testing.expectError(error.InvalidRelationalExpressionInput, set.applyTypedJsonWithExecution(&other, &document, &roots, &nulls));
    try std.testing.expect(try nulls.contains("payload"));
}

test "relational index system typed preparation encodes derived null authority and rejects forged restore" {
    const a = std.testing.allocator;
    const schema_api = @import("../../schema/mod.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    var validator = try schema_api.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"literal","type":"json","value":null}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"g":{"type":"json","nullable":true},"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    const table = try schema_api.deriveRuntimeTableSchema(a, validator.schema);
    defer @import("../schema.zig").freeSchema(a, table);
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const Run = struct {
        fn run(fault_alloc: std.mem.Allocator, compiled: schema_api.CompiledTableValidator, runtime: @import("../schema.zig").TableSchema, physical: *const codec.PhysicalLayout) !void {
            var prepared = try mapper.PreparedRelationalWrite.initTyped(fault_alloc, fault_alloc, fault_alloc, false, "r", "{\"g\":null,\"payload\":null}", compiled, runtime, physical, &.{ "g", "payload" }, false);
            defer prepared.deinit(fault_alloc);
            try prepared.finalizeMetadata(0);
            const row = try codec.ordinalRowView(prepared.packed_row, runtime, physical);
            const g = (try row.findCell(@intCast(physical.ordinalForName(runtime.relational_columns, "g").?))).?;
            const payload = (try row.findCell(@intCast(physical.ordinalForName(runtime.relational_columns, "payload").?))).?;
            try std.testing.expect(g.is_null);
            try std.testing.expect(!payload.is_null);
            try std.testing.expectEqualStrings("null", payload.value.bytes_val);
        }
    };
    try Run.run(a, validator, table, &layout);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ validator, table, &layout });
    var restored = try mapper.PreparedRelationalWrite.initTyped(a, a, a, false, "r", "{\"g\":null,\"payload\":null}", validator, table, &layout, &.{"payload"}, true);
    defer restored.deinit(a);
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, mapper.PreparedRelationalWrite.initTyped(a, a, a, false, "r", "{\"g\":null,\"payload\":null}", validator, table, &layout, &.{ "g", "payload" }, true));
    for ([_][]const []const u8{ &.{ "payload", "payload" }, &.{"missing"} }) |names| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"g\":null,\"payload\":null}", .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidBatchRequest, validator.prepareTypedValueWithNulls(parsed.arena.allocator(), a, &parsed.value, names, false));
    }
}

test "relational index system document SQL null normalization shares typed schema preparation" {
    const a = std.testing.allocator;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("document-generated-json-null");
    defer directory.cleanup();
    var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.setSchemaJson(a,
        \\{"version":1,"storage_mode":"document","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"g":{},"payload":{}},"additionalProperties":false}}}}
    );
    const options: db_mod.types.ScanOptions = .{ .relational_query = .{ .fields = &.{ "g", "payload" }, .schema_version = 1 } };
    const session = try db.openDocumentReadSession(a, "", "", options);
    defer session.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const normalized = try session.normalizeRows(arena.allocator(), &.{.{ .key = "r", .value = "{\"g\":null,\"payload\":null}", .json_null_fields = &.{"payload"} }});
    try std.testing.expectEqual(@as(usize, 1), normalized[0].json_null_fields.len);
    try std.testing.expectEqualStrings("payload", normalized[0].json_null_fields[0]);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, normalized[0].value, .{});
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.contains("g"));
    try std.testing.expectEqual(std.json.Value.null, parsed.value.object.get("payload").?);
    // Match sql_execution's document commit adapter: the JSON representation
    // now owns null semantics, so relational-only transport markers are not
    // forwarded to native document batch admission.
    try db.batch(.{ .writes = &.{.{ .key = normalized[0].key, .value = normalized[0].value }} });
    const reader = try db.openDocumentReadSession(a, "", "", options);
    defer reader.deinit();
    var page = try reader.next(a, 1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqualSlices(bool, &.{ true, false }, page.rows[0].sql_nulls.?);
}

test "relational index system typed restore admits declared domains before generated verification" {
    const a = std.testing.allocator;
    var validator = try @import("../../schema/mod.zig").CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"column","column":"n"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","x-antfly-sql-type":"int16"},"g":{"type":"integer","x-antfly-sql-type":"int16"},"j":{"type":"sql_array","x-antfly-sql-type":"jsonb","nullable":true}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    for ([_][]const u8{ "{\"n\":32768,\"g\":32768}", "{\"n\":\"bad\",\"g\":0}" }) |input| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, input, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidBatchRequest, validator.prepareTypedValueWithNulls(parsed.arena.allocator(), a, &parsed.value, &.{}, true));
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"n\":1,\"g\":1,\"j\":null}", .{});
    defer parsed.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, validator.prepareTypedValueWithNulls(parsed.arena.allocator(), a, &parsed.value, &.{"j"}, true));
    const valid = try validator.prepareTypedValueWithNulls(parsed.arena.allocator(), a, &parsed.value, &.{}, true);
    defer valid.deinit(parsed.arena.allocator());
}

test "relational index system JSONB physical display is distinct from streaming semantic identity" {
    const a = std.testing.allocator;
    const content = @import("document_content_hash.zig");
    const Case = struct { input: []const u8, physical: []const u8 };
    const cases = [_]Case{
        .{ .input = "1.20", .physical = "1.20" },
        .{ .input = "1.200e2", .physical = "120.0" },
        .{ .input = "-0.00", .physical = "0.00" },
        .{ .input = "-0e999999", .physical = "0" },
        .{ .input = "{\"z\":1.20,\"aa\":1.2000}", .physical = "{\"aa\":1.2000,\"z\":1.20}" },
        .{ .input = "{\"source\":1.200,\"1.2\":\"3.40\"}", .physical = "{\"1.2\":\"3.40\",\"source\":1.200}" },
        .{ .input = "[1.20,1.2,1e-20,true,null,\"\\\"9000\\\"\"]", .physical = "[1.20,1.2,0.00000000000000000001,true,null,\"\\\"9000\\\"\"]" },
    };
    const Run = struct {
        fn run(fault_alloc: std.mem.Allocator, input: []const u8, expected: []const u8) !void {
            var parsed = try std.json.parseFromSlice(std.json.Value, fault_alloc, input, .{ .parse_numbers = false });
            defer parsed.deinit();
            const physical = try content.canonicalJsonStorageValueAlloc(fault_alloc, parsed.value);
            defer fault_alloc.free(physical);
            try std.testing.expectEqualStrings(expected, physical);
            const semantic = try content.canonicalJsonValueAlloc(fault_alloc, parsed.value);
            defer fault_alloc.free(semantic);
            var streamed = std.crypto.hash.Blake3.init(.{});
            streamed.update("preceding row fields");
            var before: [32]u8 = undefined;
            streamed.final(&before);
            content.hashCanonicalJsonStorageBytes(fault_alloc, &streamed, physical) catch |err| {
                var after: [32]u8 = undefined;
                streamed.final(&after);
                try std.testing.expectEqualSlices(u8, &before, &after);
                return err;
            };
            var direct = std.crypto.hash.Blake3.init(.{});
            direct.update("preceding row fields");
            direct.update("j");
            var size: [8]u8 = undefined;
            std.mem.writeInt(u64, &size, semantic.len, .little);
            direct.update(&size);
            direct.update(semantic);
            var lhs: [32]u8 = undefined;
            var rhs: [32]u8 = undefined;
            streamed.final(&lhs);
            direct.final(&rhs);
            try std.testing.expectEqualSlices(u8, &lhs, &rhs);
        }
    };
    for (cases) |case| {
        try Run.run(a, case.input, case.physical);
        var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ case.input, case.physical });
    }
    try std.testing.expectError(error.InvalidBatchRequest, content.canonicalJsonStorageValueAlloc(a, .{ .string = "\x00" }));
    try std.testing.expectError(error.InvalidBatchRequest, content.canonicalJsonStorageValueAlloc(a, .{ .number_string = "1e131072" }));
}

test "relational index system JSONB restore rejects alternate physical bytes without changing semantic identity" {
    const a = std.testing.allocator;
    const schema_api = @import("../../schema/mod.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const store = @import("relational_store.zig");
    var validator = try schema_api.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    const table = try schema_api.deriveRuntimeTableSchema(a, validator.schema);
    defer @import("../schema.zig").freeSchema(a, table);
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var left = try mapper.PreparedRelationalWrite.init(a, "r", "{\"payload\":1.20}", validator, table, &layout);
    defer left.deinit(a);
    var right = try mapper.PreparedRelationalWrite.init(a, "r", "{\"payload\":1.2}", validator, table, &layout);
    defer right.deinit(a);
    try left.finalizeMetadata(0);
    try right.finalizeMetadata(0);
    try std.testing.expect(!std.mem.eql(u8, left.packed_row, right.packed_row));
    try std.testing.expectEqualSlices(u8, &left.semantic_hash, &right.semantic_hash);
    try store.validateCanonicalValueForSchemaAndLayout(a, left.packed_row, table, &layout);
    try store.validateCanonicalValueForSchemaAndLayout(a, right.packed_row, table, &layout);
    const ordinal: u32 = @intCast(layout.ordinalForName(table.relational_columns, "payload").?);
    // A valid physical checksum and matching logical digest cannot authorize
    // exponent syntax or whitespace in stored cells. The trusted preparation
    // encoder constructs the adversarial fixture without weakening validation.
    for ([_][]const u8{ "1.20e0", " 1.20", "1.20 " }) |noncanonical| {
        const physical = try codec.serializePreparedOrdinalDeferredHash(a, table.version, table.relational_columns, &.{
            .{ .ordinal = ordinal, .path = "payload", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = noncanonical } },
        }, &layout);
        defer a.free(physical);
        try codec.finalizeOrdinalMetadata(physical, left.semantic_hash, 0);
        try std.testing.expectError(error.NonCanonicalRelationalRow, store.validateCanonicalValueForSchemaAndLayout(a, physical, table, &layout));
        try std.testing.expectError(error.NonCanonicalRelationalRow, codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(a, physical, table, &layout));
    }
}

test "relational index system JSONB semantic hashing scratch is independent of payload size" {
    const a = std.testing.allocator;
    const content = @import("document_content_hash.zig");
    const payload = try a.alloc(u8, 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const encoded = try std.fmt.allocPrint(a, "{{\"n\":1.200,\"text\":\"{s}\"}}", .{payload});
    defer a.free(encoded);
    var memory: @import("../../sql/memory_budget.zig") = .{ .backing = a, .limit = 8192 };
    var streamed = std.crypto.hash.Blake3.init(.{});
    try content.hashCanonicalJsonStorageBytes(memory.allocator(), &streamed, encoded);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(memory.peak < 8192);
    const semantic = try std.fmt.allocPrint(a, "{{\"n\":1.2,\"text\":\"{s}\"}}", .{payload});
    defer a.free(semantic);
    var expected = std.crypto.hash.Blake3.init(.{});
    expected.update("j");
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, semantic.len, .little);
    expected.update(&size);
    expected.update(semantic);
    var lhs: [32]u8 = undefined;
    var rhs: [32]u8 = undefined;
    streamed.final(&lhs);
    expected.final(&rhs);
    try std.testing.expectEqualSlices(u8, &lhs, &rhs);
}

test "relational index system JSONB array scale update survives LSM reopen and portable restore" {
    const a = std.testing.allocator;
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("jsonb-array-scale-reopen");
    defer directory.cleanup();
    var target = try TestDirectory.init("jsonb-array-scale-restore");
    defer target.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    {
        var db = try db_mod.DB.open(a, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(a,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"j":{"type":"sql_array","x-antfly-sql-type":"jsonb"}},"additionalProperties":false}}}}
        );
        try std.testing.expect(db.core.schema.?.requires_jsonb_array_display);
        try std.testing.expect(!db.core.schema.?.requires_json_expressions);
        // Equal logical values with different display scale must still replace
        // their physical postimage; the content hash is not a write dedup key.
        try db.batch(.{ .writes = &.{.{ .key = "row", .value =
            \\{"j":{"dimensions":[{"length":3,"lower_bound":-2}],"values":[1.2,null,null],"sql_nulls":[false,false,true]}}
        }} });
        try db.batch(.{ .writes = &.{.{ .key = "row", .value =
            \\{"j":{"dimensions":[{"length":3,"lower_bound":-2}],"values":[1.20,null,null],"sql_nulls":[false,false,true]}}
        }} });
        try @import("../portable_backup.zig").exportPortable(a, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(a, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(a, target.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(a, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_jsonb_array_display);
        try std.testing.expect(!db.core.schema.?.requires_json_expressions);
        const bytes = (try db.get(a, "row")).?;
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .parse_numbers = false });
        defer parsed.deinit();
        var array = try @import("../../sql/array_wire.zig").decode(a, .jsonb, parsed.value.object.get("j").?, .{});
        defer array.deinit();
        try std.testing.expectEqual(@as(i32, -2), array.value.dimensions[0].lower);
        try std.testing.expectEqualStrings("1.20", array.value.elements[0].value.number_string);
        try std.testing.expect(array.value.elements[1].value == .null and !array.value.elements[1].sql_null);
        try std.testing.expect(array.value.elements[2].sql_null);
    }
}

test "relational index system JSONB scale survives LSM reopen and strict portable restore" {
    const a = std.testing.allocator;
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("jsonb-scale-reopen");
    defer directory.cleanup();
    var target = try TestDirectory.init("jsonb-scale-restore");
    defer target.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(a);
    {
        var db = try db_mod.DB.open(a, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(a,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"payload":{"type":"json"}},"additionalProperties":false}}}}
        );
        try std.testing.expect(db.core.schema.?.requires_jsonb_display);
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"payload\":{\"source\":1.200e2,\"nested\":[1.20,-0.00]}}" }} });
        try @import("../portable_backup.zig").exportPortable(a, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(a, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(a, target.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(a, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_jsonb_display);
        try std.testing.expect(!db.core.schema.?.requires_json_expressions);
        try std.testing.expectEqual(@import("../schema.zig").storage_format_version, db.core.table_catalog.schema_format_version);
        const bytes = (try db.get(a, "row")).?;
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .parse_numbers = false });
        defer parsed.deinit();
        const payload = parsed.value.object.get("payload").?;
        try std.testing.expectEqualStrings("120.0", payload.object.get("source").?.number_string);
        const items = payload.object.get("nested").?.array.items;
        try std.testing.expectEqualStrings("1.20", items[0].number_string);
        try std.testing.expectEqualStrings("0.00", items[1].number_string);
        var work: @import("../../sql/json_order.zig").Budget = .{};
        const text = try @import("../../sql/jsonb_text.zig").format(a, payload, 1024, &work);
        defer a.free(text);
        try std.testing.expectEqualStrings("{\"nested\": [1.20, 0.00], \"source\": 120.0}", text);
    }
}

test "relational index system NUMERIC public scalar constraints survive LSM reopen and portable restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("numeric-public-scalar");
    defer directory.cleanup();
    var target_directory = try TestDirectory.init("numeric-public-scalar-restore");
    defer target_directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    const valid = "{\"n\":9007199254740993.2500,\"m\":1e-999,\"special\":\"NaN\"}";
    {
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row",
            \\"checks":[{"name":"minimum_n","column":"n","op":"gte","value":"9007199254740993.25"}],
            \\"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","minimum":9007199254740993.25,"maximum":9007199254740993.26,"multipleOf":0.0001,"nullable":true},"m":{"type":"number","x-antfly-sql-type":"numeric","minimum":1e-1000,"multipleOf":1e-1000},"special":{"type":"number","x-antfly-sql-type":"numeric","nullable":true}},"additionalProperties":false}}}}
        );
        try std.testing.expect(db.core.schema.?.requires_exact_numeric_validation);
        try db.batch(.{ .writes = &.{ .{ .key = "row", .value = valid }, .{ .key = "null", .value = "{\"n\":null,\"m\":1e-999,\"special\":\"Infinity\"}" } } });
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = valid }, .{ .key = "invalid", .value = "{\"n\":9007199254740993.2499}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try std.testing.expect((try db.get(alloc, "invalid")) == null);
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target_directory.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_exact_numeric_validation);
        try std.testing.expectEqual(@import("../schema.zig").storage_format_version, db.core.table_catalog.schema_format_version);
        try std.testing.expectEqual(@import("../schema.zig").RelationalColumnType.numeric, db.core.schema.?.relational_columns[0].column_type);
        const bytes = (try db.get(alloc, "row")).?;
        defer alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{ .parse_numbers = false });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("9007199254740993.2500", parsed.value.object.get("n").?.number_string);
        try std.testing.expectEqualStrings("NaN", parsed.value.object.get("special").?.string);
        var ctx: @import("../../sql/numeric_value.zig").Context = .{ .alloc = alloc };
        var tiny = try @import("../../sql/numeric_storage.zig").fromJson(&ctx, parsed.value.object.get("m").?);
        defer tiny.deinit();
        var expected = try @import("../../sql/numeric_value.zig").parse(&ctx, "1e-999");
        defer expected.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try @import("../../sql/numeric_value.zig").order(&ctx, tiny.value, expected.value));
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{ .key = "invalid-after-restore", .value = "{\"n\":9007199254740993.25001}" }} }));
    }
}

test "relational index system SQL NUMERIC defaults CHECK and covering expression index survive reopen restore" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    var create = try compiler.compile(a, "CREATE TABLE amounts (n numeric DEFAULT 9007199254740993.2500, g numeric GENERATED ALWAYS AS (n+0.0001) STORED, CHECK (n>=9007199254740993.25))", .{});
    defer create.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(a, create.statement.create_table), .{ .parse_numbers = false });
    var index = try compiler.compile(a, "CREATE INDEX by_total ON amounts ((n+0.0001)) INCLUDE (g,n) WHERE n>=CAST(CAST(9007199254740992.5 AS bigint) AS numeric)+0.25", .{});
    defer index.deinit();
    try std.testing.expect(try @import("../../sql/schema_ddl.zig").apply(a, &schema, index.statement.catalog_ddl));
    const json = try std.json.Stringify.valueAlloc(a, schema, .{});
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("sql-numeric-index-reopen");
    defer directory.cleanup();
    var target_directory = try TestDirectory.init("sql-numeric-index-restore");
    defer target_directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, json);
        try db.batch(.{ .writes = &.{ .{ .key = "default", .value = "{}" }, .{ .key = "explicit", .value = "{\"n\":9007199254740993.2510}" } } });
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = "{}" }, .{ .key = "bad", .value = "{\"n\":9007199254740993.2499}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try ready(&db);
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored = try db_mod.DB.open(alloc, target_directory.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    for ([_]*db_mod.DB{ &reopened, &restored }) |db| {
        try std.testing.expect(db.core.schema.?.requires_exact_numeric_validation);
        try std.testing.expect(db.core.schema.?.requires_exact_numeric_expressions);
        try ready(db);
        const bound = try @import("../../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .number_string = "9007199254740993.25" });
        defer alloc.free(bound);
        var reader = try db.beginRelationalRows(alloc, .{ .index = "by_total", .fields = &.{"g"}, .conditions = &.{.{ .column = "n", .op = .gte, .value = .{ .numeric = bound } }} });
        defer reader.deinit();
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 8, .records = 256, .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 2), page.rows.len);
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        for (page.rows) |row| {
            var decoded = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{ .parse_numbers = false });
            defer decoded.deinit();
            try std.testing.expectEqualStrings(if (std.mem.eql(u8, row.key, "default")) "9007199254740993.2501" else "9007199254740993.2511", decoded.value.object.get("g").?.number_string);
        }
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "invalid-after-recovery", .value = "{\"n\":9007199254740993.2499}" }} }));
    }
}

test "relational index system SQL membership and remainder survive reopen and portable restore" {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("../../sql/compiler.zig");
    var create = try compiler.compile(a, "CREATE TABLE exprs (n smallint, label text, bucket integer GENERATED ALWAYS AS (MOD(n,3)) STORED, CHECK (label IN ('ready','pending')), CHECK ((n>0) IS NOT FALSE OR n IN (-7,-3)))", .{});
    defer create.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, try @import("../../sql/ddl_runtime.zig").createSchemaAlloc(a, create.statement.create_table), .{});
    var index = try compiler.compile(a, "CREATE INDEX by_total ON exprs ((n % 3)) INCLUDE (bucket)", .{});
    defer index.deinit();
    try std.testing.expect(try @import("../../sql/schema_ddl.zig").apply(a, &schema, index.statement.catalog_ddl));
    const json = try std.json.Stringify.valueAlloc(a, schema, .{});
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("sql-membership-remainder-reopen");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    {
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        try db.setSchemaJson(alloc, json);
        try std.testing.expect(db.core.schema.?.requires_predicate_expressions);
        try db.batch(.{ .writes = &.{ .{ .key = "negative", .value = "{\"n\":-7,\"label\":\"ready\"}" }, .{ .key = "unknown", .value = "{\"n\":null,\"label\":null}" } } });
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{ .{ .key = "not-committed", .value = "{\"n\":3,\"label\":\"pending\"}" }, .{ .key = "invalid", .value = "{\"n\":-1,\"label\":\"ready\"}" } } }));
        try std.testing.expect((try db.get(alloc, "not-committed")) == null);
        try ready(&db);
        try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    }
    var reopened = try db_mod.DB.open(alloc, directory.path(), options);
    defer reopened.close();
    var restored_directory = try TestDirectory.init("sql-membership-remainder-restored");
    defer restored_directory.cleanup();
    var restored = try db_mod.DB.open(alloc, restored_directory.path(), options);
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    try verifyPredicates(&reopened);
    try verifyPredicates(&restored);
}

test "relational index system expression CHECK activation records arithmetic failure and resumes after repair" {
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"wide":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const checked =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"ratio","expression":{"op":"gte","args":[{"op":"divide","args":[{"op":"column","column":"x"},{"op":"column","column":"y"}]},{"op":"literal","type":"integer","value":0}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"wide":{"type":"string"}},"additionalProperties":false}}}}
    ;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("expression-check-activation");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.setSchemaJson(alloc, initial);
    try db.batch(.{ .writes = &.{ .{ .key = "bad", .value = "{\"x\":1,\"y\":0,\"wide\":\"unrelated\"}" }, .{ .key = "good", .value = "{\"x\":6,\"y\":2}" } } });
    try db.setSchemaJson(alloc, checked);
    {
        // The distributed owner page uses the same expression dependency
        // projection, while retaining a whole-row version guard for its CAS.
        var owner_directory = try @import("../../common/test_directory.zig").TestDirectory.init("expression-check-owner-projection");
        defer owner_directory.cleanup();
        var owner_options = options;
        owner_options.identity_namespace = .{ .table_id = 701, .shard_id = 702 };
        var owner = try db_mod.DB.open(alloc, owner_directory.path(), owner_options);
        defer owner.close();
        try owner.setSchemaJson(alloc, initial);
        try owner.batch(.{ .writes = &.{ .{ .key = "bad", .value = "{\"x\":1,\"y\":0,\"wide\":\"unrelated\"}" }, .{ .key = "good", .value = "{\"x\":6,\"y\":2}" } } });
        try std.testing.expectEqual(@as(u64, 1), owner.core.table_catalog.row_count);
        try owner.setSchemaJson(alloc, checked);
        const activation = @import("relational_integrity_activation.zig");
        {
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            const catalog_mod = @import("relational_integrity_catalog.zig");
            var catalog = try catalog_mod.decode(alloc, try read.get(catalog_mod.key));
            defer catalog.deinit();
            try std.testing.expect(activation.hasActive(catalog));
            try std.testing.expectEqual(activation.State.validating, (try activation.status(&read, catalog)).state);
        }
        var page = try activation.Page.prepare(alloc, std.testing.io, owner.core, .{ .time_ns = std.time.ns_per_s }) orelse return error.ActivationPageUnavailable;
        defer page.deinit();
        try std.testing.expectEqual(.check, page.phase);
        try std.testing.expectEqual(@as(usize, 2), page.rows.rows.len);
        for (page.rows.rows) |row| {
            const json = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
            defer json.deinit();
            try std.testing.expectEqual(@as(u32, 2), json.value.object.count());
            try std.testing.expect(json.value.object.contains("x"));
            try std.testing.expect(json.value.object.contains("y"));
            try std.testing.expect(!json.value.object.contains("wide"));
            try std.testing.expect(row.expected_content_digest != null);
        }
    }
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"x\":1,\"y\":0}" }} }));
    for (0..128) |_| {
        if ((try db.constraintValidationStatus()).state == .invalid) break;
        _ = try db.validateConstraintsStep(.{ .records = 1, .time_ns = std.time.ns_per_s });
    } else return error.ValidationDidNotConverge;
    try std.testing.expectEqual(@as(?u16, 0), (try db.constraintValidationStatus()).failed_check);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    try std.testing.expectEqual(.invalid, (try db.constraintValidationStatus()).state);
    try db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"x\":8,\"y\":2}" }} });
    try std.testing.expect(try db.retryConstraintValidation(2));
    for (0..128) |_| {
        if ((try db.constraintValidationStatus()).state == .enforced) break;
        _ = try db.validateConstraintsStep(.{ .records = 1, .time_ns = std.time.ns_per_s });
    } else return error.ValidationDidNotConverge;
    try std.testing.expectEqual(@as(?u16, null), (try db.constraintValidationStatus()).failed_check);
}

const schema_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"doubled","expression":{"op":"multiply","args":[{"op":"column","column":"total"},{"op":"literal","type":"integer","value":"2"}]}},{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"relational_indexes":[{"name":"by_total","keys":[{"column":"total"}],"include_columns":["doubled"]}],"checks":[{"name":"positive","column":"total","op":"gt","value":"0"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":["integer","null"]},"total":{"type":["integer","null"]},"doubled":{"type":["integer","null"]}},"required":["a","b","total","doubled"],"additionalProperties":false}}}}
;

test "relational index system portable restore preserves generated historical rows and resumed writes" {
    const portable = @import("../portable_backup.zig");
    const identity = @import("doc_identity.zig");
    var source_directory = try @import("../../common/test_directory.zig").TestDirectory.init("generated-portable-source");
    defer source_directory.cleanup();
    var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .start_optional_runtimes = false });
    defer source.close();
    try source.setSchemaJson(alloc, schema_json);
    try source.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"a\":3}" }} });
    var changed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{ .parse_numbers = false });
    defer changed.deinit();
    const arena = changed.arena.allocator();
    try changed.value.object.put(arena, "version", .{ .integer = 2 });
    changed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?.* = .{ .string = "7" };
    var predicate = try std.json.parseFromSlice(std.json.Value, arena, "[{\"column\":\"total\",\"op\":\"gt\",\"value\":7}]", .{});
    defer predicate.deinit();
    try changed.value.object.getPtr("relational_indexes").?.array.items[0].object.put(arena, "where", predicate.value);
    const schema_v2 = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    try source.setSchemaJson(alloc, schema_v2);
    try source.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"a\":3}" }} });
    try ready(&source);
    var expected = try scan(&source, false);
    defer expected.deinit();
    try std.testing.expectEqual(@as(usize, 2), expected.rows.len);
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    try portable.exportPortable(alloc, source.core.store, &archive);

    inline for (.{ false, true }) |unpublished| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init(if (unpublished) "generated-portable-staged" else "generated-portable-live");
        defer directory.cleanup();
        var restored = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        defer restored.close();
        if (unpublished) {
            try restored.importPortableIntoUnpublishedEmpty(alloc, archive.items, identity.default_namespace);
        } else {
            try restored.importPortableIntoEmpty(alloc, archive.items, identity.default_namespace);
        }
        // Inspect imported records before maintenance can repair a broken
        // archive rebuild. The historical total=5 row is not a member.
        const records = @import("relational_index_records.zig");
        const status = try restored.relationalIndexBuildStatus("by_total");
        const prefix = try records.forwardPrefix(.{ .generation = status.generation, .slot = status.slot });
        const keys = try restored.core.store.scanPrefixKeysPage(alloc, &prefix, null, 3);
        defer {
            for (keys) |key| alloc.free(key);
            alloc.free(keys);
        }
        try std.testing.expectEqual(@as(usize, 1), keys.len);
        try ready(&restored);
        {
            var covered_reader = try restored.beginRelationalRows(alloc, .{ .index = "by_total", .fields = &.{ "total", "doubled" }, .conditions = &.{.{ .column = "total", .op = .gt, .value = .{ .integer = 7 } }} });
            defer covered_reader.deinit();
            var covered = try covered_reader.nextPage(alloc, std.testing.io, .{ .time_ns = std.time.ns_per_s });
            defer covered.deinit();
            try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
            try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
            try std.testing.expectEqualStrings("{\"total\":10,\"doubled\":20}", covered.rows[0].json);
        }
        var actual = try scan(&restored, false);
        defer actual.deinit();
        try std.testing.expectEqual(expected.rows.len, actual.rows.len);
        for (expected.rows, actual.rows) |a, b| {
            try std.testing.expectEqualStrings(a.key, b.key);
            try std.testing.expectEqualStrings(a.json, b.json);
            try std.testing.expectEqualSlices(u8, &a.semantic_hash, &b.semantic_hash);
        }
        restored.close();
        restored = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        try std.testing.expectEqual(@as(u32, 2), restored.core.schema.?.version);
        try restored.batch(.{ .writes = &.{.{ .key = "after", .value = "{\"a\":4}" }} });
        const row = (try restored.get(alloc, "after")) orelse return error.TestExpectedEqual;
        defer alloc.free(row);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("total").?.integer);
        try std.testing.expectEqual(@as(i64, 22), parsed.value.object.get("doubled").?.integer);
    }
}

fn ready(db: *db_mod.DB) !void {
    for (0..1024) |_| {
        if ((try db.relationalIndexBuildStatus("by_total")).state == .ready) return;
        _ = try db.runRelationalIndexMaintenancePass();
    }
    return error.IndexBuildDidNotConverge;
}

fn verifyPredicates(db: *db_mod.DB) !void {
    try std.testing.expect(db.core.schema.?.requires_predicate_expressions);
    try std.testing.expectEqual(@import("../schema.zig").storage_format_version, db.core.table_catalog.schema_format_version);
    try ready(db);
    var reader = try db.beginRelationalRows(alloc, .{ .index = "by_total", .fields = &.{"bucket"} });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 8, .records = 256, .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    for (page.rows) |row| {
        var decoded = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
        defer decoded.deinit();
        const bucket = decoded.value.object.get("bucket").?;
        if (std.mem.eql(u8, row.key, "negative")) try std.testing.expectEqual(@as(i64, -1), bucket.integer) else try std.testing.expect(bucket == .null);
    }
    try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "bad-after-recovery", .value = "{\"n\":1,\"label\":\"invalid\"}" }} }));
    try db.batch(.{ .writes = &.{.{ .key = "valid-after-recovery", .value = "{\"n\":-3,\"label\":\"pending\"}" }} });
}

test "relational index system staged restore preserves historical absence and rejects forged generated values" {
    const staging = @import("restore_staging.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const internal = @import("../internal_keys.zig");
    const source_namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    const target_namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 };
    var source_directory = try @import("../../common/test_directory.zig").TestDirectory.init("preserved-restore-source");
    defer source_directory.cleanup();
    var target_directory = try @import("../../common/test_directory.zig").TestDirectory.init("preserved-restore-target");
    defer target_directory.cleanup();
    var changed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer changed.deinit();
    const arena = changed.arena.allocator();
    try changed.value.object.put(arena, "version", .{ .integer = 2 });
    const optional = try std.json.parseFromSlice(std.json.Value, arena, "{\"type\":[\"integer\",\"null\"]}", .{});
    try changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.put(arena, "later", optional.value);
    const later_default = try std.json.parseFromSlice(std.json.Value, arena, "{\"column\":\"later\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":\"7\"}}", .{});
    try changed.value.object.getPtr("column_defaults").?.array.append(later_default.value);
    const target_schema = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    {
        var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .identity_namespace = source_namespace, .start_optional_runtimes = false });
        defer source.close();
        try source.setSchemaJson(alloc, schema_json);
        try source.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"a\":3}" }}, .timestamp_ns = 123 });
        try source.setSchemaJson(alloc, target_schema);
    }
    var source = try db_mod.DB.open(alloc, source_directory.path(), .{ .identity_namespace = source_namespace, .open_mode = .query_readonly, .start_optional_runtimes = false });
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_directory.path(), .{ .identity_namespace = target_namespace, .start_optional_runtimes = false });
    defer target.close();
    try target.setSchemaJson(alloc, target_schema);
    const runtime = try @import("../schema.zig").serializeSchema(alloc, target.core.schema.?);
    defer alloc.free(runtime);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_namespace, .target_namespace = target_namespace, .target_schema_digest = staging.digest(runtime) };
    try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, target_namespace);
    try target.beginRestoreStaging(alloc, scope);
    var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.batch.?.writes.len);
    var forged = page.batch.?;
    forged.writes = &.{.{ .key = "old", .value = "{\"a\":3,\"b\":2,\"total\":6,\"doubled\":12}" }};
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, target.batchReplicatedApply(forged));
    var before = (try target.restoreStagingStatus(alloc)).?;
    defer before.deinit();
    try std.testing.expectEqual(@as(u64, 0), before.value.rows);
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&target, page.batch.?, .{ .index = 1, .term = 1 });
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&target, page.batch.?, .{ .index = 1, .term = 1 });
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), .{ .identity_namespace = target_namespace, .start_optional_runtimes = false });
    const key = try internal.relationalRowKeyAlloc(alloc, "old");
    defer alloc.free(key);
    const original = try source.core.store.get(alloc, key);
    defer alloc.free(original);
    const restored = try target.core.store.get(alloc, key);
    defer alloc.free(restored);
    try std.testing.expectEqualSlices(u8, &try codec.rowSemanticHash(original), &try codec.rowSemanticHash(restored));
    var view = target.core.acquireSchemaView().?;
    defer view.release();
    const row = try codec.ordinalRowView(restored, view.tableSchema().*, view.physicalLayout());
    try std.testing.expect((try row.findCell(row.ordinalForName("later").?)) == null);
    try std.testing.expectEqual(@as(u64, 123), row.writeTimestampNs());

    // The individually owned constructor is the serialized fallback contract.
    var fallback = try mapper.PreparedRelationalWrite.initPreserved(alloc, "old", page.batch.?.writes[0].value, view.validator(), view.tableSchema().*, view.physicalLayout());
    defer fallback.deinit(alloc);
    try std.testing.expectEqualSlices(u8, &try codec.rowSemanticHash(original), &fallback.semantic_hash);
    var mutation = try mapper.PreparedRelationalWrite.init(alloc, "new", page.batch.?.writes[0].value, view.validator(), view.tableSchema().*, view.physicalLayout());
    defer mutation.deinit(alloc);
    const mutation_row = try mutation.typedView(view.tableSchema().*, view.physicalLayout());
    try std.testing.expect((try mutation_row.findCell(mutation_row.ordinalForName("later").?)) != null);

    // A newly required column cannot be silently filled to make a restore
    // compatible. The explicit target validation must still reject it.
    try changed.value.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("required").?.array.append(.{ .string = "later" });
    const strict_json = try std.json.Stringify.valueAlloc(arena, changed.value, .{});
    const schema_api = @import("../../schema/mod.zig");
    var strict = try schema_api.CompiledTableValidator.init(alloc, strict_json);
    defer strict.deinit(alloc);
    const strict_schema = try schema_api.deriveRuntimeTableSchema(alloc, strict.schema);
    defer @import("../schema.zig").freeSchema(alloc, strict_schema);
    var strict_layout = try codec.PhysicalLayout.init(alloc, strict_schema);
    defer strict_layout.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, mapper.PreparedRelationalWrite.initPreserved(alloc, "old", page.batch.?.writes[0].value, strict, strict_schema, &strict_layout));
}

test "relational index system fallback consumer failure releases transferred computed effects" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-expression-consumer-error");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"target\",\"field\":\"total\"}]}" });
    // Duplicate keys force the serialized fallback. Graph extraction rejects
    // the computed numeric field after takeExtracted has transferred ownership;
    // neither row/index publication nor extracted allocations may survive.
    try std.testing.expectError(error.InvalidGraphEdges, db.batch(.{ .writes = &.{
        .{ .key = "row", .value = "{\"a\":1}" },
        .{ .key = "row", .value = "{\"a\":2}" },
    } }));
    try std.testing.expect((try db.lookup(alloc, "row", .{})) == null);
}

fn scan(db: *db_mod.DB, indexed: bool) !rows.Page {
    var reader = try db.beginRelationalRows(alloc, .{ .index = if (indexed) "by_total" else null, .fields = &.{ "total", "doubled" } });
    defer reader.deinit();
    return reader.nextPage(alloc, std.testing.io, .{ .rows = 32, .records = 256, .time_ns = std.time.ns_per_s });
}

test "relational index system generated rows share defaults checks covering hash and durable replay" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-expressions");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    // Stored generated columns cannot be overridden, even by a supplied value
    // of the wrong type. The default applies only to the absent b column.
    try db.batch(.{ .timestamp_ns = 123, .writes = &.{.{ .key = "row", .value = "{\"a\":3,\"total\":\"ignored\"}" }} });
    try ready(&db);
    var covered = try scan(&db, true);
    defer covered.deinit();
    var primary = try scan(&db, false);
    defer primary.deinit();
    try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
    try std.testing.expectEqualStrings("{\"total\":5,\"doubled\":10}", covered.rows[0].json);
    try std.testing.expectEqualStrings(primary.rows[0].json, covered.rows[0].json);
    try std.testing.expectEqualSlices(u8, &primary.rows[0].semantic_hash, &covered.rows[0].semantic_hash);
    try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
    try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":-3}" }} }));
    try std.testing.expectError(error.RelationalExpressionOverflow, db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":9223372036854775807}" }} }));
    try db.batch(.{ .writes = &.{
        .{ .key = "row", .value = "{\"a\":4}" },
        .{ .key = "row", .value = "{\"a\":5,\"b\":null}" },
    } });
    var nullable = try scan(&db, true);
    defer nullable.deinit();
    try std.testing.expectEqualStrings("{\"total\":null,\"doubled\":null}", nullable.rows[0].json);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try ready(&db);
    var reopened = try scan(&db, true);
    defer reopened.deinit();
    try std.testing.expectEqualStrings(nullable.rows[0].json, reopened.rows[0].json);

    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "intent", "{\"a\":3}", view.validator(), view.tableSchema().*, view.physicalLayout());
    defer prepared.deinit(alloc);
    const logical = (try prepared.extracted.logicalJson()).?;
    var logical_root = try std.json.parseFromSlice(std.json.Value, alloc, logical, .{});
    defer logical_root.deinit();
    try std.testing.expectEqual(@as(i64, 5), logical_root.value.object.get("total").?.integer);
    try std.testing.expectEqual(@as(i64, 10), logical_root.value.object.get("doubled").?.integer);
    try prepared.finalizeMetadata(999);
    // Commit consumes the canonical durable row without filling defaults into
    // its specials-only sidecar or evaluating the generated graph again.
    var replay = try mapper.PreparedRelationalWrite.initFromIntent(alloc, "intent", "{}", view.validator(), view.tableSchema().*, view.physicalLayout(), prepared.packed_row);
    defer replay.deinit(alloc);
    try std.testing.expectEqualSlices(u8, prepared.packed_row, replay.packed_row);
    try std.testing.expectEqual(@as(usize, 0), replay.parsedValue().object.count());
}

test "relational index system generated schema changes require rewrite with durable data presence" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-generated-schema-guard");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"a\":3}" }} });
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const arena = parsed.arena.allocator();
    try parsed.value.object.put(arena, "version", .{ .integer = 2 });
    // Declaration order is not semantics, and changed defaults only affect
    // future writes. Neither operation needs an existing-row rewrite.
    const declarations = parsed.value.object.getPtr("generated_columns").?.array.items;
    std.mem.swap(std.json.Value, &declarations[0], &declarations[1]);
    const default_value = parsed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?;
    default_value.* = .{ .string = "7" };
    const reordered = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try db.setSchemaJson(alloc, reordered);
    try parsed.value.object.put(arena, "version", .{ .integer = 3 });
    const multiplier = declarations[1].object.getPtr("expression").?.object.getPtr("args").?.array.items[1].object.getPtr("value").?;
    multiplier.* = .{ .string = "3" };
    const changed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, changed));
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
    try parsed.value.object.put(arena, "generated_columns", .{ .array = std.array_list.Managed(std.json.Value).init(arena) });
    const removed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, removed));
    // Presence follows exact cardinality, remains true after a partial delete,
    // and survives restart. No schema preflight performs a user-row scan.
    try db.batch(.{ .writes = &.{.{ .key = "survivor", .value = "{\"a\":4}" }} });
    try db.batch(.{ .deletes = &.{"row"} });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, db.setSchemaJson(alloc, changed));
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
}
