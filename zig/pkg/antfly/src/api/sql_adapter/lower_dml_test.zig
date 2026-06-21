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

const lexer = @import("lexer.zig");
const parser_context = @import("parser_context.zig");
const plan = @import("plan.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");
const schema_api = @import("../../schema/mod.zig");
const sql_value = @import("value.zig");

fn runtimeSchemaFromJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !runtime_schema.TableSchema {
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
}

fn lowerInsertForTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
) !plan.LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);

    var parser = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return parser_context.ParserState.ContextAccessors.parseInsert(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

fn lowerInsertWithResolverForTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !plan.LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);

    var parser = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return parser_context.ParserState.ContextAccessors.parseInsert(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

const TestPrimaryResolver = struct {
    row_json: []const u8,
    version: u64,
    exists: bool = true,
    resolved_key: []const u8 = "test-existing-primary",

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolve,
            .resolve_temporal_overlap = resolveTemporalOverlap,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolve(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, self.resolved_key);
    }

    fn resolveTemporalOverlap(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        if (encoded_start.len == 0 or encoded_end.len == 0) return error.TestUnexpectedResult;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, self.resolved_key);
    }

    fn primaryExists(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool {
        _ = alloc;
        _ = table_name;
        _ = physical_key;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.exists;
    }

    fn lookupPrimary(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?relational_rows.ResolvedPrimaryRow {
        _ = table_name;
        if (physical_key.len == 0) return null;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .json = try alloc.dupe(u8, self.row_json),
            .version = self.version,
        };
    }
};

test "sql adapter lower dml lowers insert default values into defaulted row batch" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","default":"u_default"},"status":{"type":"keyword","default":"active"},"amount":{"type":"numeric","default":0}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records DEFAULT VALUES RETURNING id, status, amount",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u_default\",\"status\":\"active\",\"amount\":0}", lowered.batch.returning_rows[0]);

    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u_default\",\"status\":\"existing\",\"amount\":9}", .version = 5 };
    var do_nothing = try lowerInsertWithResolverForTestAlloc(
        alloc,
        "INSERT INTO usage_records DEFAULT VALUES ON CONFLICT (id) DO NOTHING RETURNING id",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer do_nothing.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), do_nothing.batch.inserted);
    try std.testing.expectEqual(@as(u32, 0), do_nothing.batch.transformed);
    try std.testing.expectEqual(@as(usize, 0), do_nothing.batch.returning_rows.len);

    var conflict_update = try lowerInsertWithResolverForTestAlloc(
        alloc,
        "INSERT INTO usage_records DEFAULT VALUES ON CONFLICT (id) DO UPDATE SET status = excluded.status, amount = excluded.amount RETURNING id, status, amount",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer conflict_update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), conflict_update.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), conflict_update.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), conflict_update.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("{\"id\":\"u_default\",\"status\":\"active\",\"amount\":0}", conflict_update.batch.returning_rows[0]);

    var omitted_default_conflict_update = try lowerInsertWithResolverForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id) VALUES ('u_default') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING id, status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer omitted_default_conflict_update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), omitted_default_conflict_update.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), omitted_default_conflict_update.batch.transformed);
    try std.testing.expectEqualStrings("{\"id\":\"u_default\",\"status\":\"active\"}", omitted_default_conflict_update.batch.returning_rows[0]);
}

test "sql adapter lower dml lowers insert jsonb literal" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', '{\"source\":\"literal\"}'::jsonb) RETURNING *",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("literal", returned.value.object.get("metadata").?.object.get("source").?.string);

    var wrapped = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u2', to_jsonb($1)) RETURNING metadata",
        schema,
        &.{.{ .string = "wrapped" }},
    );
    defer wrapped.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), wrapped.batch.inserted);
    var wrapped_returned = try std.json.parseFromSlice(std.json.Value, alloc, wrapped.batch.returning_rows[0], .{});
    defer wrapped_returned.deinit();
    try std.testing.expectEqualStrings("wrapped", wrapped_returned.value.object.get("metadata").?.string);
}

test "sql adapter lower dml lowers jsonb_build_object insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', jsonb_build_object('source', $1, 'count', 3, 'active', true)) RETURNING metadata",
        schema,
        &.{.{ .string = "builder" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    const metadata = returned.value.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("builder", metadata.get("source").?.string);
    try std.testing.expectEqual(@as(i64, 3), metadata.get("count").?.integer);
    try std.testing.expect(metadata.get("active").?.bool);

    var decoded_nested = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u_decoded', jsonb_build_object('source', 'decoded', 'nested', convert_from($1, 'UTF8')::jsonb)) RETURNING metadata",
        schema,
        &.{.{ .string = "{\"plan\":\"pro\"}" }},
    );
    defer decoded_nested.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), decoded_nested.batch.inserted);
    var decoded_returned = try std.json.parseFromSlice(std.json.Value, alloc, decoded_nested.batch.returning_rows[0], .{});
    defer decoded_returned.deinit();
    const decoded_metadata = decoded_returned.value.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("decoded", decoded_metadata.get("source").?.string);
    try std.testing.expectEqualStrings("pro", decoded_metadata.get("nested").?.object.get("plan").?.string);

    var parameterized_key = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u2', jsonb_build_object($1::text, $2, 'active', true)) RETURNING metadata",
        schema,
        &.{ .{ .string = "source" }, .{ .string = "dynamic" } },
    );
    defer parameterized_key.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), parameterized_key.batch.inserted);
    var parameterized_returned = try std.json.parseFromSlice(std.json.Value, alloc, parameterized_key.batch.returning_rows[0], .{});
    defer parameterized_returned.deinit();
    const parameterized_metadata = parameterized_returned.value.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("dynamic", parameterized_metadata.get("source").?.string);
    try std.testing.expect(parameterized_metadata.get("active").?.bool);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u3', jsonb_build_object($1::text, 'one', 'source', 'two'))",
        schema,
        &.{.{ .string = "source" }},
    ));
}

test "sql adapter lower dml lowers convert_from jsonb insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertForTestAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', convert_from($1, 'UTF8')::jsonb) RETURNING metadata",
        schema,
        &.{.{ .string = "{\"source\":\"converted\",\"count\":4}" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"converted\",\"count\":4}}", lowered.batch.returning_rows[0]);
}
