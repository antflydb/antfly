// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy at
// https://www.antfly.io/licensing/ELv2-license.

//! Adapter from the unified /indexes resource to the schema-owned relational
//! catalog. No second definition is persisted in artifact indexes_json: schema
//! epoch publication installs the native generation and its resumable build.
const std = @import("std");
const wire = @import("antfly_schema_openapi");
const tables = @import("tables.zig");
const records = @import("../common/topology_records.zig");
const indexes = @import("indexes.zig");

pub fn isRelational(alloc: std.mem.Allocator, config_json: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(struct { type: []const u8 = "full_text" }, alloc, config_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return std.mem.eql(u8, parsed.value.type, "relational");
}

pub fn contains(alloc: std.mem.Allocator, schema_json: []const u8, name: []const u8) !bool {
    if (schema_json.len == 0) return false;
    // Namespace admission needs names, not thousands of copied composite-key
    // configurations. Full typed validation happens once for the candidate.
    var parsed = try std.json.parseFromSlice(struct { relational_indexes: []const struct { name: []const u8 } = &.{} }, alloc, schema_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    for (parsed.value.relational_indexes) |definition| if (std.mem.eql(u8, definition.name, name)) return true;
    return false;
}

pub fn configForDefinition(alloc: std.mem.Allocator, definition: wire.RelationalIndexDefinition, columns: *@import("relational_expression_contract.zig").ColumnTypes) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const keys: []const wire.RelationalIndexKey = blk: {
        // Column-only indexes can serialize their immutable slice directly.
        for (definition.keys) |key| if (key.expression != null) {
            const copies = try owned.dupe(wire.RelationalIndexKey, definition.keys);
            for (copies) |*copy| if (copy.expression) |expression| {
                copy.expression = try @import("relational_expression_contract.zig").cloneCanonicalExpression(owned, expression);
            };
            break :blk copies;
        };
        break :blk definition.keys;
    };
    const conditions: ?[]const wire.RelationalIndexPredicate = if (definition.where) |where| blk: {
        const copies = try owned.dupe(wire.RelationalIndexPredicate, where);
        for (copies) |*condition| if (condition.value) |value| {
            condition.value = try columns.canonicalValue(owned, condition.column, value);
        };
        break :blk copies;
    } else null;
    return std.json.Stringify.valueAlloc(alloc, .{ .name = definition.name, .type = "relational", .keys = keys, .include_columns = definition.include_columns, .where = conditions, .description = definition.description }, .{ .emit_null_optional_fields = false });
}

/// Returned TableRecord is independently owned. Caller performs one exact
/// whole-definition CAS and then schedules normal schema reconciliation.
pub fn create(alloc: std.mem.Allocator, table: records.TableRecord, name: []const u8, config_json: []const u8) !records.TableRecord {
    if (try indexes.hasIndexConfig(alloc, table.indexes_json, name)) return error.TableGenerationChanged;
    if (table.relational_retirement_json.len != 0 or table.restore_backup_id.len != 0) return error.TableTransitionActive;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const config = try std.json.parseFromSlice(std.json.Value, owned, config_json, .{ .parse_numbers = false });
    return mutate(alloc, owned, table, name, try definitionFromConfig(owned, name, config.value));
}

fn definitionFromConfig(owned: std.mem.Allocator, name: []const u8, config: std.json.Value) !std.json.Value {
    if (config != .object) return error.InvalidCreateIndexRequest;
    const object = config.object;
    if (object.get("name")) |provided| if (provided != .string or !std.mem.eql(u8, provided.string, name)) return error.InvalidCreateIndexRequest;
    if (object.get("version")) |version| if (version != .number_string or !std.mem.eql(u8, version.number_string, "0")) return error.InvalidCreateIndexRequest;
    // Positive schema conversion: common artifact enrichments and any other
    // executable config must not be accepted and then silently discarded.
    var fields = object.iterator();
    while (fields.next()) |field| {
        if (!std.mem.eql(u8, field.key_ptr.*, "name") and !std.mem.eql(u8, field.key_ptr.*, "type") and
            !std.mem.eql(u8, field.key_ptr.*, "keys") and !std.mem.eql(u8, field.key_ptr.*, "include_columns") and !std.mem.eql(u8, field.key_ptr.*, "where") and !std.mem.eql(u8, field.key_ptr.*, "description") and
            !std.mem.eql(u8, field.key_ptr.*, "version")) return error.InvalidCreateIndexRequest;
    }
    const kind = object.get("type") orelse return error.InvalidCreateIndexRequest;
    if (kind != .string or !std.mem.eql(u8, kind.string, "relational")) return error.InvalidCreateIndexRequest;
    var definition: std.json.ObjectMap = .empty;
    try definition.put(owned, "name", .{ .string = name });
    try definition.put(owned, "keys", object.get("keys") orelse return error.InvalidCreateIndexRequest);
    if (object.get("include_columns")) |includes| try definition.put(owned, "include_columns", includes);
    if (object.get("where")) |where| try definition.put(owned, "where", where);
    if (object.get("description")) |description| try definition.put(owned, "description", description);
    return .{ .object = definition };
}

/// Hoist unified create-table relational configs into the single schema
/// catalog before generated/public parsing and artifact normalization. The
/// returned body has no relational entries in indexes; null means no rewrite.
pub fn normalizeCreateTableBody(alloc: std.mem.Allocator, body: []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, owned, body, .{ .parse_numbers = false });
    if (parsed.value != .object) return null;
    const configs = parsed.value.object.get("indexes") orelse return null;
    if (configs != .object) return null;
    var source_schema = parsed.value.object.get("schema") orelse std.json.Value{ .object = .empty };
    if (source_schema != .object) return null;
    var declarations: std.array_list.Managed(std.json.Value) = .init(owned);
    var names: std.StringHashMapUnmanaged(void) = .empty;
    if (source_schema.object.get("relational_indexes")) |existing| {
        if (existing != .array) return error.InvalidCreateTableRequest;
        for (existing.array.items) |definition| {
            if (definition != .object) return error.InvalidCreateTableRequest;
            const name = definition.object.get("name") orelse return error.InvalidCreateTableRequest;
            if (name != .string or (try names.getOrPut(owned, name.string)).found_existing) return error.InvalidCreateTableRequest;
            try declarations.append(definition);
        }
    }
    var artifacts: std.json.ObjectMap = .empty;
    var changed = false;
    var iterator = configs.object.iterator();
    while (iterator.next()) |entry| {
        // A single unified resource name may not select two catalogs, even
        // when the duplicate definitions happen to have equivalent keys.
        if (names.contains(entry.key_ptr.*)) return error.InvalidCreateTableRequest;
        const kind = if (entry.value_ptr.* == .object) entry.value_ptr.object.get("type") else null;
        if (kind != null and kind.? == .string and std.mem.eql(u8, kind.?.string, "relational")) {
            const raw_config = try std.json.Stringify.valueAlloc(owned, entry.value_ptr.*, .{});
            _ = @import("table_contract.zig").parseCreateIndexRequest(owned, entry.key_ptr.*, raw_config) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidCreateTableRequest,
            };
            try declarations.append(definitionFromConfig(owned, entry.key_ptr.*, entry.value_ptr.*) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidCreateTableRequest,
            });
            try names.put(owned, entry.key_ptr.*, {});
            changed = true;
        } else try artifacts.put(owned, entry.key_ptr.*, entry.value_ptr.*);
    }
    if (!changed) return null;
    try source_schema.object.put(owned, "relational_indexes", .{ .array = declarations });
    try parsed.value.object.put(owned, "schema", source_schema);
    try parsed.value.object.put(owned, "indexes", .{ .object = artifacts });
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

pub fn drop(alloc: std.mem.Allocator, table: records.TableRecord, name: []const u8) !?records.TableRecord {
    if (!try contains(alloc, table.schema_json, name)) return null;
    if (table.relational_retirement_json.len != 0 or table.restore_backup_id.len != 0) return error.TableTransitionActive;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    return try mutate(alloc, arena.allocator(), table, name, null);
}

fn mutate(alloc: std.mem.Allocator, scratch: std.mem.Allocator, table: records.TableRecord, name: []const u8, definition: ?std.json.Value) !records.TableRecord {
    if (name.len == 0 or name.len > 256 or table.schema_json.len == 0) return error.InvalidCreateIndexRequest;
    var schema = try std.json.parseFromSlice(std.json.Value, scratch, table.schema_json, .{ .parse_numbers = false });
    if (schema.value != .object) return error.InvalidCreateIndexRequest;
    const mode = schema.value.object.get("storage_mode") orelse return error.RelationalTableRequired;
    if (mode != .string or !std.mem.eql(u8, mode.string, "relational")) return error.RelationalTableRequired;
    var definitions: std.array_list.Managed(std.json.Value) = .init(scratch);
    var replaced = false;
    if (schema.value.object.get("relational_indexes")) |prior| {
        if (prior != .array) return error.InvalidCreateIndexRequest;
        for (prior.array.items) |item| {
            if (item != .object) return error.InvalidCreateIndexRequest;
            const prior_name = item.object.get("name") orelse return error.InvalidCreateIndexRequest;
            if (prior_name != .string) return error.InvalidCreateIndexRequest;
            if (std.mem.eql(u8, prior_name.string, name)) {
                if (definition) |value| try definitions.append(value);
                replaced = true;
            } else try definitions.append(item);
        }
    }
    if (!replaced) if (definition) |value| try definitions.append(value);
    try schema.value.object.put(scratch, "relational_indexes", .{ .array = definitions });
    const candidate = try std.json.Stringify.valueAlloc(scratch, schema.value, .{});
    // Shared schema machinery validates declared types/columns, bumps the
    // write epoch, retains the prior read mapping, and rebuilds projections.
    return tables.applySchemaUpdateRecord(alloc, &table, candidate);
}

test "relational mutation unified index create and drop have one schema authority and epoch" {
    const alloc = std.testing.allocator;
    const metadata = @import("../metadata/table_manager.zig");
    const declaration =
        \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"tag":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const table: records.TableRecord = .{ .table_id = 100, .name = "rows", .schema_json = declaration, .indexes_json = "{}" };
    const config = try @import("table_contract.zig").parseCreateIndexRequest(alloc, "by_tag", "{\"type\":\"relational\",\"description\":\"Ordered tags\",\"keys\":[{\"column\":\"tag\",\"direction\":\"desc\",\"nulls\":\"first\"},{\"column\":\"id\"}]}");
    defer alloc.free(config);
    const created = try create(alloc, table, "by_tag", config);
    defer metadata.freeTable(alloc, created);
    try std.testing.expectEqual(@as(u32, 5), try tables.schemaVersion(created.schema_json));
    try std.testing.expectEqual(@as(u32, 4), try tables.schemaVersion(created.read_schema_json));
    try std.testing.expect(!try indexes.hasIndexConfig(alloc, created.indexes_json, "by_tag"));
    try std.testing.expect(try contains(alloc, created.schema_json, "by_tag"));
    const same = try create(alloc, created, "by_tag", config);
    defer metadata.freeTable(alloc, same);
    try std.testing.expectEqual(@as(u32, 5), try tables.schemaVersion(same.schema_json));
    const removed = (try drop(alloc, created, "by_tag")).?;
    defer metadata.freeTable(alloc, removed);
    try std.testing.expectEqual(@as(u32, 6), try tables.schemaVersion(removed.schema_json));
    try std.testing.expectEqualStrings(created.read_schema_json, removed.read_schema_json);
    try std.testing.expect(!try contains(alloc, removed.schema_json, "by_tag"));
    try std.testing.expect(try drop(alloc, removed, "by_tag") == null);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, create(alloc, table, "bad", "{\"type\":\"relational\",\"keys\":[{\"column\":\"missing\"}]}"));
    try std.testing.expectError(error.InvalidCreateIndexRequest, create(alloc, table, "bad", "{\"type\":\"relational\",\"version\":1,\"keys\":[{\"column\":\"id\"}]}"));
    try std.testing.expectError(error.InvalidCreateIndexRequest, create(alloc, table, "bad", "{\"type\":\"relational\",\"enrichments\":[],\"keys\":[{\"column\":\"id\"}]}"));
    var collision = table;
    collision.indexes_json = "{\"by_tag\":{\"type\":\"full_text\"}}";
    try std.testing.expectError(error.TableGenerationChanged, create(alloc, collision, "by_tag", config));
    var retiring = table;
    retiring.relational_retirement_json = "active";
    try std.testing.expectError(error.TableTransitionActive, create(alloc, retiring, "by_tag", config));
}

test "relational mutation create table hoists unified indexes before artifact admission" {
    const alloc = std.testing.allocator;
    const parser = @import("table_contract.zig");
    const body =
        \\{"schema":{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"tag":{"type":"keyword"}},"additionalProperties":false}}}},"indexes":{"by_tag":{"type":"relational","description":"Tags","keys":[{"column":"tag"},{"column":"id","direction":"desc"}]},"text":{"type":"full_text"}}}
    ;
    var request = try parser.parseCreateTableRequest(alloc, body);
    defer request.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), try tables.schemaVersion(request.schema_json.?));
    try std.testing.expect(try contains(alloc, request.schema_json.?, "by_tag"));
    try std.testing.expect(!try indexes.hasIndexConfig(alloc, request.indexes_json.?, "by_tag"));
    try std.testing.expect(try indexes.hasIndexConfig(alloc, request.indexes_json.?, "text"));
    try std.testing.expectError(error.InvalidCreateTableRequest, parser.parseCreateTableRequest(alloc,
        \\{"schema":{"relational_indexes":[{"name":"dupe","keys":[{"column":"id"}]}]},"indexes":{"dupe":{"type":"full_text"}}}
    ));
    try std.testing.expectError(error.InvalidCreateTableRequest, parser.parseCreateTableRequest(alloc,
        \\{"indexes":{"default":{"type":"relational","keys":[{"column":"id"}]}}}
    ));
}

test "relational mutation INCLUDE fields survive unified resource and reject duplicate or unknown columns" {
    const alloc = std.testing.allocator;
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const table: records.TableRecord = .{ .table_id = 100, .name = "rows", .schema_json = declaration, .indexes_json = "{}" };
    const config = try @import("table_contract.zig").parseCreateIndexRequest(alloc, "covered", "{\"type\":\"relational\",\"keys\":[{\"column\":\"id\"}],\"include_columns\":[\"label\"]}");
    defer alloc.free(config);
    const created = try create(alloc, table, "covered", config);
    defer @import("../metadata/table_manager.zig").freeTable(alloc, created);
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, created.schema_json);
    defer parsed.deinit(alloc);
    const definition = parsed.relational_indexes.?.value[0];
    try std.testing.expectEqualStrings("label", definition.include_columns.?[0]);
    var columns: @import("relational_expression_contract.zig").ColumnTypes = .{ .alloc = alloc, .source = .{ .parsed = &parsed } };
    defer columns.deinit();
    const projected = try configForDefinition(alloc, definition, &columns);
    defer alloc.free(projected);
    try std.testing.expect(std.mem.indexOf(u8, projected, "\"include_columns\":[\"label\"]") != null);
    for ([_][]const u8{
        "{\"type\":\"relational\",\"keys\":[{\"column\":\"id\"}],\"include_columns\":[\"id\"]}",
        "{\"type\":\"relational\",\"keys\":[{\"column\":\"id\"}],\"include_columns\":[\"missing\"]}",
    }) |invalid| try std.testing.expectError(error.InvalidSchemaUpdateRequest, create(alloc, table, "bad", invalid));
}

test "relational mutation public create preserves exact typed schema tokens before validation" {
    const alloc = std.testing.allocator;
    const base =
        \\"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}
    ;
    for ([_][]const u8{
        \\"column_defaults":[{"column":"id","expression":{"op":"literal","type":"integer","value":1.00000000000000001}}]
        ,
        \\"checks":[{"name":"exact","expression":{"op":"eq","args":[{"op":"column","column":"id"},{"op":"literal","type":"integer","value":1.00000000000000001}]}}]
        ,
    }) |extra| {
        const body = try std.fmt.allocPrint(alloc, "{{\"schema\":{{{s},{s}}}}}", .{ base, extra });
        defer alloc.free(body);
        try std.testing.expectError(error.InvalidCreateTableSchemaRequest, @import("table_contract.zig").parseCreateTableRequest(alloc, body));
    }
}

test "relational mutation expression composite keys use the unified generated contract" {
    const alloc = std.testing.allocator;
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const table: records.TableRecord = .{ .table_id = 100, .name = "rows", .schema_json = declaration, .indexes_json = "{}" };
    const body =
        \\{"type":"relational","version":0,"keys":[{"column":"label"},{"expression":{"op":"add","args":[{"op":"column","column":"id"},{"op":"literal","type":"integer","value":9007199254740993}]},"result_type":"integer","direction":"desc"}],"include_columns":["id"]}
    ;
    const config = try @import("table_contract.zig").parseCreateIndexRequest(alloc, "computed", body);
    defer alloc.free(config);
    const created = try create(alloc, table, "computed", config);
    defer @import("../metadata/table_manager.zig").freeTable(alloc, created);
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, created.schema_json);
    defer parsed.deinit(alloc);
    const definition = parsed.relational_indexes.?.value[0];
    try std.testing.expectEqualStrings("label", definition.keys[0].column.?);
    try std.testing.expect(definition.keys[1].column == null);
    var columns: @import("relational_expression_contract.zig").ColumnTypes = .{ .alloc = alloc, .source = .{ .parsed = &parsed } };
    defer columns.deinit();
    const projected = try configForDefinition(alloc, definition, &columns);
    defer alloc.free(projected);
    try std.testing.expect(std.mem.indexOf(u8, projected, "\"value\":\"9007199254740993\"") != null);
    // Admission and projection use the same lossless literal spelling.
    try std.testing.expectEqualStrings("9007199254740993", definition.keys[1].expression.?.args.?[1].value.?.string);
    const roundtrip = try create(alloc, created, "computed", projected);
    defer @import("../metadata/table_manager.zig").freeTable(alloc, roundtrip);
    try std.testing.expectEqual(try tables.schemaVersion(created.schema_json), try tables.schemaVersion(roundtrip.schema_json));
    var roundtrip_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, roundtrip.schema_json);
    defer roundtrip_schema.deinit(alloc);
    const native_schema = @import("../storage/schema.zig");
    const runtime = try @import("../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed);
    defer native_schema.freeSchema(alloc, runtime);
    var layout = try @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout.init(alloc, runtime);
    defer layout.deinit();
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const old_keys = (try parsed.relationalIndexDefinitions(scratch.allocator())).?[0].keys;
    const new_keys = (try roundtrip_schema.relationalIndexDefinitions(scratch.allocator())).?[0].keys;
    var old_plan = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(alloc, runtime, &layout, old_keys);
    defer old_plan.deinit();
    var new_plan = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(alloc, runtime, &layout, new_keys);
    defer new_plan.deinit();
    try std.testing.expectEqualSlices(u8, &old_plan.fingerprint, &new_plan.fingerprint);
    const public = try indexes.encodeCreatedIndexConfig(alloc, "computed",
        \\{"type":"relational","keys":[{"expression":{"op":"literal","type":"string","value":"${secret:literal}"},"result_type":"string"}]}
    );
    defer alloc.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "${secret:literal}") != null);
    const fractional = try @import("table_contract.zig").parseCreateIndexRequest(alloc, "bad",
        \\{"type":"relational","keys":[{"expression":{"op":"literal","type":"integer","value":1.00000000000000001},"result_type":"integer"}]}
    );
    defer alloc.free(fractional);
    try std.testing.expect(std.mem.indexOf(u8, fractional, "1.00000000000000001") != null);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, create(alloc, table, "bad", fractional));
    for ([_][]const u8{
        \\{"type":"relational","keys":[{"column":"id","expression":{"op":"column","column":"id"},"result_type":"integer"}]}
        ,
        \\{"type":"relational","keys":[{"expression":{"op":"column","column":"id"}}]}
        ,
        \\{"type":"relational","keys":[{"expression":{"op":"column","column":"id","value":1},"result_type":"integer"}]}
        ,
    }) |invalid| try std.testing.expectError(error.InvalidCreateIndexRequest, @import("table_contract.zig").parseCreateIndexRequest(alloc, "bad", invalid));
}

test "relational mutation partial predicates round trip unified indexes with typed validation" {
    const alloc = std.testing.allocator;
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const table: records.TableRecord = .{ .table_id = 100, .name = "rows", .schema_json = declaration, .indexes_json = "{}" };
    const config = try @import("table_contract.zig").parseCreateIndexRequest(alloc, "partial",
        \\{"type":"relational","keys":[{"column":"id"}],"where":[{"column":"label","op":"eq","value":"active","collation":"ci"},{"column":"id","op":"gt","value":9007199254740993}]}
    );
    defer alloc.free(config);
    const created = try create(alloc, table, "partial", config);
    defer @import("../metadata/table_manager.zig").freeTable(alloc, created);
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, created.schema_json);
    defer parsed.deinit(alloc);
    const definition = parsed.relational_indexes.?.value[0];
    try std.testing.expectEqual(@as(usize, 2), definition.where.?.len);
    try std.testing.expectEqualStrings("active", definition.where.?[0].value.?.string);
    var columns: @import("relational_expression_contract.zig").ColumnTypes = .{ .alloc = alloc, .source = .{ .parsed = &parsed } };
    defer columns.deinit();
    const projected = try configForDefinition(alloc, definition, &columns);
    defer alloc.free(projected);
    try std.testing.expect(std.mem.indexOf(u8, projected, "9007199254740993") != null);
    const literal =
        \\{"type":"relational","keys":[{"column":"id"}],"where":[{"column":"label","op":"eq","value":"${secret:literal}"}]}
    ;
    const public = try indexes.encodeCreatedIndexConfig(alloc, "partial", literal);
    defer alloc.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "${secret:literal}") != null);
    const same = try create(alloc, created, "partial", projected);
    defer @import("../metadata/table_manager.zig").freeTable(alloc, same);
    try std.testing.expectEqual(@as(u32, 2), try tables.schemaVersion(same.schema_json));
    for ([_][]const u8{
        \\{"type":"relational","keys":[{"column":"id"}],"where":[{"column":"missing","op":"eq","value":1}]}
        ,
        \\{"type":"relational","keys":[{"column":"id"}],"where":[{"column":"id","op":"eq","value":"not an integer"}]}
        ,
        \\{"type":"relational","keys":[{"column":"id"}],"where":[{"column":"id","op":"is_null","value":1}]}
        ,
    }) |invalid| try std.testing.expectError(error.InvalidSchemaUpdateRequest, create(alloc, table, "bad", invalid));
}
