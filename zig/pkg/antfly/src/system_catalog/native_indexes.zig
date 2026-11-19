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

//! Native index DDL uses the existing metadata transaction, native index
//! workers and shared catalog outcome journal. SQL adds namespace ownership.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Outcome = outcomes.Outcome;
pub const Result = outcomes.Result;
pub const names = local.system_catalog_relation_names.native_indexes;
const A = std.mem.Allocator;
pub const Preparation = struct { guard: domain.RelationMutationGuard };
pub const Prepared = struct { head: u64, revision: u64 };
pub const Mutation = struct {
    admission: outcomes.Admission,
    guard: domain.RelationMutationGuard,
    name: []const u8,
    /// Private stored configuration, including its preassigned incarnation.
    /// Null withdraws an existing SQL-owned native index.
    config_json: ?[]const u8,
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.guard.validate();
        if (!local.api_index_metadata.isValidIndexName(self.name)) return error.InvalidCatalogMutation;
        try (local.system_catalog_relation_names.Key{ .namespace_id = 1, .name = self.name }).validate();
        if (self.config_json) |config| {
            if (self.guard.owner.kind != .table or config.len == 0 or config.len > 256 * 1024) return error.InvalidCatalogMutation;
        } else if (self.guard.owner.kind != .index or !names.isNativeIdentity(self.guard.owner.publication_id) or !std.mem.eql(u8, self.guard.target.name, self.name)) return error.InvalidCatalogMutation;
    }
};
pub const OutcomeRequest = struct {
    admission: outcomes.Admission,
    target: domain.Target,
    incarnation: [16]u8,
    table_id: u64,
    name: []const u8,
    digest: [32]u8,
    operation: enum { create, drop },
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try self.target.validate();
        try (local.system_catalog_relation_names.Key{ .namespace_id = 1, .name = self.name }).validate();
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
fn hash(a: A, domain_tag: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var result = std.crypto.hash.sha2.Sha256.init(.{});
    result.update(domain_tag);
    result.update(bytes);
    return result.finalResult();
}
pub fn observation(a: A, request: Mutation) !OutcomeRequest {
    try request.validate();
    return .{ .admission = request.admission, .target = .{ .database = request.guard.target.database, .namespace = request.guard.target.namespace, .table = request.guard.logical_table }, .incarnation = request.guard.incarnation, .table_id = request.guard.owner.table_id, .name = request.name, .digest = try hash(a, "antfly:sql-native-index-mutation:v1", request), .operation = if (request.config_json != null) .create else .drop };
}
pub fn boundDigest(a: A, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    return hash(a, "antfly:sql-native-index-outcome:v1", request);
}
pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 9, .expected_revision = 0, .mutation = .{ .kind = .table, .action = .comment, .database = request.guard.target.database, .namespace = request.guard.target.namespace, .name = request.guard.logical_table }, .sql_native_index = request };
}

/// SQL has a typed native intent; private callers cannot use it to bypass the
/// artifact/resource validators of the general /indexes mutation API.
pub fn validateSqlConfig(a: A, encoded: []const u8) !void {
    var header = std.json.parseFromSlice(struct { type: enum { full_text, algebraic } }, a, encoded, .{ .ignore_unknown_fields = true }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogMutation;
    defer header.deinit();
    if (header.value.type == .algebraic) {
        const Config = struct {
            type: enum { algebraic },
            derive_from_schema: bool,
            field: []const u8,
            operator_class: local.sql_ast.NativeIndex.OperatorClass,
            _index_incarnation: u64,
        };
        var parsed = std.json.parseFromSlice(Config, a, encoded, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogMutation;
        defer parsed.deinit();
        if (!parsed.value.derive_from_schema or !validField(parsed.value.field) or parsed.value._index_incarnation == 0 or parsed.value._index_incarnation > std.math.maxInt(i64)) return error.InvalidCatalogMutation;
        return;
    }
    const Config = struct {
        type: enum { full_text },
        field: []const u8,
        analysis_config: struct { field_analyzers: std.json.ArrayHashMap([]const u8) },
        _index_incarnation: u64,
    };
    var parsed = std.json.parseFromSlice(Config, a, encoded, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogMutation;
    defer parsed.deinit();
    const config = parsed.value;
    if (config.field.len == 0 or config.field.len > 256 or !std.unicode.utf8ValidateSlice(config.field) or std.mem.indexOfScalar(u8, config.field, 0) != null or config._index_incarnation == 0 or config._index_incarnation > std.math.maxInt(i64)) return error.InvalidCatalogMutation;
    if (config.analysis_config.field_analyzers.map.count() != 1) return error.InvalidCatalogMutation;
    const analyzer = config.analysis_config.field_analyzers.map.get(config.field) orelse return error.InvalidCatalogMutation;
    if (!std.mem.eql(u8, analyzer, "standard")) return error.InvalidCatalogMutation;
}

fn validField(field: []const u8) bool {
    return field.len != 0 and field.len <= 256 and std.unicode.utf8ValidateSlice(field) and std.mem.indexOfScalar(u8, field, 0) == null;
}

/// Operator classes bind under the same immutable table guard as publication.
/// The sidecar supplies typed path postings, not btree ordering or uniqueness.
pub fn bindGinField(a: A, layout: local.storage_schema.TableSchema, logical: []const u8, operator_class: local.sql_ast.NativeIndex.OperatorClass) ![]u8 {
    if (layout.external_base_source != null or layout.storage_mode != .relational) return error.UnsupportedSqlShape;
    const column = for (layout.relational_columns) |candidate| {
        if (std.mem.eql(u8, candidate.name, logical)) break candidate;
    } else return error.UnknownColumn;
    switch (operator_class) {
        .jsonb_path_ops => if (column.column_type != .json or (column.sql_element_type != null and column.sql_element_type != .jsonb)) return error.SqlIndexOperatorClassTypeMismatch,
        .array_ops => if (column.column_type != .sql_array) return error.SqlIndexOperatorClassTypeMismatch,
    }
    return selectorAlloc(a, &.{column.path});
}

/// Generic schema writers must preserve the SQL type binding, too. A successful
/// sidecar rebuild cannot certify a missing/retyped source.
pub fn validateStoredGinBindings(a: A, schema_json: []const u8, indexes_json: []const u8, selected_names: []const u8) !void {
    if (selected_names.len == 0) return;
    var bindings = try names.read(a, selected_names, indexes_json);
    defer bindings.deinit();
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var layout: ?local.storage_schema.TableSchema = null;
    for (bindings.entries) |binding| if (binding.gin) |gin| {
        if (layout == null) {
            const parsed = local.schema_mod.parseValidatedTableSchema(scratch, schema_json) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
            layout = local.schema_mod.deriveRuntimeTableSchema(scratch, parsed) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
        }
        const bound = bindStoredGinField(scratch, layout.?, gin) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, bound, gin.field)) return error.InvalidSchemaUpdateRequest;
    };
}

// Namespace custody records physical field identities, not mutable SQL names.
// Resolve the current logical name from the pinned layout before checking its
// operator class; an alias cannot retarget a retained definition.
fn bindStoredGinField(a: A, layout: local.storage_schema.TableSchema, gin: names.GinBinding) ![]u8 {
    const logical = for (layout.relational_columns) |column| {
        if (std.mem.eql(u8, column.path, gin.field)) break column.name;
    } else return error.UnknownColumn;
    return bindGinField(a, layout, logical, gin.operator_class);
}

/// Bind the SQL column using the same declared layout as reads/mutations.
/// Native field selectors currently address dot-separated object keys. Literal
/// dots in one key have no unambiguous representation; never publish an empty
/// or incorrectly targeted index for that shape.
pub fn bindSqlField(a: A, parsed: local.schema_mod.ParsedTableSchema, layout: local.storage_schema.TableSchema, logical: []const u8) ![]u8 {
    if (layout.external_base_source != null) return error.UnsupportedSqlShape;
    if (layout.storage_mode == .relational) {
        const column = for (layout.relational_columns) |candidate| {
            if (std.mem.eql(u8, candidate.name, logical)) break candidate;
        } else return error.UnknownColumn;
        if (column.column_type != .string or (column.sql_element_type != null and column.sql_element_type != .text)) return error.SqlTypeMismatch;
        return selectorAlloc(a, &.{column.path});
    }
    const document = local.sql_document_row;
    const columns = try document.deriveColumns(a, parsed);
    defer document.freeColumns(a, columns);
    const column = for (columns) |candidate| {
        if (std.mem.eql(u8, candidate.name, logical)) break candidate;
    } else return error.UnknownColumn;
    if (column.type != .string or (column.element_type != null and column.element_type != .text)) return error.SqlTypeMismatch;
    return selectorAlloc(a, if (column.document_path.len != 0) column.document_path else &.{column.path});
}

fn selectorAlloc(a: A, parts: []const []const u8) ![]u8 {
    if (parts.len == 0) return error.UnsupportedSqlShape;
    var bytes: usize = parts.len - 1;
    for (parts) |part| {
        if (part.len == 0 or part.len > 256 or std.mem.indexOfAny(u8, part, ".\x00") != null or !std.unicode.utf8ValidateSlice(part)) return error.UnsupportedSqlShape;
        bytes += part.len;
        if (bytes > 256) return error.UnsupportedSqlShape;
    }
    return std.mem.join(a, ".", parts);
}

test "SQL native index field binding shares declared document aliases and typed row guards" {
    const a = std.testing.allocator;
    const json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{"type":"object","properties":{"state":{"type":"keyword","x-antfly-column-name":"status"}}},"count":{"type":"integer"},"literal.dot":{"type":"text"}},"additionalProperties":true}}}}
    ;
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, json);
    defer parsed.deinit(a);
    const layout = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, layout);
    const Probe = struct {
        fn run(alloc: A, schema: local.schema_mod.ParsedTableSchema, native: local.storage_schema.TableSchema) !void {
            const field = try bindSqlField(alloc, schema, native, "status");
            defer alloc.free(field);
            try std.testing.expectEqualStrings("metadata.state", field);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ parsed, layout });
    try std.testing.expectError(error.UnknownColumn, bindSqlField(a, parsed, layout, "missing"));
    try std.testing.expectError(error.SqlTypeMismatch, bindSqlField(a, parsed, layout, "count"));
    try std.testing.expectError(error.UnsupportedSqlShape, bindSqlField(a, parsed, layout, "literal.dot"));
    const relational: local.storage_schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "title", .path = "title", .column_type = .string, .sql_element_type = .text },
        .{ .name = "id", .path = "id", .column_type = .string, .sql_element_type = .uuid },
        .{ .name = "literal.dot", .path = "literal.dot", .column_type = .string, .sql_element_type = .text },
    } };
    const field = try bindSqlField(a, parsed, relational, "title");
    defer a.free(field);
    try std.testing.expectEqualStrings("title", field);
    try std.testing.expectError(error.SqlTypeMismatch, bindSqlField(a, parsed, relational, "id"));
    try std.testing.expectError(error.UnsupportedSqlShape, bindSqlField(a, parsed, relational, "literal.dot"));
}

test "SQL native index configuration prevents artifact and analyzer authority injection" {
    const a = std.testing.allocator;
    const valid = "{\"type\":\"full_text\",\"field\":\"status\",\"_index_incarnation\":17,\"analysis_config\":{\"field_analyzers\":{\"status\":\"standard\"}}}";
    try validateSqlConfig(a, valid);
    for ([_][]const u8{
        "{\"type\":\"full_text\",\"field\":\"status\",\"_index_incarnation\":17,\"artifact_name\":\"secret\",\"analysis_config\":{\"field_analyzers\":{\"status\":\"standard\"}}}",
        "{\"type\":\"full_text\",\"field\":\"status\",\"_index_incarnation\":0,\"analysis_config\":{\"field_analyzers\":{\"status\":\"standard\"}}}",
        "{\"type\":\"full_text\",\"field\":\"status\",\"_index_incarnation\":17,\"analysis_config\":{\"field_analyzers\":{\"other\":\"standard\"}}}",
    }) |bad| try std.testing.expectError(error.InvalidCatalogMutation, validateSqlConfig(a, bad));
    const Probe = struct {
        fn run(alloc: A, config: []const u8) !void {
            try validateSqlConfig(alloc, config);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{valid});
}

test "SQL GIN configuration binds typed columns and excludes caller supplied materializations" {
    const a = std.testing.allocator;
    const valid = "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"field\":\"metadata\",\"operator_class\":\"jsonb_path_ops\",\"_index_incarnation\":17}";
    try validateSqlConfig(a, valid);
    for ([_][]const u8{
        "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"field\":\"metadata\",\"operator_class\":\"jsonb_path_ops\",\"_index_incarnation\":17,\"materializations\":[]}",
        "{\"type\":\"algebraic\",\"derive_from_schema\":false,\"field\":\"metadata\",\"operator_class\":\"jsonb_path_ops\",\"_index_incarnation\":17}",
        "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"field\":\"metadata\",\"operator_class\":\"array_ops\",\"_index_incarnation\":0}",
    }) |bad| try std.testing.expectError(error.InvalidCatalogMutation, validateSqlConfig(a, bad));
    const layout: local.storage_schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "metadata", .path = "metadata", .column_type = .json, .sql_element_type = .jsonb },
        .{ .name = "tags", .path = "tags", .column_type = .sql_array, .sql_element_type = .text },
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "native_json", .path = "native_json", .column_type = .json },
        .{ .name = "aliased_json", .path = "physical_json", .column_type = .json, .sql_element_type = .jsonb },
    } };
    const Probe = struct {
        fn run(alloc: A, schema: local.storage_schema.TableSchema, config: []const u8) !void {
            try validateSqlConfig(alloc, config);
            const json = try bindGinField(alloc, schema, "metadata", .jsonb_path_ops);
            defer alloc.free(json);
            try std.testing.expectEqualStrings("metadata", json);
            const array = try bindGinField(alloc, schema, "tags", .array_ops);
            defer alloc.free(array);
            try std.testing.expectEqualStrings("tags", array);
            const native_json = try bindGinField(alloc, schema, "native_json", .jsonb_path_ops);
            defer alloc.free(native_json);
            try std.testing.expectEqualStrings("native_json", native_json);
            const alias = try bindGinField(alloc, schema, "aliased_json", .jsonb_path_ops);
            defer alloc.free(alias);
            try std.testing.expectEqualStrings("physical_json", alias);
            const retained = try bindStoredGinField(alloc, schema, .{ .name = "gin", .field = "physical_json", .operator_class = .jsonb_path_ops });
            defer alloc.free(retained);
            try std.testing.expectEqualStrings(alias, retained);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Probe.run, .{ layout, valid });
    try std.testing.expectError(error.UnknownColumn, bindGinField(a, layout, "missing", .array_ops));
    try std.testing.expectError(error.SqlIndexOperatorClassTypeMismatch, bindGinField(a, layout, "tags", .jsonb_path_ops));
    try std.testing.expectError(error.SqlIndexOperatorClassTypeMismatch, bindGinField(a, layout, "metadata", .array_ops));
    try std.testing.expectError(error.SqlIndexOperatorClassTypeMismatch, bindGinField(a, layout, "id", .array_ops));
}

test "SQL GIN stored bindings survive regeneration and reject missing or retyped fields" {
    const a = std.testing.allocator;
    var created = try local.sql_compiler.compile(a, "CREATE TABLE items(id BIGINT PRIMARY KEY,metadata JSONB,tags TEXT[])", .{});
    defer created.deinit();
    const schema = try local.sql_ddl_runtime.createSchemaAlloc(a, created.statement.create_table);
    defer a.free(schema);
    const indexes = "{\"gin\":{\"type\":\"algebraic\",\"derive_from_schema\":true,\"_index_incarnation\":17}}";
    const selected = try names.addGin(a, "", .{ .name = "gin", .field = "metadata", .operator_class = .jsonb_path_ops });
    defer a.free(selected);
    try validateStoredGinBindings(a, schema, indexes, selected);
    const regenerated = try @import("../api/tables.zig").regenerateAlgebraicIndexesFromSchemaAlloc(a, "items", indexes, schema);
    defer a.free(regenerated);
    try validateStoredGinBindings(a, schema, regenerated, selected);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, regenerated, .{});
    defer parsed.deinit();
    const gin = parsed.value.object.get("gin").?.object;
    try std.testing.expect(gin.get("field") == null and gin.get("operator_class") == null);
    try std.testing.expectEqual(@as(i64, 17), gin.get("_index_incarnation").?.integer);
    var retyped = try local.sql_compiler.compile(a, "CREATE TABLE items(id BIGINT PRIMARY KEY,metadata TEXT,tags TEXT[])", .{});
    defer retyped.deinit();
    const wrong = try local.sql_ddl_runtime.createSchemaAlloc(a, retyped.statement.create_table);
    defer a.free(wrong);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validateStoredGinBindings(a, wrong, regenerated, selected));
    const missing = try names.addGin(a, "", .{ .name = "gin", .field = "missing", .operator_class = .jsonb_path_ops });
    defer a.free(missing);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validateStoredGinBindings(a, schema, regenerated, missing));
}
