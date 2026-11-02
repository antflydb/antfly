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
