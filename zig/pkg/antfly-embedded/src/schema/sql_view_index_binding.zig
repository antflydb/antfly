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

//! Same canonical definition binding used by SQL preparation and the native
//! metadata writer. Runtime readiness belongs to retained physical owners.
const std = @import("std");
const policy = @import("sql_view_requirements.zig");
const required = @import("../common/sql_view_index_requirement.zig");
const configs = @import("../api/table_index_config.zig");
const coverage = @import("../api/coverage_policy.zig");
pub const Requirement = required.Requirement;

/// Inspect only the two authored table-level declarations. A document
/// property's coincidentally equal name is data, never an admission marker.
pub fn usesSchemaDeclaration(a: std.mem.Allocator, schema_json: []const u8) !bool {
    const Header = struct { document_sql_columns: ?[]const std.json.Value = null, sql_view_requirements: ?[]const std.json.Value = null };
    var header = try std.json.parseFromSlice(Header, a, schema_json, .{ .ignore_unknown_fields = true });
    defer header.deinit();
    return (header.value.document_sql_columns != null and header.value.document_sql_columns.?.len != 0) or
        (header.value.sql_view_requirements != null and header.value.sql_view_requirements.?.len != 0);
}

pub fn capture(a: std.mem.Allocator, schema_json: []const u8, indexes_json: []const u8, source_database: []const u8, source_namespace: []const u8, view_database: []const u8, view_namespace: []const u8, view_name: []const u8) ![]const Requirement {
    if (!std.mem.eql(u8, source_database, view_database) or !std.mem.eql(u8, source_namespace, view_namespace)) return &.{};
    const Header = struct { storage_mode: ?[]const u8 = null, sql_view_requirements: ?std.json.Value = null };
    var header = try std.json.parseFromSlice(Header, a, schema_json, .{ .ignore_unknown_fields = true });
    defer header.deinit();
    const authored = header.value.sql_view_requirements orelse return &.{};
    var declarations = try policy.parseValue(a, authored);
    defer declarations.deinit();
    const declaration = policy.forView(declarations.value, view_name) orelse return &.{};
    if (!std.mem.eql(u8, header.value.storage_mode orelse "document", "document")) return error.InvalidSchemaUpdateRequest;
    var indexes = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer indexes.deinit();
    if (indexes.value != .object) return error.InvalidIndexConfig;
    const result = try a.alloc(Requirement, declaration.required_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |entry| a.free(entry.name);
        a.free(result);
    }
    for (declaration.required_indexes, result) |name, *out| {
        out.* = try fromConfig(a, indexes.value, name);
        initialized += 1;
    }
    return result;
}
pub fn requireSame(a: std.mem.Allocator, actual: []const Requirement, expected: []const Requirement) !void {
    _ = a;
    try required.validate(actual);
    try required.validate(expected);
    if (actual.len != expected.len) return error.CatalogGenerationChanged;
    for (actual, expected) |left, right| if (!left.eql(right)) return error.CatalogGenerationChanged;
}

pub const validate = required.validate;
fn fromConfig(a: std.mem.Allocator, indexes: std.json.Value, name: []const u8) !Requirement {
    if (indexes != .object) return error.InvalidIndexConfig;
    const config = indexes.object.get(name) orelse return error.IndexNotFound;
    if (try configs.parseIndexKind(config) != .full_text) return error.InvalidIndexConfig;
    const incarnation = coverage.incarnation(config) orelse return error.IndexNotFound;
    const runtime_json = try configs.extractIndexConfigJson(a, name, config);
    defer a.free(runtime_json);
    const config_hash = try @import("../storage/internal_keys.zig").derivedCoverageConfigFingerprint(a, runtime_json);
    return .{ .name = try a.dupe(u8, name), .incarnation = incarnation, .config_hash = config_hash };
}
/// Already-admitted dependencies survive a canonical view rename. A later
/// policy edit cannot relax them; the immutable captured source schema is
/// checked by the caller before this exact live index definition check.
pub fn requireCurrent(a: std.mem.Allocator, indexes_json: []const u8, expected: []const Requirement) !void {
    try required.validate(expected);
    if (expected.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer parsed.deinit();
    for (expected) |guard| {
        const actual = try fromConfig(a, parsed.value, guard.name);
        defer a.free(actual.name);
        if (!guard.eql(actual)) return error.CatalogGenerationChanged;
    }
}

pub const digest = required.digest;
pub const requireDigest = required.requireDigest;

test "document SQL view requirements bind only actual source namespace logical index incarnation and semantic configuration" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const declaration = "{\"version\":1,\"storage_mode\":\"document\",\"sql_view_requirements\":[{\"view_name\":\"support_view\",\"required_indexes\":[\"support_fts\"]}]}";
    const index = "{\"support_fts\":{\"type\":\"full_text\",\"_index_incarnation\":11}}";
    const bound = try capture(a, declaration, index, "app", "public", "app", "public", "support_view");
    try std.testing.expectEqual(@as(usize, 1), bound.len);
    try requireCurrent(a, index, bound);
    try std.testing.expectEqual(@as(usize, 0), (try capture(a, declaration, index, "app", "public", "other", "public", "support_view")).len);
    try std.testing.expectEqual(@as(usize, 0), (try capture(a, declaration, index, "app", "public", "app", "foreign", "support_view")).len);
    try std.testing.expectEqual(@as(usize, 0), (try capture(a, declaration, index, "app", "public", "app", "public", "unrelated_view")).len);
    try std.testing.expectError(error.IndexNotFound, capture(a, declaration, "{}", "app", "public", "app", "public", "support_view"));
    try std.testing.expectError(error.IndexNotFound, capture(a, declaration, "{\"support_fts\":{\"type\":\"full_text\"}}", "app", "public", "app", "public", "support_view"));
    try std.testing.expectError(error.CatalogGenerationChanged, requireCurrent(a, "{\"support_fts\":{\"type\":\"full_text\",\"_index_incarnation\":12}}", bound));
    try std.testing.expectError(error.CatalogGenerationChanged, requireCurrent(a, "{\"support_fts\":{\"type\":\"full_text\",\"_index_incarnation\":11,\"field\":\"other\"}}", bound));
    try std.testing.expectError(error.UnsupportedSqlExecution, requireDigest(bound, null));
    try std.testing.expectError(error.UnsupportedSqlExecution, requireDigest(bound, @splat(0)));
    try requireDigest(bound, try digest(bound));
}

test "document SQL view admission markers do not reinterpret arbitrary schema property names" {
    try std.testing.expect(!try usesSchemaDeclaration(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":{\"properties\":{\"required_indexes\":{},\"sql_view_requirements\":{},\"document_sql_columns\":{}}}}}}"));
    try std.testing.expect(!try usesSchemaDeclaration(std.testing.allocator, "{\"document_sql_columns\":[],\"sql_view_requirements\":[]}"));
    try std.testing.expect(try usesSchemaDeclaration(std.testing.allocator, "{\"document_sql_columns\":[{}]}"));
}
