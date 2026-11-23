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

//! Schema-owned dependency policy for ordinary canonical SQL views. A policy
//! never creates a relation or grants an owner; CREATE VIEW binds it only in
//! the actual source relation's namespace. Paths/types remain schema aliases.
const std = @import("std");
pub const max_views = 64;
pub const max_indexes = 16;
pub const Declaration = struct {
    view_name: []const u8,
    required_indexes: []const []const u8,
};

fn validName(name: []const u8) bool {
    if (name.len == 0 or name.len > 128 or !std.unicode.utf8ValidateSlice(name)) return false;
    for (name) |byte| if (byte < 32 or byte == 127 or byte == '/' or byte == '\\') return false;
    return true;
}
pub fn validate(declarations: []const Declaration) !void {
    if (declarations.len > max_views) return error.InvalidSchemaUpdateRequest;
    for (declarations, 0..) |declaration, position| {
        if (!validName(declaration.view_name) or declaration.required_indexes.len == 0 or declaration.required_indexes.len > max_indexes) return error.InvalidSchemaUpdateRequest;
        for (declarations[0..position]) |prior| if (std.mem.eql(u8, prior.view_name, declaration.view_name)) return error.InvalidSchemaUpdateRequest;
        for (declaration.required_indexes, 0..) |name, index| {
            if (!validName(name)) return error.InvalidSchemaUpdateRequest;
            for (declaration.required_indexes[0..index]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidSchemaUpdateRequest;
        }
    }
}
pub fn parseValue(a: std.mem.Allocator, value: std.json.Value) !std.json.Parsed([]const Declaration) {
    if (value != .array or value.array.items.len > max_views) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |entry| {
        if (entry != .object) return error.InvalidSchemaUpdateRequest;
        const indexes = entry.object.get("required_indexes") orelse return error.InvalidSchemaUpdateRequest;
        if (indexes != .array or indexes.array.items.len == 0 or indexes.array.items.len > max_indexes) return error.InvalidSchemaUpdateRequest;
    }
    var parsed = std.json.parseFromValue([]const Declaration, a, value, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}
pub fn forView(declarations: []const Declaration, name: []const u8) ?Declaration {
    for (declarations) |declaration| if (std.mem.eql(u8, declaration.view_name, name)) return declaration;
    return null;
}

test "document SQL view policy is bounded literal source-local data without supplied owner or runtime identity" {
    const a = std.testing.allocator;
    var value = try std.json.parseFromSlice(std.json.Value, a, "[{\"view_name\":\"support_view\",\"required_indexes\":[\"support_fts\"]}]", .{});
    defer value.deinit();
    var policy = try parseValue(a, value.value);
    defer policy.deinit();
    try std.testing.expectEqualStrings("support_fts", forView(policy.value, "support_view").?.required_indexes[0]);
    try std.testing.expect(forView(policy.value, "foreign_view") == null);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(&.{.{ .view_name = "other/support_view", .required_indexes = &.{"fts"} }}));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(&.{.{ .view_name = "support_view", .required_indexes = &.{ "fts", "fts" } }}));
    var forged = try std.json.parseFromSlice(std.json.Value, a, "[{\"view_name\":\"support_view\",\"required_indexes\":[\"fts\"],\"owner\":\"admin\"}]", .{});
    defer forged.deinit();
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValue(a, forged.value));
}
