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

//! Typed SQL projections over literal JSON object paths. These declarations
//! do not narrow the document write-validation domain or create relations.
const std = @import("std");
pub const max_columns = 256;
pub const Kind = enum { keyword, text, integer, number, boolean, json };
pub const Column = struct { name: []const u8, path: []const []const u8, type: Kind };
pub fn validate(columns: []const Column) !void {
    if (columns.len > max_columns) return error.InvalidSchemaUpdateRequest;
    for (columns, 0..) |column, position| {
        if (column.name.len == 0 or column.name.len > 128 or std.mem.indexOfScalar(u8, column.name, 0) != null or !std.unicode.utf8ValidateSlice(column.name) or std.mem.eql(u8, column.name, "_id") or std.mem.eql(u8, column.name, "_doc") or std.mem.eql(u8, column.name, "_version") or column.path.len == 0 or column.path.len > 16) return error.InvalidSchemaUpdateRequest;
        for (column.path) |part| if (part.len == 0 or part.len > 128 or std.mem.indexOfScalar(u8, part, 0) != null or !std.unicode.utf8ValidateSlice(part)) return error.InvalidSchemaUpdateRequest;
        for (columns[0..position]) |prior| if (std.mem.eql(u8, prior.name, column.name)) return error.InvalidSchemaUpdateRequest;
    }
}
pub fn parseValue(a: std.mem.Allocator, value: std.json.Value) !std.json.Parsed([]const Column) {
    if (value != .array or value.array.items.len > max_columns) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |entry| {
        if (entry != .object) return error.InvalidSchemaUpdateRequest;
        const path = entry.object.get("path") orelse return error.InvalidSchemaUpdateRequest;
        if (path != .array or path.array.items.len == 0 or path.array.items.len > 16) return error.InvalidSchemaUpdateRequest;
        const name = entry.object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0 or name.string.len > 128) return error.InvalidSchemaUpdateRequest;
        for (path.array.items) |part| if (part != .string or part.string.len == 0 or part.string.len > 128) return error.InvalidSchemaUpdateRequest;
    }
    var parsed = std.json.parseFromValue([]const Column, a, value, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSchemaUpdateRequest;
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

test "document SQL aliases retain literal path segments and reject reserved or duplicate columns" {
    const a = std.testing.allocator;
    var body = try std.json.parseFromSlice(std.json.Value, a,
        \\[{"name":"plan","path":["account.settings","plan"],"type":"keyword"}]
    , .{});
    defer body.deinit();
    var columns = try parseValue(a, body.value);
    defer columns.deinit();
    const col = columns.value[0];
    try std.testing.expectEqualStrings("account.settings", col.path[0]);
    try std.testing.expectEqualStrings("plan", col.path[1]);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(&.{ col, col }));
    var reserved = col;
    reserved.name = "_version";
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(&.{reserved}));
}
