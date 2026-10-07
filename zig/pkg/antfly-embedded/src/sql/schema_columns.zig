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

//! Declared SQL identities at the raw durable-schema DDL boundary. Never infer
//! types from data or erase builtin widths while binding checks/defaults/indexes.
const std = @import("std");
const Json = std.json.Value;
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const Builtin = @import("../common/sql_builtin_type.zig").Type;

pub fn properties(schema: Json) !Json {
    if (schema != .object) return error.InvalidSqlBackendResponse;
    const default = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
    if (default != .string) return error.InvalidSqlBackendResponse;
    const documents = schema.object.get("document_schemas") orelse return error.InvalidSqlBackendResponse;
    if (documents != .object) return error.InvalidSqlBackendResponse;
    const document = documents.object.get(default.string) orelse return error.InvalidSqlBackendResponse;
    if (document != .object) return error.InvalidSqlBackendResponse;
    const row = document.object.get("schema") orelse return error.InvalidSqlBackendResponse;
    if (row != .object) return error.InvalidSqlBackendResponse;
    const fields = row.object.get("properties") orelse return error.InvalidSqlBackendResponse;
    if (fields != .object) return error.InvalidSqlBackendResponse;
    return fields;
}

pub fn column(name: []const u8, property: Json) !catalog.Column {
    if (property != .object) return error.InvalidSqlBackendResponse;
    const declared = property.object.get("type") orelse return error.UnsupportedSqlShape;
    const wire = if (declared == .string) declared.string else if (declared == .array) blk: {
        var selected: ?[]const u8 = null;
        for (declared.array.items) |item| {
            if (item != .string) return error.InvalidSqlBackendResponse;
            if (std.mem.eql(u8, item.string, "null")) continue;
            if (selected != null) return error.UnsupportedSqlShape;
            selected = item.string;
        }
        break :blk selected orelse return error.UnsupportedSqlShape;
    } else return error.InvalidSqlBackendResponse;
    const identity: ?Builtin = if (property.object.get("x-antfly-sql-type")) |value| blk: {
        if (value != .string) return error.InvalidSqlBackendResponse;
        break :blk std.meta.stringToEnum(Builtin, value.string) orelse return error.InvalidSqlBackendResponse;
    } else null;
    const format = property.object.get("format") orelse .null;
    if (format != .null and format != .string) return error.InvalidSqlBackendResponse;
    const kind: ast.ColumnType = if (std.mem.eql(u8, wire, "sql_array")) .array else if (std.mem.eql(u8, wire, "keyword") or std.mem.eql(u8, wire, "string") or std.mem.eql(u8, wire, "text") or std.mem.eql(u8, wire, "html") or std.mem.eql(u8, wire, "link"))
        (if (identity == .uuid or (format == .string and std.mem.eql(u8, format.string, "uuid"))) .uuid else .string)
    else if (std.mem.eql(u8, wire, "numeric") or std.mem.eql(u8, wire, "number")) .number else if (std.mem.eql(u8, wire, "object") or std.mem.eql(u8, wire, "array") or std.mem.eql(u8, wire, "json")) .json else std.meta.stringToEnum(ast.ColumnType, wire) orelse return error.UnsupportedSqlShape;
    if (kind == .array and identity == null) return error.InvalidSqlBackendResponse;
    if (identity) |builtin| if (kind != .array and kind != scalarType(builtin)) return error.InvalidSqlBackendResponse;
    return .{ .name = name, .path = name, .type = kind, .element_type = identity };
}

fn scalarType(kind: Builtin) ast.ColumnType {
    return switch (kind) {
        .text => .string,
        .int16, .int32, .int64 => .integer,
        .float32, .float64 => .number,
        .boolean => .boolean,
        .uuid => .uuid,
        .jsonb => .json,
    };
}
