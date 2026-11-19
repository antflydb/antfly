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

//! LIKE copies declarations into the ordinary schema/publication pipeline.
//! Live physical identities, policies, row data and counter state never copy.
const std = @import("std");
const ast = @import("ast.zig");
const names = @import("../system_catalog/constraint_names.zig");
const columns = @import("schema_columns.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;

fn rowSchema(schema: Json) !Json {
    if (schema != .object) return error.InvalidSqlBackendResponse;
    const mode = schema.object.get("storage_mode") orelse return error.UnsupportedSqlShape;
    if (mode != .string or !std.mem.eql(u8, mode.string, "relational")) return error.UnsupportedSqlShape;
    const default = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
    if (default != .string) return error.InvalidSqlBackendResponse;
    const documents = schema.object.get("document_schemas") orelse return error.InvalidSqlBackendResponse;
    if (documents != .object) return error.InvalidSqlBackendResponse;
    const document = documents.object.get(default.string) orelse return error.InvalidSqlBackendResponse;
    if (document != .object) return error.InvalidSqlBackendResponse;
    const row = document.object.get("schema") orelse return error.InvalidSqlBackendResponse;
    if (row != .object) return error.InvalidSqlBackendResponse;
    return row;
}

pub fn appendProperties(a: A, destination: *std.json.ObjectMap, required: *std.ArrayList([]const u8), source: Json, options: ast.LikeOptions) !void {
    const row = try rowSchema(source);
    const properties = try columns.properties(source);
    const mandatory = row.object.get("required") orelse Json{ .array = .init(a) };
    if (mandatory != .array) return error.InvalidSqlBackendResponse;
    if (properties.object.count() > 256 -| destination.count()) return error.SqlLimitExceeded;
    var iterator = properties.object.iterator();
    while (iterator.next()) |entry| {
        const name = entry.key_ptr.*;
        if (std.mem.eql(u8, name, "_id") or destination.contains(name)) return error.DuplicateSqlColumn;
        if (entry.value_ptr.* != .object) return error.InvalidSqlBackendResponse;
        // Maps belong to the request region; duplicating the shallow map
        // keeps source descriptors immutable while strings/trees are borrowed.
        var property = entry.value_ptr.*;
        property.object = try property.object.clone(a);
        if (!options.comments) _ = property.object.orderedRemove("description");
        try destination.put(a, name, property);
        for (mandatory.array.items) |field| {
            if (field != .string) return error.InvalidSqlBackendResponse;
            if (std.mem.eql(u8, field.string, name)) {
                try required.append(a, name);
                break;
            }
        }
    }
}

fn list(a: A, schema: *Json, key: []const u8) !*std.array_list.Managed(Json) {
    if (!schema.object.contains(key)) try schema.object.put(a, key, .{ .array = .init(a) });
    const entry = schema.object.getPtr(key).?;
    if (entry.* != .array) return error.InvalidSqlBackendResponse;
    return &entry.array;
}

fn declarations(source: Json, key: []const u8) ![]const Json {
    const value = source.object.get(key) orelse return &.{};
    if (value != .array or value.array.items.len > 256) return error.InvalidSqlBackendResponse;
    return value.array.items;
}

fn string(value: Json, key: []const u8) ![]const u8 {
    if (value != .object) return error.InvalidSqlBackendResponse;
    const field = value.object.get(key) orelse return error.InvalidSqlBackendResponse;
    if (field != .string) return error.InvalidSqlBackendResponse;
    return field.string;
}

fn copy(a: A, value: Json) !Json {
    if (value != .object) return error.InvalidSqlBackendResponse;
    return .{ .object = try value.object.clone(a) };
}

fn namesForRule(a: A, rule: Json) ![]const []const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    if (rule.object.get("columns")) |fields| {
        if (fields != .array) return error.InvalidSqlBackendResponse;
        for (fields.array.items) |field| {
            if (field != .string) return error.InvalidSqlBackendResponse;
            try result.append(a, field.string);
        }
    } else if (rule.object.get("keys")) |keys| {
        if (keys != .array) return error.InvalidSqlBackendResponse;
        for (keys.array.items) |key| {
            if (key != .object) return error.InvalidSqlBackendResponse;
            try result.append(a, if (key.object.get("column")) |field| if (field == .string) field.string else return error.InvalidSqlBackendResponse else "expr");
        }
    }
    if (rule.object.get("include_columns")) |fields| {
        if (fields != .array) return error.InvalidSqlBackendResponse;
        for (fields.array.items) |field| {
            if (field != .string) return error.InvalidSqlBackendResponse;
            try result.append(a, field.string);
        }
    }
    if (result.items.len == 0) return error.InvalidSqlBackendResponse;
    return result.items;
}

fn chooseName(a: A, schema: Json, table: []const u8, rule: Json, index: bool) ![]const u8 {
    const fields = try namesForRule(a, rule);
    const primary = if (rule.object.get("primary")) |value| if (value == .bool) value.bool else return error.InvalidSqlBackendResponse else false;
    var candidate = if (index) try names.Candidate.initIndex(table, fields) else try names.Candidate.init(table, fields, primary);
    for (0..4096) |attempt| {
        var buffer: [names.max_name_bytes]u8 = undefined;
        const name = try candidate.format(@intCast(attempt), &buffer);
        if (std.mem.eql(u8, name, table)) continue;
        const occupied = occupied: {
            for ([_][]const u8{ "unique_constraints", "checks", "foreign_keys", "relational_indexes" }) |key| {
                for (try declarations(schema, key)) |entry| if (std.mem.eql(u8, try string(entry, "name"), name)) break :occupied true;
            }
            break :occupied false;
        };
        if (!occupied) return a.dupe(u8, name);
    }
    return error.SqlLimitExceeded;
}

pub fn appendDecorations(a: A, destination: *Json, source: Json, options: ast.LikeOptions, table: []const u8, sequences: []const @import("../system_catalog/column_sequences.zig").Creation) !void {
    _ = try rowSchema(source);
    for (try declarations(source, "column_defaults")) |entry| {
        if (entry != .object) return error.InvalidSqlBackendResponse;
        if (entry.object.get("sequence")) |reference| {
            var parsed = try @import("../system_catalog/column_sequences.zig").Binding.parse(a, reference);
            defer parsed.deinit();
            const selected = if (parsed.value.generation == .serial) options.defaults else options.identity;
            if (selected) {
                const name = try string(entry, "column");
                const creation = for (sequences) |intent| {
                    if (std.mem.eql(u8, intent.column, name)) break intent;
                } else return error.UnsupportedSqlExecution;
                try creation.validate();
                if (creation.generation != parsed.value.generation) return error.InvalidSequenceDefinition;
                // The containing native transaction resolves guarded source
                // intent and binds the destination's stable attribute.
            }
        } else if (options.defaults) try (try list(a, destination, "column_defaults")).append(entry);
    }
    if (options.generated) for (try declarations(source, "generated_columns")) |entry| {
        try (try list(a, destination, "generated_columns")).append(entry);
    };
    if (options.constraints) for (try declarations(source, "checks")) |entry| {
        const name = try string(entry, "name");
        const checks = try list(a, destination, "checks");
        for (checks.items) |prior| {
            if (std.mem.eql(u8, try string(prior, "name"), name)) return error.SqlConstraintAlreadyExists;
        }
        var declaration = try copy(a, entry);
        if (!options.comments) _ = declaration.object.orderedRemove("description");
        try checks.append(declaration);
    };
    if (!options.indexes) return;
    // Index-owned UNIQUE rules share exactly the destination index name.
    var renamed: std.StringHashMapUnmanaged([]const u8) = .empty;
    for (try declarations(source, "relational_indexes")) |entry| {
        var declaration = try copy(a, entry);
        if (!options.comments) _ = declaration.object.orderedRemove("description");
        const old = try string(entry, "name");
        const name = try chooseName(a, destination.*, table, entry, true);
        try declaration.object.put(a, "name", .{ .string = name });
        try renamed.put(a, old, name);
        try (try list(a, destination, "relational_indexes")).append(declaration);
    }
    for (try declarations(source, "unique_constraints")) |entry| {
        var declaration = try copy(a, entry);
        // PostgreSQL LIKE copies a UNIQUE's backing-index comment, not
        // the newly allocated pg_constraint annotation. The index copy intent
        // captures its effective source text independently at admission.
        _ = declaration.object.orderedRemove("description");
        const old = try string(entry, "name");
        const name = renamed.get(old) orelse try chooseName(a, destination.*, table, entry, false);
        try declaration.object.put(a, "name", .{ .string = name });
        try (try list(a, destination, "unique_constraints")).append(declaration);
    }
}
