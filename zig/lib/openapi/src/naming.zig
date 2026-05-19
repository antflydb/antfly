// Copyright 2026 Antfly, Inc.
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

//! Identifier naming utilities for OpenAPI → Zig code generation.
//!
//! Handles conversion between OpenAPI naming conventions and Zig identifiers:
//! - Schema names (PascalCase) → Zig type names
//! - Property names (camelCase/snake_case) → Zig field names (snake_case)
//! - Reserved word escaping

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Zig reserved words that need escaping with @"..."
const reserved_words = std.StaticStringMap(void).initComptime(.{
    .{ "addrspace", {} },
    .{ "align", {} },
    .{ "allowzero", {} },
    .{ "and", {} },
    .{ "anyframe", {} },
    .{ "anytype", {} },
    .{ "asm", {} },
    .{ "async", {} },
    .{ "await", {} },
    .{ "break", {} },
    .{ "catch", {} },
    .{ "comptime", {} },
    .{ "const", {} },
    .{ "continue", {} },
    .{ "defer", {} },
    .{ "else", {} },
    .{ "enum", {} },
    .{ "errdefer", {} },
    .{ "error", {} },
    .{ "export", {} },
    .{ "extern", {} },
    .{ "false", {} },
    .{ "fn", {} },
    .{ "for", {} },
    .{ "if", {} },
    .{ "inline", {} },
    .{ "linksection", {} },
    .{ "noalias", {} },
    .{ "nosuspend", {} },
    .{ "null", {} },
    .{ "opaque", {} },
    .{ "or", {} },
    .{ "orelse", {} },
    .{ "packed", {} },
    .{ "pub", {} },
    .{ "resume", {} },
    .{ "return", {} },
    .{ "struct", {} },
    .{ "suspend", {} },
    .{ "switch", {} },
    .{ "test", {} },
    .{ "threadlocal", {} },
    .{ "true", {} },
    .{ "try", {} },
    .{ "type", {} },
    .{ "undefined", {} },
    .{ "union", {} },
    .{ "unreachable", {} },
    .{ "var", {} },
    .{ "volatile", {} },
    .{ "while", {} },
});

pub fn isReserved(name: []const u8) bool {
    return reserved_words.has(name);
}

/// Convert an OpenAPI schema name to a Zig type name (PascalCase).
/// Examples: "cluster_status" → "ClusterStatus", "ClusterStatus" → "ClusterStatus"
pub fn toTypeName(allocator: Allocator, name: []const u8) ![]u8 {
    return toPascalCase(allocator, name);
}

/// Convert an OpenAPI property name to a Zig field name (snake_case).
pub fn toFieldName(allocator: Allocator, name: []const u8) ![]u8 {
    return toSnakeCase(allocator, name);
}

/// Returns the field name as it should appear in generated Zig code.
/// Converts to snake_case and escapes reserved words with @"...".
pub fn zigFieldName(allocator: Allocator, name: []const u8) ![]u8 {
    // If already valid and doesn't need conversion, fast-path
    if (!needsConversion(name) and !isReserved(name) and !needsEscaping(name)) {
        return allocator.dupe(u8, name);
    }
    const snake = try toSnakeCase(allocator, name);
    if (isReserved(snake) or needsEscaping(snake)) {
        const result = try std.fmt.allocPrint(allocator, "@\"{s}\"", .{snake});
        allocator.free(snake);
        return result;
    }
    return snake;
}

fn needsConversion(name: []const u8) bool {
    for (name) |c| {
        if (std.ascii.isUpper(c) or c == '-' or c == ' ' or c == '.') return true;
    }
    return false;
}

fn needsEscaping(name: []const u8) bool {
    if (name.len == 0) return true;
    // Must start with letter or underscore
    if (!std.ascii.isAlphabetic(name[0]) and name[0] != '_') return true;
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_') return true;
    }
    return false;
}

/// Convert to PascalCase: "cluster_status" → "ClusterStatus", "camelCase" → "CamelCase"
fn toPascalCase(allocator: Allocator, name: []const u8) ![]u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    var capitalize_next = true;

    for (name) |c| {
        if (c == '_' or c == '-' or c == ' ' or c == '.') {
            capitalize_next = true;
            continue;
        }
        if (capitalize_next) {
            try result.append(allocator, std.ascii.toUpper(c));
            capitalize_next = false;
        } else {
            try result.append(allocator, c);
        }
    }

    return result.toOwnedSlice(allocator);
}

/// Convert to snake_case: "CamelCase" → "camel_case", "HTTPStatus" → "http_status"
fn toSnakeCase(allocator: Allocator, name: []const u8) ![]u8 {
    var result = std.ArrayListUnmanaged(u8).empty;

    for (name, 0..) |c, i| {
        if (c == '-' or c == ' ' or c == '.') {
            try result.append(allocator, '_');
            continue;
        }
        if (std.ascii.isUpper(c)) {
            // Insert underscore before uppercase if:
            // - not at start
            // - previous char was lowercase, OR
            // - next char is lowercase (for "HTTPStatus" → "http_status")
            if (i > 0) {
                const prev = name[i - 1];
                const next_lower = (i + 1 < name.len) and std.ascii.isLower(name[i + 1]);
                if (std.ascii.isLower(prev) or next_lower) {
                    try result.append(allocator, '_');
                }
            }
            try result.append(allocator, std.ascii.toLower(c));
        } else {
            try result.append(allocator, c);
        }
    }

    return result.toOwnedSlice(allocator);
}

/// Convert an operationId to a Zig method name (camelCase).
/// "getClusterStatus" stays "getClusterStatus", "get_status" → "getStatus"
/// "admin-api-keys-list" → "adminApiKeysList"
pub fn toMethodName(allocator: Allocator, operation_id: []const u8) ![]u8 {
    // If already camelCase (starts lowercase, no separators), use as-is
    if (operation_id.len > 0 and std.ascii.isLower(operation_id[0]) and
        std.mem.indexOfAny(u8, operation_id, "_-") == null)
    {
        return allocator.dupe(u8, operation_id);
    }
    // Convert snake_case, kebab-case, or PascalCase to camelCase
    const result = try toPascalCase(allocator, operation_id);
    if (result.len == 0) return result;
    result[0] = std.ascii.toLower(result[0]);
    return result;
}

/// Extract the schema name from a $ref string.
/// "#/components/schemas/ClusterStatus" → "ClusterStatus"
/// "../../lib/schema/openapi.yaml#/components/schemas/TableSchema" → "TableSchema"
pub fn refToName(ref: []const u8) ?[]const u8 {
    // Handle external file refs: find the "#" anchor first
    const fragment = if (std.mem.indexOf(u8, ref, "#")) |hash_pos|
        ref[hash_pos..]
    else
        ref;

    const prefixes = [_][]const u8{
        "#/components/schemas/",
        "#/components/parameters/",
        "#/components/requestBodies/",
        "#/components/responses/",
    };
    for (prefixes) |p| {
        if (std.mem.startsWith(u8, fragment, p)) {
            return fragment[p.len..];
        }
    }
    return null;
}

/// Check if a $ref points to an external file (contains a path before #).
/// "../../lib/schema/openapi.yaml#/components/schemas/Foo" → true
/// "#/components/schemas/Foo" → false
pub fn isExternalRef(ref: []const u8) bool {
    if (ref.len == 0 or ref[0] == '#') return false;
    return std.mem.indexOf(u8, ref, "#") != null;
}

/// Extract the file path from an external $ref.
/// "../../lib/schema/openapi.yaml#/components/schemas/Foo" → "../../lib/schema/openapi.yaml"
pub fn refToFilePath(ref: []const u8) ?[]const u8 {
    const hash_pos = std.mem.indexOf(u8, ref, "#") orelse return null;
    if (hash_pos == 0) return null;
    return ref[0..hash_pos];
}

test "toPascalCase" {
    const alloc = std.testing.allocator;

    const r1 = try toPascalCase(alloc, "cluster_status");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("ClusterStatus", r1);

    const r2 = try toPascalCase(alloc, "ClusterStatus");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("ClusterStatus", r2);

    const r3 = try toPascalCase(alloc, "api-key");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("ApiKey", r3);
}

test "toSnakeCase" {
    const alloc = std.testing.allocator;

    const r1 = try toSnakeCase(alloc, "ClusterStatus");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("cluster_status", r1);

    const r2 = try toSnakeCase(alloc, "HTTPStatus");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("http_status", r2);

    const r3 = try toSnakeCase(alloc, "camelCase");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("camel_case", r3);
}

test "zigFieldName reserved" {
    const alloc = std.testing.allocator;

    const r1 = try zigFieldName(alloc, "error");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("@\"error\"", r1);

    const r2 = try zigFieldName(alloc, "type");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("@\"type\"", r2);

    const r3 = try zigFieldName(alloc, "name");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("name", r3);

    // camelCase input gets snake_cased
    const r4 = try zigFieldName(alloc, "petId");
    defer alloc.free(r4);
    try std.testing.expectEqualStrings("pet_id", r4);

    // Already snake_case passes through
    const r5 = try zigFieldName(alloc, "pet_id");
    defer alloc.free(r5);
    try std.testing.expectEqualStrings("pet_id", r5);
}

test "toMethodName" {
    const alloc = std.testing.allocator;

    const r1 = try toMethodName(alloc, "getClusterStatus");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("getClusterStatus", r1);

    const r2 = try toMethodName(alloc, "get_cluster_status");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("getClusterStatus", r2);

    // Kebab-case operation IDs (e.g. OpenAI admin endpoints)
    const r3 = try toMethodName(alloc, "admin-api-keys-list");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("adminApiKeysList", r3);
}

test "refToName" {
    try std.testing.expectEqualStrings("ClusterStatus", refToName("#/components/schemas/ClusterStatus").?);
    try std.testing.expect(refToName("invalid") == null);

    // External refs
    try std.testing.expectEqualStrings("TableSchema", refToName("../../lib/schema/openapi.yaml#/components/schemas/TableSchema").?);
    try std.testing.expectEqualStrings("EmbedderProvider", refToName("../embeddings/openapi.yaml#/components/schemas/EmbedderProvider").?);
}

test "isExternalRef" {
    try std.testing.expect(!isExternalRef("#/components/schemas/Foo"));
    try std.testing.expect(isExternalRef("../../lib/schema/openapi.yaml#/components/schemas/Foo"));
    try std.testing.expect(isExternalRef("../api.yaml#/components/schemas/Bar"));
    try std.testing.expect(!isExternalRef(""));
}

test "refToFilePath" {
    try std.testing.expectEqualStrings("../../lib/schema/openapi.yaml", refToFilePath("../../lib/schema/openapi.yaml#/components/schemas/Foo").?);
    try std.testing.expect(refToFilePath("#/components/schemas/Foo") == null);
}
