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

//! Fixed-memory decoder admission for table requirements nested in catalog,
//! FK and restore envelopes. Schema/row strings are validated but never copied.
const std = @import("std");

pub fn requiresPublication(bytes: []const u8) bool {
    return requiresMarker(bytes, "schema_reader_requirements");
}

pub fn requiresColumnTransforms(bytes: []const u8) bool {
    return requiresMarker(bytes, "column_transforms");
}

pub fn requiresTextGenerationBinding(bytes: []const u8) bool {
    return requiresMarker(bytes, "full_text_read_index");
}

/// One bounded token walk handles both command envelopes and stored records.
/// Quoted payload strings are never mistaken for authoritative field names.
pub fn requiresRoleDefaults(bytes: []const u8) bool {
    return requiresMarker(bytes, "@sql-role-defaults");
}

test "SQL role defaults decoder projects escaped fields and ignores JSON payloads" {
    for ([_][]const u8{
        "{}",
        "{\"role_defaults\":[]}",
        "{\"role_defaults\":[{\"principal\":\"alice\",\"database\":\"main\"}]}",
        "{\"value\":{\"string\":\"database principal_instance role_defaults_only\"}}",
    }) |legacy| try std.testing.expect(!requiresRoleDefaults(legacy));
    for ([_][]const u8{
        "{\"change\":{\"put\":{\"role_defaults\":[{\"database\":\"\"}]}}}",
        "{\"role_defaults\":[{\"database\":\"main\",\"principal_instance\":[1]}]}",
        "{\"role_defaults\":[{\"principal_\\u0069nstance\":null}]}",
        "{\"role_defaults_only\":false}",
        "{\"role_defaults_only\":true}",
        "{",
    }) |current| try std.testing.expect(requiresRoleDefaults(current));
}

fn requiresMarker(bytes: []const u8, comptime marker: []const u8) bool {
    if (bytes.len > 64 * 1024 * 1024) return true;
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var scanner = std.json.Scanner.initCompleteInput(fixed.allocator(), bytes);
    defer scanner.deinit();
    if ((scanner.peekNextTokenType() catch return true) != .object_begin) return true;
    if (walk(void, false, marker, &scanner, fixed.allocator(), 0) catch return true) return true;
    return (scanner.next() catch return true) != .end_of_document;
}

/// Project requirements at preparation, not from jsonStringify (whose error
/// set only admits writer failures). Leave envelope syntax/shape errors to its
/// existing domain decoder; never replace legacy error identities. A recognized
/// non-null requirement is always checked against the supplied shared contract.
pub fn validatePublication(comptime Requirements: type, bytes: []const u8) !void {
    if (bytes.len > 64 * 1024 * 1024) return error.InvalidSchemaReaderRequirements;
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var scanner = std.json.Scanner.initCompleteInput(fixed.allocator(), bytes);
    defer scanner.deinit();
    if ((scanner.peekNextTokenType() catch return) != .object_begin) return;
    _ = walk(Requirements, true, "schema_reader_requirements", &scanner, fixed.allocator(), 0) catch |err| switch (err) {
        error.InvalidSchemaReaderRequirements => return err,
        else => return,
    };
}

fn walk(comptime Requirements: type, comptime validate: bool, comptime marker_name: []const u8, scanner: *std.json.Scanner, alloc: std.mem.Allocator, depth: usize) !bool {
    if (depth >= 128) return error.InvalidMetadataTransitionEncoding;
    var found = false;
    switch (try scanner.peekNextTokenType()) {
        .object_begin => {
            _ = try scanner.next();
            while (try scanner.peekNextTokenType() != .object_end) {
                const key = try scanner.nextAllocMax(alloc, .alloc_if_needed, 1024);
                const Marker = enum { none, present, non_null, empty_database };
                const Match = struct {
                    fn keyKind(name: []const u8) Marker {
                        if (comptime std.mem.eql(u8, marker_name, "@sql-role-defaults")) {
                            if (std.mem.eql(u8, name, "principal_instance") or std.mem.eql(u8, name, "role_defaults_only") or std.mem.eql(u8, name, "database_id")) return .present;
                            if (std.mem.eql(u8, name, "database")) return .empty_database;
                            return .none;
                        }
                        return if (std.mem.eql(u8, name, marker_name)) .non_null else .none;
                    }
                };
                const marker = switch (key) {
                    .string => |value| Match.keyKind(value),
                    .allocated_string => |value| blk: {
                        const equal = Match.keyKind(value);
                        alloc.free(value);
                        break :blk equal;
                    },
                    else => return error.InvalidMetadataTransitionEncoding,
                };
                if (marker == .present) return true;
                if (marker == .empty_database and try scanner.peekNextTokenType() == .string) {
                    const value = try scanner.nextAllocMax(alloc, .alloc_if_needed, 1024);
                    const empty = switch (value) {
                        .string => |bytes| bytes.len == 0,
                        .allocated_string => |bytes| blk: {
                            const is_empty = bytes.len == 0;
                            alloc.free(bytes);
                            break :blk is_empty;
                        },
                        else => return error.InvalidMetadataTransitionEncoding,
                    };
                    if (empty) return true;
                    continue;
                }
                if (marker == .non_null and try scanner.peekNextTokenType() != .null) {
                    if (comptime !validate) return true;
                    const requirement = std.json.innerParse(Requirements, alloc, scanner, .{ .allocate = .alloc_if_needed, .max_value_len = 1024 }) catch
                        return error.InvalidSchemaReaderRequirements;
                    if (!requirement.valid()) return error.InvalidSchemaReaderRequirements;
                    found = true;
                } else if (try walk(Requirements, validate, marker_name, scanner, alloc, depth + 1)) {
                    if (comptime !validate) return true;
                    found = true;
                }
            }
            _ = try scanner.next();
        },
        .array_begin => {
            _ = try scanner.next();
            while (try scanner.peekNextTokenType() != .array_end) {
                if (try walk(Requirements, validate, marker_name, scanner, alloc, depth + 1)) {
                    if (comptime !validate) return true;
                    found = true;
                }
            }
            _ = try scanner.next();
        },
        .string, .number, .true, .false, .null => try scanner.skipValue(),
        else => return error.InvalidMetadataTransitionEncoding,
    }
    return found;
}

test "system catalog schema reader publication projection skips payloads and finds escaped nested markers" {
    try std.testing.expect(!requiresPublication("{\"schema_reader_requirements\":null}"));
    try std.testing.expect(!requiresPublication("{\"payload\":\"schema_reader_requirements\"}"));
    try std.testing.expect(requiresPublication("{\"plan\":{\"targets\":[{\"table\":{\"schema_reader_requirements\":{\"semantic_version\":30,\"encoding_version\":31}}}]}}"));
    try std.testing.expect(requiresPublication("{\"table\":{\"schema_reader_\\u0072equirements\":{}}}"));
    for ([_][]const u8{ "", "{", "[]", "{\"schema_reader_requirements\":false}", "{}{}", "{\"a\":null,}", "{\"a\":null,\"b\":[}" }) |invalid|
        try std.testing.expect(requiresPublication(invalid));
    const large = try std.testing.allocator.alloc(u8, 128 * 1024);
    defer std.testing.allocator.free(large);
    @memset(large, '"');
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .plan = .{ .child = .{ .schema_json = large }, .table = .{ .schema_reader_requirements = @as(?u32, null) } } }, .{});
    defer std.testing.allocator.free(bytes);
    try std.testing.expect(!requiresPublication(bytes));
}

test "system catalog USING rewrite decoder projection skips expression payloads and recognizes escaped markers" {
    try std.testing.expect(!requiresColumnTransforms("{\"expression_json\":\"column_transforms\"}"));
    try std.testing.expect(!requiresColumnTransforms("{\"column_transforms\":null}"));
    try std.testing.expect(requiresColumnTransforms("{\"plan\":{\"targets\":[{\"rewrite\":{\"column_\\u0074ransforms\":[{}]}}]}}"));
    try std.testing.expect(requiresColumnTransforms("{"));
    const a = std.testing.allocator;
    const large = try a.alloc(u8, 128 * 1024);
    defer a.free(large);
    @memset(large, '"');
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .source_schema = large, .targets = &.{.{ .rewrite = .{ .column_transforms = &.{.{ .column = "n", .expression_json = large }} } }} }, .{});
    defer a.free(bytes);
    try std.testing.expect(requiresColumnTransforms(bytes));
}

test "system catalog schema reader publication validation checks every pair and preserves envelope decoding" {
    // The wire layer delegates semantics to its supplied contract. The native
    // codec suite additionally exercises this with the actual shared type.
    const FixtureRequirements = struct {
        semantic_version: u32,
        encoding_version: u32,
        pub fn valid(self: @This()) bool {
            return self.semantic_version == 30 and self.encoding_version == 31;
        }
    };
    try validatePublication(FixtureRequirements, "{\"plan\":{\"table\":{\"schema_reader_requirements\":{\"semantic_version\":30,\"encoding_version\":31}}}}");
    try validatePublication(FixtureRequirements, "{\"schema_reader_requirements\":null}");
    try validatePublication(FixtureRequirements, "{"); // The domain decoder still owns envelope syntax.
    for ([_][]const u8{
        "{\"schema_reader_requirements\":{}}",
        "{\"schema_reader_requirements\":false}",
        "{\"schema_reader_requirements\":{\"semantic_version\":30,\"encoding_version\":0}}",
        "{\"schema_reader_requirements\":{\"semantic_version\":32,\"encoding_version\":31}}",
        "{\"schema_reader_requirements\":{\"semantic_version\":30,\"encoding_version\":31,\"future\":true}}",
        "{\"tables\":[{\"schema_reader_requirements\":{\"semantic_version\":30,\"encoding_version\":31}},{\"schema_reader_requirements\":{}}]}",
    }) |invalid| try std.testing.expectError(error.InvalidSchemaReaderRequirements, validatePublication(FixtureRequirements, invalid));
}
