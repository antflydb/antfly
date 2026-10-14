// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Fixed-memory decoder admission for table requirements nested in catalog,
//! FK and restore envelopes. Schema/row strings are validated but never copied.
const std = @import("std");

pub fn requiresPublication(bytes: []const u8) bool {
    if (bytes.len > 64 * 1024 * 1024) return true;
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var scanner = std.json.Scanner.initCompleteInput(fixed.allocator(), bytes);
    defer scanner.deinit();
    if ((scanner.peekNextTokenType() catch return true) != .object_begin) return true;
    if (walk(void, false, &scanner, fixed.allocator(), 0) catch return true) return true;
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
    _ = walk(Requirements, true, &scanner, fixed.allocator(), 0) catch |err| switch (err) {
        error.InvalidSchemaReaderRequirements => return err,
        else => return,
    };
}

fn walk(comptime Requirements: type, comptime validate: bool, scanner: *std.json.Scanner, alloc: std.mem.Allocator, depth: usize) !bool {
    if (depth >= 128) return error.InvalidMetadataTransitionEncoding;
    var found = false;
    switch (try scanner.peekNextTokenType()) {
        .object_begin => {
            _ = try scanner.next();
            while (try scanner.peekNextTokenType() != .object_end) {
                const key = try scanner.nextAllocMax(alloc, .alloc_if_needed, 1024);
                const marker = switch (key) {
                    .string => |value| std.mem.eql(u8, value, "schema_reader_requirements"),
                    .allocated_string => |value| blk: {
                        const equal = std.mem.eql(u8, value, "schema_reader_requirements");
                        alloc.free(value);
                        break :blk equal;
                    },
                    else => return error.InvalidMetadataTransitionEncoding,
                };
                if (marker and try scanner.peekNextTokenType() != .null) {
                    if (comptime !validate) return true;
                    const requirement = std.json.innerParse(Requirements, alloc, scanner, .{ .allocate = .alloc_if_needed, .max_value_len = 1024 }) catch
                        return error.InvalidSchemaReaderRequirements;
                    if (!requirement.valid()) return error.InvalidSchemaReaderRequirements;
                    found = true;
                } else if (try walk(Requirements, validate, scanner, alloc, depth + 1)) {
                    if (comptime !validate) return true;
                    found = true;
                }
            }
            _ = try scanner.next();
        },
        .array_begin => {
            _ = try scanner.next();
            while (try scanner.peekNextTokenType() != .array_end) {
                if (try walk(Requirements, validate, scanner, alloc, depth + 1)) {
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
