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

//! Database-owned casts. Identity is the ordered pair of canonical SQL types,
//! never a name in search_path or a function's current spelling.
const std = @import("std");
const routines = @import("routines.zig");
pub const Type = routines.Type;
pub const Context = enum {
    explicit,
    assignment,
    implicit,

    pub fn allows(self: Context, use: Context) bool {
        return switch (use) {
            .explicit => true,
            .assignment => self != .explicit,
            .implicit => self == .implicit,
        };
    }
};
pub const Pair = struct {
    source: Type,
    target: Type,

    pub fn validate(self: Pair) !void {
        try self.source.validate();
        try self.target.validate();
    }
    pub fn keyAlloc(self: Pair, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        return std.fmt.allocPrint(a, "{s}{s};{s}{s}", .{ @tagName(self.source.scalar), if (self.source.array) @as([]const u8, "[]") else "", @tagName(self.target.scalar), if (self.target.array) @as([]const u8, "[]") else "" });
    }
    pub fn displayAlloc(self: Pair, a: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(a, "({s}{s} AS {s}{s})", .{ self.source.sqlName(), if (self.source.array) @as([]const u8, "[]") else "", self.target.sqlName(), if (self.target.array) @as([]const u8, "[]") else "" });
    }
};
pub const Definition = struct {
    pair: Pair,
    context: Context = .explicit,
    function: routines.FunctionReference,
    /// PostgreSQL support functions optionally receive typmod and explicitness.
    /// The exact signature is validated at publication and after restoration.
    arguments: u8 = 1,
    owner: @import("owners.zig").Owner,

    pub fn validate(self: Definition) !void {
        try self.pair.validate();
        try self.function.validate();
        if (self.arguments == 0 or self.arguments > 3 or (self.arguments == 1 and std.meta.eql(self.pair.source, self.pair.target))) return error.InvalidSqlCastRecord;
        // Reuse the canonical principal validator; the body is never executed.
        try (routines.Definition{ .arguments = &.{}, .result = self.pair.target, .body = "SELECT NULL", .owner = self.owner }).validate();
    }
    pub fn functionUse(self: *const Definition, scratch: *[3]Type) routines.FunctionUse {
        scratch.* = .{ self.pair.source, .{ .scalar = .int32 }, .{ .scalar = .boolean } };
        return .{ .reference = self.function, .arguments = scratch[0..self.arguments], .result = self.pair.target };
    }
};
pub const Record = struct {
    version: u16 = 1,
    id: u64,
    generation: u32,
    database_id: u64,
    definition: Definition,

    pub fn validate(self: Record) !void {
        if (self.version != 1 or self.id == 0 or self.generation == 0 or self.database_id == 0) return error.InvalidSqlCastRecord;
        try self.definition.validate();
        if (@import("builtin_casts.zig").exists(self.definition.pair)) return error.InvalidSqlCastRecord;
    }
    pub fn encode(self: Record, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len > 16 * 1024) return error.InvalidSqlCastRecord;
        var parsed = std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSqlCastRecord,
        };
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};

test "SQL database cast identities contexts and support contracts are canonical" {
    const a = std.testing.allocator;
    const definition: Definition = .{ .pair = .{ .source = .{ .scalar = .jsonb }, .target = .{ .scalar = .text } }, .context = .assignment, .function = .{ .database = "default", .namespace = "public", .name = "jsonb_to_text", .namespace_id = 2, .id = 9 }, .owner = .auth_disabled };
    const record: Record = .{ .id = 10, .generation = 1, .database_id = 1, .definition = definition };
    const bytes = try record.encode(a);
    defer a.free(bytes);
    var restored = try Record.decode(a, bytes);
    defer restored.deinit();
    try std.testing.expectEqualDeep(record, restored.value);
    const key = try definition.pair.keyAlloc(a);
    defer a.free(key);
    try std.testing.expectEqualStrings("jsonb;text", key);
    const display = try definition.pair.displayAlloc(a);
    defer a.free(display);
    try std.testing.expectEqualStrings("(jsonb AS text)", display);
    var scratch: [3]Type = undefined;
    const use = definition.functionUse(&scratch);
    try std.testing.expectEqual(@as(usize, 1), use.arguments.len);
    try std.testing.expectEqualDeep(definition.pair.source, use.arguments[0]);
    inline for (std.meta.tags(Context)) |context| inline for (std.meta.tags(Context)) |use_context| {
        try std.testing.expectEqual(@backingInt(use_context) <= @backingInt(context), context.allows(use_context));
    };
}

test "SQL database cast builtin identities exclude automatic string coercions" {
    const builtin = @import("builtin_casts.zig");
    try std.testing.expect(builtin.exists(Pair{ .source = .{ .scalar = .int32 }, .target = .{ .scalar = .numeric } }));
    try std.testing.expect(!builtin.exists(Pair{ .source = .{ .scalar = .jsonb }, .target = .{ .scalar = .text } }));
    try std.testing.expect(!builtin.exists(Pair{ .source = .{ .scalar = .text }, .target = .{ .scalar = .int64 } }));
}
