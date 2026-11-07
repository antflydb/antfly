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

//! Durable callable identity, independent of parser trees or execution programs.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_arguments = 100;
pub const max_body_bytes = 64 * 1024;
pub const Type = struct {
    pub const Scalar = enum { text, uuid, int16, int32, int64, float32, float64, boolean, jsonb, numeric, date, time, time_tz, timestamp, timestamp_tz, interval, numrange, daterange, tsrange, tstzrange };
    scalar: Scalar,
    array: bool = false,
    pub fn sqlName(self: Type) []const u8 {
        return switch (self.scalar) {
            .int16 => "smallint",
            .int32 => "integer",
            .int64 => "bigint",
            .float32 => "real",
            .float64 => "double precision",
            .time_tz => "time with time zone",
            .timestamp_tz => "timestamp with time zone",
            else => @tagName(self.scalar),
        };
    }
    pub fn validate(self: Type) !void {
        if (self.array) switch (self.scalar) {
            .text, .uuid, .int16, .int32, .int64, .float32, .float64, .boolean, .jsonb, .numeric => {},
            else => return error.InvalidSqlRoutineRecord,
        };
    }
};
pub const Argument = struct { name: ?[]const u8 = null, type: Type };
pub const Volatility = enum { immutable, stable, @"volatile" };
pub const Kind = enum { function, procedure, operator, aggregate };
/// An operator owns its identity while referencing a stable function identity.
/// Function replacement can advance its executable generation; name reuse can
/// never retarget this reference.
pub const RoutineReference = struct {
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    namespace_id: u64,
    id: u64,
    pub fn validate(self: @This()) !void {
        const domain = @import("domain.zig");
        try domain.validateName(self.database);
        try domain.validateName(self.namespace);
        try domain.validateTableName(self.name);
        if (self.namespace_id == 0 or self.id == 0) return error.InvalidSqlRoutineRecord;
    }
};
pub const FunctionReference = RoutineReference;

pub const AggregateDefinition = struct {
    state: Type,
    initial: ?[]const u8 = null,
    final: ?FunctionReference = null,
    pub fn validate(self: @This()) !void {
        try self.state.validate();
        if (self.initial) |value| if (value.len > max_body_bytes or !std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidSqlRoutineRecord;
        if (self.final) |reference| try reference.validate();
    }
};
pub const FunctionUse = struct { reference: FunctionReference, arguments: []const Type, result: Type };
pub const Definition = struct {
    arguments: []const Argument,
    result: ?Type = null,
    returns_trigger: bool = false,
    language: enum { sql, plpgsql } = .sql,
    body: []const u8,
    function_reference: ?FunctionReference = null,
    aggregate: ?AggregateDefinition = null,
    strict: bool = false,
    volatility: Volatility = .@"volatile",
    owner: @import("owners.zig").Owner,
    pub fn validate(self: Definition) !void {
        if (self.arguments.len > max_arguments or (self.body.len == 0 and self.function_reference == null) or self.body.len > max_body_bytes or
            !std.unicode.utf8ValidateSlice(self.body) or std.mem.indexOfScalar(u8, self.body, 0) != null) return error.InvalidSqlRoutineRecord;
        if (self.returns_trigger and (self.result != null or self.arguments.len != 0 or self.language != .plpgsql or self.function_reference != null or self.aggregate != null)) return error.InvalidSqlRoutineRecord;
        if (self.function_reference) |reference| try reference.validate();
        if (self.aggregate) |aggregate| try aggregate.validate();
        if (self.result) |result| try result.validate();
        // There are at most 100 names; validation needs no heap or global state.
        for (self.arguments, 0..) |argument, i| {
            try argument.type.validate();
            if (argument.name) |name| {
                try @import("domain.zig").validateTableName(name);
                for (self.arguments[0..i]) |previous| if (previous.name) |other| if (std.mem.eql(u8, name, other)) return error.InvalidSqlRoutineRecord;
            }
        }
        switch (self.owner) {
            .auth_disabled => {},
            .user, .role => |principal| {
                if (principal.name.len == 0 or principal.name.len > 255 or std.mem.allEqual(u8, &principal.instance_id, 0)) return error.InvalidSqlRoutineRecord;
            },
        }
    }
    pub fn nativeJsonSkipField(self: Definition, comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "returns_trigger") and !self.returns_trigger;
    }
    pub fn jsonStringify(self: Definition, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Definition).@"struct".field_names) |field| {
            if (!self.nativeJsonSkipField(field)) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn displaySignatureAlloc(self: Definition, a: A) ![]u8 {
        var text: std.ArrayList(u8) = .empty;
        errdefer text.deinit(a);
        for (self.arguments, 0..) |argument, ordinal| {
            if (ordinal != 0) try text.append(a, ',');
            try text.appendSlice(a, argument.type.sqlName());
            if (argument.type.array) try text.appendSlice(a, "[]");
        }
        return text.toOwnedSlice(a);
    }
    pub fn signatureAlloc(self: Definition, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        var bytes: std.ArrayList(u8) = .empty;
        errdefer bytes.deinit(a);
        for (self.arguments, 0..) |argument, i| {
            if (i != 0) try bytes.append(a, ';');
            try bytes.appendSlice(a, @tagName(argument.type.scalar));
            if (argument.type.array) try bytes.appendSlice(a, "[]");
        }
        return bytes.toOwnedSlice(a);
    }
};
pub const Record = struct {
    version: u16 = 2,
    kind: Kind = .function,
    id: u64,
    generation: u32,
    definition: Definition,
    pub fn validate(self: Record) !void {
        if (self.version != 2 or self.id == 0 or self.generation == 0) return error.InvalidSqlRoutineRecord;
        try self.definition.validate();
        switch (self.kind) {
            .function => if (self.definition.aggregate != null or self.definition.function_reference != null or (self.definition.result == null and !self.definition.returns_trigger) or (self.definition.language != .sql and !self.definition.returns_trigger)) return error.InvalidSqlRoutineRecord,
            .procedure => if (self.definition.returns_trigger or self.definition.aggregate != null or self.definition.function_reference != null or self.definition.result != null or self.definition.strict or self.definition.volatility != .@"volatile") return error.InvalidSqlRoutineRecord,
            .operator => if (self.definition.aggregate != null or self.definition.function_reference == null or self.definition.body.len != 0 or self.definition.result == null or self.definition.arguments.len != 2 or self.definition.language != .sql or self.definition.strict) return error.InvalidSqlRoutineRecord,
            .aggregate => {
                if (self.definition.aggregate == null or self.definition.function_reference == null or self.definition.body.len != 0 or self.definition.result == null or self.definition.arguments.len == 0 or self.definition.arguments.len >= max_arguments or self.definition.language != .sql or self.definition.strict) return error.InvalidSqlRoutineRecord;
                if (self.definition.aggregate.?.final == null and !std.meta.eql(self.definition.result.?, self.definition.aggregate.?.state)) return error.InvalidSqlRoutineRecord;
            },
        }
    }
    /// Every supporting function has an exact declared contract. Native apply,
    /// restore and execution share these signatures; names cannot retarget them.
    pub fn functionUsesAlloc(self: Record, a: A) ![]const FunctionUse {
        try self.validate();
        const reference = self.definition.function_reference orelse return &.{};
        const aggregate = self.definition.aggregate;
        const uses = try a.alloc(FunctionUse, if (aggregate != null and aggregate.?.final != null) 2 else 1);
        errdefer a.free(uses);
        const arguments = try a.alloc(Type, self.definition.arguments.len + @intFromBool(aggregate != null));
        errdefer a.free(arguments);
        const offset: usize = @intFromBool(aggregate != null);
        if (aggregate) |value| arguments[0] = value.state;
        for (self.definition.arguments, arguments[offset..]) |argument, *out| out.* = argument.type;
        uses[0] = .{ .reference = reference, .arguments = arguments, .result = if (aggregate) |value| value.state else self.definition.result.? };
        if (uses.len == 2) {
            const final_arguments = try a.alloc(Type, 1);
            final_arguments[0] = aggregate.?.state;
            uses[1] = .{ .reference = aggregate.?.final.?, .arguments = final_arguments, .result = self.definition.result.? };
        }
        return uses;
    }

    pub fn freeFunctionUses(a: A, uses: []const FunctionUse) void {
        for (uses) |use| a.free(use.arguments);
        a.free(uses);
    }

    /// Functions/procedures share a signature namespace; operator signatures
    /// are disjoint even from a quoted function named with the same symbol.
    pub fn catalogSignatureAlloc(self: Record, a: A) ![]u8 {
        const signature = try self.definition.signatureAlloc(a);
        if (self.kind != .operator) return signature;
        defer a.free(signature);
        return std.mem.concat(a, u8, &.{ "operator:", signature });
    }
    pub fn encode(self: Record, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len > 512 * 1024) return error.InvalidSqlRoutineRecord;
        var parsed = std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSqlRoutineRecord,
        };
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};

pub const Input = struct { type: ?Type = null, unknown_literal: bool = false };
pub const Header = struct { id: u64, signature: []const u8 };

pub fn parseSignature(a: std.mem.Allocator, signature: []const u8) ![]const Type {
    if (signature.len == 0) return &.{};
    if (signature.len > 4096) return error.InvalidSqlRoutineRecord;
    var result: std.ArrayList(Type) = .empty;
    var parts = std.mem.splitScalar(u8, signature, ';');
    while (parts.next()) |part| {
        if (result.items.len >= max_arguments) return error.InvalidSqlRoutineRecord;
        const array = std.mem.endsWith(u8, part, "[]");
        const spelling = if (array) part[0 .. part.len - 2] else part;
        const kind = std.meta.stringToEnum(Type.Scalar, spelling) orelse return error.InvalidSqlRoutineRecord;
        const value: Type = .{ .scalar = kind, .array = array };
        try value.validate();
        try result.append(a, value);
    }
    return result.toOwnedSlice(a);
}

fn numericRank(kind: Type.Scalar) ?u8 {
    return switch (kind) {
        .int16 => 0,
        .int32 => 1,
        .int64 => 2,
        .numeric => 3,
        .float32 => 4,
        .float64 => 5,
        else => null,
    };
}

fn implicit(source: Type, target: Type) bool {
    if (std.meta.eql(source, target)) return true;
    if (source.array != target.array) return false;
    if (numericRank(source.scalar)) |from| if (numericRank(target.scalar)) |to| return from <= to;
    return switch (source.scalar) {
        .date => target.scalar == .timestamp or target.scalar == .timestamp_tz,
        .time => target.scalar == .time_tz,
        .timestamp => target.scalar == .timestamp_tz,
        else => false,
    };
}

fn category(kind: Type) u8 {
    if (kind.array) return 'A';
    if (numericRank(kind.scalar) != null) return 'N';
    return switch (kind.scalar) {
        .text => 'S',
        .boolean => 'B',
        .date, .time, .time_tz, .timestamp, .timestamp_tz => 'D',
        .interval => 'T',
        .numrange, .daterange, .tsrange, .tstzrange => 'R',
        else => 'U',
    };
}
fn preferred(kind: Type) bool {
    return !kind.array and switch (kind.scalar) {
        .text, .boolean, .float64, .timestamp_tz, .interval => true,
        else => false,
    };
}
fn unknown(input: Input) bool {
    return input.type == null or input.unknown_literal;
}

/// PostgreSQL's builtin-type overload selection: exact inputs, preferred
/// conversions, unknown-position categories, then the single known-type rule.
/// Search-path shadowing removes duplicate signatures before this point.
pub fn choose(a: std.mem.Allocator, headers: []const Header, inputs: []const Input) !?Header {
    if (headers.len > 6400 or inputs.len > max_arguments) return error.SqlProgramLimitExceeded;
    const Candidate = struct { header: Header, arguments: []const Type, exact: usize, preferred_count: usize, live: bool = true };
    var candidates: std.ArrayList(Candidate) = .empty;
    defer candidates.deinit(a);
    var max_exact: usize = 0;
    for (headers) |header| {
        const arguments = try parseSignature(a, header.signature);
        if (arguments.len != inputs.len) continue;
        var exact: usize = 0;
        var preferred_count: usize = 0;
        var compatible = true;
        for (arguments, inputs) |argument, input| {
            if (unknown(input)) continue;
            const source = input.type.?;
            if (!implicit(source, argument)) {
                compatible = false;
                break;
            }
            if (std.meta.eql(source, argument)) exact += 1 else if (category(source) == category(argument) and preferred(argument)) {
                preferred_count += 1;
            }
        }
        if (!compatible) continue;
        max_exact = @max(max_exact, exact);
        try candidates.append(a, .{ .header = header, .arguments = arguments, .exact = exact, .preferred_count = preferred_count });
    }
    var remaining: usize = 0;
    var max_preferred: usize = 0;
    for (candidates.items) |*candidate| {
        candidate.live = candidate.exact == max_exact;
        if (candidate.live) {
            remaining += 1;
            max_preferred = @max(max_preferred, candidate.preferred_count);
        }
    }
    if (remaining == 0) return null;
    if (remaining == 1) {
        for (candidates.items) |candidate| if (candidate.live) return candidate.header;
    }
    for (candidates.items) |*candidate| if (candidate.live and candidate.preferred_count != max_preferred) {
        candidate.live = false;
        remaining -= 1;
    };
    if (remaining == 1) {
        for (candidates.items) |candidate| if (candidate.live) return candidate.header;
    }
    for (inputs, 0..) |input, ordinal| {
        if (!unknown(input)) continue;
        var chosen: ?u8 = null;
        var conflict = false;
        var has_preferred = false;
        for (candidates.items) |candidate| {
            if (!candidate.live) continue;
            const cat = category(candidate.arguments[ordinal]);
            if (cat == 'S') {
                chosen = 'S';
                conflict = false;
                break;
            }
            if (chosen) |prior| {
                if (prior != cat) conflict = true;
            } else chosen = cat;
        }
        if (conflict or chosen == null) return error.SqlAmbiguousFunction;
        for (candidates.items) |candidate| if (candidate.live and category(candidate.arguments[ordinal]) == chosen.? and preferred(candidate.arguments[ordinal])) {
            has_preferred = true;
        };
        for (candidates.items) |*candidate| {
            if (!candidate.live) continue;
            const target = candidate.arguments[ordinal];
            if (category(target) != chosen.? or (has_preferred and !preferred(target))) {
                candidate.live = false;
                remaining -= 1;
            }
        }
    }
    if (remaining == 1) {
        for (candidates.items) |candidate| if (candidate.live) return candidate.header;
    }
    var known: ?Type = null;
    var same = true;
    var has_unknown = false;
    for (inputs) |input| {
        if (unknown(input)) {
            has_unknown = true;
            continue;
        }
        if (known) |prior| {
            if (!std.meta.eql(prior, input.type.?)) same = false;
        } else known = input.type;
    }
    if (same and has_unknown and known != null) {
        var selected: ?Header = null;
        for (candidates.items) |candidate| {
            if (!candidate.live) continue;
            const compatible = for (candidate.arguments, inputs) |argument, input| {
                if (unknown(input) and !implicit(known.?, argument)) break false;
            } else true;
            if (!compatible) continue;
            if (selected != null) return error.SqlAmbiguousFunction;
            selected = candidate.header;
        }
        if (selected) |value| return value;
    }
    return error.SqlAmbiguousFunction;
}

test "SQL routine overload selection uses PostgreSQL exact preferred and unknown category rules" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const headers = [_]Header{ .{ .id = 1, .signature = "int32" }, .{ .id = 2, .signature = "int64" }, .{ .id = 3, .signature = "numeric" }, .{ .id = 4, .signature = "float64" } };
    try std.testing.expectEqual(@as(u64, 1), (try choose(a, &headers, &.{.{ .type = .{ .scalar = .int32 } }})).?.id);
    try std.testing.expectEqual(@as(u64, 3), (try choose(a, &headers, &.{.{ .type = .{ .scalar = .numeric } }})).?.id);
    try std.testing.expectEqual(@as(u64, 4), (try choose(a, &headers, &.{.{ .type = .{ .scalar = .int16 } }})).?.id);
    try std.testing.expectEqual(@as(u64, 4), (try choose(a, &headers, &.{.{ .unknown_literal = true }})).?.id);
    const text = headers ++ [_]Header{.{ .id = 5, .signature = "text" }};
    try std.testing.expectEqual(@as(u64, 5), (try choose(a, &text, &.{.{ .unknown_literal = true }})).?.id);
    try std.testing.expectError(error.SqlAmbiguousFunction, choose(a, headers[0..2], &.{.{ .unknown_literal = true }}));
}
