// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Public restore mappings are durable identity data, never executable bodies
//! or name-resolution requests. Decimal IDs remain lossless in every SDK.
const std = @import("std");
const A = std.mem.Allocator;
pub const Mapping = @import("antfly_metadata_openapi").RestoreRoutineMapping;
/// Immutable managed user behind a durable credential. A recreated username
/// cannot inherit an already admitted restore's function-creation authority.
pub const Actor = struct {
    username: []const u8,
    instance: [16]u8,
    pub fn validate(self: @This()) !void {
        if (self.username.len == 0 or self.username.len > 256 or !std.unicode.utf8ValidateSlice(self.username) or std.mem.indexOfScalar(u8, self.username, 0) != null or std.mem.allEqual(u8, &self.instance, 0)) return error.InvalidRequest;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.mem.eql(u8, self.username, other.username) and std.mem.eql(u8, &self.instance, &other.instance);
    }
};
pub fn equalOptional(left: ?[]const Mapping, right: ?[]const Mapping) bool {
    const lhs = left orelse &.{};
    const rhs = right orelse &.{};
    if (lhs.len != rhs.len) return false;
    for (lhs) |value| {
        const present = for (rhs) |candidate| {
            if (std.mem.eql(u8, value.source_metadata_incarnation, candidate.source_metadata_incarnation) and std.mem.eql(u8, value.source_routine_id, candidate.source_routine_id) and std.mem.eql(u8, value.destination_routine_id, candidate.destination_routine_id)) break true;
        } else false;
        if (!present) return false;
    }
    return true;
}

pub const max_mappings = 256;
pub const Source = struct { incarnation: [16]u8, routine_id: u64, destination_id: u64 };
pub fn decode(value: Mapping) !Source {
    if (value.source_metadata_incarnation.len != 32) return error.InvalidRequest;
    for (value.source_metadata_incarnation) |byte| if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return error.InvalidRequest;
    var incarnation: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&incarnation, value.source_metadata_incarnation) catch return error.InvalidRequest;
    if (std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidRequest;
    return .{ .incarnation = incarnation, .routine_id = try identifier(value.source_routine_id), .destination_id = try identifier(value.destination_routine_id) };
}
fn identifier(value: []const u8) !u64 {
    if (value.len == 0 or value.len > 20 or value[0] < '1' or value[0] > '9') return error.InvalidRequest;
    for (value) |byte| if (byte < '0' or byte > '9') return error.InvalidRequest;
    return std.fmt.parseInt(u64, value, 10) catch return error.InvalidRequest;
}
pub fn validate(values: []const Mapping) !void {
    if (values.len > max_mappings) return error.SqlProgramLimitExceeded;
    for (values, 0..) |value, index| {
        const current = try decode(value);
        for (values[0..index]) |previous| {
            const old = try decode(previous);
            if (current.routine_id == old.routine_id and std.mem.eql(u8, &current.incarnation, &old.incarnation)) return error.InvalidRequest;
        }
    }
}
fn less(_: void, left: Mapping, right: Mapping) bool {
    const order = std.mem.order(u8, left.source_metadata_incarnation, right.source_metadata_incarnation);
    if (order != .eq) return order == .lt;
    // Canonical decimal syntax makes length then lexical order numeric.
    if (left.source_routine_id.len != right.source_routine_id.len) return left.source_routine_id.len < right.source_routine_id.len;
    return std.mem.lessThan(u8, left.source_routine_id, right.source_routine_id);
}
pub fn canonicalAlloc(a: A, values: []const Mapping) ![]u8 {
    try validate(values);
    const ordered = try a.dupe(Mapping, values);
    defer a.free(ordered);
    std.mem.sort(Mapping, ordered, {}, less);
    return std.json.Stringify.valueAlloc(a, ordered, .{});
}
pub fn cloneOptional(a: A, values: ?[]const Mapping) !?[]const Mapping {
    const source = values orelse return null;
    try validate(source);
    const owned = try a.alloc(Mapping, source.len);
    var initialized: usize = 0;
    errdefer {
        freeItems(a, owned[0..initialized]);
        a.free(owned);
    }
    for (source, owned) |value, *out| {
        const incarnation = try a.dupe(u8, value.source_metadata_incarnation);
        errdefer a.free(incarnation);
        const old_id = try a.dupe(u8, value.source_routine_id);
        errdefer a.free(old_id);
        const new_id = try a.dupe(u8, value.destination_routine_id);
        out.* = .{ .source_metadata_incarnation = incarnation, .source_routine_id = old_id, .destination_routine_id = new_id };
        initialized += 1;
    }
    return owned;
}
fn freeItems(a: A, values: []const Mapping) void {
    for (values) |value| {
        a.free(value.source_metadata_incarnation);
        a.free(value.source_routine_id);
        a.free(value.destination_routine_id);
    }
}
pub fn freeOptional(a: A, values: ?[]const Mapping) void {
    const owned = values orelse return;
    freeItems(a, owned);
    a.free(owned);
}
test "restore routine mapping preserves lossless identities and canonical set semantics" {
    const a = std.testing.allocator;
    const first: Mapping = .{ .source_metadata_incarnation = "11111111111111111111111111111111", .source_routine_id = "18446744073709551615", .destination_routine_id = "9007199254740993" };
    const second: Mapping = .{ .source_metadata_incarnation = "11111111111111111111111111111111", .source_routine_id = "3", .destination_routine_id = "4" };
    try std.testing.expectEqual(@as(u64, 18446744073709551615), (try decode(first)).routine_id);
    const forward = try canonicalAlloc(a, &.{ first, second });
    defer a.free(forward);
    const backward = try canonicalAlloc(a, &.{ second, first });
    defer a.free(backward);
    try std.testing.expectEqualStrings(forward, backward);
    try std.testing.expectError(error.InvalidRequest, validate(&.{ first, first }));
    var invalid = first;
    invalid.source_routine_id = "01";
    try std.testing.expectError(error.InvalidRequest, decode(invalid));
    invalid = first;
    invalid.destination_routine_id = "18446744073709551616";
    try std.testing.expectError(error.InvalidRequest, decode(invalid));
}
