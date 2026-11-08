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

//! Stateless owner-planned domain transport. The owner sees the complete
//! predicate once, then certifies which IN suffix subsequent calls may send.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const A = std.mem.Allocator;
pub const width = 128;
pub const Cursor = struct { sparse_seek: struct { window: b.SeekWindow, after: []const u8 } };
pub const Input = struct { filters: []const b.PropertyFilter, after: ?[]const u8, primary: bool = false };
/// Unused IN hints are inactive NULL operands, retaining the original ordinal
/// space of the owner's opaque plan. The Cypher predicate stays authoritative.
pub fn retain(a: A, filters: []const b.PropertyFilter, primary: usize, prefix: []const usize) ![]b.PropertyFilter {
    const selected = try a.dupe(b.PropertyFilter, filters);
    for (selected, 0..) |*filter, ordinal| if (filter.op == .in and ordinal != primary and std.mem.indexOfScalar(usize, prefix, ordinal) == null) {
        filter.value = .null;
        filter.domain = null;
        filter.plan = null;
    };
    return selected;
}
pub fn prepare(a: A, filters: []const b.PropertyFilter, after: ?[]const u8) !Input {
    return prepareBounded(a, filters, after, (1 << 20) - 4096);
}
/// The envelope budget includes filters and the escaped native continuation.
/// Keep ordinals stable while omitting unused membership hints.
pub fn prepareBounded(a: A, filters: []const b.PropertyFilter, after: ?[]const u8, bytes_budget: usize) !Input {
    const bytes = after orelse return .{ .filters = filters, .after = null };
    // Unmodified cursors from backends without this capability stay opaque.
    if (bytes.len == 0 or bytes[0] != '{') return .{ .filters = filters, .after = after };
    const object = std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) catch return .{ .filters = filters, .after = after };
    if (object != .object or !object.object.contains("sparse_seek")) return .{ .filters = filters, .after = after };
    const cursor = try std.json.parseFromSliceLeaky(Cursor, a, bytes, .{});
    const window = cursor.sparse_seek.window;
    if (window.primary) return .{ .filters = &.{}, .after = cursor.sparse_seek.after, .primary = true };
    if (window.filter) |index| {
        if (index >= filters.len) return error.CypherInvalidBackendCursor;
        const source = filters[index];
        if (source.op != .in or source.domain == null or source.value != .list) return error.CypherInvalidBackendCursor;
        if (source.value.list.len > 4096) return error.CypherOwnerBudgetExceeded;
        const selected = try retain(a, filters, index, window.prefix_filters);
        if (window.prefix_filters.len != 0) {
            if (window.prefix_filters.len > 3) return error.CypherInvalidBackendCursor;
            for (window.prefix_filters) |ordinal| if (ordinal >= selected.len or selected[ordinal].op != .in or selected[ordinal].domain == null or selected[ordinal].value != .list) return error.CypherInvalidBackendCursor;
            var size = std.Io.Writer.Discarding.init(&.{});
            try std.json.Stringify.value(.{ .filters = selected, .after = cursor.sparse_seek.after }, .{}, &size.writer);
            if (size.fullCount() > bytes_budget) return error.CypherRequestTooLarge;
            return .{ .filters = selected, .after = cursor.sparse_seek.after };
        }
        if (window.offset >= source.value.list.len) return error.CypherInvalidBackendCursor;
        var low: usize = 0;
        var high: usize = @min(source.value.list.len - window.offset, width);
        while (low < high) {
            const count = low + (high - low + 1) / 2;
            selected[index].value = .{ .list = source.value.list[window.offset..][0..count] };
            var size = std.Io.Writer.Discarding.init(&.{});
            try std.json.Stringify.value(.{ .filters = selected, .after = cursor.sparse_seek.after }, .{}, &size.writer);
            if (size.fullCount() <= bytes_budget) low = count else high = count - 1;
        }
        if (low == 0) return error.CypherRequestTooLarge;
        selected[index].value = .{ .list = source.value.list[window.offset..][0..low] };
        return .{ .filters = selected, .after = cursor.sparse_seek.after };
    }
    var selected: std.ArrayList(b.PropertyFilter) = .empty;
    for (filters) |filter| if (filter.op != .in) try selected.append(a, filter);
    return .{ .filters = selected.items, .after = cursor.sparse_seek.after, .primary = window.primary };
}
/// Oversized initial domains retain all predicate hints and complete-domain
/// proof without making accepted paged domains exceed the transport ceiling.
pub fn planning(a: A, filters: []const b.PropertyFilter) ![]const b.PropertyFilter {
    const selected = try a.dupe(b.PropertyFilter, filters);
    for (selected) |*filter| {
        if (filter.op != .in or filter.domain == null or filter.value != .list or filter.value.list.len <= 3) continue;
        const source = filter.value.list;
        // Cheap complete domains can still form a bounded composite prefix
        // alongside another domain that needs a sampled planning handoff.
        if (source.len <= width) {
            var size = std.Io.Writer.Discarding.init(&.{});
            try std.json.Stringify.value(source, .{}, &size.writer);
            if (size.fullCount() <= 16 << 10) continue;
        }
        if (source.len > 4096) return error.CypherOwnerBudgetExceeded;
        var maximum: usize = 0;
        for (source) |value| if (value == .string) {
            maximum = @max(maximum, value.string.len);
        };
        filter.plan = .{ .length = source.len, .max_string_bytes = maximum };
        filter.value = .{ .list = try a.dupe(v.Value, &.{ source[0], source[source.len / 2], source[source.len - 1] }) };
    }
    return selected;
}
pub fn finish(a: A, input: b.Page(*const v.Node)) !b.Page(*const v.Node) {
    var page = input;
    if (page.next) |next| if (page.seek_window) |window| {
        page.next = try std.json.Stringify.valueAlloc(a, Cursor{ .sparse_seek = .{ .window = window, .after = next } }, .{});
    };
    page.seek_window = null;
    return page;
}

test "owner plans the full predicate before bounded domain continuations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 128, 256, 257, 400, 4096 }) |size| {
        const values = try a.alloc(v.Value, size);
        for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
        values[size - 1] = .{ .integer = 9007199254740993 };
        const filters = &[_]b.PropertyFilter{
            .{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .integer_inexact_double },
            .{ .property = "tenant", .value = .{ .integer = 7 } },
        };
        const initial = try prepare(a, filters, null);
        try std.testing.expectEqual(size, initial.filters[0].value.list.len);
        var offset: usize = @min(width, size);
        while (offset < size) {
            const page = try finish(a, .{ .items = &.{}, .next = "native", .seek_window = .{ .filter = 0, .offset = offset } });
            const resumed = try prepare(a, filters, page.next);
            try std.testing.expectEqual(@min(width, size - offset), resumed.filters[0].value.list.len);
            try std.testing.expectEqual(b.ScalarDomain.integer_inexact_double, resumed.filters[0].domain.?);
            try std.testing.expectEqualStrings("native", resumed.after.?);
            offset += resumed.filters[0].value.list.len;
        }
        const base = try finish(a, .{ .items = &.{}, .next = "base", .seek_window = .{ .filter = null } });
        const resumed = try prepare(a, filters, base.next);
        try std.testing.expectEqual(@as(usize, 1), resumed.filters.len);
        try std.testing.expectEqualStrings("tenant", resumed.filters[0].property);
        try std.testing.expectEqualStrings("base", resumed.after.?);
    }
    const foreign = try prepare(a, &.{}, "foreign-position");
    try std.testing.expectEqualStrings("foreign-position", foreign.after.?);
}
