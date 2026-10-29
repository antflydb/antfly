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

//! Stateless bounded seek windows. The caller retains the immutable domain;
//! owner requests and continuation tokens retain only one fixed-size window.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const A = std.mem.Allocator;
pub const width = 128;
pub const Cursor = struct { sparse_seek: struct { offset: usize = 0, after: ?[]const u8 = null, full_scan: bool = false } = .{} };
pub const Window = struct {
    filters: []const b.PropertyFilter,
    after: ?[]const u8,
    position: Cursor,
    length: usize,
    end: usize,
    pub fn open(a: A, filters: []const b.PropertyFilter, after: ?[]const u8) !?Window {
        const chosen = for (filters, 0..) |filter, i| {
            if (filter.op != .in) continue;
            if (filter.domain == null or filter.value != .list or filter.value.list.len <= width) return null;
            break i;
        } else return null;
        const domain = filters[chosen].value.list;
        if (domain.len > 4096) return error.CypherOwnerBudgetExceeded;
        const position = if (after) |bytes| try std.json.parseFromSliceLeaky(Cursor, a, bytes, .{}) else Cursor{};
        if (position.sparse_seek.offset >= domain.len) return error.CypherInvalidBackendCursor;
        const end = @min(domain.len, position.sparse_seek.offset + width);
        const selected = try a.dupe(b.PropertyFilter, filters);
        if (!position.sparse_seek.full_scan) selected[chosen].value = .{ .list = domain[position.sparse_seek.offset..end] } else selected[chosen].domain = null;
        return .{ .filters = selected, .after = position.sparse_seek.after, .position = position, .length = domain.len, .end = end };
    }
    pub fn finish(self: Window, a: A, page: b.Page(*const v.Node)) !b.Page(*const v.Node) {
        var result = page;
        var position = self.position;
        if (page.unfiltered) {
            // A fallback after an earlier indexed window could duplicate rows.
            // An immutable, type-certified owner must make this decision on
            // its first window, including integer-to-DOUBLE representability.
            if (position.sparse_seek.offset != 0) return error.CatalogGenerationChanged;
            position.sparse_seek.full_scan = true;
        }
        if (page.next) |next| {
            position.sparse_seek.after = next;
        } else if (!position.sparse_seek.full_scan and self.end < self.length) {
            position.sparse_seek.offset = self.end;
            position.sparse_seek.after = null;
        } else {
            result.next = null;
            return result;
        }
        result.next = try std.json.Stringify.valueAlloc(a, position, .{});
        return result;
    }
};

test "sparse seek windows bound transport and preserve fallback continuation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try a.alloc(v.Value, 400);
    for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    const filters = &[_]b.PropertyFilter{.{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .integer }};
    var after: ?[]const u8 = null;
    var delivered: usize = 0;
    var packets: usize = 0;
    while (true) {
        const window = (try Window.open(a, filters, after)).?;
        const selected = window.filters[0].value.list;
        try std.testing.expect(selected.len <= width);
        try std.testing.expectEqual(@as(i64, @intCast(delivered)), selected[0].integer);
        delivered += selected.len;
        packets += 1;
        after = (try window.finish(a, .{ .items = &.{}, .examined = 0 })).next;
        if (after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 400), delivered);
    try std.testing.expectEqual(@as(usize, 4), packets);
    const initial = (try Window.open(a, filters, null)).?;
    const fallback = try initial.finish(a, .{ .items = &.{}, .unfiltered = true, .next = "native-primary-position" });
    const resumed = (try Window.open(a, filters, fallback.next)).?;
    try std.testing.expectEqual(@as(usize, 400), resumed.filters[0].value.list.len);
    try std.testing.expect(resumed.filters[0].domain == null);
    try std.testing.expectEqualStrings("native-primary-position", resumed.after.?);
    try std.testing.expect((try resumed.finish(a, .{ .items = &.{}, .unfiltered = true })).next == null);
    const second = (try Window.open(a, filters, (try initial.finish(a, .{ .items = &.{} })).next)).?;
    try std.testing.expectError(error.CatalogGenerationChanged, second.finish(a, .{ .items = &.{}, .unfiltered = true }));
}

test "sparse seek certified boundaries and global precision proof" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 128, 256, 257, 400, 4096 }) |size| {
        const values = try a.alloc(v.Value, size);
        for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
        values[size - 1] = .{ .integer = 9007199254740993 };
        const filters = &[_]b.PropertyFilter{.{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .integer_inexact_double }};
        if (size == width) {
            try std.testing.expect(try Window.open(a, filters, null) == null);
            continue;
        }
        var after: ?[]const u8 = null;
        var delivered: usize = 0;
        while (true) {
            const window = (try Window.open(a, filters, after)).?;
            try std.testing.expectEqual(b.ScalarDomain.integer_inexact_double, window.filters[0].domain.?);
            delivered += window.filters[0].value.list.len;
            after = (try window.finish(a, .{ .items = &.{} })).next;
            if (after == null) break;
        }
        try std.testing.expectEqual(size, delivered);
        // Unproved mixed domains never get split: an owner's residual scan
        // must see the complete original list and its native cursor.
        var unproved = filters.*;
        unproved[0].domain = null;
        try std.testing.expect(try Window.open(a, &unproved, "native-position") == null);
    }
}
