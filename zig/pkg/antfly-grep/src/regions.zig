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

//! Lightweight source declaration scanner. Lexically masks strings/comments
//! before recognizing Python indentation and brace-language declaration bodies.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Region = struct { start: usize, end: usize, declaration: bool = false, label: []const u8 = "", label_line: usize = 0 };
pub fn build(a: Allocator, path: []const u8, bytes: []const u8, lines: anytype, max: usize) ![]Region {
    if (max == 1) return buildPrepared(a, bytes, lines, max, .{ .declarations = &.{}, .calls = &.{}, .complete = true });
    var index = try @import("source.zig").parse(a, path, bytes);
    defer index.deinit(a);
    return buildPrepared(a, bytes, lines, max, index);
}
/// Borrow the revision-validated preparation; never parse a source twice.
pub fn buildPrepared(a: Allocator, bytes: []const u8, lines: anytype, max: usize, index: @import("source.zig").Index) ![]Region {
    var out: std.ArrayList(Region) = .empty;
    errdefer out.deinit(a);
    if (max == 1) {
        for (lines, 0..) |_, i| try out.append(a, .{ .start = i, .end = i + 1 });
        return out.toOwnedSlice(a);
    }
    const covered = try a.alloc(bool, lines.len);
    defer a.free(covered);
    @memset(covered, false);
    const order = try a.alloc(usize, index.declarations.len);
    defer a.free(order);
    for (order, 0..) |*item, i| item.* = i;
    std.mem.sort(usize, order, index.declarations, struct {
        fn less(decls: []const @import("source.zig").Declaration, x: usize, y: usize) bool {
            const a_span = decls[x].end - decls[x].start;
            const b_span = decls[y].end - decls[y].start;
            return if (a_span == b_span) decls[x].start < decls[y].start else a_span < b_span;
        }
    }.less);
    for (order) |decl_index| {
        const decl = index.declarations[decl_index];
        var start = @min(decl.start - 1, lines.len);
        const end = @min(decl.end, lines.len);
        while (start < end) {
            if (covered[start]) {
                start += 1;
                continue;
            }
            var stop = start;
            while (stop < end and !covered[stop] and stop - start < max) stop += 1;
            for (covered[start..stop]) |*value| value.* = true;
            try out.append(a, .{ .start = start, .end = stop, .declaration = true, .label = decl.name, .label_line = decl.label_line });
            start = stop;
        }
    }
    var start: usize = 0;
    while (start < lines.len) {
        if (covered[start]) {
            start += 1;
            continue;
        }
        var end = start;
        while (end < lines.len and !covered[end] and end - start < max) : (end += 1) {
            if (end > start and std.mem.trim(u8, bytes[lines[end - 1].start..lines[end - 1].end], " \t\r").len == 0) break;
        }
        if (end == start) end += 1;
        try out.append(a, .{ .start = start, .end = end });
        start = end;
    }
    std.mem.sort(Region, out.items, {}, struct {
        fn less(_: void, x: Region, y: Region) bool {
            return if (x.start == y.start) x.end < y.end else x.start < y.start;
        }
    }.less);
    return out.toOwnedSlice(a);
}
test "masked braces cannot terminate a Go declaration" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const text = "func retry() {\n  log(\"}\")\n  stop()\n}\n";
    const Line = struct { start: usize, end: usize };
    const lines = [_]Line{ .{ .start = 0, .end = 14 }, .{ .start = 15, .end = 25 }, .{ .start = 26, .end = 34 }, .{ .start = 35, .end = 36 } };
    const result = try build(alloc, "a.go", text, &lines, 80);
    try std.testing.expect(result[0].declaration);
    try std.testing.expectEqual(@as(usize, 4), result[0].end);
}

test "prepared and parsing region builders agree on nested declarations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]struct { path: []const u8, text: []const u8 }{
        .{ .path = "a.py", .text = "class Outer:\n    def retry(self):\n        def inner():\n            STOP()\n        inner()\n\nother()\n" },
        .{ .path = "a.go", .text = "package main\nfunc retry() {\n STOP()\n}\n" },
        .{ .path = "a.rs", .text = "impl Retry {\n fn run() { stop(); }\n}\n" },
    }) |item| {
        const rows = try @import("source_cache.zig").lines(a, item.text, '\n', false);
        const index = try @import("source.zig").parse(a, item.path, item.text);
        for ([_]usize{ 1, 2, 80 }) |max| {
            const expected = try build(a, item.path, item.text, rows, max);
            const actual = try buildPrepared(a, item.text, rows, max, index);
            try std.testing.expectEqualDeep(expected, actual);
        }
    }
}
