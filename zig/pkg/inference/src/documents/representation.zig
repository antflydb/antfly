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

//! Ownership, source geometry and text offsets for accepted document revisions.
const std = @import("std");
const c = @import("contract.zig");
const A = std.mem.Allocator;

pub fn cloneValue(a: A, value: ?std.json.Value) !?std.json.Value {
    const v = value orelse return null;
    return try cloneJson(a, v, 0);
}
// Copy the tree once, without an intermediate serialization or conversion of
// exact number strings to floating point. The caller owns an arena.
fn cloneJson(a: A, value: std.json.Value, depth: usize) error{ OutOfMemory, InvalidDocumentModelResult }!std.json.Value {
    if (depth > 64) return error.InvalidDocumentModelResult;
    return switch (value) {
        .string => |s| .{ .string = try a.dupe(u8, s) },
        .number_string => |s| .{ .number_string = try a.dupe(u8, s) },
        .array => |items| blk: {
            var copied = std.json.Array.init(a);
            try copied.ensureTotalCapacity(items.items.len);
            for (items.items) |item| copied.appendAssumeCapacity(try cloneJson(a, item, depth + 1));
            break :blk .{ .array = copied };
        },
        .object => |fields| blk: {
            var copied: std.json.ObjectMap = .empty;
            try copied.ensureTotalCapacity(a, fields.count());
            for (fields.keys(), fields.values()) |key, item| try copied.put(a, try a.dupe(u8, key), try cloneJson(a, item, depth + 1));
            break :blk .{ .object = copied };
        },
        else => value,
    };
}
pub fn cloneTables(a: A, tables: []const c.Table, limit: usize) ![]c.Table {
    if (tables.len > limit) return error.DocumentRegionLimitExceeded;
    const out = try a.dupe(c.Table, tables);
    var cells: usize = 0;
    for (out) |*table| {
        if (table.cells.len > limit -| cells) return error.DocumentRegionLimitExceeded;
        cells += table.cells.len;
        const copied = try a.dupe(c.Cell, table.cells);
        for (copied) |*cell| cell.text = try a.dupe(u8, cell.text);
        table.cells = copied;
    }
    return out;
}
pub fn cloneRegions(a: A, regions: []const c.Region, limit: usize) ![]c.Region {
    var remaining = limit;
    return cloneRegionTree(a, regions, &remaining, 0);
}
fn cloneRegionTree(a: A, regions: []const c.Region, remaining: *usize, depth: usize) ![]c.Region {
    if (depth > 16 or regions.len > remaining.*) return error.DocumentRegionLimitExceeded;
    remaining.* -= regions.len;
    const out = try a.dupe(c.Region, regions);
    for (out) |*region| {
        region.text = try a.dupe(u8, region.text);
        if (region.label) |label| region.label = try a.dupe(u8, label);
        if (region.model) |model| region.model = try a.dupe(u8, model);
        region.fields = try cloneValue(a, region.fields);
        region.tables = try cloneTables(a, region.tables, remaining.* + regions.len);
        region.children = try cloneRegionTree(a, region.children, remaining, depth + 1);
    }
    return out;
}

/// Keep text outside detected regions. A regional correction replaces only its
/// original text range; it never rebuilds a page from an incomplete region list.
pub fn assemble(a: A, text: []const u8, before: []const c.Region, after: []c.Region) ![]const u8 {
    var output = std.Io.Writer.Allocating.init(a);
    var cursor: usize = 0;
    for (before, after) |old, *new| {
        const start = std.mem.indexOfPos(u8, text, cursor, old.text) orelse {
            if (!std.mem.eql(u8, old.text, new.text)) return error.InvalidDocumentRegion;
            new.text_start = null;
            new.text_end = null;
            continue;
        };
        try output.writer.writeAll(text[cursor..start]);
        if (old.text.len == 0 and output.written().len > 0 and !std.ascii.isWhitespace(output.written()[output.written().len - 1])) try output.writer.writeByte('\n');
        new.text_start = output.written().len;
        try output.writer.writeAll(new.text);
        new.text_end = output.written().len;
        cursor = start + old.text.len;
    }
    try output.writer.writeAll(text[cursor..]);
    return output.toOwnedSlice();
}
pub fn annotate(text: []const u8, regions: []c.Region) void {
    annotateAt(text, regions, 0, 0);
}
fn annotateAt(text: []const u8, regions: []const c.Region, base: usize, depth: usize) void {
    if (depth > 16) return;
    var cursor: usize = 0;
    for (@constCast(regions)) |*region| {
        region.text_start = null;
        region.text_end = null;
        const start = std.mem.indexOfPos(u8, text, cursor, region.text) orelse continue;
        region.text_start = base + start;
        region.text_end = base + start + region.text.len;
        cursor = start + region.text.len;
        annotateAt(region.text, region.children, base + start, depth + 1);
    }
}
pub fn hasFinding(page: c.Page, needle: []const u8) bool {
    for (page.findings) |item| if (std.mem.eql(u8, item, needle)) return true;
    return false;
}
pub fn finding(a: A, page: *c.Page, value: []const u8, present: bool) !void {
    var out = std.ArrayList([]const u8).empty;
    for (page.findings) |item| if (!std.mem.eql(u8, item, value)) try out.append(a, item);
    if (present) try out.append(a, value);
    page.findings = try out.toOwnedSlice(a);
}

pub const TableProjection = struct { tables: []const c.Table, resolved: bool };
pub fn projectTables(a: A, initial: []const c.Table, regions: []const c.Region, limit: usize) !TableProjection {
    var out = std.ArrayList(c.Table).empty;
    var resolved = true;
    for (initial) |table| {
        var replaced = false;
        for (regions) |region| {
            // Accepted revisions replace their owned structure, including an empty table set.
            if (!region.validated and region.tables.len == 0) continue;
            if (table.region_id) |owner| {
                if (owner == region.id) replaced = true;
            } else if (region.bbox) |box| {
                var located = table.cells.len > 0;
                var inside = true;
                var intersects = false;
                for (table.cells) |cell| {
                    const cell_box = cell.bbox orelse {
                        located = false;
                        break;
                    };
                    intersects = intersects or (cell_box[0] < box[2] and cell_box[2] > box[0] and cell_box[1] < box[3] and cell_box[3] > box[1]);
                    inside = inside and cell_box[0] >= box[0] and cell_box[1] >= box[1] and cell_box[2] <= box[2] and cell_box[3] <= box[3];
                }
                if (located and inside) replaced = true;
                if (!located or (intersects and !inside)) resolved = false;
            } else resolved = false;
        }
        if (!replaced) try out.append(a, table);
    }
    for (regions) |region| for (region.tables) |table| {
        if (out.items.len >= limit) return error.DocumentRegionLimitExceeded;
        var projected = table;
        projected.region_id = region.id;
        try out.append(a, projected);
    };
    return .{ .tables = try out.toOwnedSlice(a), .resolved = resolved };
}

pub fn transformRegions(regions: []c.Region, transform: c.Transform, offset: [2]f64, interpret: bool) !void {
    for (regions) |*region| {
        if (interpret) region.origin = .interpretation;
        if (region.bbox) |box| region.bbox = try translatedBox(transform, box, offset);
        try transformRegions(@constCast(region.children), transform, offset, interpret);
        try transformTables(@constCast(region.tables), transform, offset, interpret);
    }
}
pub fn transformTables(tables: []c.Table, transform: c.Transform, offset: [2]f64, interpret: bool) !void {
    for (tables) |*table| {
        if (interpret) table.origin = .interpretation;
        for (@constCast(table.cells)) |*cell| if (cell.bbox) |box| {
            cell.bbox = try translatedBox(transform, box, offset);
        };
    }
}
fn translatedBox(transform: c.Transform, supplied: [4]f64, offset: [2]f64) ![4]f64 {
    var box = supplied;
    box[0] += offset[0];
    box[2] += offset[0];
    box[1] += offset[1];
    box[3] += offset[1];
    return transform.box(box);
}

test "document JSON copies preserve exact numeric types and own nested values" {
    const Fixture = struct {
        fn check(backing: A) !void {
            var target = std.heap.ArenaAllocator.init(backing);
            defer target.deinit();
            const copied = blk: {
                var source = std.heap.ArenaAllocator.init(std.testing.allocator);
                defer source.deinit();
                const value = try std.json.parseFromSliceLeaky(std.json.Value, source.allocator(),
                    \\{"amount":9007199254740993.01,"items":[{"tiny":1e-999,"big":18446744073709551616}],"text":"owned"}
                , .{ .parse_numbers = false });
                break :blk (try cloneValue(target.allocator(), value)).?;
            };
            const encoded = try std.json.Stringify.valueAlloc(target.allocator(), copied, .{});
            try std.testing.expectEqualStrings(
                \\{"amount":9007199254740993.01,"items":[{"tiny":1e-999,"big":18446744073709551616}],"text":"owned"}
            , encoded);
            const integer = (try cloneValue(target.allocator(), .{ .integer = 7 })).?;
            try std.testing.expect(integer == .integer and integer.integer == 7);
            const float = (try cloneValue(target.allocator(), .{ .float = 1.25 })).?;
            try std.testing.expect(float == .float and float.float == 1.25);
        }
    };
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
