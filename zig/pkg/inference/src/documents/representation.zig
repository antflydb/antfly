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

/// Source-local IDs use the full unsigned machine range. Keep wide values as
/// exact JSON tokens instead of narrowing them to signed integers or floats.
pub fn unsignedValue(a: A, value: usize) !std.json.Value {
    if (std.math.cast(i64, value)) |integer| return .{ .integer = integer };
    return .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{value}) };
}

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
// One page-wide bound per structure family, including nested and inherited
// content. Validate without allocation before retaining/copying model results.
const StructureBudget = struct {
    regions_left: usize,
    tables_left: usize,
    cells_left: usize,
    fn init(limit: usize) StructureBudget {
        return .{ .regions_left = limit, .tables_left = limit, .cells_left = limit };
    }
    fn tables(self: *StructureBudget, items: []const c.Table) !void {
        if (items.len > self.tables_left) return error.DocumentRegionLimitExceeded;
        self.tables_left -= items.len;
        for (items) |table| {
            if (table.cells.len > self.cells_left) return error.DocumentRegionLimitExceeded;
            self.cells_left -= table.cells.len;
        }
    }
    fn regions(self: *StructureBudget, items: []const c.Region, depth: usize) error{DocumentRegionLimitExceeded}!void {
        if (depth > 16 or items.len > self.regions_left) return error.DocumentRegionLimitExceeded;
        self.regions_left -= items.len;
        for (items) |region| {
            try self.tables(region.tables);
            try self.regions(region.children, depth + 1);
        }
    }
};

pub fn validatePage(page: c.Page, limit: usize) !void {
    var budget = StructureBudget.init(limit);
    try budget.regions(page.regions, 0);
    try budget.tables(page.tables);
    for (page.inherited_parts) |part| try budget.tables(part.tables);
}

pub fn cloneTables(a: A, tables: []const c.Table, limit: usize) ![]c.Table {
    var budget = StructureBudget.init(limit);
    try budget.tables(tables);
    return cloneTablesUnchecked(a, tables);
}
fn cloneTablesUnchecked(a: A, tables: []const c.Table) ![]c.Table {
    const out = try a.dupe(c.Table, tables);
    for (out) |*table| {
        const copied = try a.dupe(c.Cell, table.cells);
        for (copied) |*cell| cell.text = try a.dupe(u8, cell.text);
        table.cells = copied;
    }
    return out;
}
pub fn cloneRegions(a: A, regions: []const c.Region, limit: usize) ![]c.Region {
    var budget = StructureBudget.init(limit);
    try budget.regions(regions, 0);
    return cloneRegionTree(a, regions);
}
// The complete tree was bounded before allocating. Each family keeps its
// own allowance; consuming region slots must not shrink a child's cell budget.
fn cloneRegionTree(a: A, regions: []const c.Region) error{ OutOfMemory, InvalidDocumentModelResult }![]c.Region {
    const out = try a.dupe(c.Region, regions);
    for (out) |*region| {
        region.text = try a.dupe(u8, region.text);
        if (region.label) |label| region.label = try a.dupe(u8, label);
        if (region.model) |model| region.model = try a.dupe(u8, model);
        region.fields = try cloneValue(a, region.fields);
        region.tables = try cloneTablesUnchecked(a, region.tables);
        region.children = try cloneRegionTree(a, region.children);
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
    return projectRevisedTables(a, initial, regions, &.{}, limit);
}

/// Only this run's accepted revisions replace an owner's authoritative tables.
/// Existing validation metadata and nested tables do not imply a new revision.
pub fn projectRevisedTables(a: A, initial: []const c.Table, regions: []const c.Region, revisions: []const bool, limit: usize) !TableProjection {
    if (initial.len > limit or regions.len > limit) return error.DocumentRegionLimitExceeded;
    std.debug.assert(revisions.len == 0 or revisions.len == regions.len);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const work = scratch.allocator();
    const Key = struct { owner: usize, id: usize, payload: [32]u8 };
    var owners: std.AutoHashMapUnmanaged(usize, bool) = .empty;
    const Capacity = struct { total: usize = 0, owned: usize = 0, used: usize = 0, used_owned: usize = 0 };
    var aliases: std.AutoHashMapUnmanaged(Key, Capacity) = .empty;
    var changed = std.ArrayList(usize).empty;
    var table_count: usize = 0;
    for (regions) |region| {
        if (region.tables.len > limit -| table_count) return error.DocumentRegionLimitExceeded;
        table_count += region.tables.len;
    }
    for (regions, 0..) |region, i| {
        const revised = revisions.len > 0 and revisions[i];
        const owner = try owners.getOrPut(work, region.id);
        if (owner.found_existing) return error.InvalidDocumentModelResult;
        owner.value_ptr.* = revised;
        if (revised) try changed.append(work, i);
        var self_shadowed = false;
        for (region.children) |child| if (child.id == region.id) {
            self_shadowed = true;
            break;
        };
        if (initial.len > 0) for (region.tables) |table| {
            const alias = try aliases.getOrPut(work, .{ .owner = region.id, .id = table.id, .payload = @import("content.zig").tableViewDigest(table) });
            if (!alias.found_existing) alias.value_ptr.* = .{};
            alias.value_ptr.total += 1;
            if (table.region_id == null or (table.region_id.? == region.id and !self_shadowed)) alias.value_ptr.owned += 1;
        };
    }
    var out = std.ArrayList(c.Table).empty;
    errdefer out.deinit(a);
    var resolved = true;
    // Reserve explicit links before inferring legacy aliases. This keeps table
    // multiplicity independent of the order of flat records.
    for ([_]bool{ true, false }) |declared| for (initial) |table| {
        if (table.projection != declared) continue;
        if (table.region_id) |id| {
            if (owners.get(id) orelse false) continue;
            const key = Key{ .owner = id, .id = table.id, .payload = @import("content.zig").tableViewDigest(table) };
            if (aliases.getPtr(key)) |remaining| if (remaining.used < remaining.total and (declared or remaining.used_owned < remaining.owned)) {
                remaining.used += 1;
                if (!declared) remaining.used_owned += 1;
                continue;
            };
            if (declared) return error.InvalidDocumentModelResult;
        } else {
            if (declared) return error.InvalidDocumentModelResult;
            var replaced = false;
            for (changed.items) |i| {
                const box = regions[i].bbox orelse {
                    resolved = false;
                    continue;
                };
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
            }
            if (replaced) continue;
        }
        if (out.items.len >= limit) return error.DocumentRegionLimitExceeded;
        try out.append(a, table);
    };
    for (regions) |region| for (region.tables) |table| {
        if (out.items.len >= limit) return error.DocumentRegionLimitExceeded;
        var projected = table;
        projected.region_id = region.id;
        projected.projection = true;
        try out.append(a, projected);
    };
    return .{ .tables = try out.toOwnedSlice(a), .resolved = resolved };
}

pub fn isLiteral(origin: c.Origin) bool {
    return origin == .native or origin == .transcription;
}

/// Parent interpretation can never become literal evidence in a child. Keep
/// a child's more specific nonliteral provenance when one was provided.
pub fn inheritOrigin(declared: ?c.Origin, parent: c.Origin) c.Origin {
    const origin = declared orelse return parent;
    return switch (origin) {
        .interpretation, .derived, .estimated => origin,
        .native, .transcription => switch (parent) {
            .interpretation, .derived, .estimated => parent,
            .native, .transcription => origin,
        },
    };
}

pub fn transformRegions(regions: []c.Region, transform: c.Transform, offset: [2]f64, interpret: bool) !void {
    for (regions) |*region| {
        if (interpret) region.origin = inheritOrigin(region.origin, .interpretation);
        if (region.bbox) |box| region.bbox = try translatedBox(transform, box, offset);
        try transformRegions(@constCast(region.children), transform, offset, interpret);
        try transformTables(@constCast(region.tables), transform, offset, interpret);
    }
}
pub fn transformTables(tables: []c.Table, transform: c.Transform, offset: [2]f64, interpret: bool) !void {
    for (tables) |*table| {
        if (interpret) table.origin = inheritOrigin(table.origin, .interpretation);
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

test "document structure bounds include nested sibling tables and inherited parts before copies" {
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "Cell" }};
    const tables = [_]c.Table{.{ .id = 0, .rows = 1, .columns = 1, .cells = &cells }};
    const children = [_]c.Region{.{ .id = 1, .text = "Child", .tables = &tables }};
    const regions = [_]c.Region{.{ .id = 0, .text = "Parent", .tables = &tables, .children = &children }};
    const inherited = [_]c.InheritedPart{.{ .path = "master", .role = .master, .text = "Master", .tables = &tables, .shapes_visible = true, .background_visible = false, .shape_visual = false, .background_visual = false }};
    var page: c.Page = .{ .source_id = "source", .number = 1, .kind = .page, .regions = &regions, .inherited_parts = &inherited };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, validatePage(page, 2));
    try validatePage(page, 3);
    page.inherited_parts = &.{};
    try validatePage(page, 2);
    try std.testing.expectError(error.DocumentRegionLimitExceeded, validatePage(page, 1));
    // No output allocator capacity is needed to reject an oversized tree.
    const siblings = [_]c.Region{ .{ .id = 0, .text = "One", .tables = &tables }, .{ .id = 1, .text = "Two", .tables = &tables }, .{ .id = 2, .text = "Three", .tables = &tables } };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, cloneRegions(std.testing.failing_allocator, &siblings, 2));
    const repeated = [_]c.Table{ tables[0], tables[0], tables[0] };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, cloneTables(std.testing.failing_allocator, &repeated, 2));
}

test "document nested region cells retain their independent structural allowance" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cells = [_]c.Cell{ .{ .row = 0, .column = 0, .text = "One" }, .{ .row = 0, .column = 1, .text = "Two" } };
    const tables = [_]c.Table{.{ .id = 0, .rows = 1, .columns = 2, .cells = &cells }};
    const children = [_]c.Region{.{ .id = 1, .text = "Child", .tables = &tables }};
    const regions = [_]c.Region{.{ .id = 0, .text = "Parent", .children = &children }};
    const copy = try cloneRegions(arena.allocator(), &regions, 2);
    try std.testing.expectEqualStrings("Two", copy[0].children[0].tables[0].cells[1].text);
}

test "document projection preserves authoritative multiplicity and only replaces explicit revisions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "cell", .bbox = .{ 0, 0, 1, 1 } }};
    const table = c.Table{ .id = 1, .region_id = 7, .rows = 1, .columns = 1, .cells = &cells };
    const nested = c.Table{ .id = 2, .rows = 1, .columns = 1, .cells = &cells };
    const regions = [_]c.Region{.{ .id = 7, .text = "cell", .validated = true, .bbox = .{ 0, 0, 2, 2 }, .tables = &.{nested} }};
    // Even a previously validated reader report is not a revision in this run.
    const preserved = try projectTables(a, &.{table}, &regions, 8);
    try std.testing.expectEqual(@as(usize, 2), preserved.tables.len);
    try std.testing.expectEqual(@as(usize, 1), preserved.tables[0].id);
    const replaced = try projectRevisedTables(a, &.{table}, &regions, &.{true}, 8);
    try std.testing.expectEqual(@as(usize, 1), replaced.tables.len);
    try std.testing.expectEqual(@as(usize, 2), replaced.tables[0].id);
    const empty = [_]c.Region{.{ .id = 7, .text = "cell", .validated = true }};
    try std.testing.expectEqual(@as(usize, 0), (try projectRevisedTables(a, &.{table}, &empty, &.{true}, 8)).tables.len);
    var legacy = nested;
    legacy.region_id = 7;
    var declared = legacy;
    declared.projection = true;
    const repeat = try projectTables(a, &.{ legacy, declared }, &regions, 8);
    try std.testing.expectEqual(@as(usize, 2), repeat.tables.len);
    try std.testing.expect(!repeat.tables[0].projection and repeat.tables[1].projection);
    try std.testing.expectError(error.InvalidDocumentModelResult, projectTables(a, &.{ declared, declared }, &regions, 8));
    const stable = try projectTables(a, preserved.tables, &regions, 8);
    try std.testing.expectEqual(@as(usize, 2), stable.tables.len);
    var unowned = table;
    unowned.region_id = null;
    try std.testing.expectEqual(@as(usize, 1), (try projectRevisedTables(a, &.{unowned}, &empty, &.{true}, 8)).tables.len);
    try std.testing.expectEqual(@as(usize, 1), (try projectRevisedTables(a, &.{unowned}, &regions, &.{true}, 8)).tables.len);
}

test "document projection releases indexed scratch and output on allocation failures" {
    const Runner = struct {
        fn run(a: A) !void {
            const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "cell" }};
            const table = c.Table{ .id = 1, .region_id = 7, .rows = 1, .columns = 1, .cells = &cells };
            const regions = [_]c.Region{.{ .id = 7, .text = "cell", .tables = &.{.{ .id = 2, .rows = 1, .columns = 1, .cells = &cells }} }};
            const result = try projectTables(a, &.{table}, &regions, 8);
            defer a.free(result.tables);
            try std.testing.expectEqual(@as(usize, 2), result.tables.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
