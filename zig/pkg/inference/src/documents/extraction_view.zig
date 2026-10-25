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

//! Canonical extraction context for structured readings. Logical table views
//! share the corroboration index; raw cell bytes retain component-local proof.
const std = @import("std");
const c = @import("contract.zig");
const content = @import("content.zig");
const unsignedValue = @import("representation.zig").unsignedValue;
const output = @import("output.zig");
const V = std.json.Value;
const A = std.mem.Allocator;
fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
fn present(value: ?V) ?V {
    const item = value orelse return null;
    return if (item == .null) null else item;
}
fn typed(comptime T: type, a: A, value: V) !T {
    return std.json.parseFromValueLeaky(T, a, value, .{ .ignore_unknown_fields = true, .allocate = .alloc_if_needed }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidDocumentModelResult,
    };
}
fn origin(a: A, value: V, inherited: c.Origin) !c.Origin {
    return if (present(field(value, "origin"))) |declared| try typed(c.Origin, a, declared) else inherited;
}
fn tables(a: A, value: ?V, inherited: c.Origin) ![]c.Table {
    const items = (present(value) orelse return &.{}).array.items;
    const result = try a.alloc(c.Table, items.len);
    for (items, result) |item, *table| {
        table.* = try typed(c.Table, a, item);
        table.origin = try origin(a, item, inherited);
    }
    return result;
}
fn regions(a: A, value: ?V, inherited: c.Origin) error{ OutOfMemory, InvalidDocumentModelResult }![]c.Region {
    const items = (present(value) orelse return &.{}).array.items;
    const result = try a.alloc(c.Region, items.len);
    for (items, result, 0..) |item, *region, index| {
        const effective = try origin(a, item, inherited);
        region.* = .{
            .id = if (field(item, "id")) |id| try typed(usize, a, id) else index,
            .text = "",
            .origin = effective,
            .tables = try tables(a, field(item, "tables"), effective),
            .children = try regions(a, field(item, "children"), effective),
        };
    }
    return result;
}
fn hasTables(page: V) bool {
    if (present(field(page, "tables"))) |list| if (list.array.items.len > 0) return true;
    if (present(field(page, "regions"))) |list| for (list.array.items) |region| {
        if (hasRegionTables(region)) return true;
    };
    return false;
}
fn hasRegionTables(region: V) bool {
    if (present(field(region, "tables"))) |list| if (list.array.items.len > 0) return true;
    if (present(field(region, "children"))) |list| for (list.array.items) |child| {
        if (hasRegionTables(child)) return true;
    };
    return false;
}
pub fn appendSegment(segments: *std.json.Array, segment: V, remaining: *usize) !void {
    const bytes = try output.measure(segment, remaining.*);
    if (bytes >= remaining.*) return error.DocumentOutputTooLarge;
    remaining.* -= bytes + 1;
    try segments.append(segment);
}
fn componentSegment(a: A, page: V, reference: content.TableReference, path: V, start: usize, end: usize, role: []const u8, effective: c.Origin) !V {
    var value = V{ .object = .{} };
    try value.object.put(a, "number", field(page, "number") orelse return error.InvalidDocumentArtifact);
    try value.object.put(a, "start", .{ .integer = @intCast(start) });
    try value.object.put(a, "end", .{ .integer = @intCast(end) });
    try value.object.put(a, "role", .{ .string = role });
    try value.object.put(a, "origin", .{ .string = @tagName(effective) });
    try value.object.put(a, "table_id", try unsignedValue(a, reference.table.id));
    try value.object.put(a, "region_path", path);
    var scope = std.json.Array.init(a);
    try scope.ensureTotalCapacity(reference.table_scope.len);
    for (reference.table_scope) |id| scope.appendAssumeCapacity(try unsignedValue(a, id));
    try value.object.put(a, "table_scope", .{ .array = scope });
    try value.object.put(a, "table_index", try unsignedValue(a, reference.table_index));
    if (field(page, "coordinate_space")) |space| try value.object.put(a, "coordinate_space", space);
    return value;
}
const CellView = struct { cell: c.Cell, index: usize };
fn less(_: void, left: CellView, right: CellView) bool {
    if (left.cell.row != right.cell.row) return left.cell.row < right.cell.row;
    if (left.cell.column != right.cell.column) return left.cell.column < right.cell.column;
    return left.index < right.index;
}
pub fn appendTables(a: A, backing: A, page: V, writer: *output.Writer, segments: *std.json.Array, remaining: *usize, limits: c.Limits, control: c.Control) !void {
    try control.check();
    try @import("reader_result.zig").validateStructures(page, limits.max_regions);
    if (!hasTables(page)) return;
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    const work = scratch.allocator();
    const effective = try origin(work, page, .transcription);
    const tree = try regions(work, field(page, "regions"), effective);
    const flat = try tables(work, field(page, "tables"), effective);
    const selected = try content.authoritativeTables(work, tree, flat);
    for (selected) |reference| {
        try control.check();
        const table = reference.table;
        var path = std.json.Array.init(a);
        try path.ensureTotalCapacity(reference.region_path.len);
        for (reference.region_path) |id| path.appendAssumeCapacity(try unsignedValue(a, id));
        const owner = V{ .array = path };
        var start = writer.len();
        try writer.raw("\n\n[Table ");
        try writer.value(table.id);
        try writer.raw("; scope ");
        try writer.value(reference.table_scope);
        try writer.raw("; index ");
        try writer.value(reference.table_index);
        try writer.raw("; region ");
        try writer.value(reference.region_path);
        try writer.raw("; rows ");
        try writer.value(table.rows);
        try writer.raw("; columns ");
        try writer.value(table.columns);
        try writer.raw("]");
        try appendSegment(segments, try componentSegment(a, page, reference, owner, start, writer.len(), "table_structure", .derived), remaining);
        const cells = try work.alloc(CellView, table.cells.len);
        for (table.cells, cells, 0..) |cell, *view, index| view.* = .{ .cell = cell, .index = index };
        std.mem.sort(CellView, cells, {}, less);
        for (cells) |view| {
            const cell = view.cell;
            try control.check();
            if (cell.row >= table.rows or cell.column >= table.columns or cell.row_span == 0 or cell.column_span == 0 or cell.row_span > table.rows - cell.row or cell.column_span > table.columns - cell.column) return error.InvalidDocumentModelResult;
            start = writer.len();
            try writer.raw("\n[row ");
            try writer.value(cell.row);
            try writer.raw(", column ");
            try writer.value(cell.column);
            try writer.raw(", row_span ");
            try writer.value(cell.row_span);
            try writer.raw(", column_span ");
            try writer.value(cell.column_span);
            try writer.raw("] ");
            try appendSegment(segments, try componentSegment(a, page, reference, owner, start, writer.len(), "table_structure", .derived), remaining);
            start = writer.len();
            try writer.raw(cell.text);
            if (writer.len() == start) continue;
            var proof = try componentSegment(a, page, reference, owner, start, writer.len(), "table_cell", table.origin);
            try proof.object.put(a, "cell_index", try unsignedValue(a, view.index));
            inline for (.{ "row", "column", "row_span", "column_span" }) |key| try proof.object.put(a, key, try unsignedValue(a, @field(cell, key)));
            if (cell.bbox) |box| {
                var bounds = std.json.Array.init(a);
                try bounds.ensureTotalCapacity(4);
                for (box) |coordinate| bounds.appendAssumeCapacity(.{ .float = coordinate });
                try proof.object.put(a, "bbox", .{ .array = bounds });
            }
            try appendSegment(segments, proof, remaining);
        }
    }
}

const TestView = struct {
    text: []const u8,
    metadata: V,
    fn init(a: A, page: V, limit: usize) !TestView {
        var writer = output.Writer.init(a, limit);
        defer writer.deinit();
        const text = field(page, "text").?.string;
        try writer.raw(text);
        var segments = std.json.Array.init(a);
        var original = V{ .object = .{} };
        try original.object.put(a, "number", field(page, "number").?);
        try original.object.put(a, "start", .{ .integer = 0 });
        try original.object.put(a, "end", .{ .integer = @intCast(text.len) });
        for ([_][]const u8{ "origin", "regions" }) |key| if (field(page, key)) |value| try original.object.put(a, key, value);
        var remaining = limit;
        try appendSegment(&segments, original, &remaining);
        try appendTables(a, a, page, &writer, &segments, &remaining, .{ .max_output_bytes = limit }, .{});
        var metadata = V{ .object = .{} };
        try metadata.object.put(a, "source_id", .{ .string = "receipt" });
        try metadata.object.put(a, "segments", .{ .array = segments });
        return .{ .text = try writer.toOwnedSlice(), .metadata = metadata };
    }
    fn extract(self: TestView, a: A, value: []const u8, allowed: bool) !struct { supported: bool, value: V } {
        const start = std.mem.indexOf(u8, self.text, value).?;
        const wire = try std.json.Stringify.valueAlloc(a, .{ .structures = .{ .r = .{.{ .field = .{ .value = value, .start = start, .end = start + value.len } }} } }, .{});
        var item = try std.json.parseFromSliceLeaky(V, a, wire, .{});
        const supported = try @import("evidence.zig").attach(a, &item, self.text, self.metadata, true, allowed);
        return .{ .supported = supported, .value = item.object.get("structures").?.object.get("r").?.array.items[0].object.get("field").? };
    }
};

test "document extraction table views share logical aliases and retain qualified cell evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const page = try std.json.parseFromSliceLeaky(V, a,
        \\{"number":4,"text":"Receipt","origin":"transcription","coordinate_space":"image_pixels","regions":[{"id":7,"text":"Receipt","tables":[{"id":3,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"12.50","bbox":[0,0,1,1]}],"origin":"transcription"}]}],"tables":[{"id":3,"region_id":7,"projection":true,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"12.50","bbox":[0,0,1,1]}],"origin":"transcription"}]}
    , .{});
    const view = try TestView.init(a, page, 16384);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, view.text, "12.50"));
    const result = try view.extract(a, "12.50", false);
    try std.testing.expect(result.supported);
    const proof = field(result.value, "evidence").?.array.items[0];
    try std.testing.expectEqualStrings("table_cell", field(proof, "role").?.string);
    try std.testing.expectEqual(@as(i64, 4), field(proof, "page_number").?.integer);
    try std.testing.expectEqual(@as(i64, 3), field(proof, "table_id").?.integer);
    try std.testing.expectEqual(@as(i64, 7), field(proof, "table_scope").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 0), field(proof, "table_index").?.integer);
    try std.testing.expectEqual(@as(i64, 0), field(proof, "start").?.integer);
    try std.testing.expectEqual(@as(i64, 5), field(proof, "end").?.integer);
    try std.testing.expectEqual(@as(i64, 0), field(proof, "row").?.integer);
    try std.testing.expectEqual(@as(usize, 4), field(proof, "bbox").?.array.items.len);
    // Generated layout labels cannot become literal document text.
    try std.testing.expect(!(try view.extract(a, "Table", false)).supported);
    const label = try view.extract(a, "Table", true);
    try std.testing.expect(label.supported);
    try std.testing.expectEqualStrings("derived", field(label.value, "origin").?.string);
    try std.testing.expect(field(label.value, "start") == null);
}

test "document table context retains duplicate instances nested ownership and cell byte order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const page = try std.json.parseFromSliceLeaky(V, a,
        \\{"number":1,"text":"Receipt","regions":[{"id":7,"text":"Receipt","children":[{"id":11,"text":"Receipt","tables":[{"id":5,"rows":1,"columns":2,"cells":[{"row":0,"column":1,"text":"Éva\n12.50"},{"row":0,"column":0,"text":"Total"}]},{"id":5,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"Total"}]}]}],"tables":[{"id":5,"region_id":11,"projection":true,"rows":1,"columns":2,"cells":[{"row":0,"column":1,"text":"Éva\n12.50"},{"row":0,"column":0,"text":"Total"}]}]}],"tables":[{"id":5,"region_id":7,"projection":true,"rows":1,"columns":2,"cells":[{"row":0,"column":1,"text":"Éva\n12.50"},{"row":0,"column":0,"text":"Total"}]}]}
    , .{});
    const view = try TestView.init(a, page, 16384);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, view.text, "[Table "));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, view.text, "Total"));
    try std.testing.expect(std.mem.indexOf(u8, view.text, "Total").? < std.mem.indexOf(u8, view.text, "Éva").?);
    const result = try view.extract(a, "Éva\n12.50", false);
    try std.testing.expect(result.supported);
    const proof = field(result.value, "evidence").?.array.items[0];
    const path = field(proof, "region_path").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), path.len);
    try std.testing.expectEqual(@as(i64, 11), path[1].integer);
    try std.testing.expectEqual(@as(i64, 1), field(proof, "column").?.integer);
    try std.testing.expectEqual(@as(i64, 0), field(proof, "cell_index").?.integer);
}

test "document extraction table context preserves nonliteral origins and bounds before dispatch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]c.Origin{ .interpretation, .derived, .estimated }) |effective| {
        const wire = try std.json.Stringify.valueAlloc(a, .{ .number = 1, .text = "Receipt", .origin = effective, .tables = .{.{ .id = 3, .rows = 1, .columns = 1, .cells = .{.{ .row = 0, .column = 0, .text = "12.50" }} }} }, .{});
        const page = try std.json.parseFromSliceLeaky(V, a, wire, .{});
        const view = try TestView.init(a, page, 16384);
        try std.testing.expect(!(try view.extract(a, "12.50", false)).supported);
        try std.testing.expect((try view.extract(a, "12.50", true)).supported);
        try std.testing.expectError(error.DocumentOutputTooLarge, TestView.init(a, page, 128));
    }
    const malformed = try std.json.parseFromSliceLeaky(V, a,
        \\{"number":1,"text":"Receipt","tables":[{"id":1,"rows":1,"columns":1,"cells":[{"row":1,"column":0,"text":"bad"}]}]}
    , .{});
    try std.testing.expectError(error.InvalidDocumentModelResult, TestView.init(a, malformed, 16384));
}

test "document extraction table view releases every partial allocation" {
    const Runner = struct {
        fn run(backing: A) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const page = try std.json.parseFromSliceLeaky(V, a,
                \\{"number":1,"text":"Receipt","regions":[{"id":7,"text":"Receipt","tables":[{"id":1,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"12.50"}]}]}]}
            , .{});
            const view = try TestView.init(a, page, 16384);
            try std.testing.expect((try view.extract(a, "12.50", false)).supported);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "document table evidence preserves unsigned source identities without narrowing" {
    if (@bitSizeOf(usize) < 64) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const page = try std.json.parseFromSliceLeaky(V, a,
        \\{"number":1,"text":"Receipt","regions":[{"id":18446744073709551615,"text":"Receipt","tables":[{"id":18446744073709551615,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"12.50"}]}]}]}
    , .{ .parse_numbers = false });
    const view = try TestView.init(a, page, 16384);
    const result = try view.extract(a, "12.50", false);
    try std.testing.expect(result.supported);
    const proof = field(result.value, "evidence").?.array.items[0];
    try std.testing.expectEqualStrings("18446744073709551615", field(proof, "table_id").?.number_string);
    try std.testing.expectEqualStrings("18446744073709551615", field(proof, "region_path").?.array.items[0].number_string);
}
