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

//! Decode ordinary reader reports into the document representation. Structured
//! reports are authoritative; legacy text/region reports use the same path.
const std = @import("std");
const c = @import("contract.zig");
const ir = @import("representation.zig");
const readers = @import("antfly_readers");
const executor = @import("executor.zig");
const output = @import("antfly_documents").output;
const A = std.mem.Allocator;
const V = std.json.Value;

fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
fn present(value: ?V) ?V {
    const item = value orelse return null;
    return if (item == .null) null else item;
}
fn text(value: V) error{InvalidDocumentModelResult}![]const u8 {
    return if (value == .string) value.string else error.InvalidDocumentModelResult;
}
fn optionalText(value: ?V) error{InvalidDocumentModelResult}!?[]const u8 {
    return if (present(value)) |item| try text(item) else null;
}
fn parse(a: A, bytes: []const u8) !V {
    return std.json.parseFromSliceLeaky(V, a, bytes, .{ .duplicate_field_behavior = .@"error", .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidDocumentModelResult,
    };
}
fn typed(comptime T: type, a: A, value: V) !T {
    return std.json.parseFromValueLeaky(T, a, value, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidDocumentModelResult,
    };
}
fn origin(a: A, value: V, parent: c.Origin) !c.Origin {
    const declared = if (present(field(value, "origin"))) |item| try typed(c.Origin, a, item) else null;
    return ir.inheritOrigin(declared, parent);
}

pub fn decode(a: A, result: readers.Result, binding: c.Binding, limit: usize, byte_limit: usize, transform: c.Transform, offset: [2]f64) !executor.Candidate {
    var root = if (result.document_json) |json| try parse(a, json) else V{ .object = .{} };
    if (root != .object) return error.InvalidDocumentModelResult;
    // The ordinary boundary already owns these buffers in the item arena. Do
    // not serialize or deep-copy them again before the executor accepts them.
    if (!root.object.contains("text")) try root.object.put(a, "text", .{ .string = result.text });
    if (!root.object.contains("regions")) if (result.regions_json) |json| try root.object.put(a, "regions", try parse(a, json));
    if (!root.object.contains("fields")) if (result.fields_json) |json| try root.object.put(a, "fields", try parse(a, json));
    _ = try output.measure(root, byte_limit);
    var budget = Budget{ .regions = limit, .tables = limit, .cells = limit };
    try budget.checkRegions(present(field(root, "regions")), 0);
    try budget.checkTables(present(field(root, "tables")));
    const content = try text(field(root, "text").?);
    if (!std.mem.eql(u8, content, result.text)) return error.InvalidDocumentModelResult;
    const parent_origin = try origin(a, root, binding.output_origin);
    const regions = try decodeRegions(a, present(field(root, "regions")), parent_origin, binding.model);
    const tables = try decodeTables(a, present(field(root, "tables")), parent_origin);
    try ir.transformRegions(regions, transform, offset, false);
    try ir.transformTables(tables, transform, offset, false);
    const status = if (present(field(root, "status"))) |value| try typed(c.Status, a, value) else null;
    return .{
        .text = content,
        .model = binding.model,
        .origin = parent_origin,
        .regions = regions,
        .tables = tables,
        .fields = present(field(root, "fields")),
        // Neither a flattened completion nor a structured status may upgrade
        // an incomplete report supplied by the other representation.
        .truncated = result.completion == .length or status == .truncated,
        .status = if (result.completion == .unresolved) .unresolved else status,
    };
}

/// Bound retained structured pages before building a typed table view.
pub fn validateStructures(root: V, limit: usize) !void {
    var budget = Budget{ .regions = limit, .tables = limit, .cells = limit };
    try budget.checkRegions(present(field(root, "regions")), 0);
    try budget.checkTables(present(field(root, "tables")));
}

const Budget = struct {
    regions: usize,
    tables: usize,
    cells: usize,
    fn array(value: V) ![]const V {
        if (value != .array) return error.InvalidDocumentModelResult;
        return value.array.items;
    }
    fn checkRegions(self: *Budget, value: ?V, depth: usize) error{ InvalidDocumentModelResult, DocumentRegionLimitExceeded }!void {
        const items = try array(value orelse return);
        if (depth > 16 or items.len > self.regions) return error.DocumentRegionLimitExceeded;
        self.regions -= items.len;
        for (items) |region| {
            if (region != .object) return error.InvalidDocumentModelResult;
            try self.checkTables(present(field(region, "tables")));
            try self.checkRegions(present(field(region, "children")), depth + 1);
        }
    }
    fn checkTables(self: *Budget, value: ?V) error{ InvalidDocumentModelResult, DocumentRegionLimitExceeded }!void {
        const items = try array(value orelse return);
        if (items.len > self.tables) return error.DocumentRegionLimitExceeded;
        self.tables -= items.len;
        for (items) |table| {
            const cells = try array(field(table, "cells") orelse return error.InvalidDocumentModelResult);
            if (cells.len > self.cells) return error.DocumentRegionLimitExceeded;
            self.cells -= cells.len;
        }
    }
};

fn decodeRegions(a: A, value: ?V, parent_origin: c.Origin, model: []const u8) error{ OutOfMemory, InvalidDocumentModelResult }![]c.Region {
    const items = (value orelse return &.{}).array.items;
    const out = try a.alloc(c.Region, items.len);
    // Decode shallow fields once; children/tables share the preflight's single
    // page-wide allowances and strings/JSON fields borrow the parsed arena.
    const Shallow = struct {
        id: ?usize = null,
        bbox: ?[4]f64 = null,
        confidence: ?f64 = null,
        validated: bool = false,
        text_start: ?usize = null,
        text_end: ?usize = null,
    };
    for (items, out, 0..) |item, *region, index| {
        const decoded = try typed(Shallow, a, item);
        const effective_origin = try origin(a, item, parent_origin);
        region.* = .{
            .id = decoded.id orelse index,
            .text = try text(field(item, "text") orelse return error.InvalidDocumentModelResult),
            .bbox = decoded.bbox,
            .confidence = decoded.confidence,
            .label = try optionalText(field(item, "label")),
            .model = (try optionalText(field(item, "model"))) orelse model,
            .validated = decoded.validated,
            .text_start = decoded.text_start,
            .text_end = decoded.text_end,
            .fields = present(field(item, "fields")),
            .origin = effective_origin,
            .children = try decodeRegions(a, present(field(item, "children")), effective_origin, model),
            .tables = try decodeTables(a, present(field(item, "tables")), effective_origin),
        };
    }
    return out;
}
fn decodeTables(a: A, value: ?V, parent_origin: c.Origin) error{ OutOfMemory, InvalidDocumentModelResult }![]c.Table {
    const items = (value orelse return &.{}).array.items;
    const tables = try a.alloc(c.Table, items.len);
    const Shape = struct { id: usize, region_id: ?usize = null, projection: bool = false, rows: usize, columns: usize };
    const Cell = struct { row: usize, column: usize, row_span: usize = 1, column_span: usize = 1, bbox: ?[4]f64 = null };
    for (items, tables) |item, *table| {
        const shape = try typed(Shape, a, item);
        const raw_cells = field(item, "cells").?.array.items;
        const cells = try a.alloc(c.Cell, raw_cells.len);
        for (raw_cells, cells) |raw, *cell| {
            const decoded = try typed(Cell, a, raw);
            cell.* = .{ .row = decoded.row, .column = decoded.column, .row_span = decoded.row_span, .column_span = decoded.column_span, .bbox = decoded.bbox, .text = try text(field(raw, "text") orelse return error.InvalidDocumentModelResult) };
        }
        table.* = .{ .id = shape.id, .region_id = shape.region_id, .projection = shape.projection, .rows = shape.rows, .columns = shape.columns, .cells = cells, .origin = try origin(a, item, parent_origin) };
        try table.validate();
    }
    return tables;
}

test "document reader reports preserve nested provenance fields tables and nullable geometry" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const json =
        \\{"text":"caption","origin":"interpretation","status":"complete","fields":{"amount":9007199254740993.01},"regions":[{"id":42,"text":"caption","bbox":null,"origin":"native","children":[{"id":7,"text":"caption","bbox":[0,0,1,1],"origin":"transcription","validated":true,"fields":{"amount":9007199254740993.01},"tables":[{"id":9,"region_id":7,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"caption","bbox":[0,0,1,1]}]}]}]}],"tables":[{"id":3,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"caption","bbox":null}]}]}
    ;
    const report = try decode(work, .{ .text = "caption", .document_json = json }, .{ .name = "leaf", .model = "hosted", .operation = .read }, 4, 8192, .{ .matrix = .{ 2, 0, 0, 0, 3, 0, 0, 0, 1 } }, .{ 10, 20 });
    try std.testing.expectEqual(c.Origin.interpretation, report.origin);
    try std.testing.expectEqual(c.Origin.interpretation, report.regions[0].origin);
    try std.testing.expectEqual(@as(usize, 42), report.regions[0].id);
    try std.testing.expect(report.regions[0].bbox == null);
    const child = report.regions[0].children[0];
    try std.testing.expectEqual(c.Origin.interpretation, child.origin);
    try std.testing.expect(child.validated);
    try std.testing.expectEqual(@as(usize, 7), child.id);
    try std.testing.expectEqual([4]f64{ 20, 60, 22, 63 }, child.bbox.?);
    try std.testing.expectEqual([4]f64{ 20, 60, 22, 63 }, child.tables[0].cells[0].bbox.?);
    try std.testing.expectEqual(c.Origin.interpretation, child.tables[0].origin);
    try std.testing.expectEqual(c.Origin.interpretation, report.tables[0].origin);
    try std.testing.expect(report.tables[0].cells[0].bbox == null);
    try std.testing.expectEqualStrings("9007199254740993.01", report.fields.?.object.get("amount").?.number_string);
    try std.testing.expectEqualStrings("9007199254740993.01", child.fields.?.object.get("amount").?.number_string);
    // Evidence policy must still reject interpreted text as a literal source.
    var item = try parse(work,
        \\{"structures":{"record":[{"value":{"value":"caption","start":0,"end":7}}]}}
    );
    const metadata = try parse(work, try std.json.Stringify.valueAlloc(work, .{ .source_id = "receipt", .segments = .{.{ .number = 1, .start = 0, .end = 7, .origin = report.origin, .regions = report.regions }} }, .{}));
    try std.testing.expect(!try @import("evidence.zig").attach(work, &item, "caption", metadata, true, false));
}

test "document reader reports keep nonliteral provenance through binding overrides and nested transforms" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json =
        \\{"text":"caption","origin":"derived","regions":[{"id":42,"text":"caption","origin":"estimated","children":[{"id":7,"text":"caption","origin":"native"}]}]}
    ;
    const report = try decode(a, .{ .text = "caption", .document_json = json }, .{ .name = "leaf", .model = "hosted", .operation = .read, .output_origin = .interpretation }, 4, 8192, .{}, .{ 0, 0 });
    try std.testing.expectEqual(c.Origin.derived, report.origin);
    try std.testing.expectEqual(c.Origin.estimated, report.regions[0].origin);
    try std.testing.expectEqual(c.Origin.estimated, report.regions[0].children[0].origin);
    try ir.transformRegions(@constCast(report.regions), .{}, .{ 0, 0 }, true);
    try std.testing.expectEqual(c.Origin.estimated, report.regions[0].children[0].origin);
}

test "document legacy reader reports accept missing geometry and preserve completion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const binding = c.Binding{ .name = "leaf", .model = "legacy", .operation = .read };
    const report = try decode(a, .{ .text = "caption", .completion = .length, .regions_json = "[{\"text\":\"caption\",\"bbox\":null},{\"text\":\"caption\"}]" }, binding, 4, 8192, .{}, .{ 0, 0 });
    try std.testing.expect(report.truncated);
    try std.testing.expectEqual(c.Origin.transcription, report.origin);
    try std.testing.expectEqual(@as(usize, 1), report.regions[1].id);
    try std.testing.expect(report.regions[0].bbox == null and report.regions[1].bbox == null);
    const incomplete = try decode(a, .{ .text = "caption", .completion = .unresolved, .document_json = "{\"text\":\"caption\",\"status\":\"complete\"}" }, binding, 4, 8192, .{}, .{ 0, 0 });
    try std.testing.expectEqual(c.Status.unresolved, incomplete.status.?);
    try std.testing.expectError(error.InvalidDocumentRegion, decode(a, .{ .text = "caption", .regions_json = "[{\"text\":\"caption\",\"bbox\":[0,0,0,1]}]" }, binding, 4, 8192, .{}, .{ 0, 0 }));
}

test "document reader reports bound structures before typed allocation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try parse(a,
        \\{"regions":[{"text":"x","tables":[{"cells":[{}]}],"children":[{"text":"x","tables":[{"cells":[{}]}]}]}],"tables":[{"cells":[{}]}]}
    );
    var regions = Budget{ .regions = 1, .tables = 8, .cells = 8 };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, regions.checkRegions(field(root, "regions"), 0));
    var tables = Budget{ .regions = 8, .tables = 1, .cells = 8 };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, tables.checkRegions(field(root, "regions"), 0));
    var cells = Budget{ .regions = 8, .tables = 8, .cells = 1 };
    try std.testing.expectError(error.DocumentRegionLimitExceeded, cells.checkRegions(field(root, "regions"), 0));
    var shared = Budget{ .regions = 2, .tables = 2, .cells = 2 };
    try shared.checkRegions(field(root, "regions"), 0);
    try std.testing.expectError(error.DocumentRegionLimitExceeded, shared.checkTables(field(root, "tables")));
    const binding = c.Binding{ .name = "leaf", .model = "legacy", .operation = .read };
    try std.testing.expectError(error.DocumentOutputTooLarge, decode(a, .{ .text = "caption", .document_json = "{\"text\":\"caption\"}" }, binding, 4, 1, .{}, .{ 0, 0 }));
}

test "document reader typed structures borrow parsed text and fields" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const value = try parse(a,
        \\[{"id":1,"text":"caption","label":"table","model":"leaf","fields":{"amount":9007199254740993.01},"tables":[{"id":2,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"caption"}]}]}]
    );
    const regions = try decodeRegions(a, value, .transcription, "default");
    const raw = value.array.items[0];
    try std.testing.expect(regions[0].text.ptr == raw.object.get("text").?.string.ptr);
    try std.testing.expect(regions[0].label.?.ptr == raw.object.get("label").?.string.ptr);
    try std.testing.expect(regions[0].fields.?.object.values().ptr == raw.object.get("fields").?.object.values().ptr);
    const cell_text = raw.object.get("tables").?.array.items[0].object.get("cells").?.array.items[0].object.get("text").?.string;
    try std.testing.expect(regions[0].tables[0].cells[0].text.ptr == cell_text.ptr);
}

test "document reader allocation failures release partial structured decoding" {
    const Runner = struct {
        fn run(backing: A) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            _ = try decode(arena.allocator(), .{ .text = "caption", .document_json =
                \\{"text":"caption","origin":"interpretation","regions":[{"id":7,"text":"caption","bbox":null,"children":[{"id":9,"text":"caption"}]}],"tables":[{"id":2,"rows":1,"columns":1,"cells":[{"row":0,"column":0,"text":"caption"}]}]}
            }, .{ .name = "leaf", .model = "hosted", .operation = .read }, 4, 8192, .{}, .{ 0, 0 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
