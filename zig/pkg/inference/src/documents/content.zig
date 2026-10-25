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

//! Allocation-free document structure comparison. Advisory model metadata is
//! excluded; exact comparison retains geometry and identity, while corroboration
//! compares content and table topology independently of generated IDs/boxes.
const std = @import("std");
const c = @import("contract.zig");
const executor = @import("executor.zig");
const ir = @import("representation.zig");
const V = std.json.Value;
const Mode = enum { exact, evidence };

pub fn sameAccepted(page: c.Page, region: ?c.Region, candidate: executor.Candidate) !bool {
    const before = if (region) |r| try signature(r.children, r.tables, r.fields, .exact) else try signature(page.regions, page.tables, page.fields, .exact);
    return std.mem.eql(u8, &before, &try signature(candidate.regions, candidate.tables, candidate.fields, .exact));
}
pub fn sameEvidence(left: executor.Candidate, right: executor.Candidate) !bool {
    return std.mem.eql(u8, &try signature(left.regions, left.tables, left.fields, .evidence), &try signature(right.regions, right.tables, right.fields, .evidence));
}
fn signature(regions: []const c.Region, tables: []const c.Table, fields: ?V, mode: Mode) ![32]u8 {
    var d = Digest{};
    try d.regions(regions, mode, 0);
    try d.tables(tables, mode);
    try d.value(fields orelse .null, 0);
    return d.finish();
}

const Digest = struct {
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    writer: std.Io.Writer = .{ .buffer = &.{}, .vtable = &.{ .drain = drain } },
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var count: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            self.hash.update(bytes);
            count += bytes.len;
        }
        for (0..splat) |_| {
            self.hash.update(data[data.len - 1]);
            count += data[data.len - 1].len;
        }
        return count;
    }
    fn scalar(self: *Digest, item: anytype) void {
        std.json.Stringify.value(item, .{}, &self.writer) catch unreachable;
        self.hash.update(";");
    }
    fn finish(self: *Digest) [32]u8 {
        var out: [32]u8 = undefined;
        self.hash.final(&out);
        return out;
    }
    fn provenance(self: *Digest, origin: c.Origin, mode: Mode) void {
        if (mode == .exact) self.scalar(origin) else self.scalar(ir.isLiteral(origin));
    }
    fn regions(self: *Digest, items: []const c.Region, mode: Mode, depth: usize) error{InvalidDocumentModelResult}!void {
        if (depth > 16) return error.InvalidDocumentModelResult;
        self.scalar(items.len);
        for (items) |region| {
            if (mode == .exact) {
                self.scalar(region.id);
                self.scalar(region.bbox);
            }
            self.scalar(region.text);
            self.scalar(region.label);
            self.provenance(region.origin, mode);
            try self.value(region.fields orelse .null, 0);
            try self.tables(region.tables, mode);
            try self.regions(region.children, mode, depth + 1);
        }
    }
    fn tables(self: *Digest, items: []const c.Table, mode: Mode) !void {
        self.scalar(items.len);
        var sum: [32]u8 = @splat(0);
        for (items) |table| {
            var d = Digest{};
            if (mode == .exact) d.scalar(table.id);
            // Explicit ownership must agree even when table IDs differ.
            d.scalar(table.region_id);
            d.scalar(table.rows);
            d.scalar(table.columns);
            d.provenance(table.origin, mode);
            d.scalar(table.cells.len);
            var cells: [32]u8 = @splat(0);
            for (table.cells) |cell| {
                var entry = Digest{};
                entry.scalar(.{ cell.row, cell.column, cell.row_span, cell.column_span, cell.text });
                if (mode == .exact) entry.scalar(cell.bbox);
                add(&cells, entry.finish());
            }
            d.hash.update(&cells);
            add(&sum, d.finish());
        }
        self.hash.update(&sum);
    }
    fn value(self: *Digest, item: V, depth: usize) error{InvalidDocumentModelResult}!void {
        if (depth > 64) return error.InvalidDocumentModelResult;
        switch (item) {
            .object => |obj| {
                self.hash.update("object");
                self.scalar(obj.count());
                var sum: [32]u8 = @splat(0);
                for (obj.keys(), obj.values()) |key, child| {
                    var entry = Digest{};
                    entry.scalar(key);
                    try entry.value(child, depth + 1);
                    add(&sum, entry.finish());
                }
                self.hash.update(&sum);
            },
            .array => |array| {
                self.hash.update("array");
                self.scalar(array.items.len);
                for (array.items) |child| try self.value(child, depth + 1);
            },
            .number_string => |raw| try self.number(raw),
            .integer, .float => {
                if (item == .float and !std.math.isFinite(item.float)) return error.InvalidDocumentModelResult;
                var buffer: [128]u8 = undefined;
                var writer = std.Io.Writer.fixed(&buffer);
                std.json.Stringify.value(item, .{}, &writer) catch unreachable;
                try self.number(buffer[0..writer.end]);
            },
            else => self.scalar(item),
        }
    }
    fn number(self: *Digest, raw: []const u8) error{InvalidDocumentModelResult}!void {
        if (raw.len == 0) return error.InvalidDocumentModelResult;
        const negative = raw[0] == '-';
        const begin = @intFromBool(negative);
        var i: usize = begin;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == begin or (i - begin > 1 and raw[begin] == '0')) return error.InvalidDocumentModelResult;
        var fraction: usize = 0;
        if (i < raw.len and raw[i] == '.') {
            i += 1;
            const start = i;
            while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
            fraction = i - start;
            if (fraction == 0) return error.InvalidDocumentModelResult;
        }
        const end = i;
        var exponent: i64 = 0;
        if (i < raw.len and (raw[i] == 'e' or raw[i] == 'E')) {
            i += 1;
            const start = i;
            if (i < raw.len and (raw[i] == '+' or raw[i] == '-')) i += 1;
            const digits = i;
            while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
            if (i == digits) return error.InvalidDocumentModelResult;
            exponent = std.fmt.parseInt(i64, raw[start..i], 10) catch return error.InvalidDocumentModelResult;
        }
        if (i != raw.len) return error.InvalidDocumentModelResult;
        self.hash.update("number:");
        var first: usize = begin;
        while (first < end and (raw[first] == '0' or raw[first] == '.')) : (first += 1) {}
        if (first == end) {
            self.scalar("zero");
            return;
        }
        var last = end;
        var trailing: usize = 0;
        while (raw[last - 1] == '0' or raw[last - 1] == '.') {
            if (raw[last - 1] == '0') trailing += 1;
            last -= 1;
        }
        exponent = std.math.sub(i64, exponent, std.math.cast(i64, fraction) orelse return error.InvalidDocumentModelResult) catch return error.InvalidDocumentModelResult;
        exponent = std.math.add(i64, exponent, std.math.cast(i64, trailing) orelse return error.InvalidDocumentModelResult) catch return error.InvalidDocumentModelResult;
        self.scalar(negative);
        self.scalar(exponent);
        for (raw[first..last]) |ch| if (ch != '.') self.hash.update(&.{ch});
        self.hash.update(";");
    }
};
// Commutative 256-bit addition plus cardinality makes keyed fields and table
// cells independent of enumeration order without sorting or allocating JSON.
fn add(sum: *[32]u8, entry: [32]u8) void {
    var carry: u16 = 0;
    var i: usize = sum.len;
    while (i > 0) {
        i -= 1;
        const n = @as(u16, sum[i]) + entry[i] + carry;
        sum[i] = @truncate(n);
        carry = n >> 8;
    }
}

test "document structure comparison detects tables without treating quality bookkeeping as content" {
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption", .bbox = .{ 0, 0, 1, 1 } }};
    const tables = [_]c.Table{.{ .id = 7, .rows = 1, .columns = 1, .cells = &cells }};
    const page = c.Page{ .source_id = "receipt", .number = 1, .kind = .image, .text = "caption" };
    const candidate = executor.Candidate{ .text = "caption", .tables = &tables };
    try std.testing.expect(!try sameAccepted(page, null, candidate));
    var other = candidate;
    other.model = "different model";
    other.confidence = 0.99;
    try std.testing.expect(try sameEvidence(candidate, other));
    const revised = [_]c.Table{.{ .id = 9, .rows = 1, .columns = 1, .cells = &cells }};
    other.tables = &revised;
    try std.testing.expect(try sameEvidence(candidate, other));
    var wrong = cells;
    wrong[0].text = "invented";
    const changed = [_]c.Table{.{ .id = 9, .rows = 1, .columns = 1, .cells = &wrong }};
    other.tables = &changed;
    try std.testing.expect(!try sameEvidence(candidate, other));
}

test "document structure comparison preserves exact numeric fields and ignores key order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try std.json.parseFromSliceLeaky(V, a, "{\"a\":9007199254740993.01,\"b\":true}", .{ .parse_numbers = false });
    const right = try std.json.parseFromSliceLeaky(V, a, "{\"b\":true,\"a\":9007199254740993.01}", .{ .parse_numbers = false });
    const wrong = try std.json.parseFromSliceLeaky(V, a, "{\"b\":true,\"a\":9007199254740993.02}", .{ .parse_numbers = false });
    try std.testing.expect(try sameEvidence(.{ .text = "", .fields = left }, .{ .text = "", .fields = right }));
    try std.testing.expect(!try sameEvidence(.{ .text = "", .fields = left }, .{ .text = "", .fields = wrong }));
}

test "document evidence signatures compare table topology independently of cell order and geometry" {
    const cells = [_]c.Cell{ .{ .row = 0, .column = 0, .text = "A", .bbox = .{ 0, 0, 1, 1 } }, .{ .row = 0, .column = 1, .text = "B" } };
    var reordered = [_]c.Cell{ cells[1], cells[0] };
    reordered[1].bbox = .{ 1, 1, 2, 2 };
    const before = [_]c.Table{.{ .id = 1, .rows = 1, .columns = 2, .cells = &cells }};
    const after = [_]c.Table{.{ .id = 9, .rows = 1, .columns = 2, .cells = &reordered }};
    const page = c.Page{ .source_id = "receipt", .number = 1, .kind = .image, .tables = &before };
    const left = executor.Candidate{ .text = "A B", .tables = &before };
    const right = executor.Candidate{ .text = "A B", .tables = &after };
    try std.testing.expect(try sameEvidence(left, right));
    try std.testing.expect(!try sameAccepted(page, null, right));
    reordered[0].column_span = 2;
    try std.testing.expect(!try sameEvidence(left, right));
}

test "document numeric signatures normalize equivalent decimals without rounding or exponent expansion" {
    const precise = executor.Candidate{ .text = "", .fields = .{ .number_string = "9007199254740993.0100" } };
    const equivalent = executor.Candidate{ .text = "", .fields = .{ .number_string = "900719925474099301e-2" } };
    try std.testing.expect(try sameEvidence(precise, equivalent));
    try std.testing.expect(try sameEvidence(.{ .text = "", .fields = .{ .integer = 1 } }, .{ .text = "", .fields = .{ .float = 1.0 } }));
    try std.testing.expect(try sameEvidence(.{ .text = "", .fields = .{ .number_string = "-0.0e1000000000" } }, .{ .text = "", .fields = .{ .integer = 0 } }));
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(precise, .{ .text = "", .fields = .{ .number_string = "1e99999999999999999999999" } }));
}
