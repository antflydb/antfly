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

//! Streaming document structure comparison. Advisory model metadata is
//! excluded; exact comparison retains geometry and identity, while corroboration
//! compares content and table topology independently of generated IDs/boxes.
const std = @import("std");
const c = @import("contract.zig");
const executor = @import("executor.zig");
const ir = @import("representation.zig");
const V = std.json.Value;
const A = std.mem.Allocator;
const Mode = enum { exact, evidence };

pub fn sameAccepted(page: c.Page, region: ?c.Region, candidate: executor.Candidate) !bool {
    const before = if (region) |r| try signature(r.children, r.tables, r.fields, .exact, null) else try signature(page.regions, page.tables, page.fields, .exact, null);
    return std.mem.eql(u8, &before, &try signature(candidate.regions, candidate.tables, candidate.fields, .exact, null));
}
pub fn sameEvidence(backing: A, left: executor.Candidate, right: executor.Candidate) !bool {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var l = try Ownership.init(a, left.regions, left.tables);
    var r = try Ownership.init(a, right.regions, right.tables);
    return std.mem.eql(u8, &try signature(left.regions, left.tables, left.fields, .evidence, &l), &try signature(right.regions, right.tables, right.fields, .evidence, &r));
}
fn signature(regions: []const c.Region, tables: []const c.Table, fields: ?V, mode: Mode, ownership: ?*Ownership) ![32]u8 {
    var d = Digest{};
    try d.regions(regions, mode, 0, ownership, 0);
    try d.tables(tables, mode, ownership, 0);
    try d.value(fields orelse .null, 0);
    return d.finish();
}

// Reader-local IDs are scoped to sibling regions. Canonical preorder positions
// identify the same tree paths in independently numbered but matching trees.
const Ownership = struct {
    const Key = struct { scope: usize, id: usize };
    const Node = struct { id: usize };
    ids: std.AutoHashMapUnmanaged(Key, usize) = .empty,
    nodes: []Node = &.{},
    next: usize = 1,
    fn init(a: A, regions: []const c.Region, tables: []const c.Table) !Ownership {
        var count: usize = 0;
        var referenced = false;
        try inspect(regions, tables, 0, &count, &referenced);
        var self = Ownership{};
        // Text-only and implicitly owned reports keep the allocation-free path.
        if (!referenced) return self;
        self.nodes = try a.alloc(Node, count + 1);
        try self.ids.ensureTotalCapacity(a, @intCast(count));
        var position: usize = 1;
        try self.index(a, regions, 0, &position);
        return self;
    }
    fn inspect(regions: []const c.Region, tables: []const c.Table, depth: usize, count: *usize, referenced: *bool) error{InvalidDocumentModelResult}!void {
        for (tables) |table| if (table.region_id != null) {
            referenced.* = true;
        };
        if (regions.len == 0) return;
        if (depth > 16 or regions.len > 65536 -| count.*) return error.InvalidDocumentModelResult;
        count.* += regions.len;
        for (regions) |region| try inspect(region.children, region.tables, depth + 1, count, referenced);
    }
    fn index(self: *Ownership, a: A, regions: []const c.Region, scope: usize, position: *usize) error{ OutOfMemory, InvalidDocumentModelResult }!void {
        for (regions) |region| {
            const node = position.*;
            position.* += 1;
            self.nodes[node] = .{ .id = region.id };
            const entry = try self.ids.getOrPut(a, .{ .scope = scope, .id = region.id });
            if (entry.found_existing) return error.InvalidDocumentModelResult;
            entry.value_ptr.* = node;
            try self.index(a, region.children, node, position);
        }
    }
    fn resolve(self: *const Ownership, scope: usize, owner: ?usize) error{InvalidDocumentModelResult}!usize {
        const id = owner orelse return scope;
        // Explicit references resolve in the immediate child scope first.
        // This preserves crop reports when composition wraps their regions
        // beneath a parent that happens to reuse one of those local IDs.
        if (self.ids.get(.{ .scope = scope, .id = id })) |child| return child;
        if (scope > 0 and self.nodes[scope].id == id) return scope;
        return error.InvalidDocumentModelResult;
    }
};

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
    fn regions(self: *Digest, items: []const c.Region, mode: Mode, depth: usize, ownership: ?*Ownership, scope: usize) error{InvalidDocumentModelResult}!void {
        if (depth > 16) return error.InvalidDocumentModelResult;
        self.scalar(items.len);
        for (items) |region| {
            const position = if (ownership) |owners| blk: {
                const node = owners.next;
                owners.next += 1;
                break :blk node;
            } else scope;
            if (mode == .exact) {
                self.scalar(region.id);
                self.scalar(region.bbox);
            }
            self.scalar(region.text);
            self.scalar(region.label);
            self.provenance(region.origin, mode);
            try self.value(region.fields orelse .null, 0);
            try self.tables(region.tables, mode, ownership, position);
            try self.regions(region.children, mode, depth + 1, ownership, position);
        }
    }
    fn tables(self: *Digest, items: []const c.Table, mode: Mode, ownership: ?*Ownership, scope: usize) !void {
        self.scalar(items.len);
        var sum: [32]u8 = @splat(0);
        for (items) |table| {
            var d = Digest{};
            if (mode == .exact) d.scalar(table.id);
            if (mode == .exact) d.scalar(table.region_id) else d.scalar(try ownership.?.resolve(scope, table.region_id));
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
    try std.testing.expect(try sameEvidence(std.testing.allocator, candidate, other));
    const revised = [_]c.Table{.{ .id = 9, .rows = 1, .columns = 1, .cells = &cells }};
    other.tables = &revised;
    try std.testing.expect(try sameEvidence(std.testing.allocator, candidate, other));
    var wrong = cells;
    wrong[0].text = "invented";
    const changed = [_]c.Table{.{ .id = 9, .rows = 1, .columns = 1, .cells = &wrong }};
    other.tables = &changed;
    try std.testing.expect(!try sameEvidence(std.testing.allocator, candidate, other));
}

test "document structure comparison preserves exact numeric fields and ignores key order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try std.json.parseFromSliceLeaky(V, a, "{\"a\":9007199254740993.01,\"b\":true}", .{ .parse_numbers = false });
    const right = try std.json.parseFromSliceLeaky(V, a, "{\"b\":true,\"a\":9007199254740993.01}", .{ .parse_numbers = false });
    const wrong = try std.json.parseFromSliceLeaky(V, a, "{\"b\":true,\"a\":9007199254740993.02}", .{ .parse_numbers = false });
    try std.testing.expect(try sameEvidence(std.testing.allocator, .{ .text = "", .fields = left }, .{ .text = "", .fields = right }));
    try std.testing.expect(!try sameEvidence(std.testing.allocator, .{ .text = "", .fields = left }, .{ .text = "", .fields = wrong }));
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
    try std.testing.expect(try sameEvidence(std.testing.allocator, left, right));
    try std.testing.expect(!try sameAccepted(page, null, right));
    reordered[0].column_span = 2;
    try std.testing.expect(!try sameEvidence(std.testing.allocator, left, right));
}

test "document numeric signatures normalize equivalent decimals without rounding or exponent expansion" {
    const precise = executor.Candidate{ .text = "", .fields = .{ .number_string = "9007199254740993.0100" } };
    const equivalent = executor.Candidate{ .text = "", .fields = .{ .number_string = "900719925474099301e-2" } };
    try std.testing.expect(try sameEvidence(std.testing.allocator, precise, equivalent));
    try std.testing.expect(try sameEvidence(std.testing.allocator, .{ .text = "", .fields = .{ .integer = 1 } }, .{ .text = "", .fields = .{ .float = 1.0 } }));
    try std.testing.expect(try sameEvidence(std.testing.allocator, .{ .text = "", .fields = .{ .number_string = "-0.0e1000000000" } }, .{ .text = "", .fields = .{ .integer = 0 } }));
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(std.testing.allocator, precise, .{ .text = "", .fields = .{ .number_string = "1e99999999999999999999999" } }));
}

test "document evidence ownership uses canonical scoped positions instead of reader-local IDs" {
    const a = std.testing.allocator;
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
    const left_regions = [_]c.Region{ .{ .id = 7, .text = "caption" }, .{ .id = 8, .text = "other" } };
    const right_regions = [_]c.Region{ .{ .id = 42, .text = "caption" }, .{ .id = 43, .text = "other" } };
    const left_tables = [_]c.Table{.{ .id = 1, .region_id = 7, .rows = 1, .columns = 1, .cells = &cells }};
    var right_tables = [_]c.Table{.{ .id = 9, .region_id = 42, .rows = 1, .columns = 1, .cells = &cells }};
    const left = executor.Candidate{ .text = "caption other", .regions = &left_regions, .tables = &left_tables };
    const right = executor.Candidate{ .text = "caption other", .regions = &right_regions, .tables = &right_tables };
    try std.testing.expect(try sameEvidence(a, left, right));
    right_tables[0].region_id = 43;
    try std.testing.expect(!try sameEvidence(a, left, right));
    right_tables[0].region_id = 99;
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, left, right));
    const duplicates = [_]c.Region{ left_regions[0], left_regions[0] };
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, left, .{ .text = "caption", .regions = &duplicates, .tables = &left_tables }));
    // Numeric IDs may repeat across different sibling scopes. Tables attached
    // to a region can be implicitly or explicitly owned by that same region.
    const implicit = [_]c.Table{.{ .id = 3, .rows = 1, .columns = 1, .cells = &cells }};
    const explicit = [_]c.Table{.{ .id = 4, .region_id = 42, .rows = 1, .columns = 1, .cells = &cells }};
    const nested_left = [_]c.Region{.{ .id = 7, .text = "caption", .tables = &implicit }};
    const nested_right = [_]c.Region{.{ .id = 42, .text = "caption", .tables = &explicit }};
    try std.testing.expect(try sameEvidence(a, .{ .text = "caption", .regions = &nested_left }, .{ .text = "caption", .regions = &nested_right }));
    const child_left = [_]c.Region{.{ .id = 7, .text = "caption" }};
    const child_right = [_]c.Region{.{ .id = 42, .text = "caption" }};
    const roots_left = [_]c.Region{ .{ .id = 1, .text = "caption", .children = &child_left, .tables = &left_tables }, .{ .id = 2, .text = "caption", .children = &child_left, .tables = &left_tables } };
    right_tables[0].region_id = 42;
    const roots_right = [_]c.Region{ .{ .id = 3, .text = "caption", .children = &child_right, .tables = &right_tables }, .{ .id = 4, .text = "caption", .children = &child_right, .tables = &right_tables } };
    try std.testing.expect(try sameEvidence(a, .{ .text = "captioncaption", .regions = &roots_left }, .{ .text = "captioncaption", .regions = &roots_right }));
    const wrapped_left = [_]c.Region{.{ .id = 7, .text = "caption", .children = &child_left, .tables = &left_tables }};
    const wrapped_right = [_]c.Region{.{ .id = 8, .text = "caption", .children = &child_right, .tables = &right_tables }};
    try std.testing.expect(try sameEvidence(a, .{ .text = "caption", .regions = &wrapped_left }, .{ .text = "caption", .regions = &wrapped_right }));
}

test "document owner indexing releases scratch after every allocation failure and skips unreferenced trees" {
    const Runner = struct {
        fn run(backing: A) !void {
            const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
            const regions = [_]c.Region{.{ .id = 42, .text = "caption" }};
            const tables = [_]c.Table{.{ .id = 1, .region_id = 42, .rows = 1, .columns = 1, .cells = &cells }};
            const value = executor.Candidate{ .text = "caption", .regions = &regions, .tables = &tables };
            try std.testing.expect(try sameEvidence(backing, value, value));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
    const regions = [_]c.Region{.{ .id = 42, .text = "caption" }};
    const value = executor.Candidate{ .text = "caption", .regions = &regions };
    try std.testing.expect(try sameEvidence(std.testing.failing_allocator, value, value));
}
