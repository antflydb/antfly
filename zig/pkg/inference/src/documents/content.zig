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

/// Identity of a physical table view, excluding its placement and alias marker.
pub fn tableViewDigest(supplied: c.Table) [32]u8 {
    var table = supplied;
    table.region_id = null;
    var digest = Digest{};
    digest.table(table, .exact);
    return digest.finish();
}

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
    const lt = try logicalTables(a, left.regions, left.tables, &l);
    const rt = try logicalTables(a, right.regions, right.tables, &r);
    return std.mem.eql(u8, &try signature(left.regions, left.tables, left.fields, .evidence, lt), &try signature(right.regions, right.tables, right.fields, .evidence, rt));
}
fn signature(regions: []const c.Region, tables: []const c.Table, fields: ?V, mode: Mode, logical_tables: ?[32]u8) ![32]u8 {
    var d = Digest{};
    try d.regions(regions, mode, 0);
    if (mode == .exact) d.tables(tables, mode) else d.hash.update(&logical_tables.?);
    try d.value(fields orelse .null, 0);
    return d.finish();
}

// Reader-local IDs are scoped to sibling regions. Canonical preorder positions
// identify the same tree paths in independently numbered but matching trees.
const Ownership = struct {
    const Key = struct { scope: usize, id: usize };
    const Node = struct { id: usize, parent: usize };
    ids: std.AutoHashMapUnmanaged(Key, usize) = .empty,
    nodes: []Node = &.{},
    table_count: usize = 0,
    fn init(a: A, regions: []const c.Region, tables: []const c.Table) !Ownership {
        return initIndexed(a, regions, tables, false);
    }
    fn initIndexed(a: A, regions: []const c.Region, tables: []const c.Table, force: bool) !Ownership {
        var count: usize = 0;
        var referenced = force;
        var table_count: usize = 0;
        try inspect(regions, tables, 0, &count, &referenced, &table_count);
        var self = Ownership{ .table_count = table_count };
        // Text-only and implicitly owned reports keep the allocation-free path.
        if (!referenced) return self;
        self.nodes = try a.alloc(Node, count + 1);
        try self.ids.ensureTotalCapacity(a, @intCast(count));
        var position: usize = 1;
        try self.index(a, regions, 0, &position);
        return self;
    }
    fn inspect(regions: []const c.Region, tables: []const c.Table, depth: usize, count: *usize, referenced: *bool, table_count: *usize) error{InvalidDocumentModelResult}!void {
        if (tables.len > 65536 -| table_count.*) return error.InvalidDocumentModelResult;
        table_count.* += tables.len;
        for (tables) |table| {
            if (table.projection and table.region_id == null) return error.InvalidDocumentModelResult;
            if (table.region_id != null) referenced.* = true;
        }
        if (regions.len == 0) return;
        if (depth > 16 or regions.len > 65536 -| count.*) return error.InvalidDocumentModelResult;
        count.* += regions.len;
        for (regions) |region| try inspect(region.children, region.tables, depth + 1, count, referenced, table_count);
    }
    fn index(self: *Ownership, a: A, regions: []const c.Region, scope: usize, position: *usize) error{ OutOfMemory, InvalidDocumentModelResult }!void {
        for (regions) |region| {
            const node = position.*;
            position.* += 1;
            self.nodes[node] = .{ .id = region.id, .parent = scope };
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

// Physical table views are indexed once by their containing scope, local ID
// and payload. Declared projections reserve their aliases before compatibility
// matching, so duplicates never disappear merely because their content agrees.
const TableSet = struct {
    const Key = struct { scope: usize, id: usize, payload: [32]u8 };
    const Group = struct { count: usize = 0, self_count: usize = 0, used: usize = 0, used_self: usize = 0 };
    const Entry = struct { scope: usize, owner: usize, id: usize, payload: [32]u8, projection: bool, table: *const c.Table, index: usize };
    owners: *const Ownership,
    groups: std.AutoHashMapUnmanaged(Key, Group) = .empty,
    entries: []Entry = &.{},
    length: usize = 0,
    selected: []Entry = &.{},
    count: usize = 0,
    sum: [32]u8 = @splat(0),
    fn collectRegions(self: *TableSet, a: A, regions: []const c.Region, position: *usize) !void {
        for (regions) |region| {
            const node = position.*;
            position.* += 1;
            try self.collect(a, region.tables, node);
            try self.collectRegions(a, region.children, position);
        }
    }
    fn collect(self: *TableSet, a: A, items: []const c.Table, scope: usize) !void {
        for (items, 0..) |*item, index| {
            var payload = Digest{};
            payload.table(item.*, .evidence);
            const owner = try self.owners.resolve(scope, item.region_id);
            const entry = Entry{ .scope = scope, .owner = owner, .id = item.id, .payload = payload.finish(), .projection = item.projection, .table = item, .index = index };
            if (item.projection and owner == scope) return error.InvalidDocumentModelResult;
            if (self.entries.len == 0) {
                self.include(entry);
                continue;
            }
            self.entries[self.length] = entry;
            self.length += 1;
            const group = try self.groups.getOrPut(a, .{ .scope = scope, .id = item.id, .payload = entry.payload });
            if (!group.found_existing) group.value_ptr.* = .{};
            group.value_ptr.count += 1;
            if (owner == scope) group.value_ptr.self_count += 1;
        }
    }
    fn include(self: *TableSet, entry: Entry) void {
        if (self.selected.len > 0) self.selected[self.count] = entry;
        var d = Digest{};
        d.scalar(entry.owner);
        d.hash.update(&entry.payload);
        add(&self.sum, d.finish());
        self.count += 1;
    }
    fn alias(self: *TableSet, entry: Entry) bool {
        if (entry.owner == entry.scope) return false;
        const group = self.groups.getPtr(.{ .scope = entry.owner, .id = entry.id, .payload = entry.payload }) orelse return false;
        if (group.used >= group.count) return false;
        // Without an explicit marker, only infer a projection when its owner
        // also agrees with the authoritative record's containing region.
        if (!entry.projection) {
            if (group.used_self >= group.self_count) return false;
            group.used_self += 1;
        }
        group.used += 1;
        return true;
    }
    fn finish(self: *TableSet) ![32]u8 {
        for (self.entries[0..self.length]) |entry| if (entry.projection) {
            if (!self.alias(entry)) return error.InvalidDocumentModelResult;
        };
        for (self.entries[0..self.length]) |entry| if (!entry.projection and !self.alias(entry)) {
            self.include(entry);
        };
        var d = Digest{};
        d.scalar(self.count);
        d.hash.update(&self.sum);
        return d.finish();
    }
};
fn logicalTables(a: A, regions: []const c.Region, tables: []const c.Table, owners: *const Ownership) ![32]u8 {
    var set = TableSet{ .owners = owners };
    if (owners.nodes.len > 0 and owners.table_count > 0) {
        set.entries = try a.alloc(TableSet.Entry, owners.table_count);
        try set.groups.ensureTotalCapacity(a, @intCast(owners.table_count));
    }
    var position: usize = 1;
    try set.collectRegions(a, regions, &position);
    try set.collect(a, tables, 0);
    return set.finish();
}

/// One authoritative physical record per logical table. The borrowed table
/// retains its own provenance; owner paths qualify model-local IDs. Scratch
/// belongs to the caller's arena, as with the document representation itself.
pub const TableReference = struct { table: *const c.Table, region_path: []const usize, table_scope: []const usize, table_index: usize };
pub fn authoritativeTables(a: A, regions: []const c.Region, tables: []const c.Table) ![]TableReference {
    var owners = try Ownership.initIndexed(a, regions, tables, true);
    var set = TableSet{ .owners = &owners };
    if (owners.table_count == 0) return &.{};
    set.entries = try a.alloc(TableSet.Entry, owners.table_count);
    set.selected = try a.alloc(TableSet.Entry, owners.table_count);
    try set.groups.ensureTotalCapacity(a, @intCast(owners.table_count));
    var position: usize = 1;
    try set.collectRegions(a, regions, &position);
    try set.collect(a, tables, 0);
    _ = try set.finish();
    const result = try a.alloc(TableReference, set.count);
    for (set.selected[0..set.count], result) |entry, *reference| {
        reference.* = .{ .table = entry.table, .region_path = try regionPath(a, &owners, entry.owner), .table_scope = try regionPath(a, &owners, entry.scope), .table_index = entry.index };
    }
    return result;
}

fn regionPath(a: A, owners: *const Ownership, selected: usize) ![]usize {
    var reverse: [17]usize = undefined;
    var length: usize = 0;
    var node = selected;
    while (node != 0) {
        if (length >= reverse.len) return error.InvalidDocumentModelResult;
        reverse[length] = owners.nodes[node].id;
        length += 1;
        node = owners.nodes[node].parent;
    }
    const path = try a.alloc(usize, length);
    for (path, 0..) |*id, index| id.* = reverse[length - index - 1];
    return path;
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
            if (mode == .exact) self.tables(region.tables, mode);
            try self.regions(region.children, mode, depth + 1);
        }
    }
    fn table(self: *Digest, item: c.Table, mode: Mode) void {
        if (mode == .exact) {
            self.scalar(item.id);
            self.scalar(item.region_id);
        }
        self.scalar(item.rows);
        self.scalar(item.columns);
        self.provenance(item.origin, mode);
        self.scalar(item.cells.len);
        var sum: [32]u8 = @splat(0);
        for (item.cells) |cell| {
            var entry = Digest{};
            entry.scalar(.{ cell.row, cell.column, cell.row_span, cell.column_span, cell.text });
            if (mode == .exact) entry.scalar(cell.bbox);
            add(&sum, entry.finish());
        }
        self.hash.update(&sum);
    }
    fn tables(self: *Digest, items: []const c.Table, mode: Mode) void {
        self.scalar(items.len);
        var sum: [32]u8 = @splat(0);
        for (items) |item| {
            var entry = Digest{};
            entry.table(item, mode);
            add(&sum, entry.finish());
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

test "document logical tables agree across authoritative flat and projected views" {
    const a = std.testing.allocator;
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
    const tables = [_]c.Table{.{ .id = 3, .rows = 1, .columns = 1, .cells = &cells }};
    const regions = [_]c.Region{.{ .id = 7, .text = "caption", .tables = &tables }};
    const base = executor.Candidate{ .text = "caption", .regions = &regions };
    const projected = try ir.projectTables(a, &.{}, &regions, 16);
    defer a.free(projected.tables);
    try std.testing.expect(projected.tables[0].projection);
    var view = base;
    view.tables = projected.tables;
    try std.testing.expect(try sameEvidence(a, base, view));
    var legacy = [_]c.Table{projected.tables[0]};
    legacy[0].projection = false;
    view.tables = &legacy;
    try std.testing.expect(try sameEvidence(a, base, view));
    // A primitive may publish only its flat authoritative view. Presentation
    // location is not semantic evidence when the qualified owner still agrees.
    const flat_regions = [_]c.Region{.{ .id = 42, .text = "caption" }};
    legacy[0].region_id = 42;
    legacy[0].id = 9;
    try std.testing.expect(try sameEvidence(a, base, .{ .text = "caption", .regions = &flat_regions, .tables = &legacy }));
    // Without cross-references the canonical bag still needs no allocations.
    try std.testing.expect(try sameEvidence(std.testing.failing_allocator, base, base));
    var wrong_cells = cells;
    wrong_cells[0].text = "different";
    legacy[0].cells = &wrong_cells;
    legacy[0].id = 3;
    legacy[0].region_id = 7;
    view.tables = &legacy;
    try std.testing.expect(!try sameEvidence(a, base, view));
    legacy[0].projection = true;
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, base, view));
    legacy[0].cells = &cells;
    legacy[0].region_id = null;
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, base, view));
}

test "document projection aliases retain genuine multiplicity and reserve declared links first" {
    const a = std.testing.allocator;
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "same" }};
    const table = c.Table{ .id = 3, .rows = 1, .columns = 1, .cells = &cells };
    const repeated = [_]c.Table{ table, table };
    const regions = [_]c.Region{.{ .id = 7, .text = "same", .tables = &repeated }};
    const base = executor.Candidate{ .text = "same", .regions = &regions };
    const projected = try ir.projectTables(a, &.{}, &regions, 16);
    defer a.free(projected.tables);
    try std.testing.expect(try sameEvidence(a, base, .{ .text = "same", .regions = &regions, .tables = projected.tables }));
    const single = [_]c.Region{.{ .id = 7, .text = "same", .tables = &.{table} }};
    try std.testing.expect(!try sameEvidence(a, base, .{ .text = "same", .regions = &single }));
    var extra = [_]c.Table{ projected.tables[0], projected.tables[0], projected.tables[1] };
    extra[0].projection = false;
    try std.testing.expect(!try sameEvidence(a, base, .{ .text = "same", .regions = &regions, .tables = &extra }));
    extra[0].projection = true;
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, base, .{ .text = "same", .regions = &regions, .tables = &extra }));
    const distinct = [_]c.Table{ table, .{ .id = 4, .rows = 1, .columns = 1, .cells = &cells } };
    const distinct_regions = [_]c.Region{.{ .id = 7, .text = "same", .tables = &distinct }};
    const distinct_projection = try ir.projectTables(a, &.{}, &distinct_regions, 16);
    defer a.free(distinct_projection.tables);
    try std.testing.expect(try sameEvidence(a, base, .{ .text = "same", .regions = &distinct_regions, .tables = distinct_projection.tables }));
    const owned = [_]c.Region{ .{ .id = 7, .text = "same", .tables = &.{table} }, .{ .id = 8, .text = "same", .tables = &.{table} } };
    const owned_projection = try ir.projectTables(a, &.{}, &owned, 16);
    defer a.free(owned_projection.tables);
    const reversed = [_]c.Table{ owned_projection.tables[1], owned_projection.tables[0] };
    try std.testing.expect(try sameEvidence(a, .{ .text = "samesame", .regions = &owned }, .{ .text = "samesame", .regions = &owned, .tables = &reversed }));
}

test "document nested projections remain qualified through hosted reader decoding" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
    const tables = [_]c.Table{.{ .id = 5, .rows = 1, .columns = 1, .cells = &cells }};
    const children = [_]c.Region{.{ .id = 11, .text = "caption", .tables = &tables }};
    const parent_projection = try ir.projectTables(work, &.{}, &children, 16);
    const base_regions = [_]c.Region{.{ .id = 7, .text = "caption", .children = &children }};
    const projected_regions = [_]c.Region{.{ .id = 7, .text = "caption", .children = &children, .tables = parent_projection.tables }};
    const root_projection = try ir.projectTables(work, &.{}, &projected_regions, 16);
    const base = executor.Candidate{ .text = "caption", .regions = &base_regions };
    const body = try std.json.Stringify.valueAlloc(work, c.Page{ .source_id = "receipt", .number = 1, .kind = .image, .text = "caption", .status = .complete, .regions = &projected_regions, .tables = root_projection.tables }, .{});
    const decoded = try @import("reader_result.zig").decode(work, .{ .text = "caption", .document_json = body }, .{ .name = "hosted", .model = "hosted", .operation = .read }, 16, 16384, .{}, .{ 0, 0 });
    try std.testing.expect(decoded.tables[0].projection);
    try std.testing.expect(decoded.regions[0].tables[0].projection);
    try std.testing.expect(try sameEvidence(a, base, decoded));
    // Projection may intentionally coarsen the owner to its containing crop.
    // The explicit link keeps the authoritative child ownership intact.
    const child_regions = [_]c.Region{.{ .id = 11, .text = "caption" }};
    const child_owned = [_]c.Table{.{ .id = 5, .region_id = 11, .rows = 1, .columns = 1, .cells = &cells }};
    const crop = [_]c.Region{.{ .id = 7, .text = "caption", .children = &child_regions, .tables = &child_owned }};
    const coarse = try ir.projectTables(work, &.{}, &crop, 16);
    try std.testing.expect(try sameEvidence(a, .{ .text = "caption", .regions = &crop }, .{ .text = "caption", .regions = &crop, .tables = coarse.tables }));
    const missing = [_]c.Table{.{ .id = 999, .region_id = 7, .projection = true, .rows = 1, .columns = 1, .cells = &cells }};
    try std.testing.expectError(error.InvalidDocumentModelResult, sameEvidence(a, base, .{ .text = "caption", .regions = &base_regions, .tables = &missing }));
}

test "document logical table indexing cleans up every allocation failure" {
    const Runner = struct {
        fn run(backing: A) !void {
            const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
            const tables = [_]c.Table{.{ .id = 3, .rows = 1, .columns = 1, .cells = &cells }};
            const regions = [_]c.Region{.{ .id = 7, .text = "caption", .tables = &tables }};
            const projected = [_]c.Table{.{ .id = 3, .region_id = 7, .projection = true, .rows = 1, .columns = 1, .cells = &cells }};
            try std.testing.expect(try sameEvidence(backing, .{ .text = "caption", .regions = &regions }, .{ .text = "caption", .regions = &regions, .tables = &projected }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
