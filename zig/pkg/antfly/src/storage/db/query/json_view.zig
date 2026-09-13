// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;

/// Arena-owned, lazy navigation over pinned JSON bytes. Only visited containers
/// acquire an index; unselected subtrees remain raw spans. Scanner.skipValue
/// validates their syntax without allocating their strings or DOM. A view may
/// be reused across predicates and projections while its input stays pinned.
pub const View = struct {
    alloc: Allocator,
    root: Node,
    materialized_values: usize = 0,

    pub const Node = struct {
        raw: []const u8,
        children: ?[]Child = null,
        object_index: std.StringHashMapUnmanaged(usize) = .empty,
        array_index: std.AutoHashMapUnmanaged(usize, *Node) = .empty,
        logical: ?Value = null,

        fn kind(self: Node) u8 {
            const raw = std.mem.trim(u8, self.raw, " \r\n\t");
            return if (raw.len == 0) 0 else raw[0];
        }
    };
    const Child = struct { node: Node };

    pub fn init(alloc: Allocator, raw: []const u8) View {
        return .{ .alloc = alloc, .root = .{ .raw = raw } };
    }

    fn children(self: *View, node: *Node) ![]Child {
        if (node.children) |cached| return cached;
        const object = node.kind() == '{';
        if (!object and node.kind() != '[') return &.{};
        var scanner = std.json.Scanner.initCompleteInput(self.alloc, node.raw);
        defer scanner.deinit();
        _ = try scanner.next();
        var entries = std.ArrayListUnmanaged(Child).empty;
        var index = std.StringHashMapUnmanaged(usize).empty;
        while (true) {
            const token = try scanner.peekNextTokenType();
            if (token == .object_end or token == .array_end) break;
            var name: []const u8 = "";
            if (object) {
                name = switch (try scanner.nextAllocMax(self.alloc, .alloc_if_needed, node.raw.len)) {
                    .string, .allocated_string => |key| key,
                    else => return error.SyntaxError,
                };
            }
            switch (try scanner.peekNextTokenType()) {
                .object_end, .array_end, .end_of_document => return error.SyntaxError,
                else => {},
            }
            const start = scanner.cursor;
            try scanner.skipValue();
            if (object) {
                const entry = try index.getOrPut(self.alloc, name);
                if (entry.found_existing) return error.DuplicateField;
                entry.value_ptr.* = entries.items.len;
            }
            try entries.append(self.alloc, .{ .node = .{ .raw = node.raw[start..scanner.cursor] } });
        }
        _ = try scanner.next();
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
        node.children = try entries.toOwnedSlice(self.alloc);
        node.object_index = index;
        return node.children.?;
    }

    fn field(self: *View, node: *Node, name: []const u8) !?*Node {
        const entries = try self.children(node);
        const i = node.object_index.get(name) orelse return null;
        return &entries[i].node;
    }

    fn element(self: *View, node: *Node, index: usize) !?*Node {
        if (node.children) |entries| return if (index < entries.len) &entries[index].node else null;
        if (node.array_index.get(index)) |cached| return cached;
        var scanner = std.json.Scanner.initCompleteInput(self.alloc, node.raw);
        defer scanner.deinit();
        _ = try scanner.next();
        var i: usize = 0;
        while (try scanner.peekNextTokenType() != .array_end) : (i += 1) {
            switch (try scanner.peekNextTokenType()) {
                .object_end, .end_of_document => return error.SyntaxError,
                else => {},
            }
            const start = scanner.cursor;
            try scanner.skipValue();
            if (i == index) {
                const child = try self.alloc.create(Node);
                child.* = .{ .raw = node.raw[start..scanner.cursor] };
                try node.array_index.put(self.alloc, index, child);
                return child;
            }
        }
        return null;
    }

    pub fn materialize(self: *View, node: *Node) !Value {
        if (node.logical) |value| return value;
        // Predicates share the document evaluator's numeric interpretation.
        // Projection below separately retains exact stored number lexemes.
        const value = try std.json.parseFromSliceLeaky(Value, self.alloc, node.raw, .{});
        node.logical = value;
        self.materialized_values += 1;
        return value;
    }

    fn pointerIndex(segment: []const u8) bool {
        if (segment.len == 0 or (segment.len > 1 and segment[0] == '0')) return false;
        for (segment) |c| if (c < '0' or c > '9') return false;
        return true;
    }

    /// Dotted paths fan out through arrays; JSON pointers require a canonical
    /// numeric index. Iterative traversal also handles deeply nested arrays
    /// without turning user data depth into native call-stack depth.
    pub fn collect(self: *View, path: []const []const u8, pointer: bool, out: *std.ArrayListUnmanaged(Value)) !void {
        const Work = struct { node: *Node, depth: usize };
        var pending = std.ArrayListUnmanaged(Work).empty;
        defer pending.deinit(self.alloc);
        try pending.append(self.alloc, .{ .node = &self.root, .depth = 0 });
        while (pending.pop()) |work| {
            if (work.depth == path.len) {
                try out.append(self.alloc, try self.materialize(work.node));
                continue;
            }
            const part = path[work.depth];
            switch (work.node.kind()) {
                '{' => if (try self.field(work.node, part)) |node| {
                    try pending.append(self.alloc, .{ .node = node, .depth = work.depth + 1 });
                },
                '[' => {
                    if (pointer and !pointerIndex(part)) continue;
                    if (std.fmt.parseInt(usize, part, 10)) |i| {
                        if (try self.element(work.node, i)) |child| try pending.append(self.alloc, .{ .node = child, .depth = work.depth + 1 });
                    } else |_| if (!pointer) {
                        const entries = try self.children(work.node);
                        var i = entries.len;
                        while (i != 0) {
                            i -= 1;
                            try pending.append(self.alloc, .{ .node = &entries[i].node, .depth = work.depth });
                        }
                    }
                },
                else => {},
            }
        }
    }

    /// Apply one positive projection in caller order. Objects merge, numeric
    /// array selections compact to one item, array fanout visits object items,
    /// and later array selections replace earlier ones, just like lookupJson.
    /// Output belongs to a separate row arena; cached navigation can outlive it.
    pub fn include(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, name: []const u8, path: []const []const u8) !void {
        try self.includeNode(alloc, dst, name, &self.root, path);
    }

    fn includeNode(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, name: []const u8, node: *Node, path: []const []const u8) anyerror!void {
        if (path.len == 0) {
            // Projection mutates merged objects. Never lend a cached logical
            // container to it: subsequent paths must still see the original.
            const value = try std.json.parseFromSliceLeaky(Value, alloc, node.raw, .{ .parse_numbers = false });
            try dst.put(alloc, name, value);
            return;
        }
        switch (node.kind()) {
            '{' => {
                const entry = try dst.getOrPut(alloc, name);
                if (!entry.found_existing or entry.value_ptr.* != .object) entry.value_ptr.* = .{ .object = .empty };
                if (try self.field(node, path[0])) |child| try self.includeNode(alloc, &entry.value_ptr.object, path[0], child, path[1..]);
            },
            '[' => {
                var result = std.json.Array.init(alloc);
                const numeric = std.fmt.parseInt(usize, path[0], 10) catch null;
                // document projection accepts only digit-only indices.
                const index = if (path[0].len != 0 and std.mem.indexOfNone(u8, path[0], "0123456789") == null) numeric else null;
                if (index) |i| {
                    const item = (try self.element(node, i)) orelse return;
                    if (path.len == 1) {
                        try result.append(try std.json.parseFromSliceLeaky(Value, alloc, item.raw, .{ .parse_numbers = false }));
                    } else if (item.kind() == '{') {
                        var object = std.json.ObjectMap.empty;
                        if (try self.field(item, path[1])) |child| try self.includeNode(alloc, &object, path[1], child, path[2..]);
                        if (object.count() != 0) try result.append(.{ .object = object });
                    }
                } else for (try self.children(node)) |*entry| {
                    if (entry.node.kind() != '{') continue;
                    var object = std.json.ObjectMap.empty;
                    if (try self.field(&entry.node, path[0])) |child| try self.includeNode(alloc, &object, path[0], child, path[1..]);
                    if (object.count() != 0) try result.append(.{ .object = object });
                }
                if (result.items.len != 0) try dst.put(alloc, name, .{ .array = result });
            },
            else => {},
        }
    }
};

test "relational lazy JSON paths skip unrelated DOM and preserve pointer semantics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var view = View.init(alloc, "{\"a\\u002fb\":[{\"n\":9007199254740993},{\"n\":null}],\"skip\":[[1,2,3]]}");
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{ "a/b", "n" }, false, &values);
    try std.testing.expectEqual(@as(usize, 2), values.items.len);
    try std.testing.expectEqual(@as(i64, 9007199254740993), values.items[0].integer);
    try std.testing.expect(values.items[1] == .null);
    try std.testing.expect((try view.field(&view.root, "skip")).?.children == null);
    values.clearRetainingCapacity();
    try view.collect(&.{ "a/b", "01", "n" }, true, &values);
    try std.testing.expectEqual(@as(usize, 0), values.items.len);
    try view.collect(&.{ "a/b", "0", "n" }, true, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
}

test "relational lazy JSON leaf and numeric array projection have bounded allocation" {
    const alloc = std.testing.allocator;
    const items = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(items);
    for (items, 0..) |*byte, i| byte.* = if (i % 2 == 0) '0' else ',';
    const raw = try std.fmt.allocPrint(alloc, "{{\"match\":1,\"items\":[{s}7]}}", .{items});
    defer alloc.free(raw);
    var buffer: [16 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var view = View.init(fixed.allocator(), raw);
    var result = std.json.ObjectMap.empty;
    try view.include(fixed.allocator(), &result, "payload", &.{"match"});
    const leaf = try std.json.Stringify.valueAlloc(fixed.allocator(), Value{ .object = result }, .{});
    try std.testing.expectEqualStrings("{\"payload\":{\"match\":1}}", leaf);
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{ "items", "131072" }, true, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
    try std.testing.expectEqual(@as(i64, 7), values.items[0].integer);
    try std.testing.expect((try view.field(&view.root, "items")).?.children == null);
    result = .empty;
    try view.include(fixed.allocator(), &result, "payload", &.{ "items", "131072" });
    const indexed = try std.json.Stringify.valueAlloc(fixed.allocator(), Value{ .object = result }, .{});
    try std.testing.expectEqualStrings("{\"payload\":{\"items\":[7]}}", indexed);
}

test "relational lazy JSON errors release all arenas and deep fanout is iterative" {
    const alloc = std.testing.allocator;
    const Check = struct {
        fn run(failing: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(failing);
            defer arena.deinit();
            const scratch = arena.allocator();
            var view = View.init(scratch, "{\"a\\u002fb\":[{\"n\":\"escaped\\nvalue\"},{\"n\":null}],\"x\":1}");
            var values = std.ArrayListUnmanaged(Value).empty;
            try view.collect(&.{ "a/b", "n" }, false, &values);
            var output = std.json.ObjectMap.empty;
            try view.include(scratch, &output, "payload", &.{ "a/b", "n" });
            try view.include(scratch, &output, "payload", &.{ "a/b", "0" });
            try view.include(scratch, &output, "payload", &.{});
            try view.include(scratch, &output, "payload", &.{"x"});
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{});
    for ([_][]const u8{ "{\"x\":[1,]}", "{\"x\":1,\"x\":2}" }) |raw| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var view = View.init(arena.allocator(), raw);
        var values = std.ArrayListUnmanaged(Value).empty;
        if (view.collect(&.{"x"}, false, &values)) |_| return error.TestExpectedError else |_| {}
    }
    const depth = 1024;
    const raw = try alloc.alloc(u8, 2 * depth + 7);
    defer alloc.free(raw);
    @memset(raw[0..depth], '[');
    @memcpy(raw[depth..][0..7], "{\"n\":1}");
    @memset(raw[depth + 7 ..], ']');
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var view = View.init(arena.allocator(), raw);
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{"n"}, false, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
}
