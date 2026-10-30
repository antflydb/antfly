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

const std = @import("std");
const d = @import("distributed.zig");
const b = @import("backend.zig");
const v = @import("value.zig");
const c = @import("catalog.zig");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
test "producer owned adjacency survives source range routing" {
    const F = struct {
        const Parent = @This();
        source_edge: bool = false,
        first: v.Relationship = .{ .identity = .{ .table = "docs", .key = "first" }, .index = "links", .type = "R", .source = .{ .table = "docs", .key = "a" }, .target = .{ .table = "docs", .key = "b" } },
        edge: v.Relationship = .{ .identity = .{ .table = "docs", .key = "r" }, .index = "links", .type = "R", .source = .{ .table = "docs", .key = "a" }, .target = .{ .table = "docs", .key = "b" }, .ownership = .artifact, .producer = .{ .table = "docs", .key = "z" } },
        const Owner = struct {
            f: *Parent,
            index: usize,
            fn batchOne(ptr: *anyopaque, a: A, mapping: c.Relationship, identity: v.Identity, direction: ast.Direction, limit: usize, _: usize) !b.Page(*const v.Relationship) {
                return expand(ptr, a, mapping, identity, direction, null, limit);
            }
            fn expandMany(ptr: *anyopaque, a: A, mapping: c.Relationship, identities: []const v.Identity, direction: ast.Direction, limit: usize, maximum: usize) !?b.ExpansionBatch {
                return @import("expansion_batch.zig").run(a, ptr, mapping, identities, direction, limit, maximum, null, batchOne);
            }
            fn check(_: *anyopaque) !void {}
            fn expand(ptr: *anyopaque, a: A, _: c.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return .{ .items = if (self.index == 1) try a.dupe(*const v.Relationship, &.{&self.f.edge}) else if (self.f.source_edge) try a.dupe(*const v.Relationship, &.{&self.f.first}) else &.{} };
            }
        };
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var f: F = .{};
    var owners = [_]F.Owner{ .{ .f = &f, .index = 0 }, .{ .f = &f, .index = 1 } };
    var snapshots: [2]d.Snapshot = undefined;
    for (&owners, &snapshots) |*owner, *snapshot| snapshot.* = .{ .ptr = owner, .validate = F.Owner.check, .close = undefined, .backend = .{ .ptr = owner, .vtable = &.{ .bind = undefined, .checkpoint = F.Owner.check, .scan = undefined, .get = undefined, .expand = F.Owner.expand, .expand_many = F.Owner.expandMany } } };
    const mapping: c.Relationship = .{ .name = "R", .stored_type = "R", .index = "links", .source_label = "D", .target_label = "D" };
    var targets = [_]d.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var set: d.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &f, .checkpoint = F.Owner.check, .admit = undefined, .capture = undefined }, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{mapping} } };
    const backend = set.backend();
    for ([_]bool{ false, true }) |certified_ranges| {
        if (certified_ranges) {
            targets[0].ranges = &.{.{ .end = "m" }};
            targets[1].ranges = &.{.{ .start = "m" }};
        }
        var after: ?[]const u8 = null;
        var count: usize = 0;
        while (true) {
            const page = try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, .out, after, 128);
            count += page.items.len;
            after = page.next orelse break;
        }
        const batch = (try backend.vtable.expand_many.?(backend.ptr, a, mapping, &.{.{ .table = "docs", .key = "a" }}, .out, 1, 128)).?;
        try std.testing.expectEqual(@as(usize, 1), batch.pages[0].?.items.len);
        try std.testing.expectEqualStrings("z", batch.pages[0].?.items[0].producer.?.key);
        try std.testing.expectEqual(@as(usize, 1), count);
    }
    // A nonempty first owner must retain a continuation to the producer's
    // owner, rather than combining pages past the requested row allowance.
    f.source_edge = true;
    const prefix = (try backend.vtable.expand_many.?(backend.ptr, a, mapping, &.{.{ .table = "docs", .key = "a" }}, .out, 1, 128)).?;
    try std.testing.expectEqual(@as(usize, 1), prefix.pages[0].?.items.len);
    try std.testing.expectEqualStrings("first", prefix.pages[0].?.items[0].identity.key);
    const suffix = try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, .out, prefix.pages[0].?.next.?, 128);
    try std.testing.expectEqual(@as(usize, 1), suffix.items.len);
    try std.testing.expectEqualStrings("z", suffix.items[0].producer.?.key);
}

test "adaptive correlated prefetch preserves nonunique batches" {
    const root = @import("root.zig");
    const F = struct {
        nodes: [254]v.Node = undefined,
        props: [254][1]v.Entry = undefined,
        fanout: usize = 1,
        probes: usize = 0,
        scans: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn bind(_: *anyopaque, _: A, _: c.Graph) !void {}
        fn get(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: A, _: c.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return .{ .items = &.{} };
        }
        fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            return error.UnexpectedPrimaryScan;
        }
        fn matches(node: *const v.Node, filters: []const b.PropertyFilter) bool {
            for (filters) |filter| {
                const value = (v.Value{ .node = node }).property(filter.property);
                if (filter.op == .eq) {
                    if (!v.Value.eql(value, filter.value)) return false;
                } else if (filter.op == .in) {
                    const found = for (filter.value.list) |item| {
                        if (v.Value.eql(value, item)) break true;
                    } else false;
                    if (!found) return false;
                } else unreachable;
            }
            return true;
        }
        fn filtered(ptr: *anyopaque, a: A, _: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scans += 1;
            const start = if (after) |text| try std.fmt.parseInt(usize, text, 10) else 0;
            var out: std.ArrayList(*const v.Node) = .empty;
            var i = start;
            while (i < 127 * self.fanout and out.items.len < limit) : (i += 1) if (matches(&self.nodes[i], filters)) {
                try out.append(a, &self.nodes[i]);
            };
            return .{ .items = out.items, .examined = out.items.len, .next = if (i < 127 * self.fanout) try std.fmt.allocPrint(a, "{d}", .{i}) else null };
        }
        fn probe(ptr: *anyopaque, a: A, _: []const u8, filters: []const b.PropertyFilter, limit: usize) !?b.Probe {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.probes += 1;
            var out: std.ArrayList(*const v.Node) = .empty;
            for (self.nodes[0 .. 127 * self.fanout]) |*node| if (matches(node, filters)) {
                try out.append(a, node);
                if (out.items.len > limit) return .{ .complete = false, .page = .{ .items = &.{}, .examined = limit + 1 } };
            };
            return .{ .complete = true, .page = .{ .items = out.items, .examined = out.items.len } };
        }
    };
    for ([_]usize{ 1, 2 }) |fanout| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var f: F = .{ .fanout = fanout };
        for (f.nodes[0 .. 127 * fanout], f.props[0 .. 127 * fanout], 0..) |*node, *prop, i| {
            prop.* = .{.{ .name = "bucket", .value = .{ .integer = @intCast(i / fanout) } }};
            node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "n{d}", .{i}) }, .labels = &.{"D"}, .properties = prop };
        }
        const backend: b.Backend = .{ .ptr = &f, .vtable = &.{ .bind = F.bind, .checkpoint = F.check, .scan = F.scan, .get = F.get, .expand = F.expand, .scan_where = F.filtered, .probe_where = F.probe } };
        var diag: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, "UNWIND range(0,126) AS i MATCH(n:D) WHERE n.bucket=i RETURN count(*)", .{}, &diag);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, backend, .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} }, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(127 * fanout)), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 0), f.scans);
        try std.testing.expectEqual(if (fanout == 1) @as(usize, 1) else 3, f.probes);
    }
}
