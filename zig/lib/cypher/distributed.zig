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

//! Statement coordinator over authenticated owner capabilities. All owners
//! finish read-index admission before any mutation fence is held. Snapshots
//! are forked with the complete cohort fenced, then every fence is released.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
pub const Target = struct { table: []const u8, table_id: u64, group_id: u64 };
pub const Snapshot = struct { backend: b.Backend, ptr: *anyopaque, validate: *const fn (*anyopaque) anyerror!void, close: *const fn (*anyopaque) void };
pub const Fence = struct { ptr: *anyopaque, validate: *const fn (*anyopaque) anyerror!void, fork: *const fn (*anyopaque, A) anyerror!Snapshot, release: *const fn (*anyopaque) void };
pub const Source = struct {
    ptr: *anyopaque,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    admit: *const fn (*anyopaque, Target) anyerror!void,
    capture: *const fn (*anyopaque, A, Target) anyerror!?Fence,
};
pub const ReadSet = struct {
    a: A,
    targets: []const Target,
    snapshots: []const Snapshot,
    source: Source,
    graph: ?catalog.Graph = null,
    pub fn acquire(a: A, source: Source, targets: []const Target) !ReadSet {
        if (targets.len == 0 or targets.len > 1024) return error.CypherOwnerBudgetExceeded;
        for (targets, 0..) |target, i| {
            if (target.table_id == 0 or target.group_id == 0 or target.table.len == 0) return error.CypherInvalidOwner;
            for (targets[0..i]) |other| {
                if (target.group_id == other.group_id and target.table_id == other.table_id) return error.CypherInvalidOwner;
                if (std.mem.eql(u8, target.table, other.table) and target.table_id != other.table_id) return error.CatalogGenerationChanged;
            }
        }
        for (targets) |target| {
            try source.checkpoint(source.ptr);
            try source.admit(source.ptr, target);
        }
        const fences = try a.alloc(Fence, targets.len);
        defer a.free(fences);
        const snapshots = try a.alloc(Snapshot, targets.len);
        errdefer a.free(snapshots);
        var captured: usize = 0;
        defer for (fences[0..captured]) |fence| fence.release(fence.ptr);
        var forked: usize = 0;
        errdefer for (snapshots[0..forked]) |snapshot| snapshot.close(snapshot.ptr);
        // A busy participant requires releasing the entire earlier cohort.
        // Never wait for transaction resolution while blocking another owner.
        for (0..4) |_| {
            for (targets) |target| {
                try source.checkpoint(source.ptr);
                const fence = (try source.capture(source.ptr, a, target)) orelse break;
                fences[captured] = fence;
                captured += 1;
            }
            if (captured == targets.len) break;
            for (fences[0..captured]) |fence| fence.release(fence.ptr);
            captured = 0;
        }
        if (captured != targets.len) return error.Backpressured;
        for (fences, snapshots) |fence, *snapshot| {
            try source.checkpoint(source.ptr);
            snapshot.* = try fence.fork(fence.ptr, a);
            forked += 1;
        }
        for (fences) |fence| try fence.validate(fence.ptr);
        const owned = try a.alloc(Target, targets.len);
        errdefer a.free(owned);
        var copied: usize = 0;
        errdefer for (owned[0..copied]) |target| a.free(target.table);
        for (targets, owned) |target, *out| {
            out.* = target;
            out.table = try a.dupe(u8, target.table);
            copied += 1;
        }
        return .{ .a = a, .source = source, .targets = owned, .snapshots = snapshots };
    }
    pub fn deinit(self: *ReadSet) void {
        for (self.snapshots) |snapshot| snapshot.close(snapshot.ptr);
        for (self.targets) |target| self.a.free(target.table);
        self.a.free(self.targets);
        self.a.free(self.snapshots);
        self.* = undefined;
    }
    pub fn backend(self: *ReadSet) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search } };
    }
    fn cast(ptr: *anyopaque) *ReadSet {
        return @ptrCast(@alignCast(ptr));
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.source.checkpoint(self.source.ptr);
        for (self.snapshots) |snapshot| try snapshot.validate(snapshot.ptr);
    }
    fn bind(ptr: *anyopaque, a: A, graph: catalog.Graph) !void {
        const self = cast(ptr);
        try checkpoint(ptr);
        for (graph.labels) |label| {
            var found = false;
            for (self.targets) |target| if (std.mem.eql(u8, target.table, label.table)) {
                if (label.table_id != 0 and label.table_id != target.table_id) return error.CatalogGenerationChanged;
                found = true;
            };
            if (!found) return error.UndefinedGraphTable;
        }
        for (self.snapshots) |snapshot| try snapshot.backend.vtable.bind(snapshot.backend.ptr, a, graph);
        self.graph = graph;
    }
    const Cursor = struct { owner: usize, after: ?[]const u8 = null };
    fn cursor(self: *ReadSet, a: A, table: []const u8, encoded: ?[]const u8) !Cursor {
        if (encoded) |json| {
            const result = try std.json.parseFromSliceLeaky(Cursor, a, json, .{});
            if (result.owner >= self.targets.len or !std.mem.eql(u8, self.targets[result.owner].table, table)) return error.CypherInvalidBackendCursor;
            return result;
        }
        for (self.targets, 0..) |target, i| if (std.mem.eql(u8, target.table, table)) return .{ .owner = i };
        return error.UndefinedGraphTable;
    }
    fn next(self: *ReadSet, a: A, table: []const u8, position: Cursor, after: ?[]const u8) !?[]const u8 {
        if (after) |continuation| return try std.json.Stringify.valueAlloc(a, Cursor{ .owner = position.owner, .after = continuation }, .{});
        for (self.targets[position.owner + 1 ..], position.owner + 1..) |target, i| if (std.mem.eql(u8, target.table, table)) return try std.json.Stringify.valueAlloc(a, Cursor{ .owner = i }, .{});
        return null;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        const position = try self.cursor(a, table, encoded);
        const owner = self.snapshots[position.owner].backend;
        const page = try owner.vtable.scan(owner.ptr, a, table, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.next(a, table, position, page.next) };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        var result: ?*const v.Node = null;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, identity.table)) continue;
            if (try snapshot.backend.vtable.get(snapshot.backend.ptr, a, identity)) |node| {
                if (result != null) return error.CypherOverlappingOwners;
                result = node;
            }
        }
        try checkpoint(ptr);
        return result;
    }
    fn expand(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, direction: ast.Direction, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        // Physical adjacency, including incoming adjacency, belongs to source
        // owners. Resolve the label from the statement-bound graph at bind.
        const table = self.mappingSource(mapping) orelse return error.UndefinedGraphTable;
        try checkpoint(ptr);
        const position = try self.cursor(a, table, encoded);
        const owner = self.snapshots[position.owner].backend;
        const page = try owner.vtable.expand(owner.ptr, a, mapping, identity, direction, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.next(a, table, position, page.next) };
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        try checkpoint(ptr);
        const parameters = input.parameters;
        if (parameters != .map) return error.CypherTypeMismatch;
        const requested = parameters.property("limit");
        const limit: usize = if (requested == .null) 10 else if (requested == .integer and requested.integer > 0 and requested.integer <= 1024) @intCast(requested.integer) else return error.CypherInvalidSearchLimit;
        var best: std.ArrayList(b.Seed) = .empty;
        var found = false;
        var examined: usize = 0;
        var approximate = false;
        var searched_owners: usize = 0;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            found = true;
            searched_owners += 1;
            try checkpoint(ptr);
            const callback = snapshot.backend.vtable.search orelse return error.CypherSearchUnavailable;
            var demand = input;
            demand.max_examined = input.max_examined -| examined;
            const page = try callback(snapshot.backend.ptr, a, table, demand);
            if (page.examined > demand.max_examined) return error.CypherAnchorBudgetExceeded;
            examined += page.examined;
            approximate = approximate or page.approximate;
            const seeds = page.seeds;
            if (seeds.len > limit) return error.CypherInvalidSearchResult;
            for (seeds) |seed| {
                if (!std.math.isFinite(seed.score) or !std.mem.eql(u8, seed.node.identity.table, table)) return error.CypherInvalidSearchResult;
                for (best.items) |other| if (other.node.identity.eql(seed.node.identity)) return error.CypherOverlappingOwners;
                var index: usize = 0;
                while (index < best.items.len and less({}, best.items[index], seed)) : (index += 1) {}
                if (index < limit) {
                    try best.insert(a, index, seed);
                    if (best.items.len > limit) best.items.len = limit;
                }
            }
        }
        if (!found) return error.UndefinedGraphTable;
        try checkpoint(ptr);
        // Text and hybrid scores currently use owner-local corpus statistics.
        // Report this explicitly until a global-statistics scorer is available.
        return .{ .seeds = try best.toOwnedSlice(a), .examined = examined, .approximate = approximate or searched_owners > 1 };
    }
    fn less(_: void, left: b.Seed, right: b.Seed) bool {
        if (left.score != right.score) return left.score > right.score;
        return std.mem.order(u8, left.node.identity.key, right.node.identity.key) == .lt;
    }
    fn mappingSource(self: *ReadSet, mapping: catalog.Relationship) ?[]const u8 {
        return (self.graph orelse return null).label(mapping.source_label).?.table;
    }
};

test "distributed graph captures the whole cohort and pages immutable owners" {
    const Fixture = struct {
        held: usize = 0,
        admitted: usize = 0,
        forks: usize = 0,
        closed: usize = 0,
        busy_once: bool = true,
        revoked: bool = false,
        owners: [2]Owner = undefined,
        const Owner = struct {
            fixture: *Self,
            index: usize,
            node: v.Node,
            fn checkpoint(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.fixture.revoked) return error.Forbidden;
            }
            fn validate(ptr: *anyopaque) !void {
                try Owner.checkpoint(ptr);
            }
            fn release(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.held -= 1;
            }
            fn fork(ptr: *anyopaque, _: A) !Snapshot {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 2), self.fixture.held);
                self.fixture.forks += 1;
                return .{ .ptr = self, .validate = validate, .close = close, .backend = .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = Owner.checkpoint, .scan = scan, .get = get, .expand = expand, .search = search } } };
            }
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.closed += 1;
            }
            fn bind(ptr: *anyopaque, _: A, _: catalog.Graph) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 0), self.fixture.held);
            }
            fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try Owner.checkpoint(ptr);
                if (after != null) return error.CypherInvalidBackendCursor;
                return .{ .items = try a.dupe(*const v.Node, &.{&self.node}), .examined = 1 };
            }
            fn get(ptr: *anyopaque, _: A, identity: v.Identity) !?*const v.Node {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return if (self.node.identity.eql(identity)) &self.node else null;
            }
            fn search(ptr: *anyopaque, a: A, _: []const u8, _: b.SearchInput) !b.SearchPage {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return .{ .seeds = try a.dupe(b.Seed, &.{.{ .node = &self.node, .score = @floatFromInt(self.index + 1) }}), .examined = 1 };
            }
            fn expand(_: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                return .{ .items = &.{} };
            }
        };
        const Self = @This();
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn admit(ptr: *anyopaque, _: Target) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 0), self.held);
            self.admitted += 1;
        }
        fn capture(ptr: *anyopaque, _: A, target: Target) !?Fence {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), self.admitted);
            if (target.group_id == 2 and self.busy_once) {
                self.busy_once = false;
                return null;
            }
            self.held += 1;
            return .{ .ptr = &self.owners[target.group_id - 1], .validate = Owner.validate, .fork = Owner.fork, .release = Owner.release };
        }
    };
    var fixture: Fixture = .{};
    fixture.owners = .{
        .{ .fixture = &fixture, .index = 0, .node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"} } },
        .{ .fixture = &fixture, .index = 1, .node = .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"Document"} } },
    };
    var set = try ReadSet.acquire(std.testing.allocator, .{ .ptr = &fixture, .checkpoint = Fixture.checkpoint, .admit = Fixture.admit, .capture = Fixture.capture }, &.{ .{ .table = "docs", .table_id = 7, .group_id = 1 }, .{ .table = "docs", .table_id = 7, .group_id = 2 } });
    defer set.deinit();
    try std.testing.expectEqual(@as(usize, 0), fixture.held);
    try std.testing.expectEqual(@as(usize, 2), fixture.forks);
    const root = @import("root.zig");
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (n:Document) RETURN antfly.documentKey(n) AS key ORDER BY key", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, set.backend(), .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs", .table_id = 7 }}, .relationships = &.{} }, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("a", result.rows[0][0].string);
    try std.testing.expectEqualStrings("b", result.rows[1][0].string);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const backend = set.backend();
    const seeds = try backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", .{ .parameters = .{ .map = &.{.{ .name = "limit", .value = .{ .integer = 1 } }} } });
    try std.testing.expectEqual(@as(usize, 1), seeds.seeds.len);
    try std.testing.expectEqualStrings("b", seeds.seeds[0].node.identity.key);
    try std.testing.expectEqual(@as(f64, 2), seeds.seeds[0].score);
    try std.testing.expect(seeds.approximate);
    try std.testing.expectError(error.CypherInvalidSearchLimit, backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", .{ .parameters = .{ .map = &.{.{ .name = "limit", .value = .{ .integer = 1025 } }} } }));
    fixture.revoked = true;
    try std.testing.expectError(error.Forbidden, set.backend().vtable.checkpoint(&set));
}
