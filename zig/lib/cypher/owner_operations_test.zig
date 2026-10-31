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
const root = @import("root.zig");
const b = root.backend;
const v = root.value;
const A = std.mem.Allocator;
const definition: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{.{ .name = "R", .stored_type = "R", .index = "links", .source_label = "D", .target_label = "D" }} };
const Fixture = struct {
    graph: root.catalog.Graph = definition,
    bound_labels: usize = 0,
    nodes: [33]v.Node = undefined,
    properties: [33][2]v.Entry = undefined,
    edges: [32]v.Relationship = undefined,
    work: ?b.Work = null,
    bulk: usize = 0,
    counts: usize = 0,
    physical_rows: usize = 0,
    rpc_scans: usize = 0,
    rpc_expands: usize = 0,
    count_enabled: bool = true,
    batch_enabled: bool = true,
    fail_edges: bool = false,
    range_start: usize = 0,
    range_end: usize = 33,
    fn init(self: *@This(), a: A) !void {
        for (&self.nodes, &self.properties, 0..) |*node, *properties, i| {
            properties.* = .{ .{ .name = "start", .value = .{ .boolean = i < 16 } }, .{ .name = "value", .value = .{ .integer = @intCast(i) } } };
            node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "n{d}", .{i}) }, .labels = &.{"D"}, .properties = properties };
        }
        for (&self.edges, 0..) |*edge, i| edge.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "r{d}", .{i}) }, .index = "links", .type = "R", .source = self.nodes[i].identity, .target = self.nodes[if (i < 16) i + 16 else 32].identity };
    }
    fn bind(ptr: *anyopaque, _: A, graph: root.catalog.Graph) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.bound_labels = graph.labels.len;
    }
    fn check(_: *anyopaque) !void {}
    fn setWork(ptr: *anyopaque, work: ?b.Work) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.work = work;
    }
    fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.work) |work| try work.charge(work.ptr, 1);
        const start = if (after) |text| try std.fmt.parseInt(usize, text, 10) else self.range_start;
        const end = @min(self.range_end, start + limit);
        const nodes = try a.alloc(*const v.Node, end - start);
        for (nodes, start..) |*node, i| node.* = &self.nodes[i];
        self.physical_rows += nodes.len;
        return .{ .items = nodes, .examined = nodes.len, .next = if (end < self.range_end) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
    }
    fn filtered(ptr: *anyopaque, a: A, table: []const u8, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        return scan(ptr, a, table, after, limit);
    }
    fn get(ptr: *anyopaque, _: A, identity: v.Identity) !?*const v.Node {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        for (&self.nodes) |*node| if (node.identity.eql(identity)) return node;
        return null;
    }
    fn expand(ptr: *anyopaque, a: A, _: root.catalog.Relationship, identity: v.Identity, direction: root.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.work) |work| try work.charge(work.ptr, 1);
        var items: std.ArrayList(*const v.Relationship) = .empty;
        const start = if (after) |text| try std.fmt.parseInt(usize, text, 10) else 0;
        var i = start;
        while (i < self.edges.len) : (i += 1) {
            const edge = &self.edges[i];
            if (!((direction != .in and edge.source.eql(identity)) or (direction != .out and edge.target.eql(identity)))) continue;
            if (items.items.len == limit) return .{ .items = items.items, .examined = items.items.len, .next = try std.fmt.allocPrint(a, "{d}", .{i}) };
            try items.append(a, edge);
        }
        return .{ .items = items.items, .examined = items.items.len };
    }
    fn batchOne(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, direction: root.ast.Direction, limit: usize, _: usize) !b.Page(*const v.Relationship) {
        return expand(ptr, a, mapping, identity, direction, null, limit);
    }
    fn expandMany(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identities: []const v.Identity, direction: root.ast.Direction, limit: usize, maximum: usize) !?b.ExpansionBatch {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.batch_enabled) return null;
        if (self.fail_edges) {
            if (self.work) |work| try work.charge(work.ptr, 1);
            return error.CypherEdgeBudgetExceeded;
        }
        self.bulk += 1;
        return @import("expansion_batch.zig").run(a, ptr, mapping, identities, direction, limit, maximum, self.work, batchOne);
    }
    fn countNodes(ptr: *anyopaque, a: A, table: []const u8, input: b.CountInput) !?b.CountResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.count_enabled) return null;
        self.counts += 1;
        const work = self.work;
        defer self.work = work;
        return @import("owner_count.zig").run(a, self.backend(), self.graph, table, input, work);
    }
    fn backend(self: *@This()) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = check, .scan = scan, .scan_where = filtered, .get = get, .expand = expand, .expand_many = expandMany, .count_nodes = countNodes, .set_work = setWork } };
    }
    const Channel = struct {
        fixture: *Fixture,
        owner: root.rpc.Owner,
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const request = try std.json.parseFromSliceLeaky(root.rpc.Request, arena.allocator(), body, .{});
            if (request.operation == .scan) self.fixture.rpc_scans += 1;
            if (request.operation == .expand) self.fixture.rpc_expands += 1;
            return self.owner.execute(a, body);
        }
    };
};
test "owner COUNT preserves predicates NULL parameters and budgets without row RPCs" {
    for ([_]bool{ false, true }) |null_parameter| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        try fixture.init(a);
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, "MATCH(n:D {start:true}) WHERE n.value >= $low RETURN count(*) AS total", .{}, &diagnostic);
        defer compiled.deinit();
        const parameters = &[_]v.Entry{.{ .name = "low", .value = if (null_parameter) .null else .{ .integer = 8 } }};
        var result = try root.runtime.execute(a, client.backend(), definition, &compiled, parameters, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, if (null_parameter) 0 else 8), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 33), result.stats.scanned_anchors);
        try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
        try std.testing.expectEqual(@as(usize, 1), fixture.counts);
        try std.testing.expectError(error.CypherStateBudgetExceeded, root.runtime.execute(a, client.backend(), definition, &compiled, parameters, .{ .intermediate_states = 10 }));
        try std.testing.expectError(error.CypherAnchorBudgetExceeded, root.runtime.execute(a, client.backend(), definition, &compiled, parameters, .{ .scanned_anchors = 2 }));
        try std.testing.expect(fixture.work == null);
    }
}
test "owner COUNT normalizes mandatory aliases and retains nullable expression semantics" {
    const Case = struct { query: []const u8, expected: i64, pushed: bool };
    for ([_]Case{
        .{ .query = "MATCH(n:D) WHERE n.value >= 8 RETURN count(n)", .expected = 25, .pushed = true },
        .{ .query = "MATCH(n:D {value:99}) RETURN count(n)", .expected = 0, .pushed = true },
        .{ .query = "MATCH(n:D) RETURN count(n.value)", .expected = 33, .pushed = true },
        .{ .query = "MATCH(n:D) RETURN count(DISTINCT n)", .expected = 33, .pushed = false },
        .{ .query = "OPTIONAL MATCH(n:D {value:99}) RETURN count(n)", .expected = 0, .pushed = false },
        .{ .query = "MATCH(n:D) RETURN count(null)", .expected = 0, .pushed = true },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        try fixture.init(a);
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected, result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, if (case.pushed) 1 else 0), fixture.counts);
        if (case.pushed) try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
        try std.testing.expect(fixture.work == null);
    }
}

test "declined distributed COUNT carries physical progress through coordinator RPC fallback" {
    const Wire = struct {
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: *root.rpc.Owner = @ptrCast(@alignCast(ptr));
            return owner.execute(a, body);
        }
    };
    for ([_][]const u8{ "MATCH(n:D) RETURN count(n)", "MATCH(n:D) RETURN count(n.value)", "MATCH(n:D) RETURN count(n.value),count(*)" }) |query| for ([_]usize{ 32, 49 }) |maximum| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixtures = [_]Fixture{ .{ .range_end = 16 }, .{ .range_start = 16, .count_enabled = false } };
        var channels: [2]Fixture.Channel = undefined;
        var clients: [2]root.rpc.Client = undefined;
        var snapshots: [2]root.distributed.Snapshot = undefined;
        for (&fixtures, &channels, &clients, &snapshots) |*fixture, *channel, *client, *snapshot| {
            try fixture.init(a);
            channel.* = .{ .fixture = fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
            client.* = .{ .channel = .{ .ptr = channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
            snapshot.* = .{ .backend = client.backend(), .ptr = fixture, .validate = Fixture.check, .close = undefined };
        }
        const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
        var set: root.distributed.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixtures, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined } };
        var coordinator: root.rpc.Owner = .{ .backend = set.backend(), .graph = definition, .table = "" };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &coordinator, .call = Wire.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const execution = root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{ .scanned_anchors = maximum });
        if (maximum == 32) {
            try std.testing.expectError(error.CypherAnchorBudgetExceeded, execution);
            // The scalar cursor may probe one candidate to distinguish EOF at
            // its ceiling, but the discarded count cannot fund another scan.
            try std.testing.expect(fixtures[0].physical_rows + fixtures[1].physical_rows <= maximum + 1);
        } else {
            var result = try execution;
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 33), result.rows[0][0].integer);
            try std.testing.expectEqual(@as(usize, 49), result.stats.scanned_anchors);
            try std.testing.expectEqual(result.stats.scanned_anchors, fixtures[0].physical_rows + fixtures[1].physical_rows);
        }
        for (fixtures) |fixture| try std.testing.expect(fixture.work == null);
    };
}
test "frontier batches reduce adjacency RPCs and retain path counts" {
    var scalar: usize = 0;
    for ([_][]const u8{ "MATCH(a:D {start:true})-[:R*1..64]->(b:D) RETURN count(*)", "MATCH(a:D {start:true})-[:R*1..]->(b:D) RETURN count(*)" }) |query| for ([_]bool{ false, true }) |batched| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{ .batch_enabled = batched };
        try fixture.init(a);
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 32), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 32), result.stats.explored_edges);
        if (!batched) scalar = fixture.rpc_expands else {
            try std.testing.expect(fixture.bulk > 0);
            try std.testing.expect(fixture.rpc_expands + fixture.bulk < scalar / 2);
        }
    };
}

test "distributed COUNT combines exact owner populations at the retained cut" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixtures = [_]Fixture{ .{ .range_end = 16 }, .{ .range_start = 16 } };
    var channels: [2]Fixture.Channel = undefined;
    var clients: [2]root.rpc.Client = undefined;
    var snapshots: [2]root.distributed.Snapshot = undefined;
    for (&fixtures, &channels, &clients, &snapshots) |*fixture, *channel, *client, *snapshot| {
        try fixture.init(a);
        channel.* = .{ .fixture = fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        client.* = .{ .channel = .{ .ptr = channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        snapshot.* = .{ .backend = client.backend(), .ptr = fixture, .validate = Fixture.check, .close = undefined };
    }
    const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var set: root.distributed.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixtures, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined } };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(n:D) WHERE n.value >= $low RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(a, set.backend(), definition, &compiled, &.{.{ .name = "low", .value = .{ .integer = 8 } }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 25), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 33), result.stats.scanned_anchors);
    for (fixtures) |fixture| {
        try std.testing.expectEqual(@as(usize, 1), fixture.counts);
        try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
    }
}

test "large adjacency payload declines only cache admission and clones typed replies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    try fixture.init(a);
    fixture.edges[1].native = .{ .weight = 7, .created_at = 42, .updated_at = 43 };
    const payload = try a.alloc(u8, 1 << 20);
    @memset(payload, 'x');
    fixture.edges[0].properties = &.{.{ .name = "large", .value = .{ .string = payload } }};
    var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
    var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
    const batch = (try client.backend().vtable.expand_many.?(client.backend().ptr, a, definition.relationships[0], &.{ fixture.nodes[0].identity, fixture.nodes[1].identity }, .out, 1, 128)).?;
    try std.testing.expectEqual(@as(usize, 2), batch.examined);
    try std.testing.expect(batch.pages[0] == null);
    try std.testing.expectEqual(@as(usize, 1), batch.pages[1].?.items.len);
    // The declined edge remains available through ordinary pagination.
    const page = try client.backend().vtable.expand(client.backend().ptr, a, definition.relationships[0], fixture.nodes[0].identity, .out, null, 128);
    try std.testing.expectEqual(@as(usize, payload.len), page.items[0].properties[0].value.string.len);
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const reply = try channel.owner.executeTyped(scratch.allocator(), .{ .operation = .expand_many, .graph_incarnation = 1, .graph_revision = 1, .relationship = "R", .identities = &.{fixture.nodes[1].identity}, .direction = .out, .limit = 1, .max_examined = 128 });
    const owned = try reply.clone(a);
    scratch.deinit();
    scratch = .init(std.testing.allocator);
    try std.testing.expectEqualStrings("n1", owned.batch.?.pages[0].?.values[0].value.relationship.source.key);
    try std.testing.expectEqual(@as(f64, 7), owned.batch.?.pages[0].?.values[0].value.relationship.native.weight);
    try std.testing.expectEqual(@as(u64, 43), owned.batch.?.pages[0].?.values[0].value.relationship.native.updated_at);
    fixture.fail_edges = true;
    try std.testing.expectError(error.CypherEdgeBudgetExceeded, client.backend().vtable.expand_many.?(client.backend().ptr, a, definition.relationships[0], &.{fixture.nodes[1].identity}, .out, 128, 128));
    try std.testing.expect(fixture.work == null);
}

test "local COUNT retains the direct evaluator when no row transport is saved" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    try fixture.init(a);
    var callbacks = fixture.backend().vtable.*;
    callbacks.prefer_count_nodes = false;
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(n:D) RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(a, .{ .ptr = &fixture, .vtable = &callbacks }, definition, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 33), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 0), fixture.counts);
}

test "bound pairs prune peer certificates across RPC orientations and native continuations" {
    const Owner = struct {
        edge: v.Relationship,
        calls: usize = 0,
        pages: bool = false,
        fn check(_: *anyopaque) !void {}
        fn bind(_: *anyopaque, _: A, _: root.catalog.Graph) !void {}
        fn get(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
            return null;
        }
        fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            return error.TestUnexpectedResult;
        }
        fn expand(_: *anyopaque, _: A, _: root.catalog.Relationship, _: v.Identity, _: root.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return error.TestUnexpectedResult;
        }
        fn pair(ptr: *anyopaque, a: A, _: root.catalog.Relationship, id: v.Identity, peer: v.Identity, direction: root.ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, _: usize, _: usize) !b.Page(*const v.Relationship) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const matches = (direction != .in and self.edge.source.eql(id) and self.edge.target.eql(peer)) or
                (direction != .out and self.edge.target.eql(id) and self.edge.source.eql(peer));
            if (!matches or after != null) return .{ .items = &.{}, .examined = 0 };
            return .{ .items = try a.dupe(*const v.Relationship, &.{&self.edge}), .examined = 1, .next = if (self.pages) "native" else null };
        }
        fn wire(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var server: root.rpc.Owner = .{ .backend = .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = check, .get = get, .scan = scan, .expand = expand, .expand_between = pair } }, .graph = definition, .table = "docs" };
            return server.execute(a, body);
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var owners: [8]Owner = undefined;
    var clients: [8]root.rpc.Client = undefined;
    var snapshots: [8]root.distributed.Snapshot = undefined;
    var targets: [8]root.distributed.Target = undefined;
    var routes: [8]b.AdjacencyRoute = undefined;
    const hub: v.Identity = .{ .table = "docs", .key = "hub" };
    const target: v.Identity = .{ .table = "docs", .key = "t7" };
    for (&owners, &clients, &snapshots, &targets, 0..) |*owner, *client, *snapshot, *destination, i| {
        const key = try std.fmt.allocPrint(a, "t{d}", .{i});
        owner.* = .{ .edge = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "e{d}", .{i}) }, .index = "links", .type = "R", .source = hub, .target = .{ .table = "docs", .key = key } } };
        client.* = .{ .channel = .{ .ptr = owner, .call = Owner.wire }, .incarnation = 1, .revision = 1 };
        snapshot.* = .{ .backend = client.backend(), .ptr = owner, .validate = Owner.check, .close = undefined };
        destination.* = .{ .table = "docs", .table_id = 1, .group_id = i + 1 };
    }
    var set: root.distributed.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &owners, .checkpoint = Owner.check, .admit = undefined, .capture = undefined }, .graph = definition };
    const backend = set.backend();
    const Proof = enum { known, unknown_peer, unknown_source, empty, missing };
    for (std.enums.values(Proof)) |proof| for ([_]bool{ false, true }) |pages| for ([_]root.ast.Direction{ .out, .in, .both }) |direction| {
        for (&owners, &snapshots, &routes, 0..) |*owner, *snapshot, *route, i| {
            owner.calls = 0;
            owner.pages = pages;
            route.* = .{ .index = "links", .outgoing = if (proof == .unknown_source) null else .{ .min = "hub", .max = "hub" }, .incoming = if (proof == .unknown_peer) null else if (proof == .empty) .{ .empty = true } else .{ .min = owner.edge.target.key, .max = owner.edge.target.key } };
            snapshot.adjacency_routes = if (proof == .missing) &.{} else routes[i..][0..1];
        }
        var after: ?[]const u8 = null;
        var edges: usize = 0;
        while (true) {
            const page = try backend.vtable.expand_between.?(backend.ptr, a, definition.relationships[0], if (direction == .in) target else hub, if (direction == .in) hub else target, direction, &.{}, after, 1, 4096);
            edges += page.items.len;
            after = page.next orelse break;
        }
        const broadcast = proof == .unknown_peer or proof == .missing;
        const matches: usize = if (proof == .empty) 0 else 1;
        const expected: usize = (if (broadcast) @as(usize, 8) else matches) + if (pages) matches else 0;
        var calls: usize = 0;
        for (owners) |owner| calls += owner.calls;
        try std.testing.expectEqual(expected, calls);
        try std.testing.expectEqual(matches, edges);
    };
    // An undirected pair can be present in both stored orientations. Do not
    // require both orientations to fit, or reverse the retained edge identity.
    owners[6].edge.source = target;
    owners[6].edge.target = hub;
    for (&owners, &snapshots, &routes, 0..) |*owner, *snapshot, *route, i| {
        route.* = .{ .index = "links", .outgoing = .{ .min = owner.edge.source.key, .max = owner.edge.source.key }, .incoming = .{ .min = owner.edge.target.key, .max = owner.edge.target.key } };
        snapshot.adjacency_routes = routes[i..][0..1];
    }
    for ([_]bool{ false, true }) |reversed| for ([_]root.ast.Direction{ .out, .in, .both }) |direction| {
        for (&owners) |*owner| {
            owner.calls = 0;
            owner.pages = true;
        }
        var after: ?[]const u8 = null;
        var edges: usize = 0;
        while (true) {
            const page = try backend.vtable.expand_between.?(backend.ptr, a, definition.relationships[0], if (reversed) target else hub, if (reversed) hub else target, direction, &.{}, after, 1, 4096);
            edges += page.items.len;
            after = page.next orelse break;
        }
        const expected: usize = if (direction == .both) 2 else 1;
        var calls: usize = 0;
        for (owners) |owner| calls += owner.calls;
        try std.testing.expectEqual(expected, edges);
        try std.testing.expectEqual(expected * 2, calls);
    };
}

test "owner COUNT vectors fuse nullable arguments and evaluate residual output once" {
    const Case = struct { query: []const u8, expected: []const i64 };
    for ([_]Case{
        .{ .query = "MATCH(n:D) RETURN count(*),count(n.value),count(n.missing),count(null),count($p)", .expected = &.{ 33, 16, 0, 0, 33 } },
        .{ .query = "MATCH(n:D) RETURN count(n.value)+count(*),coalesce(count(n.missing),99),count(*)*2", .expected = &.{ 49, 0, 66 } },
        .{ .query = "MATCH(n:D) RETURN CASE WHEN count(n.value)=16 THEN count(*) ELSE 1/0 END", .expected = &.{33} },
        .{ .query = "MATCH(n:D {value:99}) RETURN count(n.value)+1,count(*)", .expected = &.{ 1, 0 } },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        try fixture.init(a);
        for (&fixture.properties, 0..) |*props, i| if (i % 2 == 0) {
            props[1].value = .null;
        };
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, client.backend(), definition, &compiled, &.{.{ .name = "p", .value = .{ .integer = 7 } }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqual(case.expected.len, result.rows[0].len);
        for (case.expected, result.rows[0]) |expected, value| try std.testing.expectEqual(expected, value.integer);
        try std.testing.expectEqual(@as(usize, 1), fixture.counts);
        try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
        try std.testing.expectEqual(@as(usize, 33), fixture.physical_rows);
        try std.testing.expectEqual(fixture.physical_rows, result.stats.scanned_anchors);
    }
}

test "owner COUNT vectors reduce across serial and parallel coordinator RPC owners" {
    const Wire = struct {
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: *root.rpc.Owner = @ptrCast(@alignCast(ptr));
            return owner.execute(a, body);
        }
    };
    for ([_]bool{ false, true }) |parallel| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixtures = [_]Fixture{ .{ .range_end = 16 }, .{ .range_start = 16 } };
        var channels: [2]Fixture.Channel = undefined;
        var clients: [2]root.rpc.Client = undefined;
        var snapshots: [2]root.distributed.Snapshot = undefined;
        for (&fixtures, &channels, &clients, &snapshots) |*fixture, *channel, *client, *snapshot| {
            try fixture.init(a);
            for (&fixture.properties, 0..) |*props, i| if (i % 2 == 0) {
                props[1].value = .null;
            };
            channel.* = .{ .fixture = fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
            client.* = .{ .channel = .{ .ptr = channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
            snapshot.* = .{ .backend = client.backend(), .ptr = fixture, .validate = Fixture.check, .close = undefined };
        }
        const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1, .parallel = parallel }, .{ .table = "docs", .table_id = 1, .group_id = 2, .parallel = parallel } };
        var set: root.distributed.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixtures, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined, .fanout_io = std.testing.io, .fanout_width = 2 } };
        var coordinator: root.rpc.Owner = .{ .backend = set.backend(), .graph = definition, .table = "" };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &coordinator, .call = Wire.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, "MATCH(n:D) RETURN count(*),count(n.value),count(n.value)+count(*),count(n.missing)", .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{});
        defer result.deinit();
        for ([_]i64{ 33, 16, 49, 0 }, result.rows[0]) |expected, value| try std.testing.expectEqual(expected, value.integer);
        try std.testing.expectEqual(@as(usize, 33), result.stats.scanned_anchors);
        for (fixtures) |fixture| {
            try std.testing.expectEqual(@as(usize, 1), fixture.counts);
            try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
            try std.testing.expect(fixture.work == null);
        }
    }
}

test "owner COUNT residual failures and unsafe arguments keep evaluation semantics" {
    const Case = struct { query: []const u8, expected: anyerror, pushed: bool };
    for ([_]Case{
        .{ .query = "MATCH(n:D) RETURN count(*)/0", .expected = error.CypherDivisionByZero, .pushed = true },
        .{ .query = "MATCH(n:D) RETURN count(*)+9223372036854775807", .expected = error.CypherNumericOverflow, .pushed = true },
        .{ .query = "MATCH(n:D) RETURN count(*),count(1/n.value)", .expected = error.CypherDivisionByZero, .pushed = false },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        try fixture.init(a);
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(case.expected, root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, if (case.pushed) 1 else 0), fixture.counts);
        if (case.pushed) try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
    }
}

test "owner COUNT nullness projection retains predicate values and declines opaque entities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs", .predicate = &.{.{ .name = "tenant", .value = .{ .string = "acme" } }} }}, .relationships = &.{} };
    const alias: root.ast.Expr = .{ .span = .{}, .value = .{ .variable = "n" } };
    const body: root.ast.Expr = .{ .span = .{}, .value = .{ .property = .{ .base = &alias, .name = "body" } } };
    const tenant: root.ast.Expr = .{ .span = .{}, .value = .{ .property = .{ .base = &alias, .name = "tenant" } } };
    const nested: root.ast.Expr = .{ .span = .{}, .value = .{ .property = .{ .base = &body, .name = "x" } } };
    var input: b.CountInput = .{ .node = .{ .span = .{}, .alias = "n", .labels = &.{"D"}, .properties = &.{} }, .arguments = &.{ null, &body, &tenant } };
    const projection = (try @import("owner_count.zig").Projection.init(a, graph, input)).?;
    try std.testing.expectEqual(@as(usize, 2), projection.fields.len);
    try std.testing.expectEqual(@as(usize, 1), projection.presence.len);
    try std.testing.expectEqualStrings("body", projection.presence[0]);
    input.arguments = &.{&nested};
    try std.testing.expect(try @import("owner_count.zig").Projection.init(a, graph, input) == null);
    input.arguments = &.{&body};
    input.predicate = &tenant;
    try std.testing.expect(try @import("owner_count.zig").Projection.init(a, graph, input) != null);
    input.predicate = &body;
    const observed = (try @import("owner_count.zig").Projection.init(a, graph, input)).?;
    try std.testing.expectEqual(@as(usize, 0), observed.presence.len);
    input.predicate = &alias;
    try std.testing.expect(try @import("owner_count.zig").Projection.init(a, graph, input) == null);
}

test "owner COUNT predicate scopes preserve scalar semantics and native access hints" {
    const Case = struct { suffix: []const u8, expected: usize };
    for ([_]Case{
        .{ .suffix = "WHERE true", .expected = 33 },
        .{ .suffix = "WHERE false", .expected = 0 },
        .{ .suffix = "WHERE null", .expected = 0 },
        .{ .suffix = "WHERE n.value >= 8 AND n.start = true", .expected = 8 },
        .{ .suffix = "WHERE n.value IN [0,1,null]", .expected = 2 },
        .{ .suffix = "WHERE n.missing IS NULL", .expected = 33 },
        .{ .suffix = "WHERE n.value + 1 > 30", .expected = 3 },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        try fixture.init(a);
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, try std.fmt.allocPrint(a, "MATCH(n:D) {s} RETURN count(n.value)", .{case.suffix}), .{}, &diagnostic);
        defer compiled.deinit();
        const operation = compiled.query.clauses[0].operation.match;
        const input: b.CountInput = .{ .node = operation.paths[0].nodes[0], .predicate = operation.predicate };
        _ = (try @import("owner_count.zig").Projection.init(a, definition, input)).?;
        _ = try root.runtime.ownerCountFilters(a, fixture.backend(), definition, input, null);
        var found: usize = 0;
        for (&fixture.nodes) |*node| {
            var row = std.heap.ArenaAllocator.init(a);
            defer row.deinit();
            if (try root.runtime.ownerCountMatches(row.allocator(), fixture.backend(), definition, input, node, null)) found += 1;
        }
        try std.testing.expectEqual(case.expected, found);
    }
}

test "owner COUNT callback remains available below eight MiB" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    try fixture.init(a);
    var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = definition, .table = "docs" } };
    var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(n:D) RETURN count(n.value)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(a, client.backend(), definition, &compiled, &.{}, .{ .memory_bytes = 1 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 33), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 1), fixture.counts);
    try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
}

test "owner COUNT scoped predicates retain scalar failures and constraint order" {
    for ([_][]const u8{
        "MATCH(n:D) WHERE 1/0=0 RETURN count(n.value)",
        "MATCH(n:D) WHERE n.value RETURN count(n.value)",
        "MATCH(n:D {value:1/0}) RETURN count(n.value)",
    }, [_]anyerror{ error.CypherDivisionByZero, error.CypherTypeMismatch, error.CypherDivisionByZero }) |query, failure| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{ .count_enabled = false };
        try fixture.init(a);
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const operation = compiled.query.clauses[0].operation.match;
        const input: b.CountInput = .{ .node = operation.paths[0].nodes[0], .predicate = operation.predicate };
        _ = (try @import("owner_count.zig").Projection.init(a, definition, input)).?;
        try std.testing.expectError(failure, root.runtime.ownerCountMatches(a, fixture.backend(), definition, input, &fixture.nodes[0], null));
        try std.testing.expectError(failure, root.runtime.execute(a, fixture.backend(), definition, &compiled, &.{}, .{}));
    }
}

test "owner COUNT nullness demands merge with actual value consumers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "n.value IS NULL",                     "n.value IS NOT NULL", "NOT (n.value IS NULL)",
        "n.value IS NOT NULL AND n.value > 0", "n.value.x IS NULL",
    }, [_]usize{ 1, 1, 1, 0, 0 }) |predicate, expected| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, try std.fmt.allocPrint(a, "MATCH(n:D) WHERE {s} RETURN count(n.value)", .{predicate}), .{}, &diagnostic);
        defer compiled.deinit();
        const operation = compiled.query.clauses[0].operation.match;
        const projection = (try @import("owner_count.zig").Projection.init(a, definition, .{ .node = operation.paths[0].nodes[0], .predicate = operation.predicate, .arguments = &.{compiled.query.clauses[1].operation.project.value.items[0].expression.value.call.args[0]} })).?;
        try std.testing.expectEqual(expected, projection.presence.len);
    }
}

test "owner COUNT label membership is table local and does not duplicate overlaps" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "D", .table = "docs" },
        .{ .name = "A", .table = "docs", .predicate = &.{.{ .name = "start", .value = .{ .boolean = true } }} },
    }, .relationships = &.{} };
    for ([_][]const u8{ "", ":D:A", ":A:A", ":D" }, [_]i64{ 33, 16, 16, 33 }) |labels, expected| {
        var fixture: Fixture = .{ .graph = graph };
        try fixture.init(a);
        var channel: Fixture.Channel = .{ .fixture = &fixture, .owner = .{ .backend = fixture.backend(), .graph = graph, .table = "docs" } };
        var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, try std.fmt.allocPrint(a, "MATCH(n{s}) RETURN count(n.value)", .{labels}), .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(a, client.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 1), fixture.counts);
        try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
        try std.testing.expectEqual(graph.labels.len, fixture.bound_labels);
        try std.testing.expectEqual(@as(usize, 33), result.stats.scanned_anchors);
    }
}

test "unlabeled owner COUNT reduces unique tables under one anchor budget" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "D", .table = "docs" }, .{ .name = "Alias", .table = "docs" }, .{ .name = "O", .table = "other" },
    }, .relationships = &.{} };
    var fixtures = [_]Fixture{ .{ .graph = graph, .range_end = 16 }, .{ .graph = graph, .range_end = 16 } };
    var channels: [2]Fixture.Channel = undefined;
    var clients: [2]root.rpc.Client = undefined;
    var snapshots: [2]root.distributed.Snapshot = undefined;
    const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "other", .table_id = 2, .group_id = 2 } };
    for (&fixtures, &channels, &clients, &snapshots, targets) |*fixture, *channel, *client, *snapshot, target| {
        try fixture.init(a);
        for (&fixture.nodes) |*node| node.identity.table = target.table;
        channel.* = .{ .fixture = fixture, .owner = .{ .backend = fixture.backend(), .graph = graph, .table = target.table } };
        client.* = .{ .channel = .{ .ptr = channel, .call = Fixture.Channel.invoke }, .incarnation = 1, .revision = 1 };
        snapshot.* = .{ .backend = client.backend(), .ptr = fixture, .validate = Fixture.check, .close = undefined };
    }
    var set: root.distributed.ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixtures, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined } };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(n) RETURN count(n.value)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(a, set.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = 32 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 32), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 32), result.stats.scanned_anchors);
    for (fixtures) |fixture| {
        try std.testing.expectEqual(@as(usize, 1), fixture.counts);
        try std.testing.expectEqual(@as(usize, 0), fixture.rpc_scans);
        try std.testing.expectEqual(graph.labels.len, fixture.bound_labels);
    }
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, root.runtime.execute(a, set.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = 31 }));
}
