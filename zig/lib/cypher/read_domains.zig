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

//! Conservative, parameter-bound physical document-key domains. A bounded
//! domain is licensed by identity anchors and total residual tests on isolated nodes.
//! Every other data access contributes a full-table domain, which dominates
//! all bounded observations of that table across scopes and UNION arms.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const v = @import("value.zig");
const A = std.mem.Allocator;
pub const max_keys = 4096;
pub const TableSet = std.StringHashMapUnmanaged(void);
pub const Options = struct {
    max_work: usize = 65536,
    budget: ?*Budget = null,
    /// Host-selected physical tables eligible for key pruning. Null analyzes
    /// every table; excluded tables are already captured in full by the host.
    prunable_tables: ?*const TableSet = null,
    checkpoint: ?struct { ptr: *const anyopaque, call: *const fn (*const anyopaque) anyerror!void } = null,
};
/// Invocation-wide proof and union work. Exhaustion falls back to full domains;
/// cancellation remains an error and must never be converted to a fallback.
pub const Budget = struct {
    remaining: usize = 65536,
    checkpoint: ?struct { ptr: *const anyopaque, call: *const fn (*const anyopaque) anyerror!void } = null,
    pub fn spend(self: *Budget, amount: usize) !bool {
        if (self.checkpoint) |check| {
            if (amount >= 256 or self.remaining / 256 != (self.remaining -| amount) / 256 or self.remaining == 0) try check.call(check.ptr);
        }
        if (amount > self.remaining) {
            self.remaining = 0;
            return false;
        }
        self.remaining -= amount;
        return true;
    }
};

/// Keys are an unordered physical identity set, never a row ordering promise.
pub const Domain = struct { table: []const u8, keys: ?[]const []const u8 };
pub const Plan = struct {
    domains: []const Domain,
    pub fn table(self: Plan, name: []const u8) ?Domain {
        for (self.domains) |domain| if (std.mem.eql(u8, domain.table, name)) return domain;
        return null;
    }
};
const Aliases = std.StringHashMapUnmanaged(void);
const Bound = struct { safe: bool = true, keys: ?[]const []const u8 = null };
const Entry = struct { full: bool = false, keys: std.StringHashMapUnmanaged(void) = .empty };
const Builder = struct {
    a: A,
    graph: catalog.Graph,
    parameters: []const v.Entry,
    parameter_index: std.StringHashMapUnmanaged(v.Value) = .empty,
    parameters_indexed: bool = false,
    tables: std.StringHashMapUnmanaged(Entry) = .empty,
    writes: bool = false,
    exhausted: bool = false,
    options: Options,
    remaining: usize,
    fn checkpoint(self: *Builder) !void {
        if (self.options.checkpoint) |check| try check.call(check.ptr);
    }
    fn spend(self: *Builder, amount: usize) !bool {
        if (self.exhausted) return false;
        if (self.options.checkpoint) |check| {
            if (self.remaining == self.options.max_work or amount / 256 != 0 or self.remaining / 256 != (self.remaining -| amount) / 256) try check.call(check.ptr);
        }
        if (self.options.budget) |budget| if (!try budget.spend(amount)) {
            self.exhausted = true;
            return false;
        };
        if (amount > self.remaining) {
            self.exhausted = true;
            return false;
        }
        self.remaining -= amount;
        return true;
    }
    fn wanted(self: *Builder, table_name: []const u8) !bool {
        const demand = self.options.prunable_tables orelse return true;
        // Exhaustion installs conservative full domains even if the demand
        // lookup itself cannot finish under the budget.
        if (!try self.spend(1 + table_name.len / 64)) return true;
        return demand.contains(table_name);
    }
    fn nodeWanted(self: *Builder, n: ast.Node) !bool {
        if (self.options.prunable_tables == null) return true;
        if (n.labels.len == 0) {
            for (self.graph.labels) |label| if (try self.wanted(label.table)) return true;
        } else for (n.labels) |name| {
            if (!try self.spend(1 + name.len / 64)) return true;
            if (self.graph.label(name)) |label| if (try self.wanted(label.table)) return true;
        }
        return false;
    }
    fn appendKey(self: *Builder, list: *std.ArrayList([]const u8), key: []const u8) !bool {
        // Appending borrows a span; hashing its bytes is charged by add().
        if (!try self.spend(1)) return false;
        try list.append(self.a, key);
        return true;
    }
    fn add(self: *Builder, table_name: []const u8, keys: ?[]const []const u8) !void {
        if (!try self.wanted(table_name)) return;
        if (!self.exhausted) _ = try self.spend(1 + table_name.len / 64);
        // Full domains must still be installed after exhaustion, so the
        // conservative fallback cannot retain partially proved key sets.
        if (keys != null and self.exhausted) return;
        const slot = try self.tables.getOrPut(self.a, table_name);
        if (!slot.found_existing) slot.value_ptr.* = .{};
        if (keys == null) {
            slot.value_ptr.full = true;
            return;
        }
        if (slot.value_ptr.full) return;
        for (keys.?) |key| {
            if (!try self.spend(1 + key.len / 64)) return;
            try slot.value_ptr.keys.put(self.a, key, {});
            if (slot.value_ptr.keys.count() > max_keys) {
                slot.value_ptr.full = true;
                return;
            }
        }
    }
    fn all(self: *Builder) !void {
        try self.checkpoint();
        for (self.graph.labels) |label| try self.add(label.table, null);
        try self.checkpoint();
    }
    fn addUnique(self: *Builder, seen: *Aliases, table: []const u8, keys: ?[]const []const u8) !void {
        if (!try self.spend(1 + table.len / 64)) return;
        const slot = try seen.getOrPut(self.a, table);
        if (!slot.found_existing) try self.add(table, keys);
    }
    fn node(self: *Builder, n: ast.Node, predicate: ?*const ast.Expr, aliases: Aliases) !void {
        if (!try self.nodeWanted(n)) return;
        const literal_properties = for (n.properties) |property| {
            if (!try self.spend(1) or (try self.constant(property.value)) == null) break false;
        } else true;
        const bound: Bound = if (n.alias != null and literal_properties and predicate != null)
            try self.predicateDomain(predicate.?, n.alias.?, aliases)
        else
            .{ .safe = false };
        const keys = if (bound.safe) bound.keys else null;
        if (n.labels.len == 1) {
            if (!try self.spend(1 + n.labels[0].len / 64)) return;
            if (self.graph.label(n.labels[0])) |label| try self.add(label.table, keys);
        } else if (n.labels.len == 0 and self.graph.labels.len == 1) {
            try self.add(self.graph.labels[0].table, keys);
        } else {
            // Several labels can map to the same physical table. The same
            // node observation must not rehash its whole key set per label.
            var seen: Aliases = .empty;
            if (n.labels.len == 0) {
                for (self.graph.labels) |label| try self.addUnique(&seen, label.table, keys);
            } else for (n.labels) |name| {
                if (!try self.spend(1 + name.len / 64)) return;
                if (self.graph.label(name)) |label| try self.addUnique(&seen, label.table, keys);
            }
        }
    }
    fn path(self: *Builder, p: ast.Path, predicate: ?*const ast.Expr, aliases: Aliases) !void {
        // Traversals can fetch unknown endpoints from either orientation.
        for (p.nodes) |n| {
            if (!try self.spend(1)) return;
            try self.node(n, if (p.edges.len == 0) predicate else null, aliases);
        }
        for (p.edges) |edge| {
            for (self.graph.relationships) |mapping| {
                if (!try self.spend(1)) return;
                const selected = edge.types.len == 0 or for (edge.types) |name| {
                    if (std.mem.eql(u8, name, mapping.name)) break true;
                } else false;
                if (selected) {
                    if (self.graph.label(mapping.source_label)) |label| try self.add(label.table, null);
                    if (self.graph.label(mapping.target_label)) |label| try self.add(label.table, null);
                }
            }
            try self.walk(edge.properties);
        }
        for (p.nodes) |n| try self.walk(n.properties);
    }
    fn clauses(self: *Builder, list: []const ast.Clause) !void {
        for (list) |clause| {
            if (!try self.spend(1)) return;
            switch (clause.operation) {
                .match => |m| {
                    var aliases: Aliases = .empty;
                    for (m.paths) |p| {
                        for (p.nodes) |n| {
                            if (!try self.spend(1)) return;
                            if (n.alias) |name| {
                                if (!try self.spend(1 + name.len / 64)) return;
                                try aliases.put(self.a, name, {});
                            }
                        }
                    }
                    for (m.paths) |p| try self.path(p, m.predicate, aliases);
                    try self.walk(m.predicate);
                },
                .project => |p| try self.walk(p.value),
                .unwind => |u| try self.walk(u.expression),
                .subquery => |nested| try self.clauses(nested),
                .call => {
                    try self.all();
                },
                .create, .merge, .set, .remove, .delete => {
                    self.writes = true;
                },
            }
        }
    }
    fn walk(self: *Builder, value: anytype) anyerror!void {
        if (!try self.spend(1)) return;
        const T = @TypeOf(value);
        if (T == ast.Expr) {
            switch (value.value) {
                .exists => |nested| {
                    try self.clauses(nested);
                    return;
                },
                .pattern_comprehension => |p| {
                    var aliases: Aliases = .empty;
                    for (p.path.nodes) |n| if (n.alias) |name| {
                        if (!try self.spend(1 + name.len / 64)) return;
                        try aliases.put(self.a, name, {});
                    };
                    try self.path(p.path, p.predicate, aliases);
                    try self.walk(p.predicate);
                    try self.walk(p.projection);
                    return;
                },
                .literal, .variable, .parameter => return,
                else => {
                    // Only semantic expression children can introduce reads.
                    // Source spans and physical annotations need no traversal.
                    try self.walk(value.value);
                    return;
                },
            }
        }
        switch (@typeInfo(T)) {
            .@"struct" => |s| inline for (s.field_names) |name| {
                try self.walk(@field(value, name));
            },
            .@"union" => |u| if (u.tag_type != null) {
                inline for (u.field_names) |name| {
                    if (std.mem.eql(u8, @tagName(value), name)) try self.walk(@field(value, name));
                }
            },
            .optional => if (value) |child| {
                try self.walk(child);
            },
            .pointer => |p| {
                if (p.size == .one and p.child == ast.Expr) try self.walk(value.*) else if (p.size == .slice and p.child != u8) for (value) |child| {
                    if (self.exhausted) break;
                    try self.walk(child);
                };
            },
            else => {},
        }
    }
    fn constant(self: *Builder, expression: *const ast.Expr) !?v.Value {
        if (!try self.spend(1)) return null;
        return switch (expression.value) {
            .literal => |lit| switch (lit) {
                .string => |x| .{ .string = x },
                .null => .null,
                .boolean => |x| .{ .boolean = x },
                .integer => |x| .{ .integer = x },
                .number => |x| .{ .number = x },
            },
            .parameter => |name| blk: {
                if (!self.parameters_indexed) {
                    // Preserve first-binding semantics. Index once in the
                    // invocation arena, charging names before hashing them.
                    for (self.parameters) |parameter| {
                        if (!try self.spend(1 + (std.math.divCeil(usize, parameter.name.len, 64) catch unreachable))) break :blk null;
                        const entry = try self.parameter_index.getOrPut(self.a, parameter.name);
                        if (!entry.found_existing) entry.value_ptr.* = parameter.value;
                    }
                    self.parameters_indexed = true;
                }
                if (!try self.spend(1 + (std.math.divCeil(usize, name.len, 64) catch unreachable))) break :blk null;
                break :blk self.parameter_index.get(name);
            },
            else => null,
        };
    }
    fn nodeAlias(self: *Builder, name: []const u8, aliases: Aliases) !bool {
        if (!try self.spend(1 + name.len / 64)) return false;
        return aliases.contains(name);
    }
    fn keyAlias(self: *Builder, expression: *const ast.Expr, aliases: Aliases) !?[]const u8 {
        if (expression.value != .call) return null;
        const call = expression.value.call;
        if (!std.ascii.eqlIgnoreCase(call.name, "antfly.documentKey") or call.args.len != 1 or call.args[0].value != .variable) return null;
        const name = call.args[0].value.variable;
        return if (try self.nodeAlias(name, aliases)) name else null;
    }
    fn constantKeys(self: *Builder, expression: *const ast.Expr, membership: bool) !?[]const []const u8 {
        var result: std.ArrayList([]const u8) = .empty;
        if (membership and expression.value == .list) {
            if (expression.value.list.len > max_keys) return null;
            for (expression.value.list) |item| {
                const value = (try self.constant(item)) orelse return null;
                if (value == .null) continue;
                if (value != .string) return null;
                if (!try self.appendKey(&result, value.string)) return null;
            }
        } else {
            const value = (try self.constant(expression)) orelse return null;
            if (value == .null) return &.{};
            if (!membership) {
                if (value != .string) return null;
                if (!try self.appendKey(&result, value.string)) return null;
            } else {
                if (value != .list or value.list.len > max_keys) return null;
                for (value.list) |item| {
                    if (item == .null) continue;
                    if (item != .string) return null;
                    if (!try self.appendKey(&result, item.string)) return null;
                }
            }
        }
        return result.items;
    }
    fn atom(self: *Builder, op: ast.Binary, left: *const ast.Expr, right: *const ast.Expr, alias: []const u8, aliases: Aliases) !Bound {
        if (op != .eq and op != .in) return .{ .safe = false };
        const left_alias = try self.keyAlias(left, aliases);
        const right_alias = if (op == .eq) try self.keyAlias(right, aliases) else null;
        const selected = left_alias orelse right_alias orelse return .{ .safe = false };
        const rhs = if (left_alias != null) right else left;
        const domain = (try self.constantKeys(rhs, op == .in)) orelse return .{ .safe = false };
        return .{ .keys = if (std.mem.eql(u8, selected, alias)) domain else null };
    }
    fn simpleData(self: *Builder, e: *const ast.Expr, aliases: Aliases) anyerror!bool {
        if (!try self.spend(1)) return false;
        if (try self.constant(e) != null) return true;
        return switch (e.value) {
            .property => |p| p.base.value == .variable and try self.nodeAlias(p.base.value.variable, aliases),
            .call => (try self.keyAlias(e, aliases)) != null,
            else => false,
        };
    }
    // Only total predicates may accompany a key anchor. Unknown arithmetic,
    // calls and constructors could otherwise hide errors on a scan fallback.
    fn totalTest(self: *Builder, e: *const ast.Expr, aliases: Aliases) anyerror!bool {
        if (!try self.spend(1)) return false;
        return switch (e.value) {
            .literal => |lit| lit == .boolean or lit == .null,
            .unary => |u| switch (u.op) {
                .is_null, .is_not_null => self.simpleData(u.operand, aliases),
                .not => self.totalTest(u.operand, aliases),
                else => false,
            },
            .binary => |b| switch (b.op) {
                .eq, .neq, .lt, .lte, .gt, .gte => try self.simpleData(b.left, aliases) and try self.simpleData(b.right, aliases),
                .@"and", .@"or", .xor => try self.totalTest(b.left, aliases) and try self.totalTest(b.right, aliases),
                else => false,
            },
            .comparison => |c| blk: {
                for (c.operators) |op| switch (op) {
                    .eq, .neq, .lt, .lte, .gt, .gte => {},
                    else => break :blk false,
                };
                for (c.operands) |item| if (!try self.simpleData(item, aliases)) break :blk false;
                break :blk true;
            },
            else => false,
        };
    }
    fn predicateDomain(self: *Builder, e: *const ast.Expr, alias: []const u8, aliases: Aliases) anyerror!Bound {
        if (!try self.spend(1)) return .{ .safe = false };
        switch (e.value) {
            .literal => |value| switch (value) {
                .boolean => |x| return .{ .keys = if (x) null else &.{} },
                .null => return .{ .keys = &.{} },
                else => {},
            },
            .comparison => |c| if (c.operators.len == 1 and c.operands.len == 2) {
                if ((c.operators[0] == .eq or c.operators[0] == .in) and ((try self.keyAlias(c.operands[0], aliases)) != null or (try self.keyAlias(c.operands[1], aliases)) != null))
                    return self.atom(c.operators[0], c.operands[0], c.operands[1], alias, aliases);
            },
            .binary => |b| {
                if (b.op != .@"and" and b.op != .@"or") {
                    if ((b.op == .eq or b.op == .in) and ((try self.keyAlias(b.left, aliases)) != null or (try self.keyAlias(b.right, aliases)) != null)) return self.atom(b.op, b.left, b.right, alias, aliases);
                    return .{ .safe = try self.totalTest(e, aliases) };
                }
                const l = try self.predicateDomain(b.left, alias, aliases);
                const r = try self.predicateDomain(b.right, alias, aliases);
                if (!l.safe or !r.safe) return .{ .safe = false };
                if (b.op == .@"or" and (l.keys == null or r.keys == null)) return .{};
                if (b.op == .@"and") {
                    if (l.keys == null) return r;
                    if (r.keys == null) return l;
                }
                // Stream and materialized plans can choose different conjuncts.
                // Keep every possible candidate key, not just the intersection.
                if (l.keys.?.len + r.keys.?.len > max_keys) return .{ .safe = false };
                if (!try self.spend(l.keys.?.len + r.keys.?.len)) return .{ .safe = false };
                var result: std.ArrayList([]const u8) = .empty;
                try result.appendSlice(self.a, l.keys.?);
                try result.appendSlice(self.a, r.keys.?);
                return .{ .keys = result.items };
            },
            else => {},
        }
        return .{ .safe = try self.totalTest(e, aliases) };
    }
};
/// The invocation-arena result borrows graph and parameter strings. It must
/// never be stored in a prepared query or reused with different bindings.
pub fn plan(a: A, graph: catalog.Graph, query: ast.Query, parameters: []const v.Entry, options: Options) !Plan {
    if (options.prunable_tables) |demand| if (demand.count() == 0) {
        if (options.checkpoint) |check| try check.call(check.ptr);
        return .{ .domains = &.{} };
    };
    var builder: Builder = .{ .a = a, .graph = graph, .parameters = parameters, .options = options, .remaining = options.max_work };
    try builder.clauses(query.clauses);
    for (query.unions) |arm| try builder.clauses(arm.clauses);
    if (builder.exhausted) try builder.all();
    const result = try a.alloc(Domain, builder.tables.count());
    var entries = builder.tables.iterator();
    var index: usize = 0;
    while (entries.next()) |entry| : (index += 1) {
        var names: std.ArrayList([]const u8) = .empty;
        if (!entry.value_ptr.full and !builder.writes) {
            var keys = entry.value_ptr.keys.keyIterator();
            while (keys.next()) |name| {
                if (!try builder.spend(1)) break;
                try names.append(a, name.*);
            }
        }
        // Empty domains conservatively retain a cohort: the runtime still owns
        // empty-input and error semantics, including aggregate defaults.
        result[index] = .{ .table = entry.key_ptr.*, .keys = if (entry.value_ptr.full or builder.writes or names.items.len == 0) null else names.items };
    }
    if (builder.exhausted) for (result) |*domain| {
        domain.keys = null;
    };
    try builder.checkpoint();
    return .{ .domains = result };
}

test "point domains compose identity predicates and conservatively retain unknown reads" {
    const compiler = @import("compiler.zig");
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "D", .table = "docs" }, .{ .name = "D2", .table = "docs" }, .{ .name = "O", .table = "other" } }, .relationships = &.{.{ .name = "R", .source_label = "D", .target_label = "O", .index = "g", .stored_type = "r" }} };
    const Case = struct { text: []const u8, keys: ?[]const []const u8 };
    for ([_]Case{
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='b' RETURN n", .keys = &.{"b"} },
        .{ .text = "MATCH(n:D) WHERE 'b'=antfly.documentKey(n) RETURN n", .keys = &.{"b"} },
        .{ .text = "OPTIONAL MATCH(n:D) WHERE antfly.documentKey(n) IN ['b','a','b',null] RETURN count(n)", .keys = &.{ "a", "b" } },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n) IN ['a','b'] AND antfly.documentKey(n)='b' RETURN n", .keys = &.{ "a", "b" } },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='b' OR antfly.documentKey(n)='a' RETURN n", .keys = &.{ "a", "b" } },
        .{ .text = "MATCH(n:D),(m:D2) WHERE antfly.documentKey(n)='a' AND antfly.documentKey(m)='b' RETURN n,m", .keys = &.{ "a", "b" } },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' RETURN n UNION MATCH(n:D) WHERE antfly.documentKey(n)='b' RETURN n", .keys = &.{ "a", "b" } },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' RETURN n UNION MATCH(n:D) RETURN n", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' AND n.x=1 RETURN n", .keys = &.{"a"} },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' AND n.x/0>1 RETURN n", .keys = null },
        .{ .text = "MATCH(n:D {x:1/0}) WHERE antfly.documentKey(n)='a' RETURN n", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' OR true RETURN n", .keys = null },
        .{ .text = "MATCH(n:D {x:1}) WHERE antfly.documentKey(n)='a' RETURN n", .keys = &.{"a"} },
        .{ .text = "MATCH(n:D)-[:R]->(o:O) WHERE antfly.documentKey(n)='a' RETURN n", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' SET n.x=1 RETURN n", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' RETURN [(m:D)-[:R]->(o:O)|m]", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)='a' RETURN EXISTS {MATCH(m:D) RETURN m}", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n) IN [] RETURN count(n)", .keys = null },
        .{ .text = "MATCH(n:D) WHERE antfly.documentKey(n)=$missing RETURN n", .keys = null },
    }) |case| {
        var diagnostic: compiler.Diagnostic = .{};
        var compiled = compiler.compile(std.testing.allocator, case.text, .{}, &diagnostic) catch |err| {
            std.debug.print("domain syntax case: {s}\n", .{case.text});
            return err;
        };
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const result = try plan(arena.allocator(), graph, compiled.query, &.{}, .{});
        const actual = result.table("docs").?.keys;
        errdefer std.debug.print("domain case failed: {s}\n", .{case.text});
        try std.testing.expectEqual(case.keys == null, actual == null);
        if (case.keys) |expected| {
            try std.testing.expectEqual(expected.len, actual.?.len);
            for (expected) |key| try std.testing.expect(for (actual.?) |found| {
                if (std.mem.eql(u8, key, found)) break true;
            } else false);
        }
    }
}

test "point domains bind parameters per invocation and bound planning work without changing query semantics" {
    const compiler = @import("compiler.zig");
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(std.testing.allocator, "MATCH(n:D) WHERE antfly.documentKey(n) IN $keys RETURN n", .{}, &diagnostic);
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for ([_][]const u8{ "first", "second" }) |key| {
        const parameters: []const v.Entry = &.{.{ .name = "keys", .value = .{ .list = &.{ .{ .string = key }, .null } } }};
        const result = try plan(arena.allocator(), graph, compiled.query, parameters, .{});
        try std.testing.expectEqualStrings(key, result.table("docs").?.keys.?[0]);
        const declined = try plan(arena.allocator(), graph, compiled.query, parameters, .{ .max_work = 0 });
        try std.testing.expect(declined.table("docs").?.keys == null);
    }
    const Guard = struct {
        fn canceled(_: *const anyopaque) !void {
            return error.Canceled;
        }
    };
    var marker: u8 = 0;
    try std.testing.expectError(error.Canceled, plan(arena.allocator(), graph, compiled.query, &.{}, .{ .checkpoint = .{ .ptr = &marker, .call = Guard.canceled } }));
}

test "point parameter proofs index bindings once and meter indexing before lookup" {
    const compiler = @import("compiler.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parameters = try a.alloc(v.Entry, 10000);
    for (parameters, 0..) |*parameter, index| {
        const name = try a.alloc(u8, 64);
        @memset(name, 'x');
        _ = try std.fmt.bufPrint(name[56..64], "{d:0>8}", .{index});
        parameter.* = .{ .name = name, .value = .{ .string = "a" } };
    }
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    var source: std.ArrayList(u8) = .empty;
    try source.appendSlice(a, "MATCH(n:D) WHERE antfly.documentKey(n) IN [");
    for (0..4096) |index| {
        if (index != 0) try source.append(a, ',');
        try source.append(a, '$');
        try source.appendSlice(a, parameters[9999].name);
    }
    try source.appendSlice(a, "] RETURN n");
    var diagnostic: compiler.Diagnostic = .{};
    var repeated = try compiler.compile(std.testing.allocator, source.items, .{}, &diagnostic);
    defer repeated.deinit();
    const result = try plan(a, graph, repeated.query, parameters, .{});
    try std.testing.expectEqual(@as(usize, 1), result.table("docs").?.keys.?.len);
    try std.testing.expectEqualStrings("a", result.table("docs").?.keys.?[0]);
    const simple = try std.fmt.allocPrint(a, "MATCH(n:D) WHERE antfly.documentKey(n)=${s} RETURN n", .{parameters[9999].name});
    var single = try compiler.compile(std.testing.allocator, simple, .{}, &diagnostic);
    defer single.deinit();
    // A tiny proof cannot scan thousands of irrelevant bindings for free.
    const bounded = try plan(a, graph, single.query, parameters, .{ .max_work = 256 });
    try std.testing.expect(bounded.table("docs").?.keys == null);
    const Guard = struct {
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 3) return error.Canceled;
        }
    };
    var guard: Guard = .{};
    try std.testing.expectError(error.Canceled, plan(a, graph, single.query, parameters, .{ .checkpoint = .{ .ptr = &guard, .call = Guard.check } }));
    try std.testing.expectEqual(@as(usize, 3), guard.calls);
    // Duplicate bindings retain the existing first-binding behavior.
    parameters[0].name = parameters[9999].name;
    parameters[0].value = .{ .string = "first" };
    const duplicate = try plan(a, graph, single.query, parameters, .{});
    try std.testing.expectEqualStrings("first", duplicate.table("docs").?.keys.?[0]);
}

test "point domain materialization meters physical fanout and cancels key hashing" {
    const compiler = @import("compiler.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const labels = try a.alloc(catalog.Label, 64);
    for (labels, 0..) |*label, i| label.* = .{ .name = try std.fmt.allocPrint(a, "D{d}", .{i}), .table = "docs" };
    const values = try a.alloc(v.Value, max_keys);
    const keys = try a.alloc([]const u8, max_keys);
    for (values, keys, 0..) |*value, *key, i| {
        const bytes = try a.alloc(u8, 224);
        @memset(bytes, 'x');
        _ = try std.fmt.bufPrint(bytes[216..], "{d:0>8}", .{i});
        key.* = bytes;
        value.* = .{ .string = bytes };
    }
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = labels, .relationships = &.{} };
    try graph.validate();
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(a, "MATCH(n) WHERE antfly.documentKey(n) IN $keys RETURN n", .{}, &diagnostic);
    defer compiled.deinit();
    const parameters: []const v.Entry = &.{.{ .name = "keys", .value = .{ .list = values } }};
    const bounded = try plan(a, graph, compiled.query, parameters, .{ .max_work = 17000 });
    try std.testing.expect(bounded.table("docs").?.keys == null);
    // Alias labels must not multiply the same physical observation's hashing.
    const complete = try plan(a, graph, compiled.query, parameters, .{});
    try std.testing.expectEqual(@as(usize, max_keys), complete.table("docs").?.keys.?.len);
    for (labels, 0..) |*label, i| label.table = try std.fmt.allocPrint(a, "table{d}", .{i});
    const fanout = try plan(a, graph, compiled.query, parameters, .{});
    for (fanout.domains) |domain| try std.testing.expect(domain.keys == null);
    const Guard = struct {
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 2) return error.Canceled;
        }
    };
    var guard: Guard = .{};
    var builder: Builder = .{ .a = a, .graph = graph, .parameters = parameters, .remaining = 65536, .options = .{ .checkpoint = .{ .ptr = &guard, .call = Guard.check } } };
    try std.testing.expectError(error.Canceled, builder.add("docs", keys));
    try std.testing.expectEqual(@as(usize, 2), guard.calls);
}

test "point domains share one invocation budget across plans and union work" {
    const compiler = @import("compiler.zig");
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(std.testing.allocator, "MATCH(n:D) WHERE antfly.documentKey(n)=$key RETURN n", .{}, &diagnostic);
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    const parameters = &.{v.Entry{ .name = "key", .value = .{ .string = "a" } }};
    var budget: Budget = .{};
    const first = try plan(arena.allocator(), graph, compiled.query, parameters, .{ .budget = &budget });
    try std.testing.expect(first.table("docs").?.keys != null);
    const used = 65536 - budget.remaining;
    budget.remaining = used - 1;
    const second = try plan(arena.allocator(), graph, compiled.query, parameters, .{ .budget = &budget });
    try std.testing.expect(second.table("docs").?.keys == null);
    try std.testing.expectEqual(@as(usize, 0), budget.remaining);
    try std.testing.expect(!try budget.spend(1));
}

test "point domains restrict proof work to physically prunable tables" {
    const compiler = @import("compiler.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "D", .table = "docs" }, .{ .name = "O", .table = "other" } }, .relationships = &.{} };
    const values = try a.alloc(v.Value, max_keys);
    for (values, 0..) |*value, i| value.* = .{ .string = try std.fmt.allocPrint(a, "{d:0>224}", .{i}) };
    const parameters = &.{v.Entry{ .name = "keys", .value = .{ .list = values } }};
    var demand: TableSet = .empty;
    try demand.put(a, "docs", {});
    var diagnostic: compiler.Diagnostic = .{};
    for ([_][]const u8{
        "MATCH(n:D),(o:O) WHERE antfly.documentKey(n) IN $keys AND antfly.documentKey(o) IN $keys RETURN n",
        "MATCH(o:O) RETURN EXISTS {MATCH(n:D) WHERE antfly.documentKey(n) IN $keys RETURN n}",
    }) |text| {
        var compiled = try compiler.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var budget: Budget = .{};
        const result = try plan(a, graph, compiled.query, parameters, .{ .budget = &budget, .prunable_tables = &demand });
        try std.testing.expect(result.table("other") == null);
        try std.testing.expectEqual(@as(usize, max_keys), result.table("docs").?.keys.?.len);
        // The host can still hash and materialize the entire surviving keyset.
        try std.testing.expect(budget.remaining >= max_keys * 5);
        budget.remaining = 0;
        const fallback = try plan(a, graph, compiled.query, parameters, .{ .budget = &budget, .prunable_tables = &demand });
        try std.testing.expect(fallback.table("docs").?.keys == null);
    }
    var excluded = try compiler.compile(a, "MATCH(o:O) WHERE antfly.documentKey(o) IN $keys RETURN o", .{}, &diagnostic);
    defer excluded.deinit();
    var budget: Budget = .{};
    const result = try plan(a, graph, excluded.query, parameters, .{ .budget = &budget, .prunable_tables = &demand });
    try std.testing.expectEqual(@as(usize, 0), result.domains.len);
    try std.testing.expect(budget.remaining > 65000);
    var empty: TableSet = .empty;
    const skipped = try plan(a, graph, excluded.query, parameters, .{ .budget = &budget, .prunable_tables = &empty });
    try std.testing.expectEqual(@as(usize, 0), skipped.domains.len);
}
