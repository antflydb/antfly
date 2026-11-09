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

//! Invocation-local property demand for forwarded entity bindings. Equality
//! always keeps the complete graph identity; only unobserved content is pruned.
const std = @import("std");
const ast = @import("ast.zig");
const expressions = @import("expressions.zig");
const A = std.mem.Allocator;
pub const Fields = std.StringHashMapUnmanaged(void);
pub const Plans = std.AutoHashMapUnmanaged(*const ast.Expr, Fields);
const Names = std.StringHashMapUnmanaged(usize);
const Binding = struct { expression: *const ast.Expr, parent: ?usize, leader: usize, full: bool = false, fields: Fields = .empty };
const Flow = struct {
    a: A,
    source: []const u8,
    bindings: std.ArrayList(Binding) = .empty,
    fn lookup(names: Names, name: []const u8, locals: ?*const expressions.Local) ?usize {
        var current = locals;
        while (current) |local| : (current = local.parent) if (std.mem.eql(u8, local.name, name)) return null;
        return names.get(name);
    }
    fn barrier(self: *@This(), names: Names) void {
        var ids = names.valueIterator();
        while (ids.next()) |id| self.bindings.items[id.*].full = true;
    }
    fn scan(self: *@This(), value: anytype, names: Names, locals: ?*const expressions.Local) anyerror!void {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) {
            switch (value.value) {
                .variable => |name| if (lookup(names, name, locals)) |id| {
                    self.bindings.items[id].full = true;
                },
                .property => |p| {
                    if (p.base.value == .variable) if (lookup(names, p.base.value.variable, locals)) |id| {
                        try self.bindings.items[id].fields.put(self.a, p.name, {});
                        return;
                    };
                    try self.scan(p.base, names, locals);
                },
                .call => |call| {
                    const identity = std.ascii.eqlIgnoreCase(call.name, "count") or std.ascii.eqlIgnoreCase(call.name, "labels") or std.ascii.eqlIgnoreCase(call.name, "type") or std.ascii.eqlIgnoreCase(call.name, "antfly.documentKey");
                    for (call.args) |arg| {
                        if (identity and arg.value == .variable) continue;
                        try self.scan(arg, names, locals);
                    }
                },
                .exists, .pattern_comprehension => self.barrier(names),
                .comprehension => |c| {
                    try self.scan(c.input, names, locals);
                    const local: expressions.Local = .{ .name = c.alias, .parent = locals };
                    try self.scan(c.predicate, names, &local);
                    try self.scan(c.projection, names, &local);
                },
                .list_predicate => |c| {
                    try self.scan(c.input, names, locals);
                    const local: expressions.Local = .{ .name = c.alias, .parent = locals };
                    try self.scan(c.predicate, names, &local);
                },
                .reduce => |r| {
                    try self.scan(r.initial, names, locals);
                    try self.scan(r.input, names, locals);
                    const alias: expressions.Local = .{ .name = r.alias, .parent = locals };
                    const accumulator: expressions.Local = .{ .name = r.accumulator, .parent = &alias };
                    try self.scan(r.projection, names, &accumulator);
                },
                inline else => |payload| try self.scan(payload, names, locals),
            }
            return;
        }
        // Map projections and patterns can observe an entity beyond property
        // Exprs. Preserve complete content at these opaque capture boundaries.
        if (T == ast.MapSelector) if (value == .variable) if (lookup(names, value.variable, locals)) |id| {
            self.bindings.items[id].full = true;
        };
        switch (@typeInfo(T)) {
            .pointer => |p| if (p.size == .slice and p.child != u8) {
                for (value) |child| try self.scan(child, names, locals);
            },
            .@"struct" => |info| inline for (info.field_names) |name| try self.scan(@field(value, name), names, locals),
            .@"union" => switch (value) {
                inline else => |child| try self.scan(child, names, locals),
            },
            .optional => if (value) |child| try self.scan(child, names, locals),
            else => {},
        }
    }
    fn analyze(self: *@This(), clauses: []const ast.Clause, plans: *Plans) !void {
        var names: Names = .empty;
        for (clauses) |clause| switch (clause.operation) {
            .project => |p| {
                var next: Names = .empty;
                var forwarded: Names = .empty;
                var visible = try names.clone(self.a);
                if (p.value.wildcard) self.barrier(names);
                for (p.value.items) |item| {
                    const output = item.alias orelse try expressions.outputName(self.a, self.source, item.expression);
                    _ = visible.remove(output);
                    if (!p.final and !p.value.wildcard and item.expression.value == .variable) {
                        const id = self.bindings.items.len;
                        const input = item.expression.value.variable;
                        const leader = forwarded.get(input) orelse id;
                        try self.bindings.append(self.a, .{ .expression = item.expression, .parent = names.get(input), .leader = leader });
                        try forwarded.put(self.a, input, leader);
                        try next.put(self.a, output, id);
                    } else try self.scan(item.expression, names, null);
                }
                var iterator = next.iterator();
                while (iterator.next()) |entry| try visible.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                // DISTINCT may resolve an ORDER expression against an already
                // projected expression with aliases shadowing its input. Keeping
                // full incoming bindings is conservative across that boundary.
                if (p.value.distinct) {
                    self.barrier(names);
                    self.barrier(next);
                }
                try self.scan(p.value.order, visible, null);
                try self.scan(p.value.predicate, visible, null);
                names = next;
            },
            .unwind => |u| {
                try self.scan(u.expression, names, null);
                _ = names.remove(u.alias);
            },
            else => {
                self.barrier(names);
                names = .empty;
            },
        };
        var i = self.bindings.items.len;
        while (i != 0) {
            i -= 1;
            const binding = self.bindings.items[i];
            // Structurally identical grouping expressions share one key slot.
            // Merge alias demands before propagating that slot to its parent.
            if (binding.leader != i) {
                self.bindings.items[binding.leader].full = self.bindings.items[binding.leader].full or binding.full;
                var fields = binding.fields.keyIterator();
                while (fields.next()) |field| try self.bindings.items[binding.leader].fields.put(self.a, field.*, {});
                continue;
            }
            if (binding.parent) |parent| {
                self.bindings.items[parent].full = self.bindings.items[parent].full or binding.full;
                var fields = binding.fields.keyIterator();
                while (fields.next()) |field| try self.bindings.items[parent].fields.put(self.a, field.*, {});
            }
        }
        for (self.bindings.items) |binding| {
            const demand = self.bindings.items[binding.leader];
            if (!demand.full) try plans.put(self.a, binding.expression, demand.fields);
        }
    }
};
pub fn plan(a: A, source: []const u8, value: anytype, plans: *Plans) anyerror!void {
    const T = @TypeOf(value);
    if (T == []const ast.Clause) {
        var flow: Flow = .{ .a = a, .source = source };
        try flow.analyze(value, plans);
    }
    switch (@typeInfo(T)) {
        .pointer => |p| {
            if (p.size == .one and p.child == ast.Expr) try plan(a, source, value.*, plans);
            if (p.size == .slice and p.child != u8) for (value) |child| try plan(a, source, child, plans);
        },
        .@"struct" => |info| inline for (info.field_names) |name| try plan(a, source, @field(value, name), plans),
        .@"union" => switch (value) {
            inline else => |child| try plan(a, source, child, plans),
        },
        .optional => if (value) |child| try plan(a, source, child, plans),
        else => {},
    }
}

test "forwarded bindings keep stage-local fields and conservative full escapes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("compiler.zig");
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(a, "MATCH(n:L) WITH n,min(n.payload) AS p WITH n AS m,p RETURN m.score,size(p)", .{}, &diagnostic);
    defer compiled.deinit();
    var plans: Plans = .empty;
    try plan(a, compiled.query.source, compiled.query, &plans);
    const first = compiled.query.clauses[1].operation.project.value.items[0].expression;
    const second = compiled.query.clauses[2].operation.project.value.items[0].expression;
    try std.testing.expect(plans.get(first).?.contains("score"));
    try std.testing.expect(!plans.get(first).?.contains("payload"));
    try std.testing.expect(plans.get(second).?.contains("score"));
    var escaping = try compiler.compile(a, "MATCH(n:L) WITH n,min(n.payload) AS p RETURN n,p", .{}, &diagnostic);
    defer escaping.deinit();
    var full: Plans = .empty;
    try plan(a, escaping.query.source, escaping.query, &full);
    try std.testing.expect(!full.contains(escaping.query.clauses[1].operation.project.value.items[0].expression));
}
