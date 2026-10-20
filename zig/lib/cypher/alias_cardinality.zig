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

//! Invocation-local aggregate demand propagation through projection bindings.
//! Rewrites are keyed by AST identity, never by a globally visible spelling.
const std = @import("std");
const ast = @import("ast.zig");
const expressions = @import("expressions.zig");
const Index = @import("reduction.zig").Index;
const Bounds = @import("reduction.zig").Bounds;
const A = std.mem.Allocator;
pub const Rewrites = std.AutoHashMapUnmanaged(*const ast.Expr, ast.Expr);
const Names = std.StringHashMapUnmanaged(usize);
const FilterUse = struct { expression: *const ast.Expr, input: *const ast.Expr, filter: ast.CollectionFilter, window: bool = false };
const Candidate = struct { expression: *const ast.Expr, valid: bool = true, window: ?Bounds = null, uses: std.ArrayList(*const ast.Expr) = .empty, windows: std.ArrayList(*const ast.Expr) = .empty, filters: std.ArrayList(FilterUse) = .empty };
const Cohort = struct { members: std.ArrayList(usize) = .empty, full: bool = false };
fn hasCollection(value: anytype) bool {
    const T = @TypeOf(value);
    if (T == *const ast.Expr) {
        if (value.value == .call and std.ascii.eqlIgnoreCase(value.value.call.name, "collect")) return true;
        return hasCollection(value.value);
    }
    switch (@typeInfo(T)) {
        .pointer => |p| if (p.size == .slice and p.child != u8) {
            for (value) |child| if (hasCollection(child)) return true;
        },
        .@"struct" => |info| inline for (info.field_names) |name| {
            if (hasCollection(@field(value, name))) return true;
        },
        .@"union" => switch (value) {
            inline else => |child| return hasCollection(child),
        },
        .optional => if (value) |child| return hasCollection(child),
        else => {},
    }
    return false;
}
const Flow = struct {
    a: A,
    source: []const u8,
    candidates: std.ArrayList(Candidate) = .empty,
    cohorts: std.ArrayList(Cohort) = .empty,
    projection: ?struct { items: []const ast.Item, names: Names } = null,
    fn lookup(names: Names, name: []const u8, locals: ?*const expressions.Local) ?usize {
        var current = locals;
        while (current) |local| : (current = local.parent) if (std.mem.eql(u8, local.name, name)) return null;
        return names.get(name);
    }
    fn full(self: *@This(), names: Names, name: []const u8, locals: ?*const expressions.Local) void {
        if (lookup(names, name, locals)) |id| self.candidates.items[id].valid = false;
    }
    fn barrier(self: *@This(), names: Names) void {
        var ids = names.valueIterator();
        while (ids.next()) |id| self.candidates.items[id.*].valid = false;
    }
    fn cohort(self: *@This(), index: *Index, expression: *const ast.Expr) !usize {
        const slot = try index.intern(self.a, expression, self.cohorts.items.len);
        if (slot == self.cohorts.items.len) try self.cohorts.append(self.a, .{});
        return slot;
    }
    fn consumerInput(self: *@This(), input: *const ast.Expr, names: Names, locals: ?*const expressions.Local, index: ?*Index) !?usize {
        if (input.value == .variable) return lookup(names, input.value.variable, locals);
        if (index) |slots| if (input.value == .call and std.ascii.eqlIgnoreCase(input.value.call.name, "collect") and !input.value.call.star and input.value.call.args.len == 1) {
            const slot = try self.cohort(slots, input);
            const id = self.candidates.items.len;
            try self.candidates.append(self.a, .{ .expression = input });
            try self.cohorts.items[slot].members.append(self.a, id);
            try self.scan(input.value.call.args, names, locals, index);
            return id;
        };
        return null;
    }
    fn scan(self: *@This(), value: anytype, names: Names, locals: ?*const expressions.Local, index: ?*Index) anyerror!void {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) {
            // DISTINCT orders matching projected expressions by their already
            // computed value, even when an output alias shadows an input name.
            if (self.projection) |projection| if (value.value != .variable and expressions.unshadowed(value, locals)) {
                for (projection.items) |item| if (expressions.same(item.expression, value)) {
                    self.projection = null;
                    defer self.projection = projection;
                    return self.scan(value, projection.names, locals, index);
                };
            };
            if (@import("reduction.zig").collectionConsumer(value)) |prefix| {
                if (prefix.input.value == .comprehension) {
                    const c = prefix.input.value.comprehension;
                    const Context = struct {
                        alias: []const u8,
                        pub fn permits(context: @This(), name: []const u8) bool {
                            return std.mem.eql(u8, context.alias, name);
                        }
                    };
                    const context: Context = .{ .alias = c.alias };
                    if ((c.predicate == null or (!expressions.aggregates(c.predicate.?) and expressions.rowLocal(c.predicate.?, context))) and
                        (c.projection == null or (!expressions.aggregates(c.projection.?) and expressions.rowLocal(c.projection.?, context))))
                    {
                        if (try self.consumerInput(c.input, names, locals, index)) |id| {
                            try self.candidates.items[id].filters.append(self.a, .{ .expression = value, .input = c.input, .window = true, .filter = .{ .alias = c.alias, .predicate = c.predicate, .projection = c.projection, .kind = .window, .prefix = prefix.bounds.prefix, .suffix = prefix.bounds.suffix } });
                            return;
                        }
                    }
                }
                if (try self.consumerInput(prefix.input, names, locals, index)) |id| {
                    self.candidates.items[id].window = (self.candidates.items[id].window orelse Bounds{}).merge(prefix.bounds);
                    try self.candidates.items[id].windows.append(self.a, value);
                    return;
                }
            }
            switch (value.value) {
                .variable => |name| self.full(names, name, locals),
                .exists, .pattern_comprehension => self.barrier(names),
                .call => |call| {
                    if (std.ascii.eqlIgnoreCase(call.name, "size") and !call.star and !call.distinct and call.args.len == 1 and call.args[0].value == .comprehension) {
                        const c = call.args[0].value.comprehension;
                        if (try self.consumerInput(c.input, names, locals, index)) |id| {
                            const Context = struct {
                                alias: []const u8,
                                pub fn permits(context: @This(), name: []const u8) bool {
                                    return std.mem.eql(u8, context.alias, name);
                                }
                            };
                            const context: Context = .{ .alias = c.alias };
                            // Correlated, effectful and nested aggregate consumers
                            // stay materialized. Scalar failures are deferred by
                            // the physical reducer until this exact SIZE is used.
                            const safe_predicate = if (c.predicate) |e| !expressions.aggregates(e) and expressions.rowLocal(e, context) else true;
                            const safe_projection = if (c.projection) |e| !expressions.aggregates(e) and expressions.rowLocal(e, context) else true;
                            if (safe_predicate and safe_projection) {
                                try self.candidates.items[id].filters.append(self.a, .{ .expression = value, .input = c.input, .filter = .{ .alias = c.alias, .predicate = c.predicate, .projection = c.projection } });
                                return;
                            }
                        }
                    }
                    if (std.ascii.eqlIgnoreCase(call.name, "size") and !call.star and !call.distinct and call.args.len == 1) {
                        if (try self.consumerInput(call.args[0], names, locals, index)) |id| {
                            try self.candidates.items[id].uses.append(self.a, value);
                            return;
                        }
                    }
                    if (index) |slots| if (std.ascii.eqlIgnoreCase(call.name, "collect")) {
                        const slot = try self.cohort(slots, value);
                        self.cohorts.items[slot].full = true;
                    };
                    try self.scan(call.args, names, locals, index);
                },
                .comprehension => |c| {
                    try self.scan(c.input, names, locals, index);
                    const local: expressions.Local = .{ .name = c.alias, .parent = locals };
                    try self.scan(c.predicate, names, &local, index);
                    try self.scan(c.projection, names, &local, index);
                },
                .list_predicate => |c| {
                    if (try self.consumerInput(c.input, names, locals, index)) |id| {
                        const Context = struct {
                            alias: []const u8,
                            pub fn permits(context: @This(), name: []const u8) bool {
                                return std.mem.eql(u8, context.alias, name);
                            }
                        };
                        if (!expressions.aggregates(c.predicate) and expressions.rowLocal(c.predicate, Context{ .alias = c.alias })) {
                            const kind: ast.CollectionFilter.Kind = switch (c.kind) {
                                .all => .all,
                                .any => .any,
                                .none => .none,
                                .single => .single,
                            };
                            try self.candidates.items[id].filters.append(self.a, .{ .expression = value, .input = c.input, .filter = .{ .alias = c.alias, .predicate = c.predicate, .projection = null, .kind = kind } });
                            return;
                        }
                    }
                    try self.scan(c.input, names, locals, index);
                    const local: expressions.Local = .{ .name = c.alias, .parent = locals };
                    try self.scan(c.predicate, names, &local, index);
                },
                .reduce => |r| {
                    const Context = struct {
                        alias: ?[]const u8 = null,
                        accumulator: ?[]const u8 = null,
                        pub fn permits(context: @This(), name: []const u8) bool {
                            return (context.alias != null and std.mem.eql(u8, context.alias.?, name)) or (context.accumulator != null and std.mem.eql(u8, context.accumulator.?, name));
                        }
                    };
                    if (!expressions.aggregates(r.initial) and !expressions.aggregates(r.projection) and expressions.rowLocal(r.initial, Context{}) and expressions.rowLocal(r.projection, Context{ .alias = r.alias, .accumulator = r.accumulator })) {
                        if (try self.consumerInput(r.input, names, locals, index)) |id| {
                            try self.candidates.items[id].filters.append(self.a, .{ .expression = value, .input = r.input, .filter = .{ .alias = r.alias, .predicate = null, .projection = r.projection, .kind = .fold, .accumulator = r.accumulator, .initial = r.initial } });
                            return;
                        }
                    }
                    try self.scan(r.initial, names, locals, index);
                    try self.scan(r.input, names, locals, index);
                    const alias: expressions.Local = .{ .name = r.alias, .parent = locals };
                    const accumulator: expressions.Local = .{ .name = r.accumulator, .parent = &alias };
                    try self.scan(r.projection, names, &accumulator, index);
                },
                inline else => |payload| try self.scan(payload, names, locals, index),
            }
            return;
        }
        if (T == ast.Path or T == ast.Node or T == ast.Edge) if (value.alias) |name| self.full(names, name, locals);
        if (T == ast.MapSelector) if (value == .variable) self.full(names, value.variable, locals);
        switch (@typeInfo(T)) {
            .pointer => |p| if (p.size == .slice and p.child != u8) {
                for (value) |child| try self.scan(child, names, locals, index);
            },
            .@"struct" => |info| inline for (info.field_names) |name| try self.scan(@field(value, name), names, locals, index),
            .@"union" => switch (value) {
                inline else => |child| try self.scan(child, names, locals, index),
            },
            .optional => if (value) |child| try self.scan(child, names, locals, index),
            else => {},
        }
    }
    fn analyze(self: *@This(), clauses: []const ast.Clause, rewrites: *Rewrites) !void {
        if (!hasCollection(clauses)) return;
        var names: Names = .empty;
        for (clauses) |clause| switch (clause.operation) {
            .project => |p| {
                var next: Names = .empty;
                var order_names: Names = .empty;
                var iterator = names.iterator();
                while (iterator.next()) |entry| try order_names.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                var slots: Index = .{};
                var grouped = false;
                for (p.value.items) |item| grouped = grouped or expressions.aggregates(item.expression);
                if (p.value.wildcard) self.barrier(names);
                for (p.value.items) |item| {
                    const output = item.alias orelse try expressions.outputName(self.a, self.source, item.expression);
                    _ = order_names.remove(output);
                    if (item.expression.value == .variable) {
                        if (names.get(item.expression.value.variable)) |id| {
                            if (p.final or p.value.distinct or p.value.wildcard or grouped) self.candidates.items[id].valid = false else try next.put(self.a, output, id);
                            continue;
                        }
                    }
                    const expression = item.expression;
                    if (!p.final and item.alias != null and expression.value == .call and std.ascii.eqlIgnoreCase(expression.value.call.name, "collect") and !expression.value.call.star and expression.value.call.args.len == 1) {
                        try self.scan(expression.value.call.args, names, null, &slots);
                        const id = self.candidates.items.len;
                        try self.candidates.append(self.a, .{ .expression = expression, .valid = !p.value.distinct and !p.value.wildcard });
                        const slot = try self.cohort(&slots, expression);
                        try self.cohorts.items[slot].members.append(self.a, id);
                        try next.put(self.a, item.alias.?, id);
                    } else try self.scan(expression, names, null, &slots);
                }
                iterator = next.iterator();
                while (iterator.next()) |entry| try order_names.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                {
                    if (p.value.distinct) self.projection = .{ .items = p.value.items, .names = names };
                    defer self.projection = null;
                    try self.scan(p.value.order, order_names, null, &slots);
                }
                try self.scan(p.value.predicate, order_names, null, &slots);
                // Window expressions are evaluated outside row scope.
                try self.scan(p.value.skip, .empty, null, &slots);
                try self.scan(p.value.limit, .empty, null, &slots);
                names = next;
            },
            .match => |m| try self.scan(m, names, null, null),
            .unwind => |u| {
                try self.scan(u.expression, names, null, null);
                _ = names.remove(u.alias);
            },
            // Procedures, correlated captures and writes have their own binding
            // or visibility effects. Full-value demands remain conservative at
            // these boundaries; independent nested scopes are analyzed below.
            else => self.barrier(names),
        };
        for (self.cohorts.items) |cohort_| {
            var allowed = !cohort_.full;
            var used = false;
            var prefix: ?Bounds = null;
            for (cohort_.members.items) |id| {
                if (self.candidates.items[id].window) |limit| prefix = (prefix orelse Bounds{}).merge(limit);
                allowed = allowed and self.candidates.items[id].valid;
                used = used or self.candidates.items[id].uses.items.len != 0;
            }
            var filters: std.ArrayList(ast.CollectionFilter) = .empty;
            for (cohort_.members.items) |id| for (self.candidates.items[id].filters.items) |use| try filters.append(self.a, use.filter);
            if (!allowed or (!used and prefix == null and filters.items.len == 0)) continue;
            const summary = filters.items.len != 0 or (used and prefix != null);
            var filter_slot: usize = 0;
            for (cohort_.members.items) |id| {
                const original = self.candidates.items[id].expression;
                var count = original.*;
                if (summary) {
                    count.value.call.collect_summary = true;
                    count.value.call.collect_limit = if (prefix) |limit| limit.prefix else 0;
                    count.value.call.collect_suffix = if (prefix) |limit| limit.suffix else 0;
                    count.value.call.collect_filters = filters.items;
                } else if (prefix) |limit| {
                    count.value.call.collect_limit = limit.prefix;
                    count.value.call.collect_suffix = limit.suffix;
                } else count.value.call.name = "count";
                try rewrites.put(self.a, original, count);
                for (self.candidates.items[id].uses.items) |use| {
                    var scalar = use.*;
                    if (summary) {
                        scalar.value.call.collection_extract = .cardinality;
                    } else if (use.value.call.args[0].value == .variable) {
                        scalar.value = .{ .variable = use.value.call.args[0].value.variable };
                    } else {
                        scalar.value = .{ .call = count.value.call };
                    }
                    try rewrites.put(self.a, use, scalar);
                }
                if (summary) {
                    for (self.candidates.items[id].windows.items) |use| {
                        var consumer = use.*;
                        const input = @import("reduction.zig").collectionConsumer(use).?.input;
                        const extracted = try self.a.create(ast.Expr);
                        const args = try self.a.alloc(*const ast.Expr, 1);
                        args[0] = input;
                        extracted.* = .{ .span = input.span, .value = .{ .call = .{ .name = "head", .args = args, .collection_extract = .{ .window = 1 + filters.items.len * 2 } } } };
                        switch (consumer.value) {
                            .call => |*call| {
                                const window_args = try self.a.dupe(*const ast.Expr, call.args);
                                window_args[0] = extracted;
                                call.args = window_args;
                            },
                            .index => |*index| index.base = extracted,
                            .slice => |*slice| slice.base = extracted,
                            else => unreachable,
                        }
                        try rewrites.put(self.a, use, consumer);
                    }
                    for (self.candidates.items[id].filters.items) |use| {
                        var consumer = use.expression.*;
                        const args = try self.a.alloc(*const ast.Expr, 1);
                        args[0] = use.input;
                        const extracted: @FieldType(ast.Expr, "value") = .{ .call = .{ .name = "head", .args = args, .collection_extract = .{ .filtered = filter_slot } } };
                        if (use.window) {
                            const input = try self.a.create(ast.Expr);
                            input.* = .{ .span = use.expression.span, .value = extracted };
                            switch (consumer.value) {
                                .call => |*call| call.args = try self.a.dupe(*const ast.Expr, &.{input}),
                                .index => |*index| index.base = input,
                                .slice => |*slice| slice.base = input,
                                else => unreachable,
                            }
                        } else consumer.value = extracted;
                        filter_slot += 1;
                        try rewrites.put(self.a, use.expression, consumer);
                    }
                }
            }
        }
    }
};
/// Nested graph scopes get independent name maps; only exact expression
/// pointers enter the global rewrite set, so a shadowed name cannot escape.
pub fn plan(a: A, source: []const u8, value: anytype, rewrites: *Rewrites) anyerror!void {
    const T = @TypeOf(value);
    if (T == []const ast.Clause) {
        var flow: Flow = .{ .a = a, .source = source };
        try flow.analyze(value, rewrites);
    }
    switch (@typeInfo(T)) {
        .pointer => |p| {
            if (p.size == .one and p.child == ast.Expr) try plan(a, source, value.*, rewrites);
            if (p.size == .slice and p.child != u8) for (value) |child| try plan(a, source, child, rewrites);
        },
        .@"struct" => |info| inline for (info.field_names) |name| try plan(a, source, @field(value, name), rewrites),
        .@"union" => switch (value) {
            inline else => |child| try plan(a, source, child, rewrites),
        },
        .optional => if (value) |child| try plan(a, source, child, rewrites),
        else => {},
    }
}
