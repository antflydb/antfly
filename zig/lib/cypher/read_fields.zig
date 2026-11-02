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

//! Scoped entity lineage and statement-wide property liveness. Forwarding an
//! entity does not consume its properties; unknown consumers retain full values.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const A = std.mem.Allocator;
/// Literal property names; children are partial maps, never public values.
pub const Projection = struct {
    name: []const u8,
    mode: enum { value, presence, children, predicate },
    /// Private equality-only representative; never used for observed fields.
    alternatives: []const std.json.Value = &.{},
    validate: bool = false,
    children: []const Projection = &.{},
};
pub const Entity = struct {
    /// Null means the complete entity payload is observable.
    fields: ?[]const []const u8 = null,
    presence: []const []const u8 = &.{},
    projection: []const Projection = &.{},
};
pub const Metadata = struct { headers: std.json.Value, properties: []const @import("value.zig").Entry };
pub const PreparedMetadata = struct {
    headers: std.json.Value,
    context: *anyopaque,
    project: *const fn (*anyopaque, A, f64, Entity) anyerror![]const @import("value.zig").Entry,
};
pub const Demand = struct {
    pub const Table = struct { name: []const u8, nodes: Entity, labels: ?[]const []const u8 };
    pub const RelationshipDomain = struct { table: []const u8, index: []const u8, stored_type: []const u8, entity: Entity };
    relationship_domains: []const RelationshipDomain = &.{},
    tables: []const Table = &.{},
    nodes: Entity = .{},
    relationships: Entity = .{},
    /// Null retains every catalog membership (labels() or a returned node).
    labels: ?[]const []const u8 = null,
    pub fn relationship(self: Demand, table_name: []const u8, index: []const u8, stored_type: []const u8) Entity {
        for (self.relationship_domains) |domain_value| if (std.mem.eql(u8, domain_value.table, table_name) and std.mem.eql(u8, domain_value.index, index) and std.mem.eql(u8, domain_value.stored_type, stored_type)) return domain_value.entity;
        return self.relationships;
    }
    pub fn table(self: Demand, name: []const u8) ?Table {
        for (self.tables) |item| if (std.mem.eql(u8, item.name, name)) return item;
        return null;
    }
};
const Kind = enum { node, relationship, path };
const Names = std.StringHashMapUnmanaged(void);
const Binding = struct {
    kind: Kind,
    tables: []const []const u8 = &.{},
    relationship_ids: []const usize = &.{},
    prefix: []const []const u8 = &.{},
    used: ?*bool = null,
};
const PropertyPath = struct { parts: []const []const u8, presence: bool, shape: bool = false };
const NodePlan = struct {
    labels: std.ArrayList(catalog.Label) = .empty,
    cover: ?[]const u8 = null,
    values: Names = .empty,
    presence: Names = .empty,
    paths: std.ArrayList(PropertyPath) = .empty,
    full: bool = false,
    labels_full: bool = false,
};
const RelationshipPlan = struct { table: []const u8, index: []const u8, stored_type: []const u8, plan: NodePlan = .{} };
const Plan = struct {
    a: A,
    graph: catalog.Graph,
    source: []const u8,
    relationship_plans: []RelationshipPlan,
    table_names: []const []const u8,
    tables: std.StringHashMapUnmanaged(*NodePlan) = .empty,
    label_tables: std.StringHashMapUnmanaged([]const u8) = .empty,
    node_paths: std.ArrayList(PropertyPath) = .empty,
    edge_paths: std.ArrayList(PropertyPath) = .empty,
    entities: std.StringHashMapUnmanaged(Binding) = .empty,
    aliases: std.ArrayList(Binding) = .empty,
    node_values: Names = .empty,
    node_presence: Names = .empty,
    edge_values: Names = .empty,
    edge_presence: Names = .empty,
    labels: Names = .empty,
    types: Names = .empty,
    unlabeled: bool = false,
    all_types: bool = false,
    edges: bool = false,
    labels_full: bool = false,
    nodes_full: bool = false,
    edges_full: bool = false,
    fields: std.StringHashMapUnmanaged(void) = .empty,
    full: bool = false,
    fn complete(self: *Plan) void {
        self.full = true;
        self.nodes_full = true;
        self.edges_full = true;
        self.labels_full = true;
        for (self.relationship_plans) |*domain_value| domain_value.plan.full = true;
        var tables = self.tables.valueIterator();
        while (tables.next()) |item| {
            item.*.full = true;
            item.*.labels_full = true;
        }
    }
    fn whole(self: *Plan, binding: Binding) anyerror!void {
        if (binding.prefix.len != 0) return self.propertyPath(binding, binding.prefix, false);
        self.full = true;
        switch (binding.kind) {
            .node => {
                self.nodes_full = true;
                self.labels_full = true;
            },
            .relationship => self.edges_full = true,
            .path => {
                self.nodes_full = true;
                self.edges_full = true;
                self.labels_full = true;
            },
        }
        if (binding.kind != .node) for (binding.relationship_ids) |id| {
            self.relationship_plans[id].plan.full = true;
        };
        if (binding.kind != .relationship) for (binding.tables) |table| {
            const item = self.tables.get(table).?;
            item.full = true;
            item.labels_full = true;
        };
    }
    fn field(self: *Plan, binding: Binding, name: []const u8, presence: bool) !void {
        try self.fields.put(self.a, name, {});
        switch (binding.kind) {
            .node => if (presence) try self.node_presence.put(self.a, name, {}) else try self.node_values.put(self.a, name, {}),
            .relationship => if (presence) try self.edge_presence.put(self.a, name, {}) else try self.edge_values.put(self.a, name, {}),
            .path => self.complete(),
        }
        if (binding.kind == .relationship) for (binding.relationship_ids) |id| {
            const item = &self.relationship_plans[id].plan;
            if (presence) try item.presence.put(self.a, name, {}) else try item.values.put(self.a, name, {});
        };
        if (binding.kind == .node) for (binding.tables) |table| {
            const item = self.tables.get(table).?;
            if (presence) try item.presence.put(self.a, name, {}) else try item.values.put(self.a, name, {});
        };
    }
    fn edgeDomain(self: *Plan, edge: ast.Edge) ![]const usize {
        var selected: std.ArrayList(usize) = .empty;
        for (self.relationship_plans, 0..) |domain_value, id| {
            for (self.graph.relationships) |mapping| {
                if (!std.mem.eql(u8, domain_value.table, self.label_tables.get(mapping.source_label).?) or !std.mem.eql(u8, domain_value.index, mapping.index) or !std.mem.eql(u8, domain_value.stored_type, mapping.stored_type)) continue;
                const matches = edge.types.len == 0 or for (edge.types) |name| {
                    if (std.mem.eql(u8, name, mapping.name)) break true;
                } else false;
                if (matches) {
                    try selected.append(self.a, id);
                    break;
                }
            }
        }
        return selected.items;
    }
    fn pathEdges(self: *Plan, path: ast.Path) ![]const usize {
        var selected: std.ArrayList(usize) = .empty;
        for (path.edges) |edge| for (try self.edgeDomain(edge)) |id| {
            if (std.mem.indexOfScalar(usize, selected.items, id) == null) try selected.append(self.a, id);
        };
        return selected.items;
    }
    fn domain(self: *Plan, node: ast.Node) ![]const []const u8 {
        // MATCH constrains an existing binding; OPTIONAL MATCH can retain it
        // when the new pattern fails. It never changes that binding's table.
        if (node.alias) |name| if (self.entities.get(name)) |binding| {
            if (binding.kind == .node) return binding.tables;
        };
        if (node.labels.len == 0) return self.table_names;
        var selected: Names = .empty;
        for (node.labels) |name| if (self.label_tables.get(name)) |table| try selected.put(self.a, table, {});
        return sortedNames(self.a, selected);
    }
    fn endpointDomain(self: *Plan, path: ast.Path, index: usize) ![]const []const u8 {
        const node = path.nodes[index];
        if (node.labels.len != 0) return self.domain(node);
        if (node.alias) |name| if (self.entities.contains(name)) return self.domain(node);
        var selected_tables = self.table_names;
        const first = if (index == 0) 0 else index - 1;
        for (path.edges[first..@min(path.edges.len, index + 1)], first..) |edge, edge_index| {
            var allowed: Names = .empty;
            if (edge.min_hops == 0) {
                const other = path.nodes[if (index == edge_index) edge_index + 1 else edge_index];
                for (try self.domain(other)) |table| try allowed.put(self.a, table, {});
            }
            if (edge.max_hops == null or edge.max_hops.? > 0) for (self.graph.relationships) |mapping| {
                const included = edge.types.len == 0 or for (edge.types) |kind| {
                    if (std.mem.eql(u8, kind, mapping.name)) break true;
                } else false;
                if (!included) continue;
                const left = index == edge_index;
                if (edge.direction == .both or (left == (edge.direction == .out))) {
                    if (self.label_tables.get(mapping.source_label)) |table| try allowed.put(self.a, table, {});
                }
                if (edge.direction == .both or (left == (edge.direction == .in))) {
                    if (self.label_tables.get(mapping.target_label)) |table| try allowed.put(self.a, table, {});
                }
            };
            var selected: Names = .empty;
            for (selected_tables) |table| if (allowed.contains(table)) try selected.put(self.a, table, {});
            selected_tables = try sortedNames(self.a, selected);
        }
        return selected_tables;
    }
    fn pathDomain(self: *Plan, path: ast.Path) ![]const []const u8 {
        var selected: Names = .empty;
        for (path.nodes) |node| for (try self.domain(node)) |table| try selected.put(self.a, table, {});
        // Variable-length paths can visit mapped tables absent from their
        // explicit endpoint patterns. A whole path exposes those nodes too.
        for (path.edges) |edge| for (self.graph.relationships) |mapping| {
            const included = edge.types.len == 0 or for (edge.types) |kind| {
                if (std.mem.eql(u8, kind, mapping.name)) break true;
            } else false;
            if (!included) continue;
            if (self.label_tables.get(mapping.source_label)) |table| try selected.put(self.a, table, {});
            if (self.label_tables.get(mapping.target_label)) |table| try selected.put(self.a, table, {});
        };
        return sortedNames(self.a, selected);
    }
    fn searchDomain(self: *Plan, args: []const *const ast.Expr) ![]const []const u8 {
        if (args.len != 2) return self.table_names;
        var table: ?[]const u8 = null;
        var database: []const u8 = "default";
        var namespace: []const u8 = "public";
        const target = args[0].value;
        if (target == .literal and target.literal == .string) {
            table = target.literal.string;
        } else if (target == .map) {
            for (target.map) |entry| {
                if (!std.mem.eql(u8, entry.name, "table") and !std.mem.eql(u8, entry.name, "database") and !std.mem.eql(u8, entry.name, "namespace")) continue;
                if (entry.value.value != .literal or entry.value.value.literal != .string) return self.table_names;
                if (std.mem.eql(u8, entry.name, "table")) table = entry.value.value.literal.string else if (std.mem.eql(u8, entry.name, "database")) database = entry.value.value.literal.string else namespace = entry.value.value.literal.string;
            }
        } else return self.table_names;
        const name = table orelse return self.table_names;
        var selected: Names = .empty;
        for (self.graph.labels) |label| {
            const logical = std.mem.eql(u8, if (label.logical_table.len == 0) label.table else label.logical_table, name) and std.mem.eql(u8, label.database, database) and std.mem.eql(u8, label.namespace, namespace);
            if (logical or (target == .literal and std.mem.eql(u8, label.table, name))) try selected.put(self.a, label.table, {});
        }
        return if (selected.count() == 0) self.table_names else try sortedNames(self.a, selected);
    }
    fn labelsFor(self: *Plan, binding: Binding) void {
        self.labels_full = true;
        for (binding.tables) |table| self.tables.get(table).?.labels_full = true;
    }
    /// Resolve static paths through WITH aliases without consuming the parent.
    fn lineage(self: *Plan, expr: ast.Expr) !?Binding {
        var base = &expr;
        var reversed: std.ArrayList([]const u8) = .empty;
        while (true) switch (base.value) {
            .property => |property_value| {
                try reversed.append(self.a, property_value.name);
                base = property_value.base;
            },
            .index => |index| {
                if (index.index.value != .literal or index.index.value.literal != .string) return null;
                try reversed.append(self.a, index.index.value.literal.string);
                base = index.base;
            },
            else => break,
        };
        if (base.value != .variable) return null;
        var binding = self.entities.get(base.value.variable) orelse return null;
        if (reversed.items.len == 0) return binding;
        if (binding.kind == .path) return null;
        const parts = try self.a.alloc([]const u8, binding.prefix.len + reversed.items.len);
        @memcpy(parts[0..binding.prefix.len], binding.prefix);
        for (reversed.items, 0..) |part, i| parts[parts.len - 1 - i] = part;
        if (binding.used) |used| used.* = true;
        binding.prefix = parts;
        binding.used = null;
        return binding;
    }
    fn propertyPath(self: *Plan, binding: Binding, parts: []const []const u8, presence: bool) !void {
        return self.recordPath(binding, parts, presence, false);
    }
    fn recordPath(self: *Plan, binding: Binding, parts: []const []const u8, presence: bool, shape: bool) !void {
        if (binding.used) |used| used.* = true;
        if ((!shape and parts.len == 1) or parts.len > 64 or binding.kind == .path) {
            try self.field(binding, parts[0], presence and parts.len == 1);
            return;
        }
        try self.fields.put(self.a, parts[0], {});
        const path: PropertyPath = .{ .parts = parts, .presence = presence, .shape = shape };
        if (binding.kind == .node) {
            try self.node_paths.append(self.a, path);
            for (binding.tables) |table| try self.tables.get(table).?.paths.append(self.a, path);
        } else {
            try self.edge_paths.append(self.a, path);
            for (binding.relationship_ids) |id| try self.relationship_plans[id].plan.paths.append(self.a, path);
        }
    }
    fn property(self: *Plan, expr: ast.Expr, presence: bool) !bool {
        const binding = (try self.lineage(expr)) orelse return false;
        if (binding.prefix.len == 0) return false;
        try self.propertyPath(binding, binding.prefix, presence);
        return true;
    }
    fn clauses(self: *Plan, items: []const ast.Clause, internal_return: bool) anyerror!void {
        for (items) |clause| switch (clause.operation) {
            .project => |p| {
                var next = if (p.value.wildcard) try self.entities.clone(self.a) else std.StringHashMapUnmanaged(Binding).empty;
                // Grouping and DISTINCT observe the complete projected value.
                const grouping = for (p.value.items) |item| {
                    if (@import("expressions.zig").aggregates(item.expression)) break true;
                } else false;
                var visible = try self.entities.clone(self.a);
                for (p.value.items) |item| {
                    const name = item.alias orelse try @import("expressions.zig").outputName(self.a, self.source, item.expression);
                    _ = next.remove(name);
                    // Every output name shadows its input, including implicit
                    // map-projection names whose value has no entity lineage.
                    _ = visible.remove(name);
                    const candidate = if (name.len != 0 and (!p.final or internal_return)) try self.lineage(item.expression.*) else null;
                    const forwarded = if (candidate) |binding| if ((!p.value.distinct and !grouping) or binding.prefix.len == 0) candidate else null else null;
                    if (forwarded) |source| {
                        var binding = source;
                        if (binding.prefix.len != 0 and item.expression.value != .variable) {
                            binding.used = try self.a.create(bool);
                            binding.used.?.* = false;
                            try self.aliases.append(self.a, binding);
                        }
                        try next.put(self.a, name, binding);
                    } else try self.walk(item.expression.*);
                }
                // ORDER BY can see input bindings as well as projection aliases.
                // Keep both scopes while collecting requirements, then retire
                // hidden bindings before the next clause.
                var names_iter = next.keyIterator();
                while (names_iter.next()) |key| try visible.put(self.a, key.*, next.get(key.*).?);
                self.entities = visible;
                try self.walk(p.value.predicate);
                for (p.value.order) |order| {
                    const binding = if (order.expression.value == .variable) self.entities.get(order.expression.value.variable) else null;
                    if (binding == null or binding.?.prefix.len != 0) try self.walk(order.expression.*);
                }
                try self.walk(p.value.skip);
                try self.walk(p.value.limit);
                if (p.final and !internal_return and p.value.wildcard) {
                    var bindings = next.valueIterator();
                    while (bindings.next()) |binding| try self.whole(binding.*);
                }
                self.entities = next;
            },
            .subquery => |inner| {
                var outer = try self.entities.clone(self.a);
                try self.clauses(inner, true);
                // CALL's RETURN is an internal binding boundary, not a public
                // entity observer. Export only its final projected lineage.
                if (inner.len != 0 and inner[inner.len - 1].operation == .project and inner[inner.len - 1].operation.project.final) {
                    var exports = self.entities.iterator();
                    while (exports.next()) |entry| try outer.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                }
                self.entities = outer;
            },
            .call => |procedure| {
                try self.walk(procedure.args);
                if (std.mem.eql(u8, procedure.name, "antfly.search")) {
                    self.unlabeled = true;
                    for (procedure.yields) |yield| {
                        _ = self.entities.remove(yield.alias);
                        if (std.mem.eql(u8, yield.name, "node")) {
                            try self.entities.put(self.a, yield.alias, .{ .kind = .node, .tables = try self.searchDomain(procedure.args) });
                            self.unlabeled = true;
                        }
                    }
                    try self.walk(procedure.predicate);
                } else self.complete();
            },
            .unwind => |u| {
                try self.walk(u.expression.*);
                _ = self.entities.remove(u.alias);
            },
            else => {
                try self.names(clause.operation);
                try self.walk(clause.operation);
            },
        };
    }
    fn names(self: *Plan, value: anytype) anyerror!void {
        const T = @TypeOf(value);
        // Expression subqueries/comprehensions own lexical scopes. Their
        // patterns are registered by walk(), never in the outer name pass.
        if (T == ast.Expr) return;
        if (T == ast.Path) {
            // Compute every new endpoint against the same incoming scope.
            const domains = try self.a.alloc([]const []const u8, value.nodes.len);
            for (value.nodes, 0..) |_, i| domains[i] = try self.endpointDomain(value, i);
            for (value.nodes, domains) |node, tables| if (node.alias) |name| {
                if (!self.entities.contains(name)) try self.entities.put(self.a, name, .{ .kind = .node, .tables = tables });
            };
        }
        if (T == ast.Node or T == ast.Edge or T == ast.Path) {
            if (value.alias) |name| if (!self.entities.contains(name)) try self.entities.put(self.a, name, .{
                .kind = if (T == ast.Node) .node else if (T == ast.Edge) .relationship else .path,
                .tables = if (T == ast.Node) try self.domain(value) else if (T == ast.Path) try self.pathDomain(value) else &.{},
                .relationship_ids = if (T == ast.Edge) try self.edgeDomain(value) else if (T == ast.Path) try self.pathEdges(value) else &.{},
            });
            if (T == ast.Node) {
                self.unlabeled = self.unlabeled or value.labels.len == 0;
                for (value.labels) |label| try self.labels.put(self.a, label, {});
            }
            if (T == ast.Edge) {
                self.edges = true;
                self.all_types = self.all_types or value.types.len == 0;
                for (value.types) |name| try self.types.put(self.a, name, {});
            }
        }
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .one) try self.names(value.*);
                if (p.size == .slice and p.child != u8) for (value) |child| try self.names(child);
            },
            .@"struct" => |s| inline for (s.field_names) |name| try self.names(@field(value, name)),
            .@"union" => switch (value) {
                inline else => |payload| try self.names(payload),
            },
            .optional => if (value) |child| {
                try self.names(child);
            },
            else => {},
        }
    }
    fn walk(self: *Plan, value: anytype) anyerror!void {
        const T = @TypeOf(value);
        if (T == ast.Query) {
            const initial = try self.entities.clone(self.a);
            try self.clauses(value.clauses, false);
            for (value.unions) |arm| {
                self.entities = try initial.clone(self.a);
                try self.clauses(arm.clauses, false);
            }
            return;
        }
        if (T == ast.CollectionFilter) {
            try self.walk(value.initial);
            const outer = try self.entities.clone(self.a);
            _ = self.entities.remove(value.alias);
            if (value.accumulator) |name| _ = self.entities.remove(name);
            try self.walk(value.predicate);
            try self.walk(value.projection);
            self.entities = outer;
            return;
        }
        if (T == ast.Expr) {
            switch (value.value) {
                .comprehension => |local| {
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    try self.walk(local.predicate);
                    try self.walk(local.projection);
                    self.entities = outer;
                    return;
                },
                .list_predicate => |local| {
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    try self.walk(local.predicate.*);
                    self.entities = outer;
                    return;
                },
                .reduce => |local| {
                    try self.walk(local.initial.*);
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    _ = self.entities.remove(local.accumulator);
                    try self.walk(local.projection.*);
                    self.entities = outer;
                    return;
                },
                .pattern_comprehension => |pattern| {
                    // The comprehension introduces entity bindings which are
                    // invisible to the enclosing projection. Their predicates
                    // and values still contribute storage field requirements.
                    const outer = try self.entities.clone(self.a);
                    try self.names(pattern.path);
                    try self.walk(pattern.path);
                    try self.walk(pattern.predicate);
                    try self.walk(pattern.projection.*);
                    self.entities = outer;
                    return;
                },
                .exists => |inner| {
                    const outer = try self.entities.clone(self.a);
                    try self.clauses(inner, false);
                    self.entities = outer;
                    return;
                },
                .variable => |name| {
                    if (self.entities.get(name)) |kind| try self.whole(kind);
                    return;
                },
                .property, .index => {
                    if (try self.property(value, false)) return;
                },
                .map_projection => |projection| {
                    if (try self.lineage(projection.base.*)) |binding| {
                        if (binding.prefix.len != 0) try self.recordPath(binding, binding.prefix, false, true);
                        for (projection.items) |item| switch (item) {
                            .property => |name| {
                                const parts = try self.a.alloc([]const u8, binding.prefix.len + 1);
                                @memcpy(parts[0..binding.prefix.len], binding.prefix);
                                parts[parts.len - 1] = name;
                                try self.propertyPath(binding, parts, false);
                            },
                            .entry => |entry| try self.walk(entry.value.*),
                            .variable => |name| if (self.entities.get(name)) |other| try self.whole(other),
                            .all => try self.whole(binding),
                        };
                        return;
                    }
                },
                .unary => |u| {
                    if ((u.op == .is_null or u.op == .is_not_null) and (u.operand.value == .property or u.operand.value == .index or u.operand.value == .variable)) {
                        if (try self.property(u.operand.*, true)) return;
                    }
                },
                .call => |c| {
                    if (std.ascii.eqlIgnoreCase(c.name, "count") and !c.distinct and c.args.len == 1 and (c.args[0].value == .property or c.args[0].value == .index or c.args[0].value == .variable)) {
                        if (try self.property(c.args[0].*, true)) return;
                    }
                    const identity_only = std.ascii.eqlIgnoreCase(c.name, "antfly.documentKey") or std.ascii.eqlIgnoreCase(c.name, "labels") or
                        std.ascii.eqlIgnoreCase(c.name, "type") or std.ascii.eqlIgnoreCase(c.name, "length") or std.ascii.eqlIgnoreCase(c.name, "count");
                    if (std.ascii.eqlIgnoreCase(c.name, "labels")) {
                        if (c.args.len == 1 and c.args[0].value == .variable) {
                            if (self.entities.get(c.args[0].value.variable)) |binding| {
                                if (binding.prefix.len == 0) self.labelsFor(binding);
                            }
                        } else {
                            self.labels_full = true;
                            var it = self.tables.valueIterator();
                            while (it.next()) |item| item.*.labels_full = true;
                        }
                    }
                    if (identity_only) {
                        for (c.args) |arg| {
                            if (arg.value == .variable) {
                                if (self.entities.get(arg.value.variable)) |binding| {
                                    if (binding.prefix.len != 0) try self.whole(binding);
                                }
                            } else try self.walk(arg.*);
                        }
                        return;
                    }
                },
                else => {},
            }
        }
        if (T == ast.Node or T == ast.Edge) for (value.properties) |entry| try self.field(.{ .kind = if (T == ast.Node) .node else .relationship, .tables = if (T == ast.Node) try self.domain(value) else &.{}, .relationship_ids = if (T == ast.Edge) try self.edgeDomain(value) else &.{} }, entry.name, false);
        // Procedure yields may contain entities with implementation-defined
        // aliases. Preserve complete results across this extension boundary.
        if (T == ast.Procedure) self.complete();
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .one) try self.walk(value.*);
                if (p.size == .slice and p.child != u8) for (value) |child| try self.walk(child);
            },
            .@"struct" => |s| inline for (s.field_names) |name| try self.walk(@field(value, name)),
            .@"union" => switch (value) {
                inline else => |payload| try self.walk(payload),
            },
            .optional => if (value) |child| {
                try self.walk(child);
            },
            else => {},
        }
    }
};
pub const Analysis = struct { fields: ?[]const []const u8, demand: Demand };
pub fn required(a: A, query: ast.Query, graph: catalog.Graph) !?[]const []const u8 {
    return (try analyze(a, query, graph)).fields;
}
pub fn analyze(a: A, query: ast.Query, graph: catalog.Graph) !Analysis {
    var table_set: Names = .empty;
    for (graph.labels) |label| try table_set.put(a, label.table, {});
    const table_names = try sortedNames(a, table_set);
    var relationship_plans: std.ArrayList(RelationshipPlan) = .empty;
    for (graph.relationships) |mapping| {
        const table_name = graph.label(mapping.source_label).?.table;
        for (relationship_plans.items) |domain_value| {
            if (std.mem.eql(u8, domain_value.table, table_name) and std.mem.eql(u8, domain_value.index, mapping.index) and std.mem.eql(u8, domain_value.stored_type, mapping.stored_type)) break;
        } else try relationship_plans.append(a, .{ .table = table_name, .index = mapping.index, .stored_type = mapping.stored_type });
    }
    var plan: Plan = .{ .a = a, .graph = graph, .source = query.source, .table_names = table_names, .relationship_plans = relationship_plans.items };
    for (table_names) |table| {
        const item = try a.create(NodePlan);
        item.* = .{};
        try plan.tables.put(a, table, item);
    }
    for (graph.labels) |label| {
        try plan.label_tables.put(a, label.name, label.table);
        const item = plan.tables.get(label.table).?;
        try item.labels.append(a, label);
        if (item.cover == null and label.predicate.len == 0) item.cover = label.name;
    }
    try plan.walk(query);
    // An unused forwarding expression is still evaluated. Retain its value
    // and validation rather than turning eager errors into missing properties.
    for (plan.aliases.items) |binding| if (!binding.used.?.*) try plan.whole(binding);
    if (plan.edges) for (graph.relationships) |rel| {
        if (plan.all_types or plan.types.contains(rel.name)) {
            try plan.labels.put(a, rel.source_label, {});
            try plan.labels.put(a, rel.target_label, {});
        }
    };
    var membership: Names = .empty;
    const tables = try a.alloc(Demand.Table, table_names.len);
    for (table_names, tables) |table, *out| {
        const item = plan.tables.get(table).?;
        var selected: Names = .empty;
        var predicates: std.StringHashMapUnmanaged(std.ArrayList(@import("value.zig").Value)) = .empty;
        for (item.labels.items) |label| {
            var observed = item.labels_full or plan.labels.contains(label.name);
            if (plan.unlabeled and !observed) {
                const cover = item.cover;
                observed = if (cover) |name| std.mem.eql(u8, name, label.name) else true;
            }
            if (observed) {
                try selected.put(a, label.name, {});
                for (label.predicate) |entry| {
                    const observed_field = item.full or item.values.contains(entry.name) or for (item.paths.items) |path| {
                        if (std.mem.eql(u8, path.parts[0], entry.name)) break true;
                    } else false;
                    if (observed_field) {
                        try item.values.put(a, entry.name, {});
                    } else {
                        const slot = try predicates.getOrPut(a, entry.name);
                        if (!slot.found_existing) slot.value_ptr.* = .empty;
                        try slot.value_ptr.append(a, entry.value);
                    }
                }
            }
        }
        var node_entity = try entity(a, item.full, item.values, item.presence, item.paths.items);
        if (node_entity.fields) |fields| {
            var names: Names = .empty;
            for (fields) |name| try names.put(a, name, {});
            var projections: std.ArrayList(Projection) = .empty;
            try projections.appendSlice(a, node_entity.projection);
            var iter = predicates.iterator();
            while (iter.next()) |entry| {
                try names.put(a, entry.key_ptr.*, {});
                try projections.append(a, .{ .name = entry.key_ptr.*, .mode = .predicate, .alternatives = try std.json.parseFromSliceLeaky([]const std.json.Value, a, try std.json.Stringify.valueAlloc(a, entry.value_ptr.items, .{}), .{ .allocate = .alloc_always }) });
            }
            node_entity.fields = try sortedNames(a, names);
            node_entity.projection = projections.items;
        }
        out.* = .{ .name = table, .nodes = node_entity, .labels = if (item.labels_full) null else try sortedNames(a, selected) };
    }
    for (graph.labels) |label| {
        var observed = plan.labels_full or plan.labels.contains(label.name);
        if (plan.unlabeled and !observed) {
            const cover = plan.tables.get(label.table).?.cover;
            observed = if (cover) |name| std.mem.eql(u8, name, label.name) else true;
        }
        if (observed) {
            try membership.put(a, label.name, {});
            for (label.predicate) |entry| try plan.node_values.put(a, entry.name, {});
        }
        // Legacy adapters retain complete membership semantics.
        for (label.predicate) |entry| try plan.fields.put(a, entry.name, {});
    }
    const domains = try a.alloc(Demand.RelationshipDomain, plan.relationship_plans.len);
    for (plan.relationship_plans, domains) |domain_value, *out| out.* = .{ .table = domain_value.table, .index = domain_value.index, .stored_type = domain_value.stored_type, .entity = try entity(a, domain_value.plan.full, domain_value.plan.values, domain_value.plan.presence, domain_value.plan.paths.items) };
    return .{
        .fields = if (plan.full) null else try sortedNames(a, plan.fields),
        .demand = .{
            .tables = tables,
            .relationship_domains = domains,
            .nodes = try entity(a, plan.nodes_full, plan.node_values, plan.node_presence, plan.node_paths.items),
            .relationships = try entity(a, plan.edges_full, plan.edge_values, plan.edge_presence, plan.edge_paths.items),
            .labels = if (plan.labels_full) null else try sortedNames(a, membership),
        },
    };
}
fn sortedNames(a: A, input: Names) ![]const []const u8 {
    const out = try a.alloc([]const u8, input.count());
    var keys = input.keyIterator();
    for (out) |*name| name.* = keys.next().?.*;
    std.mem.sort([]const u8, out, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    return out;
}
fn entity(a: A, full: bool, values: Names, presence: Names, paths: []const PropertyPath) !Entity {
    if (full) return .{};
    var fields = try values.clone(a);
    var only_presence: Names = .empty;
    var iterator = presence.keyIterator();
    while (iterator.next()) |name| {
        try fields.put(a, name.*, {});
        if (!values.contains(name.*)) try only_presence.put(a, name.*, {});
    }
    for (paths) |path| {
        try fields.put(a, path.parts[0], {});
        _ = only_presence.remove(path.parts[0]);
    }
    var projection: std.ArrayList(Projection) = .empty;
    const roots = try sortedNames(a, fields);
    for (roots) |name| {
        if (values.contains(name)) continue;
        var selected: std.ArrayList(PropertyPath) = .empty;
        for (paths) |path| if (std.mem.eql(u8, path.parts[0], name)) try selected.append(a, path);
        if (selected.items.len != 0) {
            if (presence.contains(name)) try selected.append(a, .{ .parts = try a.dupe([]const u8, &.{name}), .presence = true });
            try projection.append(a, try projectTree(a, name, selected.items, 1));
        }
    }
    return .{ .fields = roots, .presence = try sortedNames(a, only_presence), .projection = projection.items };
}
fn projectTree(a: A, name: []const u8, paths: []const PropertyPath, depth: usize) anyerror!Projection {
    var children: Names = .empty;
    var validate = false;
    var shape = false;
    for (paths) |path| {
        if (path.parts.len == depth) {
            if (!path.presence and !path.shape) return .{ .name = name, .mode = .value };
            validate = validate or path.presence;
            shape = shape or path.shape;
        } else try children.put(a, path.parts[depth], {});
    }
    if (children.count() == 0) return if (shape) .{ .name = name, .mode = .children, .validate = validate } else .{ .name = name, .mode = .presence };
    const keys = try sortedNames(a, children);
    const out = try a.alloc(Projection, keys.len);
    for (keys, out) |key, *child| {
        var selected: std.ArrayList(PropertyPath) = .empty;
        for (paths) |path| if (path.parts.len > depth and std.mem.eql(u8, path.parts[depth], key)) try selected.append(a, path);
        child.* = try projectTree(a, key, selected.items, depth + 1);
    }
    return .{ .name = name, .mode = .children, .children = out, .validate = validate };
}
