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
pub const predicate_fingerprint = @import("predicate_fingerprint.zig");
/// Literal property names; children are partial maps, never public values.
pub const Projection = struct {
    name: []const u8,
    mode: enum { value, presence, children, predicate },
    /// Private equality-only representative; never used for observed fields.
    alternatives: []const std.json.Value = &.{},
    fingerprints: []const predicate_fingerprint.Digest = &.{},
    keys: bool = false,
    root: bool = false,
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
const PropertyPath = struct { parts: []const []const u8, presence: bool, shape: bool = false, keys: bool = false };
const NodePlan = struct {
    labels: std.ArrayList(catalog.Label) = .empty,
    cover: ?[]const u8 = null,
    values: Names = .empty,
    presence: Names = .empty,
    paths: std.ArrayList(PropertyPath) = .empty,
    full: bool = false,
    labels_full: bool = false,
    keys: bool = false,
};
const RelationshipPlan = struct { table: []const u8, index: []const u8, stored_type: []const u8, plan: NodePlan = .{} };
const Need = enum { value, identity, nullness, labels, keys, evaluate };
const Selector = union(enum) { field: []const u8, item: i64, element };
const Entities = std.StringHashMapUnmanaged(Binding);
const Derived = struct {
    expression: *const ast.Expr,
    entities: Entities,
    derived: std.StringHashMapUnmanaged(*Derived),
    selectors: []const Selector = &.{},
    seen: u8 = 0,
    used: bool = false,
    paths: Names = .empty,
};
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
    entities: Entities = .empty,
    derived: std.StringHashMapUnmanaged(*Derived) = .empty,
    terms: std.ArrayList(*Derived) = .empty,
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
        if (node.alias) |name| if (self.derived.contains(name)) return self.table_names;
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
    fn forwards(self: *Plan, expression: ast.Expr) bool {
        return switch (expression.value) {
            .variable => |name| self.entities.contains(name) or self.derived.contains(name),
            .property => |property_value| self.forwards(property_value.base.*),
            .index => |index| self.forwards(index.base.*),
            .slice => |slice| self.forwards(slice.base.*),
            .list => |items| for (items) |item| {
                if (self.forwards(item.*)) break true;
            } else false,
            .map => |entries| for (entries) |entry| {
                if (self.forwards(entry.value.*)) break true;
            } else false,
            .map_projection => |projection| self.forwards(projection.base.*),
            .case_when => |case| for (case.branches) |branch| {
                if (self.forwards(branch.value.*)) break true;
            } else if (case.otherwise) |other| self.forwards(other.*) else false,
            .call => |call| if (std.ascii.eqlIgnoreCase(call.name, "coalesce") or std.ascii.eqlIgnoreCase(call.name, "head") or std.ascii.eqlIgnoreCase(call.name, "last") or std.ascii.eqlIgnoreCase(call.name, "properties")) for (call.args) |arg| {
                if (self.forwards(arg.*)) break true;
            } else false else false,
            else => false,
        };
    }
    fn capture(self: *Plan, expression: *const ast.Expr, selectors: []const Selector) !*Derived {
        const term = try self.a.create(Derived);
        term.* = .{ .expression = expression, .entities = try self.entities.clone(self.a), .derived = try self.derived.clone(self.a), .selectors = selectors };
        try self.terms.append(self.a, term);
        return term;
    }
    fn consumeDerived(self: *Plan, term: *Derived, need: Need, selectors: []const Selector) anyerror!void {
        if (need != .evaluate) term.used = true;
        if (selectors.len == 0) {
            const bit = @as(u8, 1) << @intCast(@backingInt(need));
            if (term.seen & bit != 0) return;
            term.seen |= bit;
        } else {
            const key = try std.json.Stringify.valueAlloc(self.a, .{ need, selectors }, .{});
            if ((try term.paths.getOrPut(self.a, key)).found_existing) return;
        }
        const entities = self.entities;
        const derived = self.derived;
        // Captures are immutable. Pattern/local scopes can grow or shadow the
        // active maps, so never let their reallocations invalidate a capture.
        const active_entities = try term.entities.clone(self.a);
        const active_derived = try term.derived.clone(self.a);
        self.entities = active_entities;
        self.derived = active_derived;
        defer {
            self.entities = entities;
            self.derived = derived;
        }
        const path = try self.a.alloc(Selector, term.selectors.len + selectors.len);
        @memcpy(path[0..term.selectors.len], term.selectors);
        @memcpy(path[term.selectors.len..], selectors);
        try self.consume(term.expression.*, need, path);
    }
    fn prepend(self: *Plan, selector: Selector, rest: []const Selector) ![]const Selector {
        const result = try self.a.alloc(Selector, rest.len + 1);
        result[0] = selector;
        @memcpy(result[1..], rest);
        return result;
    }
    fn bindingNeed(self: *Plan, binding: Binding, need: Need, selectors: []const Selector) !void {
        var parts: std.ArrayList([]const u8) = .empty;
        try parts.appendSlice(self.a, binding.prefix);
        for (selectors) |selector| {
            if (selector != .field) {
                // JSON arrays/dynamic members need their owning property, not
                // unrelated properties of the enclosing graph entity.
                if (parts.items.len != 0) return self.propertyPath(binding, parts.items, false);
                return self.whole(binding);
            }
            try parts.append(self.a, selector.field);
        }
        if (parts.items.len != 0) {
            if (need == .evaluate) {
                try self.recordPath(binding, parts.items, false, true);
            } else if (need == .keys) {
                const path: PropertyPath = .{ .parts = parts.items, .presence = false, .shape = true, .keys = true };
                if (binding.used) |used| used.* = true;
                try self.fields.put(self.a, parts.items[0], {});
                if (binding.kind == .node) {
                    try self.node_paths.append(self.a, path);
                    for (binding.tables) |table| try self.tables.get(table).?.paths.append(self.a, path);
                } else if (binding.kind == .relationship) {
                    try self.edge_paths.append(self.a, path);
                    for (binding.relationship_ids) |id| try self.relationship_plans[id].plan.paths.append(self.a, path);
                } else try self.whole(binding);
            } else try self.propertyPath(binding, parts.items, need == .nullness);
            return;
        }
        if (binding.used) |used| used.* = true;
        switch (need) {
            .value => try self.whole(binding),
            .labels => self.labelsFor(binding),
            .keys => {
                // Global adapters remain conservative; capable owners receive
                // a root key-shape plan with independent value overrides.
                self.full = true;
                if (binding.kind == .node) {
                    self.nodes_full = true;
                    for (binding.tables) |table| self.tables.get(table).?.keys = true;
                } else if (binding.kind == .relationship) {
                    self.edges_full = true;
                    for (binding.relationship_ids) |id| self.relationship_plans[id].plan.keys = true;
                } else try self.whole(binding);
            },
            .identity, .nullness, .evaluate => {},
        }
    }
    fn consume(self: *Plan, expression: ast.Expr, need: Need, selectors: []const Selector) anyerror!void {
        if (!try self.observe(expression, need, selectors)) try self.walk(expression);
    }
    fn observe(self: *Plan, expression: ast.Expr, need: Need, selectors: []const Selector) anyerror!bool {
        switch (expression.value) {
            .variable => |name| {
                if (self.derived.get(name)) |term| try self.consumeDerived(term, need, selectors) else if (self.entities.get(name)) |binding| try self.bindingNeed(binding, need, selectors);
                return true;
            },
            .property => |property_value| {
                try self.consume(property_value.base.*, need, try self.prepend(.{ .field = property_value.name }, selectors));
                return true;
            },
            .index => |index| {
                try self.walk(index.index.*);
                const selector: Selector = if (index.index.value == .literal and index.index.value.literal == .string) .{ .field = index.index.value.literal.string } else if (index.index.value == .literal and index.index.value.literal == .integer) .{ .item = index.index.value.literal.integer } else .element;
                try self.consume(index.base.*, need, try self.prepend(selector, selectors));
                return true;
            },
            .list => |items| {
                for (items, 0..) |item, i| {
                    var selected = selectors.len == 0;
                    if (selectors.len != 0) selected = switch (selectors[0]) {
                        .element => true,
                        .item => |index| @as(i128, if (index < 0) @as(i128, items.len) + index else index) == i,
                        .field => false,
                    };
                    try self.consume(item.*, if (selected) (if (selectors.len == 0 and (need == .nullness or need == .keys)) .identity else need) else .identity, if (selected and selectors.len != 0) selectors[1..] else &.{});
                }
                return true;
            },
            .map => |entries| {
                for (entries) |entry| {
                    const selected = selectors.len == 0 or selectors[0] == .element or (selectors[0] == .field and std.mem.eql(u8, selectors[0].field, entry.name));
                    try self.consume(entry.value.*, if (selected) (if (selectors.len == 0 and (need == .nullness or need == .keys)) .identity else need) else .identity, if (selected and selectors.len != 0) selectors[1..] else &.{});
                }
                return true;
            },
            .case_when => |case| {
                try self.walk(case.operand);
                for (case.branches) |branch| {
                    try self.walk(branch.condition.*);
                    try self.consume(branch.value.*, need, selectors);
                }
                if (case.otherwise) |other| try self.consume(other.*, need, selectors);
                return true;
            },
            .unary => |unary| if (unary.op == .is_null or unary.op == .is_not_null) {
                try self.consume(unary.operand.*, .nullness, &.{});
                return true;
            },
            .binary => |binary| switch (binary.op) {
                .eq, .neq, .lt, .lte, .gt, .gte, .in => {
                    try self.consume(binary.left.*, .identity, &.{});
                    try self.consume(binary.right.*, .identity, &.{});
                    return true;
                },
                else => {},
            },
            .comparison => |comparison| {
                for (comparison.operands) |operand| try self.consume(operand.*, .identity, &.{});
                return true;
            },
            .call => |call| {
                if (std.ascii.eqlIgnoreCase(call.name, "coalesce")) {
                    for (call.args) |arg| try self.consume(arg.*, need, selectors);
                    return true;
                }
                if ((std.ascii.eqlIgnoreCase(call.name, "head") or std.ascii.eqlIgnoreCase(call.name, "last")) and call.args.len == 1) {
                    try self.consume(call.args[0].*, need, try self.prepend(.{ .item = if (std.ascii.eqlIgnoreCase(call.name, "head")) 0 else -1 }, selectors));
                    return true;
                }
                if (std.ascii.eqlIgnoreCase(call.name, "properties") and call.args.len == 1) {
                    try self.consume(call.args[0].*, if (need == .keys) .keys else .value, selectors);
                    return true;
                }
                const argument_need: ?Need = if (std.ascii.eqlIgnoreCase(call.name, "labels")) .labels else if (std.ascii.eqlIgnoreCase(call.name, "keys")) .keys else if (std.ascii.eqlIgnoreCase(call.name, "count")) (if (call.distinct) .identity else .nullness) else if (std.ascii.eqlIgnoreCase(call.name, "antfly.documentKey") or std.ascii.eqlIgnoreCase(call.name, "type") or std.ascii.eqlIgnoreCase(call.name, "length")) .identity else null;
                if (argument_need) |argument_observation| {
                    for (call.args) |arg| try self.consume(arg.*, argument_observation, &.{});
                    return true;
                }
            },
            else => {},
        }
        if (selectors.len != 0) {
            // Unknown producers and dynamic entity selectors keep complete
            // inputs. No private partial value can cross an opaque boundary.
            try self.walk(expression);
            return true;
        }
        return false;
    }
    fn clauses(self: *Plan, items: []const ast.Clause, internal_return: bool) anyerror!void {
        for (items) |clause| switch (clause.operation) {
            .project => |p| {
                var next = if (p.value.wildcard) try self.entities.clone(self.a) else std.StringHashMapUnmanaged(Binding).empty;
                var next_derived = if (p.value.wildcard) try self.derived.clone(self.a) else std.StringHashMapUnmanaged(*Derived).empty;
                // Grouping observes entity identities and primitive values recursively.
                const grouping = for (p.value.items) |item| {
                    if (@import("expressions.zig").aggregates(item.expression)) break true;
                } else false;
                var visible = try self.entities.clone(self.a);
                var visible_derived = try self.derived.clone(self.a);
                for (p.value.items) |item| {
                    const name = item.alias orelse try @import("expressions.zig").outputName(self.a, self.source, item.expression);
                    _ = next.remove(name);
                    _ = next_derived.remove(name);
                    // Every output name shadows its input, including implicit
                    // map-projection names whose value has no entity lineage.
                    _ = visible.remove(name);
                    _ = visible_derived.remove(name);
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
                    } else if ((!p.final or internal_return) and self.forwards(item.expression.*)) {
                        try next_derived.put(self.a, name, try self.capture(item.expression, &.{}));
                        try self.consume(item.expression.*, if (p.value.distinct or grouping) .identity else .evaluate, &.{});
                    } else try self.walk(item.expression.*);
                }
                // ORDER BY can see input bindings as well as projection aliases.
                // Keep both scopes while collecting requirements, then retire
                // hidden bindings before the next clause.
                var names_iter = next.keyIterator();
                while (names_iter.next()) |key| try visible.put(self.a, key.*, next.get(key.*).?);
                var derived_iter = next_derived.iterator();
                while (derived_iter.next()) |entry| try visible_derived.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                self.entities = visible;
                self.derived = visible_derived;
                try self.walk(p.value.predicate);
                for (p.value.order) |order| {
                    const binding = if (order.expression.value == .variable) self.entities.get(order.expression.value.variable) else null;
                    if (binding == null or binding.?.prefix.len != 0) try self.consume(order.expression.*, .identity, &.{});
                }
                try self.walk(p.value.skip);
                try self.walk(p.value.limit);
                if (p.final and !internal_return and p.value.wildcard) {
                    var bindings = next.valueIterator();
                    while (bindings.next()) |binding| try self.whole(binding.*);
                    var terms = next_derived.valueIterator();
                    while (terms.next()) |term| try self.consumeDerived(term.*, .value, &.{});
                }
                self.entities = next;
                self.derived = next_derived;
            },
            .subquery => |inner| {
                var outer = try self.entities.clone(self.a);
                var outer_derived = try self.derived.clone(self.a);
                try self.clauses(inner, true);
                // CALL's RETURN is an internal binding boundary, not a public
                // entity observer. Export only its final projected lineage.
                if (inner.len != 0 and inner[inner.len - 1].operation == .project and inner[inner.len - 1].operation.project.final) {
                    var exports = self.entities.iterator();
                    while (exports.next()) |entry| {
                        _ = outer_derived.remove(entry.key_ptr.*);
                        try outer.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                    }
                    var terms = self.derived.iterator();
                    while (terms.next()) |entry| {
                        _ = outer.remove(entry.key_ptr.*);
                        try outer_derived.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
                    }
                }
                self.entities = outer;
                self.derived = outer_derived;
            },
            .call => |procedure| {
                try self.walk(procedure.args);
                if (std.mem.eql(u8, procedure.name, "antfly.search")) {
                    self.unlabeled = true;
                    for (procedure.yields) |yield| {
                        _ = self.entities.remove(yield.alias);
                        _ = self.derived.remove(yield.alias);
                        if (std.mem.eql(u8, yield.name, "node")) {
                            try self.entities.put(self.a, yield.alias, .{ .kind = .node, .tables = try self.searchDomain(procedure.args) });
                            self.unlabeled = true;
                        }
                    }
                    try self.walk(procedure.predicate);
                } else self.complete();
            },
            .unwind => |u| {
                const term = try self.capture(u.expression, &.{.element});
                try self.consume(u.expression.*, .identity, &.{});
                _ = self.entities.remove(u.alias);
                try self.derived.put(self.a, u.alias, term);
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
                if (!self.entities.contains(name) and !self.derived.contains(name)) try self.entities.put(self.a, name, .{ .kind = .node, .tables = tables });
            };
        }
        if (T == ast.Node or T == ast.Edge or T == ast.Path) {
            if (value.alias) |name| if (!self.entities.contains(name) and !self.derived.contains(name)) try self.entities.put(self.a, name, .{
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
            const initial_derived = try self.derived.clone(self.a);
            try self.clauses(value.clauses, false);
            for (value.unions) |arm| {
                self.entities = try initial.clone(self.a);
                self.derived = try initial_derived.clone(self.a);
                try self.clauses(arm.clauses, false);
            }
            return;
        }
        if (T == ast.CollectionFilter) {
            try self.walk(value.initial);
            const outer = try self.entities.clone(self.a);
            const outer_derived = try self.derived.clone(self.a);
            _ = self.entities.remove(value.alias);
            _ = self.derived.remove(value.alias);
            if (value.accumulator) |name| {
                _ = self.entities.remove(name);
                _ = self.derived.remove(name);
            }
            try self.walk(value.predicate);
            try self.walk(value.projection);
            self.entities = outer;
            self.derived = outer_derived;
            return;
        }
        if (T == ast.Expr) {
            if (try self.observe(value, .value, &.{})) return;
            switch (value.value) {
                .comprehension => |local| {
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    const outer_derived = try self.derived.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    _ = self.derived.remove(local.alias);
                    try self.walk(local.predicate);
                    try self.walk(local.projection);
                    self.entities = outer;
                    self.derived = outer_derived;
                    return;
                },
                .list_predicate => |local| {
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    const outer_derived = try self.derived.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    _ = self.derived.remove(local.alias);
                    try self.walk(local.predicate.*);
                    self.entities = outer;
                    self.derived = outer_derived;
                    return;
                },
                .reduce => |local| {
                    try self.walk(local.initial.*);
                    try self.walk(local.input.*);
                    const outer = try self.entities.clone(self.a);
                    const outer_derived = try self.derived.clone(self.a);
                    _ = self.entities.remove(local.alias);
                    _ = self.derived.remove(local.alias);
                    _ = self.entities.remove(local.accumulator);
                    _ = self.derived.remove(local.accumulator);
                    try self.walk(local.projection.*);
                    self.entities = outer;
                    self.derived = outer_derived;
                    return;
                },
                .pattern_comprehension => |pattern| {
                    // The comprehension introduces entity bindings which are
                    // invisible to the enclosing projection. Their predicates
                    // and values still contribute storage field requirements.
                    const outer = try self.entities.clone(self.a);
                    const outer_derived = try self.derived.clone(self.a);
                    try self.names(pattern.path);
                    try self.walk(pattern.path);
                    try self.walk(pattern.predicate);
                    try self.walk(pattern.projection.*);
                    self.entities = outer;
                    self.derived = outer_derived;
                    return;
                },
                .exists => |inner| {
                    const outer = try self.entities.clone(self.a);
                    const outer_derived = try self.derived.clone(self.a);
                    try self.clauses(inner, false);
                    self.entities = outer;
                    self.derived = outer_derived;
                    return;
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
    var term_index: usize = 0;
    while (term_index < plan.terms.items.len) : (term_index += 1) {
        const term = plan.terms.items[term_index];
        if (!term.used) try plan.consumeDerived(term, .identity, &.{});
    }
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
                        for (slot.value_ptr.items) |prior| {
                            if (@import("value.zig").Value.eql(prior, entry.value)) break;
                        } else try slot.value_ptr.append(a, entry.value);
                    }
                }
            }
        }
        var node_entity = try withKeys(a, try entity(a, item.full, item.values, item.presence, item.paths.items), item.keys);
        if (node_entity.fields) |fields| {
            var names: Names = .empty;
            for (fields) |name| try names.put(a, name, {});
            var projections: std.ArrayList(Projection) = .empty;
            try projections.appendSlice(a, node_entity.projection);
            var iter = predicates.iterator();
            while (iter.next()) |entry| {
                try names.put(a, entry.key_ptr.*, {});
                const fingerprints = try a.alloc(predicate_fingerprint.Digest, entry.value_ptr.items.len);
                for (entry.value_ptr.items, fingerprints) |candidate, *fingerprint| fingerprint.* = predicate_fingerprint.value(candidate);
                try projections.append(a, .{ .name = entry.key_ptr.*, .mode = .predicate, .fingerprints = fingerprints, .alternatives = try std.json.parseFromSliceLeaky([]const std.json.Value, a, try std.json.Stringify.valueAlloc(a, entry.value_ptr.items, .{}), .{ .allocate = .alloc_always }) });
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
    for (plan.relationship_plans, domains) |domain_value, *out| out.* = .{ .table = domain_value.table, .index = domain_value.index, .stored_type = domain_value.stored_type, .entity = try withKeys(a, try entity(a, domain_value.plan.full, domain_value.plan.values, domain_value.plan.presence, domain_value.plan.paths.items), domain_value.plan.keys) };
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
    var keys_only = false;
    for (paths) |path| {
        if (path.parts.len == depth) {
            if (!path.presence and !path.shape) return .{ .name = name, .mode = .value };
            validate = validate or path.presence;
            shape = shape or path.shape;
            keys_only = keys_only or path.keys;
        } else try children.put(a, path.parts[depth], {});
    }
    if (children.count() == 0) return if (shape) .{ .name = name, .mode = .children, .validate = validate, .keys = keys_only } else .{ .name = name, .mode = .presence };
    const keys = try sortedNames(a, children);
    const out = try a.alloc(Projection, keys.len);
    for (keys, out) |key, *child| {
        var selected: std.ArrayList(PropertyPath) = .empty;
        for (paths) |path| if (path.parts.len > depth and std.mem.eql(u8, path.parts[depth], key)) try selected.append(a, path);
        child.* = try projectTree(a, key, selected.items, depth + 1);
    }
    return .{ .name = name, .mode = .children, .children = out, .validate = validate, .keys = keys_only };
}

/// Root key enumeration is private and independent of explicit field values.
pub fn rootKeys(projections: []const Projection) bool {
    for (projections) |projection| if (projection.root and projection.keys) return true;
    return false;
}
fn withKeys(a: A, input: Entity, keys: bool) !Entity {
    if (!keys or input.fields == null) return input;
    var result = input;
    var projection: std.ArrayList(Projection) = .empty;
    try projection.appendSlice(a, input.projection);
    for (input.fields.?) |name| {
        const already = for (input.projection) |item| {
            if (!item.root and std.mem.eql(u8, item.name, name)) break true;
        } else false;
        const presence = for (input.presence) |field| {
            if (std.mem.eql(u8, field, name)) break true;
        } else false;
        if (!already and !presence) try projection.append(a, .{ .name = name, .mode = .value });
    }
    try projection.append(a, .{ .name = "", .mode = .children, .root = true, .keys = true });
    result.projection = projection.items;
    return result;
}
