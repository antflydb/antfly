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
pub const Entity = struct {
    /// Null means the complete entity payload is observable.
    fields: ?[]const []const u8 = null,
    presence: []const []const u8 = &.{},
};
pub const Metadata = struct { headers: std.json.Value, properties: []const @import("value.zig").Entry };
pub const PreparedMetadata = struct {
    headers: std.json.Value,
    context: *anyopaque,
    project: *const fn (*anyopaque, A, f64, Entity) anyerror![]const @import("value.zig").Entry,
};
pub const Demand = struct {
    nodes: Entity = .{},
    relationships: Entity = .{},
    /// Null retains every catalog membership (labels() or a returned node).
    labels: ?[]const []const u8 = null,
};
const Kind = enum { node, relationship, path };
const Names = std.StringHashMapUnmanaged(void);
const Plan = struct {
    a: A,
    entities: std.StringHashMapUnmanaged(Kind) = .empty,
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
    }
    fn whole(self: *Plan, kind: Kind) void {
        self.full = true;
        switch (kind) {
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
    }
    fn field(self: *Plan, kind: Kind, name: []const u8, presence: bool) !void {
        try self.fields.put(self.a, name, {});
        switch (kind) {
            .node => if (presence) try self.node_presence.put(self.a, name, {}) else try self.node_values.put(self.a, name, {}),
            .relationship => if (presence) try self.edge_presence.put(self.a, name, {}) else try self.edge_values.put(self.a, name, {}),
            .path => self.complete(),
        }
    }
    fn clauses(self: *Plan, items: []const ast.Clause) anyerror!void {
        for (items) |clause| switch (clause.operation) {
            .project => |p| {
                var next = if (p.value.wildcard) try self.entities.clone(self.a) else std.StringHashMapUnmanaged(Kind).empty;
                for (p.value.items) |item| {
                    const forwarded = item.expression.value == .variable and self.entities.contains(item.expression.value.variable);
                    if (forwarded and !p.final) {
                        try next.put(self.a, item.alias orelse item.expression.value.variable, self.entities.get(item.expression.value.variable).?);
                    } else try self.walk(item.expression.*);
                }
                // ORDER BY can see input bindings as well as projection aliases.
                // Keep both scopes while collecting requirements, then retire
                // hidden bindings before the next clause.
                var visible = try self.entities.clone(self.a);
                var names_iter = next.keyIterator();
                while (names_iter.next()) |key| try visible.put(self.a, key.*, next.get(key.*).?);
                self.entities = visible;
                try self.walk(p.value.predicate);
                try self.walk(p.value.order);
                try self.walk(p.value.skip);
                try self.walk(p.value.limit);
                if (p.final and p.value.wildcard and next.count() != 0) self.complete();
                self.entities = next;
            },
            .subquery => |inner| {
                const outer = try self.entities.clone(self.a);
                try self.clauses(inner);
                // Returning entities across a CALL boundary already requires
                // complete values. Preserve outer bindings for later clauses.
                self.entities = outer;
            },
            .call => |procedure| {
                try self.walk(procedure.args);
                if (std.mem.eql(u8, procedure.name, "antfly.search")) {
                    self.unlabeled = true;
                    for (procedure.yields) |yield| {
                        _ = self.entities.remove(yield.alias);
                        if (std.mem.eql(u8, yield.name, "node")) {
                            try self.entities.put(self.a, yield.alias, .node);
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
        if (T == ast.Node or T == ast.Edge or T == ast.Path) {
            if (value.alias) |name| try self.entities.put(self.a, name, if (T == ast.Node) .node else if (T == ast.Edge) .relationship else .path);
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
            try self.clauses(value.clauses);
            for (value.unions) |arm| {
                self.entities = try initial.clone(self.a);
                try self.clauses(arm.clauses);
            }
            return;
        }
        if (T == ast.Expr) {
            switch (value.value) {
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
                    try self.clauses(inner);
                    self.entities = outer;
                    return;
                },
                .variable => |name| {
                    if (self.entities.get(name)) |kind| self.whole(kind);
                    return;
                },
                .property => |p| {
                    if (p.base.value == .variable and self.entities.contains(p.base.value.variable)) {
                        try self.field(self.entities.get(p.base.value.variable).?, p.name, false);
                        return;
                    }
                },
                .map_projection => |projection| {
                    if (projection.base.value == .variable) if (self.entities.get(projection.base.value.variable)) |kind| {
                        for (projection.items) |item| switch (item) {
                            .property => |name| try self.field(kind, name, false),
                            .entry => |entry| try self.walk(entry.value.*),
                            .variable => |name| if (self.entities.get(name)) |other| self.whole(other),
                            .all => self.whole(kind),
                        };
                        return;
                    };
                },
                .unary => |u| {
                    if ((u.op == .is_null or u.op == .is_not_null) and u.operand.value == .property) {
                        const p = u.operand.value.property;
                        if (p.base.value == .variable) if (self.entities.get(p.base.value.variable)) |kind| {
                            try self.field(kind, p.name, true);
                            return;
                        };
                    }
                },
                .call => |c| {
                    if (std.ascii.eqlIgnoreCase(c.name, "count") and !c.distinct and c.args.len == 1 and c.args[0].value == .property) {
                        const p = c.args[0].value.property;
                        if (p.base.value == .variable) if (self.entities.get(p.base.value.variable)) |kind| {
                            try self.field(kind, p.name, true);
                            return;
                        };
                    }
                    const identity_only = std.ascii.eqlIgnoreCase(c.name, "antfly.documentKey") or std.ascii.eqlIgnoreCase(c.name, "labels") or
                        std.ascii.eqlIgnoreCase(c.name, "type") or std.ascii.eqlIgnoreCase(c.name, "length") or std.ascii.eqlIgnoreCase(c.name, "count");
                    if (std.ascii.eqlIgnoreCase(c.name, "labels")) self.labels_full = true;
                    if (identity_only) {
                        for (c.args) |arg| if (arg.value != .variable) try self.walk(arg.*);
                        return;
                    }
                },
                else => {},
            }
        }
        if (T == ast.Node or T == ast.Edge) for (value.properties) |entry| try self.field(if (T == ast.Node) .node else .relationship, entry.name, false);
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
    var plan: Plan = .{ .a = a };
    try plan.walk(query);
    if (plan.edges) for (graph.relationships) |rel| {
        if (plan.all_types or plan.types.contains(rel.name)) {
            try plan.labels.put(a, rel.source_label, {});
            try plan.labels.put(a, rel.target_label, {});
        }
    };
    var membership: Names = .empty;
    for (graph.labels) |label| {
        var observed = plan.labels_full or plan.labels.contains(label.name);
        if (plan.unlabeled and !observed) {
            const cover = for (graph.labels) |candidate| {
                if (std.mem.eql(u8, candidate.table, label.table) and candidate.predicate.len == 0) break candidate.name;
            } else null;
            observed = if (cover) |name| std.mem.eql(u8, name, label.name) else true;
        }
        if (observed) {
            try membership.put(a, label.name, {});
            for (label.predicate) |entry| try plan.node_values.put(a, entry.name, {});
        }
        // Legacy adapters retain complete membership semantics.
        for (label.predicate) |entry| try plan.fields.put(a, entry.name, {});
    }
    return .{
        .fields = if (plan.full) null else try names(a, plan.fields),
        .demand = .{
            .nodes = try entity(a, plan.nodes_full, plan.node_values, plan.node_presence),
            .relationships = try entity(a, plan.edges_full, plan.edge_values, plan.edge_presence),
            .labels = if (plan.labels_full) null else try names(a, membership),
        },
    };
}
fn names(a: A, input: Names) ![]const []const u8 {
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
fn entity(a: A, full: bool, values: Names, presence: Names) !Entity {
    if (full) return .{};
    var fields = try values.clone(a);
    var only_presence: Names = .empty;
    var iterator = presence.keyIterator();
    while (iterator.next()) |name| {
        try fields.put(a, name.*, {});
        if (!values.contains(name.*)) try only_presence.put(a, name.*, {});
    }
    return .{ .fields = try names(a, fields), .presence = try names(a, only_presence) };
}
