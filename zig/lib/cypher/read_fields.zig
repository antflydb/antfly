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
const Plan = struct {
    a: A,
    entities: std.StringHashMapUnmanaged(void) = .empty,
    fields: std.StringHashMapUnmanaged(void) = .empty,
    full: bool = false,
    fn clauses(self: *Plan, items: []const ast.Clause) anyerror!void {
        for (items) |clause| switch (clause.operation) {
            .project => |p| {
                var next = if (p.value.wildcard) try self.entities.clone(self.a) else std.StringHashMapUnmanaged(void).empty;
                for (p.value.items) |item| {
                    const forwarded = item.expression.value == .variable and self.entities.contains(item.expression.value.variable);
                    if (forwarded and !p.final) {
                        try next.put(self.a, item.alias orelse item.expression.value.variable, {});
                    } else try self.walk(item.expression.*);
                }
                // ORDER BY can see input bindings as well as projection aliases.
                // Keep both scopes while collecting requirements, then retire
                // hidden bindings before the next clause.
                var visible = try self.entities.clone(self.a);
                var names_iter = next.keyIterator();
                while (names_iter.next()) |key| try visible.put(self.a, key.*, {});
                self.entities = visible;
                try self.walk(p.value.predicate);
                try self.walk(p.value.order);
                try self.walk(p.value.skip);
                try self.walk(p.value.limit);
                if (p.final and p.value.wildcard and next.count() != 0) self.full = true;
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
                    for (procedure.yields) |yield| {
                        _ = self.entities.remove(yield.alias);
                        if (std.mem.eql(u8, yield.name, "node")) try self.entities.put(self.a, yield.alias, {});
                    }
                    try self.walk(procedure.predicate);
                } else self.full = true;
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
            if (value.alias) |name| try self.entities.put(self.a, name, {});
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
                    if (self.entities.contains(name)) self.full = true;
                    return;
                },
                .property => |p| {
                    if (p.base.value == .variable and self.entities.contains(p.base.value.variable)) {
                        try self.fields.put(self.a, p.name, {});
                        return;
                    }
                },
                .call => |c| {
                    const identity_only = std.ascii.eqlIgnoreCase(c.name, "antfly.documentKey") or std.ascii.eqlIgnoreCase(c.name, "labels") or
                        std.ascii.eqlIgnoreCase(c.name, "type") or std.ascii.eqlIgnoreCase(c.name, "length") or std.ascii.eqlIgnoreCase(c.name, "count");
                    if (identity_only) {
                        for (c.args) |arg| if (arg.value != .variable) try self.walk(arg.*);
                        return;
                    }
                },
                else => {},
            }
        }
        if (T == ast.Node or T == ast.Edge) for (value.properties) |entry| try self.fields.put(self.a, entry.name, {});
        // Procedure yields may contain entities with implementation-defined
        // aliases. Preserve complete results across this extension boundary.
        if (T == ast.Procedure) self.full = true;
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
pub fn required(a: A, query: ast.Query, graph: catalog.Graph) !?[]const []const u8 {
    var plan: Plan = .{ .a = a };
    try plan.walk(query);
    if (plan.full) return null;
    for (graph.labels) |label| for (label.predicate) |entry| try plan.fields.put(a, entry.name, {});
    const fields = try a.alloc([]const u8, plan.fields.count());
    var keys = plan.fields.keyIterator();
    for (fields) |*field| field.* = keys.next().?.*;
    std.mem.sort([]const u8, fields, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    return fields;
}
