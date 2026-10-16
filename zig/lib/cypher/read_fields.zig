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

//! Conservative statement-wide property liveness. Unknown entity uses retain
//! complete values; only statically named scalar reads admit column pruning.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const A = std.mem.Allocator;
const Plan = struct {
    a: A,
    entities: std.StringHashMapUnmanaged(void) = .empty,
    fields: std.StringHashMapUnmanaged(void) = .empty,
    full: bool = false,
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
        if (T == ast.Expr) {
            switch (value.value) {
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
        if (T == ast.Projection) if (value.wildcard) {
            self.full = true;
        };
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
    try plan.names(query);
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
