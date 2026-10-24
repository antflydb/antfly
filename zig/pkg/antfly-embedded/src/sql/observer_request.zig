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

//! Own and forward scalar observer obligations through proved identity frames.
const std = @import("std");
const catalog = @import("catalog.zig");
const A = std.mem.Allocator;
pub const Forward = struct {
    kind: ?@import("cypher").runtime.NodeCursor.ScalarObserver = null,
    field: ?[]const u8 = null,
    keys: []const []const u8 = &.{},
    filter: ?catalog.Scan.GraphFilter = null,
};
fn name(input: []const u8, from: []const []const u8, to: []const []const u8) ?[]const u8 {
    for (from, to) |old, new| if (std.mem.eql(u8, input, old)) return new;
    return null;
}
fn filterMap(a: A, input: catalog.Scan.GraphFilter, from: []const []const u8, to: []const []const u8) anyerror!?catalog.Scan.GraphFilter {
    return switch (input) {
        .comparison => |condition| blk: {
            const mapped = name(condition.column, from, to) orelse return null;
            const datum = try @import("operators.zig").cloneDatum(a, condition.datum());
            break :blk .{ .comparison = .{ .column = try a.dupe(u8, mapped), .op = condition.op, .value = datum.value, .operand = datum } };
        },
        .all, .any => |children| blk: {
            const mapped = try a.alloc(catalog.Scan.GraphFilter, children.len);
            for (children, mapped) |child, *out| out.* = (try filterMap(a, child, from, to)) orelse return null;
            break :blk if (input == .all) .{ .all = mapped } else .{ .any = mapped };
        },
    };
}
pub fn forward(a: A, request: catalog.AggregateWeight, from: []const []const u8, to: []const []const u8) !Forward {
    const kind = request.observer orelse return .{};
    const source = request.observer_field orelse if (request.fields.len == 1) request.fields[0] else return .{};
    const field = name(source, from, to) orelse return .{};
    const keys = try a.alloc([]const u8, request.observer_keys.len);
    for (request.observer_keys, keys) |key, *out| out.* = try a.dupe(u8, name(key, from, to) orelse return .{});
    return .{ .kind = kind, .field = try a.dupe(u8, field), .keys = keys, .filter = if (request.observer_filter) |predicate| (try filterMap(a, predicate, from, to)) orelse return .{} else null };
}
