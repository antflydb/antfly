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

//! Planning-only native connection ownership demand. Never evaluate a scalar
//! or open a cursor to decide whether an execution needs command authority.
const std = @import("std");
const describe = @import("describe.zig");
const scalar = @import("scalar.zig");
const relation = @import("relation_binding.zig");

fn programs(values: []const scalar.Program) bool {
    for (values) |value| if (value.uses_advisory) return true;
    return false;
}
fn optionalPrograms(values: []const ?scalar.Program) bool {
    for (values) |value| if (value) |program| if (program.uses_advisory) return true;
    return false;
}
fn scalars(value: @import("bound_scalars.zig").Bound) bool {
    return (if (value.predicate) |program| program.uses_advisory else false) or optionalPrograms(value.projections) or optionalPrograms(value.orders);
}
const Walk = struct {
    a: std.mem.Allocator,
    seen: std.AutoHashMapUnmanaged(*const relation.Node, void) = .empty,
    fn binding(self: *Walk, value: describe.BoundStatement) anyerror!bool {
        if (scalars(value.scalars)) return true;
        if (value.aggregate) |aggregate| {
            if (scalars(aggregate.input) or programs(aggregate.outputs) or programs(aggregate.orders) or
                (if (aggregate.having) |program| program.uses_advisory else false)) return true;
        }
        if (value.window) |window| {
            if (try self.binding(window.input.*) or programs(window.outputs) or programs(window.orders)) return true;
        }
        if (value.relation) |bound| if (try self.node(bound.root)) return true;
        return false;
    }
    fn node(self: *Walk, value: *const relation.Node) anyerror!bool {
        const entry = try self.seen.getOrPut(self.a, value);
        if (entry.found_existing) return false;
        return switch (value.operation) {
            .query => |query| try self.binding(query.binding) or try self.node(query.source),
            .join => |join| (if (join.condition) |program| program.uses_advisory else false) or programs(join.left_keys) or programs(join.right_keys) or try self.node(join.left) or try self.node(join.right),
            .apply => |apply| (if (apply.condition) |program| program.uses_advisory else false) or (if (apply.demand) |program| program.uses_advisory else false) or try self.node(apply.left) or try self.node(apply.right),
            .unnest => |unnest| programs(unnest.arrays) or try self.node(unnest.source),
            .recursive => |recursive| try self.node(recursive.seed) or try self.node(recursive.step),
            .materialized_ref => |source| try self.node(source),
            .set => |set| try self.node(set.left) or try self.node(set.right),
            .values => |arms| blk: {
                for (arms) |arm| if (try self.node(arm)) break :blk true;
                break :blk false;
            },
            .singleton, .statement_cte, .recursive_ref, .outer_ref, .scan, .literal_rows, .prepared_rows => false,
        };
    }
};

pub fn required(a: std.mem.Allocator, binding: describe.BoundStatement) !bool {
    var walk: Walk = .{ .a = a };
    defer walk.seen.deinit(a);
    return walk.binding(binding);
}
