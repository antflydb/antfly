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

const std = @import("std");
const catalog = @import("catalog.zig");
const v = @import("value.zig");
test "graph definition binds equality label predicates and rejects ambiguous mappings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph = try catalog.parseDefinition(a, "people", 2, 7, "{\"labels\":[{\"name\":\"Employee\",\"table\":\"people\",\"where\":{\"kind\":\"employee\"}}]}");
    const Counter = struct {
        fn charge(_: *anyopaque, _: usize) !void {}
    };
    var token: u8 = 0;
    var work: @import("value_work.zig").Work = .{ .a = a, .ptr = &token, .charge = Counter.charge };
    const employee: v.Node = .{ .identity = .{ .table = "people", .key = "a" }, .properties = &.{.{ .name = "kind", .value = .{ .string = "employee" } }} };
    try std.testing.expect(try graph.admits(&employee, "Employee", &work));
    var other = employee;
    other.identity.table = "other";
    try std.testing.expect(!try graph.admits(&other, "Employee", &work));
    try std.testing.expectError(error.DuplicateGraphLabel, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"a\"},{\"name\":\"D\",\"table\":\"b\"}]}"));
    try std.testing.expectError(error.InvalidGraphDefinition, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"a\",\"where\":false}]}"));
    const scoped = try catalog.parseDefinition(a, "scoped", 3, 1, "{\"labels\":[{\"name\":\"D\",\"table\":{\"database\":\"research\",\"namespace\":\"papers\",\"table\":\"docs\"}}]}");
    try std.testing.expectEqualStrings("research", scoped.labels[0].database);
    try std.testing.expectEqualStrings("papers", scoped.labels[0].namespace);
    try std.testing.expectEqualStrings("docs", scoped.labels[0].table);
    try std.testing.expectError(error.AmbiguousGraphRelationship, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"FIRST\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"cites\"},{\"name\":\"SECOND\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"cites\"}]}"));
    try std.testing.expectError(error.UnknownField, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":{\"table\":\"docs\",\"table_id\":42}}]}"));
}

test "nested label predicates use linear indexed equality with work and cancellation" {
    const Counter = struct {
        units: usize = 0,
        limit: usize = 1_000_000,
        canceled: bool = false,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.Canceled;
            if (amount > self.limit -| self.units) return error.CypherStateBudgetExceeded;
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const predicate = try a.alloc(v.Entry, 4000);
    const properties = try a.alloc(v.Entry, predicate.len);
    for (predicate, 0..) |*entry, i| {
        entry.* = .{ .name = try std.fmt.allocPrint(a, "field:{d}", .{i}), .value = .{ .integer = @intCast(i) } };
        properties[properties.len - 1 - i] = entry.*;
    }
    const selected: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .relationships = &.{}, .labels = &.{.{ .name = "D", .table = "docs", .predicate = &.{.{ .name = "meta", .value = .{ .map = predicate } }} }} };
    const node: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .properties = &.{.{ .name = "meta", .value = .{ .map = properties } }} };
    var counter: Counter = .{};
    var work: @import("value_work.zig").Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    try std.testing.expect(try selected.admits(&node, "D", &work));
    try std.testing.expect(counter.units < 100_000);
    counter.limit = counter.units + 10;
    try std.testing.expectError(error.CypherStateBudgetExceeded, selected.admits(&node, "D", &work));
    counter.canceled = true;
    try std.testing.expectError(error.Canceled, selected.admits(&node, "D", &work));
    counter = .{ .limit = 10 };
    const nonmember: v.Node = .{ .identity = node.identity };
    var uncached: @import("value_work.zig").Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    try std.testing.expect(!try selected.admits(&nonmember, "D", &uncached));
    counter = .{};
    const left: v.Value = .{ .map = &.{ .{ .name = "a", .value = .null }, .{ .name = "b", .value = .{ .integer = 1 } } } };
    try std.testing.expectEqual(@as(?bool, null), try work.equal(left, left));
    try std.testing.expectEqual(@as(?bool, false), try work.equal(left, .{ .map = &.{ .{ .name = "b", .value = .{ .integer = 2 } }, .{ .name = "a", .value = .null } } }));
}
