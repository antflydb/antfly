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
    const employee: v.Node = .{ .identity = .{ .table = "people", .key = "a" }, .properties = &.{.{ .name = "kind", .value = .{ .string = "employee" } }} };
    try std.testing.expect(graph.admits(&employee, "Employee"));
    var other = employee;
    other.identity.table = "other";
    try std.testing.expect(!graph.admits(&other, "Employee"));
    try std.testing.expectError(error.DuplicateGraphLabel, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"a\"},{\"name\":\"D\",\"table\":\"b\"}]}"));
    try std.testing.expectError(error.InvalidGraphDefinition, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"a\",\"where\":false}]}"));
    const scoped = try catalog.parseDefinition(a, "scoped", 3, 1, "{\"labels\":[{\"name\":\"D\",\"table\":{\"database\":\"research\",\"namespace\":\"papers\",\"table\":\"docs\"}}]}");
    try std.testing.expectEqualStrings("research", scoped.labels[0].database);
    try std.testing.expectEqualStrings("papers", scoped.labels[0].namespace);
    try std.testing.expectEqualStrings("docs", scoped.labels[0].table);
    try std.testing.expectError(error.AmbiguousGraphRelationship, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"FIRST\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"cites\"},{\"name\":\"SECOND\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"cites\"}]}"));
    try std.testing.expectError(error.UnknownField, catalog.parseDefinition(a, "bad", 1, 1, "{\"labels\":[{\"name\":\"D\",\"table\":{\"table\":\"docs\",\"table_id\":42}}]}"));
}
