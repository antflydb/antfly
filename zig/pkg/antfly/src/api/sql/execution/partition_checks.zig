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

//! Match inherited CHECKs against immutable native validator programs. Attach
//! never promotes a NOT VALID child rule into historical validation evidence.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Binding = local.system_catalog_table_partitions.ConstraintBinding;
const Validator = local.schema_mod.CompiledTableValidator;

/// Output names belong to the caller's command arena, independent of cache
/// pins. Both cached validators remain borrowed and unchanged by matching.
pub fn appendInherited(a: A, output: *std.ArrayList(Binding), parent: Validator, child: Validator) !void {
    const required = parent.execution.checks orelse return;
    const existing = child.execution.checks orelse return error.SqlPartitionSchemaMismatch;
    var by_name: std.StringHashMapUnmanaged(usize) = .empty;
    for (existing.definitions, 0..) |definition, index| try by_name.put(a, definition.name, index);
    for (required.definitions, required.plans) |definition, program| {
        const index = by_name.get(definition.name) orelse return error.SqlPartitionSchemaMismatch;
        const matched = existing.definitions[index];
        if (!std.mem.eql(u8, &program.fingerprint, &existing.plans[index].fingerprint) or
            (!(definition.not_valid orelse false) and (matched.not_valid orelse false)))
            return error.SqlPartitionSchemaMismatch;
        try output.append(a, .{ .kind = .check, .parent_name = try a.dupe(u8, definition.name), .child_name = try a.dupe(u8, matched.name) });
    }
}

test "SQL partition parent routing inherited CHECKs compare native programs and historical coverage" {
    const a = std.testing.allocator;
    const prefix = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}},\"additionalProperties\":false}}},\"checks\":[";
    const valid = prefix ++ "{\"name\":\"positive\",\"column\":\"n\",\"op\":\"gt\",\"value\":0}]}";
    const unvalidated = prefix ++ "{\"name\":\"positive\",\"column\":\"n\",\"op\":\"gt\",\"value\":0,\"not_valid\":true}]}";
    const wrong = prefix ++ "{\"name\":\"positive\",\"column\":\"n\",\"op\":\"gt\",\"value\":1}]}";
    var parent = try Validator.init(a, valid);
    defer parent.deinit(a);
    var child = try Validator.init(a, valid);
    defer child.deinit(a);
    var pending = try Validator.init(a, unvalidated);
    defer pending.deinit(a);
    var different = try Validator.init(a, wrong);
    defer different.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var bindings: std.ArrayList(Binding) = .empty;
    try appendInherited(arena.allocator(), &bindings, parent, child);
    try std.testing.expectEqual(@as(usize, 1), bindings.items.len);
    try std.testing.expectEqualStrings("positive", bindings.items[0].child_name);
    try std.testing.expectError(error.SqlPartitionSchemaMismatch, appendInherited(arena.allocator(), &bindings, parent, pending));
    try std.testing.expectError(error.SqlPartitionSchemaMismatch, appendInherited(arena.allocator(), &bindings, parent, different));
    // A NOT VALID parent still constrains future writes; it does not demand a
    // historical proof it has never acquired for itself.
    bindings.clearRetainingCapacity();
    try appendInherited(arena.allocator(), &bindings, pending, child);
    try appendInherited(arena.allocator(), &bindings, pending, pending);
    try std.testing.expectEqual(@as(usize, 2), bindings.items.len);
}
