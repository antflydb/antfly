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

//! Pure preparation for one containing native table/catalog transaction.
//! This module grants no write authority and never commits a sequence alone.
const std = @import("std");
const columns = @import("column_sequences.zig");
const identities = @import("column_identities.zig");
const sequence = @import("sequences.zig");
const names = @import("constraint_names.zig");
const domain = @import("domain.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;

pub const Prepared = struct {
    column: []const u8,
    binding: columns.Binding,
    definition: sequence.Definition,
};
pub const Plan = struct {
    schema_json: []const u8,
    sequences: []const Prepared,
    next_id: u64,
};
pub const Input = struct {
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    first_id: u64,
    identities: identities.State,
    schema_json: []const u8,
    columns: []const columns.Creation,
    /// Only the native admission owner may first bind compiler intent. Raft
    /// validation/application must independently derive the SAME descriptors.
    allow_unbound: bool,
};

/// The returned plan borrows the request region. Occupancy must be supplied
/// from the caller's exact catalog transaction and current relation root.
pub fn prepare(a: A, input: Input, namespace: anytype) !Plan {
    try input.target.validate();
    if (input.namespace_id == 0 or input.first_id == 0 or input.columns.len == 0 or input.columns.len > 256)
        return error.InvalidSequenceDefinition;
    try input.identities.validate(a);
    const schema = try std.json.parseFromSliceLeaky(Json, a, input.schema_json, .{ .parse_numbers = false });
    if (schema != .object) return error.InvalidSequenceDefinition;
    const mode = schema.object.get("storage_mode") orelse return error.InvalidSequenceDefinition;
    if (mode != .string or !std.mem.eql(u8, mode.string, "relational")) return error.InvalidSequenceDefinition;
    var intents: std.StringHashMapUnmanaged(usize) = .empty;
    for (input.columns, 0..) |column, index| {
        try column.validate();
        const slot = try intents.getOrPut(a, column.column);
        if (slot.found_existing) return error.InvalidSequenceDefinition;
        slot.value_ptr.* = index;
    }
    var selected: std.StringHashMapUnmanaged(void) = .empty;
    try selected.put(a, input.target.table, {});
    for ([_][]const u8{ "unique_constraints", "relational_indexes" }) |field| {
        if (schema.object.get(field)) |entries| {
            if (entries != .array) return error.InvalidSequenceDefinition;
            for (entries.array.items) |entry| {
                if (entry != .object) return error.InvalidSequenceDefinition;
                const name = entry.object.get("name") orelse return error.InvalidSequenceDefinition;
                if (name != .string) return error.InvalidSequenceDefinition;
                try selected.put(a, name.string, {});
            }
        }
    }
    const old = try a.alloc(?columns.Binding, input.columns.len);
    @memset(old, null);
    var defaults: std.ArrayList(Json) = .empty;
    if (schema.object.get("column_defaults")) |entries| {
        if (entries != .array) return error.InvalidSequenceDefinition;
        for (entries.array.items) |entry| {
            if (entry != .object) return error.InvalidSequenceDefinition;
            const name = entry.object.get("column") orelse return error.InvalidSequenceDefinition;
            if (name != .string) return error.InvalidSequenceDefinition;
            if (intents.get(name.string)) |index| {
                if (entry.object.count() != 2 or old[index] != null) return error.InvalidSequenceDefinition;
                var bound = try columns.Binding.parse(a, entry.object.get("sequence") orelse return error.InvalidSequenceDefinition);
                defer bound.deinit();
                var binding = bound.value;
                binding.target.database = try a.dupe(u8, binding.target.database);
                binding.target.namespace = try a.dupe(u8, binding.target.namespace);
                binding.target.table = try a.dupe(u8, binding.target.table);
                old[index] = binding;
            } else try defaults.append(a, entry);
        }
    }
    const Namespace = struct {
        upstream: @TypeOf(namespace),
        selected: *std.StringHashMapUnmanaged(void),
        pub fn occupied(self: *@This(), name: []const u8, _: names.Kind) !bool {
            try self.upstream.checkpoint();
            return self.selected.contains(name) or try self.upstream.relationOccupied(name);
        }
    };
    var chooser: Namespace = .{ .upstream = namespace, .selected = &selected };
    const prepared = try a.alloc(Prepared, input.columns.len);
    var next_id = input.first_id;
    for (input.columns, prepared, old) |column, *output, prior| {
        try namespace.checkpoint();
        try namespace.requireColumn(column.column, column.options.integer.?);
        const attribute = for (input.identities.columns) |attribute| {
            if (std.mem.eql(u8, attribute.name, column.column)) break attribute;
        } else return error.UndefinedColumn;
        var candidate = try names.Candidate.initSequence(input.target.table, column.column);
        const name = if (prior) |before| bound: {
            const suffix = std.mem.lastIndexOf(u8, before.target.table, "_seq") orelse return error.InvalidSequenceDefinition;
            const digits = before.target.table[suffix + 4 ..];
            const attempt: u32 = if (digits.len == 0) 0 else std.fmt.parseInt(u32, digits, 10) catch return error.InvalidSequenceDefinition;
            if (attempt >= 4096) return error.InvalidSequenceDefinition;
            var buffer: [names.max_name_bytes]u8 = undefined;
            const canonical = try candidate.format(attempt, &buffer);
            if (!std.mem.eql(u8, canonical, before.target.table) or try chooser.occupied(canonical, .unique)) return error.CatalogGenerationChanged;
            break :bound try a.dupe(u8, canonical);
        } else try names.choose(a, &candidate, &chooser, 4096);
        try selected.put(a, name, {});
        const binding: columns.Binding = .{
            .generation = column.generation,
            .incarnation = input.incarnation,
            .sequence_id = next_id,
            .namespace_id = input.namespace_id,
            .owner = .{ .table_id = input.identities.table_id, .column_id = attribute.id },
            .target = .{ .database = input.target.database, .namespace = input.target.namespace, .table = name },
        };
        try binding.validate();
        if (prior) |before| {
            if (!std.mem.eql(u8, &before.fingerprint(), &binding.fingerprint()) or
                !std.mem.eql(u8, before.target.database, binding.target.database) or
                !std.mem.eql(u8, before.target.namespace, binding.target.namespace) or
                !std.mem.eql(u8, before.target.table, binding.target.table)) return error.CatalogGenerationChanged;
        } else if (!input.allow_unbound) return error.InvalidSequenceDefinition;
        var options = column.options;
        options.owned_by = .{ .column = binding.owner };
        output.* = .{ .column = column.column, .binding = binding, .definition = try sequence.Definition.prepare(null, options) };
        const entry = try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, .{ .column = column.column, .sequence = binding }, .{}), .{});
        try defaults.append(a, entry);
        next_id = std.math.add(u64, next_id, 1) catch return error.CatalogIdExhausted;
    }
    var schema_value = schema;
    try schema_value.object.put(a, "column_defaults", .{ .array = .fromOwnedSlice(a, try defaults.toOwnedSlice(a)) });
    const encoded = try std.json.Stringify.valueAlloc(a, schema_value, .{});
    if (encoded.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    return .{ .schema_json = encoded, .sequences = prepared, .next_id = next_id };
}
