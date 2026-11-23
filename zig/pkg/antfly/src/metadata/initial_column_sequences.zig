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

//! Immutable counter postimages for the existing hidden FK creation protocol.
//! Native admission must independently rederive these effects from its own cut.
const std = @import("std");
const local = @import("antfly_local_sources");
const creation = local.system_catalog_column_sequence_creation;
const columns = local.system_catalog_column_sequences;
const records = local.common_topology_records;
const A = std.mem.Allocator;

pub const Effects = struct {
    sequences: []const creation.Prepared,
    next_id: u64,
    source_inputs: []const creation.SourceInput = &.{},
    destination: ?local.system_catalog_domain.Target = null,

    pub fn jsonStringify(self: Effects, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Effects).@"struct".field_names) |name| {
            const include = if (comptime std.mem.eql(u8, name, "source_inputs")) self.source_inputs.len != 0 else if (comptime std.mem.eql(u8, name, "destination")) self.destination != null else true;
            if (include) {
                try writer.objectField(name);
                try writer.write(@field(self, name));
            }
        }
        try writer.endObject();
    }

    pub fn digest(self: Effects, a: A) ![32]u8 {
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(bytes);
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &result, .{});
        return result;
    }

    /// A shared counter may appear in more than one copied default. Lease it
    /// once per publication, in canonical ID order, with fixed working memory.
    pub fn borrowedCounters(self: Effects, buffer: *[256]u64) ![]const u64 {
        if (self.source_inputs.len > buffer.len) return error.InvalidGenerationPublication;
        var count: usize = 0;
        for (self.source_inputs) |input| if (input.source.binding.generation == .serial) {
            buffer[count] = input.source.binding.sequence_id;
            count += 1;
        };
        std.mem.sort(u64, buffer[0..count], {}, std.sort.asc(u64));
        var unique: usize = 0;
        for (buffer[0..count]) |id| {
            if (unique != 0 and buffer[unique - 1] == id) continue;
            buffer[unique] = id;
            unique += 1;
        }
        return buffer[0..unique];
    }

    pub fn asPlan(self: Effects, schema: []const u8) creation.Plan {
        return .{ .schema_json = schema, .sequences = self.sequences, .next_id = self.next_id, .source_inputs = self.source_inputs, .destination = self.destination };
    }

    pub fn fromPlan(plan: creation.Plan) Effects {
        return .{ .sequences = plan.sequences, .next_id = plan.next_id, .source_inputs = plan.source_inputs, .destination = plan.destination };
    }
};

/// Verify portable intent/postimage consistency without treating a plan as
/// namespace authority. Only the native accepting writer checks occupancy.
pub fn validate(backing: A, child: records.TableRecord, logical_name: []const u8, namespace: u64, catalog_id: u64, intents: []const columns.Creation, effects: ?Effects) !void {
    if (intents.len == 0) {
        if (effects != null) return error.InvalidGenerationPublication;
        return;
    }
    const value = effects orelse return error.InvalidGenerationPublication;
    if (intents.len > 256 or value.source_inputs.len > intents.len or catalog_id == std.math.maxInt(u64)) return error.InvalidGenerationPublication;
    var fresh: usize = 0;
    var sourced: usize = 0;
    for (intents) |intent| {
        try intent.validate();
        if (intent.source != null) sourced += 1;
        if (intent.source == null or intent.generation != .serial) fresh += 1;
    }
    if (value.sequences.len != fresh or value.source_inputs.len != sourced or
        (sourced != 0) != (value.destination != null)) return error.InvalidGenerationPublication;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const target = if (value.destination) |destination| destination else if (value.sequences.len != 0) value.sequences[0].binding.target else return error.InvalidGenerationPublication;
    if (value.destination != null and !std.mem.eql(u8, target.table, logical_name)) return error.InvalidGenerationPublication;
    const incarnation = if (value.sequences.len != 0) value.sequences[0].binding.incarnation else value.source_inputs[0].source.binding.incarnation;
    var attributes = try @import("column_identity_definition.zig").initial(a, child);
    defer attributes.deinit();
    const parsed = try local.schema_mod.parseValidatedTableSchema(a, child.schema_json);
    const native = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    const layout = try local.sql_document_row.deriveRelationalColumnsLeaky(a, a, parsed, native);
    const Namespace = struct {
        layout: std.StringHashMapUnmanaged(*const local.sql_catalog.Column),
        sources: std.StringHashMapUnmanaged(creation.SourceInput) = .empty,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn relationOccupied(_: *@This(), _: []const u8) !bool {
            return false;
        }
        pub fn requireColumn(self: *@This(), name: []const u8, width: local.system_catalog_sequences.Integer, not_null: bool) !void {
            const column = self.layout.get(name) orelse return error.InvalidGenerationPublication;
            if (column.type != .integer or (not_null and column.nullable)) return error.InvalidGenerationPublication;
            const actual: local.system_catalog_sequences.Integer = switch (column.element_type orelse .int64) {
                .int16 => .int16,
                .int32 => .int32,
                .int64 => .int64,
                else => return error.InvalidGenerationPublication,
            };
            if (actual != width) return error.InvalidGenerationPublication;
        }
        pub fn resolveSource(self: *@This(), intent: columns.Creation) !creation.Source {
            const input = self.sources.get(intent.column) orelse return error.InvalidGenerationPublication;
            if (!std.meta.eql(input.reference, intent.source orelse return error.InvalidGenerationPublication) or
                input.source.binding.owner.table_id != input.reference.table_id or input.source.binding.owner.column_id != input.reference.column_id)
                return error.InvalidGenerationPublication;
            return input.source;
        }
    };
    var ns: Namespace = .{ .layout = .empty };
    try ns.layout.ensureTotalCapacity(a, @intCast(layout.len));
    for (layout) |*column| ns.layout.putAssumeCapacity(column.name, column);
    for (value.source_inputs) |input| {
        const slot = try ns.sources.getOrPut(a, input.column);
        if (slot.found_existing) return error.InvalidGenerationPublication;
        slot.value_ptr.* = input;
    }
    const expected = try creation.prepare(a, .{
        .target = .{ .database = target.database, .namespace = target.namespace, .table = logical_name },
        .namespace_id = namespace,
        .incarnation = incarnation,
        .first_id = catalog_id + 1,
        .identities = attributes.value,
        .schema_json = child.schema_json,
        .columns = intents,
        .allow_unbound = false,
    }, &ns);
    if (!std.mem.eql(u8, expected.schema_json, child.schema_json) or
        !std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, Effects.fromPlan(expected), .{}), try std.json.Stringify.valueAlloc(a, value, .{}))) return error.InvalidGenerationPublication;
}
