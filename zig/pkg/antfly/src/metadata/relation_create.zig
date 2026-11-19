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

//! Shared immutable identity, namespace and placement preparation for new relations.
const std = @import("std");
const records = @import("antfly_local_sources").common_topology_records;

pub const Guard = struct {
    database: [32]u8,
    namespace: [32]u8,
    tablespace_id: u64,
    tablespace: ?[32]u8,
};
pub fn fingerprint(a: std.mem.Allocator, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
pub fn validPhysicalName(name: []const u8) bool {
    if (name.len != 38 or !std.mem.startsWith(u8, name, "table:")) return false;
    for (name[6..]) |byte| if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return false;
    return true;
}

pub const Purpose = enum { foreign_key, query_population };

pub const Request = struct {
    materialized_view: bool = false,
    expected_view: ?@import("../system_catalog/views.zig").Identity = null,
    physical_name: ?[]const u8 = null,
    namespace_id: u64,
    logical_name: []const u8,
    tablespace_name: ?[]const u8 = null,
    min_ranges_explicit: bool = false,
    /// Fully normalized create definition; metadata assigns physical identity
    /// and computes owner ranges from its current catalog/fence cut.
    candidate: records.TableRecord,
    /// Trusted compiler provenance, never a flag inside the public schema.
    generated_constraint_names: []const []const u8 = &.{},
    creation_copy: ?@import("initial_creation_copy.zig").Input = null,
    column_sequences: []const @import("antfly_local_sources").system_catalog_column_sequences.Creation = &.{},

    pub fn jsonStringify(self: Request, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Request).@"struct".field_names) |field_name| {
            if ((!std.mem.eql(u8, field_name, "creation_copy") or self.creation_copy != null) and (!std.mem.eql(u8, field_name, "column_sequences") or self.column_sequences.len != 0) and (!std.mem.eql(u8, field_name, "expected_view") or self.expected_view != null) and (!std.mem.eql(u8, field_name, "materialized_view") or self.materialized_view) and (!std.mem.eql(u8, field_name, "generated_constraint_names") or self.generated_constraint_names.len != 0) and (!std.mem.eql(u8, field_name, "physical_name") or self.physical_name != null)) {
                try writer.objectField(field_name);
                try writer.write(@field(self, field_name));
            }
        }
        try writer.endObject();
    }
};

pub const Previous = struct {
    resource: @import("antfly_local_sources").system_catalog_domain.Resource,
    table: records.TableRecord,
    ranges: []const records.RangeRecord,
};
pub const Prepared = struct {
    previous: ?Previous = null,
    creation_guard: ?Guard = null,
    catalog_id: u64,
    expected_catalog_revision: u64,
    tablespace_id: u64,
    min_ranges_explicit: bool,
    child: records.TableRecord,
    child_ranges: []const records.RangeRecord,
    copy_effects: ?@import("initial_creation_copy.zig").Effects = null,
    counter_effects: ?@import("initial_column_sequences.zig").Effects = null,
    transition_generation: u64,
    metadata_incarnation: @import("antfly_local_sources").metadata_incarnation.MetadataClusterIncarnation,
    metadata_epoch: u64,
    constraint_name_admission: ?@import("antfly_local_sources").system_catalog_relation_names.constraint_naming.Admission = null,

    pub fn jsonStringify(self: Prepared, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Prepared).@"struct".field_names) |field_name| {
            if ((!std.mem.eql(u8, field_name, "copy_effects") or self.copy_effects != null) and (!std.mem.eql(u8, field_name, "counter_effects") or self.counter_effects != null) and (!std.mem.eql(u8, field_name, "previous") or self.previous != null) and (!std.mem.eql(u8, field_name, "constraint_name_admission") or self.constraint_name_admission != null) and (!std.mem.eql(u8, field_name, "creation_guard") or self.creation_guard != null)) {
                try writer.objectField(field_name);
                try writer.write(@field(self, field_name));
            }
        }
        try writer.endObject();
    }
};

/// Resolve only the self edge after metadata has reserved the candidate's
/// physical identity. External edges have already been bound at DDL ingress.
pub fn bindReservedSelf(alloc: std.mem.Allocator, schema_json: []const u8, logical_name: []const u8, physical_name: []const u8) ![]const u8 {
    var value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, schema_json, .{ .allocate = .alloc_always, .parse_numbers = false });
    if (value != .object) return error.InvalidGenerationPublication;
    const fks = value.object.getPtr("foreign_keys") orelse return schema_json;
    if (fks.* != .array) return error.InvalidGenerationPublication;
    var changed = false;
    for (fks.array.items) |*fk| {
        if (fk.* != .object) return error.InvalidGenerationPublication;
        const parent = fk.object.getPtr("parent_table") orelse return error.InvalidGenerationPublication;
        if (parent.* != .string) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, parent.string, logical_name)) continue;
        parent.* = .{ .string = physical_name };
        changed = true;
    }
    return if (changed) try std.json.Stringify.valueAlloc(alloc, value, .{}) else schema_json;
}
