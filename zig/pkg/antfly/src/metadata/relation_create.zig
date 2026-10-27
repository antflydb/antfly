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

    pub fn jsonStringify(self: Request, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Request).@"struct".field_names) |field_name| {
            if ((!std.mem.eql(u8, field_name, "materialized_view") or self.materialized_view) and (!std.mem.eql(u8, field_name, "generated_constraint_names") or self.generated_constraint_names.len != 0) and (!std.mem.eql(u8, field_name, "physical_name") or self.physical_name != null)) {
                try writer.objectField(field_name);
                try writer.write(@field(self, field_name));
            }
        }
        try writer.endObject();
    }
};

pub const Prepared = struct {
    creation_guard: ?Guard = null,
    catalog_id: u64,
    expected_catalog_revision: u64,
    tablespace_id: u64,
    min_ranges_explicit: bool,
    child: records.TableRecord,
    child_ranges: []const records.RangeRecord,
    transition_generation: u64,
    metadata_incarnation: @import("antfly_local_sources").metadata_incarnation.MetadataClusterIncarnation,
    metadata_epoch: u64,
    constraint_name_admission: ?@import("antfly_local_sources").system_catalog_relation_names.constraint_naming.Admission = null,

    pub fn jsonStringify(self: Prepared, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Prepared).@"struct".field_names) |field_name| {
            if ((!std.mem.eql(u8, field_name, "constraint_name_admission") or self.constraint_name_admission != null) and (!std.mem.eql(u8, field_name, "creation_guard") or self.creation_guard != null)) {
                try writer.objectField(field_name);
                try writer.write(@field(self, field_name));
            }
        }
        try writer.endObject();
    }
};
