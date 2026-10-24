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

//! Private, bounded resolver catalog reads. No public request supplies authority.
const std = @import("std");
const metadata = @import("table_manager.zig");
pub const Request = struct {
    table_name: []const u8,
    table_id: u64 = 0,
    mode: enum { table, consumers, authority } = .table,
    key: []const u8 = "",
    authority: ?@import("resolver_authority.zig").Read = null,
    expected_revision: ?u64 = null,
    cursor: ?[]const u8 = null,
    standalone: bool = false,
    compact_consumers: bool = false,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const include = if (comptime std.mem.eql(u8, field, "authority")) self.authority != null else if (comptime std.mem.eql(u8, field, "table_id")) self.table_id != 0 else if (comptime std.mem.eql(u8, field, "compact_consumers")) self.compact_consumers else true;
            if (include) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};
pub const Page = struct {
    version: u8,
    revision: u64,
    table: metadata.TableRecord,
    configs: ?[]const @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig = null,
    ranges: []metadata.RangeRecord = &.{},
    consumers: []const Consumer = &.{},
    visited: usize = 0,
    next_cursor: ?[]const u8 = null,
    indexed: bool = false,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "configs")) {
                if (self.configs) |values| {
                    try writer.objectField(field);
                    try writer.beginArray();
                    for (values) |cfg| {
                        try writer.beginObject();
                        // Durable definition serialization excludes merges;
                        // this private execution view must carry them.
                        inline for (@typeInfo(@TypeOf(cfg)).@"struct".field_names) |name| {
                            try writer.objectField(name);
                            try writer.write(@field(cfg, name));
                        }
                        try writer.endObject();
                    }
                    try writer.endArray();
                }
            } else {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn snapshot(self: *const Page) @import("api.zig").AdminSnapshot {
        return .{ .status = .{ .metadata_group_id = 0, .metrics = .{} }, .tables = @constCast((&self.table)[0..1]), .ranges = self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
};
pub const Cursor = struct { revision: u64, table_id: u64, key_digest: [32]u8, indexed: bool, after: []const u8 };
pub fn keyDigest(key: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &out, .{});
    return out;
}

pub const Consumer = struct {
    table: metadata.TableRecord,
    names: []const []const u8,
    configs: ?[]const @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig = null,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("table");
        try writer.write(self.table);
        try writer.objectField("names");
        try writer.write(self.names);
        if (self.configs) |values| {
            try writer.objectField("configs");
            try writer.write(values);
        }
        try writer.endObject();
    }
};
pub const ConsumerReference = struct {
    version: u8 = 1,
    source_id: u64,
    source_name: []const u8,
    configs: []const @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig,
};
