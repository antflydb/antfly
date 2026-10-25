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
pub const QueryHead = struct { table_id: u64, key_template: []const u8, stamp: @import("../metadata/resolver_authority.zig").records.Stamp };
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
    /// Progress pages count individual resolver references, using individually
    /// indexed resolver descriptors. Zero retains the admission paging contract.
    consumer_limit: u32 = 0,
    /// Admission already has native heads and indexed static profiles.
    /// Only source-owned namespaces missing a head need consumer folding.
    cold_only: bool = false,
    /// null expands all policies; an empty list is a descriptor/status cut.
    projection_names: ?[]const []const u8 = null,
    routing_siblings: bool = false,
    /// Query execution hydrates only mention/candidate policy through authority reads.
    policy_descriptors: bool = false,
    expected_query_fingerprint: ?[32]u8 = null,
    /// Progress routing needs identity/ranges, never DDL or policy bodies.
    identity_only: bool = false,
    authority_admission: bool = false,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const include = if (comptime std.mem.eql(u8, field, "authority_admission")) self.authority_admission else if (comptime std.mem.eql(u8, field, "identity_only")) self.identity_only else if (comptime std.mem.eql(u8, field, "authority")) self.authority != null else if (comptime std.mem.eql(u8, field, "table_id")) self.table_id != 0 else if (comptime std.mem.eql(u8, field, "compact_consumers")) self.compact_consumers else true;
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
    query_fingerprint: ?[32]u8 = null,
    query_heads: []const QueryHead = &.{},
    admission: ?@import("resolver_authority.zig").Admission = null,
    revision: u64,
    table: metadata.TableRecord,
    configs: ?[]const @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig = null,
    execution_indexes: ?[]const u8 = null,
    ranges: []metadata.RangeRecord = &.{},
    consumers: []const Consumer = &.{},
    cold_profiles: []const @import("resolver_authority.zig").AliasProfile = &.{},
    visited: usize = 0,
    next_cursor: ?[]const u8 = null,
    indexed: bool = false,
    pub fn executionIndexes(self: @This()) []const u8 {
        return self.execution_indexes orelse self.table.indexes_json;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "admission")) {
                if (self.admission) |proof| {
                    try writer.objectField(field);
                    try writer.write(proof);
                }
            } else if (comptime std.mem.eql(u8, field, "cold_profiles")) {
                if (self.cold_profiles.len > 0) {
                    try writer.objectField(field);
                    try writer.write(self.cold_profiles);
                }
            } else if (comptime std.mem.eql(u8, field, "configs")) {
                if (self.configs) |values| {
                    try writer.objectField(field);
                    try writer.beginArray();
                    for (values) |cfg| {
                        try writer.beginObject();
                        // Durable definition serialization excludes merges;
                        // this private execution view must carry them.
                        inline for (@typeInfo(@TypeOf(cfg)).@"struct".field_names) |name| {
                            if (comptime !std.mem.eql(u8, name, "shared_projection") and !std.mem.eql(u8, name, "validation_proof")) {
                                try writer.objectField(name);
                                try writer.write(@field(cfg, name));
                            }
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
pub const Cursor = struct { revision: u64, table_id: u64, key_digest: [32]u8, indexed: bool, after: []const u8, scoped: bool = false, namespace_index: usize = 0 };
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
            try writeExecutionConfigs(writer, values);
        }
        try writer.endObject();
    }
};
fn writeExecutionConfigs(writer: anytype, values: anytype) !void {
    try writer.beginArray();
    for (values) |cfg| try writer.write(@import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverAdmission.fromConfig(cfg, false));
    try writer.endArray();
}

pub const ConsumerReference = struct {
    version: u8 = 1,
    source_id: u64,
    source_name: []const u8,
    configs: []const @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (.{ "version", "source_id", "source_name" }) |field| {
            try writer.objectField(field);
            try writer.write(@field(self, field));
        }
        try writer.objectField("configs");
        try writeExecutionConfigs(writer, self.configs);
        try writer.endObject();
    }
};

/// Metadata cursors may release or reuse a row at the next seek/advance.
pub fn decodeConsumerReference(a: std.mem.Allocator, bytes: []const u8) !ConsumerReference {
    const value = try std.json.parseFromSliceLeaky(ConsumerReference, a, bytes, .{ .allocate = .alloc_always });
    if (value.version != 1) return error.InvalidCatalogRecord;
    return value;
}

test "resolver curation namespace propagation compact consumers retain unadopted merges" {
    const a = std.testing.allocator;
    const Config = @import("antfly_local_sources").storage_db_catalog_resolver_catalog.ResolverConfig;
    const cfg: Config = .{ .name = "legacy", .table = "entities", .key_template = "other/{{_entity.text}}", .source_artifact = "source", .resolution_artifact = "resolved", .merges = &.{.{ .key = "other/a", .target = "other/b" }}, .merge_generation = 7, .merge_state_version = 1, .scorer_json = "SCORER_PROMPT_MUST_STAY_WITH_EXECUTION", .name_embedding = "EXECUTION_EMBEDDER" };
    const bytes = try std.json.Stringify.valueAlloc(a, ConsumerReference{ .source_id = 2, .source_name = "source", .configs = &.{cfg} }, .{});
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "SCORER_PROMPT") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "EXECUTION_EMBEDDER") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "scorer_json") == null);
    {
        var row_arena = std.heap.ArenaAllocator.init(a);
        defer row_arena.deinit();
        const reused_row = try a.dupe(u8, bytes);
        defer a.free(reused_row);
        const retained = try decodeConsumerReference(row_arena.allocator(), reused_row);
        @memset(reused_row, 0);
        try std.testing.expectEqualStrings("source", retained.source_name);
        try std.testing.expectEqualStrings("legacy", retained.configs[0].name);
        try std.testing.expectEqualStrings("other/b", retained.configs[0].merges[0].target);
    }
    const reference = try std.json.parseFromSlice(ConsumerReference, a, bytes, .{});
    defer reference.deinit();
    try std.testing.expectEqualStrings("", reference.value.configs[0].scorer_json);
    try std.testing.expectEqualStrings("", reference.value.configs[0].source_artifact);
    try std.testing.expectEqual(@as(u64, 7), reference.value.configs[0].effectiveGeneration());
    try std.testing.expect(Config.aliasesEqual(cfg.merges, reference.value.configs[0].merges));
    const page_bytes = try std.json.Stringify.valueAlloc(a, Consumer{ .table = .{ .table_id = 2, .name = "source" }, .names = &.{"legacy"}, .configs = reference.value.configs }, .{});
    defer a.free(page_bytes);
    const page = try std.json.parseFromSlice(Consumer, a, page_bytes, .{});
    defer page.deinit();
    try std.testing.expect(Config.aliasesEqual(cfg.merges, page.value.configs.?[0].merges));
}
