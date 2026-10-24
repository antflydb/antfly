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

//! Destination snapshots are durable independently of static source catalogs.
const std = @import("std");
const catalog = @import("catalog/resolver_catalog.zig");
const policy = @import("resolver_namespace.zig");
const A = std.mem.Allocator;
pub const prefix = "\x00\x00__metadata__:resolver_projection:v1/";
pub const epoch_key = "\x00\x00__metadata__:resolver_projection_epoch:v1";
pub const Binding = struct { name: []const u8, retired: bool, resolution_generation: u64, merge_generation: u64, merge_state_version: u32 };
pub const Record = struct { version: u8 = 1, table: []const u8, table_id: u64, namespace: policy.Namespace, bindings: []const Binding };
pub fn key(a: A, table: []const u8, template: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, .{ table, template }, .{}), &digest, .{});
    return std.mem.concat(a, u8, &.{ prefix, &std.fmt.bytesToHex(digest, .lower) });
}
pub fn parse(a: A, path: []const u8, raw: []const u8) !Record {
    const record = try std.json.parseFromSliceLeaky(Record, a, raw, .{ .allocate = .alloc_always });
    if (record.version != 1 or record.table.len == 0 or record.table_id == 0 or record.namespace.key_template.len == 0 or record.bindings.len > 16384 or !std.mem.eql(u8, path, try key(a, record.table, record.namespace.key_template))) return error.InvalidMetadataBatch;
    var names: std.StringHashMapUnmanaged(void) = .empty;
    for (record.bindings) |binding| if (binding.name.len == 0 or binding.merge_state_version > 2 or (try names.getOrPut(a, binding.name)).found_existing) return error.InvalidMetadataBatch;
    const probe: catalog.ResolverConfig = .{ .name = "projection", .table = record.table, .source_artifact = "source", .resolution_artifact = "resolved", .key_template = record.namespace.key_template, .merges = record.namespace.merges, .curation_json = try std.json.Stringify.valueAlloc(a, record.namespace.state, .{}) };
    try probe.validatePolicy();
    return record;
}
pub fn apply(cfg: catalog.ResolverConfig, record: Record, binding: Binding, shared: *catalog.SharedProjection) !catalog.ResolverConfig {
    if (!std.mem.eql(u8, cfg.name, binding.name) or cfg.retired != binding.retired or !std.mem.eql(u8, cfg.table, record.table) or !std.mem.eql(u8, cfg.key_template, record.namespace.key_template)) return error.InvalidMetadataBatch;
    var next = cfg;
    next.shared_projection = shared;
    next.merges = shared.merges;
    next.curation_json = shared.curation_json;
    next.namespace_curation_table_id = record.table_id;
    next.namespace_curation_generation = record.namespace.revision;
    next.namespace_curation_missing = !record.namespace.record_store;
    next.curation_revision = record.namespace.revision;
    next.resolution_generation = binding.resolution_generation;
    next.merge_generation = binding.merge_generation;
    next.merge_state_version = binding.merge_state_version;
    return next;
}
