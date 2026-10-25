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

//! Private metadata-owned native resolver authority contract. Ingress supplies
//! fenced component edits; public SQL and table/index bodies never decode it.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const records = local.storage_db_resolver_authority;
const c = @import("antfly_resolver").curation;
const metadata = @import("table_manager.zig");
const A = std.mem.Allocator;
pub const marker = "_resolver_record_store";
pub const owners_key = "_resolver_record_owners";
pub const AliasProfile = records.AliasProfile;
/// Private proof captured atomically with the physical table definition.
/// It is never accepted from public table/index JSON or client actors.
pub const Admission = struct { table_id: u64, name: []const u8, native: bool, schema: [32]u8, query_schema: ?[32]u8 = null };
pub const Read = struct {
    mode: enum { entity, proposal, proposals, receipt, policy } = .entity,
    key_template: []const u8 = "",
    texts: []const []const u8 = &.{},
    aliases: []const @import("antfly_resolver").KeyAlias = &.{},
    distinct: bool = false,
    expected_head: ?records.Stamp = null,
    key: []const u8 = "",
    into: ?[]const u8 = null,
    seeds: []const []const u8 = &.{},
    profiles: []const AliasProfile = &.{},
    static_profiles: bool = false,
    include_proposals: bool = true,
    headers_only: bool = false,
    heads_only: bool = false,
    key_templates: []const []const u8 = &.{},
    id: []const u8 = "",
    state: ?c.ProposalState = null,
    action: ?c.EntityAction = null,
    cursor: ?[]const u8 = null,
    limit: u32 = 64,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const include = if (comptime std.mem.eql(u8, field, "static_profiles")) self.static_profiles else if (comptime std.mem.eql(u8, field, "heads_only")) self.heads_only else true;
            if (include) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};
pub const View = struct {
    version: u8 = 1,
    native: bool,
    table_id: u64,
    namespaces: []const records.Snapshot = &.{},
    profiles_indexed: bool = false,
    sparse_profiles: bool = false,
    /// Certified by the same v7 namespace routing directory as sparse profiles.
    has_consumers: ?bool = null,
    profiles: []const AliasProfile = &.{},
    queue_revision: u64 = 0,
    headers_only: bool = false,
    proposals: c.State = .{},
    next_cursor: ?[]const u8 = null,
    receipt: bool = false,
    /// null denotes a legacy receipt; callers conservatively refresh metadata.
    definition_changed: ?bool = null,
    stamp: ?@import("api.zig").CatalogMutationStamp = null,
};
/// Bounded internal replay contract. The job digest fences cursor rotation,
/// source enrollment and coalesced invalidations across an owner/Raft turn.
pub const ReplayAdvance = struct { table_id: u64, catalog_revision: u64, standalone: bool, job_digest: [32]u8 };
pub const ReplayWork = struct {
    advance: ReplayAdvance,
    binding_only: bool,
    refresh_sources: []const u64 = &.{},
};
pub const Bootstrap = struct { expected_indexes: [32]u8 };
pub const Write = struct {
    version: u8 = 1,
    table_id: u64,
    catalog_revision: ?u64 = null,
    standalone: bool = false,
    schema: [32]u8,
    /// Used only to adopt existing authority; native edits never carry it.
    bootstrap: ?Bootstrap = null,
    patches: []const records.Patch = &.{},
    queue: ?records.QueuePatch = null,
    owned_templates: []const []const u8 = &.{},
    pub fn receipt(self: Write, a: A) ![]const u8 {
        const digest = try @import("antfly_resolver").fingerprint(a, self);
        return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    }
    pub fn validate(self: Write) !void {
        if (self.version != 1 or self.catalog_revision == null or self.table_id == 0 or self.patches.len > 4096 or self.owned_templates.len > 4096) return error.InvalidResolverCurationRequest;
        for (self.patches) |patch| if (patch.next.key_template.len == 0 or patch.edits.len > 16384) return error.InvalidResolverCurationRequest;
    }
};
pub fn isNative(root: std.json.Value) bool {
    return root == .object and if (root.object.get(marker)) |value| value == .integer and value.integer == 1 else false;
}
/// Curation head updates cannot race independent namespace edits through an
/// unrelated table-definition CAS. Schema/identity changes remain a live fence.
pub fn schemaFingerprint(a: A, table: metadata.TableRecord) ![32]u8 {
    var copy = table;
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (table.indexes_json.len == 0) "{}" else table.indexes_json, .{});
    if (root != .object) return error.InvalidResolverCurationRequest;
    inline for (.{ marker, owners_key, "_resolver_namespaces", "_curation_proposals" }) |key| _ = root.object.swapRemove(key);
    const keys = try a.dupe([]const u8, root.object.keys());
    std.mem.sort([]const u8, keys, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    var sorted: std.json.ObjectMap = .empty;
    for (keys) |key| try sorted.put(a, key, root.object.get(key).?);
    copy.indexes_json = try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = sorted }, .{});
    return @import("antfly_resolver").fingerprint(a, copy);
}
pub fn queryDefinitionFingerprint(a: A, table: metadata.TableRecord) ![32]u8 {
    var copy = table;
    copy.indexes_json = "{}";
    return @import("antfly_resolver").fingerprint(a, .{ try schemaFingerprint(a, copy), try sourceDefinitionFingerprint(a, table.indexes_json) });
}
/// Stable source declarations exclude destination discovery and derived
/// projection clocks. Private head publication cannot invalidate other scopes.
pub fn sourceDefinitionFingerprint(a: A, indexes: []const u8) ![32]u8 {
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (indexes.len == 0) "{}" else indexes, .{});
    inline for (.{ marker, owners_key, "_resolver_namespaces", "_curation_proposals", "_resolver_curation" }) |key| _ = root.object.swapRemove(key);
    const keys = try a.dupe([]const u8, root.object.keys());
    std.mem.sort([]const u8, keys, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.less);
    var sorted: std.json.ObjectMap = .empty;
    for (keys) |key| try sorted.put(a, key, root.object.get(key).?);
    var buffer: [1024]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
    try std.json.Stringify.value(std.json.Value{ .object = sorted }, .{}, &writer.writer);
    try writer.writer.flush();
    var hash: [32]u8 = undefined;
    writer.hasher.final(&hash);
    return hash;
}
pub fn mutateOnService(svc: anytype, a: A, write: Write, context: local.api_operation.RequestContext) ![]u8 {
    try context.ensureActive();
    try write.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const body = try std.json.Stringify.valueAlloc(a, write, .{});
    defer a.free(body);
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_resolver_authority = body });
    svc.waitForTransitionApplied(receipt) catch return error.MetadataMutationOutcomeUnknown;
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const receipt_id = try write.receipt(a);
    defer a.free(receipt_id);
    const bytes = try store.resolverCatalogRead(a, svc.metadata_group_id, .{ .table_name = "native", .table_id = write.table_id, .mode = .authority, .authority = .{ .mode = .receipt, .id = receipt_id } });
    errdefer a.free(bytes);
    const result = try std.json.parseFromSlice(View, a, bytes, .{});
    defer result.deinit();
    if (!result.value.receipt) return error.ResolverCurationConflict;
    try context.ensureActive();
    var response = result.value;
    const identity = try svc.catalogIdentity();
    response.stamp = .{ .metadata_group_id = identity.metadata_group_id, .metadata_incarnation = identity.metadata_incarnation, .term = receipt.term, .index = receipt.index };
    const owned = try std.json.Stringify.valueAlloc(a, response, .{});
    a.free(bytes);
    return owned;
}
