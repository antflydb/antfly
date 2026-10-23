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

//! Table-scoped pre-admission facts, not a reservation or execution grant.
//! Metadata owns the complete before/after records and range cover. Ingress
//! supplies only its authorized logical owner and affected column names.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const contract = local.storage_db_relational_schema_owner_contract;
const tables = @import("table_manager.zig");
const schema_api = @import("../api/tables.zig");
const barriers = @import("schema_validation_publication.zig");
pub const max_wire_bytes = @import("schema_validation_plan.zig").max_bytes;

pub const Request = struct {
    guard: domain.RelationMutationGuard,
    columns: []const []const u8,

    pub fn validate(self: @This()) !void {
        try self.guard.validate();
        if (self.guard.owner.kind != .table or self.guard.constraint_name_admission != null) return error.InvalidCatalogMutation;
        var projection = contract.ProjectionHasher.init();
        for (self.columns) |column| try projection.add(column);
        _ = try projection.finish();
    }
};

/// Current authorization and retained job observation share one metadata cut.
/// Possessing a token never authorizes lookup of a former logical table owner.
pub const StatusRequest = struct {
    guard: domain.RelationMutationGuard,
    lookup: @import("schema_validation_read.zig").Lookup,

    pub fn validate(self: @This()) !void {
        try self.guard.validate();
        try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = self.lookup, .ordinal = 0 }).validate();
        if (self.guard.owner.kind != .table or self.guard.constraint_name_admission != null or
            self.guard.owner.table_id != self.lookup.table or !std.mem.eql(u8, &std.fmt.bytesToHex(self.guard.incarnation, .lower), &self.lookup.metadata_incarnation)) return error.CatalogGenerationChanged;
    }
};

pub const Status = struct {
    pub const max_wire_bytes = 16 * 1024;
    lookup: @import("schema_validation_read.zig").Lookup,
    head: ?barriers.Head,
    active_schema_version: u32,

    pub fn requireFor(self: @This(), request: StatusRequest) !void {
        try request.validate();
        if (!std.meta.eql(self.lookup, request.lookup) or self.active_schema_version != request.guard.owner.schema_version) return error.CatalogGenerationChanged;
        if (self.head) |head| try self.lookup.require(head);
    }
};

pub const Cut = struct {
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    table_guard: domain.RelationMutationGuard,
    before: tables.TableRecord,
    after: tables.TableRecord,
    ranges: []const tables.RangeRecord,
    authority: local.storage_source_authority.Kind,

    pub fn requireRequest(self: @This(), a: std.mem.Allocator, request: Request) !void {
        try request.validate();
        try self.table_guard.validate();
        const actual = self.table_guard;
        const expected = request.guard;
        if (self.metadata_group_id == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation) or
            !actual.owner.eql(expected.owner) or !std.mem.eql(u8, &actual.incarnation, &expected.incarnation) or
            !std.mem.eql(u8, actual.target.database, expected.target.database) or
            !std.mem.eql(u8, actual.target.namespace, expected.target.namespace) or
            !std.mem.eql(u8, actual.target.name, expected.target.name) or
            !std.mem.eql(u8, actual.logical_table, expected.logical_table) or actual.constraint_name_admission != null or
            self.before.table_id != expected.owner.table_id or self.before.name.len == 0 or
            !std.mem.eql(u8, &contract.hash(self.before.schema_json), &expected.owner.schema_digest)) return error.CatalogGenerationChanged;
        var incarnation: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&incarnation, &self.metadata_incarnation) catch return error.InvalidMetadataIncarnation;
        if (!std.mem.eql(u8, &incarnation, &actual.incarnation)) return error.CatalogGenerationChanged;
        if (self.ranges.len == 0 or self.ranges.len > barriers.max_owners) return error.InvalidGenerationPublication;
        var owners: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
        defer owners.deinit(a);
        for (self.ranges) |range| {
            if (range.table_id != self.before.table_id or range.group_id == 0) return error.GenerationPublicationChanged;
            const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = self.before.table_id, .shard_id = tables.rangeDocIdentityShardId(range), .range_id = tables.rangeDocIdentityRangeId(range) };
            const address: @import("schema_validation_read.zig").OwnerAddress = .{ .group_id = range.group_id, .namespace = local.storage_db_online_source_contract.namespaceBytes(namespace) };
            if ((try owners.getOrPut(a, address.digest())).found_existing) return error.GenerationPublicationChanged;
        }
        try tables.validateCompleteKeyspaceRanges(self.ranges);
        const replacement = try schema_api.prepareNullabilityPublicationRecord(a, &self.before, request.columns);
        defer tables.freeTable(a, replacement);
        if (!tables.tableDefinitionsEqual(replacement, self.after)) return error.InvalidGenerationPublication;
    }

    pub fn encode(self: @This(), a: std.mem.Allocator) ![]u8 {
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_wire_bytes) return error.InvalidGenerationPublication;
        const bytes = try a.alloc(u8, @intCast(count.fullCount()));
        errdefer a.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);
        try std.json.Stringify.value(self, .{}, &writer);
        return bytes;
    }
};
