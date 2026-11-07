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

//! Immutable catalog route identity and borrowed local admission context.
const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const MetadataClusterIncarnation = @import("catalog_mutation_stamp.zig").MetadataClusterIncarnation;

pub const CatalogIdentityNamespace = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,
};

/// Native-local rows retain the cluster catalog realm while their only row
/// owner is an exact admitted process/store generation, never a Raft leader.
pub const RouteExecution = union(enum) {
    raft,
    native_local: @import("../common/topology_records.zig").RelationPersistence.Descriptor,

    pub fn fromPersistence(value: ?@import("../common/topology_records.zig").RelationPersistence.Descriptor) RouteExecution {
        return if (value) |descriptor| .{ .native_local = descriptor } else .raft;
    }
    pub fn validate(self: RouteExecution) !void {
        if (self == .native_local) try self.native_local.validate();
    }
};

pub const CatalogGroupRoute = struct {
    group_id: u64,
    range_id: u64,
    identity_namespace: CatalogIdentityNamespace,
    execution: RouteExecution = .raft,

    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return (comptime std.mem.eql(u8, field, "execution")) and self.execution == .raft;
    }

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const omit = (comptime std.mem.eql(u8, field, "execution")) and self.execution == .raft;
            if (!omit) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};

pub const catalog_route_fence_protocol_current: u16 = 1;
pub const catalog_route_fence_header = "X-Antfly-Catalog-Route-Fence";
pub const catalog_route_fence_ack_header = "X-Antfly-Catalog-Route-Fence-Ack";
pub const catalog_route_fence_ack_value = "1";
/// Separate from routing acknowledgement: emitted only after a successful
/// fenced read-index lookup proves the logical key absent.
pub const read_index_absence_header = "X-Antfly-Read-Index-Absence";
pub const read_index_absence_value = "1";
pub const catalog_route_deadline_ms_header = "X-Antfly-Catalog-Route-Deadline-Ms";
pub const catalog_route_default_deadline_ms: u32 = 5_000;
pub const catalog_route_max_deadline_ms: u32 = 30_000;

/// Immutable authority and identity carried with every first-party
/// group-local read. The receiver validates this against its compact routing
/// projection before opening storage, so an independently cached admin
/// snapshot can never select a different table generation.
pub const CatalogRouteFence = struct {
    protocol: u16 = catalog_route_fence_protocol_current,
    metadata_group_id: u64,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    catalog_revision: u64,
    table_id: u64,
    topology_epoch: u64,
    route: CatalogGroupRoute,
    /// Immutable initialized image pinned before native participant admission.
    /// It authorizes no user work and never changes with a process restart.
    native_participant: ?@import("../common/relation_persistence.zig").PhysicalPin = null,
    /// Receiver-local admission context. These fields are intentionally
    /// excluded from the wire representation: monotonic clocks and borrowed
    /// cancellation callbacks are process-local capabilities.
    /// Authenticated receiver-local owner, populated only after a signed
    /// lifetime grant has been verified. Never accepted from the fence JSON.
    admission_sql_relation_owner: ?@import("../common/relation_persistence.zig").Owner = null,
    admission_relation_minter: ?@import("../common/relation_owner_authority.zig").Minter = null,
    admission_relation_grant: []const u8 = "",
    /// Distinct trigger preparation purpose; never an owner/namespace grant.
    admission_trigger_minter: ?@import("../common/trigger_preparation_authority.zig").Minter = null,
    admission_trigger_grant: []const u8 = "",

    admission_body_digest: ?[32]u8 = null,
    admission_command_digest: ?[32]u8 = null,
    admission_deadline_ns: ?u64 = null,
    admission_deadline_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
    admission_cancellation: CancellationToken = .none,

    const Wire = struct {
        protocol: u16 = catalog_route_fence_protocol_current,
        metadata_group_id: u64,
        metadata_incarnation: ?MetadataClusterIncarnation = null,
        catalog_revision: u64,
        table_id: u64,
        topology_epoch: u64,
        route: CatalogGroupRoute,
        native_participant: ?@import("../common/relation_persistence.zig").PhysicalPin = null,

        pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
            return (comptime std.mem.eql(u8, field, "native_participant")) and self.native_participant == null;
        }
        pub fn jsonStringify(self: @This(), writer: anytype) !void {
            try writer.beginObject();
            inline for (@typeInfo(@This()).@"struct".field_names) |field| {
                if (!self.nativeJsonSkipField(field)) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            }
            try writer.endObject();
        }
    };

    fn wire(self: @This()) Wire {
        return .{
            .protocol = self.protocol,
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .catalog_revision = self.catalog_revision,
            .table_id = self.table_id,
            .topology_epoch = self.topology_epoch,
            .route = self.route,
            .native_participant = self.native_participant,
        };
    }

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(self.wire());
    }

    /// The binary-safe durable transaction encoder also uses the wire-only
    /// projection; borrowed process-local admission callbacks are never data.
    pub fn nativeJsonProjection(self: @This()) Wire {
        return self.wire();
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        const value = try std.json.innerParse(Wire, alloc, source, options);
        return .{
            .protocol = value.protocol,
            .metadata_group_id = value.metadata_group_id,
            .metadata_incarnation = value.metadata_incarnation,
            .catalog_revision = value.catalog_revision,
            .table_id = value.table_id,
            .topology_epoch = value.topology_epoch,
            .route = value.route,
            .native_participant = value.native_participant,
        };
    }

    pub fn validate(self: @This()) !void {
        if (self.protocol != catalog_route_fence_protocol_current) return error.UnsupportedCatalogRouteFence;
        if (self.metadata_group_id == 0 or self.table_id == 0 or self.route.group_id == 0) return error.InvalidCatalogRouteFence;
        if (self.route.identity_namespace.table_id != self.table_id) return error.InvalidCatalogRouteFence;
        try self.route.execution.validate();
        if (self.native_participant) |pin| {
            try pin.validate();
            if (self.route.execution != .native_local or pin.table_id != self.table_id or
                pin.shard_id != self.route.identity_namespace.shard_id or pin.range_id != self.route.identity_namespace.range_id or
                pin.data_generation != self.route.execution.native_local.data_generation) return error.InvalidCatalogRouteFence;
        }
    }

    pub fn jsonParseFromValue(alloc: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        const parsed = try std.json.innerParseFromValue(Wire, alloc, value, options);
        return .{
            .protocol = parsed.protocol,
            .metadata_group_id = parsed.metadata_group_id,
            .metadata_incarnation = parsed.metadata_incarnation,
            .catalog_revision = parsed.catalog_revision,
            .table_id = parsed.table_id,
            .topology_epoch = parsed.topology_epoch,
            .route = parsed.route,
            .native_participant = parsed.native_participant,
        };
    }
};

test "native local catalog route binds exact owner while ordinary wire and local grant stay private" {
    const a = std.testing.allocator;
    var fence: CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } };
    const ordinary = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(ordinary);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "execution") == null);
    const Generic = struct {
        value: CatalogRouteFence,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            return @import("../storage/db/relational_integrity_json.zig").write(self.value, stream);
        }
    };
    const durable_ordinary = try std.json.Stringify.valueAlloc(a, Generic{ .value = fence }, .{});
    defer a.free(durable_ordinary);
    try std.testing.expectEqualStrings(ordinary, durable_ordinary);

    fence.route.execution = .{ .native_local = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 8, .generation_instance = @splat(9), .data_generation = 10 } };
    fence.admission_relation_grant = "private-owner-grant";
    fence.admission_body_digest = @splat(11);
    fence.admission_command_digest = @splat(12);
    const native = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(native);
    try std.testing.expect(std.mem.indexOf(u8, native, "private-owner-grant") == null);
    try std.testing.expect(std.mem.indexOf(u8, native, "admission_") == null);
    var decoded = try std.json.parseFromSlice(CatalogRouteFence, a, native, .{});
    defer decoded.deinit();
    try decoded.value.validate();
    try std.testing.expect(std.meta.eql(fence.route, decoded.value.route));
    try std.testing.expect(decoded.value.admission_sql_relation_owner == null);
    try std.testing.expect(decoded.value.admission_relation_minter == null);
    var replaced = decoded.value.route;
    replaced.execution.native_local.data_generation += 1;
    try std.testing.expect(!std.meta.eql(fence.route, replaced));
}
