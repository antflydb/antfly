// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Immutable catalog route identity and borrowed local admission context.
const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const MetadataClusterIncarnation = @import("catalog_mutation_stamp.zig").MetadataClusterIncarnation;

pub const CatalogIdentityNamespace = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,
};

pub const CatalogGroupRoute = struct {
    group_id: u64,
    range_id: u64,
    identity_namespace: CatalogIdentityNamespace,
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
    /// Receiver-local admission context. These fields are intentionally
    /// excluded from the wire representation: monotonic clocks and borrowed
    /// cancellation callbacks are process-local capabilities.
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
        };
    }

    pub fn validate(self: @This()) !void {
        if (self.protocol != catalog_route_fence_protocol_current) return error.UnsupportedCatalogRouteFence;
        if (self.metadata_group_id == 0 or self.table_id == 0 or self.route.group_id == 0) return error.InvalidCatalogRouteFence;
        if (self.route.identity_namespace.table_id != self.table_id) return error.InvalidCatalogRouteFence;
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
        };
    }
};
