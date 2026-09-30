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

//! Durable route identity and process-local admission capabilities shared by
//! transaction records and storage owners. Catalog coordination stays in api.zig.
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
