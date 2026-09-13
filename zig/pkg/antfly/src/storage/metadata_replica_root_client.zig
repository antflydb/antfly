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

//! Coarse client for one complete local replica-root provisioning round.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const provision_contract = @import("../metadata/provision_contract.zig");
const table_manager = @import("../metadata/table_manager.zig");

pub const ProvisionSummary = provision_contract.ProvisionSummary;

pub fn reconcile(
    alloc: std.mem.Allocator,
    context: ?*anyopaque,
    replica_root_dir: []const u8,
    metadata_group_id: u64,
    group_ids: []const u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
) !ProvisionSummary {
    const request_json = std.json.Stringify.valueAlloc(alloc, .{
        .group_ids = group_ids,
        .tables = tables,
        .ranges = ranges,
    }, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer alloc.free(request_json);
    var summary: abi.MetadataProvisionSummary = .{};
    try error_identity.statusToError(abi.antfly_metadata_reconcile_replica_root(&.{
        .context = context,
        .replica_root_dir = .fromSlice(replica_root_dir),
        .metadata_group_id = metadata_group_id,
        .request_json = .fromSlice(request_json),
    }, &summary));
    return .{
        .groups_considered = try toUsize(summary.groups_considered),
        .dbs_opened = try toUsize(summary.dbs_opened),
        .indexes_added = try toUsize(summary.indexes_added),
        .indexes_removed = try toUsize(summary.indexes_removed),
        .indexes_pending = try toUsize(summary.indexes_pending),
        .enrichments_added = try toUsize(summary.enrichments_added),
        .enrichments_updated = try toUsize(summary.enrichments_updated),
        .enrichments_removed = try toUsize(summary.enrichments_removed),
        .resolvers_added = try toUsize(summary.resolvers_added),
        .resolvers_updated = try toUsize(summary.resolvers_updated),
        .resolvers_removed = try toUsize(summary.resolvers_removed),
    };
}

fn toUsize(value: u64) !usize {
    return std.math.cast(usize, value) orelse error.StorageKernelFailure;
}
