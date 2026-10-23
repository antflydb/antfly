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

//! Coarse client for one complete local replica-root provisioning round.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const provision_contract = @import("antfly_provision_contract");
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
