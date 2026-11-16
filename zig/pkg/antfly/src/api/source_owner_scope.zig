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

//! Shared immutable owner/route qualification for private source reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
const Owner = local.storage_transaction_completion.RoutedOwner;
const A = std.mem.Allocator;
pub const Scope = struct {
    parsed: std.json.Parsed(metadata.CatalogRouteFence),
    fence: metadata.CatalogRouteFence,
    pub fn init(a: A, group: u64, table: []const u8, owner: Owner, context: operation.RequestContext) !Scope {
        try context.ensureActive();
        if (group == 0 or table.len == 0 or table.len > 1024 or context.catalog_route_fence_json.len == 0 or context.catalog_route_fence_json.len > 16384) return error.InvalidCatalogRouteFence;
        var parsed = try std.json.parseFromSlice(metadata.CatalogRouteFence, a, context.catalog_route_fence_json, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
        errdefer parsed.deinit();
        var fence = parsed.value;
        try fence.validate();
        const identity = fence.route.identity_namespace;
        const incarnation = fence.metadata_incarnation orelse return error.InvalidCatalogRouteFence;
        if (owner.metadata_group_id != fence.metadata_group_id or !std.mem.eql(u8, &owner.metadata_incarnation, &incarnation) or owner.topology_epoch != fence.topology_epoch) return error.InvalidCatalogRouteFence;
        if (fence.route.group_id != group or owner.group_id != group or std.mem.readInt(u64, owner.namespace[0..8], .big) != identity.table_id or std.mem.readInt(u64, owner.namespace[8..16], .big) != identity.shard_id or std.mem.readInt(u64, owner.namespace[16..24], .big) != identity.range_id) return error.InvalidCatalogRouteFence;
        fence.admission_deadline_ns = context.deadline_ns;
        fence.admission_deadline_io = context.deadline_io;
        fence.admission_cancellation = context.cancellation;
        return .{ .parsed = parsed, .fence = fence };
    }
    pub fn deinit(self: *Scope) void {
        self.parsed.deinit();
    }
};
