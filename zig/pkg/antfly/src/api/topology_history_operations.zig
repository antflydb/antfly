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

//! Fenced split predecessor and merge donor history reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/api.zig");
pub const Split = Contract(local.storage_committed_history.SplitCut);
pub const Merge = Contract(local.storage_committed_history.MergeCut);

fn Contract(comptime Cut: type) type {
    return struct {
        pub const wire = Cut;
        const Context = local.api_operation.RequestContext;
        const A = std.mem.Allocator;
        pub const max_bytes = wire.max_read_bytes;
        fn validTopology(input: wire.Request) bool {
            if (comptime Cut == local.storage_committed_history.SplitCut)
                return input.transition != 0 and input.attempt != 0 and input.successor_group != 0;
            return input.transition != 0 and input.receiver_group != 0;
        }

        pub fn validate(input: wire.Request, fence: metadata.CatalogRouteFence, table: []const u8) !void {
            try fence.validate();
            const identity = fence.route.identity_namespace;
            if (table.len == 0 or table.len > 1024 or !validTopology(input) or
                std.mem.readInt(u64, input.namespace[0..8], .big) != identity.table_id or
                std.mem.readInt(u64, input.namespace[8..16], .big) != identity.shard_id or
                std.mem.readInt(u64, input.namespace[16..24], .big) != identity.range_id) return error.InvalidHistoryAdmission;
        }

        pub fn execute(port: ?Port, a: A, group: u64, table: []const u8, body: []const u8, context: Context) !?wire {
            try context.ensureActive();
            if (group == 0 or body.len > max_bytes or context.catalog_route_fence_json.len == 0 or context.catalog_route_fence_json.len > 16384)
                return error.InvalidCatalogRouteFence;
            var decoded_fence = std.json.parseFromSlice(metadata.CatalogRouteFence, a, context.catalog_route_fence_json, .{}) catch |err|
                return if (err == error.OutOfMemory) err else error.InvalidCatalogRouteFence;
            defer decoded_fence.deinit();
            const fence = decoded_fence.value;
            if (group != fence.route.group_id) return error.InvalidCatalogRouteFence;
            var parsed = std.json.parseFromSlice(wire.Request, a, body, .{}) catch |err|
                return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
            defer parsed.deinit();
            try validate(parsed.value, fence, table);
            return (port orelse return error.ChangefeedCapabilityUnsupported).read(a, fence, table, parsed.value, context);
        }

        pub const Port = struct {
            ptr: *anyopaque,
            read_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.Request, Context) anyerror!?wire,
            boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

            pub const VTable = struct { read: @FieldType(Port, "read_fn") };
            const Boundary = local.runtime_callback_abi.Boundary(VTable);

            pub fn read(self: Port, a: A, fence: metadata.CatalogRouteFence, table: []const u8, input: wire.Request, original: Context) !?wire {
                var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
                const request = scope.context();
                try request.ensureActive();
                try validate(input, fence, table);
                const result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, request });
                try request.ensureActive();
                if (result) |cut| if (!input.matches(cut)) return error.HistoryCorrupt;
                return result;
            }
        };
    };
}
