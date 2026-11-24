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

//! Authenticated bounded inventory reads. Each reply binds its complete query;
//! owned records are released on failed qualification or late cancellation.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const wire = @import("../data/source_legacy_inventory_http_wire.zig");
const metadata = @import("../metadata/api.zig");
const operation = local.api_operation;
const A = std.mem.Allocator;
pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.Request, operation.RequestContext) anyerror!wire.Reply,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(Port, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn read(self: Port, a: A, fence: metadata.CatalogRouteFence, table: []const u8, input: wire.Request, context: operation.RequestContext) !wire.Reply {
        return Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, context });
    }
};
pub fn execute(port: ?Port, a: A, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.Reply {
    try context.ensureActive();
    const input = try wire.decodeRequest(a, body);
    const digest = try input.digest(a);
    var route = try @import("source_owner_scope.zig").Scope.init(a, group, table, input.query.owner, context);
    defer route.deinit();
    var request_scope = try @import("history_publication_operations.zig").Scope.init(route.fence, context);
    const bounded = request_scope.context();
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var result = try target.read(a, route.fence, table, input, bounded);
    errdefer result.deinit(a);
    try result.validate(input, digest);
    try bounded.ensureActive();
    return result;
}

test "source legacy inventory ingress qualifies owned results and frees failed or cancelled reads" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        forge: bool = false,
        cancel_after: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn read(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, input: wire.Request, context: operation.RequestContext) !wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            self.calls += 1;
            var result = try wire.test_support.reply(alloc, input);
            if (self.forge) result.request_digest[0] ^= 1;
            if (self.cancel_after) self.cancelled.store(true, .release);
            return result;
        }
    };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .read_fn = Fake.read };
    const input = wire.test_support.fixture();
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = input.query.owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const fence_json = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(fence_json);
    const body = try wire.encodeRequest(a, input);
    defer a.free(body);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = fence_json };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    var observed = try execute(port, a, 17, "docs", body, context);
    defer observed.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), observed.observation.page.items.len);
    fake.forge = true;
    try std.testing.expectError(error.SourceAdmissionCorrupt, execute(port, a, 17, "docs", body, context));
    fake.forge = false;
    fake.cancel_after = true;
    var bounded = context;
    bounded.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled);
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, bounded));
    const calls = fake.calls;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, bounded));
    try std.testing.expectEqual(calls, fake.calls);
}
