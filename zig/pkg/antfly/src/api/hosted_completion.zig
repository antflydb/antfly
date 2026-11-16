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

//! Metadata-owned completion transport. Endpoint selection never substitutes
//! a placement or arbitrary replica for a positively known eligible leader.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/api.zig");
const group_ops = @import("internal_group_operations.zig");
const owner_ops = @import("history_owner_operations.zig");
const decision_ops = @import("history_decision_operations.zig");
const resolution_ops = @import("history_resolution_operations.zig");
const publication_ops = @import("history_publication_operations.zig");
const client_mod = @import("http_client.zig");
const participant_mod = @import("completion_participant.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;

pub const Hosted = struct {
    catalog: @import("table_catalog.zig").CatalogSource,
    router: @import("table_router.zig").HostedGroupRouter,
    executor: @import("../raft/transport/http_common.zig").RequestExecutor,
    secret: ?[]const u8,
    issuer: ?[]const u8,

    /// Returned callbacks borrow this adapter only for the synchronous commit.
    /// Native retained state, not this adapter, owns subsequent recovery.
    pub fn coordinator(self: *Hosted) @import("completion_admission.zig").Coordinator {
        return .{ .owners = .{ .ptr = self, .read_fn = Reader(owner_ops, owner_ops.Reply, "fetchGroupHistoryOwner").read }, .recovery = .{
            .catalog = self.catalog,
            .decisions = .{ .ptr = self, .read_fn = Reader(decision_ops, decision_ops.Reply, "fetchGroupHistoryDecision").read },
            .resolutions = .{ .ptr = self, .read_fn = Reader(resolution_ops, resolution_ops.wire.Reply, "fetchGroupHistoryResolution").read },
            .publications = .{ .ptr = self, .read_fn = Reader(publication_ops, publication_ops.Reply, "fetchGroupHistoryPublication").read },
            .participants = .{ .writer = .{ .ptr = self, .write_fn = write } },
        } };
    }
    fn client(self: *Hosted, a: A) client_mod.ApiHttpClient {
        var result = client_mod.ApiHttpClient.init(a, self.executor);
        _ = result.withInternalServiceAuth(self.secret, self.issuer);
        return result;
    }
    fn eligible(router: @import("table_router.zig").HostedGroupRouter, group: u64, node: u64) bool {
        if (node == router.localNodeId()) return router.localStatus(group) == .active;
        return if (router.nodeStatus(node, group)) |status| status == .active else false;
    }
    fn endpoint(self: *Hosted, a: A, group: u64, context: Context) ![]u8 {
        try context.ensureActive();
        if (group == 0) return error.InvalidCatalogRouteFence;
        if (self.secret == null) return error.Unauthorized;
        const router = self.router.withBudget(.{ .clock = .{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }, .cancellation = context.cancellation });
        try router.budget.check();
        const leader = router.groupLeaderNodeId(group) orelse return error.GroupLeaderUnavailable;
        if (leader == 0 or !eligible(router, group, leader)) return error.GroupLeaderUnavailable;
        const uri = (try router.nodeBaseUriForGroup(a, group, leader)) orelse return error.GroupLeaderUnavailable;
        errdefer a.free(uri);
        try context.ensureActive();
        if (uri.len == 0 or router.groupLeaderNodeId(group) != leader or !eligible(router, group, leader)) return error.GroupLeaderUnavailable;
        return uri;
    }
    fn Reader(comptime Ops: type, comptime Reply: type, comptime method: []const u8) type {
        return struct {
            fn read(ptr: *anyopaque, a: A, fence: metadata.CatalogRouteFence, table: []const u8, input: Ops.Request, context: Context) !Reply {
                const self: *Hosted = @ptrCast(@alignCast(ptr));
                const uri = try self.endpoint(a, fence.route.group_id, context);
                defer a.free(uri);
                var signed = self.client(a);
                return @field(client_mod.ApiHttpClient, method)(&signed, uri, fence.route.group_id, table, input, fence, context);
            }
        };
    }
    fn write(ptr: *anyopaque, a: A, authority: group_ops.RoutedBatchAuthority, group: u64, table: []const u8, request: local.storage_db_selected_root.db.types.BatchRequest, forwarding: @import("internal_batch_forwarding.zig").Context, context: Context) !?void {
        const self: *Hosted = @ptrCast(@alignCast(ptr));
        if (authority != .catalog or authority.catalog.route.group_id != group) return error.InvalidCatalogRouteFence;
        const uri = try self.endpoint(a, group, context);
        defer a.free(uri);
        var signed = self.client(a);
        var writer: participant_mod.HttpWriter = .{ .client = &signed, .base_uri = uri };
        // Exactly one mutation attempt. A failed response cannot authorize
        // rediscovery/replay, even when the current leader subsequently moves.
        return writer.writer().write(a, authority, group, table, request, forwarding, context);
    }
};

test "data raft ordered history hosted endpoints require stable eligible leaders before dispatch" {
    const Router = struct {
        leader: ?u64 = 1,
        active: bool = true,
        move_during_lookup: bool = false,
        lookups: usize = 0,
        fn localNode(_: *anyopaque) u64 {
            return 0;
        }
        fn localStatus(_: *anyopaque, _: u64) @import("../raft/host.zig").HostedReplicaStatus {
            return .absent;
        }
        fn leaderNode(ptr: *anyopaque, _: u64) ?u64 {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).leader;
        }
        fn status(ptr: *anyopaque, _: u64, _: u64) @import("../raft/host.zig").HostedReplicaStatus {
            return if ((@as(*@This(), @ptrCast(@alignCast(ptr)))).active) .active else .absent;
        }
        fn arbitrary(_: *anyopaque, _: A, _: u64) !?[]u8 {
            return error.UnexpectedFallback;
        }
        fn endpoint(ptr: *anyopaque, a: A, group: u64, node: u64, budget: @import("table_router.zig").RouteBudget) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.check();
            try std.testing.expectEqual(@as(u64, 9), group);
            try std.testing.expectEqual(@as(u64, 1), node);
            self.lookups += 1;
            if (self.move_during_lookup) self.leader = 2;
            return try a.dupe(u8, "http://leader.invalid");
        }
        fn router(self: *@This()) @import("table_router.zig").HostedGroupRouter {
            return .{ .ptr = self, .vtable = &.{ .local_node_id = localNode, .local_status = localStatus, .group_leader_node_id = leaderNode, .node_status = status, .node_base_uri = arbitrary, .node_base_uri_for_group = endpoint } };
        }
    };
    const a = std.testing.allocator;
    var router: Router = .{};
    var hosted: Hosted = .{ .catalog = undefined, .router = router.router(), .executor = undefined, .secret = "history-contract-test-secret", .issuer = "test" };
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s };
    const uri = try hosted.endpoint(a, 9, context);
    a.free(uri);
    try std.testing.expectEqual(@as(usize, 1), router.lookups);
    router.leader = null;
    try std.testing.expectError(error.GroupLeaderUnavailable, hosted.endpoint(a, 9, context));
    router.leader = 1;
    router.active = false;
    try std.testing.expectError(error.GroupLeaderUnavailable, hosted.endpoint(a, 9, context));
    try std.testing.expectEqual(@as(usize, 1), router.lookups);
    router.active = true;
    router.move_during_lookup = true;
    try std.testing.expectError(error.GroupLeaderUnavailable, hosted.endpoint(a, 9, context));
    try std.testing.expectEqual(@as(usize, 2), router.lookups);
    hosted.secret = null;
    try std.testing.expectError(error.Unauthorized, hosted.endpoint(a, 9, context));
    try std.testing.expectEqual(@as(usize, 2), router.lookups);
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, hosted.endpoint(a, 9, .{ .cancellation = .fromAtomic(&cancelled) }));
    try std.testing.expectEqual(@as(usize, 2), router.lookups);
}
