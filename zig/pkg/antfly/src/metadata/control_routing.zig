// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Routing policy over one controlled, coherent metadata capture.
const std = @import("std");
const api_table_router = @import("../api/table_router.zig");
const Snapshot = @import("storage/raft_apply_contract.zig").ControlRoutingProjection;
pub fn resolve(alloc: std.mem.Allocator, snapshot: Snapshot, local_node_id: ?u64, group_ids: []const u64, budget: api_table_router.RouteBudget, control: @import("antfly_local_sources").common_read_admission.Port) !?[]api_table_router.GroupRoute {
    try budget.check();
    const GroupNodeKey = struct { group_id: u64, node_id: u64 };
    const ServingSummary = struct { first_node_id: u64, has_other_node: bool = false };
    const CandidateState = struct {
        candidate: DataBearingStoreCandidate,
        api_url: []const u8,
        has_data: bool = false,
    };

    var requested_groups = std.AutoHashMapUnmanaged(u64, void).empty;
    defer requested_groups.deinit(alloc);
    try requested_groups.ensureTotalCapacity(alloc, @intCast(group_ids.len));
    for (group_ids) |group_id| {
        try budget.check();
        requested_groups.putAssumeCapacity(group_id, {});
    }

    // Build readability once. In particular, a draining source remains
    // readable only until another serving replica exists for that group.
    var serving_by_group = std.AutoHashMapUnmanaged(u64, ServingSummary).empty;
    defer serving_by_group.deinit(alloc);
    try serving_by_group.ensureTotalCapacity(alloc, @intCast(group_ids.len));
    for (snapshot.placements) |intent| {
        try budget.check();
        if (intent.serving_state != .serving or !requested_groups.contains(intent.group_id)) continue;
        const entry = serving_by_group.getOrPutAssumeCapacity(intent.group_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{ .first_node_id = intent.node_id };
        } else if (entry.value_ptr.first_node_id != intent.node_id) {
            entry.value_ptr.has_other_node = true;
        }
    }

    var readable_replicas = std.AutoHashMapUnmanaged(GroupNodeKey, void).empty;
    defer readable_replicas.deinit(alloc);
    try readable_replicas.ensureTotalCapacity(alloc, @intCast(snapshot.placements.len));
    for (snapshot.placements) |intent| {
        try budget.check();
        if (!requested_groups.contains(intent.group_id)) continue;
        const readable = switch (intent.serving_state) {
            .serving => true,
            .draining => if (serving_by_group.get(intent.group_id)) |summary|
                summary.first_node_id == intent.node_id and !summary.has_other_node
            else
                true,
            .planned, .bootstrapping, .replaying, .cutover_ready, .retiring => false,
        };
        if (readable) readable_replicas.putAssumeCapacity(.{
            .group_id = intent.group_id,
            .node_id = intent.node_id,
        }, {});
    }

    // Aggregate all requested store/group observations in one pass. Candidate
    // count cannot exceed readable placement count, so this stays bounded by
    // the routing snapshot rather than by groups × stores × placements.
    var candidates = std.AutoHashMapUnmanaged(GroupNodeKey, CandidateState).empty;
    defer candidates.deinit(alloc);
    try candidates.ensureTotalCapacity(alloc, @intCast(snapshot.placements.len));
    for (snapshot.stores) |store| {
        try budget.check();
        if ((local_node_id != null and store.node_id == local_node_id.?) or store.api_url.len == 0 or
            !store.live or !std.mem.eql(u8, store.health_class, "healthy")) continue;

        for (store.group_statuses) |status| {
            try budget.check();
            const key = GroupNodeKey{ .group_id = status.group_id, .node_id = store.node_id };
            if (!requested_groups.contains(status.group_id) or !readable_replicas.contains(key)) continue;
            const entry = candidates.getOrPutAssumeCapacity(key);
            if (!entry.found_existing) entry.value_ptr.* = .{
                .candidate = .{ .node_id = store.node_id, .store_id = store.store_id },
                .api_url = store.api_url,
            };
            const state = entry.value_ptr;
            state.candidate.local_leader = state.candidate.local_leader or status.local_leader;
            state.has_data = state.has_data or !status.empty or status.doc_count > 0 or status.disk_bytes > 1024;
            state.candidate.doc_count = @max(state.candidate.doc_count, status.doc_count);
            state.candidate.disk_bytes = @max(state.candidate.disk_bytes, status.disk_bytes);
            state.candidate.updated_at_millis = @max(state.candidate.updated_at_millis, status.updated_at_millis);
        }
    }

    var best_by_group = std.AutoHashMapUnmanaged(u64, CandidateState).empty;
    defer best_by_group.deinit(alloc);
    try best_by_group.ensureTotalCapacity(alloc, @intCast(group_ids.len));
    var candidate_iterator = candidates.iterator();
    while (candidate_iterator.next()) |entry| {
        try budget.check();
        const candidate = entry.value_ptr.*;
        if (!candidate.has_data and !candidate.candidate.local_leader) continue;
        const best = best_by_group.getOrPutAssumeCapacity(entry.key_ptr.group_id);
        if (!best.found_existing or dataBearingStoreCandidateLessThan(candidate.candidate, best.value_ptr.candidate))
            best.value_ptr.* = candidate;
    }

    const routes = try alloc.alloc(api_table_router.GroupRoute, group_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }
    for (group_ids, 0..) |group_id, index| {
        try budget.check();
        const best = best_by_group.get(group_id) orelse {
            for (routes[0..initialized]) |*route| route.deinit(alloc);
            alloc.free(routes);
            return null;
        };
        routes[index] = .{ .remote = .{
            .node_id = best.candidate.node_id,
            .base_uri = try @import("control_routing_codec.zig").copy(alloc, best.api_url, control),
        } };
        initialized += 1;
    }
    try budget.check();
    return routes;
}

const DataBearingStoreCandidate = struct {
    node_id: u64,
    store_id: u64,
    local_leader: bool = false,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    updated_at_millis: u64 = 0,
};

fn dataBearingStoreCandidateLessThan(a: DataBearingStoreCandidate, b: DataBearingStoreCandidate) bool {
    if (a.local_leader != b.local_leader) return a.local_leader;
    if (a.doc_count != b.doc_count) return a.doc_count > b.doc_count;
    if (a.disk_bytes != b.disk_bytes) return a.disk_bytes > b.disk_bytes;
    if (a.updated_at_millis != b.updated_at_millis) return a.updated_at_millis > b.updated_at_millis;
    return a.store_id < b.store_id;
}

const RoutingTestControl = struct {
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    cause: anyerror = error.Canceled,
    fn check(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const i = self.calls;
        self.calls += 1;
        if (i == self.fail_at) return self.cause;
    }
};

test "bounded control routing policy preserves leadership and draining visibility under original admission" {
    const a = std.testing.allocator;
    const read = @import("antfly_local_sources").common_read_admission;
    var state: RoutingTestControl = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = RoutingTestControl.check } };
    const control = read.Port.init(&payload);
    const budget: api_table_router.RouteBudget = .{ .clock = .{ .admission = control } };
    var follower = [_]@import("../metadata/domain.zig").GroupStatusReport{.{ .group_id = 7, .doc_count = 100, .disk_bytes = 2048 }};
    var leader = [_]@import("../metadata/domain.zig").GroupStatusReport{.{ .group_id = 7, .local_leader = true }};
    var stores = [_]Snapshot.Store{
        .{ .store_id = 1, .node_id = 30, .api_url = "http://follower", .health_class = "healthy", .live = true, .group_statuses = &follower },
        .{ .store_id = 2, .node_id = 31, .api_url = "http://leader", .health_class = "healthy", .live = true, .group_statuses = &leader },
    };
    var placements = [_]Snapshot.Placement{
        .{ .group_id = 7, .node_id = 30, .serving_state = .serving },
        .{ .group_id = 7, .node_id = 31, .serving_state = .serving },
    };
    const snapshot: Snapshot = .{ .stores = &stores, .placements = &placements };
    const first = (try resolve(a, snapshot, null, &.{7}, budget, control)).?;
    defer api_table_router.freeGroupRoutes(a, first);
    try std.testing.expectEqual(@as(u64, 31), first[0].remote.node_id);
    const checks = state.calls;
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.MemoryLimitExceeded }) |cause| for (0..checks) |i| {
        state = .{ .fail_at = i, .cause = cause };
        try std.testing.expectError(cause, resolve(a, snapshot, null, &.{7}, budget, control));
    };
    state = .{};
    placements[1].serving_state = .draining;
    const second = (try resolve(a, snapshot, null, &.{7}, budget, control)).?;
    defer api_table_router.freeGroupRoutes(a, second);
    try std.testing.expectEqual(@as(u64, 30), second[0].remote.node_id);
    placements[0].serving_state = .retiring;
    const third = (try resolve(a, snapshot, null, &.{7}, budget, control)).?;
    defer api_table_router.freeGroupRoutes(a, third);
    try std.testing.expectEqual(@as(u64, 31), third[0].remote.node_id);
    try std.testing.expectEqual(null, try resolve(a, snapshot, 31, &.{7}, budget, control));
    const Harness = struct {
        fn run(allocator: std.mem.Allocator, view: Snapshot, original: read.Port) !void {
            const result = (try resolve(allocator, view, null, &.{7}, .{ .clock = .{ .admission = original } }, original)).?;
            defer api_table_router.freeGroupRoutes(allocator, result);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ snapshot, control });
}
