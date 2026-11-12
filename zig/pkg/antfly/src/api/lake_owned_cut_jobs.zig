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

//! Serving-cut recovery must not hold the serialized publication coordinator.
//! The cleanup owner joins these bounded jobs before freeing the API server.
const std = @import("std");
const local = @import("antfly_local_sources");
const server_api = @import("http_server.zig");
const sync = @import("antfly_platform").sync;
fn ownsRecovery(mode: local.common_config.DeploymentMode, status: @import("../metadata/api.zig").MetadataStatus, node: u64) bool {
    return switch (mode) {
        .standalone, .embedded => true,
        .serverless => false,
        .distributed => node != 0 and status.metadata_raft_local_node_id == node and status.metadata_raft_leader_id == node and std.mem.eql(u8, status.metadata_raft_role, "leader"),
    };
}
pub const Cache = struct {
    const Key = struct { table: u64, incarnation: u64 };
    const Ticket = struct { slot: usize, sequence: u64, key: Key, dirty: bool = false };
    mutex: std.atomic.Mutex = .unlocked,
    active: [8]?Ticket = @splat(null),
    sequence: u64 = 0,
    fn claim(self: *Cache, key: Key) ?Ticket {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var free: ?usize = null;
        for (&self.active, 0..) |*entry, i| {
            if (entry.*) |*ticket| {
                if (std.meta.eql(ticket.key, key)) {
                    ticket.dirty = true;
                    return null;
                }
            } else if (free == null) free = i;
        }
        const slot = free orelse return null;
        self.sequence = std.math.add(u64, self.sequence, 1) catch return null;
        const ticket: Ticket = .{ .slot = slot, .sequence = self.sequence, .key = key };
        self.active[slot] = ticket;
        return ticket;
    }
    fn release(self: *Cache, ticket: Ticket) bool {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.active[ticket.slot]) |current| if (current.sequence == ticket.sequence) {
            self.active[ticket.slot] = null;
            return current.dirty;
        };
        return false;
    }
};
const Work = struct {
    server: *server_api.ApiHttpServer,
    ticket: Cache.Ticket,
    ran: bool = false,
    fn canceled(raw: *const anyopaque) bool {
        return @as(*const server_api.ApiHttpServer, @ptrCast(@alignCast(raw))).index_installation_closing.load(.acquire);
    }
    fn run(raw: *anyopaque) !void {
        const self: *Work = @ptrCast(@alignCast(raw));
        self.ran = true;
        const server = self.server;
        if (canceled(server) or !server.mutationBackgroundExecutionPermitted()) return;
        const request: local.api_operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s, .cancellation = .{ .ptr = server, .is_cancelled_fn = canceled } };
        var snapshot = (if (server.source.vtable.control_snapshot != null) try server.source.controlSnapshot() else try server.source.linearizableSnapshot(request)) orelse return;
        defer server.source.freeAdminSnapshot(&snapshot);
        // A routed snapshot can describe a remote leader. It does not grant
        // this process authority to start background mutation work.
        if (!ownsRecovery(server.cfg.deployment_mode, snapshot.status, server.localSessionNodeId())) return;
        const table = for (snapshot.tables) |table| {
            if (table.table_id == self.ticket.key.table and table.object_storage_generation == self.ticket.key.incarnation) break table;
        } else return;
        // Recovery remains required for authoritative accepted reads. Only its
        // eager background trigger moves; request-side validation is unchanged.
        try @import("lake_index_text_query.zig").refreshOwnedCut(std.heap.smp_allocator, server, table, request);
        try @import("lake_index_text_query.zig").reconcileRecent(std.heap.smp_allocator, server, table, request);
        @import("lake_index_navigation_warming.zig").warmNavigationWithin(std.heap.smp_allocator, server, table, request) catch |err| {
            try request.ensureActive();
            if (err == error.OutOfMemory) return err;
        };
    }
    fn deinit(raw: *anyopaque) void {
        const self: *Work = @ptrCast(@alignCast(raw));
        const server = self.server;
        const key = self.ticket.key;
        // Rejected admission must release ownership without recursively
        // resubmitting into a full queue under a stream of duplicate hints.
        const again = server.lake_owned_cut_jobs.release(self.ticket) and self.ran;
        std.heap.page_allocator.destroy(self);
        if (again) scheduleKey(server, key);
    }
};
pub fn schedule(server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord) void {
    scheduleKey(server, .{ .table = table.table_id, .incarnation = table.object_storage_generation });
}
fn scheduleKey(server: *server_api.ApiHttpServer, key: Cache.Key) void {
    if (server.cfg.node_config == null or server.index_installation_closing.load(.acquire) or !server.mutationBackgroundExecutionPermitted()) return;
    const runtime = server.cfg.backend_runtime orelse return;
    if (runtime.threaded_jobs == null or server.lake_gc_owner_id == 0) return;
    const ticket = server.lake_owned_cut_jobs.claim(key) orelse return;
    const work = std.heap.page_allocator.create(Work) catch {
        _ = server.lake_owned_cut_jobs.release(ticket);
        return;
    };
    work.* = .{ .server = server, .ticket = ticket };
    runtime.durable_jobs.submit(.{ .owner_id = server.lake_gc_owner_id, .class = .cleanup, .ptr = work, .run = Work.run, .deinit = Work.deinit }) catch Work.deinit(work);
}

test "external lake serving recovery admission bounds coalesces incarnations and fences stale completion" {
    var cache: Cache = .{};
    const key: Cache.Key = .{ .table = 1, .incarnation = 2 };
    const first = cache.claim(key).?;
    try std.testing.expect(cache.claim(key) == null);
    var tickets: [7]Cache.Ticket = undefined;
    for (&tickets, 3..) |*ticket, incarnation| ticket.* = cache.claim(.{ .table = 1, .incarnation = incarnation }).?;
    try std.testing.expect(cache.claim(.{ .table = 2, .incarnation = 1 }) == null);
    try std.testing.expect(cache.release(first));
    const replacement = cache.claim(key).?;
    try std.testing.expect(!cache.release(first));
    try std.testing.expect(cache.claim(key) == null);
    try std.testing.expect(cache.release(replacement));
    for (tickets) |ticket| try std.testing.expect(!cache.release(ticket));
    for (cache.active) |ticket| try std.testing.expect(ticket == null);
}

test "external lake serving recovery rejects followers and remote leader snapshots" {
    var status: @import("../metadata/api.zig").MetadataStatus = .{ .metadata_group_id = 1, .metrics = .{}, .metadata_raft_local_node_id = 7, .metadata_raft_leader_id = 7, .metadata_raft_role = "leader" };
    try std.testing.expect(ownsRecovery(.distributed, status, 7));
    try std.testing.expect(!ownsRecovery(.distributed, status, 8));
    try std.testing.expect(!ownsRecovery(.distributed, status, 0));
    status.metadata_raft_role = "follower";
    try std.testing.expect(!ownsRecovery(.distributed, status, 7));
    try std.testing.expect(ownsRecovery(.standalone, status, 0));
    try std.testing.expect(!ownsRecovery(.serverless, status, 7));
}
