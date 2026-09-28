// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Content identity of an observational peer view, independently of catalog
//! schemas, planning statistics, and process-local lifecycle counters. This is
//! an index-reuse token, never a quorum, placement or absence proof.
const std = @import("std");

pub fn revision(alloc: std.mem.Allocator, snapshot: anytype) ![32]u8 {
    const Store = struct { store_id: u64, node_id: u64, api_url: []const u8, healthy: bool };
    const Member = struct { group_id: u64, node_id: u64, state: []const u8 };
    const Leader = struct { group_id: u64, store_id: ?u64 };
    const stores = try alloc.alloc(Store, snapshot.stores.len);
    defer alloc.free(stores);
    for (snapshot.stores, stores) |store, *out| out.* = .{ .store_id = store.store_id, .node_id = store.node_id, .api_url = store.api_url, .healthy = store.live and std.mem.eql(u8, store.health_class, "healthy") };
    const members = try alloc.alloc(Member, snapshot.placement_intents.len);
    defer alloc.free(members);
    for (snapshot.placement_intents, members) |intent, *out| out.* = .{ .group_id = intent.record.group_id, .node_id = intent.record.local_node_id, .state = @tagName(intent.serving_state) };
    const leaders = try alloc.alloc(Leader, snapshot.merged_group_statuses.len);
    defer alloc.free(leaders);
    for (snapshot.merged_group_statuses, leaders) |status, *out| out.* = .{ .group_id = status.group_id, .store_id = if (status.leader_known) status.leader_store_id else null };
    // Preserve producer ordering too: ambiguous duplicate records must never
    // reuse an index whose last-record-wins interpretation differs.
    const bytes = try std.json.Stringify.valueAlloc(alloc, .{ .version = @as(u8, 1), .group_id = snapshot.status.metadata_group_id, .incarnation = snapshot.status.metadata_incarnation, .stores = stores, .members = members, .leaders = leaders }, .{});
    defer alloc.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}
