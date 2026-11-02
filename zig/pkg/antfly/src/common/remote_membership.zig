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

//! Membership policy for a verified attempt at ONE linearizable admission cut.
//! This does not replace durable generation fencing, resource ownership, or
//! revalidation at resume boundaries. Health/absence never proves quiescence.
const std = @import("std");
const wire = @import("remote_attempt_wire.zig");

pub const Local = struct {
    metadata_incarnation: [32]u8,
    node_id: u64,
    store_id: u64,
    root_incarnation: u128,
    reporter_incarnation: u64,
};
pub const Binding = struct {
    coordinator_node_id: u64,
    worker_node_id: u64,
    store_id: u64,
    root_incarnation: u128,
    reporter_incarnation: u64,
};

/// Stable cluster/node-scoped identity. Process restart uses a newer persisted
/// generation, not a new coordinator ID that could abandon old charges.
pub fn coordinatorIdentity(metadata_incarnation: [32]u8, node_id: u64) !u128 {
    if (node_id == 0 or std.mem.allEqual(u8, &metadata_incarnation, '0')) return error.InvalidRemoteMembership;
    for (metadata_incarnation) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidRemoteMembership;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/remote-coordinator/v1");
    hash.update(&metadata_incarnation);
    var node_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &node_bytes, node_id, .little);
    hash.update(&node_bytes);
    const digest = hash.finalResult();
    const id = std.mem.readInt(u128, digest[0..16], .little);
    if (id == 0) return error.InvalidRemoteMembership;
    return id;
}
fn knownLifecycle(value: []const u8) bool {
    return std.mem.eql(u8, value, "active") or std.mem.eql(u8, value, "draining") or std.mem.eql(u8, value, "finalizing");
}

/// `snapshot` must be the owned result of an authoritative linearizable source,
/// never a cached/observational peer view. The API adapter enforces that call.
pub fn authorize(snapshot: anytype, verified: wire.Verified, local: Local) !Binding {
    try verified.context.validate();
    if (local.node_id == 0 or local.store_id == 0 or local.root_incarnation == 0 or local.reporter_incarnation == 0) return error.InvalidRemoteMembership;
    if (snapshot.peer_view or snapshot.planning_view) return error.RemoteMembershipUnavailable;
    const incarnation = snapshot.status.metadata_incarnation orelse return error.RemoteMembershipUnavailable;
    if (!std.mem.eql(u8, &incarnation, &local.metadata_incarnation)) return error.RemoteMembershipChanged;
    if (verified.context.worker_incarnation != local.root_incarnation) return error.RemoteWorkerIdentityChanged;
    var coordinator_node_id: ?u64 = null;
    var worker_seen = false;
    for (snapshot.nodes) |node| {
        if (try coordinatorIdentity(incarnation, node.node_id) == verified.context.id.coordinator) {
            if (coordinator_node_id != null) return error.InvalidRemoteMembership;
            if (!knownLifecycle(node.lifecycle)) return error.InvalidRemoteMembership;
            if ((verified.context.kind == .read or verified.context.kind == .capabilities) and !std.mem.eql(u8, node.lifecycle, "active")) return error.RemoteNodeDraining;
            coordinator_node_id = node.node_id;
        }
        if (node.node_id == local.node_id) {
            if (worker_seen or !knownLifecycle(node.lifecycle)) return error.InvalidRemoteMembership;
            if ((verified.context.kind == .read or verified.context.kind == .capabilities) and !std.mem.eql(u8, node.lifecycle, "active")) return error.RemoteNodeDraining;
            worker_seen = true;
        }
    }
    const coordinator_node = coordinator_node_id orelse return error.RemoteCoordinatorNotMember;
    if (!worker_seen) return error.RemoteWorkerNotMember;
    var store_seen = false;
    for (snapshot.stores) |store| if (store.store_id == local.store_id) {
        if (store_seen) return error.InvalidRemoteMembership;
        store_seen = true;
        if (store.node_id != local.node_id or store.replica_root_incarnation != local.root_incarnation or store.reporter_incarnation != local.reporter_incarnation) return error.RemoteWorkerIdentityChanged;
        if ((verified.context.kind == .read or verified.context.kind == .capabilities) and (!store.live or store.drain_requested)) return error.RemoteNodeDraining;
    };
    if (!store_seen) return error.RemoteWorkerNotMember;
    return .{ .coordinator_node_id = coordinator_node, .worker_node_id = local.node_id, .store_id = local.store_id, .root_incarnation = local.root_incarnation, .reporter_incarnation = local.reporter_incarnation };
}

const realm = "0123456789abcdef0123456789abcdef".*;
/// Extract only the durably enrolled key for an exact expected root identity.
/// The caller must obtain this snapshot linearly; health and response keys are
/// never substitutes. Draining/unhealthy roots remain valid reconciliation peers.
pub fn replyPublicKey(snapshot: anytype, expected: Local) ![32]u8 {
    _ = try coordinatorIdentity(expected.metadata_incarnation, expected.node_id);
    if (expected.store_id == 0 or expected.root_incarnation == 0 or expected.reporter_incarnation == 0) return error.InvalidRemoteMembership;
    if (snapshot.peer_view or snapshot.planning_view) return error.RemoteMembershipUnavailable;
    const incarnation = snapshot.status.metadata_incarnation orelse return error.RemoteMembershipUnavailable;
    if (!std.mem.eql(u8, &incarnation, &expected.metadata_incarnation)) return error.RemoteMembershipChanged;
    var node_found = false;
    for (snapshot.nodes) |node| if (node.node_id == expected.node_id) {
        if (node_found or !knownLifecycle(node.lifecycle)) return error.InvalidRemoteMembership;
        node_found = true;
    };
    if (!node_found) return error.RemoteMembershipUnavailable;
    var key: ?[32]u8 = null;
    for (snapshot.stores) |store| {
        if (store.store_id != expected.store_id) {
            if (store.replica_root_incarnation == expected.root_incarnation) return error.InvalidRemoteMembership;
            continue;
        }
        if (key != null) return error.InvalidRemoteMembership;
        if (store.node_id != expected.node_id or store.replica_root_incarnation != expected.root_incarnation or store.reporter_incarnation != expected.reporter_incarnation) return error.RemoteWorkerIdentityChanged;
        if (std.mem.allEqual(u8, &store.replica_root_public_key, 0)) return error.RemoteMembershipUnavailable;
        key = store.replica_root_public_key;
    }
    return key orelse error.RemoteMembershipUnavailable;
}

const TestNode = struct { node_id: u64, lifecycle: []const u8 = "active" };
const TestStore = struct { store_id: u64 = 13, node_id: u64 = 9, replica_root_incarnation: u128 = 11, replica_root_public_key: [32]u8 = @splat(0), reporter_incarnation: u64 = 17, live: bool = true, drain_requested: bool = false };
const TestSnapshot = struct {
    status: struct { metadata_incarnation: ?[32]u8 = realm } = .{},
    peer_view: bool = false,
    planning_view: bool = false,
    nodes: []const TestNode,
    stores: []const TestStore,
};
const local_identity: Local = .{ .metadata_incarnation = realm, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
fn context() !wire.Verified {
    return .{ .context = .{
        .kind = .read,
        .id = .{ .coordinator = try coordinatorIdentity(realm, 7), .generation = 1, .operation = 71, .sequence = 1 },
        .worker_incarnation = 11,
        .remaining_ns = 100,
        .charged_bytes = 4096,
        .request_digest = @splat(0),
    }, .local_deadline_ns = 1000 };
}

test "remote attempt membership binds cluster node root and process incarnation" {
    const snapshot = TestSnapshot{ .nodes = &.{ .{ .node_id = 7 }, .{ .node_id = 9 } }, .stores = &.{.{}} };
    const verified = try context();
    const binding = try authorize(snapshot, verified, local_identity);
    try std.testing.expectEqual(@as(u64, 7), binding.coordinator_node_id);
    try std.testing.expectEqual(@as(u64, 9), binding.worker_node_id);
    var wrong = local_identity;
    wrong.reporter_incarnation += 1;
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, authorize(snapshot, verified, wrong));
    wrong = local_identity;
    wrong.root_incarnation += 1;
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, authorize(snapshot, verified, wrong));
    wrong = local_identity;
    wrong.metadata_incarnation[0] = 'f';
    try std.testing.expectError(error.RemoteMembershipChanged, authorize(snapshot, verified, wrong));
    var unknown = verified;
    unknown.context.id.coordinator = 7;
    try std.testing.expectError(error.RemoteCoordinatorNotMember, authorize(snapshot, unknown, local_identity));
    try std.testing.expect(try coordinatorIdentity(realm, 7) != try coordinatorIdentity(realm, 9));
}

test "remote attempt membership permits reconciliation during drain without inferring termination" {
    var nodes = [_]TestNode{ .{ .node_id = 7 }, .{ .node_id = 9 } };
    var stores = [_]TestStore{.{}};
    const snapshot = TestSnapshot{ .nodes = &nodes, .stores = &stores };
    var verified = try context();
    nodes[0].lifecycle = "draining";
    try std.testing.expectError(error.RemoteNodeDraining, authorize(snapshot, verified, local_identity));
    var capability = verified;
    capability.context.kind = .capabilities;
    capability.context.id.operation = 0;
    capability.context.id.sequence = 0;
    capability.context.charged_bytes = 0;
    try std.testing.expectError(error.RemoteNodeDraining, authorize(snapshot, capability, local_identity));
    verified.context.kind = .status;
    verified.context.charged_bytes = 0;
    _ = try authorize(snapshot, verified, local_identity);
    nodes[1].lifecycle = "finalizing";
    stores[0].live = false;
    stores[0].drain_requested = true;
    _ = try authorize(snapshot, verified, local_identity);
    verified.context.kind = .fence;
    verified.context.id.operation = 0;
    verified.context.id.sequence = 0;
    _ = try authorize(snapshot, verified, local_identity);
    // Root replacement is not evidence that an old attempt stopped.
    stores[0].replica_root_incarnation = 12;
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, authorize(snapshot, verified, local_identity));
}

test "remote attempt membership rejects observational and ambiguous authority" {
    var snapshot = TestSnapshot{ .nodes = &.{ .{ .node_id = 7 }, .{ .node_id = 9 } }, .stores = &.{.{}} };
    const verified = try context();
    snapshot.peer_view = true;
    try std.testing.expectError(error.RemoteMembershipUnavailable, authorize(snapshot, verified, local_identity));
    snapshot.peer_view = false;
    snapshot.planning_view = true;
    try std.testing.expectError(error.RemoteMembershipUnavailable, authorize(snapshot, verified, local_identity));
    snapshot.planning_view = false;
    snapshot.nodes = &.{ .{ .node_id = 7 }, .{ .node_id = 7 }, .{ .node_id = 9 } };
    try std.testing.expectError(error.InvalidRemoteMembership, authorize(snapshot, verified, local_identity));
    snapshot.nodes = &.{ .{ .node_id = 7 }, .{ .node_id = 9 } };
    snapshot.stores = &.{ .{}, .{} };
    try std.testing.expectError(error.InvalidRemoteMembership, authorize(snapshot, verified, local_identity));
    snapshot.stores = &.{};
    try std.testing.expectError(error.RemoteWorkerNotMember, authorize(snapshot, verified, local_identity));
}

test "remote attempt membership reply key requires exact enrolled root and authoritative cut" {
    var stores = [_]TestStore{.{ .replica_root_public_key = @splat(7), .live = false, .drain_requested = true }};
    var snapshot: TestSnapshot = .{ .nodes = &.{.{ .node_id = 9, .lifecycle = "draining" }}, .stores = &stores };
    try std.testing.expectEqual(stores[0].replica_root_public_key, try replyPublicKey(snapshot, local_identity));
    snapshot.peer_view = true;
    try std.testing.expectError(error.RemoteMembershipUnavailable, replyPublicKey(snapshot, local_identity));
    snapshot.peer_view = false;
    snapshot.planning_view = true;
    try std.testing.expectError(error.RemoteMembershipUnavailable, replyPublicKey(snapshot, local_identity));
    snapshot.planning_view = false;
    stores[0].replica_root_public_key = @splat(0);
    try std.testing.expectError(error.RemoteMembershipUnavailable, replyPublicKey(snapshot, local_identity));
    stores[0].replica_root_public_key = @splat(7);
    stores[0].replica_root_incarnation += 1;
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, replyPublicKey(snapshot, local_identity));
    stores[0].replica_root_incarnation -= 1;
    stores[0].reporter_incarnation += 1;
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, replyPublicKey(snapshot, local_identity));
    stores[0].reporter_incarnation -= 1;
    snapshot.stores = &.{ stores[0], stores[0] };
    try std.testing.expectError(error.InvalidRemoteMembership, replyPublicKey(snapshot, local_identity));
    snapshot.stores = &stores;
    snapshot.nodes = &.{};
    try std.testing.expectError(error.RemoteMembershipUnavailable, replyPublicKey(snapshot, local_identity));
}
