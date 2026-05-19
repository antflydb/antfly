// Copyright 2026 Antfly, Inc.
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

const core = @import("../core/mod.zig");
const group_mod = @import("group.zig");
const multi_raft_mod = @import("multi_raft.zig");
const replica_mod = @import("replica.zig");
const snapshot_transport_iface = @import("snapshot_transport_iface.zig");

pub const HostCommand = union(enum) {
    restore_replicas,
    ensure_replica: replica_mod.ReplicaDescriptor,
    remove_replica: core.types.GroupId,
    add_group: group_mod.GroupConfig,
    remove_group: core.types.GroupId,
    quiesce_group: core.types.GroupId,
    resume_group: core.types.GroupId,
    add_peer: struct {
        group_id: core.types.GroupId,
        peer: @import("transport_iface.zig").PeerDescriptor,
    },
    upsert_peer: struct {
        group_id: core.types.GroupId,
        peer: @import("transport_iface.zig").PeerDescriptor,
    },
    remove_peer: struct {
        group_id: core.types.GroupId,
        node_id: core.types.NodeId,
    },
    campaign_group: core.types.GroupId,
    transfer_leader: struct {
        group_id: core.types.GroupId,
        transferee: core.types.NodeId,
    },
    forget_leader: core.types.GroupId,
    fetch_snapshot: snapshot_transport_iface.SnapshotFetchRequest,
    tick_group: core.types.GroupId,
};

pub fn apply(host: *multi_raft_mod.MultiRaft, command: HostCommand) !void {
    switch (command) {
        .restore_replicas => _ = try host.restoreReplicasFromCatalog(host.alloc),
        .ensure_replica => |desc| _ = try host.ensureReplica(desc),
        .remove_replica => |group_id| try host.removeReplica(group_id),
        .add_group => |cfg| try host.addGroup(cfg),
        .remove_group => |group_id| {
            if (!host.removeGroup(group_id)) return error.UnknownGroup;
        },
        .quiesce_group => |group_id| try host.quiesceGroup(group_id),
        .resume_group => |group_id| try host.resumeGroup(group_id),
        .add_peer => |req| try host.addPeer(req.group_id, req.peer),
        .upsert_peer => |req| try host.upsertPeer(req.group_id, req.peer),
        .remove_peer => |req| try host.removePeer(req.group_id, req.node_id),
        .campaign_group => |group_id| try host.campaignGroup(group_id),
        .transfer_leader => |req| try host.transferLeader(req.group_id, req.transferee),
        .forget_leader => |group_id| try host.forgetLeader(group_id),
        .fetch_snapshot => |req| try host.fetchSnapshot(req),
        .tick_group => |group_id| try host.tickGroup(group_id),
    }
}
