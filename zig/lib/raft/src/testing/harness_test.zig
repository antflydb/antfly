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

const std = @import("std");
const core = @import("../core/mod.zig");
const message = @import("../core/message.zig");
const Cluster = @import("cluster.zig").Cluster;

fn expectNoLeader(cluster: *Cluster) !void {
    for (cluster.peer_ids) |id| {
        try std.testing.expect(cluster.node(id).status().soft.role != core.types.StateRole.leader);
    }
}

fn initPreVoteMigrationCluster() !Cluster {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    errdefer cluster.deinit();

    for (cluster.peer_ids) |id| {
        try cluster.node(id).step(.{
            .msg_type = .heartbeat,
            .from = 99,
            .to = id,
            .term = 1,
            .commit_index = 0,
        });
        try cluster.collectReady(id);
    }

    cluster.node(3).raft.cfg.pre_vote = false;

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.propose(1, "some data");
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.campaign(3);
    try cluster.deliverAll();

    cluster.node(3).raft.cfg.pre_vote = true;
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    cluster.unblock(2, 3);
    cluster.unblock(3, 2);

    return cluster;
}

test "leader election in three node cluster" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try std.testing.expectEqual(@as(usize, 2), cluster.pendingMessages());
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 1), committed.len);
    try std.testing.expectEqual(@as(usize, 0), committed[0].data.len);
}

test "pre-vote campaign elects leader after quorum" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try std.testing.expectEqual(core.types.StateRole.pre_candidate, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(usize, 2), cluster.pendingMessages());
    for (cluster.pendingMessageSlice()) |msg| {
        try std.testing.expectEqual(core.message.MessageType.pre_vote, msg.msg_type);
    }

    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 1), committed.len);
}

test "leader cycle with pre-vote elects each node in turn" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const first = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, first);

    try cluster.campaign(2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    const second = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, second);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    const third = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, third);
}

test "leader cycle without pre-vote elects each node in turn" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const first = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, first);

    try cluster.campaign(2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    const second = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, second);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    const third = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, third);
}

test "forget leader clears follower leader without changing term or election elapsed" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.tick(2, 1);
    const before_elapsed = cluster.node(2).raft.election_elapsed;
    const before_term = cluster.node(2).status().hard.current_term;
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(2).status().soft.leader_id);

    try cluster.forgetLeader(2);

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(2).status().soft.leader_id);
    try std.testing.expectEqual(before_term, cluster.node(2).status().hard.current_term);
    try std.testing.expectEqual(before_elapsed, cluster.node(2).raft.election_elapsed);
}

test "forget leader is ignored for lease-based reads" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.forgetLeader(2);

    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(2).status().soft.leader_id);
}

test "forget leader is a no-op on leader and candidate" {
    {
        var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
            .check_quorum = false,
            .pre_vote = false,
        });
        defer cluster.deinit();

        try cluster.campaign(1);
        try cluster.deliverAll();
        try cluster.forgetLeader(1);

        try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
        try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(1).status().soft.leader_id);
    }

    {
        var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
            .check_quorum = false,
            .pre_vote = false,
        });
        defer cluster.deinit();

        try cluster.campaign(1);
        try cluster.forgetLeader(1);

        try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(1).status().soft.role);
        try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(1).status().soft.leader_id);
    }
}

test "pre-vote with check_quorum does not disrupt followers that still know a leader" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.pre_candidate, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(2).status().soft.leader_id);
}

test "mixed pre-vote migration prevents a stale higher-term peer from winning immediately" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    cluster.setNodePreVote(3, false);
    try cluster.restart(3);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.propose(1, "some data");
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);

    cluster.setNodePreVote(3, true);
    try cluster.restart(3);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    cluster.unblock(2, 3);
    cluster.unblock(3, 2);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.pre_candidate, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 3), cluster.node(2).status().hard.current_term);
    try std.testing.expectEqual(@as(core.types.Term, 3), cluster.node(3).status().hard.current_term);
}

test "mixed pre-vote migration can complete election after the upgraded retry sequence" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    cluster.setNodePreVote(3, false);
    try cluster.restart(3);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.propose(1, "some data");
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);

    cluster.setNodePreVote(3, true);
    try cluster.restart(3);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    cluster.unblock(2, 3);
    cluster.unblock(3, 2);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.campaign(3);
    try cluster.campaign(2);
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.deliverAll();
    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 2), cluster.node(3).status().soft.leader_id);
    try std.testing.expectEqual(@as(core.types.Term, 4), cluster.node(2).status().hard.current_term);
    try std.testing.expectEqual(@as(core.types.Term, 4), cluster.node(3).status().hard.current_term);
}

test "mixed pre-vote migration settles a recovered stale peer back to follower" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    cluster.node(3).raft.cfg.pre_vote = false;

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.propose(1, "some data");
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.campaign(3);
    try cluster.deliverAll();

    cluster.node(3).raft.cfg.pre_vote = true;
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    cluster.unblock(2, 3);
    cluster.unblock(3, 2);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);
    try std.testing.expect(cluster.node(3).status().hard.current_term >= cluster.node(1).status().hard.current_term);
}

test "mixed pre-vote migration heartbeat from an older leader is rejected by the stale peer" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    cluster.node(3).raft.cfg.pre_vote = false;

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.propose(1, "some data");
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.campaign(3);
    try cluster.deliverAll();

    cluster.node(3).raft.cfg.pre_vote = true;
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    cluster.unblock(2, 3);
    cluster.unblock(3, 2);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expect(cluster.node(3).status().soft.role != core.types.StateRole.leader);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expect(cluster.node(3).status().soft.role != core.types.StateRole.leader);

    const leader_term = cluster.node(1).status().hard.current_term;
    try cluster.node(3).step(.{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 3,
        .term = leader_term,
        .commit_index = cluster.node(1).status().hard.commit_index,
    });
    try cluster.collectReady(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(cluster.node(1).status().hard.current_term, cluster.node(3).status().hard.current_term);
}

test "mixed pre-vote migration heartbeat frees stuck higher-term pre-candidate" {
    var cluster = try initPreVoteMigrationCluster();
    defer cluster.deinit();

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expect(cluster.node(3).status().soft.role != core.types.StateRole.leader);

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expect(cluster.node(3).status().soft.role != core.types.StateRole.leader);

    const leader_term = cluster.node(1).status().hard.current_term;
    try cluster.node(3).step(.{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 3,
        .term = leader_term,
        .commit_index = cluster.node(1).status().hard.commit_index,
    });
    try cluster.collectReady(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(cluster.node(1).status().hard.current_term, cluster.node(3).status().hard.current_term);
}

test "leader transfer moves leadership to up-to-date follower" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.transferLeader(1, 2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 2), cluster.node(1).status().soft.leader_id);
}

test "leader transfer aborts if transferee stops being a voter" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.transferLeader(1, 3);

    _ = try cluster.node(1).applyConfChange(.{
        .change_type = .remove_node,
        .node_id = 3,
    });

    try cluster.propose(1, "after-abort");
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 1), committed.len);
    try std.testing.expectEqualStrings("after-abort", committed[0].data);
}

test "leader transfer catches up a slow follower before handing over" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.propose(1, "stale");
    try cluster.deliverAll();

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.transferLeader(1, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
}

test "leader transfer succeeds after lagging follower catches up from compaction gap" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.propose(1, "one");
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.propose(1, "two");
    try cluster.deliverAll();
    try cluster.transferLeader(1, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
}

test "leader transfer times out and leaves leader able to accept proposals" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.transferLeader(1, 3);
    try cluster.deliverAll();
    try cluster.tick(1, 3);

    try cluster.propose(1, "after-timeout");
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 1), committed.len);
    try std.testing.expectEqualStrings("after-timeout", committed[0].data);
}

test "leader transfer request from follower is forwarded to leader" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.transferLeader(2, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
}

test "second leader transfer request replaces pending transferee" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.transferLeader(1, 3);
    try cluster.transferLeader(1, 2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
}

test "second transfer request to same transferee does not extend timeout" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.transferLeader(1, 3);
    try cluster.tick(1, 1);
    try cluster.transferLeader(1, 3);
    try cluster.tick(1, 2);

    try cluster.propose(1, "after-same-timeout");
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 1), committed.len);
    try std.testing.expectEqualStrings("after-same-timeout", committed[0].data);
}

test "higher-term election aborts pending leader transfer" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.transferLeader(1, 3);
    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
}

test "check_quorum leader steps down and replacement leader can be elected" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.tick(1, 8);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(1).status().soft.leader_id);
    try expectNoLeader(&cluster);

    try cluster.block(2, 3);
    try cluster.block(3, 2);
    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "check_quorum higher-term stuck candidate disrupts leader lease on recovery" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 1), cluster.node(1).status().hard.current_term);
    try std.testing.expectEqual(@as(core.types.Term, 2), cluster.node(3).status().hard.current_term);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(1, 1);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 2), cluster.node(1).status().hard.current_term);
    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);
}

test "check_quorum repeated higher-term campaigns still disrupt leader lease on recovery" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 1), cluster.node(1).status().hard.current_term);
    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 3), cluster.node(3).status().hard.current_term);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(1, 1);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(core.types.Term, 3), cluster.node(1).status().hard.current_term);
}

test "non-promotable node with check_quorum stays follower and learns the leader" {
    var storage_a = core.MemoryStorage.init(std.testing.allocator);
    defer storage_a.deinit();
    var storage_b = core.MemoryStorage.init(std.testing.allocator);
    defer storage_b.deinit();

    var voters_a = [_]core.types.NodeId{1};
    try storage_a.seedConfState(.{
        .voters = voters_a[0..],
    });

    var voters_b = [_]core.types.NodeId{1};
    try storage_b.seedConfState(.{
        .voters = voters_b[0..],
    });

    var node_a = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    }, storage_a.storage());
    defer node_a.deinit();

    var node_b = try core.RawNode.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    }, storage_b.storage());
    defer node_b.deinit();

    for (0..3) |_| node_b.tick();
    try std.testing.expectEqual(core.types.StateRole.follower, node_b.status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), node_b.status().soft.leader_id);

    try node_a.campaign();
    try std.testing.expectEqual(core.types.StateRole.leader, node_a.status().soft.role);
    try node_b.step(.{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = node_a.status().hard.current_term,
        .commit_index = node_a.status().hard.commit_index,
    });

    try std.testing.expectEqual(core.types.StateRole.follower, node_b.status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), node_b.status().soft.leader_id);
}

test "check_quorum follower campaign does not supersede leader while peer leases remain active" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .election_tick = 5,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.tick(2, 4);
    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(3).status().soft.role);
}

test "leader transfer still works while check_quorum lease is active" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.transferLeader(1, 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    const first_transfer = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, first_transfer);

    try cluster.propose(2, "during-check-quorum");
    try cluster.deliverAll();

    const after_propose = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, after_propose);
    try std.testing.expect(after_propose.len >= 1);
    try std.testing.expectEqualStrings("during-check-quorum", after_propose[after_propose.len - 1].data);

    try cluster.transferLeader(2, 1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
}

test "leader transfer to self is a no-op" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.transferLeader(1, 1);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(1).status().soft.leader_id);
}

test "non-member ignores timeout_now and vote responses" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]core.types.NodeId{ 2, 3, 4 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 5,
        .heartbeat_tick = 1,
    }, storage.storage());
    defer node.deinit();

    try node.step(.{
        .msg_type = .timeout_now,
        .from = 2,
        .to = 1,
    });
    try node.step(.{
        .msg_type = .request_vote_response,
        .from = 2,
        .to = 1,
        .term = 1,
    });
    try node.step(.{
        .msg_type = .request_vote_response,
        .from = 3,
        .to = 1,
        .term = 1,
    });

    const status = node.status();
    try std.testing.expectEqual(core.types.StateRole.follower, status.soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), status.soft.leader_id);
}

test "learner does not campaign or react to timeout_now" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    try storage.seedConfState(.{
        .voters = voters[0..],
        .learners = learners[0..],
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = true,
    }, storage.storage());
    defer node.deinit();

    try std.testing.expectError(error.NotPromotable, node.campaign());
    try node.step(.{
        .msg_type = .timeout_now,
        .from = 1,
        .to = 2,
        .term = 1,
    });

    const status = node.status();
    try std.testing.expectEqual(core.types.StateRole.follower, status.soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), status.soft.leader_id);
}

test "learner does not start an election on timeout" {
    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    const initial_conf_state = core.types.ConfState{
        .voters = voters[0..],
        .learners = learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.tick(2, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(2).status().soft.leader_id);
}

test "learner with pre-vote does not start an election on timeout" {
    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    const initial_conf_state = core.types.ConfState{
        .voters = voters[0..],
        .learners = learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.tick(2, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(2).status().soft.leader_id);
}

test "promoted learner can campaign and become leader" {
    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    const initial_conf_state = core.types.ConfState{
        .voters = voters[0..],
        .learners = learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    var promote = core.types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
            .{
                .change_type = .add_node,
                .node_id = 2,
            },
        }),
    };
    defer promote.deinit(std.testing.allocator);
    try cluster.proposeConfChangeV2(1, promote);
    try cluster.deliverAll();
    const promotion = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, promotion);

    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "promoted learner can win election by timeout" {
    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    const initial_conf_state = core.types.ConfState{
        .voters = voters[0..],
        .learners = learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    var promote = core.types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
            .{
                .change_type = .add_node,
                .node_id = 2,
            },
        }),
    };
    defer promote.deinit(std.testing.allocator);
    try cluster.proposeConfChangeV2(1, promote);
    try cluster.deliverAll();
    const promotion = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, promotion);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.tick(2, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.candidate, cluster.node(2).status().soft.role);

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    try cluster.tick(2, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "learner replicates leader proposals" {
    var voters = [_]core.types.NodeId{1};
    var learners = [_]core.types.NodeId{2};
    const initial_conf_state = core.types.ConfState{
        .voters = voters[0..],
        .learners = learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    const initial_leader = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial_leader);
    const initial_learner = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, initial_learner);

    try cluster.propose(1, "learner-data");
    try cluster.deliverAll();

    const leader_committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, leader_committed);
    const learner_committed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, learner_committed);

    try std.testing.expectEqual(@as(usize, 1), leader_committed.len);
    try std.testing.expectEqual(@as(usize, 1), learner_committed.len);
    try std.testing.expectEqualStrings("learner-data", leader_committed[0].data);
    try std.testing.expectEqualStrings("learner-data", learner_committed[0].data);
}

test "heartbeat suppresses follower election" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);

    try cluster.tick(1, 1);
    try cluster.deliverAll();

    try cluster.tick(2, 2);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);
}

test "proposal replicates and commits after quorum" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);

    try cluster.block(1, 3);
    try cluster.propose(1, "hello");
    try std.testing.expectEqual(@as(usize, 2), cluster.pendingMessages());

    try cluster.deliverAll();
    const committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, committed);

    try std.testing.expect(committed.len > 0);
    try std.testing.expectEqualStrings("hello", committed[0].data);
}

test "append rejection reports hint" {
    var follower_store = core.MemoryStorage.init(std.testing.allocator);
    defer follower_store.deinit();
    try follower_store.append(&.{
        .{ .index = 1, .term = 1 },
    });

    var follower = try core.RawNode.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
    }, follower_store.storage());
    defer follower.deinit();

    try follower.step(.{
        .msg_type = .append_entries,
        .from = 1,
        .to = 2,
        .term = 2,
        .log_index = 2,
        .log_term = 2,
        .entries = &.{},
    });

    const rd = follower.ready();
    defer follower.advance(rd);
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expect(rd.messages[0].reject);
    try std.testing.expectEqual(@as(core.types.Index, 1), rd.messages[0].reject_hint);
}

test "blocked link prevents delivery until unblocked" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.block(1, 2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(2).status().soft.role);

    cluster.clearBlocks();
    try cluster.tick(1, 1);
    try cluster.deliverAll();

    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(2).status().soft.leader_id);
}

test "split vote retries after partition heals" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3, 4 });
    defer cluster.deinit();

    const left = [_]core.types.NodeId{ 1, 2 };
    const right = [_]core.types.NodeId{ 3, 4 };
    for (left) |from| {
        for (right) |to| {
            try cluster.block(from, to);
            try cluster.block(to, from);
        }
    }

    try cluster.tick(1, 3);
    try cluster.tick(3, 3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.pre_candidate, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.pre_candidate, cluster.node(3).status().soft.role);
    try expectNoLeader(&cluster);

    cluster.clearBlocks();
    try cluster.campaign(1);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(3).status().soft.leader_id);
}

test "cluster election random seed deterministically derives node timeouts" {
    var cluster_a = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .random_seed = 7,
    });
    defer cluster_a.deinit();

    var cluster_b = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .random_seed = 7,
    });
    defer cluster_b.deinit();

    try std.testing.expectEqual(cluster_a.node(1).raft.randomized_election_timeout, cluster_b.node(1).raft.randomized_election_timeout);
    try std.testing.expectEqual(cluster_a.node(2).raft.randomized_election_timeout, cluster_b.node(2).raft.randomized_election_timeout);
    try std.testing.expect(cluster_a.node(1).raft.randomized_election_timeout >= cluster_a.election_tick);
    try std.testing.expect(cluster_a.node(1).raft.randomized_election_timeout < cluster_a.election_tick * 2);
}

test "follower catches up after missing replication" {
    var cluster = try Cluster.init(std.testing.allocator, &.{ 1, 2, 3 });
    defer cluster.deinit();

    try cluster.tick(1, 3);
    try cluster.deliverAll();
    const initial_leader = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial_leader);
    try std.testing.expectEqual(@as(usize, 1), initial_leader.len);
    const initial_follower_two = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, initial_follower_two);
    try std.testing.expectEqual(@as(usize, 1), initial_follower_two.len);
    const initial_follower_three = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, initial_follower_three);
    try std.testing.expectEqual(@as(usize, 1), initial_follower_three.len);

    try cluster.block(1, 3);
    try cluster.propose(1, "one");
    try cluster.deliverAll();

    const leader_first = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, leader_first);
    try std.testing.expectEqual(@as(usize, 1), leader_first.len);
    try std.testing.expectEqualStrings("one", leader_first[0].data);

    const follower_missed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_missed);
    try std.testing.expectEqual(@as(usize, 0), follower_missed.len);

    cluster.unblock(1, 3);
    try cluster.propose(1, "two");
    try cluster.deliverAll();
    try cluster.tick(1, 1);
    try cluster.deliverAll();

    const follower_caught_up = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_caught_up);
    try std.testing.expectEqual(@as(usize, 2), follower_caught_up.len);
    try std.testing.expectEqualStrings("one", follower_caught_up[0].data);
    try std.testing.expectEqualStrings("two", follower_caught_up[1].data);
}

test "restart from persisted state replays committed entries for apply" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try storage.append(&.{
        .{ .index = 1, .term = 2 },
        .{ .index = 2, .term = 2 },
    });
    storage.setHardState(.{
        .current_term = 2,
        .voted_for = 1,
        .commit_index = 2,
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
    }, storage.storage());
    defer node.deinit();

    const status = node.status();
    try std.testing.expectEqual(@as(core.types.Term, 2), status.hard.current_term);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), status.hard.voted_for);
    try std.testing.expectEqual(@as(core.types.Index, 2), status.hard.commit_index);
    try std.testing.expect(node.hasReady());

    const rd = node.ready();
    defer node.advance(rd);
    try std.testing.expectEqual(@as(?core.HardState, null), rd.hard_state);
    try std.testing.expectEqual(@as(usize, 0), rd.entries.len);
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), rd.committed_entries[0].index);
    try std.testing.expectEqual(@as(core.types.Index, 2), rd.committed_entries[1].index);
}

test "raw node restart from snapshot replays committed entries after snapshot index" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var conf_voters = [_]core.types.NodeId{ 1, 2 };
    try storage.applySnapshot(.{
        .metadata = .{
            .index = 2,
            .term = 1,
            .conf_state = .{
                .voters = conf_voters[0..],
            },
        },
        .data = &.{},
    });
    try storage.append(&.{
        .{ .index = 3, .term = 1, .data = @constCast("foo"[0..]) },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 3,
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
    }, storage.storage());
    defer node.deinit();

    try std.testing.expect(node.hasReady());
    const rd = node.ready();
    try std.testing.expectEqual(@as(?core.HardState, null), rd.hard_state);
    try std.testing.expect(rd.snapshot == null);
    try std.testing.expectEqual(@as(usize, 0), rd.entries.len);
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 3), rd.committed_entries[0].index);
    try std.testing.expectEqualStrings("foo", rd.committed_entries[0].data);
    node.advance(rd);
    try std.testing.expect(!node.hasReady());
}

test "raw node restart from compacted storage replays only post-snapshot committed entries" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var conf_voters = [_]core.types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = conf_voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("one"[0..]) },
        .{ .index = 2, .term = 1, .data = @constCast("two"[0..]) },
        .{ .index = 3, .term = 2, .data = @constCast("three"[0..]) },
        .{ .index = 4, .term = 2, .data = @constCast("four"[0..]) },
    });
    try storage.compactTo(2, .{
        .voters = conf_voters[0..],
    });
    storage.setHardState(.{
        .current_term = 2,
        .commit_index = 4,
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
    }, storage.storage());
    defer node.deinit();

    try std.testing.expect(node.hasReady());
    const rd = node.ready();
    defer node.advance(rd);
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 3), rd.committed_entries[0].index);
    try std.testing.expectEqual(@as(core.types.Index, 4), rd.committed_entries[1].index);
    try std.testing.expectEqualStrings("three", rd.committed_entries[0].data);
    try std.testing.expectEqualStrings("four", rd.committed_entries[1].data);
}

test "raw node restart from compacted storage replays a contiguous committed suffix without gaps" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var conf_voters = [_]core.types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = conf_voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("one"[0..]) },
        .{ .index = 2, .term = 1, .data = @constCast("two"[0..]) },
        .{ .index = 3, .term = 2, .data = @constCast("three"[0..]) },
        .{ .index = 4, .term = 2, .data = @constCast("four"[0..]) },
        .{ .index = 5, .term = 2, .data = @constCast("five"[0..]) },
        .{ .index = 6, .term = 2, .data = @constCast("six"[0..]) },
        .{ .index = 7, .term = 2, .data = @constCast("seven"[0..]) },
    });
    try storage.compactTo(3, .{
        .voters = conf_voters[0..],
    });
    storage.setHardState(.{
        .current_term = 2,
        .commit_index = 7,
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
    }, storage.storage());
    defer node.deinit();

    try std.testing.expect(node.hasReady());
    const rd = node.ready();
    try std.testing.expectEqual(@as(usize, 4), rd.committed_entries.len);
    for (rd.committed_entries, 0..) |entry, i| {
        const expected_index: core.types.Index = @intCast(i + 4);
        try std.testing.expectEqual(expected_index, entry.index);
    }
    try std.testing.expectEqualStrings("four", rd.committed_entries[0].data);
    try std.testing.expectEqualStrings("seven", rd.committed_entries[3].data);

    node.advance(rd);
    try std.testing.expect(!node.hasReady());
}

test "raw node restart applies the next newly committed entry without skipping" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]core.types.NodeId{1};
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("a"[0..]) },
        .{ .index = 2, .term = 1, .data = @constCast("b"[0..]) },
        .{ .index = 3, .term = 1, .data = @constCast("c"[0..]) },
        .{ .index = 4, .term = 1, .data = @constCast("d"[0..]) },
        .{ .index = 5, .term = 1, .data = @constCast("e"[0..]) },
    });
    storage.setHardState(.{
        .current_term = 1,
        .voted_for = 1,
        .commit_index = 4,
    });

    var node = try core.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .pre_vote = false,
    }, storage.storage());
    defer node.deinit();

    try std.testing.expect(node.hasReady());
    var rd = node.ready();
    try std.testing.expectEqual(@as(usize, 4), rd.committed_entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), rd.committed_entries[0].index);
    try std.testing.expectEqual(@as(core.types.Index, 4), rd.committed_entries[3].index);
    node.advance(rd);

    try std.testing.expect(!node.hasReady());
    try node.step(.{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = 1,
        .commit_index = 5,
    });

    try std.testing.expect(node.hasReady());
    rd = node.ready();
    defer node.advance(rd);
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 5), rd.committed_entries[0].index);
    try std.testing.expectEqualStrings("e", rd.committed_entries[0].data);
}

test "cluster restart replays persisted committed entries for the restarted node" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.propose(1, "one");
    try cluster.deliverAll();

    const before_restart = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, before_restart);
    try std.testing.expectEqual(@as(usize, 2), before_restart.len);

    try cluster.restart(2);

    const after_restart = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, after_restart);
    try std.testing.expectEqual(@as(usize, 2), after_restart.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), after_restart[0].index);
    try std.testing.expectEqual(@as(core.types.Index, 2), after_restart[1].index);
}

test "cluster restart with applied replays only unapplied committed suffix" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.propose(1, "one");
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    const before_restart = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, before_restart);

    try cluster.restartWithApplied(2, 1);

    const replayed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, replayed);
    try std.testing.expectEqual(@as(usize, 2), replayed.len);
    try std.testing.expectEqual(@as(core.types.Index, 2), replayed[0].index);
    try std.testing.expectEqualStrings("one", replayed[0].data);
    try std.testing.expectEqual(@as(core.types.Index, 3), replayed[1].index);
    try std.testing.expectEqualStrings("two", replayed[1].data);
}

test "check_quorum restart with applied preserves only unapplied committed suffix" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.propose(1, "one");
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    const before_restart = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, before_restart);

    try cluster.restartWithApplied(2, 1);

    const replayed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, replayed);
    try std.testing.expectEqual(@as(usize, 2), replayed.len);
    try std.testing.expectEqual(@as(core.types.Index, 2), replayed[0].index);
    try std.testing.expectEqualStrings("one", replayed[0].data);
    try std.testing.expectEqual(@as(core.types.Index, 3), replayed[1].index);
    try std.testing.expectEqualStrings("two", replayed[1].data);
}

test "async_storage_writes restart with applied replays only unapplied committed suffix" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .async_storage_writes = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.propose(1, "one");
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    const before_restart = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, before_restart);

    try cluster.restartWithApplied(2, 1);

    const replayed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, replayed);
    try std.testing.expectEqual(@as(usize, 2), replayed.len);
    try std.testing.expectEqual(@as(core.types.Index, 2), replayed[0].index);
    try std.testing.expectEqualStrings("one", replayed[0].data);
    try std.testing.expectEqual(@as(core.types.Index, 3), replayed[1].index);
    try std.testing.expectEqualStrings("two", replayed[1].data);
}

test "lease-based read requires check_quorum" {
    try std.testing.expectError(error.LeaseBasedReadRequiresCheckQuorum, Cluster.initWithOptions(
        std.testing.allocator,
        &.{ 1, 2, 3 },
        .{
            .check_quorum = false,
            .pre_vote = false,
            .read_only_option = .lease_based,
        },
    ));
}

test "multi-node read index surfaces a read state after quorum heartbeats" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);

    try cluster.readIndex(1, "ctx-one");

    const before_quorum = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, before_quorum);
    try std.testing.expectEqual(@as(usize, 0), before_quorum.len);

    try std.testing.expectEqual(@as(usize, 2), cluster.pendingMessages());
    try cluster.deliverAll();

    const read_states = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), read_states[0].index);
    try std.testing.expectEqualStrings("ctx-one", read_states[0].request_ctx);
}

test "lease-based leader read index returns immediately without heartbeat round" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.readIndex(1, "lease-local");
    try std.testing.expectEqual(@as(usize, 0), cluster.pendingMessages());

    const read_states = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), read_states[0].index);
    try std.testing.expectEqualStrings("lease-local", read_states[0].request_ctx);
}

test "multi-node read index preserves multiple pending contexts" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.readIndex(1, "ctx-one");
    try cluster.readIndex(1, "ctx-two");

    const before_quorum = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, before_quorum);
    try std.testing.expectEqual(@as(usize, 0), before_quorum.len);

    try cluster.deliverAll();

    const read_states = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 2), read_states.len);
    try std.testing.expectEqualStrings("ctx-one", read_states[0].request_ctx);
    try std.testing.expectEqualStrings("ctx-two", read_states[1].request_ctx);
}

test "follower read index forwards to leader and surfaces local read state" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.readIndex(2, "follower-ctx");

    const before = try cluster.collectReadStates(2);
    defer freeReadStates(std.testing.allocator, before);
    try std.testing.expectEqual(@as(usize, 0), before.len);

    try cluster.deliverAll();

    const read_states = try cluster.collectReadStates(2);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), read_states[0].index);
    try std.testing.expectEqualStrings("follower-ctx", read_states[0].request_ctx);
}

test "lease-based follower read index uses direct leader response" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.readIndex(2, "lease-follower");

    const before = try cluster.collectReadStates(2);
    defer freeReadStates(std.testing.allocator, before);
    try std.testing.expectEqual(@as(usize, 0), before.len);

    try std.testing.expectEqual(@as(usize, 1), cluster.pendingMessages());
    try std.testing.expectEqual(core.message.MessageType.read_index, cluster.pendingMessageSlice()[0].msg_type);

    try cluster.deliverNext();
    try std.testing.expectEqual(@as(usize, 1), cluster.pendingMessages());
    try std.testing.expectEqual(core.message.MessageType.read_index_response, cluster.pendingMessageSlice()[0].msg_type);

    try cluster.deliverNext();
    try std.testing.expectEqual(@as(usize, 0), cluster.pendingMessages());

    const read_states = try cluster.collectReadStates(2);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqual(@as(core.types.Index, 1), read_states[0].index);
    try std.testing.expectEqualStrings("lease-follower", read_states[0].request_ctx);
}

test "lease-based leader read fails after lease expiry and replacement election" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.readIndex(1, "lease-before-expiry");
    const before_expiry = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, before_expiry);
    try std.testing.expectEqual(@as(usize, 1), before_expiry.len);

    try cluster.tick(1, cluster.election_tick * 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), cluster.node(1).status().soft.leader_id);
    try std.testing.expectError(error.NotLeader, cluster.readIndex(1, "lease-after-expiry"));

    try cluster.tick(2, cluster.election_tick * 2);
    try cluster.tick(3, cluster.election_tick * 2);
    try cluster.deliverAll();
    try cluster.campaign(2);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try cluster.readIndex(2, "lease-after-replacement");
    const after_replacement = try cluster.collectReadStates(2);
    defer freeReadStates(std.testing.allocator, after_replacement);
    try std.testing.expectEqual(@as(usize, 1), after_replacement.len);
    try std.testing.expectEqualStrings("lease-after-replacement", after_replacement[0].request_ctx);
}

test "follower proposal forwards to leader and commits" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.propose(2, "follower-proposal");
    try std.testing.expectEqual(@as(usize, 1), cluster.pendingMessages());
    try std.testing.expectEqual(core.message.MessageType.propose, cluster.pendingMessageSlice()[0].msg_type);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), cluster.pendingMessageSlice()[0].from);
    try std.testing.expectEqual(@as(core.types.NodeId, 1), cluster.pendingMessageSlice()[0].to);

    try cluster.deliverAll();

    const committed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expect(committed.len >= 1);
    try std.testing.expectEqualStrings("follower-proposal", committed[committed.len - 1].data);
}

test "disable proposal forwarding drops follower proposal" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .disable_proposal_forwarding = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);
    const follower_initial = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, follower_initial);

    try std.testing.expectError(error.ProposalDropped, cluster.propose(2, "dropped-follower-proposal"));
    try std.testing.expectEqual(@as(usize, 0), cluster.pendingMessages());
    try cluster.deliverAll();

    const committed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expectEqual(@as(usize, 0), committed.len);
}

test "follower conf-change proposal forwards to leader and activates learner" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    var add_learner = core.types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
            .{
                .change_type = .add_learner_node,
                .node_id = 4,
            },
        }),
    };
    defer add_learner.deinit(std.testing.allocator);

    try cluster.proposeConfChangeV2(2, add_learner);
    try std.testing.expectEqual(@as(usize, 1), cluster.pendingMessages());
    try std.testing.expectEqual(core.message.MessageType.propose, cluster.pendingMessageSlice()[0].msg_type);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), cluster.pendingMessageSlice()[0].from);
    try std.testing.expectEqual(@as(core.types.NodeId, 1), cluster.pendingMessageSlice()[0].to);

    try cluster.deliverAll();

    try std.testing.expect(cluster.isNodeActive(4));
    const conf_state = cluster.node(1).status().conf_state;
    try std.testing.expectEqual(@as(usize, 1), conf_state.learners.len);
    try std.testing.expectEqual(@as(core.types.NodeId, 4), conf_state.learners[0]);

    try cluster.propose(1, "after-forwarded-conf-change");
    try cluster.deliverAll();

    const learner_committed = try cluster.collectCommitted(4);
    defer core.types.freeEntries(std.testing.allocator, learner_committed);
    try std.testing.expect(learner_committed.len >= 1);
    try std.testing.expectEqualStrings(
        "after-forwarded-conf-change",
        learner_committed[learner_committed.len - 1].data,
    );
}

test "disable proposal forwarding drops follower conf change" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .initial_conf_state = initial_conf_state,
        .check_quorum = false,
        .pre_vote = false,
        .disable_proposal_forwarding = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    var add_learner = core.types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
            .{
                .change_type = .add_learner_node,
                .node_id = 4,
            },
        }),
    };
    defer add_learner.deinit(std.testing.allocator);

    try std.testing.expectError(error.ProposalDropped, cluster.proposeConfChangeV2(2, add_learner));
    try std.testing.expectEqual(@as(usize, 0), cluster.pendingMessages());
    try cluster.deliverAll();
    try std.testing.expect(!cluster.isNodeActive(4));
}

test "pending read is cleared after leader stepdown and reelection" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.readIndex(1, "stale-before-reelection");
    try cluster.deliverAll();

    cluster.unblock(2, 3);
    cluster.unblock(3, 2);
    try cluster.campaign(2);
    try cluster.deliverAll();

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(2, 1);
    try cluster.deliverAll();

    try cluster.block(2, 1);
    try cluster.block(1, 2);
    try cluster.block(2, 3);
    try cluster.block(3, 2);
    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.readIndex(1, "fresh-after-reelection");
    try cluster.deliverAll();

    const read_states = try cluster.collectReadStates(1);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqualStrings("fresh-after-reelection", read_states[0].request_ctx);
}

test "physical node stays inactive until added as learner and then catches up" {
    var initial_voters = [_]core.types.NodeId{1};
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 2,
    }});
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.pendingMessages());

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const learner_initial = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, learner_initial);
    try std.testing.expectEqual(@as(usize, 3), learner_initial.len);

    try cluster.propose(1, "learner-data");
    try cluster.deliverAll();

    const learner_followup = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, learner_followup);
    try std.testing.expectEqual(@as(usize, 1), learner_followup.len);
    try std.testing.expectEqualStrings("learner-data", learner_followup[0].data);
}

test "slow follower catches up from compaction gap after leader restart" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.propose(1, "one");
    try cluster.deliverAll();

    try cluster.compact(1, 2);
    try cluster.restart(1);
    try cluster.campaign(1);
    try cluster.deliverAll();
    const post_restart = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, post_restart);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "after-restore");
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), cluster.node(3).status().soft.leader_id);
    const follower_committed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_committed);
    try std.testing.expect(follower_committed.len >= 1);
    try std.testing.expectEqualStrings("after-restore", follower_committed[follower_committed.len - 1].data);
    try std.testing.expectEqual(cluster.node(1).status().hard.commit_index, cluster.node(3).status().hard.commit_index);
}

test "lagging follower retries snapshot after rejection and catches up" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.propose(1, "one");
    try cluster.deliverAll();

    try cluster.compact(1, 2);
    try cluster.restart(1);
    try cluster.campaign(1);
    try cluster.deliverAll();
    const post_restart = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, post_restart);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(1, 1);
    try cluster.rejectSnapshot(1, 3);
    try cluster.deliverAll();
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    const follower_committed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_committed);
    try std.testing.expect(follower_committed.len >= 1);
    try std.testing.expectEqualStrings("two", follower_committed[follower_committed.len - 1].data);
    try std.testing.expectEqual(cluster.node(1).status().hard.commit_index, cluster.node(3).status().hard.commit_index);
}

test "lagging follower resumes catch-up after snapshot abort" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.propose(1, "one");
    try cluster.deliverAll();

    try cluster.compact(1, 2);
    try cluster.restart(1);
    try cluster.campaign(1);
    try cluster.deliverAll();
    const post_restart = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, post_restart);

    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.tick(1, 1);
    try cluster.abortSnapshot(1, 3, 2);
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    const follower_committed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_committed);
    try std.testing.expect(follower_committed.len >= 1);
    try std.testing.expectEqualStrings("two", follower_committed[follower_committed.len - 1].data);
    try std.testing.expectEqual(cluster.node(1).status().hard.commit_index, cluster.node(3).status().hard.commit_index);
}

test "lagging follower recovers by snapshot after leadership transfer churn" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.propose(1, "one");
    try cluster.deliverAll();
    try cluster.propose(1, "two");
    try cluster.deliverAll();

    try cluster.compact(1, 2);
    try cluster.compact(2, 2);

    try cluster.transferLeader(1, 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(2).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    try cluster.propose(2, "after-churn");
    try cluster.deliverAll();

    const follower_committed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, follower_committed);
    try std.testing.expect(follower_committed.len >= 1);
    try std.testing.expectEqualStrings("after-churn", follower_committed[follower_committed.len - 1].data);
    try std.testing.expectEqual(cluster.node(2).status().hard.commit_index, cluster.node(3).status().hard.commit_index);
    try std.testing.expectEqual(@as(?core.types.NodeId, 2), cluster.node(3).status().soft.leader_id);
}

test "single-node joint implicit conf change auto-leaves on next ready" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{1}, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 2,
    }});
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqual(@as(usize, 1), joint_status.conf_state.voters.len);
    try std.testing.expectEqual(@as(usize, 1), joint_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqual(@as(usize, 1), joint_status.conf_state.learners.len);
    try std.testing.expect(joint_status.conf_state.auto_leave);

    try cluster.collectReady(1);

    const final_status = cluster.node(1).status();
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqual(@as(usize, 1), final_status.conf_state.learners.len);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), final_status.conf_state.learners[0]);
    try std.testing.expect(!final_status.conf_state.auto_leave);

    try std.testing.expectEqual(@as(usize, 1), cluster.pendingMessages());
    const pending = cluster.pendingMessageSlice();
    try std.testing.expectEqual(core.message.MessageType.append_entries, pending[0].msg_type);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), pending[0].to);
    try std.testing.expectEqual(@as(core.types.Index, 1), pending[0].log_index);
    try std.testing.expectEqual(@as(core.types.Index, 2), pending[0].commit_index);
    try std.testing.expectEqual(@as(usize, 1), pending[0].entries.len);
    try std.testing.expectEqual(@as(core.types.Index, 2), pending[0].entries[0].index);
}

test "joint multi-add explicit leave promotes new voters" {
    var initial_voters = [_]core.types.NodeId{1};
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{1}, joint_status.conf_state.voters_outgoing);

    try cluster.deliverAll();
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
}

test "incoming voter can win election while joint config is active" {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, joint_status.conf_state.voters_outgoing);

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "incoming voter can win election while joint config is active with pre_vote" {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "incoming voter can replace leader during joint config after check_quorum lease expiry" {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.tick(1, cluster.election_tick * 2);
    try cluster.tick(2, cluster.election_tick * 2);
    try cluster.tick(3, cluster.election_tick * 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "pre-vote incoming voter can replace leader during joint config after check_quorum lease expiry" {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.tick(1, cluster.election_tick * 2);
    try cluster.tick(2, cluster.election_tick * 2);
    try cluster.tick(3, cluster.election_tick * 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "restart replays committed joint config state" {
    var initial_voters = [_]core.types.NodeId{1};
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.restart(1);

    const restarted_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, restarted_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{1}, restarted_status.conf_state.voters_outgoing);
}

test "restart from compacted joint config preserves snapshot conf state" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);
    try cluster.restart(1);

    const restarted_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 3, 4 }, restarted_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, restarted_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{5}, restarted_status.conf_state.learners);
}

test "check_quorum restart from compacted joint config preserves snapshot conf state and can make progress" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);
    try cluster.restart(1);

    var restarted_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 3, 4 }, restarted_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, restarted_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{5}, restarted_status.conf_state.learners);

    try cluster.campaign(3);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);

    try cluster.node(3).proposeConfChangeV2(.{});
    while (cluster.node(3).hasReady()) {
        try cluster.collectReady(3);
    }
    try cluster.deliverAll();
    while (cluster.node(3).hasReady()) {
        try cluster.collectReady(3);
    }
    restarted_status = cluster.node(1).status();
    try std.testing.expectEqual(@as(usize, 0), restarted_status.conf_state.voters_outgoing.len);

    try cluster.propose(3, "after-check-quorum-joint-restart");
    try cluster.deliverAll();

    const committed = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expect(committed.len >= 1);
    try std.testing.expectEqualStrings("after-check-quorum-joint-restart", committed[committed.len - 1].data);
}

test "restart replays remove-then-readd joint config proposal" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const first_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(first_changes);

    const second_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(second_changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    try cluster.deliverAll();
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.deliverAll();
    cluster.clearBlocks();
    try cluster.compact(1, 3);
    try cluster.restart(1);
    try cluster.campaign(1);
    try cluster.deliverAll();
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = second_changes,
    });
    try cluster.deliverAll();

    const status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, status.conf_state.voters_outgoing);
}

test "check_quorum incoming voter restores from joint snapshot and catches up" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 4);
    cluster.unblock(4, 1);
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    var restored_status = cluster.node(4).status();
    try std.testing.expectEqual(core.types.StateRole.follower, restored_status.soft.role);
    try cluster.tick(1, 1);
    try cluster.deliverAll();

    try cluster.propose(1, "after-joint-snapshot-restore");
    try cluster.deliverAll();

    const committed = try cluster.collectCommitted(4);
    defer core.types.freeEntries(std.testing.allocator, committed);
    try std.testing.expect(committed.len >= 1);
    try std.testing.expectEqualStrings("after-joint-snapshot-restore", committed[committed.len - 1].data);
    restored_status = cluster.node(4).status();
    try std.testing.expectEqual(@as(?core.types.NodeId, 1), restored_status.soft.leader_id);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 3, 4 }, restored_status.conf_state.voters);
    try std.testing.expectEqual(cluster.node(1).status().hard.commit_index, cluster.node(4).status().hard.commit_index);

    try cluster.readIndex(4, "restored-follower-read");
    const before_read = try cluster.collectReadStates(4);
    defer freeReadStates(std.testing.allocator, before_read);
    try std.testing.expectEqual(@as(usize, 0), before_read.len);
    try cluster.deliverAll();
    const read_states = try cluster.collectReadStates(4);
    defer freeReadStates(std.testing.allocator, read_states);
    try std.testing.expectEqual(@as(usize, 1), read_states.len);
    try std.testing.expectEqualStrings("restored-follower-read", read_states[0].request_ctx);
}

test "check_quorum restored incoming voter participates in replacement election" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 4);
    cluster.unblock(4, 1);
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "restore-before-election");
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.tick(1, 8);
    try cluster.tick(3, 8);
    try cluster.tick(4, 8);
    try cluster.deliverAll();

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(4).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 3), cluster.node(4).status().soft.leader_id);
}

test "pre-vote check_quorum restored incoming voter participates in replacement election" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 4);
    cluster.unblock(4, 1);
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "restore-before-pre-vote-election");
    try cluster.deliverAll();

    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.tick(1, 8);
    try cluster.tick(3, 8);
    try cluster.tick(4, 8);
    try cluster.deliverAll();

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(4).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 3), cluster.node(4).status().soft.leader_id);
}

test "check_quorum leader transfer can target restored incoming voter" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 4);
    cluster.unblock(4, 1);
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "restore-before-transfer");
    try cluster.deliverAll();

    try cluster.transferLeader(1, 4);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(4).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 4), cluster.node(3).status().soft.leader_id);
}

test "pre-vote check_quorum leader transfer can target restored incoming voter" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .initial_conf_state = .{
            .voters = initial_voters[0..],
        },
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.block(1, 4);
    try cluster.block(4, 1);
    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try cluster.deliverAll();
    try cluster.compact(1, 2);

    cluster.unblock(1, 4);
    cluster.unblock(4, 1);
    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.tick(1, 1);
    try cluster.deliverAll();
    try cluster.propose(1, "restore-before-pre-vote-transfer");
    try cluster.deliverAll();

    try cluster.transferLeader(1, 4);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(4).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, 4), cluster.node(3).status().soft.leader_id);
}

test "joint replace voter explicit leave swaps quorum member" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3, 4 }, joint_status.conf_state.voters_outgoing);

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
}

test "joint demote voter explicit leave moves node into learners" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, joint_status.conf_state.learners_next);

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, final_status.conf_state.learners);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.learners_next.len);
}

test "demoted outgoing voter can still win election before leaving joint" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, joint_status.conf_state.learners_next);

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "demoted outgoing voter can still win election before leaving joint with pre_vote" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "demoted outgoing voter can still win election before leaving joint with check_quorum" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.tick(1, cluster.election_tick * 2);
    try cluster.tick(2, cluster.election_tick * 2);
    try cluster.tick(3, cluster.election_tick * 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "demoted outgoing voter can still win election before leaving joint with pre_vote and check_quorum" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);
    try cluster.tick(1, cluster.election_tick * 2);
    try cluster.tick(2, cluster.election_tick * 2);
    try cluster.tick(3, cluster.election_tick * 2);
    try cluster.deliverAll();
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);

    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    cluster.unblock(1, 3);
    cluster.unblock(3, 1);
    try cluster.campaign(3);
    try cluster.deliverAll();

    try std.testing.expectEqual(core.types.StateRole.leader, cluster.node(3).status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.follower, cluster.node(1).status().soft.role);
}

test "demoted outgoing voter becomes unpromotable after leaving joint with pre_vote" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try std.testing.expectError(error.NotPromotable, cluster.campaign(3));
}

test "demoted outgoing voter becomes unpromotable after leaving joint with check_quorum" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try std.testing.expectError(error.NotPromotable, cluster.campaign(3));
}

test "demoted outgoing voter becomes unpromotable after leaving joint with pre_vote and check_quorum" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try std.testing.expectError(error.NotPromotable, cluster.campaign(3));
}

test "demoted outgoing voter becomes unpromotable after leaving joint" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, final_status.conf_state.learners);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.learners_next.len);

    try std.testing.expectError(error.NotPromotable, cluster.campaign(3));
}

test "joint idempotent mixed change keeps only surviving voter and staged learners" {
    var initial_voters = [_]core.types.NodeId{1};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4, 9 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 9 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    });
    defer std.testing.allocator.free(changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{1}, joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{2}, joint_status.conf_state.learners);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{1}, joint_status.conf_state.learners_next);

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2 }, final_status.conf_state.learners);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.learners_next.len);
}

test "joint chained learners_next demote and promote sequence converges" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const demote_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    });
    defer std.testing.allocator.free(demote_changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = demote_changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const first_joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 4 }, first_joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, first_joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 2, 3 }, first_joint_status.conf_state.learners_next);

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const demoted_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 4 }, demoted_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 2, 3 }, demoted_status.conf_state.learners);

    const promote_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 4 },
    });
    defer std.testing.allocator.free(promote_changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = promote_changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const second_joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, second_joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 4 }, second_joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{4}, second_joint_status.conf_state.learners_next);

    try cluster.node(1).proposeConfChangeV2(.{});
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    const final_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, final_status.conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.voters_outgoing.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{4}, final_status.conf_state.learners);
    try std.testing.expectEqual(@as(usize, 0), final_status.conf_state.learners_next.len);
}

test "reapplying joint mixed change while already joint is idempotent" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer std.testing.allocator.free(changes);

    const conf_change = core.types.ConfChangeV2{
        .transition = .joint_explicit,
        .changes = changes,
    };

    try cluster.proposeConfChangeV2(1, conf_change);
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters_outgoing);

    const replayed = try cluster.node(1).applyConfChangeV2(conf_change);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, replayed.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, replayed.voters_outgoing);
    try std.testing.expectEqual(@as(usize, 0), replayed.learners.len);
    try std.testing.expectEqual(@as(usize, 0), replayed.learners_next.len);
}

test "reapplying joint demote-to-learner change while already joint is idempotent" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    });
    defer std.testing.allocator.free(changes);

    const conf_change = core.types.ConfChangeV2{
        .transition = .joint_explicit,
        .changes = changes,
    };

    try cluster.proposeConfChangeV2(1, conf_change);
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, joint_status.conf_state.learners_next);

    const replayed = try cluster.node(1).applyConfChangeV2(conf_change);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4 }, replayed.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, replayed.voters_outgoing);
    try std.testing.expectEqual(@as(usize, 0), replayed.learners.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, replayed.learners_next);
}

test "reapplying joint change with preexisting learner and staged learner is idempotent" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    var initial_learners = [_]core.types.NodeId{5};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
        .learners = initial_learners[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 5 },
    });
    defer std.testing.allocator.free(changes);

    const conf_change = core.types.ConfChangeV2{
        .transition = .joint_explicit,
        .changes = changes,
    };

    try cluster.proposeConfChangeV2(1, conf_change);
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const joint_status = cluster.node(1).status();
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4, 5 }, joint_status.conf_state.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, joint_status.conf_state.voters_outgoing);
    try std.testing.expectEqual(@as(usize, 0), joint_status.conf_state.learners.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, joint_status.conf_state.learners_next);

    const replayed = try cluster.node(1).applyConfChangeV2(conf_change);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 4, 5 }, replayed.voters);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{ 1, 2, 3 }, replayed.voters_outgoing);
    try std.testing.expectEqual(@as(usize, 0), replayed.learners.len);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{3}, replayed.learners_next);
}

test "leader rejects second non-leave joint change while already joint" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    const first_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer std.testing.allocator.free(first_changes);

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();

    const second_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    });
    defer std.testing.allocator.free(second_changes);

    try std.testing.expectError(error.MustLeaveJointFirst, cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = second_changes,
    }));
}

test "pending config change blocks second proposal until restart leader applies it" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    const first_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer std.testing.allocator.free(first_changes);

    const second_changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    });
    defer std.testing.allocator.free(second_changes);

    try cluster.campaign(1);
    try cluster.deliverAll();
    const initial = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial);

    try cluster.block(1, 2);
    try cluster.block(2, 1);
    try cluster.block(1, 3);
    try cluster.block(3, 1);

    try cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try std.testing.expectError(error.PendingConfChange, cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = second_changes,
    }));

    try cluster.restart(1);
    cluster.unblock(1, 2);
    cluster.unblock(2, 1);
    try cluster.campaign(1);
    try cluster.deliverAll();

    try std.testing.expectError(error.MustLeaveJointFirst, cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = second_changes,
    }));

    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try std.testing.expectError(error.MustLeaveJointFirst, cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = second_changes,
    }));

    try cluster.proposeConfChangeV2(1, .{});
    try cluster.deliverAll();
    while (cluster.node(1).hasReady()) {
        try cluster.collectReady(1);
    }

    try cluster.node(1).proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = second_changes,
    });
}

test "step_down_on_removal turns removed leader into follower" {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .step_down_on_removal = true,
        .initial_conf_state = initial_conf_state,
    });
    defer cluster.deinit();

    const changes = try std.testing.allocator.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    });
    defer std.testing.allocator.free(changes);

    try cluster.campaign(1);
    try cluster.deliverAll();

    try cluster.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    try cluster.deliverAll();

    const status = cluster.node(1).status();
    try std.testing.expectEqual(core.types.StateRole.follower, status.soft.role);
    try std.testing.expectEqual(@as(?core.types.NodeId, null), status.soft.leader_id);
    try std.testing.expectEqualSlices(core.types.NodeId, &.{1}, status.conf_state.learners);
}

test "cluster async_storage_writes commits replicated proposal" {
    var cluster = try Cluster.initWithOptions(std.testing.allocator, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .async_storage_writes = true,
    });
    defer cluster.deinit();

    try cluster.campaign(1);
    try cluster.deliverAll();

    const initial_one = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, initial_one);
    const initial_two = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, initial_two);
    const initial_three = try cluster.collectCommitted(3);
    defer core.types.freeEntries(std.testing.allocator, initial_three);

    try cluster.propose(1, "async");
    try cluster.deliverAll();

    const leader_committed = try cluster.collectCommitted(1);
    defer core.types.freeEntries(std.testing.allocator, leader_committed);
    try std.testing.expectEqual(@as(usize, 1), leader_committed.len);
    try std.testing.expectEqualStrings("async", leader_committed[0].data);

    const follower_committed = try cluster.collectCommitted(2);
    defer core.types.freeEntries(std.testing.allocator, follower_committed);
    try std.testing.expectEqual(@as(usize, 1), follower_committed.len);
    try std.testing.expectEqualStrings("async", follower_committed[0].data);
}

fn freeReadStates(alloc: std.mem.Allocator, read_states: []core.types.ReadState) void {
    for (read_states) |*read_state| read_state.deinit(alloc);
    if (read_states.len > 0) alloc.free(read_states);
}
