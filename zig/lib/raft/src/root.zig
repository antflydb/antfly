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

pub const core = @import("core/mod.zig");
pub const runtime = @import("runtime/mod.zig");
pub const testing = @import("testing/mod.zig");

const std = @import("std");

test {
    _ = core.Config;
    _ = runtime.RuntimeConfig;
    _ = testing.Cluster;
    _ = testing.TraceRecorder;
}

test "raft runtime retains tracked proposal terms through applied log compaction" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var peers = [_]core.types.NodeId{1};
    var group = try runtime.group.Group.init(std.testing.allocator, .{
        .group_id = 8,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 8,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = storage.storage(),
    });
    defer group.deinit();

    const entries = [_]core.Entry{
        .{ .term = 2, .index = 1, .data = &.{} },
        .{ .term = 2, .index = 2, .data = &.{} },
        .{ .term = 3, .index = 3, .data = &.{} },
    };
    _ = try group.raw_node.raft.log.appendEntries(&entries);
    group.raw_node.raft.log.commitTo(3);
    group.raw_node.raft.log.appliedTo(3);

    try group.prepareProposalReceiptTracking();
    group.trackProposalReceipt(2, 2);
    try group.prepareProposalReceiptTracking();
    group.trackProposalReceipt(4, 2);
    try std.testing.expect(group.acquireProposalReceipt(2, 2));
    try std.testing.expect(group.acquireProposalReceipt(2, 2));
    try std.testing.expect(group.acquireProposalReceipt(4, 2));
    try group.compactAppliedLogTo(3);
    try std.testing.expectError(error.IndexNotFound, group.termAt(2));
    try std.testing.expectEqual(@as(core.types.Term, 2), try group.termAtTrackedProposalReceipt(2, 2));
    // A receipt from a superseded term retains the actual replacement term,
    // allowing the caller to reject it after the log entry itself is gone.
    try std.testing.expectEqual(@as(core.types.Term, 2), try group.termAtTrackedProposalReceipt(4, 2));

    group.releaseProposalReceipt(2, 2);
    try std.testing.expectEqual(@as(core.types.Term, 2), try group.termAtTrackedProposalReceipt(2, 2));
    group.releaseProposalReceipt(2, 2);
    try std.testing.expectError(error.IndexNotFound, group.termAtTrackedProposalReceipt(2, 2));
    group.releaseProposalReceipt(4, 2);
    try std.testing.expectError(error.IndexNotFound, group.termAtTrackedProposalReceipt(4, 2));
}
