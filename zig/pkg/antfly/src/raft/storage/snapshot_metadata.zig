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

//! Immutable original Raft snapshot metadata. This is an archive identity,
//! never the current configuration, a quorum proof, or installation authority.
const std = @import("std");
pub const Metadata = @import("raft_engine").core.types.SnapshotMetadata;
pub const max_members_per_set = 1024;

pub fn digest(group: u64, metadata: Metadata) ![32]u8 {
    if (group == 0 or metadata.index == 0 or metadata.term == 0) return error.InvalidSnapshotRetention;
    const conf = metadata.conf_state;
    if ((conf.auto_leave or conf.learners_next.len != 0) and conf.voters_outgoing.len == 0) return error.InvalidSnapshotRetention;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/retained-snapshot-metadata/v1");
    var encoded: [8]u8 = undefined;
    for ([_]u64{ group, metadata.index, metadata.term }) |value| {
        std.mem.writeInt(u64, &encoded, value, .little);
        hash.update(&encoded);
    }
    // Hash the exact serialized order, while checking duplicate membership in
    // O(n log n) with fixed scratch storage and no allocator under Raft's lock.
    var scratch: [max_members_per_set]u64 = undefined;
    for ([_][]const u64{ conf.voters, conf.voters_outgoing, conf.learners, conf.learners_next }) |members| {
        if (members.len > max_members_per_set) return error.InvalidSnapshotRetention;
        @memcpy(scratch[0..members.len], members);
        std.mem.sort(u64, scratch[0..members.len], {}, std.sort.asc(u64));
        for (scratch[0..members.len], 0..) |node, i| if (node == 0 or (i > 0 and scratch[i - 1] == node)) return error.InvalidSnapshotRetention;
        std.mem.writeInt(u64, &encoded, members.len, .little);
        hash.update(&encoded);
        for (members) |node| {
            std.mem.writeInt(u64, &encoded, node, .little);
            hash.update(&encoded);
        }
    }
    hash.update(&.{@intFromBool(conf.auto_leave)});
    var value: [32]u8 = undefined;
    hash.final(&value);
    return value;
}

test "retained snapshot metadata binds original joint membership and rejects invalid sets without allocation" {
    var voters = [_]u64{ 7, 11 };
    var outgoing = [_]u64{ 3, 7 };
    var next = [_]u64{3};
    const original: Metadata = .{ .index = 19, .term = 4, .conf_state = .{ .voters = &voters, .voters_outgoing = &outgoing, .learners_next = &next, .auto_leave = true } };
    const expected = try digest(72, original);
    var changed = original;
    changed.conf_state.voters_outgoing = &.{};
    try std.testing.expectError(error.InvalidSnapshotRetention, digest(72, changed));
    changed = original;
    changed.conf_state.learners = @constCast(&[_]u64{17});
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(72, changed)));
    changed = original;
    changed.conf_state.voters = @constCast(&[_]u64{ 7, 7 });
    try std.testing.expectError(error.InvalidSnapshotRetention, digest(72, changed));
    changed.conf_state.voters = @constCast(&[_]u64{0});
    try std.testing.expectError(error.InvalidSnapshotRetention, digest(72, changed));
    changed.conf_state.voters = @constCast(&(@as([max_members_per_set + 1]u64, @splat(1))));
    try std.testing.expectError(error.InvalidSnapshotRetention, digest(72, changed));
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(73, original)));
    changed = original;
    changed.term += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(72, changed)));
}

test "retained snapshot metadata cloning cleans every partial allocation and owns all membership sets" {
    const Run = struct {
        fn run(a: std.mem.Allocator, source: Metadata) !void {
            var copied = try source.clone(a);
            defer copied.deinit(a);
            try std.testing.expectEqualDeep(source, copied);
            try std.testing.expect(source.conf_state.voters.ptr != copied.conf_state.voters.ptr);
        }
    };
    const source: Metadata = .{ .index = 19, .term = 4, .conf_state = .{ .voters = @constCast(&[_]u64{ 7, 11 }), .voters_outgoing = @constCast(&[_]u64{ 3, 7 }), .learners = @constCast(&[_]u64{17}), .learners_next = @constCast(&[_]u64{3}), .auto_leave = true } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{source});
}

test "retained snapshot metadata provider checks the original snapshot rather than current membership" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    const layout_mod = @import("mod.zig");
    const Persistent = @import("replica_state.zig").PersistentReplicaState;
    const Wal = @import("wal_replica_state.zig").WalReplicaState;
    inline for (.{ Persistent, Wal }, 0..) |Provider, i| {
        const base = try std.fmt.allocPrint(a, "{s}/provider-{d}", .{ root, i });
        defer a.free(base);
        var layout = try layout_mod.ReplicaPathLayout.initForLocalNode(a, base, 72, 7);
        defer layout.deinit(a);
        var provider = if (Provider == Wal) try Provider.init(a, layout, .{}) else try Provider.init(a, layout);
        defer provider.deinit();
        const original: Metadata = .{ .index = 5, .term = 2, .conf_state = .{ .voters = @constCast(&[_]u64{ 7, 11 }) } };
        try provider.groupStorage().persistReady(72, .{ .snapshot = .{ .metadata = original, .data = @constCast("checkpoint") } });
        const expected = try digest(72, original);
        try provider.requireSnapshotMetadata(72, .{ .index = 5, .term = 2 }, expected);
        var newer = original;
        newer.conf_state.voters = @constCast(&[_]u64{ 7, 17 });
        try provider.setConfState(newer.conf_state);
        try provider.requireSnapshotMetadata(72, .{ .index = 5, .term = 2 }, expected);
        try std.testing.expectError(error.SnapshotPayloadIdentityConflict, provider.requireSnapshotMetadata(72, .{ .index = 5, .term = 2 }, try digest(72, newer)));
    }
}
