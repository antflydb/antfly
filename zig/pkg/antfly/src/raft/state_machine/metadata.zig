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

const std = @import("std");
const raft_engine = @import("raft_engine");
const applied_sink_mod = @import("applied_sink.zig");
const mod = @import("mod.zig");

pub const MetadataStateMachine = struct {
    alloc: std.mem.Allocator,
    applied_sink: applied_sink_mod.AppliedIndexSink,
    snapshot_builder: ?mod.SnapshotBuilder = null,
    delegate: ?raft_engine.runtime.storage_iface.StateMachine = null,

    pub fn stateMachine(self: *MetadataStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_snapshot = prepareSnapshot,
                .build_snapshot = buildSnapshot,
                .apply_ready = applyReady,
                .is_apply_retryable = isApplyRetryable,
                .retire_group = retireGroup,
            },
        };
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        applied_index: raft_engine.core.types.Index,
    ) !?raft_engine.runtime.storage_iface.SnapshotSource {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        const builder = self.snapshot_builder orelse return null;
        return try builder.prepareSnapshot(group_id, applied_index);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: raft_engine.core.types.GroupId) !?[]u8 {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        const builder = self.snapshot_builder orelse return null;
        return try builder.buildSnapshot(alloc, group_id);
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        snapshot: ?raft_engine.core.types.Snapshot,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        if (snapshot) |value| {
            if (self.snapshot_builder) |snapshot_builder| {
                if (!try snapshot_builder.installSnapshot(self.alloc, group_id, value.metadata.index, value.data)) {
                    return error.SnapshotInstallUnsupported;
                }
            }
        }
        if (committed_entries.len > 0) {
            if (self.snapshot_builder) |snapshot_builder| {
                const payload = try mod.encodeCommittedEntries(self.alloc, committed_entries);
                defer self.alloc.free(payload);
                try snapshot_builder.applyBatch(.{
                    .group_id = group_id,
                    .commit_index = committed_entries[committed_entries.len - 1].index,
                    .entries_bytes = payload,
                });
            }
        }
        if (self.delegate) |delegate| {
            try delegate.applyReady(group_id, snapshot, committed_entries, read_states);
        }
        const applied_index = if (committed_entries.len > 0)
            committed_entries[committed_entries.len - 1].index
        else if (snapshot) |value|
            value.metadata.index
        else
            0;
        if (applied_index > 0) try self.applied_sink.setAppliedIndex(group_id, applied_index);
    }

    fn isApplyRetryable(ptr: *anyopaque, group_id: u64, err: anyerror) bool {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        // Only the durable builder can opt this entire Ready into retry.
        // Delegate or notification failures are not implicitly retryable.
        const builder = self.snapshot_builder orelse return false;
        return builder.isApplyRetryable(group_id, err);
    }

    fn retireGroup(ptr: *anyopaque, group_id: raft_engine.core.types.GroupId) void {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        if (self.delegate) |delegate| delegate.retireGroup(group_id);
    }
};

test "relation reconciliation worker metadata retry keeps apply and read completion behind prepared evidence" {
    const Fixture = struct {
        pages_left: usize = 3,
        commits: usize = 0,
        delegates: usize = 0,
        notifications: usize = 0,
        fault: ?anyerror = null,
        fn build(_: *anyopaque, a: std.mem.Allocator, _: u64) ![]u8 {
            return a.dupe(u8, "");
        }
        fn apply(ptr: *anyopaque, batch: mod.ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), batch.group_id);
            try std.testing.expectEqual(@as(u64, 11), batch.commit_index);
            if (self.fault) |err| return err;
            if (self.pages_left != 0) {
                self.pages_left -= 1;
                return error.CatalogPublicationProofPending;
            }
            self.commits += 1;
        }
        fn retry(_: *anyopaque, group: u64, err: anyerror) bool {
            return group == 7 and err == error.CatalogPublicationProofPending;
        }
        fn delegate(ptr: *anyopaque, _: u64, _: ?raft_engine.core.types.Snapshot, _: []const raft_engine.core.Entry, reads: []const raft_engine.core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), self.commits);
            try std.testing.expectEqual(@as(usize, 1), reads.len);
            self.delegates += 1;
        }
        fn applied(ptr: *anyopaque, group: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), group);
            try std.testing.expectEqual(@as(u64, 11), index);
            try std.testing.expectEqual(@as(usize, 1), self.delegates);
            self.notifications += 1;
        }
    };
    var fixture: Fixture = .{};
    var metadata: MetadataStateMachine = .{
        .alloc = std.testing.allocator,
        .applied_sink = .{ .ptr = &fixture, .vtable = &.{ .set_applied_index = Fixture.applied } },
        .snapshot_builder = .{ .ptr = &fixture, .vtable = &.{ .build_snapshot = Fixture.build, .apply_batch = Fixture.apply, .is_apply_retryable = Fixture.retry } },
        .delegate = .{ .ptr = &fixture, .vtable = &.{ .apply_ready = Fixture.delegate } },
    };
    var routed: mod.RoutedStateMachine = .{
        .metadata_group_id = 7,
        .metadata_state_machine = metadata.stateMachine(),
        .data_state_machine = metadata.delegate.?,
    };
    var worker = raft_engine.runtime.apply_worker.QueuedApplyWorker.init(std.testing.allocator, routed.stateMachine());
    defer worker.deinit();
    const queue = worker.queue();
    try std.testing.expect(queue.isApplyRetryable(7, error.CatalogPublicationProofPending));
    try std.testing.expect(!queue.isApplyRetryable(8, error.CatalogPublicationProofPending));
    try std.testing.expect(!queue.isApplyRetryable(7, error.InvalidCatalogRecord));
    try std.testing.expect(!queue.isApplyRetryable(7, error.OutOfMemory));
    const legacy: mod.SnapshotBuilder = .{ .ptr = &fixture, .vtable = &.{ .build_snapshot = Fixture.build, .apply_batch = Fixture.apply } };
    try std.testing.expect(!legacy.isApplyRetryable(7, error.CatalogPublicationProofPending));
    for (0..3) |_| {
        try queue.enqueueApply(7, null, &.{.{ .term = 2, .index = 11 }}, &.{.{ .index = 11, .request_ctx = @constCast("reader") }});
        const result = queue.drain();
        try std.testing.expectEqual(@as(usize, 0), result.completed);
        try std.testing.expectEqual(error.CatalogPublicationProofPending, result.failure.?);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        try std.testing.expectEqual(@as(usize, 0), fixture.delegates);
        try std.testing.expectEqual(@as(usize, 0), fixture.notifications);
        queue.abort(); // The runtime, not this adapter, retains the Ready.
    }
    fixture.fault = error.InvalidCatalogRecord;
    try queue.enqueueApply(7, null, &.{.{ .term = 2, .index = 11 }}, &.{});
    try std.testing.expectEqual(error.InvalidCatalogRecord, queue.drain().failure.?);
    try std.testing.expectEqual(@as(usize, 0), fixture.notifications);
    queue.abort();
    fixture.fault = null;
    try queue.enqueueApply(7, null, &.{.{ .term = 2, .index = 11 }}, &.{.{ .index = 11, .request_ctx = @constCast("reader") }});
    const finished = queue.drain();
    try std.testing.expectEqual(@as(usize, 1), finished.completed);
    try std.testing.expect(finished.failure == null);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 1), fixture.notifications);
}
