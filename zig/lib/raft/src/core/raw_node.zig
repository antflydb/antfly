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

const std = @import("std");
const raft_mod = @import("raft.zig");
const types = @import("types.zig");
const message = @import("message.zig");
const ready_mod = @import("ready.zig");
const storage_mod = @import("storage.zig");

test "durability-independent heartbeat extraction respects persisted term and queue budget" {
    var store = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    store.setHardState(.{ .current_term = 1 });
    var node = try RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 9,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 5,
        .heartbeat_tick = 1,
        .async_storage_writes = true,
    }, store.storage());
    defer node.deinit();
    try node.step(.{ .msg_type = .heartbeat, .from = 2, .to = 1, .term = 2, .context = @constCast("read-proof") });
    const unpersisted = try node.takeDurabilityIndependentHeartbeats(std.testing.allocator, 1, 8, 1024);
    defer message.freeMessages(std.testing.allocator, unpersisted);
    try std.testing.expectEqual(@as(usize, 0), unpersisted.len);
    const denied = try node.takeDurabilityIndependentHeartbeats(std.testing.allocator, 2, 1, 1);
    defer message.freeMessages(std.testing.allocator, denied);
    try std.testing.expectEqual(@as(usize, 0), denied.len);
    store.setHardState(node.raft.hard_state);
    const admitted = try node.takeDurabilityIndependentHeartbeats(std.testing.allocator, 2, 1, 1024);
    defer message.freeMessages(std.testing.allocator, admitted);
    try std.testing.expectEqual(@as(usize, 1), admitted.len);
    try std.testing.expectEqual(message.MessageType.heartbeat_response, admitted[0].msg_type);
    try std.testing.expectEqualStrings("read-proof", admitted[0].context);
    try std.testing.expectEqual(@as(usize, 0), node.raft.messages.items.len);
    try node.step(.{ .msg_type = .heartbeat, .from = 2, .to = 1, .term = 2, .context = @constCast("too-large-first") });
    try node.step(.{ .msg_type = .heartbeat, .from = 3, .to = 1, .term = 2, .context = @constCast("r") });
    try node.step(.{ .msg_type = .heartbeat, .from = 2, .to = 1, .term = 2, .context = @constCast("too-large-last") });
    const middle = try node.takeDurabilityIndependentHeartbeats(std.testing.allocator, 2, 1, 70);
    defer message.freeMessages(std.testing.allocator, middle);
    try std.testing.expectEqual(@as(usize, 1), middle.len);
    try std.testing.expectEqualStrings("r", middle[0].context);
    try std.testing.expectEqual(@as(usize, 2), node.raft.messages.items.len);
    try std.testing.expectEqualStrings("too-large-first", node.raft.messages.items[0].context);
    try std.testing.expectEqualStrings("too-large-last", node.raft.messages.items[1].context);
}

pub const RawNode = struct {
    raft: raft_mod.Raft,
    async_storage_writes: bool,
    prev_soft_state: types.SoftState,
    prev_hard_state: types.HardState,
    ready_read_states: std.ArrayListUnmanaged(types.ReadState) = .empty,
    ready_messages: std.ArrayListUnmanaged(message.Message) = .empty,

    pub fn init(alloc: std.mem.Allocator, cfg: raft_mod.Config, storage: storage_mod.Storage) !RawNode {
        const raft = try raft_mod.Raft.init(alloc, cfg, storage);
        return .{
            .async_storage_writes = cfg.async_storage_writes,
            .prev_soft_state = raft.soft_state,
            .prev_hard_state = raft.hard_state,
            .raft = raft,
        };
    }

    pub fn deinit(self: *RawNode) void {
        self.clearReadyMessages();
        self.ready_read_states.deinit(self.raft.alloc);
        self.ready_messages.deinit(self.raft.alloc);
        self.raft.deinit();
        self.* = undefined;
    }

    pub fn tick(self: *RawNode) void {
        self.clearReadyMessages();
        self.raft.tick();
    }

    pub fn campaign(self: *RawNode) !void {
        self.clearReadyMessages();
        return try self.raft.campaign();
    }

    pub fn transferLeader(self: *RawNode, transferee: types.NodeId) !void {
        self.clearReadyMessages();
        return try self.raft.transferLeader(transferee);
    }

    pub fn forgetLeader(self: *RawNode) !void {
        self.clearReadyMessages();
        return try self.raft.step(.{
            .msg_type = .forget_leader,
            .from = self.raft.cfg.id,
            .to = self.raft.cfg.id,
        });
    }

    pub fn step(self: *RawNode, msg: message.Message) !void {
        self.clearReadyMessages();
        return try self.raft.step(msg);
    }

    pub fn propose(self: *RawNode, data: []const u8) !void {
        var accepted_index: ?types.Index = null;
        return try self.proposeWithReceipt(data, &accepted_index);
    }

    pub fn proposeWithReceipt(self: *RawNode, data: []const u8, accepted_index: *?types.Index) !void {
        self.clearReadyMessages();
        return try self.raft.proposeWithReceipt(data, accepted_index);
    }

    pub fn proposeBatchWithReceipt(
        self: *RawNode,
        payloads: []const []const u8,
        accepted_first_index: *?types.Index,
        accepted_last_index: *?types.Index,
    ) !void {
        self.clearReadyMessages();
        return try self.raft.proposeBatchWithReceipt(
            payloads,
            accepted_first_index,
            accepted_last_index,
        );
    }

    pub fn readIndex(self: *RawNode, rctx: []const u8) !void {
        self.clearReadyMessages();
        return try self.raft.readIndex(rctx);
    }

    pub fn reportSnapshotFailure(
        self: *RawNode,
        to: types.NodeId,
        leader_term: types.Term,
        snapshot_index: types.Index,
        snapshot_term: types.Term,
        attempt_generation: u64,
    ) bool {
        return self.raft.reportSnapshotFailure(to, leader_term, snapshot_index, snapshot_term, attempt_generation);
    }

    pub fn reportSnapshotDelivered(
        self: *RawNode,
        to: types.NodeId,
        leader_term: types.Term,
        snapshot_index: types.Index,
        snapshot_term: types.Term,
        attempt_generation: u64,
    ) bool {
        return self.raft.reportSnapshotDelivered(to, leader_term, snapshot_index, snapshot_term, attempt_generation);
    }

    pub fn proposeConfChange(self: *RawNode, conf_change: types.ConfChange) !void {
        self.clearReadyMessages();
        return try self.raft.proposeConfChange(conf_change);
    }

    pub fn proposeConfChangeV2(self: *RawNode, conf_change: types.ConfChangeV2) !void {
        self.clearReadyMessages();
        return try self.raft.proposeConfChangeV2(conf_change);
    }

    pub fn applyConfChange(self: *RawNode, conf_change: types.ConfChange) !types.ConfState {
        return try self.raft.applyConfChange(conf_change);
    }

    pub fn applyConfChangeV2(self: *RawNode, conf_change: types.ConfChangeV2) !types.ConfState {
        return try self.raft.applyConfChangeV2(conf_change);
    }

    pub fn hasReady(self: *const RawNode) bool {
        if (!self.async_storage_writes) return self.raft.hasReady();
        if (!types.SoftState.eql(self.raft.soft_state, self.prev_soft_state)) return true;
        if (!types.HardState.eql(self.raft.hard_state, self.prev_hard_state)) return true;
        if (self.raft.log.hasNextUnstableEntries()) return true;
        if (self.raft.pending_snapshot != null and !self.raft.snapshot_in_progress) return true;
        if (self.raft.log.hasNextCommittedEntriesAllow(false)) return true;
        if (self.raft.read_states.items.len > 0) return true;
        return self.raft.messages.items.len > 0;
    }

    pub fn ready(self: *RawNode) ready_mod.Ready {
        const rd = self.prepareReady();
        self.acceptPreparedReady(rd);
        return rd;
    }

    /// Admission may inspect this Ready repeatedly without consuming it.
    /// Its borrowed slices remain valid until the next prepare/step call.
    pub fn prepareReady(self: *RawNode) ready_mod.Ready {
        if (!self.async_storage_writes) return self.raft.ready();

        self.clearReadyMessages();
        var rd = ready_mod.Ready{
            .soft_state = if (!types.SoftState.eql(self.raft.soft_state, self.prev_soft_state)) self.raft.soft_state else null,
            .hard_state = if (!types.HardState.eql(self.raft.hard_state, self.prev_hard_state)) self.raft.hard_state else null,
            .snapshot = if (!self.raft.snapshot_in_progress) self.raft.pending_snapshot else null,
            .entries = self.raft.log.unstableEntries(),
            .committed_entries = self.raft.log.nextCommittedEntriesMaxAllow(self.raft.cfg.max_committed_size_per_ready, false),
            .read_states = &.{},
            .messages = self.raft.messages.items,
        };
        if (self.raft.read_states.items.len > 0) {
            self.ready_read_states.ensureUnusedCapacity(self.raft.alloc, self.raft.read_states.items.len) catch unreachable;
            for (self.raft.read_states.items) |read_state| {
                self.ready_read_states.appendAssumeCapacity(read_state.clone(self.raft.alloc) catch unreachable);
            }
            rd.read_states = self.ready_read_states.items;
        }
        self.raft.noteReady();

        if (needsStorageAppend(rd)) {
            tryBuildStorageAppendMessage(self, rd) catch unreachable;
        }
        if (rd.committed_entries.len > 0) {
            tryBuildStorageApplyMessage(self, rd.committed_entries) catch unreachable;
        }

        rd.messages = self.ready_messages.items;
        return rd;
    }

    pub fn acceptPreparedReady(self: *RawNode, rd: ready_mod.Ready) void {
        if (self.async_storage_writes) self.acceptAsyncReady(rd);
    }

    /// While an append is outstanding, same-term heartbeats do not promise
    /// new log durability. Votes, append responses, and every message from a
    /// new term stay behind the persistence barrier.
    pub fn takeDurabilityIndependentHeartbeats(self: *RawNode, alloc: std.mem.Allocator, durable_term: types.Term, max_messages: usize, max_bytes: usize) ![]message.Message {
        return self.takeDurabilityIndependentControl(alloc, durable_term, 0, false, max_messages, max_bytes);
    }

    fn independentControl(msg: message.Message, durable_term: types.Term, durable_index: types.Index, allow_reads: bool) bool {
        // ReadIndex messages carry no term in this protocol. They introduce
        // no vote/log durability promise; responses name a durable prefix.
        if (allow_reads and (msg.msg_type == .read_index or msg.msg_type == .read_index_response) and msg.term == 0)
            return msg.msg_type == .read_index or msg.log_index <= durable_index;
        if (msg.term != durable_term) return false;
        return switch (msg.msg_type) {
            .heartbeat, .heartbeat_response => true,
            .read_index => allow_reads,
            .read_index_response => allow_reads and msg.log_index <= durable_index,
            else => false,
        };
    }

    pub fn takeDurabilityIndependentControl(self: *RawNode, alloc: std.mem.Allocator, durable_term: types.Term, durable_index: types.Index, allow_reads: bool, max_messages: usize, max_bytes: usize) ![]message.Message {
        var out = std.ArrayListUnmanaged(message.Message).empty;
        errdefer {
            for (out.items) |*msg| msg.deinit(alloc);
            out.deinit(alloc);
        }
        if (self.raft.hard_state.current_term != durable_term) return try out.toOwnedSlice(alloc);
        var bytes: usize = 0;
        for (self.raft.messages.items) |msg| {
            const size = 64 +| msg.context.len;
            if (independentControl(msg, durable_term, durable_index, allow_reads) and
                out.items.len < max_messages and size <= max_bytes -| bytes)
            {
                const owned = try msg.clone(alloc);
                out.append(alloc, owned) catch |err| {
                    var failed = owned;
                    failed.deinit(alloc);
                    return err;
                };
                bytes += size;
            }
        }
        const result = try out.toOwnedSlice(alloc);
        bytes = 0;
        var removed_count: usize = 0;
        var retained: usize = 0;
        for (self.raft.messages.items) |msg| {
            const size = 64 +| msg.context.len;
            if (independentControl(msg, durable_term, durable_index, allow_reads) and
                removed_count < result.len and size <= max_bytes -| bytes)
            {
                var removed = msg;
                removed.deinit(self.raft.alloc);
                bytes += size;
                removed_count += 1;
            } else {
                self.raft.messages.items[retained] = msg;
                retained += 1;
            }
        }
        self.raft.messages.shrinkRetainingCapacity(retained);
        return result;
    }

    pub fn takeDurableReadStates(self: *RawNode, alloc: std.mem.Allocator, durable_index: types.Index) ![]types.ReadState {
        var out = std.ArrayListUnmanaged(types.ReadState).empty;
        errdefer {
            for (out.items) |*read| read.deinit(alloc);
            out.deinit(alloc);
        }
        for (self.raft.read_states.items) |read| {
            if (read.index > durable_index) continue;
            const owned = try read.clone(alloc);
            out.append(alloc, owned) catch |err| {
                var failed = owned;
                failed.deinit(alloc);
                return err;
            };
        }
        const result = try out.toOwnedSlice(alloc);
        var retained: usize = 0;
        for (self.raft.read_states.items) |read| {
            if (read.index <= durable_index) {
                var removed = read;
                removed.deinit(self.raft.alloc);
            } else {
                self.raft.read_states.items[retained] = read;
                retained += 1;
            }
        }
        self.raft.read_states.shrinkRetainingCapacity(retained);
        return result;
    }

    pub fn advance(self: *RawNode, rd: ready_mod.Ready) void {
        if (self.async_storage_writes) {
            @panic("advance must not be used when async_storage_writes is enabled");
        }
        self.raft.advance(rd);
    }

    pub fn status(self: *const RawNode) types.Status {
        return self.raft.status();
    }

    pub fn compactAppliedLogTo(self: *RawNode, index: types.Index) !void {
        try self.raft.compactAppliedLogTo(index);
    }

    pub fn termAt(self: *RawNode, index: types.Index) !types.Term {
        return self.raft.log.term(index) orelse error.IndexNotFound;
    }

    fn needsStorageAppend(rd: ready_mod.Ready) bool {
        return rd.entries.len > 0 or
            rd.snapshot != null or
            rd.hard_state != null or
            rd.messages.len > 0;
    }

    fn tryBuildStorageAppendMessage(self: *RawNode, rd: ready_mod.Ready) !void {
        var responses = std.ArrayListUnmanaged(message.Message).empty;
        errdefer {
            for (responses.items) |*response| response.deinit(self.raft.alloc);
            responses.deinit(self.raft.alloc);
        }

        try responses.ensureUnusedCapacity(self.raft.alloc, rd.messages.len + 1);
        for (rd.messages) |msg| responses.appendAssumeCapacity(try msg.clone(self.raft.alloc));

        if (rd.entries.len > 0 or rd.snapshot != null or rd.hard_state != null or self.raft.log.hasNextOrInProgressUnstableEntries()) {
            try responses.append(self.raft.alloc, try self.storageAppendResponseMessage(rd));
        }

        try self.ready_messages.append(self.raft.alloc, .{
            .msg_type = .storage_append,
            .from = self.raft.cfg.id,
            .to = message.LocalAppendThread,
            .term = if (rd.hard_state) |hard_state| hard_state.current_term else 0,
            .vote = if (rd.hard_state) |hard_state| hard_state.voted_for else null,
            .commit_index = if (rd.hard_state) |hard_state| hard_state.commit_index else 0,
            .entries = try types.cloneEntries(self.raft.alloc, rd.entries),
            .snapshot = if (rd.snapshot) |snapshot| try snapshot.clone(self.raft.alloc) else null,
            .responses = try responses.toOwnedSlice(self.raft.alloc),
        });
    }

    fn storageAppendResponseMessage(self: *RawNode, rd: ready_mod.Ready) !message.Message {
        var msg = message.Message{
            .msg_type = .storage_append_response,
            .from = message.LocalAppendThread,
            .to = self.raft.cfg.id,
            .term = self.raft.hard_state.current_term,
        };
        if (self.raft.log.hasNextOrInProgressUnstableEntries()) {
            const last_index = self.raft.log.lastIndex();
            msg.log_index = last_index;
            msg.log_term = self.raft.log.term(last_index) orelse 0;
        }
        if (rd.snapshot) |snapshot| {
            msg.snapshot = try snapshot.clone(self.raft.alloc);
        }
        return msg;
    }

    fn tryBuildStorageApplyMessage(self: *RawNode, committed_entries: []const types.Entry) !void {
        var responses = std.ArrayListUnmanaged(message.Message).empty;
        errdefer {
            for (responses.items) |*response| response.deinit(self.raft.alloc);
            responses.deinit(self.raft.alloc);
        }
        try responses.append(self.raft.alloc, .{
            .msg_type = .storage_apply_response,
            .from = message.LocalApplyThread,
            .to = self.raft.cfg.id,
            .entries = try types.cloneEntries(self.raft.alloc, committed_entries),
        });

        try self.ready_messages.append(self.raft.alloc, .{
            .msg_type = .storage_apply,
            .from = self.raft.cfg.id,
            .to = message.LocalApplyThread,
            .entries = try types.cloneEntries(self.raft.alloc, committed_entries),
            .responses = try responses.toOwnedSlice(self.raft.alloc),
        });
    }

    fn acceptAsyncReady(self: *RawNode, rd: ready_mod.Ready) void {
        if (rd.soft_state) |soft| self.prev_soft_state = soft;
        if (rd.hard_state) |hard| self.prev_hard_state = hard;
        if (rd.read_states.len > 0) {
            for (self.raft.read_states.items[0..rd.read_states.len]) |*read_state| read_state.deinit(self.raft.alloc);
            std.mem.copyForwards(types.ReadState, self.raft.read_states.items, self.raft.read_states.items[rd.read_states.len..]);
            self.raft.read_states.shrinkRetainingCapacity(self.raft.read_states.items.len - rd.read_states.len);
        }
        // Configuration application may emit new messages between preparation
        // and admission. Only the prefix captured in storage_append.responses
        // belongs to this Ready; preserve the new frontier for the next one.
        var captured: usize = 0;
        for (rd.messages) |msg| if (msg.msg_type == .storage_append) {
            for (msg.responses) |response| {
                if (response.msg_type != .storage_append_response) captured += 1;
            }
        };
        for (self.raft.messages.items[0..captured]) |*msg| msg.deinit(self.raft.alloc);
        std.mem.copyForwards(message.Message, self.raft.messages.items, self.raft.messages.items[captured..]);
        self.raft.messages.shrinkRetainingCapacity(self.raft.messages.items.len - captured);
        if (rd.entries.len > 0) {
            self.raft.log.acceptPersisting(rd.entries[rd.entries.len - 1].index);
        }
        if (rd.snapshot != null) self.raft.snapshot_in_progress = true;
        if (rd.committed_entries.len > 0) {
            self.raft.log.acceptApplying(rd.committed_entries[rd.committed_entries.len - 1].index);
        }
    }

    fn clearReadyMessages(self: *RawNode) void {
        for (self.ready_read_states.items) |*read_state| read_state.deinit(self.raft.alloc);
        self.ready_read_states.clearRetainingCapacity();
        for (self.ready_messages.items) |*msg| msg.deinit(self.raft.alloc);
        self.ready_messages.clearRetainingCapacity();
    }
};
