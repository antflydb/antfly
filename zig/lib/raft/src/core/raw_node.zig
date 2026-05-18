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
const raft_mod = @import("raft.zig");
const types = @import("types.zig");
const message = @import("message.zig");
const ready_mod = @import("ready.zig");
const storage_mod = @import("storage.zig");

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
        self.clearReadyMessages();
        return try self.raft.propose(data);
    }

    pub fn readIndex(self: *RawNode, rctx: []const u8) !void {
        self.clearReadyMessages();
        return try self.raft.readIndex(rctx);
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

        self.acceptAsyncReady(rd);
        rd.messages = self.ready_messages.items;
        return rd;
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
        if (rd.soft_state != null) self.prev_soft_state = self.raft.soft_state;
        if (rd.hard_state != null) self.prev_hard_state = self.raft.hard_state;
        if (rd.read_states.len > 0) {
            for (self.raft.read_states.items) |*read_state| read_state.deinit(self.raft.alloc);
            self.raft.read_states.clearRetainingCapacity();
        }
        for (self.raft.messages.items) |*msg| msg.deinit(self.raft.alloc);
        self.raft.messages.clearRetainingCapacity();
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
