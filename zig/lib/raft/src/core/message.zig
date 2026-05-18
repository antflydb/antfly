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
const types = @import("types.zig");

pub const LocalAppendThread: types.NodeId = std.math.maxInt(types.NodeId);
pub const LocalApplyThread: types.NodeId = LocalAppendThread - 1;

pub const MessageType = enum {
    propose,
    pre_vote,
    pre_vote_response,
    request_vote,
    request_vote_response,
    append_entries,
    append_entries_response,
    heartbeat,
    heartbeat_response,
    snapshot,
    snapshot_response,
    transfer_leader,
    forget_leader,
    timeout_now,
    read_index,
    read_index_response,
    storage_append,
    storage_append_response,
    storage_apply,
    storage_apply_response,
};

pub const Message = struct {
    msg_type: MessageType,
    from: types.NodeId,
    to: types.NodeId,
    term: types.Term = 0,
    vote: ?types.NodeId = null,
    log_index: types.Index = 0,
    log_term: types.Term = 0,
    commit_index: types.Index = 0,
    reject: bool = false,
    reject_hint: types.Index = 0,
    entries: []types.Entry = &.{},
    snapshot: ?types.Snapshot = null,
    context: []u8 = &.{},
    responses: []Message = &.{},

    pub fn clone(self: Message, alloc: std.mem.Allocator) std.mem.Allocator.Error!Message {
        return .{
            .msg_type = self.msg_type,
            .from = self.from,
            .to = self.to,
            .term = self.term,
            .vote = self.vote,
            .log_index = self.log_index,
            .log_term = self.log_term,
            .commit_index = self.commit_index,
            .reject = self.reject,
            .reject_hint = self.reject_hint,
            .entries = try types.cloneEntries(alloc, self.entries),
            .snapshot = if (self.snapshot) |snapshot| try snapshot.clone(alloc) else null,
            .context = try alloc.dupe(u8, self.context),
            .responses = try cloneMessages(alloc, self.responses),
        };
    }

    pub fn deinit(self: *Message, alloc: std.mem.Allocator) void {
        types.freeEntries(alloc, self.entries);
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        if (self.context.len > 0) alloc.free(self.context);
        freeMessages(alloc, self.responses);
        self.* = undefined;
    }
};

pub fn isLocalStorageThread(node_id: types.NodeId) bool {
    return node_id == LocalAppendThread or node_id == LocalApplyThread;
}

pub fn cloneMessages(alloc: std.mem.Allocator, msgs: []const Message) std.mem.Allocator.Error![]Message {
    const out = try alloc.alloc(Message, msgs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*msg| msg.deinit(alloc);
        alloc.free(out);
    }
    for (msgs, 0..) |msg, i| {
        out[i] = try msg.clone(alloc);
        initialized += 1;
    }
    return out;
}

pub fn freeMessages(alloc: std.mem.Allocator, msgs: []Message) void {
    for (msgs) |*msg| msg.deinit(alloc);
    if (msgs.len > 0) alloc.free(msgs);
}
