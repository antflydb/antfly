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

pub const Allocator = std.mem.Allocator;

pub const NodeId = u64;
pub const GroupId = u64;
pub const Term = u64;
pub const Index = u64;

pub const StateRole = enum {
    follower,
    pre_candidate,
    candidate,
    leader,
};

pub const VoteResult = enum {
    pending,
    won,
    lost,
};

pub const ReadOnlyOption = enum {
    safe,
    lease_based,
};

pub const EntryType = enum {
    normal,
    conf_change,
    conf_change_v2,
};

pub const ConfChangeType = enum(u8) {
    add_node = 1,
    remove_node = 2,
    add_learner_node = 3,
};

pub const ConfChangeTransition = enum(u8) {
    auto = 0,
    joint_explicit = 1,
    joint_implicit = 2,
};

pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,

    pub fn encode(self: ConfChange, alloc: Allocator) ![]u8 {
        const out = try alloc.alloc(u8, 9);
        out[0] = @intFromEnum(self.change_type);
        std.mem.writeInt(u64, out[1..9], self.node_id, .little);
        return out;
    }

    pub fn decode(data: []const u8) !ConfChange {
        if (data.len != 9) return error.InvalidConfChangeEncoding;
        const change_tag = switch (data[0]) {
            @intFromEnum(ConfChangeType.add_node) => ConfChangeType.add_node,
            @intFromEnum(ConfChangeType.remove_node) => ConfChangeType.remove_node,
            @intFromEnum(ConfChangeType.add_learner_node) => ConfChangeType.add_learner_node,
            else => return error.InvalidConfChangeEncoding,
        };
        return .{
            .change_type = change_tag,
            .node_id = std.mem.readInt(u64, data[1..9], .little),
        };
    }
};

pub const ConfChangeSingle = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
};

pub const ConfChangeV2 = struct {
    transition: ConfChangeTransition = .auto,
    changes: []ConfChangeSingle = &.{},
    context: []u8 = &.{},

    pub fn clone(self: ConfChangeV2, alloc: Allocator) !ConfChangeV2 {
        return .{
            .transition = self.transition,
            .changes = try alloc.dupe(ConfChangeSingle, self.changes),
            .context = try alloc.dupe(u8, self.context),
        };
    }

    pub fn deinit(self: *ConfChangeV2, alloc: Allocator) void {
        if (self.changes.len > 0) alloc.free(self.changes);
        if (self.context.len > 0) alloc.free(self.context);
        self.* = undefined;
    }

    pub fn encode(self: ConfChangeV2, alloc: Allocator) ![]u8 {
        const header_len: usize = 1 + 1 + 4;
        const changes_len: usize = self.changes.len * (1 + 8);
        const context_len: usize = 4 + self.context.len;
        const out = try alloc.alloc(u8, header_len + changes_len + context_len);

        out[0] = 2;
        out[1] = @intFromEnum(self.transition);
        std.mem.writeInt(u32, out[2..6], @intCast(self.changes.len), .little);

        var cursor: usize = 6;
        for (self.changes) |change| {
            out[cursor] = @intFromEnum(change.change_type);
            cursor += 1;
            writeIntAt(u64, out, cursor, change.node_id);
            cursor += 8;
        }

        writeIntAt(u32, out, cursor, @intCast(self.context.len));
        cursor += 4;
        @memcpy(out[cursor .. cursor + self.context.len], self.context);
        return out;
    }

    pub fn decode(data: []const u8, alloc: Allocator) !ConfChangeV2 {
        if (data.len < 10) return error.InvalidConfChangeEncoding;
        if (data[0] != 2) return error.InvalidConfChangeEncoding;

        const transition = switch (data[1]) {
            @intFromEnum(ConfChangeTransition.auto) => ConfChangeTransition.auto,
            @intFromEnum(ConfChangeTransition.joint_explicit) => ConfChangeTransition.joint_explicit,
            @intFromEnum(ConfChangeTransition.joint_implicit) => ConfChangeTransition.joint_implicit,
            else => return error.InvalidConfChangeEncoding,
        };

        const change_count: usize = std.mem.readInt(u32, data[2..6], .little);
        var cursor: usize = 6;
        const required_len = cursor + change_count * 9 + 4;
        if (data.len < required_len) return error.InvalidConfChangeEncoding;

        const changes = try alloc.alloc(ConfChangeSingle, change_count);
        errdefer alloc.free(changes);
        for (changes, 0..) |*change, i| {
            _ = i;
            change.change_type = switch (data[cursor]) {
                @intFromEnum(ConfChangeType.add_node) => .add_node,
                @intFromEnum(ConfChangeType.remove_node) => .remove_node,
                @intFromEnum(ConfChangeType.add_learner_node) => .add_learner_node,
                else => return error.InvalidConfChangeEncoding,
            };
            cursor += 1;
            change.node_id = readIntAt(u64, data, cursor);
            cursor += 8;
        }

        if (data.len < cursor + 4) return error.InvalidConfChangeEncoding;
        const context_len: usize = readIntAt(u32, data, cursor);
        cursor += 4;
        if (data.len != cursor + context_len) return error.InvalidConfChangeEncoding;

        return .{
            .transition = transition,
            .changes = changes,
            .context = try alloc.dupe(u8, data[cursor..]),
        };
    }
};

pub const Entry = struct {
    term: Term = 0,
    index: Index = 0,
    entry_type: EntryType = .normal,
    data: []u8 = &.{},

    pub fn clone(self: Entry, alloc: Allocator) !Entry {
        return .{
            .term = self.term,
            .index = self.index,
            .entry_type = self.entry_type,
            .data = try alloc.dupe(u8, self.data),
        };
    }

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        if (self.data.len > 0) alloc.free(self.data);
        self.* = undefined;
    }
};

pub fn entryApproxEncodedSize(entry: Entry) usize {
    // Approximate enough for batching semantics: fixed metadata plus payload.
    return 18 + entry.data.len;
}

pub fn entriesApproxEncodedSize(entries: []const Entry) usize {
    var total: usize = 0;
    for (entries) |entry| total += entryApproxEncodedSize(entry);
    return total;
}

fn isEmptyAutoLeaveConfChangeV2Payload(data: []const u8) bool {
    const encoded = [_]u8{ 2, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    return std.mem.eql(u8, data, &encoded);
}

pub fn entryPayloadSize(entry: Entry) usize {
    return payloadSizeForEntry(entry.entry_type, entry.data);
}

pub fn payloadSizeForEntry(entry_type: EntryType, data: []const u8) usize {
    return switch (entry_type) {
        .conf_change_v2 => if (isEmptyAutoLeaveConfChangeV2Payload(data)) 0 else data.len,
        else => data.len,
    };
}

pub fn entriesPayloadSize(entries: []const Entry) usize {
    var total: usize = 0;
    for (entries) |entry| total += entryPayloadSize(entry);
    return total;
}

pub fn limitEntriesByBytes(entries: []const Entry, max_bytes: usize) []const Entry {
    if (entries.len == 0) return entries;

    var size = entryApproxEncodedSize(entries[0]);
    var limit: usize = 1;
    while (limit < entries.len) : (limit += 1) {
        size += entryApproxEncodedSize(entries[limit]);
        if (size > max_bytes) return entries[0..limit];
    }
    return entries;
}

pub const HardState = struct {
    current_term: Term = 0,
    voted_for: ?NodeId = null,
    commit_index: Index = 0,

    pub fn eql(a: HardState, b: HardState) bool {
        return a.current_term == b.current_term and
            a.voted_for == b.voted_for and
            a.commit_index == b.commit_index;
    }
};

pub const SoftState = struct {
    leader_id: ?NodeId = null,
    role: StateRole = .follower,

    pub fn eql(a: SoftState, b: SoftState) bool {
        return a.leader_id == b.leader_id and a.role == b.role;
    }
};

pub const ProgressState = enum {
    probe,
    replicate,
};

pub const Progress = struct {
    match_index: Index = 0,
    next_index: Index = 1,
    state: ProgressState = .probe,
    probe_sent: bool = false,
    recent_active: bool = false,
};

pub const ConfState = struct {
    voters: []NodeId = &.{},
    voters_outgoing: []NodeId = &.{},
    learners: []NodeId = &.{},
    learners_next: []NodeId = &.{},
    auto_leave: bool = false,

    pub fn clone(self: ConfState, alloc: Allocator) !ConfState {
        return .{
            .voters = try alloc.dupe(NodeId, self.voters),
            .voters_outgoing = try alloc.dupe(NodeId, self.voters_outgoing),
            .learners = try alloc.dupe(NodeId, self.learners),
            .learners_next = try alloc.dupe(NodeId, self.learners_next),
            .auto_leave = self.auto_leave,
        };
    }

    pub fn deinit(self: *ConfState, alloc: Allocator) void {
        if (self.voters.len > 0) alloc.free(self.voters);
        if (self.voters_outgoing.len > 0) alloc.free(self.voters_outgoing);
        if (self.learners.len > 0) alloc.free(self.learners);
        if (self.learners_next.len > 0) alloc.free(self.learners_next);
        self.* = undefined;
    }
};

pub const ReadState = struct {
    index: Index,
    request_ctx: []u8,

    pub fn clone(self: ReadState, alloc: Allocator) !ReadState {
        return .{
            .index = self.index,
            .request_ctx = try alloc.dupe(u8, self.request_ctx),
        };
    }

    pub fn deinit(self: *ReadState, alloc: Allocator) void {
        if (self.request_ctx.len > 0) alloc.free(self.request_ctx);
        self.* = undefined;
    }
};

pub const SnapshotMetadata = struct {
    index: Index = 0,
    term: Term = 0,
    conf_state: ConfState = .{},

    pub fn clone(self: SnapshotMetadata, alloc: Allocator) !SnapshotMetadata {
        return .{
            .index = self.index,
            .term = self.term,
            .conf_state = try self.conf_state.clone(alloc),
        };
    }

    pub fn deinit(self: *SnapshotMetadata, alloc: Allocator) void {
        self.conf_state.deinit(alloc);
        self.* = undefined;
    }
};

pub const Snapshot = struct {
    metadata: SnapshotMetadata = .{},
    data: []u8 = &.{},

    pub fn clone(self: Snapshot, alloc: Allocator) !Snapshot {
        return .{
            .metadata = try self.metadata.clone(alloc),
            .data = try alloc.dupe(u8, self.data),
        };
    }

    pub fn deinit(self: *Snapshot, alloc: Allocator) void {
        self.metadata.deinit(alloc);
        if (self.data.len > 0) alloc.free(self.data);
        self.* = undefined;
    }
};

pub const Status = struct {
    id: NodeId,
    group_id: GroupId,
    soft: SoftState,
    hard: HardState,
    conf_state: ConfState,
    last_index: Index = 0,
    applied_index: Index = 0,
    election_elapsed: u32 = 0,
    randomized_election_timeout: u32 = 0,
    votes_granted: usize = 0,
    votes_rejected: usize = 0,
    votes_unknown: usize = 0,
};

pub fn cloneEntries(alloc: Allocator, entries: []const Entry) ![]Entry {
    const out = try alloc.alloc(Entry, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(out);
    }

    for (entries, 0..) |entry, i| {
        out[i] = try entry.clone(alloc);
        initialized += 1;
    }
    return out;
}

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    if (entries.len > 0) alloc.free(entries);
}

pub fn quorum(count: usize) usize {
    return count / 2 + 1;
}

fn readIntAt(comptime T: type, data: []const u8, cursor: usize) T {
    const int_len = @sizeOf(T);
    return std.mem.readInt(T, @ptrCast(data[cursor .. cursor + int_len].ptr), .little);
}

fn writeIntAt(comptime T: type, data: []u8, cursor: usize, value: T) void {
    const int_len = @sizeOf(T);
    std.mem.writeInt(T, @ptrCast(data[cursor .. cursor + int_len].ptr), value, .little);
}
