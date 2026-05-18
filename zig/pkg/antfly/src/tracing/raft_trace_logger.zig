// Copyright 2026 Antfly, Inc.
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
const core = raft_engine.core;

/// Implements the raft_engine TraceLogger vtable, emitting ndjson lines
/// compatible with Traceetcdraft.tla. Each line has the format:
///   {"tag":"trace","event":{"name":"...","nid":"...","state":{...},...}}
pub const RaftNdjsonTraceLogger = struct {
    mutex: std.atomic.Mutex = .unlocked,
    writer: *std.Io.Writer,
    /// Indices of self-ack messages pending receipt (sent but not yet received).
    /// Each Replicate (noop or client) pushes the post-append last_index;
    /// the next Ready-as-leader pops them as ReceiveAppendEntriesResponse events.
    pending_self_ack_indices: [16]u64 = undefined,
    pending_self_ack_count: u8 = 0,
    /// Set on BecomeCandidate; cleared after the first Ready-as-candidate
    /// emits the synthetic ReceiveRequestVoteResponse self-vote. Prevents
    /// duplicate self-vote receives across multiple Ready events in the
    /// same election.
    needs_self_vote_receive: bool = false,
    /// Commit index from the previous event. Used for pre-event synthesis
    /// where the current event's commit_index has already advanced but
    /// synthetic events need the pre-transition state.
    prev_commit_index: u64 = 0,

    pub fn traceLogger(self: *RaftNdjsonTraceLogger) core.TraceLogger {
        return .{
            .ptr = self,
            .vtable = &.{
                .trace_event = traceEvent,
            },
        };
    }

    fn traceEvent(ptr: *anyopaque, event: *const core.TraceEvent) void {
        // Skip events with unmapped message types — the TLA+ spec
        // (Traceetcdraft.tla) has no handlers for generic SendMessage/ReceiveMessage.
        if (event.event_type == .send_message or event.event_type == .receive_message) {
            if (event.message) |msg| {
                if (!isMappedMsgType(msg.msg_type)) return;
            } else return;
        }
        // Skip pre-candidate events — Traceetcdraft.tla doesn't model PreVote.
        // The actual become_candidate event fires later with the real election.
        if (event.event_type == .become_pre_candidate) return;

        const self: *RaftNdjsonTraceLogger = @ptrCast(@alignCast(ptr));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();

        // Pre-event synthesis: events that must appear BEFORE the main event.
        if (event.event_type == .become_leader and self.needs_self_vote_receive) {
            // For 1-node clusters the self-vote receive hasn't fired yet
            // (no Ready-as-candidate between BecomeCandidate and BecomeLeader).
            // The TLA+ pipeline is: SendDirect→pendingMessages, Ready→messages,
            // then Receive consumes from messages. We need a Ready to flush
            // the self-vote from pendingMessages→messages before the receive.
            self.writeSyntheticReadyWithRole(event, "StateCandidate", event.commit_index) catch {};
            self.writeSyntheticSelfVoteWithRole(event, "ReceiveRequestVoteResponse", "StateCandidate") catch {};
            self.needs_self_vote_receive = false;
        }
        if (event.event_type == .commit and event.role == .leader and self.pending_self_ack_count > 0) {
            // Commit fires before Ready in the zig engine, but TLA+ requires
            // AdvanceCommitIndex to see updated matchIndex (via self-ack).
            // Flush: Ready → ReceiveAppendEntriesResponse(s) → then Commit.
            self.writeSyntheticReadyWithRole(event, "StateLeader", self.prev_commit_index) catch {};
            for (self.pending_self_ack_indices[0..self.pending_self_ack_count]) |idx| {
                self.writeSyntheticSelfAppendReceive(event, idx, self.prev_commit_index) catch {};
            }
            self.pending_self_ack_count = 0;
        }

        self.writeEvent(event) catch {};

        // Post-event synthesis: events that follow the main event.
        switch (event.event_type) {
            .become_candidate => {
                // etcd emits self-vote send after BecomeCandidate
                self.writeSyntheticSelfVote(event, "SendRequestVoteResponse") catch {};
                self.needs_self_vote_receive = true;
            },
            .ready => {
                if (event.role == .candidate and self.needs_self_vote_receive) {
                    // etcd emits self-vote receive after Ready-as-candidate (once per election)
                    self.writeSyntheticSelfVote(event, "ReceiveRequestVoteResponse") catch {};
                    self.needs_self_vote_receive = false;
                } else if (event.role == .leader and self.pending_self_ack_count > 0) {
                    // Ready moves pendingMessages→messages. Emit one
                    // ReceiveAppendEntriesResponse per pending self-ack so
                    // HandleAppendEntriesResponse updates matchIndex.
                    for (self.pending_self_ack_indices[0..self.pending_self_ack_count]) |idx| {
                        self.writeSyntheticSelfAppendReceive(event, idx, event.commit_index) catch {};
                    }
                    self.pending_self_ack_count = 0;
                }
            },
            .become_leader => {
                // etcd emits Replicate (noop) + self-ack send after BecomeLeader.
                // The noop append doesn't go through handleProposal so the
                // engine doesn't fire .replicate — synthesize both events.
                // Pre-append state: last_index is before the noop.
                const noop_index = event.last_index + 1;
                self.writeSyntheticReplicate(event) catch {};
                self.writeSyntheticSelfAppendResp(event, noop_index) catch {};
                self.pushPendingSelfAck(noop_index);
            },
            .replicate => {
                // Client proposal: engine fires .replicate after appending.
                // Post-append state: last_index already includes the new entry.
                self.writeSyntheticSelfAppendResp(event, event.last_index) catch {};
                self.pushPendingSelfAck(event.last_index);
            },
            else => {},
        }

        self.prev_commit_index = event.commit_index;
        self.writer.flush() catch {};
    }

    fn writeEvent(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");

        // name
        try w.writeAll("\"name\":\"");
        try writeEventName(w, event);
        try w.writeAll("\"");

        // nid
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});

        // state: {term, vote, commit}
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term,
            event.vote orelse 0,
            event.commit_index,
        });

        // role
        try w.writeAll(",\"role\":\"");
        try w.writeAll(roleString(event.role));
        try w.writeAll("\"");

        // log (last index)
        try w.print(",\"log\":{d}", .{event.last_index});

        // conf: [[voters], [learners]]
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]");

        // message fields for send/receive events
        if (event.message) |msg| {
            try w.writeAll(",\"msg\":{");
            try w.print("\"type\":\"{s}\"", .{msgTypeString(msg.msg_type)});
            try w.print(",\"from\":\"{d}\"", .{msg.from});
            try w.print(",\"to\":\"{d}\"", .{msg.to});
            try w.print(",\"term\":{d}", .{msg.term});
            if (msg.msg_type == .snapshot) {
                // TLA+ models snapshots as AppendEntries with range <<1, index+1>>
                // where index is snapshot metadata index. mprevLogIndex=0, mprevLogTerm=0.
                // Use snapshot.metadata.index (not msg.log_index which may be 0 on received messages).
                const snap_index = if (msg.snapshot) |s| s.metadata.index else msg.log_index;
                try w.print(",\"logTerm\":{d}", .{@as(u64, 0)});
                try w.print(",\"index\":{d}", .{@as(u64, 0)});
                try w.print(",\"entries\":{d}", .{snap_index});
            } else {
                try w.print(",\"logTerm\":{d}", .{msg.log_term});
                try w.print(",\"index\":{d}", .{msg.log_index});
                try w.print(",\"entries\":{d}", .{msg.entries.len});
            }
            try w.print(",\"commit\":{d}", .{msg.commit_index});
            try w.print(",\"reject\":{}", .{msg.reject});
            try w.writeAll("}");
        }

        try w.writeAll("}}\n");
    }

    /// Write a synthetic self-vote event line. etcd's raft emits explicit
    /// MsgVoteResp-to-self send and receive events during elections; our raft
    /// engine handles the self-vote internally. This synthesizes the ndjson
    /// line that Traceetcdraft.tla expects.
    fn writeSyntheticSelfVote(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent, name: []const u8) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");

        // name
        try w.writeAll("\"name\":\"");
        try w.writeAll(name);
        try w.writeAll("\"");

        // nid
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});

        // state
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term,
            event.vote orelse 0,
            event.commit_index,
        });

        // role
        try w.writeAll(",\"role\":\"");
        try w.writeAll(roleString(event.role));
        try w.writeAll("\"");

        // log
        try w.print(",\"log\":{d}", .{event.last_index});

        // conf
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]");

        // msg: synthetic self-vote
        try w.writeAll(",\"msg\":{");
        try w.writeAll("\"type\":\"MsgVoteResp\"");
        try w.print(",\"from\":\"{d}\"", .{event.node_id});
        try w.print(",\"to\":\"{d}\"", .{event.node_id});
        try w.print(",\"term\":{d}", .{event.term});
        try w.writeAll(",\"logTerm\":0,\"index\":0,\"commit\":0,\"reject\":false,\"entries\":0");
        try w.writeAll("}");

        try w.writeAll("}}\n");
    }

    /// Like writeSyntheticSelfVote but overrides the role string.
    /// Used when emitting a self-vote receive from a become_leader event
    /// where the event role is already "leader" but we need "candidate".
    fn writeSyntheticSelfVoteWithRole(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent, name: []const u8, role: []const u8) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");
        try w.writeAll("\"name\":\"");
        try w.writeAll(name);
        try w.writeAll("\"");
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term, event.vote orelse 0, event.commit_index,
        });
        try w.writeAll(",\"role\":\"");
        try w.writeAll(role);
        try w.writeAll("\"");
        try w.print(",\"log\":{d}", .{event.last_index});
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]");
        try w.writeAll(",\"msg\":{");
        try w.writeAll("\"type\":\"MsgVoteResp\"");
        try w.print(",\"from\":\"{d}\"", .{event.node_id});
        try w.print(",\"to\":\"{d}\"", .{event.node_id});
        try w.print(",\"term\":{d}", .{event.term});
        try w.writeAll(",\"logTerm\":0,\"index\":0,\"commit\":0,\"reject\":false,\"entries\":0");
        try w.writeAll("}");
        try w.writeAll("}}\n");
    }

    /// Write a synthetic "Ready" event with an overridden role and commit index.
    /// Used to flush pendingMessages→messages in the TLA+ model when
    /// no real Ready event fires (e.g., 1-node cluster elections).
    fn writeSyntheticReadyWithRole(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent, role: []const u8, commit: u64) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");
        try w.writeAll("\"name\":\"Ready\"");
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term, event.vote orelse 0, commit,
        });
        try w.writeAll(",\"role\":\"");
        try w.writeAll(role);
        try w.writeAll("\"");
        try w.print(",\"log\":{d}", .{event.last_index});
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]}}\n");
    }

    /// Write a synthetic "Replicate" event (leader noop entry append).
    /// Uses the same state as BecomeLeader (pre-replicate).
    fn writeSyntheticReplicate(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");
        try w.writeAll("\"name\":\"Replicate\"");
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term, event.vote orelse 0, event.commit_index,
        });
        try w.writeAll(",\"role\":\"StateLeader\"");
        try w.print(",\"log\":{d}", .{event.last_index});
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]}}\n");
    }

    /// Write a synthetic "SendAppendEntriesResponse" self-ack.
    /// `ack_index` is the log index the self-ack reports (post-append).
    fn writeSyntheticSelfAppendResp(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent, ack_index: u64) !void {
        const w = self.writer;
        const new_log = ack_index;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");
        try w.writeAll("\"name\":\"SendAppendEntriesResponse\"");
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term, event.vote orelse 0, event.commit_index,
        });
        try w.writeAll(",\"role\":\"StateLeader\"");
        try w.print(",\"log\":{d}", .{new_log});
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]");
        // msg: self-ack for the noop entry
        try w.writeAll(",\"msg\":{");
        try w.writeAll("\"type\":\"MsgAppResp\"");
        try w.print(",\"from\":\"{d}\"", .{event.node_id});
        try w.print(",\"to\":\"{d}\"", .{event.node_id});
        try w.print(",\"term\":{d}", .{event.term});
        try w.print(",\"logTerm\":0,\"index\":{d},\"commit\":{d},\"reject\":false,\"entries\":0", .{
            new_log, event.commit_index,
        });
        try w.writeAll("}}}\n");
    }

    fn pushPendingSelfAck(self: *RaftNdjsonTraceLogger, index: u64) void {
        if (self.pending_self_ack_count < self.pending_self_ack_indices.len) {
            self.pending_self_ack_indices[self.pending_self_ack_count] = index;
            self.pending_self_ack_count += 1;
        }
    }

    /// Write a synthetic "ReceiveAppendEntriesResponse" self-ack receive.
    /// Emitted after Ready-as-leader so the spec's
    /// HandleAppendEntriesResponse updates matchIndex[leader][leader].
    fn writeSyntheticSelfAppendReceive(self: *RaftNdjsonTraceLogger, event: *const core.TraceEvent, index: u64, commit: u64) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"trace\",\"event\":{");
        try w.writeAll("\"name\":\"ReceiveAppendEntriesResponse\"");
        try w.print(",\"nid\":\"{d}\"", .{event.node_id});
        try w.print(",\"state\":{{\"term\":{d},\"vote\":\"{d}\",\"commit\":{d}}}", .{
            event.term, event.vote orelse 0, commit,
        });
        try w.writeAll(",\"role\":\"StateLeader\"");
        try w.print(",\"log\":{d}", .{event.last_index});
        try w.writeAll(",\"conf\":[");
        try writeNodeIdArray(w, event.voters);
        try w.writeAll(",");
        try writeNodeIdArray(w, event.learners);
        try w.writeAll("]");
        // msg: self-ack receive
        try w.writeAll(",\"msg\":{");
        try w.writeAll("\"type\":\"MsgAppResp\"");
        try w.print(",\"from\":\"{d}\"", .{event.node_id});
        try w.print(",\"to\":\"{d}\"", .{event.node_id});
        try w.print(",\"term\":{d}", .{event.term});
        try w.print(",\"logTerm\":0,\"index\":{d},\"commit\":{d},\"reject\":false,\"entries\":0", .{
            index, commit,
        });
        try w.writeAll("}}}\n");
    }

    fn writeEventName(w: *std.Io.Writer, event: *const core.TraceEvent) !void {
        switch (event.event_type) {
            .init_state => try w.writeAll("InitState"),
            .ready => try w.writeAll("Ready"),
            .commit => try w.writeAll("Commit"),
            .become_follower => try w.writeAll("BecomeFollower"),
            .become_pre_candidate => try w.writeAll("BecomeCandidate"),
            .become_candidate => try w.writeAll("BecomeCandidate"),
            .become_leader => try w.writeAll("BecomeLeader"),
            .replicate => try w.writeAll("Replicate"),
            .send_message => {
                if (event.message) |msg| {
                    switch (msg.msg_type) {
                        .append_entries => try w.writeAll("SendAppendEntriesRequest"),
                        .append_entries_response => try w.writeAll("SendAppendEntriesResponse"),
                        .request_vote, .pre_vote => try w.writeAll("SendRequestVoteRequest"),
                        .request_vote_response, .pre_vote_response => try w.writeAll("SendRequestVoteResponse"),
                        .heartbeat => try w.writeAll("SendAppendEntriesRequest"),
                        .heartbeat_response => try w.writeAll("SendAppendEntriesResponse"),
                        .snapshot => try w.writeAll("SendAppendEntriesRequest"),
                        else => try w.writeAll("SendMessage"),
                    }
                } else {
                    try w.writeAll("SendMessage");
                }
            },
            .receive_message => {
                if (event.message) |msg| {
                    switch (msg.msg_type) {
                        .append_entries => try w.writeAll("ReceiveAppendEntriesRequest"),
                        .append_entries_response => try w.writeAll("ReceiveAppendEntriesResponse"),
                        .request_vote, .pre_vote => try w.writeAll("ReceiveRequestVoteRequest"),
                        .request_vote_response, .pre_vote_response => try w.writeAll("ReceiveRequestVoteResponse"),
                        .snapshot => try w.writeAll("ReceiveSnapshot"),
                        .heartbeat => try w.writeAll("ReceiveAppendEntriesRequest"),
                        .heartbeat_response => try w.writeAll("ReceiveAppendEntriesResponse"),
                        else => try w.writeAll("ReceiveMessage"),
                    }
                } else {
                    try w.writeAll("ReceiveMessage");
                }
            },
        }
    }
};

/// Returns true for message types that map to TLA+ spec event names.
/// Unmapped types (propose, storage_*, timeout_now, pre_vote, etc.) are skipped.
/// PreVote messages are excluded because Traceetcdraft.tla doesn't model PreVote.
fn isMappedMsgType(msg_type: core.message.MessageType) bool {
    return switch (msg_type) {
        .append_entries,
        .append_entries_response,
        .request_vote,
        .request_vote_response,
        .heartbeat,
        .heartbeat_response,
        .snapshot,
        => true,
        else => false,
    };
}

fn roleString(role: raft_engine.core.types.StateRole) []const u8 {
    return switch (role) {
        .follower => "StateFollower",
        .pre_candidate => "StatePreCandidate",
        .candidate => "StateCandidate",
        .leader => "StateLeader",
    };
}

fn msgTypeString(msg_type: core.message.MessageType) []const u8 {
    return switch (msg_type) {
        .propose => "MsgProp",
        .pre_vote => "MsgPreVote",
        .pre_vote_response => "MsgPreVoteResp",
        .request_vote => "MsgVote",
        .request_vote_response => "MsgVoteResp",
        .append_entries => "MsgApp",
        .append_entries_response => "MsgAppResp",
        .heartbeat => "MsgHeartbeat",
        .heartbeat_response => "MsgHeartbeatResp",
        .snapshot => "MsgSnap",
        .snapshot_response => "MsgSnapStatus",
        .transfer_leader => "MsgTransferLeader",
        .forget_leader => "MsgForgetLeader",
        .timeout_now => "MsgTimeoutNow",
        .read_index => "MsgReadIndex",
        .read_index_response => "MsgReadIndexResp",
        .storage_append => "MsgStorageAppend",
        .storage_append_response => "MsgStorageAppendResp",
        .storage_apply => "MsgStorageApply",
        .storage_apply_response => "MsgStorageApplyResp",
    };
}

fn writeNodeIdArray(w: *std.Io.Writer, ids: []const raft_engine.core.types.NodeId) !void {
    try w.writeAll("[");
    for (ids, 0..) |id, i| {
        if (i > 0) try w.writeAll(",");
        try w.print("\"{d}\"", .{id});
    }
    try w.writeAll("]");
}

test "raft trace logger emits valid ndjson" {
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();

    var trace_logger = RaftNdjsonTraceLogger{
        .writer = &out.writer,
    };

    const voters = [_]raft_engine.core.types.NodeId{1};
    const empty = [_]raft_engine.core.types.NodeId{};

    const event = core.TraceEvent{
        .event_type = .init_state,
        .node_id = 1,
        .leader_id = null,
        .role = .follower,
        .term = 0,
        .vote = null,
        .commit_index = 0,
        .applied_index = 0,
        .last_index = 0,
        .voters = voters[0..],
        .voters_outgoing = empty[0..],
        .learners = empty[0..],
        .learners_next = empty[0..],
        .auto_leave = false,
    };

    trace_logger.traceLogger().traceEvent(&event);

    const output = out.written();
    try std.testing.expect(std.mem.indexOf(u8, output, "\"tag\":\"trace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"name\":\"InitState\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"nid\":\"1\"") != null);
}
