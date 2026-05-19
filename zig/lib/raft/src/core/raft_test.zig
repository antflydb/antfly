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
const raw_node_mod = @import("raw_node.zig");
const storage_mod = @import("storage.zig");
const message_mod = @import("message.zig");
const logger_mod = @import("logger.zig");
const random_mod = @import("random.zig");
const types = @import("types.zig");

fn clearMessages(raft: *raft_mod.Raft) void {
    for (raft.messages.items) |*msg| msg.deinit(std.testing.allocator);
    raft.messages.clearRetainingCapacity();
}

const LeaderFixture = struct {
    storage: storage_mod.MemoryStorage,
    raft: raft_mod.Raft,
};

const CapturedLog = struct {
    level: logger_mod.LogLevel,
    message: []u8,
};

const CaptureLogger = struct {
    alloc: std.mem.Allocator,
    records: std.ArrayListUnmanaged(CapturedLog) = .empty,

    fn logger(self: *CaptureLogger) logger_mod.Logger {
        return .{
            .ptr = self,
            .vtable = &.{
                .log = logImpl,
            },
        };
    }

    fn deinit(self: *CaptureLogger) void {
        for (self.records.items) |record| self.alloc.free(record.message);
        self.records.deinit(self.alloc);
    }

    fn logImpl(ptr: *anyopaque, level: logger_mod.LogLevel, msg: []const u8) void {
        const self: *CaptureLogger = @ptrCast(@alignCast(ptr));
        self.records.append(self.alloc, .{
            .level = level,
            .message = self.alloc.dupe(u8, msg) catch return,
        }) catch return;
    }
};

const CaptureTraceLogger = struct {
    events: std.ArrayListUnmanaged(logger_mod.TraceEventType) = .empty,

    fn traceLogger(self: *CaptureTraceLogger) logger_mod.TraceLogger {
        return .{
            .ptr = self,
            .vtable = &.{
                .trace_event = traceEventImpl,
            },
        };
    }

    fn deinit(self: *CaptureTraceLogger, alloc: std.mem.Allocator) void {
        self.events.deinit(alloc);
    }

    fn traceEventImpl(ptr: *anyopaque, event: *const logger_mod.TraceEvent) void {
        const self: *CaptureTraceLogger = @ptrCast(@alignCast(ptr));
        self.events.append(std.testing.allocator, event.event_type) catch return;
    }
};

fn initLeaderFromSnapshot() !LeaderFixture {
    return try initLeaderFromSnapshotWithMaxInflight(256);
}

fn initLeaderFromSnapshotWithMaxInflight(max_inflight_msgs: u32) !LeaderFixture {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    errdefer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.applySnapshot(.{
        .metadata = .{
            .index = 11,
            .term = 11,
            .conf_state = .{
                .voters = voters[0..],
            },
        },
        .data = &.{},
    });
    storage.setHardState(.{
        .current_term = 11,
        .commit_index = 11,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_inflight_msgs = max_inflight_msgs,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    errdefer raft.deinit();

    try raft.campaign();
    clearMessages(&raft);
    try raft.step(.{
        .msg_type = .request_vote_response,
        .from = 2,
        .to = 1,
        .term = 12,
    });
    clearMessages(&raft);

    return .{
        .storage = storage,
        .raft = raft,
    };
}

test "raft rejects zero max_inflight_msgs" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try std.testing.expectError(error.InvalidMaxInflightMsgs, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .max_inflight_msgs = 0,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));
}

test "raft rejects max_inflight_bytes smaller than max_size_per_msg" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try std.testing.expectError(error.InvalidMaxInflightBytes, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_size_per_msg = 128,
        .max_inflight_bytes = 64,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));
}

test "raft validates local identity and election ticks" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try std.testing.expectError(error.InvalidNodeId, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 0,
        .group_id = 1,
        .peers = &.{1},
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));

    try std.testing.expectError(error.InvalidLocalNodeId, raft_mod.Raft.init(std.testing.allocator, .{
        .id = message_mod.LocalAppendThread,
        .group_id = 1,
        .peers = &.{message_mod.LocalAppendThread},
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));

    try std.testing.expectError(error.InvalidHeartbeatTick, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .heartbeat_tick = 0,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));

    try std.testing.expectError(error.InvalidElectionTick, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 1,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));
}

test "raft normalizes zero no-limit defaults" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .max_uncommitted_entries_size = 0,
        .max_inflight_bytes = 0,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(std.math.maxInt(usize), raft.cfg.max_uncommitted_entries_size);
    try std.testing.expectEqual(std.math.maxInt(usize), raft.cfg.max_inflight_bytes);
}

test "raft applied restart index suppresses already-applied committed entries" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try storage.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("a"[0..1]) },
        .{ .index = 2, .term = 1, .data = @constCast("b"[0..1]) },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 2,
    });

    var raw = try raw_node_mod.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .applied = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raw.deinit();

    const rd = raw.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(@as(types.Index, 2), rd.committed_entries[0].index);
}

test "raft rejects applied restart index past committed" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try storage.append(&.{
        .{ .index = 1, .term = 1 },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 1,
    });

    try std.testing.expectError(error.InvalidApplied, raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .applied = 2,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage()));
}

test "custom trace logger observes init ready and role transitions" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var trace_logger = CaptureTraceLogger{};
    defer trace_logger.deinit(std.testing.allocator);

    var raw = try raw_node_mod.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .check_quorum = false,
        .pre_vote = false,
        .trace_logger = trace_logger.traceLogger(),
    }, storage.storage());
    defer raw.deinit();

    _ = raw.ready();
    try raw.campaign();

    try std.testing.expect(trace_logger.events.items.len >= 4);
    try std.testing.expectEqual(logger_mod.TraceEventType.init_state, trace_logger.events.items[0]);
    try std.testing.expectEqual(logger_mod.TraceEventType.ready, trace_logger.events.items[1]);
    try std.testing.expectEqual(logger_mod.TraceEventType.become_candidate, trace_logger.events.items[2]);
    try std.testing.expectEqual(logger_mod.TraceEventType.become_leader, trace_logger.events.items[3]);
}

test "custom logger records ignored stale snapshot" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try storage.append(&.{
        .{ .index = 1, .term = 1 },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 1,
    });

    var logger = CaptureLogger{ .alloc = std.testing.allocator };
    defer logger.deinit();

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .check_quorum = false,
        .pre_vote = false,
        .logger = logger.logger(),
    }, storage.storage());
    defer raft.deinit();

    var voters = [_]types.NodeId{1};
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 1,
                .term = 1,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expect(logger.records.items.len > 0);
    try std.testing.expectEqual(logger_mod.LogLevel.warning, logger.records.items[0].level);
    try std.testing.expect(std.mem.indexOf(u8, logger.records.items[0].message, "stale snapshot") != null);
}

test "snapshot failure leaves follower probing from the same index" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = true,
    };

    try fixture.raft.step(.{
        .msg_type = .snapshot_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .reject = true,
        .log_index = 11,
    });

    try std.testing.expectEqual(types.ProgressState.probe, fixture.raft.progress[1].state);
    try std.testing.expectEqual(@as(types.Index, 0), fixture.raft.progress[1].match_index);
    try std.testing.expectEqual(@as(types.Index, 1), fixture.raft.progress[1].next_index);
    try std.testing.expect(!fixture.raft.progress[1].probe_sent);
    try std.testing.expectEqual(@as(usize, 0), fixture.raft.messages.items.len);
}

test "snapshot success resumes probing from the snapshot index" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = true,
    };

    try fixture.raft.step(.{
        .msg_type = .snapshot_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .log_index = 11,
    });

    try std.testing.expectEqual(types.ProgressState.probe, fixture.raft.progress[1].state);
    try std.testing.expectEqual(@as(types.Index, 11), fixture.raft.progress[1].match_index);
    try std.testing.expectEqual(@as(types.Index, 12), fixture.raft.progress[1].next_index);
    try std.testing.expect(fixture.raft.progress[1].probe_sent);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 11), fixture.raft.messages.items[0].log_index);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items[0].entries.len);
    try std.testing.expectEqual(@as(types.Index, 12), fixture.raft.messages.items[0].entries[0].index);
}

test "append response at snapshot index aborts snapshot catch-up and resumes replicate state" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = true,
    };

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .log_index = 11,
    });

    try std.testing.expectEqual(types.ProgressState.replicate, fixture.raft.progress[1].state);
    try std.testing.expectEqual(@as(types.Index, 11), fixture.raft.progress[1].match_index);
    try std.testing.expectEqual(@as(types.Index, 13), fixture.raft.progress[1].next_index);
    try std.testing.expect(!fixture.raft.progress[1].probe_sent);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 11), fixture.raft.messages.items[0].log_index);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items[0].entries.len);
    try std.testing.expectEqual(@as(types.Index, 12), fixture.raft.messages.items[0].entries[0].index);
}

test "leader provides snapshot to active follower behind compaction" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = false,
        .recent_active = true,
    };

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .log_index = 0,
        .reject = true,
        .reject_hint = 0,
    });

    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.snapshot, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 11), fixture.raft.messages.items[0].snapshot.?.metadata.index);
}

test "leader ignores providing snapshot to inactive follower" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = false,
        .recent_active = false,
    };

    try fixture.raft.propose("somedata");

    try std.testing.expectEqual(@as(usize, 0), fixture.raft.messages.items.len);
}

test "heartbeat response from probing follower triggers empty append and restore to replicate" {
    var fixture = try initLeaderFromSnapshot();
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    const last_index = fixture.raft.log.lastIndex();
    fixture.raft.progress[1] = .{
        .match_index = last_index,
        .next_index = last_index + 1,
        .state = .probe,
        .probe_sent = true,
        .recent_active = true,
    };

    try fixture.raft.step(.{
        .msg_type = .heartbeat_response,
        .from = 2,
        .to = 1,
        .term = 12,
    });

    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, last_index), fixture.raft.messages.items[0].log_index);
    try std.testing.expectEqual(@as(usize, 0), fixture.raft.messages.items[0].entries.len);
    try std.testing.expect(!fixture.raft.progress[1].probe_sent);
    clearMessages(&fixture.raft);

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .log_index = last_index,
    });

    try std.testing.expectEqual(types.ProgressState.replicate, fixture.raft.progress[1].state);
    try std.testing.expectEqual(last_index, fixture.raft.progress[1].match_index);
    try std.testing.expectEqual(last_index + 1, fixture.raft.progress[1].next_index);
}

test "max_inflight_msgs limits replicate append pipelining until ack" {
    var fixture = try initLeaderFromSnapshotWithMaxInflight(1);
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    const last_index = fixture.raft.log.lastIndex();
    fixture.raft.progress[1] = .{
        .match_index = last_index,
        .next_index = last_index + 1,
        .state = .replicate,
        .probe_sent = false,
        .recent_active = true,
    };

    clearMessages(&fixture.raft);

    try fixture.raft.propose("first");
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.inflights[1].items.len);
    clearMessages(&fixture.raft);

    try fixture.raft.propose("second");
    try std.testing.expectEqual(@as(usize, 0), fixture.raft.messages.items.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.inflights[1].items.len);

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = fixture.raft.hard_state.current_term,
        .log_index = last_index + 1,
    });

    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.inflights[1].items.len);
    try std.testing.expectEqual(@as(types.Index, last_index + 2), fixture.raft.messages.items[0].entries[0].index);
}

test "append rejection clears inflight window and retries immediately" {
    var fixture = try initLeaderFromSnapshotWithMaxInflight(1);
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    const last_index = fixture.raft.log.lastIndex();
    fixture.raft.progress[1] = .{
        .match_index = last_index,
        .next_index = last_index + 1,
        .state = .replicate,
        .probe_sent = false,
        .recent_active = true,
    };

    clearMessages(&fixture.raft);
    try fixture.raft.propose("first");
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.inflights[1].items.len);
    clearMessages(&fixture.raft);

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = fixture.raft.hard_state.current_term,
        .reject = true,
        .log_index = last_index,
        .reject_hint = last_index,
    });

    try std.testing.expectEqual(types.ProgressState.probe, fixture.raft.progress[1].state);
    try std.testing.expectEqual(@as(usize, 0), fixture.raft.inflights[1].items.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
}

test "max_inflight_bytes limits replicate append pipelining until ack" {
    var fixture = try initLeaderFromSnapshotWithMaxInflight(8);
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.cfg.max_inflight_bytes = 2 * (18 + 1);
    const last_index = fixture.raft.log.lastIndex();
    fixture.raft.progress[1] = .{
        .match_index = last_index,
        .next_index = last_index + 1,
        .state = .replicate,
        .probe_sent = false,
        .recent_active = true,
    };

    clearMessages(&fixture.raft);

    try fixture.raft.propose("a");
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.inflights[1].items.len);
    clearMessages(&fixture.raft);

    try fixture.raft.propose("b");
    try std.testing.expectEqual(@as(usize, 2), fixture.raft.inflights[1].items.len);
    clearMessages(&fixture.raft);

    try fixture.raft.propose("c");
    try std.testing.expectEqual(@as(usize, 2), fixture.raft.inflights[1].items.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.raft.messages.items.len);

    try fixture.raft.step(.{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = fixture.raft.hard_state.current_term,
        .log_index = last_index + 1,
    });

    clearMessages(&fixture.raft);
    try fixture.raft.propose("d");
    try std.testing.expectEqual(@as(usize, 2), fixture.raft.inflights[1].items.len);
}

test "max_size_per_msg limits append batch to one entry" {
    var fixture = try initLeaderFromSnapshotWithMaxInflight(256);
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.cfg.max_size_per_msg = 1;
    const base_last_index = fixture.raft.log.lastIndex();

    try fixture.raft.propose("a");
    try fixture.raft.propose("b");
    clearMessages(&fixture.raft);
    for (fixture.raft.inflights) |*queue| queue.clearRetainingCapacity();

    fixture.raft.progress[1] = .{
        .match_index = base_last_index,
        .next_index = base_last_index + 1,
        .state = .replicate,
        .probe_sent = false,
        .recent_active = true,
    };

    try fixture.raft.propose("c");

    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.append_entries, fixture.raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.messages.items[0].entries.len);
    try std.testing.expectEqual(@as(types.Index, base_last_index + 1), fixture.raft.messages.items[0].entries[0].index);
}

test "max_uncommitted_entries_size drops new proposals until committed entries advance" {
    var fixture = try initLeaderFromSnapshotWithMaxInflight(256);
    defer fixture.raft.deinit();
    defer fixture.storage.deinit();

    fixture.raft.cfg.max_uncommitted_entries_size = 1;

    try fixture.raft.propose("ab");
    try std.testing.expectEqual(@as(usize, 2), fixture.raft.uncommitted_size);
    try std.testing.expectError(error.ProposalDropped, fixture.raft.propose("c"));

    fixture.raft.log.commitTo(fixture.raft.log.lastIndex());
    fixture.raft.hard_state.commit_index = fixture.raft.log.committed;
    const rd = fixture.raft.ready();
    fixture.raft.advance(rd);

    try std.testing.expectEqual(@as(usize, 0), fixture.raft.uncommitted_size);
    try fixture.raft.propose("d");
    try std.testing.expectEqual(@as(usize, 1), fixture.raft.uncommitted_size);
}

test "max_committed_size_per_ready paginates committed entries without gaps" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_committed_size_per_ready = 2 * (18 + 1),
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try raft.campaign();
    var rd = raft.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    raft.advance(rd);

    try raft.propose("a");
    try raft.propose("b");
    try raft.propose("c");

    rd = raft.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectEqual(@as(types.Index, 2), rd.committed_entries[0].index);
    try std.testing.expectEqual(@as(types.Index, 3), rd.committed_entries[1].index);
    raft.advance(rd);

    rd = raft.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(@as(types.Index, 4), rd.committed_entries[0].index);
    raft.advance(rd);
}

test "disable_conf_change_validation allows leave-joint proposal past pending unapplied joint config" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var disabled = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_committed_size_per_ready = 1,
        .check_quorum = false,
        .pre_vote = false,
        .disable_conf_change_validation = true,
    }, storage.storage());
    defer disabled.deinit();

    try disabled.campaign();
    var rd = disabled.ready();
    disabled.advance(rd);

    var changes = [_]types.ConfChangeSingle{
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    };

    try disabled.propose("foo");
    try disabled.proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = changes[0..],
    });

    rd = disabled.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(types.EntryType.normal, rd.committed_entries[0].entry_type);
    disabled.advance(rd);

    try disabled.proposeConfChangeV2(.{});

    rd = disabled.ready();
    try std.testing.expect(rd.entries.len > 0);
    try std.testing.expectEqual(types.EntryType.conf_change_v2, rd.entries[rd.entries.len - 1].entry_type);
    try std.testing.expectEqual(disabled.log.lastIndex(), disabled.pending_conf_index);

    var strict_storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer strict_storage.deinit();

    var strict = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_committed_size_per_ready = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, strict_storage.storage());
    defer strict.deinit();

    try strict.campaign();
    rd = strict.ready();
    strict.advance(rd);

    try strict.propose("foo");
    try strict.proposeConfChangeV2(.{
        .transition = .joint_explicit,
        .changes = changes[0..],
    });

    rd = strict.ready();
    strict.advance(rd);

    try std.testing.expectError(error.PendingConfChange, strict.proposeConfChangeV2(.{}));
}

test "memory storage compaction preserves snapshot term and trimmed bounds" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 2 },
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
    });

    try storage.compactTo(3, .{
        .voters = voters[0..],
    });

    try std.testing.expectEqual(@as(types.Index, 4), try storage.storage().firstIndex());
    try std.testing.expectEqual(@as(types.Index, 5), try storage.storage().lastIndex());
    try std.testing.expectEqual(@as(types.Term, 3), try storage.storage().term(3));
    try std.testing.expectError(error.IndexNotFound, storage.storage().term(2));

    const entries = try storage.storage().entries(std.testing.allocator, 4, 6, 0);
    defer types.freeEntries(std.testing.allocator, entries);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqual(@as(types.Index, 4), entries[0].index);
    try std.testing.expectEqual(@as(types.Index, 5), entries[1].index);
}

test "pre-vote rejection uses local term while granted pre-vote uses requested term" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    storage.setHardState(.{
        .current_term = 4,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft.deinit();

    try raft.step(.{
        .msg_type = .pre_vote,
        .from = 1,
        .to = 2,
        .term = 3,
        .log_index = 0,
        .log_term = 0,
    });

    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.pre_vote_response, raft.messages.items[0].msg_type);
    try std.testing.expect(raft.messages.items[0].reject);
    try std.testing.expectEqual(@as(types.Term, 4), raft.messages.items[0].term);
    clearMessages(&raft);

    try raft.step(.{
        .msg_type = .pre_vote,
        .from = 1,
        .to = 2,
        .term = 5,
        .log_index = 0,
        .log_term = 0,
    });

    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.pre_vote_response, raft.messages.items[0].msg_type);
    try std.testing.expect(!raft.messages.items[0].reject);
    try std.testing.expectEqual(@as(types.Term, 5), raft.messages.items[0].term);
}

test "higher-term rejected pre-vote response steps pre-candidate down to follower" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft.deinit();

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.pre_candidate, raft.soft_state.role);
    clearMessages(&raft);

    try raft.step(.{
        .msg_type = .pre_vote_response,
        .from = 3,
        .to = 2,
        .term = 3,
        .reject = true,
    });

    try std.testing.expectEqual(types.StateRole.follower, raft.soft_state.role);
    try std.testing.expectEqual(@as(types.Term, 3), raft.hard_state.current_term);
    try std.testing.expectEqual(@as(?types.NodeId, null), raft.soft_state.leader_id);
    try std.testing.expectEqual(@as(?types.NodeId, null), raft.hard_state.voted_for);
}

test "seeded randomized election timeout is deterministic and bounded" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft_a = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .random_seed = 42,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft_a.deinit();

    var raft_b = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .random_seed = 42,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft_b.deinit();

    var raft_fixed = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft_fixed.deinit();

    try std.testing.expect(raft_a.randomized_election_timeout >= raft_a.cfg.election_tick);
    try std.testing.expect(raft_a.randomized_election_timeout < raft_a.cfg.election_tick * 2);
    try std.testing.expectEqual(raft_a.randomized_election_timeout, raft_b.randomized_election_timeout);
    try std.testing.expectEqual(raft_fixed.cfg.election_tick, raft_fixed.randomized_election_timeout);
}

test "raft accepts custom random source for election timeout jitter" {
    const SequenceRandom = struct {
        value: u32,

        fn source(self: *@This()) random_mod.RandomSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .next_below = nextBelow,
                },
            };
        }

        fn nextBelow(ptr: *anyopaque, upper_exclusive: u32) u32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.value % upper_exclusive;
        }
    };

    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var random = SequenceRandom{ .value = 2 };
    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .random_source = random.source(),
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(@as(u32, 5), raft.randomized_election_timeout);
}

test "pre-candidate timeout resends pre-vote campaign after timeout elapses" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = true,
    }, storage.storage());
    defer raft.deinit();

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.pre_candidate, raft.soft_state.role);
    try std.testing.expectEqual(@as(usize, 2), raft.messages.items.len);
    clearMessages(&raft);
    raft.randomized_election_timeout = 4;

    raft.tick();
    raft.tick();
    raft.tick();

    try std.testing.expectEqual(types.StateRole.pre_candidate, raft.soft_state.role);
    try std.testing.expectEqual(@as(usize, 0), raft.messages.items.len);

    raft.tick();

    try std.testing.expectEqual(@as(usize, 2), raft.messages.items.len);
}

test "committed unapplied conf change blocks local campaign" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    const encoded = try (types.ConfChange{
        .change_type = .remove_node,
        .node_id = 2,
    }).encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try storage.append(&.{
        .{
            .index = 1,
            .term = 1,
            .entry_type = .conf_change,
            .data = encoded,
        },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(@as(types.Index, 0), raft.log.applied);
    try std.testing.expect(raft.status().conf_state.voters.len == 3);
    raft.log.commitTo(1);
    raft.hard_state.commit_index = 1;
    try std.testing.expectError(error.NotPromotable, raft.campaign());
    try std.testing.expectEqual(types.StateRole.follower, raft.soft_state.role);
}

test "committed unapplied conf change causes timeout_now to be ignored" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    var conf_change_v2 = types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(types.ConfChangeSingle, &.{
            .{
                .change_type = .remove_node,
                .node_id = 2,
            },
        }),
    };
    defer conf_change_v2.deinit(std.testing.allocator);
    const encoded = try conf_change_v2.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try storage.append(&.{
        .{
            .index = 1,
            .term = 1,
            .entry_type = .conf_change_v2,
            .data = encoded,
        },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expect(raft.status().conf_state.voters.len == 3);
    raft.log.commitTo(1);
    raft.hard_state.commit_index = 1;
    try raft.step(.{
        .msg_type = .timeout_now,
        .from = 1,
        .to = 2,
        .term = 1,
    });

    try std.testing.expectEqual(types.StateRole.follower, raft.soft_state.role);
    try std.testing.expectEqual(@as(usize, 0), raft.messages.items.len);
}

test "committed unapplied conf change adding the local node allows campaign" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    const encoded = try (types.ConfChange{
        .change_type = .add_node,
        .node_id = 2,
    }).encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try storage.append(&.{
        .{
            .index = 1,
            .term = 1,
            .entry_type = .conf_change,
            .data = encoded,
        },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(@as(usize, 2), raft.status().conf_state.voters.len);
    raft.log.commitTo(1);
    raft.hard_state.commit_index = 1;
    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
    try std.testing.expectEqual(@as(types.Term, 2), raft.hard_state.current_term);
    try std.testing.expectEqual(@as(?types.NodeId, 2), raft.hard_state.voted_for);
    try std.testing.expectEqual(@as(usize, 2), raft.messages.items.len);
}

test "committed unapplied conf change adding the local node allows timeout_now election" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    var conf_change_v2 = types.ConfChangeV2{
        .changes = try std.testing.allocator.dupe(types.ConfChangeSingle, &.{
            .{
                .change_type = .add_node,
                .node_id = 2,
            },
        }),
    };
    defer conf_change_v2.deinit(std.testing.allocator);
    const encoded = try conf_change_v2.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try storage.append(&.{
        .{
            .index = 1,
            .term = 1,
            .entry_type = .conf_change_v2,
            .data = encoded,
        },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 3,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(@as(usize, 2), raft.status().conf_state.voters.len);
    raft.log.commitTo(1);
    raft.hard_state.commit_index = 1;
    try raft.step(.{
        .msg_type = .timeout_now,
        .from = 1,
        .to = 2,
        .term = 1,
    });

    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
    try std.testing.expectEqual(@as(types.Term, 2), raft.hard_state.current_term);
    try std.testing.expectEqual(@as(usize, 2), raft.messages.items.len);
}

test "learner can vote when it receives a valid request_vote" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{1};
    var learners = [_]types.NodeId{2};
    try storage.seedConfState(.{
        .voters = voters[0..],
        .learners = learners[0..],
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 0,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try raft.step(.{
        .msg_type = .request_vote,
        .from = 1,
        .to = 2,
        .term = 2,
        .log_term = 11,
        .log_index = 11,
    });

    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.request_vote_response, raft.messages.items[0].msg_type);
    try std.testing.expect(!raft.messages.items[0].reject);
}

test "restoring snapshot with learners preserves learner membership" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    var learners = [_]types.NodeId{3};
    try storage.applySnapshot(.{
        .metadata = .{
            .index = 11,
            .term = 11,
            .conf_state = .{
                .voters = voters[0..],
                .learners = learners[0..],
            },
        },
        .data = &.{},
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 3,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 8,
        .heartbeat_tick = 2,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectEqual(@as(types.Index, 11), raft.log.lastIndex());
    try std.testing.expectEqual(@as(types.Term, 11), raft.log.term(11).?);
    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2 }, raft.status().conf_state.voters);
    try std.testing.expectEqualSlices(types.NodeId, &.{3}, raft.status().conf_state.learners);
    try std.testing.expectError(error.NotPromotable, raft.campaign());
}

test "snapshot can restore a voter into learner state" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2, 3 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 3,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 1, 2 };
    var snapshot_learners = [_]types.NodeId{3};
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 1,
        .to = 3,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                    .learners = snapshot_learners[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2 }, raft.status().conf_state.voters);
    try std.testing.expectEqualSlices(types.NodeId, &.{3}, raft.status().conf_state.learners);
    try std.testing.expectError(error.NotPromotable, raft.campaign());
}

test "snapshot can promote learner into voter state" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    var learners = [_]types.NodeId{3};
    try storage.seedConfState(.{
        .voters = voters[0..],
        .learners = learners[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 3,
        .group_id = 1,
        .peers = &.{ 1, 2, 3 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try std.testing.expectError(error.NotPromotable, raft.campaign());

    var snapshot_voters = [_]types.NodeId{ 1, 2, 3 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 1,
        .to = 3,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2, 3 }, raft.status().conf_state.voters);
    try std.testing.expectEqual(@as(usize, 0), raft.status().conf_state.learners.len);
    try std.testing.expectError(error.NotPromotable, raft.campaign());

    const rd = raft.ready();
    try std.testing.expect(rd.snapshot != null);
    raft.advance(rd);

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
}

test "snapshot with outgoing voters blocks campaign until applied" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 2, 3, 4 };
    var snapshot_outgoing = [_]types.NodeId{ 1, 2, 3 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                    .voters_outgoing = snapshot_outgoing[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqualSlices(types.NodeId, &.{ 2, 3, 4 }, raft.status().conf_state.voters);
    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2, 3 }, raft.status().conf_state.voters_outgoing);
    try std.testing.expectError(error.NotPromotable, raft.campaign());

    const rd = raft.ready();
    try std.testing.expect(rd.snapshot != null);
    raft.advance(rd);

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
}

test "check_quorum snapshot with outgoing voters blocks campaign until applied" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 2, 3, 4 };
    var snapshot_outgoing = [_]types.NodeId{ 1, 2, 3 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                    .voters_outgoing = snapshot_outgoing[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqualSlices(types.NodeId, &.{ 2, 3, 4 }, raft.status().conf_state.voters);
    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2, 3 }, raft.status().conf_state.voters_outgoing);
    try std.testing.expectEqual(types.StateRole.follower, raft.status().soft.role);
    try std.testing.expectError(error.NotPromotable, raft.campaign());

    const rd = raft.ready();
    try std.testing.expect(rd.snapshot != null);
    raft.advance(rd);

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
}

test "check_quorum snapshot with outgoing voters ignores timeout_now until applied" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 2, 3, 4 };
    var snapshot_outgoing = [_]types.NodeId{ 1, 2, 3 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                    .voters_outgoing = snapshot_outgoing[0..],
                },
            },
            .data = &.{},
        },
    });

    try raft.step(.{
        .msg_type = .timeout_now,
        .from = 2,
        .to = 1,
        .term = 1,
    });
    try std.testing.expectEqual(types.StateRole.follower, raft.soft_state.role);

    const rd = raft.ready();
    try std.testing.expect(rd.snapshot != null);
    raft.advance(rd);

    try raft.step(.{
        .msg_type = .timeout_now,
        .from = 2,
        .to = 1,
        .term = 1,
    });
    try std.testing.expectEqual(types.StateRole.candidate, raft.soft_state.role);
}

test "pre_vote check_quorum snapshot with outgoing voters blocks campaign until applied" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = true,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 2, 3, 4 };
    var snapshot_outgoing = [_]types.NodeId{ 1, 2, 3 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                    .voters_outgoing = snapshot_outgoing[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectError(error.NotPromotable, raft.campaign());

    const rd = raft.ready();
    try std.testing.expect(rd.snapshot != null);
    raft.advance(rd);

    try raft.campaign();
    try std.testing.expectEqual(types.StateRole.pre_candidate, raft.soft_state.role);
}

test "obsolete snapshot is ignored and does not replace local state" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 3,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 2,
                .term = 1,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqual(@as(types.Index, 3), raft.log.committed);
    try std.testing.expectEqual(@as(types.Index, 3), raft.log.lastIndex());
    try std.testing.expect(raft.pending_snapshot == null);
    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.snapshot_response, raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 3), raft.messages.items[0].log_index);
}

test "matching snapshot fast-forwards commit without replacing local log" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });
    try storage.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
    });
    storage.setHardState(.{
        .current_term = 1,
        .commit_index = 1,
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 2,
                .term = 1,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqual(@as(types.Index, 2), raft.log.committed);
    try std.testing.expectEqual(@as(types.Index, 3), raft.log.lastIndex());
    try std.testing.expect(raft.pending_snapshot == null);
    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.snapshot_response, raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 2), raft.messages.items[0].log_index);
}

test "snapshot that does not include local node is ignored" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{ 1, 2, 3, 4 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    var snapshot_voters = [_]types.NodeId{ 2, 3, 4 };
    try raft.step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 1,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = snapshot_voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqualSlices(types.NodeId, &.{ 1, 2 }, raft.status().conf_state.voters);
    try std.testing.expectEqual(@as(types.Index, 0), raft.log.committed);
    try std.testing.expect(raft.pending_snapshot == null);
    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.snapshot_response, raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 0), raft.messages.items[0].log_index);
}

test "stepping a snapshot message restores follower state and leader" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var voters = [_]types.NodeId{ 1, 2 };
    try storage.seedConfState(.{
        .voters = voters[0..],
    });

    var raft = try raft_mod.Raft.init(std.testing.allocator, .{
        .id = 2,
        .group_id = 1,
        .peers = &.{ 1, 2 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
    }, storage.storage());
    defer raft.deinit();

    try raft.step(.{
        .msg_type = .snapshot,
        .from = 1,
        .to = 2,
        .term = 2,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqual(@as(?types.NodeId, 1), raft.status().soft.leader_id);
    try std.testing.expectEqual(types.StateRole.follower, raft.status().soft.role);
    try std.testing.expectEqual(@as(types.Term, 2), raft.status().hard.current_term);
    try std.testing.expectEqual(@as(types.Index, 11), raft.log.committed);
    try std.testing.expect(raft.pending_snapshot != null);
    try std.testing.expectEqual(@as(usize, 1), raft.messages.items.len);
    try std.testing.expectEqual(message_mod.MessageType.snapshot_response, raft.messages.items[0].msg_type);
    try std.testing.expectEqual(@as(types.Index, 11), raft.messages.items[0].log_index);
}

test "raw node async storage writes emits local append then local apply" {
    var storage = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var node = try raw_node_mod.RawNode.init(std.testing.allocator, .{
        .id = 1,
        .group_id = 1,
        .peers = &.{1},
        .election_tick = 10,
        .heartbeat_tick = 1,
        .check_quorum = false,
        .pre_vote = false,
        .async_storage_writes = true,
    }, storage.storage());
    defer node.deinit();

    try node.campaign();

    var rd = node.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.entries.len);
    try std.testing.expectEqual(@as(usize, 0), rd.committed_entries.len);
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expectEqual(message_mod.MessageType.storage_append, rd.messages[0].msg_type);
    try std.testing.expectEqual(message_mod.LocalAppendThread, rd.messages[0].to);
    try std.testing.expectEqual(@as(usize, 1), rd.messages[0].entries.len);

    storage.setHardState(.{
        .current_term = rd.messages[0].term,
        .voted_for = rd.messages[0].vote,
        .commit_index = rd.messages[0].commit_index,
    });
    try storage.append(rd.messages[0].entries);
    for (rd.messages[0].responses) |response| {
        try node.step(response);
    }

    rd = node.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
    try std.testing.expectEqual(@as(types.Index, 1), rd.committed_entries[0].index);
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expectEqual(message_mod.MessageType.storage_apply, rd.messages[0].msg_type);
    try std.testing.expectEqual(message_mod.LocalApplyThread, rd.messages[0].to);
}
