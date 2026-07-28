// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;

/// Sparse facts shared by implementation-bound protocol trace families.
/// Fields are intentionally concrete observations; the extraction layer owns
/// any bounded-model normalization.
pub const ProtocolTraceFacts = struct {
    group_id: ?u64 = null,
    store_id: ?u64 = null,
    index_name: ?[]const u8 = null,
    phase: ?[]const u8 = null,
    reason: ?[]const u8 = null,
    fingerprint: ?[]const u8 = null,
    repair_id: ?u128 = null,
    config_hash: ?u64 = null,
    raft_term: ?u64 = null,
    membership_index: ?u64 = null,
    voter_count: ?u64 = null,
    expected_voters: ?u64 = null,
    healthy_voter_reports: ?u64 = null,
    applied_sequence: ?u64 = null,
    target_sequence: ?u64 = null,
    last_sequence: ?u64 = null,
    next_retry_at_ms: ?u64 = null,
    scanned_entries: ?u64 = null,
    matched_entries: ?u64 = null,
    filtered_entries: ?u64 = null,
    pending_work: ?u64 = null,
    lease_epoch: ?u64 = null,
    owner_id: ?[]const u8 = null,
    local_leader: ?bool = null,
    leader_known: ?bool = null,
    leader_placed: ?bool = null,
    voter_count_known: ?bool = null,
    voter_set_known: ?bool = null,
    ambiguous: ?bool = null,
    joint_consensus: ?bool = null,
    stable_placement: ?bool = null,
    durable_work: ?bool = null,
    worker_admitted: ?bool = null,
    fallback_used: ?bool = null,
    lease_valid: ?bool = null,
    applied: ?bool = null,
    retrying: ?bool = null,
    worker_failed: ?bool = null,
};

pub const ProtocolTracingEvent = struct {
    family: []const u8,
    trace_id: u128,
    name: []const u8,
    facts: ProtocolTraceFacts = .{},
};

pub const ProtocolTraceWriter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        trace_event: *const fn (ptr: *anyopaque, event: *const ProtocolTracingEvent) void,
    };

    pub fn traceEvent(self: ProtocolTraceWriter, event: *const ProtocolTracingEvent) void {
        self.vtable.trace_event(self.ptr, event);
    }
};

/// Writes one self-contained NDJSON line per lifecycle observation:
/// {"tag":"<family>-trace","family":"...", ...}
pub const ProtocolNdjsonTraceWriter = struct {
    mutex: std.atomic.Mutex = .unlocked,
    sequence: std.atomic.Value(u64) = .init(0),
    writer: *std.Io.Writer,

    pub fn traceWriter(self: *ProtocolNdjsonTraceWriter) ProtocolTraceWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .trace_event = traceEvent },
        };
    }

    fn traceEvent(ptr: *anyopaque, event: *const ProtocolTracingEvent) void {
        const self: *ProtocolNdjsonTraceWriter = @ptrCast(@alignCast(ptr));
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const seq = self.sequence.fetchAdd(1, .monotonic) + 1;
        self.writeEvent(seq, event) catch {};
        self.writer.flush() catch {};
    }

    fn writeEvent(self: *ProtocolNdjsonTraceWriter, seq: u64, event: *const ProtocolTracingEvent) !void {
        const w = self.writer;
        try w.writeAll("{\"tag\":\"");
        try w.print("{s}-trace\",\"family\":", .{event.family});
        try writeJsonString(w, event.family);
        try w.print(",\"traceId\":\"{x}\",\"seq\":{d},\"event\":{{\"name\":", .{ event.trace_id, seq });
        try writeJsonString(w, event.name);
        try w.writeAll(",\"facts\":{");

        var first = true;
        try optionalU64(w, &first, "groupId", event.facts.group_id);
        try optionalU64(w, &first, "storeId", event.facts.store_id);
        try optionalString(w, &first, "indexName", event.facts.index_name);
        try optionalString(w, &first, "phase", event.facts.phase);
        try optionalString(w, &first, "reason", event.facts.reason);
        try optionalString(w, &first, "fingerprint", event.facts.fingerprint);
        try optionalU128String(w, &first, "repairId", event.facts.repair_id);
        // Opaque hashes may exceed TLC/Java's signed 64-bit JSON range.
        try optionalU64String(w, &first, "configHash", event.facts.config_hash);
        try optionalU64(w, &first, "raftTerm", event.facts.raft_term);
        try optionalU64(w, &first, "membershipIndex", event.facts.membership_index);
        try optionalU64(w, &first, "voterCount", event.facts.voter_count);
        try optionalU64(w, &first, "expectedVoters", event.facts.expected_voters);
        try optionalU64(w, &first, "healthyVoterReports", event.facts.healthy_voter_reports);
        try optionalU64(w, &first, "appliedSequence", event.facts.applied_sequence);
        try optionalU64(w, &first, "targetSequence", event.facts.target_sequence);
        try optionalU64(w, &first, "lastSequence", event.facts.last_sequence);
        try optionalU64(w, &first, "nextRetryAtMs", event.facts.next_retry_at_ms);
        try optionalU64(w, &first, "scannedEntries", event.facts.scanned_entries);
        try optionalU64(w, &first, "matchedEntries", event.facts.matched_entries);
        try optionalU64(w, &first, "filteredEntries", event.facts.filtered_entries);
        try optionalU64(w, &first, "pendingWork", event.facts.pending_work);
        try optionalU64(w, &first, "leaseEpoch", event.facts.lease_epoch);
        try optionalString(w, &first, "ownerId", event.facts.owner_id);
        try optionalBool(w, &first, "localLeader", event.facts.local_leader);
        try optionalBool(w, &first, "leaderKnown", event.facts.leader_known);
        try optionalBool(w, &first, "leaderPlaced", event.facts.leader_placed);
        try optionalBool(w, &first, "voterCountKnown", event.facts.voter_count_known);
        try optionalBool(w, &first, "voterSetKnown", event.facts.voter_set_known);
        try optionalBool(w, &first, "ambiguous", event.facts.ambiguous);
        try optionalBool(w, &first, "jointConsensus", event.facts.joint_consensus);
        try optionalBool(w, &first, "stablePlacement", event.facts.stable_placement);
        try optionalBool(w, &first, "durableWork", event.facts.durable_work);
        try optionalBool(w, &first, "workerAdmitted", event.facts.worker_admitted);
        try optionalBool(w, &first, "fallbackUsed", event.facts.fallback_used);
        try optionalBool(w, &first, "leaseValid", event.facts.lease_valid);
        try optionalBool(w, &first, "applied", event.facts.applied);
        try optionalBool(w, &first, "retrying", event.facts.retrying);
        try optionalBool(w, &first, "workerFailed", event.facts.worker_failed);
        try w.writeAll("}}}\n");
    }
};

fn separator(w: *std.Io.Writer, first: *bool) !void {
    if (!first.*) try w.writeByte(',');
    first.* = false;
}

fn optionalU64(w: *std.Io.Writer, first: *bool, name: []const u8, value: ?u64) !void {
    const v = value orelse return;
    try separator(w, first);
    try w.print("\"{s}\":{d}", .{ name, v });
}

fn optionalU128String(w: *std.Io.Writer, first: *bool, name: []const u8, value: ?u128) !void {
    const v = value orelse return;
    try separator(w, first);
    try w.print("\"{s}\":\"{x}\"", .{ name, v });
}

fn optionalU64String(w: *std.Io.Writer, first: *bool, name: []const u8, value: ?u64) !void {
    const v = value orelse return;
    try separator(w, first);
    try w.print("\"{s}\":\"{x}\"", .{ name, v });
}

fn optionalBool(w: *std.Io.Writer, first: *bool, name: []const u8, value: ?bool) !void {
    const v = value orelse return;
    try separator(w, first);
    try w.print("\"{s}\":{s}", .{ name, if (v) "true" else "false" });
}

fn optionalString(w: *std.Io.Writer, first: *bool, name: []const u8, value: ?[]const u8) !void {
    const v = value orelse return;
    try separator(w, first);
    try w.print("\"{s}\":", .{name});
    try writeJsonString(w, v);
}

fn writeJsonString(w: *std.Io.Writer, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => if (c >= 0x20) try w.writeByte(c),
    };
    try w.writeByte('"');
}

test "protocol trace writer emits concrete lifecycle facts" {
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    var writer: ProtocolNdjsonTraceWriter = .{ .writer = &out.writer };
    writer.traceWriter().traceEvent(&.{
        .family = "placement-readiness",
        .trace_id = 42,
        .name = "RecomputeEvidence",
        .facts = .{
            .group_id = 42,
            .voter_count = 3,
            .voter_count_known = true,
            .stable_placement = true,
        },
    });
    const output = out.written();
    try std.testing.expect(std.mem.indexOf(u8, output, "\"tag\":\"placement-readiness-trace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"family\":\"placement-readiness\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"stablePlacement\":true") != null);
}
