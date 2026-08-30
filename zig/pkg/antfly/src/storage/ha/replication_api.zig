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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Transport-agnostic HA replication API.
//!
//! This module is the storage-facing contract for the future streaming
//! transport. HTTP handlers, CLI commands, and operators can expose these verbs
//! directly: identify the primary, create a slot, start streaming from a slot
//! LSN, and report standby progress.

const std = @import("std");
const Allocator = std.mem.Allocator;
const primary_mod = @import("primary.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

var test_path_counter: u64 = 0;

pub const Command = enum {
    identify_system,
    create_replication_slot,
    pause_replication_slot,
    resume_replication_slot,
    drop_replication_slot,
    start_replication,
    standby_status_update,
};

pub const IdentifySystemResponse = struct {
    identity: primary_mod.Identity,
    current_lsn: u64,
    next_lsn: u64,
    record_format_version: u16,
};

pub const CreateReplicationSlotRequest = struct {
    slot_name: []const u8,
    initial_lsn: ?u64 = null,
};

pub const CreateReplicationSlotResponse = struct {
    slot_name: []const u8,
    timeline_id: u64,
    restart_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    active: bool,
    reseed_required: bool,
    last_error: ?[]const u8 = null,
    current_lsn: u64,
};

pub const SlotLifecycleRequest = struct {
    slot_name: []const u8,
};

pub const SlotLifecycleResponse = struct {
    slot_name: []const u8,
    timeline_id: u64,
    restart_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    active: bool,
    reseed_required: bool,
    last_error: ?[]const u8 = null,
    current_lsn: u64,
    dropped: bool = false,
};

pub const StartReplicationRequest = struct {
    slot_name: []const u8,
    from_lsn: u64,
    max_records: usize = 0,
    max_encoded_bytes: usize = 0,
};

pub const ReplicationFrame = struct {
    lsn: u64,
    kind: replication_record.RecordKind,
    payload_codec: replication_record.PayloadCodec,
    encoded: []const u8,

    fn deinit(self: *ReplicationFrame, alloc: Allocator) void {
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

pub const StartReplicationResponse = struct {
    slot_name: []const u8,
    identity: primary_mod.Identity,
    record_format_version: u16,
    timeline_id: u64,
    from_lsn: u64,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
    end_of_wal: bool,
    encoded_bytes: usize,
    records: []ReplicationFrame,

    pub fn deinit(self: *StartReplicationResponse, alloc: Allocator) void {
        for (self.records) |*record| record.deinit(alloc);
        alloc.free(self.records);
        alloc.free(self.slot_name);
        self.* = undefined;
    }
};

pub const StandbyStatusUpdateRequest = struct {
    slot_name: []const u8,
    timeline_id: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: ?u64 = null,
};

pub const StandbyStatusUpdateResponse = struct {
    slot_name: []const u8,
    timeline_id: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    restart_lsn: u64,
    active: bool,
    reseed_required: bool,
    last_error: ?[]const u8 = null,
    current_lsn: u64,
};

pub fn identifySystem(primary: *const primary_mod.Primary) IdentifySystemResponse {
    return .{
        .identity = primary.identity,
        .current_lsn = primary.lastLsn(),
        .next_lsn = primary.nextLsn(),
        .record_format_version = replication_record.format_version,
    };
}

pub fn createReplicationSlot(
    primary: *primary_mod.Primary,
    request: CreateReplicationSlotRequest,
) !CreateReplicationSlotResponse {
    try validateSlotName(request.slot_name);
    const initial_lsn = request.initial_lsn orelse primary.lastLsn();
    if (initial_lsn > primary.lastLsn()) return error.InitialLsnAheadOfPrimary;
    try primary.createSlot(request.slot_name, initial_lsn);
    const slot = primary.slot(request.slot_name) orelse return error.SlotNotFound;
    return .{
        .slot_name = slot.name,
        .timeline_id = slot.timeline_id,
        .restart_lsn = slot.restart_lsn,
        .received_lsn = slot.received_lsn,
        .applied_lsn = slot.applied_lsn,
        .safe_read_lsn = slot.safe_read_lsn,
        .active = slot.active,
        .reseed_required = slot.reseed_required,
        .last_error = slot.last_error,
        .current_lsn = primary.lastLsn(),
    };
}

pub fn pauseReplicationSlot(
    primary: *primary_mod.Primary,
    request: SlotLifecycleRequest,
) !SlotLifecycleResponse {
    try validateSlotName(request.slot_name);
    try primary.pauseSlot(request.slot_name);
    return try lifecycleResponse(primary, request.slot_name, false);
}

pub fn resumeReplicationSlot(
    primary: *primary_mod.Primary,
    request: SlotLifecycleRequest,
) !SlotLifecycleResponse {
    try validateSlotName(request.slot_name);
    try primary.resumeSlot(request.slot_name);
    return try lifecycleResponse(primary, request.slot_name, false);
}

pub fn dropReplicationSlot(
    primary: *primary_mod.Primary,
    request: SlotLifecycleRequest,
) !SlotLifecycleResponse {
    try validateSlotName(request.slot_name);
    const slot = primary.slot(request.slot_name) orelse return error.SlotNotFound;
    const response = SlotLifecycleResponse{
        .slot_name = request.slot_name,
        .timeline_id = slot.timeline_id,
        .restart_lsn = slot.restart_lsn,
        .received_lsn = slot.received_lsn,
        .applied_lsn = slot.applied_lsn,
        .safe_read_lsn = slot.safe_read_lsn,
        .active = slot.active,
        .reseed_required = slot.reseed_required,
        .last_error = null,
        .current_lsn = primary.lastLsn(),
        .dropped = true,
    };
    try primary.dropSlot(request.slot_name);
    return response;
}

pub fn startReplication(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    request: StartReplicationRequest,
) !StartReplicationResponse {
    try validateSlotName(request.slot_name);
    if (request.from_lsn == 0) return error.InvalidReplicationStartLsn;
    const slot = primary.slot(request.slot_name) orelse return error.SlotNotFound;

    const entries = try primary.streamFrom(alloc, request.slot_name, request.from_lsn);
    defer replication_log.freeEntries(alloc, entries);

    var frames = std.ArrayListUnmanaged(ReplicationFrame).empty;
    errdefer {
        for (frames.items) |*frame| frame.deinit(alloc);
        frames.deinit(alloc);
    }

    var encoded_bytes: usize = 0;
    var last_sent_lsn: u64 = request.from_lsn - 1;
    for (entries) |entry| {
        if (request.max_records > 0 and frames.items.len >= request.max_records) break;
        const next_encoded_bytes = try std.math.add(usize, encoded_bytes, entry.encoded.len);
        if (request.max_encoded_bytes > 0 and
            frames.items.len > 0 and
            next_encoded_bytes > request.max_encoded_bytes) break;

        const owned = try alloc.dupe(u8, entry.encoded);
        errdefer alloc.free(owned);
        try frames.append(alloc, .{
            .lsn = entry.record.lsn,
            .kind = entry.record.kind,
            .payload_codec = entry.record.payload_codec,
            .encoded = owned,
        });
        encoded_bytes = next_encoded_bytes;
        last_sent_lsn = entry.record.lsn;
    }

    const current_lsn = primary.lastLsn();
    const next_lsn = last_sent_lsn + 1;
    const end_of_wal = next_lsn > current_lsn;
    const owned_records = try frames.toOwnedSlice(alloc);
    errdefer {
        for (owned_records) |*record| record.deinit(alloc);
        alloc.free(owned_records);
    }
    const owned_slot_name = try alloc.dupe(u8, request.slot_name);
    return .{
        .slot_name = owned_slot_name,
        .identity = primary.identity,
        .record_format_version = replication_record.format_version,
        .timeline_id = slot.timeline_id,
        .from_lsn = request.from_lsn,
        .current_lsn = current_lsn,
        .last_sent_lsn = last_sent_lsn,
        .next_lsn = next_lsn,
        .end_of_wal = end_of_wal,
        .encoded_bytes = encoded_bytes,
        .records = owned_records,
    };
}

fn lifecycleResponse(primary: *const primary_mod.Primary, slot_name: []const u8, dropped: bool) !SlotLifecycleResponse {
    const slot = primary.slot(slot_name) orelse return error.SlotNotFound;
    return .{
        .slot_name = slot.name,
        .timeline_id = slot.timeline_id,
        .restart_lsn = slot.restart_lsn,
        .received_lsn = slot.received_lsn,
        .applied_lsn = slot.applied_lsn,
        .safe_read_lsn = slot.safe_read_lsn,
        .active = slot.active,
        .reseed_required = slot.reseed_required,
        .last_error = slot.last_error,
        .current_lsn = primary.lastLsn(),
        .dropped = dropped,
    };
}

pub fn standbyStatusUpdate(
    primary: *primary_mod.Primary,
    request: StandbyStatusUpdateRequest,
) !StandbyStatusUpdateResponse {
    try validateSlotName(request.slot_name);
    try primary.standbyStatusUpdateWithSafeRead(
        request.slot_name,
        request.timeline_id,
        request.received_lsn,
        request.applied_lsn,
        request.safe_read_lsn orelse request.applied_lsn,
    );
    const slot = primary.slot(request.slot_name) orelse return error.SlotNotFound;
    return .{
        .slot_name = slot.name,
        .timeline_id = slot.timeline_id,
        .received_lsn = slot.received_lsn,
        .applied_lsn = slot.applied_lsn,
        .safe_read_lsn = slot.safe_read_lsn,
        .restart_lsn = slot.restart_lsn,
        .active = slot.active,
        .reseed_required = slot.reseed_required,
        .last_error = slot.last_error,
        .current_lsn = primary.lastLsn(),
    };
}

fn validateSlotName(slot_name: []const u8) !void {
    if (!validation.isIdentifier(slot_name)) return error.InvalidSlotName;
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};

    return .{
        .primary_log = try alloc.dupeSentinel(u8, primary_log, 0),
        .primary_slots = try alloc.dupeSentinel(u8, primary_slots, 0),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-replication-api-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

test "storage.ha replication api identifies primary and creates restartable slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "identify-create-slot");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    const identified = identifySystem(&primary);
    try std.testing.expectEqual(identity.cluster_id, identified.identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 2), identified.current_lsn);
    try std.testing.expectEqual(@as(u64, 3), identified.next_lsn);
    try std.testing.expectEqual(replication_record.format_version, identified.record_format_version);

    const created = try createReplicationSlot(&primary, .{
        .slot_name = "standby-a",
    });
    try std.testing.expectEqualStrings("standby-a", created.slot_name);
    try std.testing.expectEqual(@as(u64, 2), created.restart_lsn);
    try std.testing.expectEqual(@as(u64, 2), created.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), created.applied_lsn);
    try std.testing.expectEqual(@as(u64, 2), created.safe_read_lsn);
    try std.testing.expect(created.last_error == null);
    try std.testing.expectError(error.InitialLsnAheadOfPrimary, createReplicationSlot(&primary, .{
        .slot_name = "bad",
        .initial_lsn = 99,
    }));
}

test "storage.ha replication api rejects invalid slot identifiers" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-slot-identifiers");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try createReplicationSlot(&primary, .{
        .slot_name = "standby-a",
        .initial_lsn = 1,
    });

    try std.testing.expectError(error.InvalidSlotName, createReplicationSlot(&primary, .{ .slot_name = "" }));
    try std.testing.expectError(error.InvalidSlotName, createReplicationSlot(&primary, .{ .slot_name = " standby-a" }));
    try std.testing.expectError(error.InvalidSlotName, pauseReplicationSlot(&primary, .{ .slot_name = "standby a" }));
    try std.testing.expectError(error.InvalidSlotName, resumeReplicationSlot(&primary, .{ .slot_name = "standby/a" }));
    try std.testing.expectError(error.InvalidSlotName, dropReplicationSlot(&primary, .{ .slot_name = "standby-a\n" }));
    try std.testing.expectError(error.InvalidSlotName, startReplication(alloc, &primary, .{
        .slot_name = "standby/a",
        .from_lsn = 1,
    }));
    try std.testing.expectError(error.InvalidSlotName, standbyStatusUpdate(&primary, .{
        .slot_name = "standby a",
        .timeline_id = identity.timeline_id,
        .received_lsn = 1,
        .applied_lsn = 1,
    }));
}

test "storage.ha replication api starts replication with record and byte batching" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "start-replication");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    _ = try primary.append(.{ .payload = "three" });
    _ = try createReplicationSlot(&primary, .{
        .slot_name = "standby-a",
        .initial_lsn = 1,
    });

    {
        var batch = try startReplication(alloc, &primary, .{
            .slot_name = "standby-a",
            .from_lsn = 1,
            .max_records = 2,
        });
        defer batch.deinit(alloc);
        try std.testing.expectEqualStrings("standby-a", batch.slot_name);
        try std.testing.expectEqual(identity, batch.identity);
        try std.testing.expectEqual(replication_record.format_version, batch.record_format_version);
        try std.testing.expectEqual(@as(usize, 2), batch.records.len);
        try std.testing.expectEqual(@as(u64, 1), batch.records[0].lsn);
        try std.testing.expectEqual(@as(u64, 2), batch.last_sent_lsn);
        try std.testing.expectEqual(@as(u64, 3), batch.next_lsn);
        try std.testing.expect(!batch.end_of_wal);

        const decoded = try replication_record.decode(batch.records[0].encoded);
        try std.testing.expectEqual(@as(u64, 1), decoded.lsn);
        try std.testing.expectEqualStrings("one", decoded.payload);
    }

    {
        var tail = try startReplication(alloc, &primary, .{
            .slot_name = "standby-a",
            .from_lsn = 3,
            .max_encoded_bytes = replication_record.header_size,
        });
        defer tail.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), tail.records.len);
        try std.testing.expectEqual(@as(u64, 3), tail.last_sent_lsn);
        try std.testing.expect(tail.encoded_bytes > replication_record.header_size);
        try std.testing.expect(tail.end_of_wal);
    }

    {
        var poll = try startReplication(alloc, &primary, .{
            .slot_name = "standby-a",
            .from_lsn = primary.nextLsn(),
        });
        defer poll.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), poll.records.len);
        try std.testing.expectEqual(primary.nextLsn(), poll.next_lsn);
        try std.testing.expect(poll.end_of_wal);
    }

    try std.testing.expectError(error.InvalidReplicationStartLsn, startReplication(alloc, &primary, .{
        .slot_name = "standby-a",
        .from_lsn = 0,
    }));
    try std.testing.expectError(error.ReplicationStartAheadOfPrimary, startReplication(alloc, &primary, .{
        .slot_name = "standby-a",
        .from_lsn = primary.nextLsn() + 1,
    }));
}

test "storage.ha replication api pauses resumes and drops slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "slot-lifecycle");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try createReplicationSlot(&primary, .{
        .slot_name = "standby-a",
        .initial_lsn = 1,
    });
    try primary.reportReplicationError("standby-a", "IntentionalApplyFailure");

    const paused = try pauseReplicationSlot(&primary, .{ .slot_name = "standby-a" });
    try std.testing.expectEqualStrings("standby-a", paused.slot_name);
    try std.testing.expect(!paused.active);
    try std.testing.expect(!paused.dropped);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", paused.last_error.?);
    try std.testing.expectError(error.SlotInactive, startReplication(alloc, &primary, .{
        .slot_name = "standby-a",
        .from_lsn = 1,
    }));

    const resumed = try resumeReplicationSlot(&primary, .{ .slot_name = "standby-a" });
    try std.testing.expect(resumed.active);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", resumed.last_error.?);
    var batch = try startReplication(alloc, &primary, .{
        .slot_name = "standby-a",
        .from_lsn = 1,
    });
    defer batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), batch.records.len);

    const dropped = try dropReplicationSlot(&primary, .{ .slot_name = "standby-a" });
    try std.testing.expect(dropped.dropped);
    try std.testing.expectEqualStrings("standby-a", dropped.slot_name);
    try std.testing.expect(dropped.last_error == null);
    try std.testing.expectError(error.SlotNotFound, startReplication(alloc, &primary, .{
        .slot_name = "standby-a",
        .from_lsn = 1,
    }));
}

test "storage.ha replication api persists standby status updates" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "status-update");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        _ = try primary.append(.{ .payload = "one" });
        _ = try primary.append(.{ .payload = "two" });
        _ = try createReplicationSlot(&primary, .{
            .slot_name = "standby-a",
            .initial_lsn = 0,
        });
        try primary.reportReplicationError("standby-a", "IntentionalApplyFailure");

        const updated = try standbyStatusUpdate(&primary, .{
            .slot_name = "standby-a",
            .timeline_id = identity.timeline_id,
            .received_lsn = 2,
            .applied_lsn = 1,
            .safe_read_lsn = 0,
        });
        try std.testing.expectEqual(@as(u64, 2), updated.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), updated.applied_lsn);
        try std.testing.expectEqual(@as(u64, 0), updated.safe_read_lsn);
        try std.testing.expectEqual(@as(u64, 2), updated.restart_lsn);
        try std.testing.expect(updated.active);
        try std.testing.expect(!updated.reseed_required);
        try std.testing.expect(updated.last_error == null);
        try std.testing.expectError(error.StandbyAheadOfPrimary, standbyStatusUpdate(&primary, .{
            .slot_name = "standby-a",
            .timeline_id = identity.timeline_id,
            .received_lsn = 9,
            .applied_lsn = 9,
        }));
    }

    {
        var reopened = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer reopened.close();
        const slot = reopened.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
        try std.testing.expectEqual(@as(u64, 0), slot.safe_read_lsn);
    }
}
