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

//! Persisted pending replication effects. This owner contains only storage
//! keys and codecs; replication log publication and acknowledgement belong to
//! the replication adapter. Keep formats stable across crash/reopen and upgrades.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Kind = enum(u8) {
    batch = 0,
    replay = 1,
    schema = 2,
    restore_batch = 3,
    primary_effect = 4,
    row_policy = 5,
};

pub const replication_batch_outbox_key = "\x00\x00__metadata__:ha_batch_outbox_v1";

pub const replication_replay_outbox_key = "\x00\x00__metadata__:ha_replay_outbox_v1";

pub const replication_schema_outbox_key = "\x00\x00__metadata__:ha_schema_outbox_v1";

pub const replication_outbox_v2_prefix = "\x00\x00__metadata__:ha_outbox_v2:";

pub const replication_outbox_id_len: usize = 16;

pub const replication_outbox_magic = "AHO1";

pub const replication_outbox_header_len: usize = 16;

pub const replication_outbox_checksum_len: usize = 4;

pub const replication_outbox_recovery_batch_size: usize = 16;

pub const RecoveryIdentity = [replication_outbox_id_len]u8;

/// One DB-owned recovery cursor. Scalar identity/history survives attempts;
/// an optional provider continuation owns its funded read and working storage.
/// Join callers and reset before retiring the allocator or DB storage. Neither
/// expected payload nor per-attempt execution control may be retained.
pub const RecoveryCursor = struct {
    pub const Position = struct { origin: u64 = 0, owner: u64 = 0, revision: u64 = 0, next_lsn: u64 = 0 };
    pub const State = struct {
        active: bool = false,
        identity: RecoveryIdentity = @splat(0),
        from_lsn: u64 = 0,
        next_lsn: u64 = 0,
        matched_lsn: u64 = 0,
        kind: @import("replication_record.zig").RecordKind = .batch_mutation,
        payload_len: usize = 0,
        shard_id: u64 = 0,
        table_id: u64 = 0,
        cluster_id: u64 = 0,
        timeline_id: u64 = 0,
        epoch: u64 = 0,
        position: Position = .{},
    };
    pub const Continuation = struct {
        ptr: *anyopaque,
        // Grant-free destruction executes in the unit owning the allocation.
        destroy: *const fn (*anyopaque) callconv(.c) void,
    };
    allocator: ?Allocator = null,
    mutex: std.atomic.Mutex = .unlocked,
    state: State = .{},
    pending: ?Continuation = null,
    progress_generation: std.atomic.Value(u64) = .init(0),
    runnable: bool = false,
    pub const Progress = struct { generation: u64, runnable: bool = false };

    /// Observation only: no grant or publication authority. A contended probe
    /// preserves generation while refusing readiness, without waiting.
    pub fn progress(self: *RecoveryCursor) Progress {
        const generation = self.progress_generation.load(.acquire);
        if (!self.mutex.tryLock()) return .{ .generation = generation };
        defer self.mutex.unlock();
        return .{ .generation = self.progress_generation.load(.acquire), .runnable = self.runnable };
    }

    pub fn retireIdentity(self: *RecoveryCursor, identity: RecoveryIdentity) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.state.active and std.mem.eql(u8, &self.state.identity, &identity)) {
            self.retireRead();
            self.state = .{};
        }
    }
    pub fn init(allocator: Allocator) RecoveryCursor {
        return .{ .allocator = allocator };
    }
    /// Caller holds mutex, or has joined every caller during teardown.
    pub fn retireRead(self: *RecoveryCursor) void {
        self.runnable = false;
        const pending = self.pending;
        self.pending = null;
        if (pending) |owner| owner.destroy(owner.ptr);
    }
    /// Grant-free retirement joins any synchronous use. The atomic mutex
    /// itself is never copied, and the allocator remains available for reuse.
    pub fn reset(self: *RecoveryCursor) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.retireRead();
        self.state = .{};
    }
};

pub const DurableReplicationOutbox = struct {
    from_lsn: u64,
    payload: []const u8,
    /// Copied from an immutable persisted v2 intent key, never a payload borrow.
    /// Legacy singleton keys have no reusable identity and remain null.
    recovery_identity: ?RecoveryIdentity = null,
    /// Borrowed only during publisher recovery; never retained by that publisher.
    recovery_cursor: ?*RecoveryCursor = null,

    pub fn withRecoveryKey(self: @This(), key: []const u8) !@This() {
        _ = try durableReplicationOutboxKindFromKey(key);
        var result = self;
        result.recovery_identity = key[key.len - replication_outbox_id_len ..][0..replication_outbox_id_len].*;
        return result;
    }
};

pub fn durableReplicationOutboxKeyAlloc(
    alloc: Allocator,
    kind: Kind,
    from_lsn: u64,
    root_generation: u64,
    payload: []const u8,
) ![]u8 {
    if (from_lsn == 0) return error.InvalidHAOutbox;
    var id: [replication_outbox_id_len]u8 = undefined;
    // Bind the identity to both the WAL fence and logical mutation. Schema
    // requests prepare before apply admission and may observe the same next
    // LSN; distinct payloads must still never overwrite each other's outbox.
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(&.{@backingInt(kind)});
    var identity_context: [16]u8 = undefined;
    std.mem.writeInt(u64, identity_context[0..8], root_generation, .big);
    std.mem.writeInt(u64, identity_context[8..16], from_lsn, .big);
    hasher.update(&identity_context);
    hasher.update(payload);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hasher.final(&digest);
    @memcpy(&id, digest[0..id.len]);
    const key = try alloc.alloc(u8, replication_outbox_v2_prefix.len + 1 + id.len);
    @memcpy(key[0..replication_outbox_v2_prefix.len], replication_outbox_v2_prefix);
    key[replication_outbox_v2_prefix.len] = @backingInt(kind);
    @memcpy(key[replication_outbox_v2_prefix.len + 1 ..], &id);
    return key;
}

pub fn durableReplicationOutboxKindFromKey(key: []const u8) !Kind {
    if (!std.mem.startsWith(u8, key, replication_outbox_v2_prefix) or
        key.len != replication_outbox_v2_prefix.len + 1 + replication_outbox_id_len)
        return error.InvalidHAOutbox;
    return switch (key[replication_outbox_v2_prefix.len]) {
        @backingInt(Kind.batch) => .batch,
        @backingInt(Kind.replay) => .replay,
        @backingInt(Kind.schema) => .schema,
        @backingInt(Kind.restore_batch) => .restore_batch,
        @backingInt(Kind.primary_effect) => .primary_effect,
        @backingInt(Kind.row_policy) => .row_policy,
        else => error.InvalidHAOutbox,
    };
}

pub fn encodeDurableReplicationOutboxAlloc(alloc: Allocator, from_lsn: u64, payload: []const u8) ![]u8 {
    if (from_lsn == 0) return error.InvalidHAOutbox;
    const payload_len = std.math.cast(u32, payload.len) orelse return error.InvalidHAOutbox;
    const out = try alloc.alloc(u8, replication_outbox_header_len + payload.len + replication_outbox_checksum_len);
    errdefer alloc.free(out);
    @memcpy(out[0..4], replication_outbox_magic);
    std.mem.writeInt(u64, out[4..12], from_lsn, .little);
    std.mem.writeInt(u32, out[12..16], payload_len, .little);
    @memcpy(out[replication_outbox_header_len..][0..payload.len], payload);
    const checksum = @import("antfly_hash").Crc32.hash(out[0 .. out.len - replication_outbox_checksum_len]);
    std.mem.writeInt(u32, out[out.len - replication_outbox_checksum_len ..][0..replication_outbox_checksum_len], checksum, .little);
    return out;
}

pub fn decodeDurableReplicationOutbox(raw: []const u8) !DurableReplicationOutbox {
    return decodeDurableReplicationOutboxWithAdmission(raw, null);
}

/// Recovery still holds service leases. Inspect the retained original lifetime
/// between bounded CRC updates without transferring CPU or authorizing delivery.
pub fn decodeDurableReplicationOutboxWithAdmission(raw: []const u8, control: ?@import("../../common/read_admission.zig").Port) !DurableReplicationOutbox {
    var cursor: DecodeCursor = .{};
    while (try cursor.step(raw, 64 * 1024, control) != .ready) {}
    return cursor.view(raw);
}

/// Incremental AHO1 validation over an immutable owned value. No input/control
/// span is retained. A ready view borrows the same owner's bytes and grants no
/// authority to publish or delete a durable intent.
pub const DecodeCursor = struct {
    pub const Status = enum { pending, ready };
    const Phase = enum { header, checksum, trailer, ready, failed };
    phase: Phase = .header,
    raw_len: ?usize = null,
    header: [replication_outbox_header_len]u8 = undefined,
    trailer: [replication_outbox_checksum_len]u8 = undefined,
    partial: usize = 0,
    checksum_offset: usize = 0,
    crc: @import("antfly_hash").Crc32 = .init(),
    from_lsn: u64 = 0,
    failure: ?anyerror = null,

    pub fn step(self: *@This(), raw: []const u8, quantum: usize, control: ?@import("../../common/read_admission.zig").Port) !Status {
        if (quantum == 0 or quantum > 64 * 1024) return error.InvalidArgument;
        if (self.failure) |err| return err;
        return self.advance(raw, quantum, control) catch |err| {
            const cause = if (control) |port| blk: {
                port.checkpoint() catch |original| break :blk original;
                break :blk err;
            } else err;
            self.phase = .failed;
            self.failure = cause;
            return cause;
        };
    }
    fn advance(self: *@This(), raw: []const u8, quantum: usize, control: ?@import("../../common/read_admission.zig").Port) !Status {
        if (control) |port| try port.checkpoint();
        if (raw.len < replication_outbox_header_len + replication_outbox_checksum_len) return error.InvalidHAOutbox;
        if (self.raw_len) |len| {
            if (len != raw.len) return error.InvalidHAOutbox;
        } else self.raw_len = raw.len;
        switch (self.phase) {
            .header => {
                const take = @min(quantum, self.header.len - self.partial);
                @memcpy(self.header[self.partial..][0..take], raw[self.partial..][0..take]);
                self.partial += take;
                if (self.partial == self.header.len) {
                    if (!std.mem.eql(u8, self.header[0..4], replication_outbox_magic)) return error.InvalidHAOutbox;
                    const len: usize = std.mem.readInt(u32, self.header[12..16], .little);
                    if (len != raw.len - self.header.len - self.trailer.len) return error.InvalidHAOutbox;
                    self.partial = 0;
                    self.phase = .checksum;
                }
            },
            .checksum => {
                const end = raw.len - self.trailer.len;
                const take = @min(quantum, end - self.checksum_offset);
                self.crc.update(raw[self.checksum_offset..][0..take]);
                self.checksum_offset += take;
                if (self.checksum_offset == end) self.phase = .trailer;
            },
            .trailer => {
                const take = @min(quantum, self.trailer.len - self.partial);
                @memcpy(self.trailer[self.partial..][0..take], raw[raw.len - self.trailer.len + self.partial ..][0..take]);
                self.partial += take;
                if (self.partial == self.trailer.len) {
                    if (self.crc.final() != std.mem.readInt(u32, &self.trailer, .little)) return error.InvalidHAOutbox;
                    self.from_lsn = std.mem.readInt(u64, self.header[4..12], .little);
                    if (self.from_lsn == 0) return error.InvalidHAOutbox;
                    self.phase = .ready;
                }
            },
            .ready => {},
            .failed => return self.failure.?,
        }
        if (control) |port| try port.checkpoint();
        return if (self.phase == .ready) .ready else .pending;
    }
    pub fn view(self: *const @This(), raw: []const u8) !DurableReplicationOutbox {
        if (self.failure) |err| return err;
        if (self.phase != .ready or raw.len != self.raw_len.?) return error.InvalidArgument;
        return .{ .from_lsn = self.from_lsn, .payload = raw[replication_outbox_header_len .. raw.len - replication_outbox_checksum_len] };
    }
};

test "storage.hot_standby durable outbox codec preserves pre-extraction persisted bytes" {
    // Independent fixture: AHO1, little-endian LSN and length, payload, IEEE CRC32.
    const fixture = [_]u8{ 0x41, 0x48, 0x4f, 0x31, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x7e, 0x55, 0xb7, 0xf9 };
    const decoded = try decodeDurableReplicationOutbox(&fixture);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), decoded.from_lsn);
    try std.testing.expectEqualStrings("abc", decoded.payload);
    const encoded = try encodeDurableReplicationOutboxAlloc(std.testing.allocator, decoded.from_lsn, decoded.payload);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualSlices(u8, &fixture, encoded);
}

test "storage.hot_standby durable outbox codec rejects corrupt and incomplete obligations" {
    const alloc = std.testing.allocator;
    const valid = try encodeDurableReplicationOutboxAlloc(alloc, 7, "pending mutation");
    defer alloc.free(valid);
    for (0..valid.len) |i| {
        const corrupt = try alloc.dupe(u8, valid);
        defer alloc.free(corrupt);
        corrupt[i] ^= 1;
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(corrupt));
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(valid[0..i]));
    }
    // Valid checksum alone cannot authorize a record with no WAL fence.
    @memset(valid[4..12], 0);
    std.mem.writeInt(u32, valid[valid.len - 4 ..][0..4], @import("antfly_hash").Crc32.hash(valid[0 .. valid.len - 4]), .little);
    try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(valid));
    try std.testing.expectError(error.InvalidHAOutbox, encodeDurableReplicationOutboxAlloc(alloc, 0, "pending"));
}

test "storage.hot_standby durable outbox keys isolate payload generation fence and kind" {
    const alloc = std.testing.allocator;
    const baseline = try durableReplicationOutboxKeyAlloc(alloc, .batch, 7, 2, "first");
    defer alloc.free(baseline);
    const duplicate = try durableReplicationOutboxKeyAlloc(alloc, .batch, 7, 2, "first");
    defer alloc.free(duplicate);
    try std.testing.expectEqualSlices(u8, baseline, duplicate);
    const variants = [_]struct { kind: Kind = .batch, lsn: u64 = 7, generation: u64 = 2, payload: []const u8 = "first" }{
        .{ .payload = "second" }, .{ .generation = 3 }, .{ .lsn = 8 }, .{ .kind = .schema },
    };
    for (variants) |variant| {
        const key = try durableReplicationOutboxKeyAlloc(alloc, variant.kind, variant.lsn, variant.generation, variant.payload);
        defer alloc.free(key);
        try std.testing.expect(!std.mem.eql(u8, baseline, key));
        try std.testing.expectEqual(variant.kind, try durableReplicationOutboxKindFromKey(key));
    }
    baseline[replication_outbox_v2_prefix.len] = 255;
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(baseline));
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(replication_batch_outbox_key));
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(duplicate[0 .. duplicate.len - 1]));
}

test "durable outbox storage admitted checksum retains original errors at every bounded quantum" {
    const read = @import("../../common/read_admission.zig");
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        rejection: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.rejection;
        }
    };
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 2 * 64 * 1024 + 17);
    defer a.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i);
    const raw = try encodeDurableReplicationOutboxAlloc(a, 37, payload);
    defer a.free(raw);
    const saved = try a.dupe(u8, raw);
    defer a.free(saved);
    var state: State = .{};
    const owner: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const control = read.Port.init(&owner);
    // Independent persisted AHO1 fixture, not generated by this codec.
    const fixture = [_]u8{ 0x41, 0x48, 0x4f, 0x31, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x7e, 0x55, 0xb7, 0xf9 };
    const stored = try decodeDurableReplicationOutboxWithAdmission(&fixture, control);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), stored.from_lsn);
    try std.testing.expectEqualStrings("abc", stored.payload);
    state = .{};
    const result = try decodeDurableReplicationOutboxWithAdmission(raw, control);
    try std.testing.expectEqual(@as(u64, 37), result.from_lsn);
    try std.testing.expectEqualSlices(u8, payload, result.payload);
    // Separate bounded header, three CRC updates and trailer, each with
    // original authority checked before and after its work.
    const successful_checks = state.checks;
    try std.testing.expectEqual(@as(usize, 10), successful_checks);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |rejection| {
        for (1..successful_checks + 1) |at| {
            state = .{ .reject_at = at, .rejection = rejection };
            try std.testing.expectError(rejection, decodeDurableReplicationOutboxWithAdmission(raw, control));
            try std.testing.expect(state.checks >= at and state.checks <= at + 1);
            try std.testing.expectEqualSlices(u8, saved, raw);
        }
    }
    state = .{};
    const retry = try decodeDurableReplicationOutboxWithAdmission(raw, control);
    try std.testing.expectEqualSlices(u8, result.payload, retry.payload);
    // Payload corruption in every quantum still rejects with identical persisted
    // semantics. No partially verified record escapes to a replication mirror.
    for ([_]usize{ 16, 64 * 1024 + 7, 2 * 64 * 1024 + 7 }) |at| {
        raw[at] ^= 1;
        state = .{};
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutboxWithAdmission(raw, control));
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(raw));
        raw[at] ^= 1;
    }
    try std.testing.expectEqualSlices(u8, saved, raw);
}

test "durable outbox recovery identity copies only immutable persisted v2 keys" {
    const a = std.testing.allocator;
    const key = try durableReplicationOutboxKeyAlloc(a, .batch, 7, 3, "payload");
    defer a.free(key);
    const raw = try encodeDurableReplicationOutboxAlloc(a, 7, "payload");
    defer a.free(raw);
    const decoded = try decodeDurableReplicationOutbox(raw);
    try std.testing.expect(decoded.recovery_identity == null);
    const bound = try decoded.withRecoveryKey(key);
    const saved = bound.recovery_identity.?;
    key[key.len - 1] ^= 1;
    try std.testing.expectEqualDeep(saved, bound.recovery_identity.?);
    const changed = (try decoded.withRecoveryKey(key)).recovery_identity.?;
    try std.testing.expect(!std.mem.eql(u8, &saved, &changed));
    try std.testing.expectEqual(@as(u64, 7), bound.from_lsn);
    try std.testing.expectEqualStrings("payload", bound.payload);
    try std.testing.expectError(error.InvalidHAOutbox, decoded.withRecoveryKey(replication_batch_outbox_key));
    try std.testing.expectError(error.InvalidHAOutbox, decoded.withRecoveryKey(key[0 .. key.len - 1]));
    key[replication_outbox_v2_prefix.len] = 255;
    try std.testing.expectError(error.InvalidHAOutbox, decoded.withRecoveryKey(key));
}

test "durable outbox owned decoder validates independent persisted fixture one byte per step" {
    const fixture = [_]u8{ 0x41, 0x48, 0x4f, 0x31, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x7e, 0x55, 0xb7, 0xf9 };
    var cursor: DecodeCursor = .{};
    try std.testing.expectError(error.InvalidArgument, cursor.view(&fixture));
    try std.testing.expectError(error.InvalidArgument, cursor.step(&fixture, 0, null));
    try std.testing.expectError(error.InvalidArgument, cursor.step(&fixture, 65537, null));
    var calls: usize = 0;
    while (true) {
        const before = cursor.checksum_offset;
        const status = try cursor.step(&fixture, 1, null);
        try std.testing.expect(cursor.checksum_offset - before <= 1);
        calls += 1;
        if (status == .ready) break;
        try std.testing.expectError(error.InvalidArgument, cursor.view(&fixture));
        try std.testing.expect(calls < 100);
    }
    try std.testing.expectEqual(@as(usize, 16 + 19 + 4), calls);
    const decoded = try cursor.view(&fixture);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), decoded.from_lsn);
    try std.testing.expectEqualStrings("abc", decoded.payload);
    try std.testing.expectError(error.InvalidArgument, cursor.view(fixture[0..22]));
    try std.testing.expectEqual(DecodeCursor.Status.ready, try cursor.step(&fixture, 1, null));
    // A truncated capture permanently rejects; later bytes cannot revive it.
    var truncated: DecodeCursor = .{};
    try std.testing.expectError(error.InvalidHAOutbox, truncated.step(fixture[0..19], 1, null));
    try std.testing.expectError(error.InvalidHAOutbox, truncated.step(&fixture, 1, null));
    try std.testing.expectError(error.InvalidHAOutbox, truncated.view(&fixture));
}
