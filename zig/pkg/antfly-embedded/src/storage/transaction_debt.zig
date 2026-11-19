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

//! Epoch-qualified transaction recovery debt. The owner must establish the
//! schema fence and upgraded-writer barrier before initialization. This is a
//! derived directory, never a replacement for coordinator decisions, ACKs,
//! physical-root adoption, or independently durable intent/outbox guards.
const std = @import("std");
pub const Namespace = @import("db/doc_identity_namespace.zig").Namespace;
pub const TxnId = [16]u8;
pub const Digest = [32]u8;
pub const family_prefix = "\x00\x00__metadata__:schema_transaction_debt:";
pub const head_key = family_prefix ++ "v1:head";
const member_prefix = family_prefix ++ "v1:member:";
const dirty_prefix = family_prefix ++ "v1:dirty:";
const seen_prefix = family_prefix ++ "v1:seen:";
const orphan_prefix = family_prefix ++ "v1:orphan:";
pub const encoded_len = 256;
pub const max_page_units = 64;

pub const Scope = struct {
    namespace: Namespace,
    generation: Digest,
    epoch: u64,

    pub fn validate(self: Scope) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.epoch == 0 or std.mem.allEqual(u8, &self.generation, 0)) return error.InvalidTxnRecord;
    }
    pub fn eql(self: Scope, other: Scope) bool {
        return self.namespace.eql(other.namespace) and self.epoch == other.epoch and std.mem.eql(u8, &self.generation, &other.generation);
    }
    pub fn fromBinding(binding: anytype) !Scope {
        try binding.validate();
        var buffer: [8192]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(binding, .{}, &writer);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/schema-transaction-debt/v1");
        state.update(writer.buffered());
        var generation: Digest = undefined;
        state.final(&generation);
        return .{ .namespace = binding.fence.namespace, .generation = generation, .epoch = binding.fence.admission_epoch };
    }
};

pub const Phase = enum(u8) { records = 1, dirty = 2, ready = 3, retired = 4, gc = 5 };
pub const Kind = enum(u8) { blocked = 1, dirty = 2 };
pub const LegacyPhase = enum(u8) { none = 0, enlisted = 1, resolved = 2, indexed = 3, indexed_legacy = 4, cleanup = 5 };
pub const Progress = struct {
    phase: LegacyPhase = .none,
    offset: u64 = 0,
    seen: u64 = 0,
    expected: u64 = 0,
    enlisted: u64 = 0,
    resolved: u64 = 0,
    indexed_expected: u64 = 0,
    after_member: Digest = @splat(0),
    work_revision: u64 = 0,
};

pub const Header = struct {
    scope: Scope,
    revision: u64 = 1,
    debt: u64 = 0,
    phase: Phase = .records,
    after: ?TxnId = null,
    pending: ?TxnId = null,
    progress: Progress = .{},

    pub fn validate(self: Header) !void {
        try self.scope.validate();
        if (self.revision == 0 or (self.pending != null and self.phase != .dirty) or
            (self.pending == null and !std.meta.eql(self.progress, Progress{})) or
            ((self.phase == .ready or self.phase == .retired or self.phase == .gc) and self.after != null) or
            (self.phase == .retired and self.debt != 0) or self.progress.seen > self.progress.expected) return error.InvalidTxnRecord;
        if (self.pending != null and (self.progress.work_revision == 0 or self.progress.work_revision > self.revision)) return error.InvalidTxnRecord;
    }
    pub fn encode(self: Header) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "ATDG\x01\x00\x00\x00");
        std.mem.writeInt(u64, bytes[8..16], self.scope.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[16..24], self.scope.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[24..32], self.scope.namespace.range_id, .little);
        @memcpy(bytes[32..64], &self.scope.generation);
        std.mem.writeInt(u64, bytes[64..72], self.scope.epoch, .little);
        std.mem.writeInt(u64, bytes[72..80], self.revision, .little);
        std.mem.writeInt(u64, bytes[80..88], self.debt, .little);
        bytes[88] = @backingInt(self.phase);
        bytes[89] = @intFromBool(self.after != null);
        bytes[90] = @intFromBool(self.pending != null);
        bytes[91] = @backingInt(self.progress.phase);
        if (self.after) |id| @memcpy(bytes[96..112], &id);
        if (self.pending) |id| @memcpy(bytes[112..128], &id);
        inline for (.{ "offset", "seen", "expected", "enlisted", "resolved", "indexed_expected" }, 0..) |field, index|
            std.mem.writeInt(u64, bytes[128 + index * 8 ..][0..8], @field(self.progress, field), .little);
        @memcpy(bytes[176..208], &self.progress.after_member);
        std.mem.writeInt(u64, bytes[208..216], self.progress.work_revision, .little);
        std.crypto.hash.Blake3.hash(bytes[0..224], bytes[224..256], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Header {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "ATDG\x01\x00\x00\x00") or
            bytes[89] > 1 or bytes[90] > 1 or !std.mem.allEqual(u8, bytes[92..96], 0) or !std.mem.allEqual(u8, bytes[216..224], 0) or
            (bytes[89] == 0 and !std.mem.allEqual(u8, bytes[96..112], 0)) or
            (bytes[90] == 0 and !std.mem.allEqual(u8, bytes[112..128], 0))) return error.InvalidTxnRecord;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..224], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[224..256])) return error.InvalidTxnRecord;
        var header: Header = .{
            .scope = .{
                .namespace = .{ .table_id = std.mem.readInt(u64, bytes[8..16], .little), .shard_id = std.mem.readInt(u64, bytes[16..24], .little), .range_id = std.mem.readInt(u64, bytes[24..32], .little) },
                .generation = bytes[32..64].*,
                .epoch = std.mem.readInt(u64, bytes[64..72], .little),
            },
            .revision = std.mem.readInt(u64, bytes[72..80], .little),
            .debt = std.mem.readInt(u64, bytes[80..88], .little),
            .phase = std.enums.fromInt(Phase, bytes[88]) orelse return error.InvalidTxnRecord,
            .after = if (bytes[89] == 1) bytes[96..112].* else null,
            .pending = if (bytes[90] == 1) bytes[112..128].* else null,
            .progress = .{ .phase = std.enums.fromInt(LegacyPhase, bytes[91]) orelse return error.InvalidTxnRecord, .after_member = bytes[176..208].*, .work_revision = std.mem.readInt(u64, bytes[208..216], .little) },
        };
        inline for (.{ "offset", "seen", "expected", "enlisted", "resolved", "indexed_expected" }, 0..) |field, index|
            @field(header.progress, field) = std.mem.readInt(u64, bytes[128 + index * 8 ..][0..8], .little);
        try header.validate();
        return header;
    }
    pub fn digest(self: Header) !Digest {
        return (try self.encode())[224..256].*;
    }
    pub fn bump(self: *Header) !void {
        self.revision = std.math.add(u64, self.revision, 1) catch return error.InvalidTxnRecord;
    }
};

pub fn load(txn: anytype) !?Header {
    const raw = txn.get(head_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Header.decode(raw);
}
pub fn require(txn: anytype, scope: Scope) !Header {
    try scope.validate();
    const header = (try load(txn)) orelse return error.SchemaValidationRuntimeRecoveryRequired;
    if (!header.scope.eql(scope) or header.phase == .retired) return error.SchemaValidationJobChanged;
    return header;
}

pub const SnapshotStats = struct { members: u64 = 0, dirty: u64 = 0, seen: u64 = 0, orphans: u64 = 0 };

/// Read-only structural qualification on one pinned image. Point lookups use
/// an independent scope so they cannot invalidate the streaming cursor's key.
/// This verifies no transaction decisions: callers must separately qualify
/// membership against their retained coordinator and participant evidence.
/// Bounded local work policy. Clock limits are cooperative at validated-key
/// boundaries; each turn also has a hard key-count ceiling.
pub const AuditBudget = struct {
    records: usize = max_page_units,
    time_ns: u64 = 5 * std.time.ns_per_ms,
    io: ?std.Io = null,

    fn validate(self: @This()) !void {
        if (self.records == 0 or self.records > max_page_units or self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidTxnRecord;
    }
};

/// Cursor and fixed counters belong to one immutable read image. The caller
/// retains that read until deinit and derives every point scope from it.
/// No request allocator or cancellation callback is retained across turns.
pub fn SnapshotAudit(comptime Read: type) type {
    const Cursor = @typeInfo(@TypeOf(@as(Read, undefined).openCursor())).error_union.payload;
    return struct {
        read: Read,
        cursor: Cursor,
        header: ?Header,
        stats: SnapshotStats = .{},
        started: bool = false,
        complete: bool = false,
        invalid: bool = false,
        last_units: usize = 0,

        pub fn init(read: Read, points: anytype, expected: ?Scope, control: anytype) !@This() {
            try control.check();
            const header = try load(points);
            if (expected) |scope| {
                try scope.validate();
                if (header == null) return error.SchemaValidationRuntimeRecoveryRequired;
                if (!header.?.scope.eql(scope)) return error.SchemaValidationJobChanged;
            } else if (header) |value| {
                // Only a never-visited cancellation may retain an older, inert head.
                if (value.phase != .retired) return error.SchemaValidationRuntimeRecoveryRequired;
            }
            return .{ .read = read, .cursor = try read.openCursor(), .header = header };
        }
        pub fn deinit(self: *@This()) void {
            self.cursor.close();
            self.* = undefined;
        }
        pub fn result(self: *const @This()) !SnapshotStats {
            if (self.invalid or !self.complete) return error.SchemaValidationRuntimeRecoveryRequired;
            return self.stats;
        }
        pub fn step(self: *@This(), points: anytype, budget: AuditBudget, control: anytype) !bool {
            try control.check();
            try budget.validate();
            if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
            if (self.complete) return true;
            self.last_units = 0;
            // After any failed accepted turn, the cursor/counters must not be
            // resumed as though an advanced cursor had reached a valid EOF.
            errdefer self.invalid = true;
            if (!std.meta.eql(self.header, try load(points))) return error.SchemaValidationJobChanged;
            const start = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
            while (true) {
                try control.check();
                const entry = if (!self.started) blk: {
                    self.started = true;
                    break :blk try self.cursor.seekAtOrAfter(family_prefix);
                } else try self.cursor.next();
                const row = entry orelse return self.finish(points, control);
                if (!std.mem.startsWith(u8, row.key, family_prefix)) return self.finish(points, control);
                try self.validateRow(points, row);
                self.last_units += 1;
                if (self.last_units == budget.records) return false;
                if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - start >= budget.time_ns) return false;
            }
        }
        fn validateRow(self: *@This(), points: anytype, row: anytype) !void {
            const header = self.header;
            const stats = &self.stats;
            if (header == null) return error.SchemaValidationRuntimeRecoveryRequired;
            const h = header.?;
            if (std.mem.eql(u8, row.key, head_key)) return;
            if (std.mem.startsWith(u8, row.key, member_prefix)) {
                if (row.key.len != member_prefix.len + 48 or
                    !std.mem.eql(u8, row.key[member_prefix.len..][0..32], &h.scope.generation) or row.value.len != 1) return error.InvalidTxnRecord;
                const kind = std.enums.fromInt(Kind, row.value[0]) orelse return error.InvalidTxnRecord;
                const id: TxnId = row.key[member_prefix.len + 32 ..][0..16].*;
                const dirty = try optional(points, &dirtyKey(h.scope.generation, id));
                if ((kind == .dirty) != (dirty != null)) return error.InvalidTxnRecord;
                if (dirty) |bytes| if (!std.mem.eql(u8, bytes, &.{2})) return error.InvalidTxnRecord;
                stats.members = try std.math.add(u64, stats.members, 1);
            } else if (std.mem.startsWith(u8, row.key, dirty_prefix)) {
                if (row.key.len != dirty_prefix.len + 48 or
                    !std.mem.eql(u8, row.key[dirty_prefix.len..][0..32], &h.scope.generation) or !std.mem.eql(u8, row.value, &.{2})) return error.InvalidTxnRecord;
                const id: TxnId = row.key[dirty_prefix.len + 32 ..][0..16].*;
                if (h.phase == .dirty and ((h.after != null and std.mem.order(u8, &id, &h.after.?) != .gt) or
                    (h.pending != null and std.mem.order(u8, &id, &h.pending.?) == .lt))) return error.InvalidTxnRecord;
                if (try member(points, h.scope.generation, id) != .dirty) return error.InvalidTxnRecord;
                stats.dirty = try std.math.add(u64, stats.dirty, 1);
            } else if (std.mem.startsWith(u8, row.key, seen_prefix)) {
                if (row.key.len != seen_prefix.len + 89 or
                    !std.mem.eql(u8, row.key[seen_prefix.len..][0..32], &h.scope.generation) or !std.mem.eql(u8, row.value, &.{1})) return error.InvalidTxnRecord;
                const id: TxnId = row.key[seen_prefix.len + 32 ..][0..16].*;
                const revision = std.mem.readInt(u64, row.key[seen_prefix.len + 48 ..][0..8], .big);
                const kind = row.key[seen_prefix.len + 56];
                if (revision == 0 or revision > h.revision or (kind != 1 and kind != 2)) return error.InvalidTxnRecord;
                const pending = h.pending != null and std.mem.eql(u8, &h.pending.?, &id) and h.progress.work_revision == revision;
                if (!pending) {
                    const orphan = (try optional(points, &orphanKey(h.scope.generation, id, revision))) orelse return error.InvalidTxnRecord;
                    if (!std.mem.eql(u8, orphan, &.{1})) return error.InvalidTxnRecord;
                }
                stats.seen = try std.math.add(u64, stats.seen, 1);
            } else if (std.mem.startsWith(u8, row.key, orphan_prefix)) {
                if (row.key.len != orphan_prefix.len + 56 or
                    !std.mem.eql(u8, row.key[orphan_prefix.len..][0..32], &h.scope.generation) or !std.mem.eql(u8, row.value, &.{1})) return error.InvalidTxnRecord;
                const id: TxnId = row.key[orphan_prefix.len + 32 ..][0..16].*;
                const revision = std.mem.readInt(u64, row.key[orphan_prefix.len + 48 ..][0..8], .big);
                if (revision == 0 or revision >= h.revision or
                    (h.pending != null and std.mem.eql(u8, &h.pending.?, &id) and h.progress.work_revision == revision)) return error.InvalidTxnRecord;
                stats.orphans = try std.math.add(u64, stats.orphans, 1);
            } else return error.InvalidTxnRecord; // Unknown/future private spelling.
        }
        fn finish(self: *@This(), points: anytype, control: anytype) !bool {
            const header = self.header;
            const stats = self.stats;
            if (header) |h| {
                if (stats.members != h.debt or stats.dirty > stats.members) return error.InvalidTxnRecord;
                if (h.pending) |id| if (try member(points, h.scope.generation, id) != .dirty) return error.InvalidTxnRecord;
                if (h.phase == .ready or h.phase == .retired) {
                    if (stats.dirty != 0 or stats.seen != 0 or stats.orphans != 0) return error.InvalidTxnRecord;
                }
                if (h.phase == .gc and stats.dirty != 0) return error.InvalidTxnRecord;
                if (h.phase == .records and (stats.seen != 0 or stats.orphans != 0)) return error.InvalidTxnRecord;
                if (h.phase == .retired and stats.members != 0) return error.InvalidTxnRecord;
            }
            try control.check();
            self.complete = true;
            return true;
        }
    };
}

/// Synchronous restore/inspection uses exactly the same bounded validator as
/// resident owner work. It retains one cursor across turns, not a new table
/// seek per page, and leaves scheduling between steps to its caller.
pub fn validateSnapshot(read: anytype, points: anytype, expected: ?Scope, control: anytype) !SnapshotStats {
    var audit = try SnapshotAudit(@TypeOf(read)).init(read, points, expected, control);
    defer audit.deinit();
    while (!try audit.step(points, .{}, control)) {}
    return audit.result();
}

fn optional(view: anytype, key: []const u8) !?[]const u8 {
    return view.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn save(txn: anytype, header: Header) !void {
    try txn.put(head_key, &try header.encode());
}
pub fn initialize(txn: anytype, scope: Scope) !Header {
    try scope.validate();
    if (try load(txn)) |previous| {
        if (previous.scope.eql(scope)) {
            if (previous.phase == .retired) return error.SchemaValidationJobChanged;
            return previous;
        }
        if (previous.phase != .retired) return error.SchemaValidationRuntimeRecoveryRequired;
    }
    const header: Header = .{ .scope = scope };
    try save(txn, header);
    return header;
}

pub fn memberPrefix(generation: Digest) [member_prefix.len + 32]u8 {
    return prefixFor(member_prefix, generation);
}
pub fn dirtyPrefix(generation: Digest) [dirty_prefix.len + 32]u8 {
    return prefixFor(dirty_prefix, generation);
}
pub fn orphanPrefix(generation: Digest) [orphan_prefix.len + 32]u8 {
    return prefixFor(orphan_prefix, generation);
}
pub fn seenPrefix(generation: Digest) [seen_prefix.len + 32]u8 {
    return prefixFor(seen_prefix, generation);
}
pub fn workPrefix(generation: Digest, id: TxnId, revision: u64) [seen_prefix.len + 56]u8 {
    var key: [seen_prefix.len + 56]u8 = undefined;
    @memcpy(key[0 .. seen_prefix.len + 32], &seenPrefix(generation));
    @memcpy(key[seen_prefix.len + 32 ..][0..16], &id);
    std.mem.writeInt(u64, key[seen_prefix.len + 48 ..][0..8], revision, .big);
    return key;
}
fn orphanKey(generation: Digest, id: TxnId, revision: u64) [orphan_prefix.len + 56]u8 {
    var key: [orphan_prefix.len + 56]u8 = undefined;
    @memcpy(key[0 .. orphan_prefix.len + 32], &orphanPrefix(generation));
    @memcpy(key[orphan_prefix.len + 32 ..][0..16], &id);
    std.mem.writeInt(u64, key[orphan_prefix.len + 48 ..][0..8], revision, .big);
    return key;
}
fn seenKey(generation: Digest, id: TxnId, revision: u64, kind: u8, name: Digest) [seen_prefix.len + 89]u8 {
    var key: [seen_prefix.len + 89]u8 = undefined;
    @memcpy(key[0 .. seen_prefix.len + 56], &workPrefix(generation, id, revision));
    key[seen_prefix.len + 56] = kind;
    @memcpy(key[seen_prefix.len + 57 ..], &name);
    return key;
}
fn prefixFor(comptime prefix: []const u8, generation: Digest) [prefix.len + 32]u8 {
    var key: [prefix.len + 32]u8 = undefined;
    @memcpy(key[0..prefix.len], prefix);
    @memcpy(key[prefix.len..], &generation);
    return key;
}
pub fn memberKey(generation: Digest, id: TxnId) [member_prefix.len + 48]u8 {
    return keyFor(member_prefix, generation, id);
}
pub fn dirtyKey(generation: Digest, id: TxnId) [dirty_prefix.len + 48]u8 {
    return keyFor(dirty_prefix, generation, id);
}
fn keyFor(comptime prefix: []const u8, generation: Digest, id: TxnId) [prefix.len + 48]u8 {
    var key: [prefix.len + 48]u8 = undefined;
    @memcpy(key[0 .. prefix.len + 32], &prefixFor(prefix, generation));
    @memcpy(key[prefix.len + 32 ..], &id);
    return key;
}
pub fn member(txn: anytype, generation: Digest, id: TxnId) !?Kind {
    const raw = txn.get(&memberKey(generation, id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 1) return error.InvalidTxnRecord;
    return std.enums.fromInt(Kind, raw[0]) orelse return error.InvalidTxnRecord;
}
fn erase(txn: anytype, key: []const u8) !void {
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}
pub fn setMember(txn: anytype, header: *Header, id: TxnId, next: ?Kind) !void {
    if (header.phase == .retired) return error.SchemaValidationJobChanged;
    const previous = try member(txn, header.scope.generation, id);
    if (previous == null and next != null) header.debt = std.math.add(u64, header.debt, 1) catch return error.InvalidTxnRecord;
    if (previous != null and next == null) header.debt = std.math.sub(u64, header.debt, 1) catch return error.InvalidTxnRecord;
    if (next) |kind| {
        try txn.put(&memberKey(header.scope.generation, id), &.{@backingInt(kind)});
    } else try erase(txn, &memberKey(header.scope.generation, id));
    if (next == .dirty) {
        try txn.put(&dirtyKey(header.scope.generation, id), &.{2});
        if (header.phase == .ready or header.phase == .gc) header.phase = .dirty;
        if (header.phase == .dirty) header.after = null;
    } else try erase(txn, &dirtyKey(header.scope.generation, id));
    if (header.pending) |pending| if (std.mem.eql(u8, &pending, &id) or
        (next == .dirty and std.mem.order(u8, &id, &pending) == .lt))
    {
        // Publishing the later pending cursor must not skip a newly dirty
        // predecessor. Retire its partial work and restart in key order.
        try txn.put(&orphanKey(header.scope.generation, pending, header.progress.work_revision), &.{1});
        header.pending = null;
        header.progress = .{};
    };
}
pub fn hasPrefix(txn: anytype, prefix: []const u8) !bool {
    const T = switch (@typeInfo(@TypeOf(txn))) {
        .pointer => |p| p.child,
        else => @TypeOf(txn),
    };
    if (comptime @hasDecl(T, "hasPrefix")) {
        // Provider prefix probes avoid snapshot clones and value hydration.
        return txn.hasPrefix(prefix);
    } else {
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (comptime @hasDecl(@TypeOf(cursor), "setKeysOnly")) cursor.setKeysOnly();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }
}
pub fn drained(txn: anytype, scope: Scope) !bool {
    const header = try require(txn, scope);
    if (header.phase != .ready or header.debt != 0) return false;
    if (try hasPrefix(txn, &memberPrefix(scope.generation)) or try hasPrefix(txn, &dirtyPrefix(scope.generation)) or
        try hasPrefix(txn, &seenPrefix(scope.generation)) or try hasPrefix(txn, &orphanPrefix(scope.generation))) return error.InvalidTxnRecord;
    return true;
}
pub fn retire(txn: anytype, scope: Scope) !void {
    if (!try drained(txn, scope)) return error.TransactionTopologyBusy;
    var header = try require(txn, scope);
    header.phase = .retired;
    try header.bump();
    try save(txn, header);
}

pub const Classification = union(enum) { more, done: ?Kind };
pub const Change = struct { id: TxnId, kind: ?Kind };
const OwnedKey = struct {
    bytes: [192]u8 = undefined,
    len: usize = 0,
    fn init(key: []const u8) !OwnedKey {
        if (key.len > 192) return error.InvalidTxnRecord;
        var result: OwnedKey = .{ .len = key.len };
        @memcpy(result.bytes[0..key.len], key);
        return result;
    }
    fn slice(self: *const OwnedKey) []const u8 {
        return self.bytes[0..self.len];
    }
};
/// Owned fixed-size preparation. No borrowed cursor/value escapes the pinned
/// read, and no legacy participant parsing is repeated under apply-exclusive.
pub const Page = struct {
    expected: Digest,
    next: Header,
    changes: [max_page_units]Change = undefined,
    len: usize = 0,
    units: usize = 0,
    marks: [max_page_units]OwnedKey = undefined,
    marks_len: usize = 0,
    removes: [max_page_units + 1]OwnedKey = undefined,
    removes_len: usize = 0,

    /// Bounded durable set membership instead of a cohort-sized hash table.
    /// Duplicate legacy list elements count once, including within this page.
    pub fn markSeen(self: *Page, points: anytype, kind: u8, name: Digest) !bool {
        if (kind != 1 and kind != 2) return error.InvalidTxnRecord;
        const key = seenKey(self.next.scope.generation, self.next.pending orelse return error.InvalidTxnRecord, self.next.progress.work_revision, kind, name);
        for (self.marks[0..self.marks_len]) |*marked| if (std.mem.eql(u8, marked.slice(), &key)) return true;
        const existing = points.get(&key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing) |bytes| {
            if (!std.mem.eql(u8, bytes, &.{1})) return error.InvalidTxnRecord;
            return true;
        }
        if (self.marks_len == self.marks.len) return error.InvalidTxnRecord;
        self.marks[self.marks_len] = try OwnedKey.init(&key);
        self.marks_len += 1;
        return false;
    }
    pub fn wasEnlisted(self: *const Page, points: anytype, name: Digest) !bool {
        const key = seenKey(self.next.scope.generation, self.next.pending orelse return error.InvalidTxnRecord, self.next.progress.work_revision, 1, name);
        for (self.marks[0..self.marks_len]) |*marked| if (std.mem.eql(u8, marked.slice(), &key)) return true;
        const bytes = points.get(&key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        if (!std.mem.eql(u8, bytes, &.{1})) return error.InvalidTxnRecord;
        return true;
    }
    fn remove(self: *Page, key: []const u8) !void {
        if (self.removes_len == self.removes.len) return error.InvalidTxnRecord;
        self.removes[self.removes_len] = try OwnedKey.init(key);
        self.removes_len += 1;
    }

    fn append(self: *Page, read: anytype, id: TxnId, kind: ?Kind) !void {
        if (self.len == self.changes.len) return error.InvalidTxnRecord;
        const old = try member(read, self.next.scope.generation, id);
        if (old == null and kind != null) self.next.debt = std.math.add(u64, self.next.debt, 1) catch return error.InvalidTxnRecord;
        if (old != null and kind == null) self.next.debt = std.math.sub(u64, self.next.debt, 1) catch return error.InvalidTxnRecord;
        self.changes[self.len] = .{ .id = id, .kind = kind };
        self.len += 1;
    }
    pub fn stage(self: *const Page, txn: anytype) !void {
        const current = try require(txn, self.next.scope);
        if (!std.mem.eql(u8, &try current.digest(), &self.expected)) return error.SchemaValidationPreparationChanged;
        const revision = std.math.add(u64, current.revision, 1) catch return error.InvalidTxnRecord;
        if (self.units > max_page_units or self.len > self.changes.len or self.marks_len > self.marks.len or self.removes_len > self.removes.len or
            self.next.revision != revision) return error.InvalidTxnRecord;
        var working = current;
        // Prepared cleanup already retires its exact work set. Only a racing
        // transaction mutation, not this page, creates an orphan GC objective.
        working.pending = null;
        working.progress = .{};
        for (self.marks[0..self.marks_len]) |*key| try txn.put(key.slice(), &.{1});
        for (self.removes[0..self.removes_len]) |*key| try erase(txn, key.slice());
        for (self.changes[0..self.len]) |change| try setMember(txn, &working, change.id, change.kind);
        if (working.debt != self.next.debt) return error.InvalidTxnRecord;
        try save(txn, self.next);
    }
};

fn nextId(cursor: anytype, prefix: []const u8, after: ?TxnId) !?TxnId {
    var key: [192]u8 = undefined;
    if (prefix.len + 16 > key.len) return error.InvalidTxnRecord;
    const entry = if (after) |id| blk: {
        @memcpy(key[0..prefix.len], prefix);
        @memcpy(key[prefix.len..][0..16], &id);
        const found = try cursor.seekAtOrAfter(key[0 .. prefix.len + 16]);
        break :blk if (found != null and std.mem.eql(u8, found.?.key, key[0 .. prefix.len + 16])) try cursor.next() else found;
    } else try cursor.seekAtOrAfter(prefix);
    if (entry) |row| {
        if (!std.mem.startsWith(u8, row.key, prefix)) return null;
        if (row.key.len != prefix.len + 16) return error.InvalidTxnRecord;
        return row.key[prefix.len..][0..16].*;
    }
    return null;
}

/// Classifier.fast is constant-work and marks legacy evidence dirty rather
/// than parsing a whole cohort. Classifier.step consumes at most one legacy
/// evidence unit and persists its exact cursor in Progress. Both execute on
/// the control/read lane; stage only checks the small header CAS and applies
/// this bounded owned delta. The independent point scope protects borrowed
/// legacy bytes while membership probes execute.
pub fn preparePage(read: anytype, points: anytype, scope: Scope, comptime record_prefix: []const u8, comptime Classifier: type, control: anytype) !?Page {
    try control.ensureActive();
    const current = try require(read, scope);
    if (current.phase == .ready) return null;
    var page: Page = .{ .expected = try current.digest(), .next = current };
    var cursor = try read.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    while (page.units < max_page_units) {
        try control.ensureActive();
        if (page.next.phase == .records) {
            const id = (try nextId(&cursor, record_prefix, page.next.after)) orelse {
                page.next.after = null;
                page.next.phase = if (page.next.debt == 0) .ready else .dirty;
                break;
            };
            try page.append(read, id, try Classifier.fast(read, id));
            page.next.after = id;
            page.units += 1;
        } else if (page.next.phase == .dirty) {
            const id = page.next.pending orelse (try nextId(&cursor, &dirtyPrefix(scope.generation), page.next.after)) orelse {
                page.next.after = null;
                page.next.phase = .gc;
                break;
            };
            if (try member(read, scope.generation, id) != .dirty) return error.InvalidTxnRecord;
            if (page.next.pending == null) {
                page.next.pending = id;
                page.next.progress.work_revision = current.revision;
            }
            if (page.next.progress.phase == .cleanup) {
                const scope_prefix = workPrefix(scope.generation, id, page.next.progress.work_revision);
                var seek: [192]u8 = undefined;
                @memcpy(seek[0..scope_prefix.len], &scope_prefix);
                const row = if (page.next.progress.offset == 0) try cursor.seekAtOrAfter(&scope_prefix) else blk: {
                    seek[scope_prefix.len] = @intCast(page.next.progress.offset);
                    @memcpy(seek[scope_prefix.len + 1 ..][0..32], &page.next.progress.after_member);
                    const found = try cursor.seekAtOrAfter(seek[0 .. scope_prefix.len + 33]);
                    break :blk if (found != null and std.mem.eql(u8, found.?.key, seek[0 .. scope_prefix.len + 33])) try cursor.next() else found;
                };
                if (row != null and std.mem.startsWith(u8, row.?.key, &scope_prefix)) {
                    const key = row.?.key;
                    if (key.len != scope_prefix.len + 33 or (key[scope_prefix.len] != 1 and key[scope_prefix.len] != 2)) return error.InvalidTxnRecord;
                    try page.remove(key);
                    page.next.progress.offset = key[scope_prefix.len];
                    page.next.progress.after_member = key[scope_prefix.len + 1 ..][0..32].*;
                } else {
                    const kind: ?Kind = if (page.next.progress.resolved != page.next.progress.enlisted) .blocked else null;
                    try page.append(read, id, kind);
                    page.next.pending = null;
                    page.next.progress = .{};
                    page.next.after = id;
                }
                page.units += 1;
                continue;
            }
            switch (try Classifier.step(read, points, id, &page)) {
                .more => {},
                .done => |kind| {
                    if (kind == .dirty) return error.InvalidTxnRecord;
                    try page.append(read, id, kind);
                    page.next.pending = null;
                    page.next.progress = .{};
                    page.next.after = id;
                },
            }
            page.units += 1;
            // Publish newly prepared uniqueness marks before their cleanup.
            if (page.next.progress.phase == .cleanup) break;
        } else if (page.next.phase == .gc) {
            const orphan_scope = orphanPrefix(scope.generation);
            const row = try cursor.seekAtOrAfter(&orphan_scope);
            if (row == null or !std.mem.startsWith(u8, row.?.key, &orphan_scope)) {
                if (try hasPrefix(read, &seenPrefix(scope.generation))) return error.InvalidTxnRecord;
                page.next.phase = .ready;
                break;
            }
            if (row.?.key.len != orphan_scope.len + 24) return error.InvalidTxnRecord;
            const id: TxnId = row.?.key[orphan_scope.len..][0..16].*;
            const revision = std.mem.readInt(u64, row.?.key[orphan_scope.len + 16 ..][0..8], .big);
            if (revision == 0 or revision >= current.revision) return error.InvalidTxnRecord;
            const obsolete_key = try OwnedKey.init(row.?.key);
            const obsolete = workPrefix(scope.generation, id, revision);
            var entry = try cursor.seekAtOrAfter(&obsolete);
            while (entry) |item| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, item.key, &obsolete)) break;
                if (page.units == max_page_units) break;
                try control.ensureActive();
                if (item.key.len != obsolete.len + 33) return error.InvalidTxnRecord;
                try page.remove(item.key);
                page.units += 1;
            }
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, &obsolete)) try page.remove(obsolete_key.slice());
            break;
        } else return error.InvalidTxnRecord;
    }
    try page.next.bump();
    try page.next.validate();
    try control.ensureActive();
    return page;
}

test "transaction debt header is canonical and epoch scoped" {
    const scope: Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(4), .epoch = 5 };
    const header: Header = .{ .scope = scope };
    const bytes = try header.encode();
    try std.testing.expectEqualDeep(header, try Header.decode(&bytes));
    for (0..bytes.len) |index| {
        var bad = bytes;
        bad[index] ^= 1;
        try std.testing.expectError(error.InvalidTxnRecord, Header.decode(&bad));
    }
    var noncanonical = bytes;
    noncanonical[92] = 1;
    std.crypto.hash.Blake3.hash(noncanonical[0..224], noncanonical[224..256], .{});
    try std.testing.expectError(error.InvalidTxnRecord, Header.decode(&noncanonical));
    var invalid = header;
    invalid.phase = .ready;
    invalid.after = @splat(0);
    try std.testing.expectError(error.InvalidTxnRecord, invalid.encode());
    try std.testing.expect(!std.mem.eql(u8, &memberKey(scope.generation, @splat(1)), &memberKey(@splat(6), @splat(1))));
}

test "transaction debt presence guards use native key probes without opening value cursors" {
    const Probe = struct {
        calls: usize = 0,
        pub fn hasPrefix(self: *@This(), prefix: []const u8) !bool {
            self.calls += 1;
            return std.mem.eql(u8, prefix, "present:");
        }
    };
    var probe: Probe = .{};
    try std.testing.expect(try hasPrefix(&probe, "present:"));
    try std.testing.expect(!try hasPrefix(&probe, "absent:"));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}
