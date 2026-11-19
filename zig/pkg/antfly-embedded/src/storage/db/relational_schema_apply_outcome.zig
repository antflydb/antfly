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

//! Exact committed schema-command outcomes, independent of the job's phase.
//! The outcome and ordered marker must share the effect's store transaction.
//! Missing evidence is recovery-required, never proof of success/noncommit.
//! Immutable namespace keys keep copied parent history inert after adoption.
const std = @import("std");
const Entry = @import("apply_receipts.zig").OrderedApplyReceipt;
pub const Namespace = @import("doc_identity_namespace.zig").Namespace;
pub const family_prefix = "\x00\x00__metadata__:schema_apply_outcome:";
pub const head_family_prefix = "\x00\x00__metadata__:schema_apply_head:";
pub const prefix = family_prefix ++ "v1:";
pub const head_prefix = head_family_prefix ++ "v1:";
pub const encoded_len = 120;
pub const Reason = enum(u8) {
    accepted = 0,
    job_changed = 1,
    canceled = 2,
    job_required = 3,
    schema_changed = 4,
    predecessor_changed = 5,
    publication_required = 6,
    topology_changed = 7,
    physical_catalog_changed = 8,
    owner_changed = 9,
    transactions_pending = 10,
};

fn requireCutCovered(entry: Entry, applied: Entry) !void {
    if (applied.term < entry.term or applied.index < entry.index or
        (applied.index == entry.index and applied.term != entry.term)) return error.CorruptSchemaApplyOutcome;
}

pub const Receipt = struct {
    namespace: Namespace,
    entry: Entry,
    command: [32]u8,
    reason: Reason,

    fn validate(self: Receipt) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.entry.term == 0 or self.entry.index == 0 or std.mem.allEqual(u8, &self.command, 0)) return error.CorruptSchemaApplyOutcome;
    }
    pub fn encode(self: Receipt) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "ASVO\x01\x00\x00\x00");
        std.mem.writeInt(u64, bytes[8..16], self.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[16..24], self.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[24..32], self.namespace.range_id, .little);
        std.mem.writeInt(u64, bytes[32..40], self.entry.term, .little);
        std.mem.writeInt(u64, bytes[40..48], self.entry.index, .little);
        @memcpy(bytes[48..80], &self.command);
        bytes[80] = @backingInt(self.reason);
        std.crypto.hash.Blake3.hash(bytes[0..88], bytes[88..120], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "ASVO\x01\x00\x00\x00") or
            !std.mem.allEqual(u8, bytes[81..88], 0)) return error.CorruptSchemaApplyOutcome;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..88], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[88..120])) return error.CorruptSchemaApplyOutcome;
        const result: Receipt = .{
            .namespace = .{ .table_id = std.mem.readInt(u64, bytes[8..16], .little), .shard_id = std.mem.readInt(u64, bytes[16..24], .little), .range_id = std.mem.readInt(u64, bytes[24..32], .little) },
            .entry = .{ .term = std.mem.readInt(u64, bytes[32..40], .little), .index = std.mem.readInt(u64, bytes[40..48], .little) },
            .command = bytes[48..80].*,
            .reason = std.enums.fromInt(Reason, bytes[80]) orelse return error.CorruptSchemaApplyOutcome,
        };
        try result.validate();
        return result;
    }
    pub fn requireCoveredBy(self: Receipt, applied: Entry) !void {
        try requireCutCovered(self.entry, applied);
    }
    pub fn require(self: Receipt, namespace: Namespace, applied: Entry, entry: Entry, command: [32]u8) !void {
        if (!self.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
        if (!std.meta.eql(self.entry, entry) or !std.mem.eql(u8, &self.command, &command)) return error.ConflictingOrderedApplyReceipt;
        try self.requireCoveredBy(applied);
        if (self.reason != .accepted) return error.SchemaValidationCommandRejected;
    }
};

/// Namespace prefix permits bounded range GC without mixing clock domains.
pub fn namespacePrefix(namespace: Namespace) [prefix.len + 24]u8 {
    var result: [prefix.len + 24]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.mem.writeInt(u64, result[prefix.len..][0..8], namespace.table_id, .big);
    std.mem.writeInt(u64, result[prefix.len + 8 ..][0..8], namespace.shard_id, .big);
    std.mem.writeInt(u64, result[prefix.len + 16 ..][0..8], namespace.range_id, .big);
    return result;
}
pub fn key(namespace: Namespace, index: u64) [prefix.len + 32]u8 {
    var result: [prefix.len + 32]u8 = undefined;
    const scope = namespacePrefix(namespace);
    @memcpy(result[0..scope.len], &scope);
    std.mem.writeInt(u64, result[scope.len..], index, .big);
    return result;
}
pub fn read(txn: anytype, namespace: Namespace, index: u64) !?Receipt {
    const raw = txn.get(&key(namespace, index)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try validateEntry(&key(namespace, index), raw);
}

/// Canonical key/value verification is shared by point replay and staged
/// snapshot validation. A checksummed value cannot be relabeled at another key.
pub fn validateEntry(stored_key: []const u8, raw: []const u8) !Receipt {
    const result = try Receipt.decode(raw);
    if (!std.mem.eql(u8, stored_key, &key(result.namespace, result.entry.index))) return error.CorruptSchemaApplyOutcome;
    return result;
}

pub const SnapshotStats = struct { records: u64 = 0, foreign_records: u64 = 0 };

/// Commitment from a separately verified source image. The cut is the actual
/// data-entry marker, not a snapshot's possibly larger completed prefix.
pub const Anchor = struct {
    cut: ?Entry,
    head: ?[32]u8,
    record_digest: ?[32]u8 = null,

    pub fn validate(self: Anchor, current: ?Entry) !void {
        if (self.cut) |cut| {
            if (cut.term == 0 or cut.index == 0) return error.CorruptSchemaApplyOutcome;
            try requireCutCovered(cut, current orelse return error.SchemaValidationRuntimeRecoveryRequired);
        } else if (self.head != null) return error.CorruptSchemaApplyOutcome;
        if (self.head) |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.CorruptSchemaApplyOutcome;
        if (self.record_digest) |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.CorruptSchemaApplyOutcome;
    }
    pub fn require(self: Anchor, head: ?Head) !void {
        const actual: ?[32]u8 = if (head) |value| blk: {
            try value.requireCoveredBy(self.cut orelse return error.CorruptSchemaApplyOutcome);
            const bytes = try value.encode();
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(&bytes, &digest, .{});
            break :blk digest;
        } else null;
        if (!std.meta.eql(self.head, actual)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
};

/// Cooperative work policy shared with the transaction-image auditor. IO
/// latency remains the backend's admitted-read contract, not a hard deadline.
pub const AuditBudget = struct {
    records: usize = 64,
    time_ns: u64 = 5 * std.time.ns_per_ms,
    io: ?std.Io = null,

    fn validate(self: @This()) !void {
        if (self.records == 0 or self.records > 64 or self.time_ns == 0 or self.time_ns > std.time.ns_per_s)
            return error.InvalidSchemaValidationJob;
    }
};

/// Fixed-size completeness commitment for one namespace's append-only ordered
/// outcomes. This is not a physical-root adoption grant or a GC watermark.
pub const Head = struct {
    namespace: Namespace,
    last: Entry,
    count: u64,
    digest: [32]u8,

    pub fn requireCoveredBy(self: Head, applied: Entry) !void {
        try self.validate();
        try requireCutCovered(self.last, applied);
    }

    fn validate(self: Head) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.last.term == 0 or self.last.index == 0 or self.count == 0 or self.count > self.last.index or
            std.mem.allEqual(u8, &self.digest, 0)) return error.CorruptSchemaApplyOutcome;
    }
    pub fn encode(self: Head) ![120]u8 {
        try self.validate();
        var bytes: [120]u8 = undefined;
        @memcpy(bytes[0..8], "ASVH\x01\x00\x00\x00");
        std.mem.writeInt(u64, bytes[8..16], self.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[16..24], self.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[24..32], self.namespace.range_id, .little);
        std.mem.writeInt(u64, bytes[32..40], self.last.term, .little);
        std.mem.writeInt(u64, bytes[40..48], self.last.index, .little);
        std.mem.writeInt(u64, bytes[48..56], self.count, .little);
        @memcpy(bytes[56..88], &self.digest);
        std.crypto.hash.Blake3.hash(bytes[0..88], bytes[88..120], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len != 120 or !std.mem.eql(u8, bytes[0..8], "ASVH\x01\x00\x00\x00")) return error.CorruptSchemaApplyOutcome;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..88], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[88..120])) return error.CorruptSchemaApplyOutcome;
        const result: Head = .{
            .namespace = .{ .table_id = std.mem.readInt(u64, bytes[8..16], .little), .shard_id = std.mem.readInt(u64, bytes[16..24], .little), .range_id = std.mem.readInt(u64, bytes[24..32], .little) },
            .last = .{ .term = std.mem.readInt(u64, bytes[32..40], .little), .index = std.mem.readInt(u64, bytes[40..48], .little) },
            .count = std.mem.readInt(u64, bytes[48..56], .little),
            .digest = bytes[56..88].*,
        };
        try result.validate();
        return result;
    }
};

pub fn headKey(namespace: Namespace) [head_prefix.len + 24]u8 {
    var result: [head_prefix.len + 24]u8 = undefined;
    @memcpy(result[0..head_prefix.len], head_prefix);
    const suffix = namespacePrefix(namespace);
    @memcpy(result[head_prefix.len..], suffix[prefix.len..]);
    return result;
}
pub fn readHead(txn: anytype, namespace: Namespace) !?Head {
    const raw = txn.get(&headKey(namespace)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const result = try validateHeadEntry(&headKey(namespace), raw);
    return result;
}
fn validateHeadEntry(stored_key: []const u8, raw: []const u8) !Head {
    const result = try Head.decode(raw);
    if (!std.mem.eql(u8, stored_key, &headKey(result.namespace))) return error.CorruptSchemaApplyOutcome;
    return result;
}
pub fn requireHeadCut(txn: anytype, namespace: Namespace, applied: ?Entry) !?Head {
    const head = (try readHead(txn, namespace)) orelse return null;
    try requireCutCovered(head.last, applied orelse return error.CorruptSchemaApplyOutcome);
    return head;
}
pub fn requireReplay(txn: anytype, namespace: Namespace, applied: Entry, entry: Entry, command: [32]u8) !void {
    const receipt = (try read(txn, namespace, entry.index)) orelse return error.SchemaValidationRuntimeRecoveryRequired;
    const head = (try requireHeadCut(txn, namespace, applied)) orelse return error.SchemaValidationRuntimeRecoveryRequired;
    // The head is an independently checksummed local cut, never a replacement
    // for the exact command outcome or the actual primary-store watermark.
    try receipt.requireCoveredBy(head.last);
    try receipt.require(namespace, applied, entry, command);
}
fn extendDigest(previous: [32]u8, encoded: []const u8) [32]u8 {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/schema-apply-history/v1");
    state.update(&previous);
    state.update(encoded);
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}

/// The caller supplies one immutable staging/snapshot read. Only outcome/head
/// families are streamed; no retained payloads or schema-sized allocation graph.
/// Foreign namespaces have independent clocks: validate their bytes, but never
/// treat their history as coverage or adoption authority for this owner.
pub fn SnapshotAudit(comptime Read: type) type {
    const Cursor = @typeInfo(@TypeOf(@as(Read, undefined).openCursor())).error_union.payload;
    return struct {
        // The owner must retain this immutable image until deinit. No request
        // allocator, borrowed row value or cancellation callback is retained.
        read: Read,
        namespace: Namespace,
        marker: ?Entry,
        expected: ?Head,
        anchor: ?Anchor,
        anchor_head: ?Head = null,
        cursor: Cursor,
        phase: enum { heads, outcomes, done } = .heads,
        started: bool = false,
        previous: ?Receipt = null,
        stats: SnapshotStats = .{},
        digest: [32]u8 = @splat(0),
        local_count: u64 = 0,
        last: ?Entry = null,
        invalid: bool = false,
        last_units: usize = 0,

        pub fn init(image: Read, namespace: Namespace, control: anytype) !@This() {
            return initAnchored(image, namespace, null, control);
        }
        pub fn initAnchored(image: Read, namespace: Namespace, anchor: ?Anchor, control: anytype) !@This() {
            try control.check();
            const marker = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(image);
            const expected = try requireHeadCut(image, namespace, marker);
            if (anchor) |value| try value.validate(marker);
            return .{ .read = image, .namespace = namespace, .marker = marker, .expected = expected, .anchor = anchor, .cursor = try image.openCursor() };
        }
        pub fn deinit(self: *@This()) void {
            self.cursor.close();
            self.* = undefined;
        }
        pub fn result(self: *const @This()) !SnapshotStats {
            if (self.invalid or self.phase != .done) return error.SchemaValidationRuntimeRecoveryRequired;
            return self.stats;
        }
        pub fn anchoredHead(self: *const @This()) !?Head {
            _ = try self.result();
            if (self.anchor == null) return error.SchemaValidationRuntimeRecoveryRequired;
            return self.anchor_head;
        }
        pub fn step(self: *@This(), budget: AuditBudget, control: anytype) !bool {
            // Cancellation before acceptance leaves the cursor resumable.
            try control.check();
            try budget.validate();
            if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
            self.last_units = 0;
            if (self.phase == .done) return true;
            errdefer self.invalid = true;
            const start = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
            while (self.last_units < budget.records) {
                try control.check();
                const family = if (self.phase == .heads) head_family_prefix else family_prefix;
                const row = if (!self.started) blk: {
                    self.started = true;
                    break :blk try self.cursor.seekAtOrAfter(family);
                } else try self.cursor.next();
                self.last_units += 1;
                if (row != null and std.mem.startsWith(u8, row.?.key, family)) {
                    if (self.phase == .heads) {
                        _ = try validateHeadEntry(row.?.key, row.?.value);
                    } else try self.accept(row.?.key, row.?.value);
                } else if (self.phase == .heads) {
                    self.phase = .outcomes;
                    self.started = false;
                } else {
                    try self.finish();
                    self.phase = .done;
                    return true;
                }
                if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - start >= budget.time_ns) return false;
            }
            return false;
        }
        fn accept(self: *@This(), stored_key: []const u8, raw: []const u8) !void {
            const receipt = try validateEntry(stored_key, raw);
            if (self.previous) |prior| if (prior.namespace.eql(receipt.namespace) and
                (prior.entry.index >= receipt.entry.index or prior.entry.term > receipt.entry.term)) return error.CorruptSchemaApplyOutcome;
            if (receipt.namespace.eql(self.namespace)) {
                try receipt.requireCoveredBy(self.marker orelse return error.CorruptSchemaApplyOutcome);
                self.digest = extendDigest(self.digest, raw);
                self.local_count += 1;
                self.last = receipt.entry;
                if (self.anchor) |anchor| if (anchor.cut) |cut| if (receipt.entry.index <= cut.index) {
                    try receipt.requireCoveredBy(cut);
                    self.anchor_head = .{ .namespace = self.namespace, .last = receipt.entry, .count = self.local_count, .digest = self.digest };
                };
            } else self.stats.foreign_records += 1;
            self.stats.records += 1;
            self.previous = receipt;
        }
        fn finish(self: *const @This()) !void {
            if (self.expected) |head| {
                if (self.local_count < head.count) return error.SchemaValidationRuntimeRecoveryRequired;
                if (head.count != self.local_count or self.last == null or !std.meta.eql(head.last, self.last.?) or
                    !std.mem.eql(u8, &head.digest, &self.digest)) return error.CorruptSchemaApplyOutcome;
            } else if (self.local_count != 0) return error.SchemaValidationRuntimeRecoveryRequired;
            if (self.anchor) |anchor| try anchor.require(self.anchor_head);
        }
    };
}

/// Restore and retained owner qualification drive the same verifier. A valid
/// prefix or copied foreign clock cannot stand in for the final local head.
pub fn validateSnapshot(txn: anytype, namespace: Namespace, cancellation: anytype) !SnapshotStats {
    var audit = try SnapshotAudit(@TypeOf(txn)).init(txn, namespace, cancellation);
    defer audit.deinit();
    while (!try audit.step(.{}, cancellation)) {}
    return try audit.result();
}
pub fn stage(txn: anytype, value: Receipt) !void {
    const head = try readHead(txn, value.namespace);
    if (try read(txn, value.namespace, value.entry.index)) |previous| {
        if (!std.meta.eql(previous, value)) return error.ConflictingOrderedApplyReceipt;
        const retained = head orelse return error.SchemaValidationRuntimeRecoveryRequired;
        try value.requireCoveredBy(retained.last);
        return;
    }
    if (head) |previous| {
        if (previous.last.index >= value.entry.index or previous.last.term > value.entry.term) return error.CorruptSchemaApplyOutcome;
    } else {
        // A missing head beside existing local history is not permission to
        // rebuild or bless it from a caller's new command.
        var cursor = try txn.openCursor();
        defer cursor.close();
        const scope = namespacePrefix(value.namespace);
        if (try cursor.seekAtOrAfter(&scope)) |row|
            if (std.mem.startsWith(u8, row.key, &scope)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
    const bytes = try value.encode();
    const next: Head = .{
        .namespace = value.namespace,
        .last = value.entry,
        .count = std.math.add(u64, if (head) |previous| previous.count else 0, 1) catch return error.CorruptSchemaApplyOutcome,
        .digest = extendDigest(if (head) |previous| previous.digest else @splat(0), &bytes),
    };
    try txn.put(&key(value.namespace, value.entry.index), &bytes);
    try txn.put(&headKey(value.namespace), &try next.encode());
}

test "schema apply outcomes history heads reject corruption and foreign namespace cuts" {
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const head: Head = .{ .namespace = namespace, .last = .{ .term = 2, .index = 9 }, .count = 3, .digest = @splat(4) };
    const encoded = try head.encode();
    try std.testing.expectEqualDeep(head, try Head.decode(&encoded));
    for (0..encoded.len) |i| {
        var corrupt = encoded;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.CorruptSchemaApplyOutcome, Head.decode(&corrupt));
    }
    var invalid = head;
    invalid.count = 10;
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, invalid.encode());
    invalid.count = 0;
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, invalid.encode());
    const Reader = struct {
        bytes: []const u8,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.bytes;
        }
    };
    var reader: Reader = .{ .bytes = &encoded };
    var foreign = namespace;
    foreign.range_id += 1;
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, readHead(&reader, foreign));
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, requireHeadCut(&reader, namespace, null));
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, requireHeadCut(&reader, namespace, .{ .term = 3, .index = 9 }));
    try std.testing.expectEqualDeep(head, (try requireHeadCut(&reader, namespace, .{ .term = 3, .index = 10 })).?);
    const receipt: Receipt = .{ .namespace = namespace, .entry = head.last, .command = @splat(6), .reason = .accepted };
    try std.testing.expect(!std.mem.eql(u8, &extendDigest(@splat(0), &try receipt.encode()), &extendDigest(@splat(1), &try receipt.encode())));
}

test "schema apply outcomes retain exact rejection command identity and canonical bytes" {
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const receipt: Receipt = .{ .namespace = namespace, .entry = .{ .term = 2, .index = 9 }, .command = @splat(7), .reason = .canceled };
    const encoded = try receipt.encode();
    try std.testing.expectEqualDeep(receipt, try Receipt.decode(&encoded));
    try std.testing.expectError(error.SchemaValidationCommandRejected, receipt.require(namespace, receipt.entry, receipt.entry, receipt.command));
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, receipt.require(namespace, receipt.entry, .{ .term = 3, .index = 9 }, receipt.command));
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, receipt.require(namespace, receipt.entry, receipt.entry, @splat(8)));
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, receipt.require(namespace, .{ .term = 1, .index = 10 }, receipt.entry, receipt.command));
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, receipt.require(namespace, .{ .term = 2, .index = 8 }, receipt.entry, receipt.command));
    var accepted = receipt;
    accepted.reason = .accepted;
    try accepted.require(namespace, accepted.entry, accepted.entry, accepted.command);
    for (0..encoded.len) |i| {
        var corrupt = encoded;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.CorruptSchemaApplyOutcome, Receipt.decode(&corrupt));
    }
    var noncanonical = encoded;
    noncanonical[81] = 1;
    std.crypto.hash.Blake3.hash(noncanonical[0..88], noncanonical[88..120], .{});
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, Receipt.decode(&noncanonical));
    try std.testing.expect(std.mem.order(u8, &key(namespace, 9), &key(namespace, 10)) == .lt);
    var foreign = namespace;
    foreign.range_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &key(namespace, 9), &key(foreign, 9)));
    try std.testing.expectError(error.IdentityNamespaceMismatch, receipt.require(foreign, receipt.entry, receipt.entry, receipt.command));
    const Fixture = struct {
        bytes: []const u8,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.bytes;
        }
    };
    var fixture: Fixture = .{ .bytes = &encoded };
    try std.testing.expectError(error.CorruptSchemaApplyOutcome, read(&fixture, foreign, receipt.entry.index));
}
