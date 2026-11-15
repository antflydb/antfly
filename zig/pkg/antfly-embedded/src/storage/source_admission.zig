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

//! Trusted source admission compiled into canonical native transaction intents.
//! Opaque provider checkpoints are never ordered. A separate monotonic source
//! sequence fences expired retries while the exact receipt window stays bounded.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const prefix = "\x00\x00__metadata__:source_admission:";
const head_prefix = prefix ++ "head:";
const receipt_prefix = prefix ++ "receipt:";
const slot_prefix = prefix ++ "slot:";
pub const default_receipt_window: u32 = 1024;
pub const max_receipt_window: u32 = 65536;

pub fn isProtectedKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, prefix);
}
pub fn isKey(key: []const u8) bool {
    return (key.len == head_prefix.len + 32 and std.mem.startsWith(u8, key, head_prefix)) or
        (key.len == receipt_prefix.len + 32 and std.mem.startsWith(u8, key, receipt_prefix)) or
        (key.len == slot_prefix.len + 36 and std.mem.startsWith(u8, key, slot_prefix));
}
fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn number(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn field(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    number(hash, bytes.len);
    hash.update(bytes);
}
pub const Command = struct {
    table_id: u64,
    generation: u64,
    source: []const u8,
    epoch: []const u8,
    definition_digest: Digest,
    batch_id: []const u8,
    source_sequence: u64,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    payload_digest: Digest,
    participant_digest: Digest,
    accepted_at_ns: u64,
    receipt_window: u32 = default_receipt_window,

    pub fn validate(self: Command) !void {
        if (self.table_id == 0 or self.generation == 0 or self.source_sequence == 0 or
            self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or
            self.receipt_window == 0 or self.receipt_window > max_receipt_window or
            self.source.len == 0 or self.source.len > 256 or self.epoch.len == 0 or self.epoch.len > 256 or
            self.batch_id.len == 0 or self.batch_id.len > 256 or self.checkpoint.len == 0 or self.checkpoint.len > 1024)
            return error.InvalidSourceAdmission;
        if (self.expected_checkpoint) |previous| if (previous.len == 0 or previous.len > 1024) return error.InvalidSourceAdmission;
    }
    pub fn binding(self: Command) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-binding:v1:");
        number(&hash, self.table_id);
        number(&hash, self.generation);
        field(&hash, self.source);
        field(&hash, self.epoch);
        hash.update(&self.definition_digest);
        number(&hash, self.receipt_window);
        return hash.finalResult();
    }
    pub fn identity(self: Command) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-batch:v1:");
        hash.update(&self.binding());
        field(&hash, self.batch_id);
        return hash.finalResult();
    }
    pub fn transactionId(self: Command) [16]u8 {
        return self.identity()[0..16].*;
    }
    pub fn fingerprint(self: Command) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-admission:v1:");
        hash.update(&self.identity());
        number(&hash, self.source_sequence);
        number(&hash, self.accepted_at_ns);
        field(&hash, self.checkpoint);
        number(&hash, @intFromBool(self.expected_checkpoint != null));
        if (self.expected_checkpoint) |previous| field(&hash, previous);
        hash.update(&self.payload_digest);
        hash.update(&self.participant_digest);
        return hash.finalResult();
    }
    pub fn jsonStringify(self: Command, stream: anytype) @TypeOf(stream.*).Error!void {
        try @import("db/relational_integrity_json.zig").write(self, stream);
    }
};

/// Canonical input effects shared by bound and routed admission. No temporary
/// serialization proportional to a transaction's payload is retained.
pub fn operationsDigest(request: anytype) Digest {
    const Output = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            for (data[0 .. data.len - 1]) |bytes| self.hash.update(bytes);
            for (0..splat) |_| self.hash.update(data[data.len - 1]);
            return std.Io.Writer.countSplat(data, splat);
        }
    };
    var output: Output = .{};
    output.hash.update("antfly:source-participant-effects:v1:");
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    stream.beginObject() catch unreachable;
    inline for (.{ "writes", "deletes", "transforms", "graph_writes", "graph_deletes", "predicates", "integrity", "integrity_commands", "relational_activation", "relational_retirement", "relational_index_maintenance", "relational_schema_version", "relational_integrity_generation_set", "restore_staging_scope", "restore_staging_plan_id", "relational_repair", "schema_version", "normalized_graph_rows" }) |name| {
        stream.objectField(name) catch unreachable;
        @import("db/relational_integrity_json.zig").write(@field(request, name), &stream) catch unreachable;
    }
    stream.endObject() catch unreachable;
    return output.hash.finalResult();
}

pub const Head = struct {
    table_id: u64,
    binding_digest: Digest,
    generation: u64,
    source_sequence: u64,
    ordinal: u64,
    receipt_window: u32,
    checkpoint: []const u8,
    pub fn encode(self: Head, a: A) ![]u8 {
        if (self.checkpoint.len == 0 or self.checkpoint.len > 1024) return error.InvalidSourceAdmission;
        const out = try a.alloc(u8, 108 + self.checkpoint.len);
        @memcpy(out[0..4], "ASH1");
        std.mem.writeInt(u64, out[4..12], self.table_id, .little);
        @memcpy(out[12..44], &self.binding_digest);
        std.mem.writeInt(u64, out[44..52], self.generation, .little);
        std.mem.writeInt(u64, out[52..60], self.source_sequence, .little);
        std.mem.writeInt(u64, out[60..68], self.ordinal, .little);
        std.mem.writeInt(u32, out[68..72], self.receipt_window, .little);
        std.mem.writeInt(u32, out[72..76], @intCast(self.checkpoint.len), .little);
        @memcpy(out[76 .. out.len - 32], self.checkpoint);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }
    pub fn decode(raw: []const u8) !Head {
        if (raw.len < 109 or raw.len > 1132 or !std.mem.eql(u8, raw[0..4], "ASH1") or
            std.mem.readInt(u32, raw[72..76], .little) != raw.len - 108 or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &digest(raw[0 .. raw.len - 32]))) return error.SourceAdmissionCorrupt;
        const result: Head = .{ .table_id = std.mem.readInt(u64, raw[4..12], .little), .binding_digest = raw[12..44].*, .generation = std.mem.readInt(u64, raw[44..52], .little), .source_sequence = std.mem.readInt(u64, raw[52..60], .little), .ordinal = std.mem.readInt(u64, raw[60..68], .little), .receipt_window = std.mem.readInt(u32, raw[68..72], .little), .checkpoint = raw[76 .. raw.len - 32] };
        if (result.table_id == 0 or result.generation == 0 or result.source_sequence == 0 or result.ordinal == 0 or result.receipt_window == 0 or result.receipt_window > max_receipt_window) return error.SourceAdmissionCorrupt;
        return result;
    }
};
pub const Receipt = struct {
    table_id: u64,
    identity: Digest,
    request_digest: Digest,
    payload_digest: Digest,
    source_sequence: u64,
    ordinal: u64,
    transaction_id: [16]u8,
    accepted_at_ns: u64,
    pub fn encode(self: Receipt) [180]u8 {
        var out: [180]u8 = undefined;
        @memcpy(out[0..4], "ASR1");
        std.mem.writeInt(u64, out[4..12], self.table_id, .little);
        @memcpy(out[12..44], &self.identity);
        @memcpy(out[44..76], &self.request_digest);
        @memcpy(out[76..108], &self.payload_digest);
        std.mem.writeInt(u64, out[108..116], self.source_sequence, .little);
        std.mem.writeInt(u64, out[116..124], self.ordinal, .little);
        @memcpy(out[124..140], &self.transaction_id);
        std.mem.writeInt(u64, out[140..148], self.accepted_at_ns, .little);
        @memcpy(out[148..180], &digest(out[0..148]));
        return out;
    }
    pub fn decode(raw: []const u8) !Receipt {
        if (raw.len != 180 or !std.mem.eql(u8, raw[0..4], "ASR1") or !std.mem.eql(u8, raw[148..180], &digest(raw[0..148]))) return error.SourceAdmissionCorrupt;
        const result: Receipt = .{ .table_id = std.mem.readInt(u64, raw[4..12], .little), .identity = raw[12..44].*, .request_digest = raw[44..76].*, .payload_digest = raw[76..108].*, .source_sequence = std.mem.readInt(u64, raw[108..116], .little), .ordinal = std.mem.readInt(u64, raw[116..124], .little), .transaction_id = raw[124..140].*, .accepted_at_ns = std.mem.readInt(u64, raw[140..148], .little) };
        if (result.table_id == 0 or result.source_sequence == 0 or result.ordinal == 0 or result.accepted_at_ns == 0) return error.SourceAdmissionCorrupt;
        return result;
    }
};
pub fn receiptKey(identity: Digest) [receipt_prefix.len + 32]u8 {
    return receipt_prefix.* ++ identity;
}
/// Stable scope excludes epoch/generation, so a new binding cannot silently
/// abandon a previous binding's checkpoint or retention ring.
fn sourceScope(command: Command) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-scope:v1:");
    number(&hash, command.table_id);
    field(&hash, command.source);
    return hash.finalResult();
}
pub fn headKey(command: Command) [head_prefix.len + 32]u8 {
    return head_prefix.* ++ sourceScope(command);
}
fn slotKey(command: Command, ordinal: u64) [slot_prefix.len + 36]u8 {
    var result: [slot_prefix.len + 36]u8 = undefined;
    @memcpy(result[0..slot_prefix.len], slot_prefix);
    @memcpy(result[slot_prefix.len..][0..32], &sourceScope(command));
    std.mem.writeInt(u32, result[slot_prefix.len + 32 ..], @intCast(ordinal % command.receipt_window), .big);
    return result;
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
}
fn pendingReceiptKey(command: Command) ["\x00\x00__txn_intents__:".len + 17 + receipt_prefix.len + 32]u8 {
    return "\x00\x00__txn_intents__:".* ++ command.transactionId() ++ ":".* ++ receiptKey(command.identity());
}
fn requireReceipt(receipt: Receipt, table_id: u64, command: Command) !void {
    if (receipt.table_id != table_id or !std.mem.eql(u8, &receipt.identity, &command.identity()) or
        !std.mem.eql(u8, &receipt.request_digest, &command.fingerprint()) or
        !std.mem.eql(u8, &receipt.transaction_id, &command.transactionId())) return error.SourcePayloadConflict;
}
fn requireNext(command: Command, previous: ?Head) !void {
    if (previous) |head| {
        if (head.table_id != command.table_id or head.generation != command.generation or !std.mem.eql(u8, &head.binding_digest, &command.binding()) or head.receipt_window != command.receipt_window) return error.SourceBindingConflict;
        if (command.source_sequence <= head.source_sequence) return error.SourceReceiptExpired;
        if (command.expected_checkpoint == null or !std.mem.eql(u8, head.checkpoint, command.expected_checkpoint.?)) return error.SourceCheckpointConflict;
    } else if (command.expected_checkpoint != null) return error.SourceCheckpointConflict;
}
pub const Status = union(enum) {
    absent,
    prepared: Receipt,
    committed: Receipt,
};
pub fn lookup(txn: anytype, table_id: u64, command: Command) !?Receipt {
    return switch (try lookupStatus(txn, table_id, command)) {
        .committed => |receipt| receipt,
        .absent, .prepared => null,
    };
}
pub fn lookupStatus(txn: anytype, table_id: u64, command: Command) !Status {
    try command.validate();
    if (table_id != command.table_id) return error.SourceTableMismatch;
    if (try optional(txn, &receiptKey(command.identity()))) |raw| {
        const receipt = try Receipt.decode(raw);
        try requireReceipt(receipt, table_id, command);
        return .{ .committed = receipt };
    }
    // A prepared source transaction owns its exact payload before its receipt
    // is visible in primary state. Never allow retries to replace that vote
    // with another set of rows under the same durable transaction identity.
    var prepared: ?Receipt = null;
    if (try optional(txn, &pendingReceiptKey(command))) |raw| {
        const pending = try @import("transaction_intent_value.zig").Value.decode(raw);
        if (pending.prepared_row != null or pending.value == null) return error.SourceAdmissionCorrupt;
        prepared = try Receipt.decode(pending.value.?);
        try requireReceipt(prepared.?, table_id, command);
    }
    const raw = try optional(txn, &headKey(command));
    try requireNext(command, if (raw) |bytes| try Head.decode(bytes) else null);
    return if (prepared) |receipt| .{ .prepared = receipt } else .absent;
}

pub const Pair = struct { key: []const u8, value: []const u8 };
pub const Plan = struct {
    head_key: []const u8,
    head_before: ?[]const u8,
    writes: []const Pair,
    deletes: []const []const u8,
    receipt: Receipt,
};
/// All returned bytes belong to an attempt arena. The caller MUST add an exact
/// predicate on plan.head_key/head_before, and commit every write/delete with rows.
pub fn prepare(a: A, txn: anytype, table_id: u64, txn_id: [16]u8, command: Command) !Plan {
    if (!std.mem.eql(u8, &txn_id, &command.transactionId())) return error.SourceTransactionIdentityConflict;
    if (try lookup(txn, table_id, command) != null) return error.SourceAlreadyCommitted;
    const head_before = if (try optional(txn, &headKey(command))) |raw| try a.dupe(u8, raw) else null;
    const previous = if (head_before) |raw| try Head.decode(raw) else null;
    try requireNext(command, previous);
    const ordinal = if (previous) |head| std.math.add(u64, head.ordinal, 1) catch return error.SourceAdmissionCorrupt else 1;
    const receipt: Receipt = .{ .table_id = table_id, .identity = command.identity(), .request_digest = command.fingerprint(), .payload_digest = command.payload_digest, .source_sequence = command.source_sequence, .ordinal = ordinal, .transaction_id = txn_id, .accepted_at_ns = command.accepted_at_ns };
    const next: Head = .{ .table_id = table_id, .binding_digest = command.binding(), .generation = command.generation, .source_sequence = command.source_sequence, .ordinal = ordinal, .receipt_window = command.receipt_window, .checkpoint = command.checkpoint };
    const selected_slot = slotKey(command, ordinal);
    var deletes: std.ArrayList([]const u8) = .empty;
    if (try optional(txn, &selected_slot)) |raw| {
        if (raw.len != 32) return error.SourceAdmissionCorrupt;
        const old_key = receiptKey(raw[0..32].*);
        const old = try Receipt.decode((try optional(txn, &old_key)) orelse return error.SourceAdmissionCorrupt);
        if (old.ordinal +| command.receipt_window != ordinal or old.table_id != table_id) return error.SourceAdmissionCorrupt;
        try deletes.append(a, try a.dupe(u8, &old_key));
    }
    const writes = try a.alloc(Pair, 3);
    writes[0] = .{ .key = try a.dupe(u8, &headKey(command)), .value = try next.encode(a) };
    writes[1] = .{ .key = try a.dupe(u8, &receiptKey(receipt.identity)), .value = try a.dupe(u8, &receipt.encode()) };
    writes[2] = .{ .key = try a.dupe(u8, &selected_slot), .value = try a.dupe(u8, &receipt.identity) };
    return .{ .head_key = writes[0].key, .head_before = head_before, .writes = writes, .deletes = deletes.items, .receipt = receipt };
}

test "source admission bounds receipts and fences altered retries and cyclic opaque checkpoints" {
    const a = std.testing.allocator;
    const Store = struct {
        values: std.StringHashMap([]const u8),
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        fn apply(self: *@This(), plan: Plan) !void {
            const previous = try optional(self, plan.head_key);
            if ((previous == null) != (plan.head_before == null) or (previous != null and !std.mem.eql(u8, previous.?, plan.head_before.?))) return error.VersionConflict;
            for (plan.deletes) |key| _ = self.values.remove(key);
            for (plan.writes) |write| try self.values.put(write.key, write.value);
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var store: Store = .{ .values = .init(scratch) };
    var command: Command = .{ .table_id = 7, .generation = 1, .source = "postgres", .epoch = "slot-1", .definition_digest = @splat(1), .batch_id = "one", .source_sequence = 10, .checkpoint = "cycle", .payload_digest = @splat(2), .participant_digest = @splat(3), .accepted_at_ns = 100, .receipt_window = 2 };
    const first = command;
    const prepared = try prepare(scratch, &store, 7, command.transactionId(), command);
    const pending_key = try scratch.dupe(u8, &pendingReceiptKey(command));
    const pending_value = try std.mem.concat(scratch, u8, &.{ "\x00", &prepared.receipt.encode() });
    try store.values.put(pending_key, pending_value);
    try std.testing.expect((try lookup(&store, 7, command)) == null);
    try std.testing.expect((try lookupStatus(&store, 7, command)) == .prepared);
    command.payload_digest = @splat(4);
    try std.testing.expectError(error.SourcePayloadConflict, lookup(&store, 7, command));
    command = first;
    _ = store.values.remove(pending_key);
    try store.apply(prepared);
    try std.testing.expect((try lookup(&store, 7, first)) != null);
    command.payload_digest = @splat(4);
    try std.testing.expectError(error.SourcePayloadConflict, lookup(&store, 7, command));
    command = first;
    command.batch_id = "two";
    command.source_sequence = 20;
    command.expected_checkpoint = "cycle";
    command.checkpoint = "next";
    const next = try prepare(scratch, &store, 7, command.transactionId(), command);
    var rival = command;
    rival.batch_id = "rival";
    rival.source_sequence = 21;
    const conflicting = try prepare(scratch, &store, 7, rival.transactionId(), rival);
    try store.apply(next);
    try std.testing.expectError(error.VersionConflict, store.apply(conflicting));
    command.batch_id = "three";
    command.source_sequence = 30;
    command.expected_checkpoint = "next";
    command.checkpoint = "cycle";
    try store.apply(try prepare(scratch, &store, 7, command.transactionId(), command));
    try std.testing.expectEqual(@as(usize, 5), store.values.count());
    try std.testing.expectError(error.SourceReceiptExpired, lookup(&store, 7, first));
    rival.source_sequence = 15;
    try std.testing.expectError(error.SourceReceiptExpired, lookup(&store, 7, rival));
    try std.testing.expectError(error.SourceTableMismatch, lookup(&store, 8, command));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, prepare(scratch, &store, 7, @splat(0), command));
    var second_source = first;
    second_source.source = "independent-source";
    try store.apply(try prepare(scratch, &store, 7, second_source.transactionId(), second_source));
    try std.testing.expectEqual(@as(usize, 8), store.values.count());
    try std.testing.expect((try lookup(&store, 7, command)) != null);
    try std.testing.expect((try lookup(&store, 7, second_source)) != null);
    second_source.epoch = "replaced-slot";
    try std.testing.expectError(error.SourceBindingConflict, lookup(&store, 7, second_source));
    var corrupt = next.receipt.encode();
    corrupt[76] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, Receipt.decode(&corrupt));
}
