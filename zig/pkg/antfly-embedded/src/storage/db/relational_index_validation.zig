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

//! Read-only verification of an unpublished, fenced index generation. This is
//! physical progress, not metadata authentication or permission to publish.
//! Primary coverage and both generation-local passes share the repair path's
//! typed projection, but never repair a mismatch while claiming validation.
const std = @import("std");
const jobs = @import("relational_index_jobs.zig");
const owner = @import("relational_index_owner_job.zig");
const claims = @import("relational_index_private_claims.zig");
const catalog = @import("relational_index_catalog.zig");
const records = @import("relational_index_records.zig");
const internal = @import("../internal_keys.zig");
const range_state = @import("range_state.zig");
const authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
const Position = @import("receipt_position.zig").Position;
const Context = @import("../../api/operation.zig").RequestContext;
const Allocator = std.mem.Allocator;
const Digest = [32]u8;
const Expected = claims.TargetExpectation;
const prefix = owner.key ++ ":generation-validation:";
const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;
const max_summary = 8192;
const max_record = max_cursor + max_summary + 48;
pub const max_wire_bytes = 2 * max_cursor + 64 * 1024;
pub const Phase = enum { primary, forward, ownership, ready, failed };
pub const Failure = enum { none, malformed_key, missing_primary, invalid_row, tuple_mismatch, missing_forward, invalid_payload, missing_reverse, invalid_ownership, out_of_range, count_mismatch };

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nonzero(value: Digest) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn cutAfter(value: Position, previous: Position) bool {
    return value.strictlyAfter(previous);
}
fn optional(txn: anytype, key_: []const u8) !?[]const u8 {
    return txn.get(key_) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn key(binding: owner.Binding) [prefix.len + 16]u8 {
    var result: [prefix.len + 16]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &binding.id);
    return result;
}
pub const Fields = struct {
    phase: Phase = .primary,
    failure: Failure = .none,
    counts: [3]u64 = @splat(0),
    chain: Digest,
    cursor_digest: Digest,
    pub fn validate(self: Fields, source_members: u64) !void {
        if (!nonzero(self.chain) or !nonzero(self.cursor_digest) or (self.phase == .failed) != (self.failure != .none)) return error.InvalidRelationalIndexValidation;
        if (self.phase != .failed) {
            for (self.counts) |count| if (count > source_members) return error.InvalidRelationalIndexValidation;
            if (self.phase != .primary and self.counts[0] != source_members) return error.InvalidRelationalIndexValidation;
            if ((self.phase == .ownership or self.phase == .ready) and self.counts[1] != source_members) return error.InvalidRelationalIndexValidation;
            if (self.phase == .ready and (self.counts[2] != source_members or !equal(self.cursor_digest, hash("")))) return error.InvalidRelationalIndexValidation;
        }
    }
};
/// Fixed response/recovery identity. Large legal forward keys stay in a
/// separately framed cursor, never in the public/private progress response.
pub const Summary = struct {
    expected: Expected,
    inventory_digest: Digest,
    source_members: u64,
    fields: Fields,
    position: Position,
    page_digest: Digest,
    pub fn validate(self: Summary) !void {
        try self.expected.validate();
        try self.fields.validate(self.source_members);
        try self.position.validate();
        if (!nonzero(self.inventory_digest) or !nonzero(self.page_digest)) return error.InvalidRelationalIndexValidation;
    }
};
pub const Loaded = struct {
    alloc: Allocator,
    summary: Summary,
    cursor: []u8,
    digest: Digest,
    pub fn encodeAlloc(self: @This(), alloc: Allocator) ![]u8 {
        const bytes = try encodeRecord(alloc, self.summary, self.cursor);
        errdefer alloc.free(bytes);
        if (!equal(hash(bytes), self.digest)) return error.InvalidRelationalIndexValidation;
        return bytes;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !@This() {
        return decodeRecord(alloc, bytes);
    }
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.cursor);
        self.* = undefined;
    }
};
fn encodeRecord(alloc: Allocator, summary: Summary, cursor: []const u8) ![]u8 {
    try summary.validate();
    if (cursor.len > max_cursor or !equal(hash(cursor), summary.fields.cursor_digest)) return error.InvalidRelationalIndexValidation;
    const json = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(json);
    if (json.len > max_summary) return error.InvalidRelationalIndexValidation;
    const bytes = try alloc.alloc(u8, 48 + json.len + cursor.len);
    @memcpy(bytes[0..8], "AIGV\x02\x00\x00\x00");
    std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(cursor.len), .little);
    @memcpy(bytes[16..][0..json.len], json);
    @memcpy(bytes[16 + json.len ..][0..cursor.len], cursor);
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    return bytes;
}
fn decodeRecord(alloc: Allocator, bytes: []const u8) !Loaded {
    if (bytes.len < 48 or bytes.len > max_record or !std.mem.eql(u8, bytes[0..8], "AIGV\x02\x00\x00\x00") or !equal(bytes[bytes.len - 32 ..][0..32].*, hash(bytes[0 .. bytes.len - 32]))) return error.InvalidRelationalIndexValidation;
    const json_len = std.mem.readInt(u32, bytes[8..12], .little);
    const cursor_len = std.mem.readInt(u32, bytes[12..16], .little);
    if (json_len > max_summary or cursor_len > max_cursor or @as(u64, json_len) + cursor_len + 48 != bytes.len) return error.InvalidRelationalIndexValidation;
    var parsed = try std.json.parseFromSlice(Summary, alloc, bytes[16..][0..json_len], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
    defer parsed.deinit();
    const summary = parsed.value;
    try summary.validate();
    const canonical = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(canonical);
    const raw_cursor = bytes[16 + json_len ..][0..cursor_len];
    if (!std.mem.eql(u8, canonical, bytes[16..][0..json_len]) or !equal(hash(raw_cursor), summary.fields.cursor_digest)) return error.InvalidRelationalIndexValidation;
    const digest = hash(bytes);
    const copied = try alloc.dupe(u8, raw_cursor);
    errdefer alloc.free(copied);
    return .{ .alloc = alloc, .summary = summary, .cursor = copied, .digest = digest };
}
pub fn load(alloc: Allocator, txn: anytype, binding: owner.Binding) !?Loaded {
    const bytes = (try optional(txn, &key(binding))) orelse return null;
    var loaded = try decodeRecord(alloc, bytes);
    errdefer loaded.deinit();
    try loaded.summary.expected.requireOwnerBinding(binding);
    const cut = loaded.summary.position;
    try cut.requireNamespace(binding.fence.namespace);
    if ((cut == .native) != (binding.authority == .native)) return error.InvalidRelationalIndexValidation;
    if (cut == .native) {
        const current = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        if (cut.native.sequence > current.sequence) return error.InvalidRelationalIndexValidation;
    } else {
        const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidRelationalIndexValidation;
        if (cut.raft.index > current.index or cut.raft.term > current.term or (cut.raft.index == current.index and cut.raft.term != current.term)) return error.InvalidRelationalIndexValidation;
    }
    return loaded;
}
pub const Command = struct {
    binding: owner.Binding,
    expected: Expected,
    inventory_digest: Digest,
    source_members: u64,
    previous: Digest,
    scanned: u16,
    finish: bool,
    next: Fields,
    cursor_b64: []const u8,
    pub fn validate(self: Command) !void {
        try self.expected.requireOwnerBinding(self.binding);
        try self.next.validate(self.source_members);
        if (!nonzero(self.inventory_digest) or self.scanned > claims.max_claims or self.cursor_b64.len > std.base64.standard.Encoder.calcSize(max_cursor)) return error.InvalidRelationalIndexValidation;
        if (try std.base64.standard.Decoder.calcSizeForSlice(self.cursor_b64) > max_cursor) return error.InvalidRelationalIndexValidation;
    }
    pub fn digest(self: Command) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-generation-page/v1");
        state.update(&try self.binding.encode());
        state.update(&try self.expected.digest());
        inline for (.{ "inventory_digest", "previous" }) |field| state.update(&@field(self, field));
        var numbers: [35]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.source_members, .little);
        std.mem.writeInt(u16, numbers[8..10], self.scanned, .little);
        numbers[10] = @intFromBool(self.finish);
        for (self.next.counts, 0..) |count, i| std.mem.writeInt(u64, numbers[11 + i * 8 ..][0..8], count, .little);
        state.update(&numbers);
        state.update(&.{ @backingInt(self.next.phase), @backingInt(self.next.failure) });
        state.update(&self.next.chain);
        state.update(&self.next.cursor_digest);
        state.update(self.cursor_b64);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};

/// Structural recovery only. Restore/adoption still requires a complete
/// physical rescan before this progress can become authenticated publication
/// evidence; a retained cursor is not an audit of its already-scanned prefix.
pub fn verifyRecovery(alloc: Allocator, txn: anytype, binding: owner.Binding) !void {
    var loaded = (try load(alloc, txn, binding)) orelse {
        if (try Seal.load(alloc, txn, binding) != null) return error.InvalidRelationalIndexValidation;
        return;
    };
    defer loaded.deinit();
    if (try @import("relational_index_cancellation_intent.zig").load(txn)) |closed| if (closed.intent.fence.admission_epoch == binding.fence.admission_epoch) return;
    const frozen = try claims.InventoryValidation.requireReadyAt(alloc, txn, binding, loaded.summary.expected);
    if (!equal(frozen.digest, loaded.summary.inventory_digest) or frozen.source_members != loaded.summary.source_members or !cutAfter(loaded.summary.position, frozen.position)) return error.InvalidRelationalIndexValidation;
    if (try Seal.load(alloc, txn, binding)) |sealed| {
        const command = try SealCommand.prepare(alloc, txn, binding, loaded.summary.expected);
        if (!std.meta.eql(command, sealed.command)) return error.InvalidRelationalIndexValidation;
    }
}

/// A logical validation commitment, independent of the signing key, local
/// checkpoint and physical DB incarnation. Restored roots must independently
/// re-audit the physical bytes before signing/accepting this durable receipt.
pub const SealCommand = struct {
    binding: owner.Binding,
    expected: Expected,
    generation_digest: Digest,
    inventory_digest: Digest,
    source_members: u64,
    generation_position: Position,

    pub fn validate(self: @This()) !void {
        try self.expected.requireOwnerBinding(self.binding);
        try self.generation_position.validate();
        if ((self.generation_position == .native) != (self.binding.authority == .native) or self.binding.phase != .reserved or
            (self.generation_position == .native and !self.generation_position.native.namespace.eql(self.binding.fence.namespace)) or
            !nonzero(self.generation_digest) or !nonzero(self.inventory_digest)) return error.InvalidRelationalIndexValidation;
    }

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.validate();
        var buffer: [16384]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(self, .{}, &writer);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-logical-validation/v1");
        state.update(writer.buffered());
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    /// O(1) in table size: all row/claim scans precede this atomic cut. Recheck
    /// the complete canonical records and frozen source/target scope in the
    /// applying transaction; a caller-supplied digest alone is not evidence.
    pub fn prepare(alloc: Allocator, txn: anytype, binding: owner.Binding, expected: Expected) !@This() {
        const frozen = try claims.InventoryValidation.requireReadyAt(alloc, txn, binding, expected);
        var generation = (try load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired;
        defer generation.deinit();
        if (generation.summary.fields.phase != .ready or
            !equal(try generation.summary.expected.digest(), try expected.digest()) or
            !equal(generation.summary.inventory_digest, frozen.digest) or
            generation.summary.source_members != frozen.source_members or
            !cutAfter(generation.summary.position, frozen.position)) return error.PrivateIndexTargetValidationRequired;
        const result: @This() = .{ .binding = binding, .expected = expected, .generation_digest = generation.digest, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .generation_position = generation.summary.position };
        try result.validate();
        return result;
    }

    pub fn receipt(self: @This(), position: Native) !@import("relational_index_publication_progress.zig").Receipt {
        if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return self.receiptAt(.{ .native = position });
    }
    pub fn receiptAt(self: @This(), position: Position) !@import("relational_index_publication_progress.zig").Receipt {
        try self.validate();
        try position.validate();
        if (position == .native and !position.native.namespace.eql(self.binding.fence.namespace)) return error.InvalidPrivateIndexNativeReceipt;
        if (!cutAfter(position, self.generation_position)) return if (position == .native and self.generation_position == .native) error.InvalidPrivateIndexNativeReceipt else error.InvalidControlReceiptPosition;
        const binding = self.binding;
        const result: @import("relational_index_publication_progress.zig").Receipt = .{
            .owner = .{ .plan_id = binding.id, .plan_digest = binding.plan_digest, .ordinal = binding.ordinal, .group_id = binding.fence.owner_group_id, .namespace = binding.fence.namespace, .authority = binding.authority, .fence_digest = binding.owner_digest },
            .action = .validated,
            .position = position,
            .candidate_digest = binding.integrity_candidate,
            .reservation_digest = binding.integrity_reserved,
            .evidence_digest = try self.evidenceDigest(),
        };
        _ = try result.digest();
        return result;
    }
};

/// One reusable namespace slot, committed with its authority and replay marker
/// (plus the standby outbox for native execution). No local
/// root identity is retained here. This seal is not publication permission.
pub const Seal = struct {
    pub const key = owner.key ++ ":validated";
    pub const max_wire_bytes = 32 * 1024;
    command: SealCommand,
    position: Position,

    pub fn load(alloc: Allocator, txn: anytype, binding: owner.Binding) !?@This() {
        const raw = (try optional(txn, Seal.key)) orelse return null;
        if (raw.len < 40 or raw.len > Seal.max_wire_bytes or !std.mem.eql(u8, raw[0..8], "AIVS\x02\x00\x00\x00") or
            !equal(raw[raw.len - 32 ..][0..32].*, hash(raw[0 .. raw.len - 32]))) return error.InvalidRelationalIndexValidation;
        var parsed = try std.json.parseFromSlice(@This(), alloc, raw[8 .. raw.len - 32], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
        defer parsed.deinit();
        const result = parsed.value;
        _ = try result.command.receiptAt(result.position);
        const canonical = try std.json.Stringify.valueAlloc(alloc, result, .{});
        defer alloc.free(canonical);
        if (!std.mem.eql(u8, canonical, raw[8 .. raw.len - 32])) return error.InvalidRelationalIndexValidation;
        if (!std.meta.eql(result.command.binding, binding)) return null;
        if (result.position == .native) {
            const current = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
            if (result.position.native.sequence > current.sequence) return error.InvalidRelationalIndexValidation;
        } else {
            _ = try authority.require(txn, .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
            const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidRelationalIndexValidation;
            if (result.position.raft.index > current.index or result.position.raft.term > current.term or (result.position.raft.index == current.index and result.position.raft.term != current.term)) return error.InvalidRelationalIndexValidation;
        }
        return result;
    }

    pub const Outcome = struct { seal: Seal, duplicate: bool };
    pub fn stage(alloc: Allocator, txn: anytype, command: SealCommand, replay: ?Native) !Outcome {
        if (command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return stageAt(alloc, txn, command, if (replay) |cut| .{ .native = cut } else null);
    }
    pub fn stageAt(alloc: Allocator, txn: anytype, command: SealCommand, replay: ?Position) !Outcome {
        try command.validate();
        if (command.binding.authority == .raft and replay == null) return error.InvalidControlReceiptPosition;
        if (replay) |position| _ = try command.receiptAt(position);
        const actual = try SealCommand.prepare(alloc, txn, command.binding, command.expected);
        if (!std.meta.eql(actual, command)) return error.InvalidRelationalIndexValidation;
        if (try Seal.load(alloc, txn, command.binding)) |existing| {
            if (!std.meta.eql(command, existing.command)) return error.InvalidRelationalIndexValidation;
            if (replay) |position| {
                if (position == .native and !std.meta.eql(position, existing.position)) return error.InvalidPrivateIndexNativeReceipt;
                if (!std.meta.eql(position, existing.position) and !cutAfter(position, existing.position)) return error.InvalidControlReceiptPosition;
            }
            return .{ .seal = existing, .duplicate = true };
        }
        const position: Position = if (command.binding.authority == .raft) replay.? else .{ .native = .{ .namespace = command.binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(command.binding.fence.namespace), if (replay) |value| value.native.sequence else null) } };
        if (position == .raft) {
            const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            if (position.raft.index <= current.index or position.raft.term < current.term) return error.InvalidControlReceiptPosition;
        }
        const result: @This() = .{ .command = command, .position = position };
        _ = try command.receiptAt(position);
        const json = try std.json.Stringify.valueAlloc(alloc, result, .{});
        defer alloc.free(json);
        if (json.len + 40 > Seal.max_wire_bytes) return error.InvalidRelationalIndexValidation;
        const bytes = try alloc.alloc(u8, json.len + 40);
        defer alloc.free(bytes);
        @memcpy(bytes[0..8], "AIVS\x02\x00\x00\x00");
        @memcpy(bytes[8..][0..json.len], json);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        try txn.put(Seal.key, bytes);
        return .{ .seal = result, .duplicate = false };
    }
};
/// Stable logical installation identity. The receiving owner must obtain the
/// metadata decision independently; a digest supplied to these storage
/// primitives is not permission to install or adopt a different physical root.
pub const InstallIntent = struct {
    validation: Seal,
    metadata_decision: Digest,

    pub fn validate(self: @This()) !void {
        _ = try self.validation.command.receiptAt(self.validation.position);
        if (!nonzero(self.metadata_decision)) return error.InvalidPrivateIndexNativeReceipt;
    }

    pub fn validateBefore(self: @This(), alloc: Allocator, txn: anytype) !void {
        try self.validate();
        const binding = self.validation.command.binding;
        try owner.requireReserved(txn, binding);
        const actual = try SealCommand.prepare(alloc, txn, binding, self.validation.command.expected);
        if (!std.meta.eql(actual, self.validation.command) or
            !std.meta.eql((try Seal.load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired, self.validation)) return error.InvalidRelationalIndexValidation;
    }
};

pub const InstallCommand = struct {
    validation: Seal,
    metadata_decision: Digest,
    schema_digest: Digest,
    activation_digest: Digest,

    pub fn intent(self: @This()) InstallIntent {
        return .{ .validation = self.validation, .metadata_decision = self.metadata_decision };
    }
    pub fn validate(self: @This()) !void {
        try self.intent().validate();
        if (!nonzero(self.schema_digest) or !nonzero(self.activation_digest)) return error.InvalidPrivateIndexNativeReceipt;
    }
    pub fn validateBefore(self: @This(), alloc: Allocator, txn: anytype) !void {
        try self.validate();
        try self.intent().validateBefore(alloc, txn);
    }

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.validate();
        var buffer: [32 * 1024]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(self, .{}, &writer);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-installed/v1");
        state.update(writer.buffered());
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    pub fn receipt(self: @This(), position: Native) !@import("relational_index_publication_progress.zig").Receipt {
        if (self.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return self.receiptAt(.{ .native = position });
    }
    pub fn receiptAt(self: @This(), position: Position) !@import("relational_index_publication_progress.zig").Receipt {
        try self.validate();
        try position.validate();
        if (!cutAfter(position, self.validation.position)) return if (position == .native and self.validation.position == .native) error.InvalidPrivateIndexNativeReceipt else error.InvalidControlReceiptPosition;
        var result = try self.validation.command.receiptAt(self.validation.position);
        result.action = .installed;
        result.position = position;
        result.evidence_digest = try self.evidenceDigest();
        _ = try result.digest();
        return result;
    }
};

/// One reusable installed cut, committed with schema, catalogs, activation,
/// replay marker and outbox. A staged record MUST NOT be signed or returned as
/// a publication receipt before that containing transaction commits.
pub const Installed = struct {
    pub const key = owner.key ++ ":installed";
    pub const max_wire_bytes = 32 * 1024;
    command: InstallCommand,
    position: Position,

    fn reserved(binding: owner.Binding) owner.Binding {
        var before = binding;
        before.phase = .reserved;
        return before;
    }

    pub fn load(alloc: Allocator, txn: anytype, binding: owner.Binding) !?@This() {
        const bytes = (try optional(txn, Installed.key)) orelse return null;
        if (bytes.len < 40 or bytes.len > Installed.max_wire_bytes or !std.mem.eql(u8, bytes[0..8], "AIIS\x02\x00\x00\x00") or
            !equal(bytes[bytes.len - 32 ..][0..32].*, hash(bytes[0 .. bytes.len - 32]))) return error.InvalidPrivateIndexNativeReceipt;
        var parsed = try std.json.parseFromSlice(@This(), alloc, bytes[8 .. bytes.len - 32], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
        defer parsed.deinit();
        const result = parsed.value;
        _ = try result.command.receiptAt(result.position);
        const canonical = try std.json.Stringify.valueAlloc(alloc, result, .{});
        defer alloc.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes[8 .. bytes.len - 32])) return error.InvalidPrivateIndexNativeReceipt;
        if (!std.meta.eql(result.command.validation.command.binding, reserved(binding))) return null;
        const source = try authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        if (result.position == .native) {
            if (result.position.native.sequence > source.sequence) return error.InvalidPrivateIndexNativeReceipt;
        } else {
            const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            if (result.position.raft.index > current.index or result.position.raft.term > current.term or (result.position.raft.index == current.index and result.position.raft.term != current.term)) return error.InvalidControlReceiptPosition;
        }
        return result;
    }

    pub fn requireCurrent(self: @This(), alloc: Allocator, txn: anytype) !void {
        const current = (try owner.load(txn)) orelse return error.PrivateIndexJobRequired;
        if (current.phase != .installed or !std.meta.eql(reserved(current), self.command.validation.command.binding)) return error.PrivateIndexOwnerJobChanged;
        try self.requireSuccessor(alloc, txn);
    }

    fn requireSuccessor(self: @This(), alloc: Allocator, txn: anytype) !void {
        const binding = self.command.validation.command.binding;
        try @import("relational_index_cancellation_intent.zig").requireOpen(txn, binding);
        const fence = (try @import("relational_integrity_topology.zig").current(txn)) orelse return error.IntegrityTopologyFenceMissing;
        if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
        const identity = try @import("relational_integrity_catalog.zig").boundSchemaIdentity(try txn.get(@import("relational_integrity_catalog.zig").key), binding.integrity_candidate);
        if (identity.version != binding.physical_candidate.schema_version or !equal(identity.digest, self.command.schema_digest)) return error.RelationalIndexCatalogSchemaMismatch;
        if (!equal(hash(try txn.get(@import("../schema.zig").schema_key)), self.command.schema_digest)) return error.RelationalIndexCatalogSchemaMismatch;
        const physical = try catalog.Head.decode(try txn.get(catalog.head_key));
        if (!physical.eql(binding.physical_candidate)) return error.PreparedGenerationChanged;
        const sealed = (try Seal.load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (!std.meta.eql(sealed, self.command.validation)) return error.InvalidRelationalIndexValidation;
        const activation = @import("relational_integrity_activation.zig");
        const raw = try txn.get(activation.key);
        if (!equal(hash(raw), self.command.activation_digest)) return error.ConstraintActivationChanged;
        const progress = try activation.Progress.decode(raw);
        if (!equal(progress.owner, try activation.ownership(txn)) or progress.schema_version != identity.version) return error.ConstraintActivationOwnerChanged;
    }

    /// Called AFTER the exact successor schema/catalog/activation are staged.
    /// The caller also stages its replay marker and durable outbox, then aborts
    /// on every error. This neither commits nor publishes runtime objects.
    pub const Outcome = struct { record: Installed, duplicate: bool };
    pub fn stage(alloc: Allocator, txn: anytype, command: InstallCommand, candidate_catalog: @import("relational_integrity_catalog.zig").Catalog, replay: ?Native) !Outcome {
        if (command.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return stageAt(alloc, txn, command, candidate_catalog, if (replay) |cut| .{ .native = cut } else null);
    }
    pub fn stageAt(alloc: Allocator, txn: anytype, command: InstallCommand, candidate_catalog: @import("relational_integrity_catalog.zig").Catalog, replay: ?Position) !Outcome {
        try command.validate();
        const binding = command.validation.command.binding;
        if (binding.authority == .raft and replay == null) return error.InvalidControlReceiptPosition;
        if (replay) |position| _ = try command.receiptAt(position);
        const current = (try owner.load(txn)) orelse return error.PrivateIndexJobRequired;
        if (!std.meta.eql(reserved(current), binding)) return error.PrivateIndexOwnerJobChanged;
        if (try Installed.load(alloc, txn, binding)) |previous| {
            if (current.phase != .installed or !std.meta.eql(previous.command, command)) return error.InvalidPrivateIndexNativeReceipt;
            if (replay) |position| {
                if (position == .native and !std.meta.eql(position, previous.position)) return error.InvalidPrivateIndexNativeReceipt;
                if (!std.meta.eql(position, previous.position) and !cutAfter(position, previous.position)) return error.InvalidControlReceiptPosition;
            }
            try previous.requireSuccessor(alloc, txn);
            return .{ .record = previous, .duplicate = true };
        }
        if (current.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
        const source = try authority.require(txn, if (binding.authority == .native) .native else .raft, namespace);
        const position: Position = if (binding.authority == .raft) replay.? else .{ .native = .{ .namespace = binding.fence.namespace, .sequence = std.math.add(u64, source.sequence, 1) catch return error.InvalidControlReceiptPosition } };
        if (position == .raft) {
            const current_marker = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            if (position.raft.index <= current_marker.index or position.raft.term < current_marker.term) return error.InvalidControlReceiptPosition;
        }
        const record: @This() = .{ .command = command, .position = position };
        _ = try command.receiptAt(position);
        try record.requireSuccessor(alloc, txn);
        const activation = @import("relational_integrity_activation.zig");
        const progress = try activation.Progress.decode(try txn.get(activation.key));
        if (!equal(candidate_catalog.schema_digest, command.schema_digest) or
            candidate_catalog.schema_version != binding.physical_candidate.schema_version or
            !progress.matches(candidate_catalog, try activation.ownership(txn))) return error.ConstraintActivationChanged;
        const json = try std.json.Stringify.valueAlloc(alloc, record, .{});
        defer alloc.free(json);
        if (json.len + 40 > Installed.max_wire_bytes) return error.InvalidPrivateIndexNativeReceipt;
        const bytes = try alloc.alloc(u8, json.len + 40);
        defer alloc.free(bytes);
        @memcpy(bytes[0..8], "AIIS\x02\x00\x00\x00");
        @memcpy(bytes[8..][0..json.len], json);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        var installed = binding;
        installed.phase = .installed;
        if (position == .native) {
            const sequence = try authority.advance(txn, namespace, if (replay) |value| value.native.sequence else null);
            std.debug.assert(sequence == position.native.sequence);
        }
        try txn.put(owner.key, &try installed.encode());
        try txn.put(Installed.key, bytes);
        return .{ .record = record, .duplicate = false };
    }

    /// Owned immutable predecessor metadata for read-only proof verification
    /// and physical auditing. Verify the REAL successor first. Never mask data,
    /// range, namespace or clocks; expose no writes or commit operation.
    pub const History = struct {
        alloc: Allocator,
        record: Installed,
        binding: owner.Binding,
        candidate: @import("relational_index_candidate.zig").Candidate,
        before_schema: []u8,
        before_catalog: catalog.Loaded,
        job: [owner.Binding.encoded_len]u8,
        head: @TypeOf(catalog.Head.encode(undefined)),

        pub fn init(alloc: Allocator, txn: anytype, binding: owner.Binding) !History {
            if (binding.phase != .installed) return error.InvalidPrivateIndexOwnerJob;
            const record = (try Installed.load(alloc, txn, binding)) orelse return error.InvalidPrivateIndexNativeReceipt;
            try record.requireCurrent(alloc, txn);
            var candidate = try owner.reopenCandidate(alloc, txn, binding);
            errdefer candidate.deinit();
            const schema = @import("../schema.zig");
            const before_key = try schema.schemaVersionKeyAlloc(alloc, binding.physical_reserved.schema_version);
            defer alloc.free(before_key);
            const before_schema = try alloc.dupe(u8, try txn.get(before_key));
            errdefer alloc.free(before_schema);
            if (!equal(hash(before_schema), binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
            var before_catalog = try catalog.decode(alloc, binding.physical_reserved, try txn.get(&catalog.blobKey(binding.physical_reserved.blob_digest)));
            errdefer before_catalog.deinit();
            if (!equal(before_catalog.schema_digest, binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
            return .{ .alloc = alloc, .record = record, .binding = Installed.reserved(binding), .candidate = candidate, .before_schema = before_schema, .before_catalog = before_catalog, .job = try Installed.reserved(binding).encode(), .head = binding.physical_reserved.encode() };
        }
        pub fn deinit(self: *History) void {
            self.before_catalog.deinit();
            self.alloc.free(self.before_schema);
            self.candidate.deinit();
            self.* = undefined;
        }
        pub fn Reader(comptime Source: type) type {
            return struct {
                history: *const History,
                source: Source,
                pub fn get(self: *@This(), key_: []const u8) ![]const u8 {
                    if (std.mem.eql(u8, key_, owner.key)) return &self.history.job;
                    if (std.mem.eql(u8, key_, catalog.head_key)) return &self.history.head;
                    if (std.mem.eql(u8, key_, @import("../schema.zig").schema_key)) return self.history.before_schema;
                    if (std.mem.eql(u8, key_, @import("relational_integrity_catalog.zig").key)) return self.history.candidate.reservation.value;
                    return self.source.get(key_);
                }
                pub fn openCursor(self: *@This()) @TypeOf(self.source.openCursor()) {
                    return self.source.openCursor();
                }
            };
        }
        pub fn reader(self: *const History, txn: anytype) Reader(@TypeOf(txn)) {
            return .{ .history = self, .source = txn };
        }
    };

    /// Structural recovery only, never physical-root adoption or execution.
    pub fn verifyRecovery(alloc: Allocator, txn: anytype, binding: owner.Binding, active: ?*const catalog.Loaded) !void {
        var history = try History.init(alloc, txn, binding);
        defer history.deinit();
        const current = active orelse return error.MissingRelationalIndexDefinitions;
        if (!current.head.eql(binding.physical_candidate) or !equal(current.schema_digest, history.record.command.schema_digest)) return error.RelationalIndexCatalogSchemaMismatch;
        var before = history.reader(txn);
        try owner.verifyUninstalledRecovery(alloc, &before, history.binding, &history.before_catalog);
        const activation = @import("relational_integrity_activation.zig");
        const progress = try activation.Progress.decode(try txn.get(activation.key));
        if (!progress.matches(history.candidate.next.catalog, try activation.ownership(txn))) return error.ConstraintActivationChanged;
    }
};

/// Owned, root-local progress only. Never import this from a replicated
/// snapshot or submit it as an ordered native effect. Durable checkpoint
/// ownership and adoption/signing authority belong to the physical-root owner.
pub const Audit = struct {
    root_incarnation: u128,
    canonical_digest: Digest,
    inventory_audit_digest: Digest,
    progress: Loaded,
    pub fn deinit(self: *@This()) void {
        self.progress.deinit();
        self.* = undefined;
    }
};
/// Recompute every physical pass without disturbing the canonical generation
/// cursor. Requires this root's completed inventory audit; neither that audit
/// nor this result grants permission to publish or attest the generation.
pub fn auditPage(alloc: Allocator, core: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, root_incarnation: u128, inventory_audit: claims.InventoryValidation.Audit, previous: ?*const Audit, budget: jobs.Budget, context: Context) !Audit {
    var read = try core.store.beginReadTxn();
    defer read.abort();
    return auditReadPage(alloc, core, &read, binding, expected, addition, root_incarnation, inventory_audit, previous, budget, context);
}

/// Read-only physical audit at a caller-pinned snapshot. Installed owners use
/// a verified historical metadata view; data/cursors remain real store reads.
pub fn auditReadPage(alloc: Allocator, core: anytype, read: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, root_incarnation: u128, inventory_audit: claims.InventoryValidation.Audit, previous: ?*const Audit, budget: jobs.Budget, context: Context) !Audit {
    try context.ensureActive();
    try budget.validate();
    try expected.requireOwnerBinding(binding);
    if (root_incarnation == 0 or inventory_audit.root_incarnation != root_incarnation or
        !equal(inventory_audit.expectation_digest, try expected.digest())) return error.PrivateIndexAuditScopeChanged;
    try inventory_audit.fields.validate(expected);
    if (inventory_audit.fields.phase != .ready or !nonzero(inventory_audit.page_digest)) return error.PrivateIndexTargetValidationRequired;
    if (!addition.candidate.head.eql(binding.physical_candidate) or !addition.reservation.head.eql(binding.physical_reserved) or addition.index.mapKey() != binding.index.mapKey()) return error.PrivateIndexOwnerJobChanged;
    const pinned: catalog.WriteSnapshot = .{ .head = addition.candidate.head, .plan = addition.plan.clone() };
    defer {
        var copy = pinned;
        copy.deinit();
    }
    const index = for (pinned.plan.boundIndexes()) |value| {
        if (value.id().mapKey() == binding.index.mapKey()) break value;
    } else return error.IndexNotFound;
    const frozen = try claims.InventoryValidation.requireReadyAt(alloc, read, binding, expected);
    if (!equal(inventory_audit.canonical_digest, frozen.digest)) return error.PrivateIndexAuditScopeChanged;
    var canonical = (try load(alloc, read, binding)) orelse return error.PrivateIndexTargetValidationRequired;
    defer canonical.deinit();
    if (!equal(try canonical.summary.expected.digest(), try expected.digest()) or
        !equal(canonical.summary.inventory_digest, frozen.digest) or canonical.summary.source_members != frozen.source_members or
        !canonical.summary.position.strictlyAfter(frozen.position)) return error.PrivateIndexAuditScopeChanged;
    if (canonical.summary.fields.phase != .ready) return error.PrivateIndexTargetValidationRequired;
    var local: ?Loaded = null;
    if (previous) |state| {
        if (state.root_incarnation != root_incarnation or !equal(state.canonical_digest, canonical.digest) or
            !equal(state.inventory_audit_digest, inventory_audit.page_digest) or
            !equal(try state.progress.summary.expected.digest(), try expected.digest()) or
            !equal(state.progress.summary.inventory_digest, frozen.digest) or state.progress.summary.source_members != frozen.source_members or
            !std.meta.eql(state.progress.summary.position, canonical.summary.position)) return error.PrivateIndexAuditScopeChanged;
        const encoded = try encodeRecord(alloc, state.progress.summary, state.progress.cursor);
        defer alloc.free(encoded);
        if (!equal(hash(encoded), state.progress.digest)) return error.InvalidRelationalIndexValidation;
        if (state.progress.summary.fields.phase == .ready or state.progress.summary.fields.phase == .failed) {
            const cursor = try alloc.dupe(u8, state.progress.cursor);
            return .{ .root_incarnation = root_incarnation, .canonical_digest = canonical.digest, .inventory_audit_digest = inventory_audit.page_digest, .progress = .{ .alloc = alloc, .summary = state.progress.summary, .cursor = cursor, .digest = state.progress.digest } };
        }
        local = state.progress;
    }
    var page = (try Prepared.preparePage(alloc, core, read, binding, expected, pinned, index, core.schemaNamespaceGeneration(), frozen, local, budget, context, null)) orelse return error.InvalidRelationalIndexValidation;
    defer page.deinit();
    var digest = std.crypto.hash.Blake3.init(.{});
    digest.update("antfly/private-index-generation-root-local-audit/v1");
    var root_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &root_bytes, root_incarnation, .little);
    digest.update(&root_bytes);
    digest.update(&canonical.digest);
    digest.update(&inventory_audit.page_digest);
    digest.update(&try page.command.digest());
    var page_digest: Digest = undefined;
    digest.final(&page_digest);
    const cursor = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(page.command.cursor_b64));
    errdefer alloc.free(cursor);
    try std.base64.standard.Decoder.decode(cursor, page.command.cursor_b64);
    const summary: Summary = .{ .expected = expected, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .fields = page.command.next, .position = canonical.summary.position, .page_digest = page_digest };
    const encoded = try encodeRecord(alloc, summary, cursor);
    defer alloc.free(encoded);
    return .{ .root_incarnation = root_incarnation, .canonical_digest = canonical.digest, .inventory_audit_digest = inventory_audit.page_digest, .progress = .{ .alloc = alloc, .summary = summary, .cursor = cursor, .digest = hash(encoded) } };
}
fn cursorEnd(alloc: Allocator, value: []const u8) ![]u8 {
    var end = value.len;
    while (end != 0) {
        end -= 1;
        if (value[end] != 255) {
            const result = try alloc.dupe(u8, value[0 .. end + 1]);
            result[end] += 1;
            return result;
        }
    }
    return error.InvalidRelationalIndexValidation;
}
fn observe(chain: *Digest, key_: []const u8, value: ?[]const u8) void {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/private-index-generation-record/v1");
    state.update(chain);
    state.update(&hash(key_));
    state.update(&.{@intFromBool(value != null)});
    if (value) |bytes| state.update(&hash(bytes));
    state.final(chain);
}
fn Probe(comptime Txn: type) type {
    return struct {
        txn: Txn,
        chain: *Digest,
        bytes: usize = 0,
        observed_key: []const u8 = "",
        observed_value: []const u8 = "",
        fn get(self: *@This(), key_: []const u8) !?[]const u8 {
            if (self.observed_key.len != 0 and std.mem.eql(u8, key_, self.observed_key)) return self.observed_value;
            const value = try optional(self.txn, key_);
            self.bytes +|= key_.len +| (if (value) |raw| raw.len else @as(usize, 0));
            observe(self.chain, key_, value);
            return value;
        }
    };
}
fn verifyDocument(alloc: Allocator, core: anytype, pinned: catalog.WriteSnapshot, index: @import("relational_index_plan.zig").BoundIndex, phase: Phase, document: []const u8, projection: *jobs.RowProjection, probe: anytype, next: *Fields) !Failure {
    const primary = try internal.relationalRowKeyAlloc(alloc, document);
    const row = try probe.get(primary);
    var reverse: std.ArrayList(u8) = .empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, index.id(), document);
    var ownership_key: std.ArrayList(u8) = .empty;
    defer ownership_key.deinit(alloc);
    try records.appendOwnershipFromReverse(alloc, &ownership_key, reverse.items);
    const projected = if (row) |raw| projection.prepare(alloc, core, pinned, index, raw, document) catch |err| {
        if (jobs.classifyRowFailure(err) != null) return .invalid_row;
        return err;
    } else null;
    if (projected == null) {
        if (phase != .primary) return if (row == null) .missing_primary else .tuple_mismatch;
        if (try probe.get(reverse.items) != null) return if (row == null) .missing_primary else .tuple_mismatch;
        if (try probe.get(ownership_key.items) != null) return .invalid_ownership;
        return .none;
    }
    const value = projected.?;
    if (phase == .primary) next.counts[0] = std.math.add(u64, next.counts[0], 1) catch return error.InvalidRelationalIndexValidation;
    var reverse_value: std.ArrayList(u8) = .empty;
    defer reverse_value.deinit(alloc);
    try records.appendReverseValue(alloc, &reverse_value, reverse.items, value.tuple);
    var forward: std.ArrayList(u8) = .empty;
    defer forward.deinit(alloc);
    try records.appendForwardFromReverse(alloc, &forward, reverse.items, reverse_value.items);
    if (phase == .forward and !std.mem.eql(u8, probe.observed_key, forward.items)) return .tuple_mismatch;
    if (phase == .ownership and !std.mem.eql(u8, probe.observed_key, ownership_key.items)) return .invalid_ownership;
    const actual_reverse = (try probe.get(reverse.items)) orelse return .missing_reverse;
    if (!std.mem.eql(u8, actual_reverse, reverse_value.items)) return .tuple_mismatch;
    const actual_forward = (try probe.get(forward.items)) orelse return .missing_forward;
    if ((value.payload.len == 0 and actual_forward.len != 0) or (value.payload.len != 0 and actual_forward.len != value.payload.len + 4)) return .invalid_payload;
    const payload = records.forwardPayload(forward.items, actual_forward) catch return .invalid_payload;
    if (!std.mem.eql(u8, payload, value.payload)) return .invalid_payload;
    const actual_ownership = (try probe.get(ownership_key.items)) orelse return .invalid_ownership;
    if (actual_ownership.len != 0) return .invalid_ownership;
    return .none;
}
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    command: Command,
    context: Context,
    namespace_generation: u64,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn prepare(alloc: Allocator, core: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, budget: jobs.Budget, context: Context) !?Prepared {
        return prepareImpl(alloc, core, binding, expected, addition, budget, context, null);
    }
    pub fn replay(alloc: Allocator, core: anytype, addition: *const catalog.PrivateAddition, command: Command) !Prepared {
        try command.validate();
        var result = (try prepareImpl(alloc, core, command.binding, command.expected, addition, .{ .records = @max(1, command.scanned), .bytes = 16 * 1024 * 1024, .time_ns = std.time.ns_per_s }, .{}, command)) orelse return error.InvalidRelationalIndexValidation;
        errdefer result.deinit();
        if (!equal(try result.command.digest(), try command.digest())) return error.InvalidRelationalIndexValidation;
        return result;
    }
    fn prepareImpl(alloc: Allocator, core: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, budget: jobs.Budget, context: Context, replay_command: ?Command) !?Prepared {
        try context.ensureActive();
        try budget.validate();
        try expected.requireOwnerBinding(binding);
        if (!addition.candidate.head.eql(binding.physical_candidate) or !addition.reservation.head.eql(binding.physical_reserved) or addition.index.mapKey() != binding.index.mapKey()) return error.PrivateIndexOwnerJobChanged;
        const namespace_generation = core.schemaNamespaceGeneration();
        const pinned: catalog.WriteSnapshot = .{ .head = addition.candidate.head, .plan = addition.plan.clone() };
        defer {
            var copy = pinned;
            copy.deinit();
        }
        const index = for (pinned.plan.boundIndexes()) |value| {
            if (value.id().mapKey() == binding.index.mapKey()) break value;
        } else return error.IndexNotFound;
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const frozen = try claims.InventoryValidation.requireReadyAt(alloc, &read, binding, expected);
        var loaded = try load(alloc, &read, binding);
        defer if (loaded) |*value| value.deinit();
        if (loaded) |value| {
            if (!equal(try value.summary.expected.digest(), try expected.digest()) or !equal(value.summary.inventory_digest, frozen.digest) or value.summary.source_members != frozen.source_members or !cutAfter(value.summary.position, frozen.position)) return error.PrivateIndexOwnerJobChanged;
        }
        return preparePage(alloc, core, &read, binding, expected, pinned, index, namespace_generation, frozen, loaded, budget, context, replay_command);
    }
    fn preparePage(alloc: Allocator, core: anytype, read: anytype, binding: owner.Binding, expected: Expected, pinned: catalog.WriteSnapshot, index: @import("relational_index_plan.zig").BoundIndex, namespace_generation: u64, frozen: anytype, loaded: ?Loaded, budget: jobs.Budget, context: Context, replay_command: ?Command) !?Prepared {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        if (replay_command) |command| if (loaded) |value| if (equal(value.summary.page_digest, try command.digest())) {
            var owned = command;
            owned.cursor_b64 = try a.dupe(u8, command.cursor_b64);
            return .{ .arena = arena, .command = owned, .context = context, .namespace_generation = namespace_generation };
        };
        if (loaded) |value| if (value.summary.fields.phase == .ready or value.summary.fields.phase == .failed) {
            arena.deinit();
            return null;
        };
        const previous = if (loaded) |value| value.digest else @as(Digest, @splat(0));
        var next: Fields = if (loaded) |value| value.summary.fields else initial: {
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/private-index-generation/v1");
            state.update(&try expected.digest());
            state.update(&frozen.digest);
            var chain: Digest = undefined;
            state.final(&chain);
            break :initial .{ .chain = chain, .cursor_digest = hash("") };
        };
        const phase = next.phase;
        const range = try range_state.decodeRangeAlloc(a, (try optional(read, range_state.range_key)) orelse &@as([8]u8, @splat(0)));
        const lower: []const u8 = switch (phase) {
            .primary => try internal.documentExactPrefixAlloc(a, range.start),
            .forward => try a.dupe(u8, &try records.forwardPrefix(binding.index)),
            .ownership => try a.dupe(u8, &try records.ownershipPrefix(binding.index)),
            else => unreachable,
        };
        const upper: []const u8 = if (phase == .primary) (if (range.end.len != 0) try internal.documentExactPrefixAlloc(a, range.end) else &.{internal.user_namespace + 1}) else try cursorEnd(a, lower);
        var after: std.ArrayList(u8) = .empty;
        defer after.deinit(alloc);
        if (loaded) |value| try after.appendSlice(alloc, value.cursor);
        if (after.items.len != 0 and (std.mem.order(u8, after.items, lower) == .lt or std.mem.order(u8, after.items, upper) == .gt)) return error.InvalidRelationalIndexValidation;
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(upper);
        var row_arena = std.heap.ArenaAllocator.init(alloc);
        defer row_arena.deinit();
        var projection: jobs.RowProjection = .{ .alloc = alloc };
        defer projection.deinit();
        var scanned: u16 = 0;
        var bytes: usize = 0;
        const start = @import("antfly_platform").time.monotonicNs();
        var entry = try cursor.seekAtOrAfter(if (after.items.len == 0) lower else after.items);
        if (phase != .primary and after.items.len != 0) if (entry) |kv| if (std.mem.eql(u8, kv.key, after.items)) {
            entry = try cursor.next();
        };
        while (entry) |kv| {
            if (std.mem.order(u8, kv.key, upper) != .lt) break;
            if (replay_command) |command| if (scanned >= command.scanned) break;
            try context.ensureActive();
            _ = row_arena.reset(.retain_capacity);
            const row_alloc = row_arena.allocator();
            if (kv.key.len > max_cursor) return error.InvalidRelationalIndexValidation;
            const observed = try row_alloc.dupe(u8, kv.key);
            var probe: Probe(@TypeOf(read)) = .{ .txn = read, .chain = &next.chain, .bytes = observed.len };
            var document: ?[]const u8 = null;
            var continuation: []const u8 = observed;
            var failure: Failure = .none;
            if (phase == .primary) {
                if (internal.findComponentTerminator(observed, 1)) |term| {
                    continuation = try cursorEnd(row_alloc, observed[0 .. term + 2]);
                    document = try internal.decodeDocumentComponentAlloc(row_alloc, observed);
                    observe(&next.chain, observed[0 .. term + 2], null);
                } else failure = .malformed_key;
            } else {
                probe.observed_key = observed;
                probe.observed_value = try row_alloc.dupe(u8, kv.value);
                probe.bytes +|= kv.value.len;
                observe(&next.chain, observed, probe.observed_value);
                const component = if (phase == .forward) component: {
                    const parsed = records.forwardOwnership(observed) catch {
                        failure = .malformed_key;
                        break :component null;
                    };
                    break :component parsed.document_component;
                } else records.ownershipDocument(observed) catch component: {
                    failure = .malformed_key;
                    break :component null;
                };
                if (component) |value| {
                    const raw_key = try std.mem.concat(row_alloc, u8, &.{ &.{internal.user_namespace}, value });
                    document = try internal.decodeDocumentComponentAlloc(row_alloc, raw_key);
                }
                const slot: usize = if (phase == .forward) 1 else 2;
                next.counts[slot] = std.math.add(u64, next.counts[slot], 1) catch return error.InvalidRelationalIndexValidation;
            }
            if (failure == .none) {
                if (document) |value| {
                    failure = if (!range.contains(value)) .out_of_range else try verifyDocument(row_alloc, core, pinned, index, phase, value, &projection, &probe, &next);
                } else failure = .malformed_key;
            }
            after.clearRetainingCapacity();
            try after.appendSlice(alloc, continuation);
            scanned += 1;
            bytes +|= probe.bytes;
            if (failure != .none) {
                next.phase = .failed;
                next.failure = failure;
                break;
            }
            if (next.counts[@as(usize, if (phase == .primary) 0 else if (phase == .forward) 1 else 2)] > frozen.source_members) {
                next.phase = .failed;
                next.failure = .count_mismatch;
                break;
            }
            entry = if (phase == .primary) try cursor.seekAtOrAfter(after.items) else try cursor.next();
            // A byte/time budget is soft for one indivisible row, like build
            // pages. It must not make a previously admitted large row unusable.
            if (scanned >= @min(budget.records, claims.max_claims) or bytes >= budget.bytes or (replay_command == null and @import("antfly_platform").time.monotonicNs() -| start >= budget.time_ns)) break;
        }
        if (replay_command) |command| if (scanned != command.scanned) return error.InvalidRelationalIndexValidation;
        const exhausted = next.phase != .failed and (entry == null or std.mem.order(u8, entry.?.key, upper) != .lt);
        if (next.phase != .failed and exhausted and (replay_command == null or replay_command.?.finish)) {
            const slot: usize = if (phase == .primary) 0 else if (phase == .forward) 1 else 2;
            if (next.counts[slot] != frozen.source_members) {
                next.phase = .failed;
                next.failure = .count_mismatch;
            } else {
                next.phase = switch (phase) {
                    .primary => .forward,
                    .forward => .ownership,
                    .ownership => .ready,
                    else => unreachable,
                };
                after.clearRetainingCapacity();
            }
        }
        const finish = phase != next.phase;
        next.cursor_digest = hash(after.items);
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(after.items.len));
        _ = std.base64.standard.Encoder.encode(encoded, after.items);
        const command: Command = .{ .binding = binding, .expected = expected, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .previous = previous, .scanned = scanned, .finish = finish, .next = next, .cursor_b64 = encoded };
        try command.validate();
        return .{ .arena = arena, .command = command, .context = context, .namespace_generation = namespace_generation };
    }
    /// Caller commits progress with its ordered receipt/outbox and MUST abort
    /// the whole transaction on every error, including encode/allocation failure.
    pub const Outcome = struct { summary: Summary, duplicate: bool };
    pub fn stage(self: *@This(), core: anytype, txn: anytype, replay_position: ?Native) !Outcome {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return self.stageAt(core, txn, if (replay_position) |cut| .{ .native = cut } else null);
    }
    pub fn stageAt(self: *@This(), core: anytype, txn: anytype, replay_position: ?Position) !Outcome {
        try self.context.ensureActive();
        try self.command.validate();
        if (self.command.binding.authority == .raft and replay_position == null) return error.InvalidControlReceiptPosition;
        if (replay_position) |cut| {
            try cut.requireNamespace(self.command.binding.fence.namespace);
            if ((cut == .native) != (self.command.binding.authority == .native)) return error.InvalidControlReceiptPosition;
        }
        if (core.schemaNamespaceGeneration() != self.namespace_generation) return error.PreparedGenerationChanged;
        const frozen = try claims.InventoryValidation.requireReadyAt(self.arena.allocator(), txn, self.command.binding, self.command.expected);
        if (!equal(frozen.digest, self.command.inventory_digest) or frozen.source_members != self.command.source_members) return error.PrivateIndexOwnerJobChanged;
        var loaded = try load(self.arena.allocator(), txn, self.command.binding);
        defer if (loaded) |*value| value.deinit();
        const digest = try self.command.digest();
        if (loaded) |value| if (equal(value.summary.page_digest, digest)) {
            if (replay_position) |cut| {
                if (cut == .native and !std.meta.eql(cut, value.summary.position)) return error.InvalidPrivateIndexNativeReceipt;
                if (!std.meta.eql(cut, value.summary.position) and !cutAfter(cut, value.summary.position)) return error.InvalidControlReceiptPosition;
            }
            return .{ .summary = value.summary, .duplicate = true };
        };
        const previous = if (loaded) |value| value.digest else @as(Digest, @splat(0));
        if (!equal(previous, self.command.previous)) return error.VersionConflict;
        const a = self.arena.allocator();
        const raw_cursor = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(self.command.cursor_b64));
        try std.base64.standard.Decoder.decode(raw_cursor, self.command.cursor_b64);
        if (!equal(hash(raw_cursor), self.command.next.cursor_digest)) return error.InvalidRelationalIndexValidation;
        try self.context.ensureActive();
        const position: Position = if (self.command.binding.authority == .raft) replay_position.? else .{ .native = .{ .namespace = self.command.binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.command.binding.fence.namespace), if (replay_position) |value| value.native.sequence else null) } };
        if (!cutAfter(position, frozen.position)) return error.InvalidControlReceiptPosition;
        if (position == .raft) {
            const current = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            if (position.raft.index <= current.index or position.raft.term < current.term) return error.InvalidControlReceiptPosition;
        }
        const summary: Summary = .{ .expected = self.command.expected, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .fields = self.command.next, .position = position, .page_digest = digest };
        const bytes = try encodeRecord(a, summary, raw_cursor);
        try txn.put(&key(self.command.binding), bytes);
        return .{ .summary = summary, .duplicate = false };
    }
};

test "relational index system generation checkpoint keeps maximum cursors out of fixed summaries" {
    const alloc = std.testing.allocator;
    const cursor = try alloc.alloc(u8, max_cursor);
    defer alloc.free(cursor);
    @memset(cursor, 255);
    const summary: Summary = .{
        .expected = .{ .metadata_incarnation = @splat('1'), .binding_digest = @splat(1), .generation = @splat(2), .completion_revision = 1, .source_consumption_digest = @splat(3), .source_progress_digest = @splat(4), .table_name_digest = @splat(5), .rows = 0, .inventory_xor = @splat(0), .provenance = @splat(0) },
        .inventory_digest = @splat(6),
        .source_members = 0,
        .fields = .{ .chain = @splat(7), .cursor_digest = hash(cursor) },
        .position = .{ .native = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .sequence = 1 } },
        .page_digest = @splat(8),
    };
    const bytes = try encodeRecord(alloc, summary, cursor);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len <= max_record);
    const response = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(response);
    try std.testing.expect(response.len < max_summary);
    var loaded = try decodeRecord(alloc, bytes);
    defer loaded.deinit();
    try std.testing.expectEqualDeep(summary, loaded.summary);
    try std.testing.expectEqualSlices(u8, cursor, loaded.cursor);
    const DecodeFaults = struct {
        fn run(a: Allocator, encoded: []const u8) !void {
            var decoded = try decodeRecord(a, encoded);
            defer decoded.deinit();
        }
    };
    for ([_]Position{
        .{ .native = .{ .namespace = .{ .table_id = std.math.maxInt(u64), .shard_id = 2, .range_id = 3 }, .sequence = std.math.maxInt(u64) - 1 } },
        .{ .raft = .{ .term = 19, .index = std.math.maxInt(u64) - 1 } },
    }) |cut| {
        var exact = summary;
        exact.position = cut;
        const encoded = try encodeRecord(alloc, exact, cursor);
        defer alloc.free(encoded);
        var decoded = try decodeRecord(alloc, encoded);
        defer decoded.deinit();
        try std.testing.expectEqualDeep(exact, decoded.summary);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, DecodeFaults.run, .{encoded});
        encoded[4] = 1;
        @memcpy(encoded[encoded.len - 32 ..], &hash(encoded[0 .. encoded.len - 32]));
        try std.testing.expectError(error.InvalidRelationalIndexValidation, decodeRecord(alloc, encoded));
    }
    // A source may produce 32 local index rows while all UNIQUE claims route
    // to other owners. Physical index counts bind the consumed source rows,
    // not the independently routed target-inventory cardinality.
    var different_cardinality = summary;
    different_cardinality.source_members = 32;
    different_cardinality.fields.phase = .ready;
    different_cardinality.fields.counts = @splat(32);
    different_cardinality.fields.cursor_digest = hash("");
    try different_cardinality.validate();
    different_cardinality.fields.counts[2] -= 1;
    try std.testing.expectError(error.InvalidRelationalIndexValidation, different_cardinality.validate());
    bytes[bytes.len - 33] ^= 1;
    try std.testing.expectError(error.InvalidRelationalIndexValidation, decodeRecord(alloc, bytes));
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    try std.testing.expectError(error.InvalidRelationalIndexValidation, decodeRecord(alloc, bytes));
    bytes[bytes.len - 33] ^= 1;
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    for (0..8) |failure| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = failure });
        if (decodeRecord(failing.allocator(), bytes)) |value| {
            var owned = value;
            owned.deinit();
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
    }
}
