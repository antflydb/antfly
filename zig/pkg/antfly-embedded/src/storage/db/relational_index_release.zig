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

//! Terminal release identity. Checksums/receipts do not authorize execution:
//! receivers must obtain metadata permission and stage the record, exact fence
//! release, authority cut and transport marker/outbox in one transaction.
const std = @import("std");
const validation = @import("relational_index_validation.zig");
const owner = @import("relational_index_owner_job.zig");
const positions = @import("receipt_position.zig");
const publication = @import("relational_index_publication_progress.zig");
const authority = @import("../source_authority.zig");
const Digest = [32]u8;
const magic = "AIIR\x02\x00\x00\x00";

fn requireCommitted(txn: anytype, binding: owner.Binding, cut: positions.Position) !void {
    try cut.requireNamespace(binding.fence.namespace);
    if ((cut == .native) != (binding.authority == .native)) return error.InvalidControlReceiptPosition;
    const source = try authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
    if (cut == .native) {
        if (cut.native.sequence > source.sequence) return error.InvalidPrivateIndexNativeReceipt;
    } else {
        const applied = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
        if (cut.raft.index > applied.index or cut.raft.term > applied.term or (cut.raft.index == applied.index and cut.raft.term != applied.term)) return error.InvalidControlReceiptPosition;
    }
}

fn preparePosition(txn: anytype, binding: owner.Binding, supplied: ?positions.Position) !positions.Position {
    const source = try authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
    if (binding.authority == .native) {
        const cut: positions.Position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = std.math.add(u64, source.sequence, 1) catch return error.InvalidControlReceiptPosition } };
        if (supplied) |value| if (!std.meta.eql(cut, value)) return error.InvalidPrivateIndexNativeReceipt;
        return cut;
    }
    const cut = supplied orelse return error.InvalidControlReceiptPosition;
    try cut.validate();
    if (cut != .raft) return error.InvalidControlReceiptPosition;
    const applied = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
    if (cut.raft.index <= applied.index or cut.raft.term < applied.term) return error.InvalidControlReceiptPosition;
    return cut;
}

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

/// Root-independent metadata intent. The installed command/position must come
/// from the retained accepted installation proof, not from an RPC caller.
pub const Intent = struct {
    installed: validation.Installed,
    metadata_decision: Digest,

    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.metadata_decision, 0)) return error.InvalidPrivateIndexNativeReceipt;
        _ = try self.installed.command.receiptAt(self.installed.position);
    }

    pub fn requireInstalled(self: @This(), alloc: std.mem.Allocator, txn: anytype) !void {
        try self.validate();
        const actual = (try validation.Installed.load(alloc, txn, self.installed.command.validation.command.binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (!std.meta.eql(actual, self.installed)) return error.InvalidPrivateIndexNativeReceipt;
        try actual.requireCurrent(alloc, txn);
    }

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-release-intent/v1");
        state.update(&try self.installed.command.evidenceDigest());
        state.update(&try self.installed.position.encode());
        state.update(&self.metadata_decision);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    pub fn receipt(self: @This(), position: positions.Native) !publication.Receipt {
        if (self.installed.position != .native) return error.InvalidControlReceiptPosition;
        return self.receiptAt(.{ .native = position });
    }
    pub fn receiptAt(self: @This(), position: positions.Position) !publication.Receipt {
        try self.validate();
        try position.validate();
        if (!position.strictlyAfter(self.installed.position)) return error.InvalidPrivateIndexNativeReceipt;
        var result = try self.installed.command.receiptAt(self.installed.position);
        result.action = .released;
        result.position = position;
        result.evidence_digest = try self.evidenceDigest();
        _ = try result.digest();
        return result;
    }
};

/// Owned, bounded evidence survives retirement of the large build inputs.
/// It records a historical committed cut; it is not a claim that mutable index
/// data still matches an old build audit, nor a replacement-root capability.
pub const Record = struct {
    pub const key = owner.key ++ ":released";
    /// A bounded cleanup objective, not permission to delete live claims.
    /// A later full companion pass may clear it only at this exact release cut.
    pub const retirement_pending_key = owner.key ++ ":released-companions-dirty";
    pub const max_wire_bytes = 32 * 1024;
    intent: Intent,
    position: positions.Position,

    pub const Encoded = struct {
        bytes: [max_wire_bytes]u8,
        len: usize,
        pub fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
    };

    pub fn validate(self: @This()) !void {
        _ = try self.intent.receiptAt(self.position);
    }

    pub fn encode(self: @This()) !Encoded {
        try self.validate();
        var result: Encoded = .{ .bytes = undefined, .len = 0 };
        @memcpy(result.bytes[0..magic.len], magic);
        var writer = std.Io.Writer.fixed(result.bytes[magic.len .. max_wire_bytes - 32]);
        std.json.Stringify.value(self, .{}, &writer) catch return error.InvalidPrivateIndexNativeReceipt;
        result.len = magic.len + writer.end;
        @memcpy(result.bytes[result.len..][0..32], &hash(result.bytes[0..result.len]));
        result.len += 32;
        return result;
    }

    pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !@This() {
        if (bytes.len <= magic.len + 32 or bytes.len > max_wire_bytes or !std.mem.eql(u8, bytes[0..magic.len], magic) or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]))) return error.InvalidPrivateIndexNativeReceipt;
        var parsed = std.json.parseFromSlice(@This(), alloc, bytes[magic.len .. bytes.len - 32], .{ .ignore_unknown_fields = false, .max_value_len = 128 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidPrivateIndexNativeReceipt,
        };
        defer parsed.deinit();
        const result = parsed.value;
        try result.validate();
        const canonical = try result.encode();
        if (!std.mem.eql(u8, bytes, canonical.slice())) return error.InvalidPrivateIndexNativeReceipt;
        return result;
    }

    /// Decode an actual durable slot and its authority cut. A different job's
    /// retained terminal receipt cannot stand in for this requested intent.
    pub fn load(alloc: std.mem.Allocator, txn: anytype, intent: Intent) !?@This() {
        try intent.validate();
        const record = (try loadRaw(alloc, txn)) orelse return null;
        if (!std.meta.eql(record.intent, intent)) return null;
        return record;
    }

    fn loadRaw(alloc: std.mem.Allocator, txn: anytype) !?@This() {
        const bytes = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const record = try decode(alloc, bytes);
        try requireCommitted(txn, record.intent.installed.command.validation.command.binding, record.position);
        return record;
    }

    /// The single terminal slot owns unfinished companion retirement. Do not
    /// admit a later build that could replace it before bounded GC completes.
    pub fn requireRetirementClear(txn: anytype) !void {
        const bytes = txn.get(retirement_pending_key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const position = try positions.Position.decode(bytes);
        const source = (try authority.load(txn)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if ((position == .native) != (source.kind == .native)) return error.InvalidPrivateIndexNativeReceipt;
        return error.GenerationRetirementPending;
    }

    pub fn pending(alloc: std.mem.Allocator, txn: anytype) !?@This() {
        const bytes = txn.get(retirement_pending_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const position = try positions.Position.decode(bytes);
        const record = (try loadRaw(alloc, txn)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if (!std.meta.eql(position, record.position)) return error.InvalidPrivateIndexNativeReceipt;
        try verifyRecovery(alloc, txn);
        return record;
    }

    pub fn verifyRecovery(alloc: std.mem.Allocator, txn: anytype) !void {
        const maybe_record = try loadRaw(alloc, txn);
        const marker = txn.get(retirement_pending_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (marker) |bytes| {
            const position = try positions.Position.decode(bytes);
            const retained = maybe_record orelse return error.InvalidPrivateIndexNativeReceipt;
            if (!std.meta.eql(position, retained.position)) return error.InvalidPrivateIndexNativeReceipt;
        }
        const topology = @import("relational_integrity_topology.zig");
        const bytes = txn.get(topology.receipt_key) catch |err| switch (err) {
            error.NotFound => {
                if (maybe_record != null) return error.IntegrityTopologyFenceMissing;
                return;
            },
            else => return err,
        };
        const receipt = try topology.Fence.decode(bytes);
        const record = maybe_record orelse {
            if (receipt.role == .index_build and (try owner.load(txn)) == null and (try CancellationRecord.loadRaw(txn)) == null) return error.InvalidPrivateIndexNativeReceipt;
            return;
        };
        const binding = record.intent.installed.command.validation.command.binding;
        if (receipt.admission_epoch < binding.fence.admission_epoch or
            ((receipt.role == .index_build or receipt.admission_epoch == binding.fence.admission_epoch) and !receipt.eql(binding.fence))) return error.IntegrityTopologyChanged;
        if (try owner.load(txn)) |current| {
            var original = current;
            original.phase = .reserved;
            if (std.meta.eql(original, binding)) return error.InvalidPrivateIndexOwnerJob;
        }
    }

    /// Caller owns commit/abort and stages transport markers/outboxes in THIS
    /// transaction. No row scan, artifact GC or metadata ACK occurs here.
    pub const Outcome = struct { record: Record, duplicate: bool };
    pub fn stage(alloc: std.mem.Allocator, txn: anytype, intent: Intent, replay: ?positions.Native) !Outcome {
        if (intent.installed.position != .native) return error.InvalidControlReceiptPosition;
        return stageAt(alloc, txn, intent, if (replay) |cut| .{ .native = cut } else null);
    }
    pub fn stageAt(alloc: std.mem.Allocator, txn: anytype, intent: Intent, replay: ?positions.Position) !Outcome {
        try intent.validate();
        if (intent.installed.position == .raft and replay == null) return error.InvalidControlReceiptPosition;
        if (replay) |position| _ = try intent.receiptAt(position);
        if (try load(alloc, txn, intent)) |previous| {
            if (replay) |position| {
                if (position == .native and !std.meta.eql(position, previous.position)) return error.InvalidPrivateIndexNativeReceipt;
                if (!std.meta.eql(position, previous.position) and !position.strictlyAfter(previous.position)) return error.InvalidControlReceiptPosition;
            }
            try verifyRecovery(alloc, txn);
            return .{ .record = previous, .duplicate = true };
        }
        try intent.requireInstalled(alloc, txn);
        const binding = intent.installed.command.validation.command.binding;
        const expected = intent.installed.command.validation.command.expected;
        const claims = @import("relational_index_private_claims.zig");
        const stream = claims.SourceStream;
        const head = (try stream.load(txn, binding)) orelse return error.PrivateIndexRootRecoveryRequired;
        const consumed = (try stream.loadConsumption(txn, binding, head)) orelse return error.PrivateIndexRootRecoveryRequired;
        if (head.state != .ready or head.pending() != 0 or
            !std.mem.eql(u8, &head.progress_digest, &expected.source_progress_digest) or
            !std.mem.eql(u8, &try consumed.prefix().digest(), &expected.source_consumption_digest)) return error.PrivateIndexTargetValidationRequired;
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
        const position = try preparePosition(txn, binding, replay);
        const record: Record = .{ .intent = intent, .position = position };
        const encoded = try record.encode();
        try @import("relational_integrity_topology.zig").stageInstalledIndexRelease(alloc, txn, intent.installed);
        if (position == .native) {
            const sequence = try authority.advance(txn, namespace, if (replay) |supplied| supplied.native.sequence else null);
            std.debug.assert(sequence == position.native.sequence);
        }
        try txn.put(key, encoded.slice());
        try txn.delete(CancellationRecord.key);
        try txn.put(retirement_pending_key, &try position.encode());
        // These are private retry/proof slots, never primary/index rows or live
        // UNIQUE claims. Preserve schema-version history and active progress.
        for ([_][]const u8{ owner.key, @import("relational_index_inputs.zig").key, validation.Installed.key, validation.Seal.key, stream.head_key, stream.consumption_key, @import("relational_index_owner_control.zig").PreparedBuild.checkpoint_key }) |retired| try txn.delete(retired);
        if (binding.authority == .native) {
            for ([_]owner.NativeEffect{ .admitted, .fenced, .reserved }) |effect| try txn.delete(&owner.nativeEffectKey(effect));
        } else try @import("relational_index_raft_control.zig").stageRetireCuts(txn, binding);
        try txn.delete(&claims.InventoryValidation.key(binding));
        try txn.delete(&validation.key(binding));
        try txn.delete(&claims.registrationKey(expected.generation));
        const physical = @import("relational_index_catalog.zig");
        if (!std.mem.eql(u8, &binding.physical_reserved.blob_digest, &binding.physical_candidate.blob_digest)) try txn.delete(&physical.blobKey(binding.physical_reserved.blob_digest));
        return .{ .record = record, .duplicate = false };
    }
};

/// Metadata supplies the accepted cleanup checkpoint and immutable cohort
/// decision. Neither a completed local GC pass nor a caller flag grants release.
pub const CancellationIntent = struct {
    canceled: owner.Canceled,
    metadata_decision: Digest,

    pub fn validate(self: @This()) !void {
        try self.canceled.validate();
        if (std.mem.allEqual(u8, &self.metadata_decision, 0)) return error.InvalidPrivateIndexNativeReceipt;
    }

    pub fn receipt(self: @This(), position: positions.Native) !publication.Receipt {
        try self.validate();
        try position.validate();
        if (!position.namespace.eql(self.canceled.position.namespace) or position.sequence <= self.canceled.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-cancellation-release-intent/v1");
        state.update(&try self.canceled.encode());
        state.update(&self.metadata_decision);
        var result = try self.canceled.receipt();
        result.action = .cancel_released;
        result.position = .{ .native = position };
        state.final(&result.evidence_digest);
        _ = try result.digest();
        return result;
    }

    pub fn requireCurrent(self: @This(), txn: anytype) !void {
        try self.validate();
        const actual = (try owner.Canceled.load(txn)) orelse return error.PrivateIndexCancellationPending;
        if (!std.meta.eql(actual, self.canceled)) return error.PrivateIndexOwnerJobChanged;
        try actual.requireCurrent(txn);
    }
};

/// Fixed historical retry slot, separate from the still-fenced cleanup cut.
/// Replays and retries never re-audit mutable data after the fence is lifted.
pub const CancellationRecord = struct {
    pub const key = owner.key ++ ":cancel-released";
    pub const encoded_len = 1328;
    intent: CancellationIntent,
    position: positions.Native,

    pub fn encode(self: @This()) ![encoded_len]u8 {
        _ = try self.intent.receipt(self.position);
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AICR\x01\x00\x00\x00");
        @memcpy(bytes[8..1224], &try self.intent.canceled.encode());
        @memcpy(bytes[1224..1256], &self.intent.metadata_decision);
        @memcpy(bytes[1256..1289], &try (positions.Position{ .native = self.position }).encode());
        @memcpy(bytes[1296..1328], &hash(bytes[0..1296]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICR\x01\x00\x00\x00") or
            !std.mem.allEqual(u8, bytes[1289..1296], 0) or !std.mem.eql(u8, bytes[1296..1328], &hash(bytes[0..1296]))) return error.InvalidPrivateIndexNativeReceipt;
        const cut = try positions.Position.decode(bytes[1256..1289]);
        if (cut != .native) return error.InvalidPrivateIndexNativeReceipt;
        const record: @This() = .{ .intent = .{ .canceled = try owner.Canceled.decode(bytes[8..1224]), .metadata_decision = bytes[1224..1256].* }, .position = cut.native };
        _ = try record.intent.receipt(record.position);
        return record;
    }

    fn loadRaw(txn: anytype) !?@This() {
        const bytes = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const record = try decode(bytes);
        const source = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(record.position.namespace));
        if (source.sequence < record.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
        return record;
    }

    pub fn load(txn: anytype, intent: CancellationIntent) !?@This() {
        try intent.validate();
        const record = (try loadRaw(txn)) orelse return null;
        return if (std.meta.eql(record.intent, intent)) record else null;
    }

    pub fn verifyRecovery(txn: anytype) !void {
        const record = (try loadRaw(txn)) orelse return;
        const fence = record.intent.canceled.binding.fence;
        const topology = @import("relational_integrity_topology.zig");
        const completed = (try topology.completed(txn)) orelse return error.IntegrityTopologyFenceMissing;
        if (!completed.namespace.eql(fence.namespace) or completed.admission_epoch < fence.admission_epoch or
            (completed.admission_epoch == fence.admission_epoch and !completed.eql(fence))) return error.IntegrityTopologyChanged;
        if (try owner.load(txn)) |current| if (current.fence.admission_epoch <= fence.admission_epoch) return error.InvalidPrivateIndexOwnerJob;
        if (try topology.current(txn)) |current| if (current.admission_epoch <= fence.admission_epoch) return error.IntegrityTopologyChanged;
    }

    pub fn stage(txn: anytype, manager: *@import("../transactions.zig").TxnManager, intent: CancellationIntent, replay: ?positions.Native) !struct { record: CancellationRecord, duplicate: bool } {
        try intent.validate();
        if (replay) |cut| _ = try intent.receipt(cut);
        if (try load(txn, intent)) |previous| {
            if (replay) |cut| if (!std.meta.eql(cut, previous.position)) return error.InvalidPrivateIndexNativeReceipt;
            try verifyRecovery(txn);
            return .{ .record = previous, .duplicate = true };
        }
        try intent.requireCurrent(txn);
        const binding = intent.canceled.binding;
        const claims = @import("relational_index_private_claims.zig");
        const allocation = try claims.requireRegistered(txn, binding);
        const source = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        const position: positions.Native = .{ .namespace = binding.fence.namespace, .sequence = std.math.add(u64, source.sequence, 1) catch return error.InvalidControlReceiptPosition };
        if (replay) |cut| if (!std.meta.eql(cut, position)) return error.InvalidPrivateIndexNativeReceipt;
        const record: @This() = .{ .intent = intent, .position = position };
        const encoded = try record.encode();
        try @import("relational_integrity_topology.zig").stageCanceledIndexRelease(txn, manager, intent.canceled);
        _ = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), if (replay) |cut| cut.sequence else null);
        try txn.put(key, &encoded);
        // Both terminal slots are bounded. The predecessor's companion GC was
        // an admission barrier, so it cannot be forgotten while still dirty.
        try Record.requireRetirementClear(txn);
        try txn.delete(Record.key);
        try txn.delete(owner.key);
        try txn.delete(@import("relational_index_inputs.zig").key);
        for ([_]owner.NativeEffect{ .admitted, .fenced, .reserved }) |effect| try txn.delete(&owner.nativeEffectKey(effect));
        try txn.delete(&claims.registrationKey(allocation.generation));
        return .{ .record = record, .duplicate = false };
    }
};

/// Canonical bounded retirement commands commit with their authority and
/// transport marker. Replicas replay the same deletes before a later build is admitted.
/// Never use cancellation GC: installed UNIQUE claims remain live user data.
pub const Gc = struct {
    const claims = @import("relational_index_private_claims.zig");
    pub const max_records = 128;
    pub const max_bytes = 2 * 1024 * 1024;
    pub const max_wire_bytes = 256 * 1024;
    const max_key_bytes = 256;
    const Key = struct {
        bytes: [max_key_bytes]u8 = undefined,
        len: usize,
        fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
    };
    pub const Entry = struct {
        key_b64: []const u8,
        value_digest: Digest,
        value_bytes: u32,

        fn decodedKey(self: @This()) !Key {
            const len = std.base64.standard.Decoder.calcSizeForSlice(self.key_b64) catch return error.InvalidPrivateIndexNativeReceipt;
            if (len == 0 or len > max_key_bytes) return error.InvalidPrivateIndexNativeReceipt;
            var result: Key = .{ .len = len };
            std.base64.standard.Decoder.decode(result.bytes[0..len], self.key_b64) catch return error.InvalidPrivateIndexNativeReceipt;
            var canonical: [std.base64.standard.Encoder.calcSize(max_key_bytes)]u8 = undefined;
            const encoded = std.base64.standard.Encoder.encode(&canonical, result.slice());
            if (!std.mem.eql(u8, encoded, self.key_b64)) return error.InvalidPrivateIndexNativeReceipt;
            return result;
        }
    };
    const Prefixes = struct {
        inventory: [claims.prefix.len + 16]u8,
        source: [claims.SourceStream.batch_prefix.len + 16]u8,
        fn init(record: Record) @This() {
            var result: @This() = undefined;
            @memcpy(result.inventory[0..claims.prefix.len], claims.prefix);
            @memcpy(result.inventory[claims.prefix.len..], &record.intent.installed.command.validation.command.expected.generation);
            @memcpy(result.source[0..claims.SourceStream.batch_prefix.len], claims.SourceStream.batch_prefix);
            @memcpy(result.source[claims.SourceStream.batch_prefix.len..], &record.intent.installed.command.validation.command.binding.id);
            return result;
        }
        fn contains(self: *const @This(), key_: []const u8) bool {
            return (key_.len == self.inventory.len + 64 and std.mem.startsWith(u8, key_, &self.inventory)) or
                (key_.len == self.source.len + 9 and std.mem.startsWith(u8, key_, &self.source) and key_[key_.len - 1] <= 1);
        }
        fn empty(self: *const @This(), txn: anytype) !bool {
            var cursor = try txn.openCursor();
            defer cursor.close();
            for ([_][]const u8{ &self.inventory, &self.source }) |prefix| {
                if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return false;
            }
            return true;
        }
    };

    pub const Outcome = struct {
        released: positions.Native,
        position: positions.Native,
        records: usize = 0,
        bytes: usize = 0,
        retired: bool = false,
        duplicate: bool = false,
        replication_pending: bool = false,
        error_name: ?[]const u8 = null,
    };
    pub const OrderedOutcome = struct {
        released: positions.Position,
        position: positions.Position,
        records: usize,
        bytes: usize,
        retired: bool,
    };

    pub const Command = struct {
        record: Record,
        entries: []const Entry,

        pub fn validate(self: @This()) !void {
            _ = try self.record.intent.receiptAt(self.record.position);
            if (self.entries.len > max_records) return error.InvalidPrivateIndexNativeReceipt;
            var previous: ?Key = null;
            var bytes: usize = 0;
            for (self.entries) |entry| {
                const decoded = try entry.decodedKey();
                if (previous) |prior| if (std.mem.order(u8, prior.slice(), decoded.slice()) != .lt) return error.InvalidPrivateIndexNativeReceipt;
                bytes = std.math.add(usize, bytes, decoded.len + @as(usize, entry.value_bytes)) catch return error.InvalidPrivateIndexNativeReceipt;
                if (bytes > max_bytes) return error.InvalidPrivateIndexNativeReceipt;
                previous = decoded;
            }
        }

        pub fn stage(self: @This(), alloc: std.mem.Allocator, txn: anytype, replay: ?positions.Native) !Outcome {
            if (self.record.position != .native) return error.InvalidControlReceiptPosition;
            if (replay) |cut| if (!cut.namespace.eql(self.record.position.native.namespace)) return error.IdentityNamespaceMismatch;
            const outcome = try self.stageAt(alloc, txn, if (replay) |cut| .{ .native = cut } else null);
            return .{ .released = outcome.released.native, .position = outcome.position.native, .records = outcome.records, .bytes = outcome.bytes, .retired = outcome.retired };
        }
        pub fn stageAt(self: @This(), _: std.mem.Allocator, txn: anytype, replay: ?positions.Position) !OrderedOutcome {
            try self.validate();
            const marker = txn.get(Record.retirement_pending_key) catch |err| switch (err) {
                error.NotFound => return error.VersionConflict,
                else => return err,
            };
            const pending_position = try positions.Position.decode(marker);
            if (!std.meta.eql(pending_position, self.record.position)) return error.InvalidPrivateIndexNativeReceipt;
            const encoded = try self.record.encode();
            if (!std.mem.eql(u8, encoded.slice(), try txn.get(Record.key))) return error.VersionConflict;
            if ((try owner.load(txn)) != null) return error.PrivateIndexOwnerJobChanged;
            const binding = self.record.intent.installed.command.validation.command.binding;
            try requireCommitted(txn, binding, self.record.position);
            const position = try preparePosition(txn, binding, replay);
            const prefixes = Prefixes.init(self.record);
            var bytes: usize = 0;
            for (self.entries) |entry| {
                const decoded = try entry.decodedKey();
                if (!prefixes.contains(decoded.slice())) return error.InvalidPrivateIndexNativeReceipt;
                const current = txn.get(decoded.slice()) catch |err| switch (err) {
                    error.NotFound => return error.VersionConflict,
                    else => return err,
                };
                if (current.len != entry.value_bytes or !std.mem.eql(u8, &hash(current), &entry.value_digest)) return error.VersionConflict;
                bytes += decoded.len + current.len;
            }
            for (self.entries) |entry| {
                const decoded = try entry.decodedKey();
                try txn.delete(decoded.slice());
            }
            const retired = try prefixes.empty(txn);
            if (!retired and self.entries.len == 0) return error.InvalidPrivateIndexNativeReceipt;
            if (retired) try txn.delete(Record.retirement_pending_key);
            if (position == .native) {
                const sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), if (replay) |cut| cut.native.sequence else null);
                std.debug.assert(sequence == position.native.sequence);
            }
            return .{ .released = self.record.position, .position = position, .records = self.entries.len, .bytes = bytes, .retired = retired };
        }
    };

    pub const Page = struct {
        arena: std.heap.ArenaAllocator,
        command: Command,

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
            self.* = undefined;
        }
        pub fn prepare(alloc: std.mem.Allocator, io: ?std.Io, txn: anytype) !?@This() {
            const record = (try Record.pending(alloc, txn)) orelse return null;
            const prefixes = Prefixes.init(record);
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const owned = arena.allocator();
            var entries: std.ArrayList(Entry) = .empty;
            var cursor = try txn.openCursor();
            defer cursor.close();
            const time = @import("antfly_platform").time;
            const started = time.monotonicNs();
            var bytes: usize = 0;
            scan: for ([_][]const u8{ &prefixes.inventory, &prefixes.source }) |prefix| {
                var next = try cursor.seekAtOrAfter(prefix);
                while (next) |entry| : (next = try cursor.next()) {
                    if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                    if (io) |runtime| try runtime.checkCancel();
                    if (!prefixes.contains(entry.key)) return error.InvalidPrivateIndexNativeReceipt;
                    const size = std.math.add(usize, entry.key.len, entry.value.len) catch return error.InvalidPrivateIndexNativeReceipt;
                    if (size > max_bytes) return error.InvalidPrivateIndexNativeReceipt;
                    if (entries.items.len == max_records or size > max_bytes - bytes) break :scan;
                    const encoded = try owned.alloc(u8, std.base64.standard.Encoder.calcSize(entry.key.len));
                    _ = std.base64.standard.Encoder.encode(encoded, entry.key);
                    try entries.append(owned, .{ .key_b64 = encoded, .value_digest = hash(entry.value), .value_bytes = @intCast(entry.value.len) });
                    bytes += size;
                    if (time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) break :scan;
                }
            }
            const command: Command = .{ .record = record, .entries = entries.items };
            try command.validate();
            return .{ .arena = arena, .command = command };
        }
    };
};

fn testIntent() !Intent {
    const namespace: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 };
    const binding: owner.Binding = .{ .id = @splat(1), .plan_digest = @splat(2), .owner_digest = @splat(3), .ordinal = 0, .authority = .native, .phase = .reserved, .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .admission_epoch = 7, .owner_group_id = 11, .peer_group_id = 11, .namespace = namespace, .catalog_digest = @splat(4) }, .integrity_before = @splat(4), .integrity_reserved = @splat(5), .integrity_candidate = @splat(6), .schema_before = @splat(7), .physical_before = null, .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(8) }, .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(9) }, .index = .{ .generation = 2, .slot = 0 }, .inputs_digest = @splat(10) };
    const expected: @import("relational_index_private_claims.zig").TargetExpectation = .{ .metadata_incarnation = @splat('1'), .binding_digest = hash(&try binding.encode()), .generation = @splat(2), .completion_revision = 1, .source_consumption_digest = @splat(3), .source_progress_digest = @splat(4), .table_name_digest = hash("items"), .rows = 0, .inventory_xor = @splat(0), .provenance = @splat(0) };
    const seal: validation.Seal = .{ .command = .{ .binding = binding, .expected = expected, .generation_digest = @splat(5), .inventory_digest = @splat(6), .source_members = 0, .generation_position = .{ .native = .{ .namespace = namespace, .sequence = 1 } } }, .position = .{ .native = .{ .namespace = namespace, .sequence = 2 } } };
    return .{ .installed = .{ .command = .{ .validation = seal, .metadata_decision = @splat(7), .schema_digest = @splat(8), .activation_digest = @splat(9) }, .position = .{ .native = .{ .namespace = namespace, .sequence = 3 } } }, .metadata_decision = @splat(10) };
}

fn testDecodeAllocations(alloc: std.mem.Allocator, bytes: []const u8) !void {
    _ = try Record.decode(alloc, bytes);
}

test "relational index release receipt binds installed cut decision and later native position" {
    const intent = try testIntent();
    var position = intent.installed.position.native;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, intent.receipt(position));
    position.sequence += 1;
    const receipt = try intent.receipt(position);
    try std.testing.expectEqual(publication.Action.released, receipt.action);
    try std.testing.expectEqual(position, receipt.position.native);
    var changed = intent;
    changed.metadata_decision[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try receipt.digest(), &try (try changed.receipt(position)).digest()));
    changed = intent;
    changed.metadata_decision = @splat(0);
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, changed.validate());
    position.namespace.table_id += 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, intent.receipt(position));
}

test "relational index release terminal codec is bounded canonical and allocation failure clean" {
    const intent = try testIntent();
    const seal = intent.installed.command.validation;
    const actual_json = try std.json.Stringify.valueAlloc(std.testing.allocator, seal, .{});
    defer std.testing.allocator.free(actual_json);
    var parsed_seal = try std.json.parseFromSlice(validation.Seal, std.testing.allocator, actual_json, .{});
    defer parsed_seal.deinit();
    try std.testing.expectEqualDeep(seal, parsed_seal.value);
    var actual_wire: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer actual_wire.deinit();
    var actual_stream: std.json.Stringify = .{ .writer = &actual_wire.writer };
    const wire = @import("relational_integrity_json.zig");
    try wire.write(seal, &actual_stream);
    var parsed_wire = try std.json.parseFromSlice(validation.Seal, std.testing.allocator, actual_wire.written(), .{});
    defer parsed_wire.deinit();
    try std.testing.expectEqualDeep(seal, parsed_wire.value);
    var raft_seal = seal;
    raft_seal.command.binding.authority = .raft;
    raft_seal.command.expected.binding_digest = hash(&try raft_seal.command.binding.encode());
    raft_seal.command.generation_position = .{ .raft = .{ .term = 17, .index = std.math.maxInt(u64) - 1 } };
    raft_seal.position = .{ .raft = .{ .term = 18, .index = std.math.maxInt(u64) } };
    _ = try raft_seal.command.receiptAt(raft_seal.position);
    const raft_json = try std.json.Stringify.valueAlloc(std.testing.allocator, raft_seal, .{});
    defer std.testing.allocator.free(raft_json);
    var parsed_raft = try std.json.parseFromSlice(validation.Seal, std.testing.allocator, raft_json, .{});
    defer parsed_raft.deinit();
    try std.testing.expectEqualDeep(raft_seal, parsed_raft.value);
    const record: Record = .{ .intent = intent, .position = .{ .native = .{ .namespace = intent.installed.position.native.namespace, .sequence = 4 } } };
    const encoded = try record.encode();
    try std.testing.expectEqualDeep(record, try Record.decode(std.testing.allocator, encoded.slice()));
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, testDecodeAllocations, .{encoded.slice()});
    var corrupt = encoded;
    corrupt.bytes[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, Record.decode(std.testing.allocator, corrupt.slice()));
    var spaced: Record.Encoded = .{ .bytes = undefined, .len = encoded.len + 1 };
    @memcpy(spaced.bytes[0..8], magic);
    spaced.bytes[8] = ' ';
    @memcpy(spaced.bytes[9 .. spaced.len - 32], encoded.bytes[8 .. encoded.len - 32]);
    @memcpy(spaced.bytes[spaced.len - 32 ..][0..32], &hash(spaced.bytes[0 .. spaced.len - 32]));
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, Record.decode(std.testing.allocator, spaced.slice()));
}
