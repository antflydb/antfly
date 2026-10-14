// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Ordered complete-delivery acceptance postimages. One source head and one
//! inventory per touched target, not a historical row/body log. This module
//! neither commits transactions nor enables source deletion. Production command
//! activation additionally needs snapshot/reader barriers and native journal
//! consumption; no public or Raft command currently stages these records.
const std = @import("std");
const local = @import("antfly_local_sources");
const jobs = local.storage_db_relational_index_owner_job;
const Source = jobs.private_claims.SourceStream;
const positions = local.storage_db_receipt_position;
const delivery = @import("relational_index_source_delivery.zig");
const publication = @import("relational_index_publication.zig");
const Digest = [32]u8;
const prefix = "\x00\x00__metadata__:relational_index_delivery:v1:";

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn nonzero(bytes: []const u8) bool {
    return !std.mem.allEqual(u8, bytes, 0);
}
fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
pub fn bindingDigest(binding: jobs.Binding) !Digest {
    return hash(&try binding.encode());
}
pub fn sourceKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}source:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}
pub fn targetKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}target:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

/// A native consumer must derive this statement from its retained consumption
/// journal, never from a request or a mirror ACK. Signature verification here
/// is not current ownership: metadata repeats enrollment/cut checks separately.
pub const Consumed = struct {
    metadata_incarnation: Digest,
    binding_digest: Digest,
    ordinal: u64,
    batch_digest: Digest,
    decision_digest: Digest,
    rows: u64,
    position: positions.Native,
    proof: publication.RootProof,

    pub fn digest(self: Consumed) !Digest {
        try self.position.validate();
        if (self.ordinal == 0 or self.proof.authority != .native or
            !nonzero(&self.metadata_incarnation) or !nonzero(&self.binding_digest) or
            !nonzero(&self.batch_digest) or !nonzero(&self.decision_digest) or
            !equal(self.metadata_incarnation, self.proof.identity.metadata_incarnation)) return error.InvalidPrivateIndexClaimReceipt;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-source-consumed/v1");
        state.update(&self.metadata_incarnation);
        state.update(&self.binding_digest);
        state.update(&self.batch_digest);
        state.update(&self.decision_digest);
        var numbers: [16]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.ordinal, .little);
        std.mem.writeInt(u64, numbers[8..16], self.rows, .little);
        state.update(&numbers);
        state.update(&try (positions.Position{ .native = self.position }).encode());
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
    pub fn verify(self: Consumed) !void {
        self.proof.verify(try self.digest()) catch return error.InvalidPrivateIndexClaimReceipt;
    }
};

pub const Head = struct {
    pub const encoded_len = 416;
    metadata_incarnation: Digest,
    binding_digest: Digest,
    scope_digest: Digest,
    ordinal: u64,
    rows: u64,
    batch_rows: u64,
    source_sequence: u64,
    batch_digest: Digest,
    logical_digest: Digest,
    previous_decision: Digest,
    decision_digest: Digest,
    accepted_root: Digest,
    provenance: Digest,
    previous_rows: u64,
    previous_provenance: Digest,
    state: Source.State,

    fn decision(self: Head) Digest {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-accepted-decision/v1");
        inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest", "batch_digest", "logical_digest", "previous_decision" }) |field| state.update(&@field(self, field));
        var numbers: [32]u8 = undefined;
        inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| std.mem.writeInt(u64, numbers[i * 8 ..][0..8], @field(self, field), .little);
        state.update(&numbers);
        state.update(&.{@backingInt(self.state)});
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
    fn prefixDigest(self: Head) Digest {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-accepted-provenance/v1");
        state.update(&self.previous_provenance);
        state.update(&self.decision_digest);
        state.update(&self.accepted_root);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
    fn validate(self: Head) !void {
        inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest", "batch_digest", "logical_digest", "accepted_root" }) |field| if (!nonzero(&@field(self, field))) return error.InvalidPrivateIndexClaimInventory;
        if (self.ordinal == 0 or self.source_sequence == 0 or self.batch_rows > jobs.private_claims.max_claims or self.state == .failed or
            @as(u128, self.rows) > @as(u128, self.ordinal) * jobs.private_claims.max_claims or
            (std.math.add(u64, self.previous_rows, self.batch_rows) catch return error.InvalidPrivateIndexClaimInventory) != self.rows or
            (self.ordinal == 1 and (self.previous_rows != 0 or nonzero(&self.previous_decision) or nonzero(&self.previous_provenance))) or
            (self.ordinal > 1 and (!nonzero(&self.previous_decision) or !nonzero(&self.previous_provenance))) or
            !equal(self.decision(), self.decision_digest) or !equal(self.prefixDigest(), self.provenance)) return error.InvalidPrivateIndexClaimInventory;
    }
    pub fn encode(self: Head) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AIDA\x01\x00\x00\x00");
        inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest" }, 0..) |field, i| @memcpy(bytes[8 + i * 32 ..][0..32], &@field(self, field));
        inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| std.mem.writeInt(u64, bytes[104 + i * 8 ..][0..8], @field(self, field), .little);
        inline for (.{ "batch_digest", "logical_digest", "previous_decision", "decision_digest", "accepted_root", "provenance" }, 0..) |field, i| @memcpy(bytes[136 + i * 32 ..][0..32], &@field(self, field));
        std.mem.writeInt(u64, bytes[328..336], self.previous_rows, .little);
        @memcpy(bytes[336..368], &self.previous_provenance);
        bytes[368] = @backingInt(self.state);
        @memcpy(bytes[384..416], &hash(bytes[0..384]));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AIDA\x01\x00\x00\x00") or !std.mem.allEqual(u8, bytes[369..384], 0) or
            !std.mem.eql(u8, bytes[384..416], &hash(bytes[0..384]))) return error.InvalidPrivateIndexClaimInventory;
        var result: Head = undefined;
        inline for (.{ "metadata_incarnation", "binding_digest", "scope_digest" }, 0..) |field, i| @field(result, field) = bytes[8 + i * 32 ..][0..32].*;
        inline for (.{ "ordinal", "rows", "batch_rows", "source_sequence" }, 0..) |field, i| @field(result, field) = std.mem.readInt(u64, bytes[104 + i * 8 ..][0..8], .little);
        inline for (.{ "batch_digest", "logical_digest", "previous_decision", "decision_digest", "accepted_root", "provenance" }, 0..) |field, i| @field(result, field) = bytes[136 + i * 32 ..][0..32].*;
        result.previous_rows = std.mem.readInt(u64, bytes[328..336], .little);
        result.previous_provenance = bytes[336..368].*;
        result.state = std.enums.fromInt(Source.State, bytes[368]) orelse return error.InvalidPrivateIndexClaimInventory;
        try result.validate();
        return result;
    }
};

/// Expected logical inventory, not an EOF seal or physical target validation.
/// XOR is only diagnostic; provenance commits exact accepted target proofs.
pub const Inventory = struct {
    pub const encoded_len = 184;
    metadata_incarnation: Digest,
    binding_digest: Digest,
    rows: u64,
    batches: u64,
    inventory_xor: Digest,
    provenance: Digest,
    pub fn encode(self: Inventory) ![encoded_len]u8 {
        if (!nonzero(&self.metadata_incarnation) or !nonzero(&self.binding_digest) or self.batches == 0 or self.rows < self.batches or !nonzero(&self.provenance)) return error.InvalidPrivateIndexClaimInventory;
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AIDI\x01\x00\x00\x00");
        @memcpy(bytes[8..40], &self.metadata_incarnation);
        @memcpy(bytes[40..72], &self.binding_digest);
        std.mem.writeInt(u64, bytes[72..80], self.rows, .little);
        std.mem.writeInt(u64, bytes[80..88], self.batches, .little);
        @memcpy(bytes[88..120], &self.inventory_xor);
        @memcpy(bytes[120..152], &self.provenance);
        @memcpy(bytes[152..184], &hash(bytes[0..152]));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Inventory {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AIDI\x01\x00\x00\x00") or !std.mem.eql(u8, bytes[152..184], &hash(bytes[0..152]))) return error.InvalidPrivateIndexClaimInventory;
        const result: Inventory = .{ .metadata_incarnation = bytes[8..40].*, .binding_digest = bytes[40..72].*, .rows = std.mem.readInt(u64, bytes[72..80], .little), .batches = std.mem.readInt(u64, bytes[80..88], .little), .inventory_xor = bytes[88..120].*, .provenance = bytes[120..152].* };
        _ = try result.encode();
        return result;
    }
};

pub const Request = struct { signed: delivery.Signed, consumed: ?Consumed = null };
const TargetUpdate = struct { ordinal: u16, bytes: [Inventory.encoded_len]u8 };
pub const Prepared = struct {
    alloc: std.mem.Allocator,
    table_id: u64,
    ordinal: u16,
    head: Head,
    head_bytes: [Head.encoded_len]u8,
    targets: []TargetUpdate,
    changed: bool,
    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.targets);
        self.* = undefined;
    }
    /// The outer transaction MUST abort on any failure, including the last put.
    /// Preparation performs all reads/validation before this first write.
    pub fn stage(self: *const Prepared, txn: anytype, group: u64) !void {
        if (!self.changed) return;
        var buf: [192]u8 = undefined;
        for (self.targets) |target| try txn.put(try targetKey(&buf, group, self.table_id, target.ordinal), &target.bytes);
        try txn.put(try sourceKey(&buf, group, self.table_id, self.ordinal), &self.head_bytes);
    }
};

/// `authority` performs actual current metadata reads in this same transaction;
/// it is not a boolean supplied by a client. No writes occur until stage().
pub fn prepare(alloc: std.mem.Allocator, txn: anytype, group: u64, request: Request, authority: anytype) !Prepared {
    const accepted_root = try authority.verifyDelivery(txn, group, request.signed);
    const data = request.signed.data;
    const incarnation = request.signed.proof.identity.metadata_incarnation;
    const binding_digest = try bindingDigest(data.source);
    const logical_digest = try data.logicalDigest();
    const raw = &data.header.bytes;
    const ordinal = std.mem.readInt(u64, raw[104..112], .little);
    const sequence = (try data.header.position()).sequence;
    var buf: [192]u8 = undefined;
    // Decode borrowed native buffers before the next read.
    const previous = if (try optional(txn, try sourceKey(&buf, group, data.source.fence.namespace.table_id, data.source.ordinal))) |bytes| try Head.decode(bytes) else null;
    if (previous) |head| {
        if (!equal(head.metadata_incarnation, incarnation) or !equal(head.binding_digest, binding_digest) or !equal(head.scope_digest, raw[40..72].*)) return error.PrivateIndexOwnerJobChanged;
        if (ordinal == head.ordinal) {
            if (!equal(head.batch_digest, data.header.digest()) or !equal(head.logical_digest, logical_digest)) return error.GenerationPublicationChanged;
            return .{ .alloc = alloc, .table_id = data.source.fence.namespace.table_id, .ordinal = data.source.ordinal, .head = head, .head_bytes = try head.encode(), .targets = try alloc.alloc(TargetUpdate, 0), .changed = false };
        }
        if (head.state != .building or head.ordinal == std.math.maxInt(u64) or ordinal != head.ordinal + 1 or sequence <= head.source_sequence or !equal(raw[256..288].*, head.batch_digest)) return error.GenerationPublicationChanged;
        const consumed = request.consumed orelse return error.PrivateIndexSourceConsumptionRequired;
        if (!equal(consumed.metadata_incarnation, incarnation) or !equal(consumed.binding_digest, binding_digest) or consumed.ordinal != head.ordinal or
            !equal(consumed.batch_digest, head.batch_digest) or !equal(consumed.decision_digest, head.decision_digest) or consumed.rows != head.rows or
            consumed.position.sequence <= head.source_sequence or !consumed.position.namespace.eql(data.source.fence.namespace) or
            !std.meta.eql(consumed.proof.identity, request.signed.proof.identity) or consumed.proof.root_generation != request.signed.proof.root_generation or
            consumed.proof.reporter_incarnation != request.signed.proof.reporter_incarnation or !std.meta.eql(consumed.proof.authority, request.signed.proof.authority)) return error.InvalidPrivateIndexClaimReceipt;
        try authority.verifyConsumption(txn, group, data.source, consumed);
    } else if (ordinal != 1 or nonzero(raw[256..288]) or request.consumed != null) return error.GenerationPublicationChanged;
    const previous_rows = if (previous) |head| head.rows else 0;
    const batch_rows = std.mem.readInt(u32, raw[96..100], .little);
    var head: Head = .{
        .metadata_incarnation = incarnation,
        .binding_digest = binding_digest,
        .scope_digest = raw[40..72].*,
        .ordinal = ordinal,
        .rows = std.math.add(u64, previous_rows, batch_rows) catch return error.InvalidPrivateIndexClaimInventory,
        .batch_rows = batch_rows,
        .source_sequence = sequence,
        .batch_digest = data.header.digest(),
        .logical_digest = logical_digest,
        .previous_decision = if (previous) |old| old.decision_digest else @splat(0),
        .decision_digest = undefined,
        .accepted_root = accepted_root,
        .provenance = undefined,
        .previous_rows = previous_rows,
        .previous_provenance = if (previous) |old| old.provenance else @splat(0),
        .state = std.enums.fromInt(Source.State, raw[184]) orelse return error.InvalidPrivateIndexClaimInventory,
    };
    head.decision_digest = head.decision();
    head.provenance = head.prefixDigest();
    const head_bytes = try head.encode();
    const targets = try alloc.alloc(TargetUpdate, data.parts.items.len);
    errdefer alloc.free(targets);
    var seen: [local.storage_db_relational_index_publication_progress.max_owners]bool = @splat(false);
    for (data.parts.items, targets) |part, *target| {
        const binding = part.target.outcome.binding;
        if (seen[binding.ordinal]) return error.InvalidPrivateIndexClaimReceipt;
        seen[binding.ordinal] = true;
        const target_digest = try bindingDigest(binding);
        var inventory: Inventory = if (try optional(txn, try targetKey(&buf, group, data.source.fence.namespace.table_id, binding.ordinal))) |bytes| try Inventory.decode(bytes) else .{ .metadata_incarnation = incarnation, .binding_digest = target_digest, .rows = 0, .batches = 0, .inventory_xor = @splat(0), .provenance = @splat(0) };
        if (!equal(inventory.metadata_incarnation, incarnation) or !equal(inventory.binding_digest, target_digest)) return error.PrivateIndexOwnerJobChanged;
        inventory.rows = std.math.add(u64, inventory.rows, part.rows) catch return error.InvalidPrivateIndexClaimInventory;
        inventory.batches = std.math.add(u64, inventory.batches, 1) catch return error.InvalidPrivateIndexClaimInventory;
        for (&inventory.inventory_xor, part.inventory_xor) |*byte, contribution| byte.* ^= contribution;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-target-provenance/v1");
        state.update(&inventory.provenance);
        state.update(&head.decision_digest);
        state.update(&try part.target.commitment());
        state.update(&part.row_mask);
        state.update(&part.inventory_xor);
        state.final(&inventory.provenance);
        target.* = .{ .ordinal = binding.ordinal, .bytes = try inventory.encode() };
    }
    return .{ .alloc = alloc, .table_id = data.source.fence.namespace.table_id, .ordinal = data.source.ordinal, .head = head, .head_bytes = head_bytes, .targets = targets, .changed = true };
}

test "relational index publication source acceptance propagates every staged write failure and skips duplicate writes" {
    const Writer = struct {
        calls: usize = 0,
        fail_at: usize,
        fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            self.calls += 1;
            if (self.calls == self.fail_at) return error.StorageWriteFailed;
        }
    };
    var targets = [_]TargetUpdate{
        .{ .ordinal = 17, .bytes = @splat(0) },
        .{ .ordinal = 4095, .bytes = @splat(0) },
    };
    var prepared: Prepared = .{ .alloc = std.testing.allocator, .table_id = 101, .ordinal = 3999, .head = undefined, .head_bytes = @splat(0), .targets = &targets, .changed = true };
    for (1..4) |fail_at| {
        var writer: Writer = .{ .fail_at = fail_at };
        try std.testing.expectError(error.StorageWriteFailed, prepared.stage(&writer, 13));
        try std.testing.expectEqual(fail_at, writer.calls);
    }
    var writer: Writer = .{ .fail_at = 4 };
    try prepared.stage(&writer, 13);
    try std.testing.expectEqual(@as(usize, 3), writer.calls);
    prepared.changed = false;
    writer = .{ .fail_at = 1 };
    try prepared.stage(&writer, 13);
    try std.testing.expectEqual(@as(usize, 0), writer.calls);
}

test "relational index publication target inventory codec rejects invalid counts and canonical corruption" {
    const inventory: Inventory = .{ .metadata_incarnation = @splat(1), .binding_digest = @splat(2), .rows = 2, .batches = 1, .inventory_xor = @splat(0), .provenance = @splat(3) };
    const bytes = try inventory.encode();
    try std.testing.expectEqualDeep(inventory, try Inventory.decode(&bytes));
    for (0..bytes.len) |i| {
        var corrupt = bytes;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Inventory.decode(&corrupt));
    }
    var invalid = inventory;
    invalid.batches = 3;
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, invalid.encode());
    invalid = inventory;
    invalid.batches = 0;
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, invalid.encode());
    invalid = inventory;
    invalid.binding_digest = @splat(0);
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, invalid.encode());
}
