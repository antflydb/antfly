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
//! never commits transactions or enables source deletion. Metadata acceptance
//! and native consumption are separate durable decisions.
const std = @import("std");
const local = @import("antfly_local_sources");
const jobs = local.storage_db_relational_index_owner_job;
const Source = jobs.private_claims.SourceStream;
const positions = local.storage_db_receipt_position;
const delivery = @import("relational_index_source_delivery.zig");
const publication = @import("relational_index_publication.zig");
const progress = local.storage_db_relational_index_publication_progress;
const Digest = [32]u8;
const prefix = "\x00\x00__metadata__:relational_index_delivery:v1:";
pub const max_command_bytes = delivery.max_wire_bytes + 16 * 1024;
pub fn allGroupsPrefix() []const u8 {
    return prefix;
}
pub fn sourcesPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}source:{d}:", .{ prefix, group });
}
pub fn targetsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}target:{d}:", .{ prefix, group });
}
pub const Key = struct {
    kind: enum { source, target },
    group: u64,
    table: u64,
    ordinal: u16,
    pub fn parse(bytes: []const u8) !?Key {
        if (!std.mem.startsWith(u8, bytes, prefix)) return null;
        var parts = std.mem.splitScalar(u8, bytes[prefix.len..], ':');
        const kind = parts.next() orelse return error.InvalidCatalogRecord;
        const result: Key = .{
            .kind = if (std.mem.eql(u8, kind, "source")) .source else if (std.mem.eql(u8, kind, "target")) .target else return error.InvalidCatalogRecord,
            .group = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidCatalogRecord, 10) catch return error.InvalidCatalogRecord,
            .table = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord,
            .ordinal = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord,
        };
        if (parts.next() != null or result.group == 0 or result.table == 0 or result.ordinal >= progress.max_owners) return error.InvalidCatalogRecord;
        var buf: [192]u8 = undefined;
        const canonical = if (result.kind == .source) try sourceKey(&buf, result.group, result.table, result.ordinal) else try targetKey(&buf, result.group, result.table, result.ordinal);
        if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidCatalogRecord;
        return result;
    }
};

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

    pub fn fromRecord(record: Source.Consumption, proof: publication.RootProof) Consumed {
        const consumed_prefix = record.prefix();
        return .{ .metadata_incarnation = consumed_prefix.metadata_incarnation, .binding_digest = consumed_prefix.binding_digest, .ordinal = consumed_prefix.ordinal, .batch_digest = consumed_prefix.batch_digest, .decision_digest = consumed_prefix.decision_digest, .rows = consumed_prefix.rows, .position = consumed_prefix.position, .proof = proof };
    }

    pub fn digest(self: Consumed) !Digest {
        if (self.ordinal == 0 or self.proof.authority != .native or
            !nonzero(&self.metadata_incarnation) or !nonzero(&self.binding_digest) or
            !nonzero(&self.batch_digest) or !nonzero(&self.decision_digest) or
            !equal(self.metadata_incarnation, self.proof.identity.metadata_incarnation)) return error.InvalidPrivateIndexClaimReceipt;
        return (Source.ConsumedPrefix{ .metadata_incarnation = self.metadata_incarnation, .binding_digest = self.binding_digest, .ordinal = self.ordinal, .batch_digest = self.batch_digest, .decision_digest = self.decision_digest, .rows = self.rows, .position = self.position }).digest();
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

    pub fn accepted(self: Head) Source.Accepted {
        return .{ .metadata_incarnation = self.metadata_incarnation, .binding_digest = self.binding_digest, .scope_digest = self.scope_digest, .ordinal = self.ordinal, .rows = self.rows, .batch_rows = self.batch_rows, .source_sequence = self.source_sequence, .batch_digest = self.batch_digest, .logical_digest = self.logical_digest, .previous_decision = self.previous_decision, .decision_digest = self.decision_digest, .state = self.state };
    }
    fn decision(self: Head) Digest {
        return self.accepted().computedDigest();
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

pub const SourceRecord = struct {
    head: Head,
    signed: std.json.Parsed(delivery.Signed),
    pub fn deinit(self: *SourceRecord) void {
        self.signed.deinit();
        self.* = undefined;
    }
    fn requireHead(head: Head, signed: delivery.Signed) !void {
        const data = signed.data;
        const raw = &data.header.bytes;
        if (!equal(head.metadata_incarnation, signed.proof.identity.metadata_incarnation) or !equal(head.binding_digest, try bindingDigest(data.source)) or
            !equal(head.scope_digest, raw[40..72].*) or head.ordinal != std.mem.readInt(u64, raw[104..112], .little) or
            head.batch_rows != std.mem.readInt(u32, raw[96..100], .little) or head.source_sequence != (try data.header.position()).sequence or
            @backingInt(head.state) != raw[184] or !equal(head.batch_digest, data.header.digest()) or !equal(head.logical_digest, try data.logicalDigest()) or
            !equal(head.accepted_root, try signed.proof.commitment(try data.digest()))) return error.InvalidCatalogRecord;
    }
    pub fn encode(alloc: std.mem.Allocator, head: Head, signed: delivery.Signed) ![]u8 {
        try requireHead(head, signed);
        const json = try std.json.Stringify.valueAlloc(alloc, signed, .{});
        defer alloc.free(json);
        if (json.len == 0 or json.len > delivery.max_wire_bytes) return error.InvalidCatalogRecord;
        return std.mem.concat(alloc, u8, &.{ &try head.encode(), json });
    }
    pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !SourceRecord {
        if (bytes.len <= Head.encoded_len or bytes.len > Head.encoded_len + delivery.max_wire_bytes) return error.InvalidCatalogRecord;
        const head = try Head.decode(bytes[0..Head.encoded_len]);
        var signed = try std.json.parseFromSlice(delivery.Signed, alloc, bytes[Head.encoded_len..], .{ .allocate = .alloc_always, .max_value_len = 128 });
        errdefer signed.deinit();
        try signed.value.verify();
        for (signed.value.data.parts.items) |part| try part.target.verifyRecorded(part.rows);
        try requireHead(head, signed.value);
        const canonical = try std.json.Stringify.valueAlloc(alloc, signed.value, .{});
        defer alloc.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes[Head.encoded_len..])) return error.InvalidCatalogRecord;
        return .{ .head = head, .signed = signed };
    }
};

pub const TargetRecord = struct {
    pub const encoded_len = jobs.Binding.encoded_len + Inventory.encoded_len;
    binding: jobs.Binding,
    inventory: Inventory,
    pub fn encode(self: TargetRecord) ![encoded_len]u8 {
        if (!equal(try bindingDigest(self.binding), self.inventory.binding_digest) or self.binding.authority != .native or self.binding.phase != .reserved) return error.InvalidCatalogRecord;
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..jobs.Binding.encoded_len], &try self.binding.encode());
        @memcpy(bytes[jobs.Binding.encoded_len..], &try self.inventory.encode());
        return bytes;
    }
    pub fn decode(bytes: []const u8) !TargetRecord {
        if (bytes.len != encoded_len) return error.InvalidCatalogRecord;
        const result: TargetRecord = .{ .binding = try jobs.Binding.decode(bytes[0..jobs.Binding.encoded_len]), .inventory = try Inventory.decode(bytes[jobs.Binding.encoded_len..]) };
        _ = try result.encode();
        return result;
    }
};

pub const Query = struct {
    table_id: u64,
    group_id: u64,
    id: [16]u8,
    pub fn from(request: Request) Query {
        const binding = request.signed.data.source;
        return .{ .table_id = binding.fence.namespace.table_id, .group_id = binding.fence.owner_group_id, .id = binding.id };
    }
    pub fn validate(self: Query) !void {
        if (self.table_id == 0 or self.group_id == 0 or !nonzero(&self.id)) return error.InvalidGenerationPublication;
    }
};
pub const Status = struct {
    query: Query,
    head: ?Head,
    replication_pending: bool = false,
    pub fn isAccepted(self: Status, request: Request) !bool {
        if (!std.meta.eql(self.query, Query.from(request))) return error.InvalidGenerationPublication;
        if (self.replication_pending) return false;
        const head = self.head orelse return false;
        _ = try head.encode();
        return equal(head.metadata_incarnation, request.signed.proof.identity.metadata_incarnation) and equal(head.binding_digest, try bindingDigest(request.signed.data.source)) and
            equal(head.batch_digest, request.signed.data.header.digest()) and equal(head.logical_digest, try request.signed.data.logicalDigest());
    }
};
pub const Request = struct { signed: delivery.Signed, consumed: ?Consumed = null };
const TargetUpdate = struct { ordinal: u16, bytes: [TargetRecord.encoded_len]u8 };
const OwnerUpdate = struct { ordinal: u16, bytes: [progress.OwnerState.encoded_len]u8 };
pub const Prepared = struct {
    alloc: std.mem.Allocator,
    table_id: u64,
    ordinal: u16,
    head: Head,
    head_bytes: [Head.encoded_len]u8,
    targets: []TargetUpdate,
    source_bytes: ?[]u8 = null,
    owners: []OwnerUpdate = &.{},
    changed: bool,
    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.targets);
        if (self.source_bytes) |bytes| self.alloc.free(bytes);
        self.alloc.free(self.owners);
        self.* = undefined;
    }
    /// The outer transaction MUST abort on any failure, including the last put.
    /// Preparation performs all reads/validation before this first write.
    pub fn stage(self: *const Prepared, txn: anytype, group: u64) !void {
        if (!self.changed) return;
        var buf: [192]u8 = undefined;
        for (self.targets) |target| try txn.put(try targetKey(&buf, group, self.table_id, target.ordinal), &target.bytes);
        try txn.put(try sourceKey(&buf, group, self.table_id, self.ordinal), self.source_bytes orelse &self.head_bytes);
        for (self.owners, 0..) |owner, i| {
            if (owner.ordinal == self.ordinal and i + 1 != self.owners.len) continue;
            try txn.put(try publication.ownerKey(&buf, group, self.table_id, owner.ordinal), &owner.bytes);
        }
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
    var source_owner = try progress.OwnerState.decode(try txn.get(try publication.ownerKey(&buf, group, data.source.fence.namespace.table_id, data.source.ordinal)));
    // Decode/hash borrowed native buffers before the next read.
    const previous = if (try optional(txn, try sourceKey(&buf, group, data.source.fence.namespace.table_id, data.source.ordinal))) |bytes| blk: {
        if (bytes.len <= Head.encoded_len or bytes.len > Head.encoded_len + delivery.max_wire_bytes or !equal(hash(bytes), source_owner.source_delivery)) return error.InvalidCatalogRecord;
        break :blk try Head.decode(bytes[0..Head.encoded_len]);
    } else blk: {
        if (nonzero(&source_owner.source_delivery)) return error.InvalidCatalogRecord;
        break :blk null;
    };
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
    const source_bytes = try SourceRecord.encode(alloc, head, request.signed);
    errdefer alloc.free(source_bytes);
    source_owner.source_delivery = hash(source_bytes);
    const targets = try alloc.alloc(TargetUpdate, data.parts.items.len);
    errdefer alloc.free(targets);
    const owners = try alloc.alloc(OwnerUpdate, data.parts.items.len + 1);
    errdefer alloc.free(owners);
    var seen: [local.storage_db_relational_index_publication_progress.max_owners]bool = @splat(false);
    for (data.parts.items, targets) |part, *target| {
        const binding = part.target.outcome.binding;
        if (seen[binding.ordinal]) return error.InvalidPrivateIndexClaimReceipt;
        seen[binding.ordinal] = true;
        const target_digest = try bindingDigest(binding);
        var owner = if (binding.ordinal == data.source.ordinal) source_owner else try progress.OwnerState.decode(try txn.get(try publication.ownerKey(&buf, group, data.source.fence.namespace.table_id, binding.ordinal)));
        var inventory: Inventory = if (try optional(txn, try targetKey(&buf, group, data.source.fence.namespace.table_id, binding.ordinal))) |bytes| blk: {
            if (!equal(hash(bytes), owner.target_inventory)) return error.InvalidCatalogRecord;
            break :blk (try TargetRecord.decode(bytes)).inventory;
        } else blk: {
            if (nonzero(&owner.target_inventory)) return error.InvalidCatalogRecord;
            break :blk .{ .metadata_incarnation = incarnation, .binding_digest = target_digest, .rows = 0, .batches = 0, .inventory_xor = @splat(0), .provenance = @splat(0) };
        };
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
        target.* = .{ .ordinal = binding.ordinal, .bytes = try (TargetRecord{ .binding = binding, .inventory = inventory }).encode() };
        owner.target_inventory = hash(&target.bytes);
        if (binding.ordinal == data.source.ordinal) source_owner = owner;
        owners[part.index] = .{ .ordinal = binding.ordinal, .bytes = try owner.encode() };
    }
    owners[data.parts.items.len] = .{ .ordinal = data.source.ordinal, .bytes = try source_owner.encode() };
    return .{ .alloc = alloc, .table_id = data.source.fence.namespace.table_id, .ordinal = data.source.ordinal, .head = head, .head_bytes = head_bytes, .source_bytes = source_bytes, .targets = targets, .owners = owners, .changed = true };
}

fn anchored(reader: anytype, key: []const u8, expected: Digest) !?[]const u8 {
    const bytes = try optional(reader, key);
    if (bytes) |value| {
        if (!equal(hash(value), expected)) return error.InvalidCatalogRecord;
    } else if (nonzero(&expected)) return error.InvalidCatalogRecord;
    return bytes;
}
fn requireRetainedBinding(alloc: std.mem.Allocator, reader: anytype, group: u64, head: progress.Head, binding: jobs.Binding) !void {
    if (binding.ordinal >= head.owner_count or binding.phase != .reserved or !std.mem.eql(u8, &binding.id, &head.plan_id) or !equal(binding.plan_digest, head.plan_digest)) return error.InvalidCatalogRecord;
    var buf: [192]u8 = undefined;
    const owner = try progress.OwnerState.decode(try reader.get(try publication.ownerKey(&buf, group, head.table_id, binding.ordinal)));
    if (owner.owner.namespace.table_id != head.table_id) return error.InvalidCatalogRecord;
    var dispatch = try publication.readDispatch(alloc, reader, group, owner.owner);
    defer dispatch.deinit();
    try (publication.OwnerDecision{ .metadata_group_id = group, .metadata_incarnation = @splat(0), .head = head, .owner = owner, .disposition = .execute, .dispatch = dispatch.value }).requireRetainedNativeBinding(binding);
}
fn requireEnrollment(alloc: std.mem.Allocator, reader: anytype, group: u64, proof: publication.RootProof, incarnation: Digest) !void {
    const enrollment = @import("store_root_enrollment.zig");
    if (!equal(proof.identity.metadata_incarnation, incarnation)) return error.InvalidCatalogRecord;
    var buf: [192]u8 = undefined;
    const bytes = try reader.get(try enrollment.key(&buf, group, proof.identity.store_id, proof.identity.root_incarnation));
    if (bytes.len > 1024) return error.InvalidCatalogRecord;
    var identity = try std.json.parseFromSlice(enrollment.Identity, alloc, bytes, .{});
    defer identity.deinit();
    if (!std.meta.eql(identity.value, proof.identity)) return error.InvalidCatalogRecord;
}
pub const Counts = struct { sources: usize = 0, targets: usize = 0, source_rows: u64 = 0, target_rows: u64 = 0 };
/// Complete forward checkpoint verification; online acceptance never scans the
/// owner cohort. Exact owner anchors also protect zero-row source decisions.
pub fn verifyOwner(alloc: std.mem.Allocator, reader: anytype, group: u64, head: progress.Head, owner: progress.OwnerState, incarnation: Digest) !Counts {
    var result: Counts = .{};
    var buf: [192]u8 = undefined;
    if (try anchored(reader, try sourceKey(&buf, group, head.table_id, owner.owner.ordinal), owner.source_delivery)) |bytes| {
        var record = try SourceRecord.decode(alloc, bytes);
        defer record.deinit();
        if (!equal(record.head.metadata_incarnation, incarnation) or record.signed.value.data.source.ordinal != owner.owner.ordinal) return error.InvalidCatalogRecord;
        try requireRetainedBinding(alloc, reader, group, head, record.signed.value.data.source);
        try requireEnrollment(alloc, reader, group, record.signed.value.proof, incarnation);
        for (record.signed.value.data.parts.items) |part| {
            try requireRetainedBinding(alloc, reader, group, head, part.target.outcome.binding);
            try requireEnrollment(alloc, reader, group, part.target.proof, incarnation);
        }
        result.sources = 1;
        result.source_rows = record.head.rows;
    }
    if (try anchored(reader, try targetKey(&buf, group, head.table_id, owner.owner.ordinal), owner.target_inventory)) |bytes| {
        const record = try TargetRecord.decode(bytes);
        if (!equal(record.inventory.metadata_incarnation, incarnation) or record.binding.ordinal != owner.owner.ordinal) return error.InvalidCatalogRecord;
        try requireRetainedBinding(alloc, reader, group, head, record.binding);
        result.targets = 1;
        result.target_rows = record.inventory.rows;
    }
    return result;
}
pub fn deleteOwner(txn: anytype, group: u64, table: u64, owner: progress.OwnerState) !void {
    var buf: [192]u8 = undefined;
    if (try anchored(txn, try sourceKey(&buf, group, table, owner.owner.ordinal), owner.source_delivery) != null) try txn.delete(try sourceKey(&buf, group, table, owner.owner.ordinal));
    if (try anchored(txn, try targetKey(&buf, group, table, owner.owner.ordinal), owner.target_inventory) != null) try txn.delete(try targetKey(&buf, group, table, owner.owner.ordinal));
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
