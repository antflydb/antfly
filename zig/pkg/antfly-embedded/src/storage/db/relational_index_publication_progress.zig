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

//! Bounded metadata progress for a regular, index-only UNIQUE build.
//! The head and one owner record are updated in the SAME metadata transaction.
//! Owner membership is immutable, admitted from the complete range-owner cut.
//! Receipts are evidence only AFTER the controller independently verifies the
//! owner operation at its durable applied cut; hashes are not authentication.
//! This module does not admit plans, grant owner authority, or publish schemas.
const std = @import("std");
const positions = @import("receipt_position.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;

pub const Id = [16]u8;
pub const Digest = [32]u8;
pub const max_owners = 4096;
pub const Authority = enum(u8) { raft = 1, native = 2 };
/// Verified owner evidence must have these semantics:
/// fenced: exact source fence is durable and predecessor participants drained;
/// reserved: exact allocation high-water burn is committed, no schema exposed;
/// validated: private generation and distributed unique claims are complete;
/// installed: exact candidate schema/catalog and activation state are committed;
/// released: the matching source fence is durably released;
/// canceled: fence/drain/burn complete, only this plan's artifacts/claims cleaned;
/// cancel_released: the matching canceled source fence is durably released.
/// In particular, canceled is NOT just an ACK that an unknown job was absent.
pub const Action = enum(u8) { fenced, reserved, validated, installed, released, canceled, cancel_released };
pub const Phase = enum(u8) { fencing, reserving, building, installing, releasing, published, canceling, cancel_releasing, canceled };
pub const Failure = enum(u8) { none, duplicate_key, invalid_candidate, requested };

fn nonzero(bytes: []const u8) bool {
    return !std.mem.allEqual(u8, bytes, 0);
}
fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn bit(action: Action) u8 {
    return @as(u8, 1) << @intCast(@backingInt(action));
}
fn checksum(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

/// Fields here are copied from the admitted plan, never from an ACK request.
pub const Owner = struct {
    plan_id: Id,
    plan_digest: Digest,
    ordinal: u16,
    group_id: u64,
    namespace: Namespace,
    authority: Authority,
    fence_digest: Digest,

    pub fn validate(self: Owner) !void {
        if (!nonzero(&self.plan_id) or !nonzero(&self.plan_digest) or !nonzero(&self.fence_digest) or
            self.ordinal >= max_owners or self.group_id == 0 or self.namespace.table_id == 0 or
            self.namespace.shard_id == 0 or self.namespace.range_id == 0)
            return error.InvalidGenerationPublication;
    }
    fn eql(a: Owner, b: Owner) bool {
        return std.mem.eql(u8, &a.plan_id, &b.plan_id) and equal(a.plan_digest, b.plan_digest) and
            a.ordinal == b.ordinal and a.group_id == b.group_id and a.namespace.eql(b.namespace) and a.authority == b.authority and equal(a.fence_digest, b.fence_digest);
    }
};

pub const Receipt = struct {
    owner: Owner,
    action: Action,
    position: positions.Position,
    candidate_digest: Digest,
    reservation_digest: Digest,
    /// Owner-bound validation/cleanup/install evidence, not just an HTTP ACK.
    evidence_digest: Digest,

    pub fn digest(self: Receipt) !Digest {
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update("antfly:index-publication-receipt:v2");
        hasher.update(&self.owner.plan_id);
        hasher.update(&self.owner.plan_digest);
        var numbers: [36]u8 = undefined;
        std.mem.writeInt(u16, numbers[0..2], self.owner.ordinal, .little);
        std.mem.writeInt(u64, numbers[2..10], self.owner.group_id, .little);
        std.mem.writeInt(u64, numbers[10..18], self.owner.namespace.table_id, .little);
        std.mem.writeInt(u64, numbers[18..26], self.owner.namespace.shard_id, .little);
        std.mem.writeInt(u64, numbers[26..34], self.owner.namespace.range_id, .little);
        numbers[34] = @backingInt(self.action);
        numbers[35] = @backingInt(self.owner.authority);
        hasher.update(&numbers);
        try self.position.requireNamespace(self.owner.namespace);
        hasher.update(&try self.position.encode());
        hasher.update(&self.owner.fence_digest);
        hasher.update(&self.candidate_digest);
        hasher.update(&self.reservation_digest);
        hasher.update(&self.evidence_digest);
        var result: Digest = undefined;
        hasher.final(&result);
        return result;
    }
};

/// Fixed-size head: ACK application does not rewrite/reparse a 4,096-owner plan.
pub const Head = struct {
    pub const encoded_len = 208;
    reader_requirements: ?@import("../../common/schema_readers.zig").ReaderRequirements = null,
    plan_id: Id,
    plan_digest: Digest,
    candidate_digest: Digest,
    reservation_digest: Digest,
    table_id: u64,
    revision: u64 = 1,
    owner_count: u16,
    phase: Phase = .fencing,
    failure: Failure = .none,
    counts: [7]u16 = @splat(0),
    completed_sources: u16 = 0,
    /// Stable metadata barrier once the last source's consumed EOF commits.
    completion_revision: u64 = 0,

    pub fn validate(self: Head) !void {
        if (self.reader_requirements) |requirements| if (!requirements.valid()) return error.InvalidGenerationPublication;
        if (!nonzero(&self.plan_id) or !nonzero(&self.plan_digest) or !nonzero(&self.candidate_digest) or
            !nonzero(&self.reservation_digest) or equal(self.candidate_digest, self.reservation_digest) or
            self.table_id == 0 or self.revision == 0 or self.owner_count == 0 or self.owner_count > max_owners)
            return error.InvalidGenerationPublication;
        for (self.counts) |count| if (count > self.owner_count) return error.InvalidGenerationPublication;
        if (self.completed_sources > self.owner_count or
            (self.completed_sources == self.owner_count) != (self.completion_revision != 0) or
            self.completion_revision > self.revision or
            (self.completed_sources != 0 and (self.phase == .fencing or self.phase == .reserving)) or
            (self.completed_sources != 0 and self.counts[1] != self.owner_count)) return error.InvalidGenerationPublication;
        const c = self.counts;
        if (c[1] > c[0] or c[2] > c[1] or c[3] > c[2] or c[4] > c[3] or c[5] > c[1] or c[6] > c[5])
            return error.InvalidGenerationPublication;
        const canceling = self.phase == .canceling or self.phase == .cancel_releasing or self.phase == .canceled;
        if (canceling != (self.failure != .none)) return error.InvalidGenerationPublication;
        if (canceling) {
            if (c[3] != 0 or c[4] != 0) return error.InvalidGenerationPublication;
            if (self.phase != .canceling and c[5] != self.owner_count) return error.InvalidGenerationPublication;
            if (self.phase == .canceling and c[6] != 0) return error.InvalidGenerationPublication;
            if (self.phase == .canceled and c[6] != self.owner_count) return error.InvalidGenerationPublication;
            return;
        }
        if (c[5] != 0 or c[6] != 0) return error.InvalidGenerationPublication;
        const required: usize = switch (self.phase) {
            .fencing => 0,
            .reserving => 1,
            .building => 2,
            .installing => 3,
            .releasing => 4,
            .published => 5,
            else => unreachable,
        };
        for (c[0..required]) |count| if (count != self.owner_count) return error.InvalidGenerationPublication;
        for (c[@min(required + 1, 5)..5]) |count| if (count != 0) return error.InvalidGenerationPublication;
    }

    fn checkRevision(self: Head, expected: u64) !void {
        try self.validate();
        if (self.revision != expected) return error.GenerationPublicationChanged;
    }
    fn bump(self: *Head) !void {
        self.revision = std.math.add(u64, self.revision, 1) catch return error.GenerationPublicationChanged;
    }

    /// Metadata verifies the actual signed consumption before using this
    /// bounded counter update. Owner completion is staged in the same txn.
    pub fn completeSource(self: Head, owner: OwnerState) !Head {
        try self.validate();
        try owner.validate();
        if (self.phase != .building or owner.owner.authority != .native or
            !equal(owner.owner.plan_digest, self.plan_digest) or !std.mem.eql(u8, &owner.owner.plan_id, &self.plan_id) or
            owner.owner.namespace.table_id != self.table_id or owner.owner.ordinal >= self.owner_count or
            !owner.has(.reserved) or !nonzero(&owner.source_delivery) or owner.source_completed or
            self.completed_sources == self.owner_count) return error.GenerationPublicationChanged;
        var next = self;
        next.completed_sources += 1;
        try next.bump();
        if (next.completed_sources == next.owner_count) next.completion_revision = next.revision;
        try next.validate();
        return next;
    }

    /// Stage this decision AND the public descriptor/name-reservation switch
    /// atomically. Once decided, cancellation is forbidden; recovery installs.
    pub fn decidePublish(self: Head, expected: u64) !Head {
        try self.checkRevision(expected);
        if (self.phase != .building or self.counts[2] != self.owner_count) return error.GenerationPublicationChanged;
        var next = self;
        next.phase = .installing;
        try next.bump();
        return next;
    }

    /// No source fence may be released here. Cancellation first obtains cleanup
    /// proof from EVERY owner, including unvisited ones, with generation burn.
    pub fn cancel(self: Head, expected: u64, failure: Failure) !Head {
        try self.checkRevision(expected);
        if (failure == .none) return error.InvalidGenerationPublication;
        if (self.phase == .canceling or self.phase == .cancel_releasing or self.phase == .canceled) {
            if (self.failure != failure) return error.GenerationPublicationChanged;
            return self;
        }
        if (self.phase != .fencing and self.phase != .reserving and self.phase != .building)
            return error.GenerationPublicationChanged;
        var next = self;
        next.phase = .canceling;
        next.failure = failure;
        try next.bump();
        return next;
    }

    pub fn encode(self: Head) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "AIPH");
        bytes[4] = 3;
        bytes[5] = @backingInt(self.phase);
        bytes[6] = @backingInt(self.failure);
        @memcpy(bytes[8..24], &self.plan_id);
        @memcpy(bytes[24..56], &self.plan_digest);
        @memcpy(bytes[56..88], &self.candidate_digest);
        @memcpy(bytes[88..120], &self.reservation_digest);
        std.mem.writeInt(u64, bytes[120..128], self.table_id, .little);
        std.mem.writeInt(u64, bytes[128..136], self.revision, .little);
        std.mem.writeInt(u16, bytes[136..138], self.owner_count, .little);
        for (self.counts, 0..) |count, i| std.mem.writeInt(u16, bytes[138 + i * 2 ..][0..2], count, .little);
        if (self.reader_requirements) |requirements| {
            std.mem.writeInt(u32, bytes[152..156], requirements.semantic_version, .little);
            std.mem.writeInt(u32, bytes[156..160], requirements.encoding_version, .little);
        }
        std.mem.writeInt(u16, bytes[160..162], self.completed_sources, .little);
        std.mem.writeInt(u64, bytes[168..176], self.completion_revision, .little);
        @memcpy(bytes[176..208], &checksum(bytes[0..176]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "AIPH") or bytes[4] != 3 or bytes[7] != 0 or
            !std.mem.allEqual(u8, bytes[162..168], 0) or
            !std.mem.eql(u8, &checksum(bytes[0..176]), bytes[176..208])) return error.InvalidGenerationPublication;
        var result: Head = .{
            .plan_id = bytes[8..24].*,
            .plan_digest = bytes[24..56].*,
            .candidate_digest = bytes[56..88].*,
            .reservation_digest = bytes[88..120].*,
            .table_id = std.mem.readInt(u64, bytes[120..128], .little),
            .revision = std.mem.readInt(u64, bytes[128..136], .little),
            .owner_count = std.mem.readInt(u16, bytes[136..138], .little),
            .phase = std.enums.fromInt(Phase, bytes[5]) orelse return error.InvalidGenerationPublication,
            .failure = std.enums.fromInt(Failure, bytes[6]) orelse return error.InvalidGenerationPublication,
            .completed_sources = std.mem.readInt(u16, bytes[160..162], .little),
            .completion_revision = std.mem.readInt(u64, bytes[168..176], .little),
        };
        const semantic = std.mem.readInt(u32, bytes[152..156], .little);
        const encoding = std.mem.readInt(u32, bytes[156..160], .little);
        if (semantic != 0 or encoding != 0) result.reader_requirements = .{ .semantic_version = semantic, .encoding_version = encoding };
        for (&result.counts, 0..) |*count, i| count.* = std.mem.readInt(u16, bytes[138 + i * 2 ..][0..2], .little);
        try result.validate();
        return result;
    }
};

pub const OwnerState = struct {
    pub const encoded_len = 712;
    owner: Owner,
    flags: u8 = 0,
    position: ?positions.Position = null,
    receipts: [7]Digest = @splat(@splat(0)),
    /// Physical commitments are installed by the authenticated metadata
    /// adapter. The pure logical progress machine does not invent root proof.
    attestations: [7]Digest = @splat(@splat(0)),
    /// Metadata-owned record commitments; never native publication evidence.
    source_delivery: Digest = @splat(0),
    target_inventory: Digest = @splat(0),
    source_completed: bool = false,

    fn has(self: OwnerState, action: Action) bool {
        return self.flags & bit(action) != 0;
    }

    pub fn latestReceiptAction(self: OwnerState) !?Action {
        try self.validate();
        var index = self.receipts.len;
        while (index != 0) {
            index -= 1;
            if (nonzero(&self.receipts[index])) return @fromBackingInt(@intCast(index));
        }
        return null;
    }
    pub fn validate(self: OwnerState) !void {
        try self.owner.validate();
        if (self.flags & 0x80 != 0 or (self.flags == 0) != (self.position == null)) return error.InvalidGenerationPublication;
        if (self.position) |position| position.requireNamespace(self.owner.namespace) catch return error.InvalidGenerationPublication;
        if (self.position) |position| if (!matchesAuthority(self.owner.authority, position)) return error.InvalidGenerationPublication;
        for (self.receipts, 0..) |digest, i| {
            if (!nonzero(&digest) and nonzero(&self.attestations[i])) return error.InvalidGenerationPublication;
            const action: Action = @fromBackingInt(@intCast(i));
            // Cancellation synthesizes fenced/reserved progress: the cleanup
            // receipt proves both, even if individual ACKs were never received.
            const implicit = (action == .fenced or action == .reserved) and self.has(.canceled);
            if ((!implicit and self.has(action) != nonzero(&digest)) or (!self.has(action) and nonzero(&digest)))
                return error.InvalidGenerationPublication;
        }
        if ((self.has(.reserved) and !self.has(.fenced)) or (self.has(.validated) and !self.has(.reserved)) or
            (self.has(.installed) and (!self.has(.validated) or self.has(.canceled))) or
            (self.has(.released) and !self.has(.installed)) or (self.has(.canceled) and !self.has(.reserved)) or
            (self.has(.cancel_released) and !self.has(.canceled))) return error.InvalidGenerationPublication;
        if ((nonzero(&self.source_delivery) or nonzero(&self.target_inventory)) and (self.owner.authority != .native or !self.has(.reserved))) return error.InvalidGenerationPublication;
        if (self.source_completed and !nonzero(&self.source_delivery)) return error.InvalidGenerationPublication;
    }

    pub fn encode(self: OwnerState) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "AIPO");
        bytes[4] = 5;
        bytes[5] = self.flags;
        bytes[6] = @backingInt(self.owner.authority);
        bytes[7] = @intFromBool(self.source_completed);
        @memcpy(bytes[8..24], &self.owner.plan_id);
        @memcpy(bytes[24..56], &self.owner.plan_digest);
        std.mem.writeInt(u16, bytes[56..58], self.owner.ordinal, .little);
        std.mem.writeInt(u64, bytes[64..72], self.owner.group_id, .little);
        std.mem.writeInt(u64, bytes[72..80], self.owner.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[80..88], self.owner.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[88..96], self.owner.namespace.range_id, .little);
        @memcpy(bytes[96..128], &self.owner.fence_digest);
        if (self.position) |position| @memcpy(bytes[128..161], &try position.encode());
        for (self.receipts, 0..) |digest, i| @memcpy(bytes[168 + i * 32 ..][0..32], &digest);
        for (self.attestations, 0..) |digest, i| @memcpy(bytes[392 + i * 32 ..][0..32], &digest);
        @memcpy(bytes[616..648], &self.source_delivery);
        @memcpy(bytes[648..680], &self.target_inventory);
        @memcpy(bytes[680..712], &checksum(bytes[0..680]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !OwnerState {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "AIPO") or bytes[4] != 5 or
            bytes[7] > 1 or !std.mem.allEqual(u8, bytes[58..64], 0) or
            !std.mem.allEqual(u8, bytes[161..168], 0) or
            !std.mem.eql(u8, &checksum(bytes[0..680]), bytes[680..712])) return error.InvalidGenerationPublication;
        var result: OwnerState = .{
            .owner = .{
                .plan_id = bytes[8..24].*,
                .plan_digest = bytes[24..56].*,
                .ordinal = std.mem.readInt(u16, bytes[56..58], .little),
                .group_id = std.mem.readInt(u64, bytes[64..72], .little),
                .namespace = .{
                    .table_id = std.mem.readInt(u64, bytes[72..80], .little),
                    .shard_id = std.mem.readInt(u64, bytes[80..88], .little),
                    .range_id = std.mem.readInt(u64, bytes[88..96], .little),
                },
                .authority = std.enums.fromInt(Authority, bytes[6]) orelse return error.InvalidGenerationPublication,
                .fence_digest = bytes[96..128].*,
            },
            .flags = bytes[5],
            .source_delivery = bytes[616..648].*,
            .target_inventory = bytes[648..680].*,
            .source_completed = bytes[7] != 0,
            .position = if (std.mem.allEqual(u8, bytes[128..161], 0)) null else positions.Position.decode(bytes[128..161]) catch return error.InvalidGenerationPublication,
        };
        for (&result.receipts, 0..) |*digest, i| digest.* = bytes[168 + i * 32 ..][0..32].*;
        for (&result.attestations, 0..) |*digest, i| digest.* = bytes[392 + i * 32 ..][0..32].*;
        try result.validate();
        return result;
    }
};

pub const Update = struct {
    head: Head,
    owner: OwnerState,
    changed: bool,
};

fn matchesAuthority(authority: Authority, position: positions.Position) bool {
    return switch (authority) {
        .raft => position == .raft,
        .native => position == .native,
    };
}

/// Owned, fixed-size staging buffers. The metadata apply transaction must also
/// stage its outbox/receipt before commit, and abort on any staging error.
pub const PreparedAck = struct {
    expected_head: [Head.encoded_len]u8,
    expected_owner: [OwnerState.encoded_len]u8,
    next_head: [Head.encoded_len]u8,
    next_owner: [OwnerState.encoded_len]u8,
    changed: bool,

    pub fn prepare(head_bytes: []const u8, owner_bytes: []const u8, expected_revision: u64, receipt: Receipt) !PreparedAck {
        const head = try Head.decode(head_bytes);
        const owner = try OwnerState.decode(owner_bytes);
        const update = try acknowledge(head, owner, expected_revision, receipt);
        return .{
            .expected_head = head_bytes[0..Head.encoded_len].*,
            .expected_owner = owner_bytes[0..OwnerState.encoded_len].*,
            .next_head = try update.head.encode(),
            .next_owner = try update.owner.encode(),
            .changed = update.changed,
        };
    }

    /// Keys come from the admitted metadata job, never the requesting owner.
    /// Check BOTH exact predecessors before staging either update.
    pub fn stage(self: *const PreparedAck, txn: anytype, head_key: []const u8, owner_key: []const u8) !void {
        if (head_key.len == 0 or owner_key.len == 0 or std.mem.eql(u8, head_key, owner_key)) return error.InvalidGenerationPublication;
        const head = txn.get(head_key) catch |err| switch (err) {
            error.NotFound => return error.GenerationPublicationChanged,
            else => return err,
        };
        // Consume a borrowed result before the next get can reuse its buffer.
        // Both exact predecessors still pass before either write is staged.
        if (!std.mem.eql(u8, head, &self.expected_head)) return error.GenerationPublicationChanged;
        const owner = txn.get(owner_key) catch |err| switch (err) {
            error.NotFound => return error.GenerationPublicationChanged,
            else => return err,
        };
        if (!std.mem.eql(u8, owner, &self.expected_owner)) return error.GenerationPublicationChanged;
        if (self.changed) {
            try txn.put(head_key, &self.next_head);
            try txn.put(owner_key, &self.next_owner);
        }
    }
};

/// O(1), allocation-free ACK application over a head and one point-read owner.
/// The caller must CAS the exact durable revision and owner bytes, then stage
/// both outputs in one transaction. Never apply these outputs independently.
pub fn acknowledge(head: Head, owner: OwnerState, expected_revision: u64, receipt: Receipt) !Update {
    try head.checkRevision(expected_revision);
    try owner.validate();
    // The complete relation follows inductively from atomic head/owner CAS.
    // Reject locally provable corruption without scanning the other owners.
    for (head.counts, 0..) |count, i| {
        const present = owner.has(@fromBackingInt(@intCast(i)));
        if ((count == 0 and present) or (count == head.owner_count and !present)) return error.InvalidGenerationPublication;
    }
    if (!Owner.eql(owner.owner, receipt.owner) or !std.mem.eql(u8, &head.plan_id, &owner.owner.plan_id) or
        !equal(head.plan_digest, owner.owner.plan_digest) or owner.owner.ordinal >= head.owner_count or
        !equal(head.candidate_digest, receipt.candidate_digest) or !equal(head.reservation_digest, receipt.reservation_digest) or
        owner.owner.namespace.table_id != head.table_id or
        !nonzero(&receipt.evidence_digest)) return error.InvalidGenerationPublication;
    receipt.position.requireNamespace(owner.owner.namespace) catch return error.InvalidGenerationPublication;
    if (!matchesAuthority(owner.owner.authority, receipt.position)) return error.InvalidGenerationPublication;
    const digest = try receipt.digest();
    const action = receipt.action;
    const index = @backingInt(action);
    if (owner.has(action)) {
        if (!equal(owner.receipts[index], digest)) return error.GenerationPublicationChanged;
        return .{ .head = head, .owner = owner, .changed = false };
    }
    if (owner.position) |previous| {
        if (std.meta.activeTag(previous) != std.meta.activeTag(receipt.position)) return error.GenerationPublicationChanged;
        switch (previous) {
            .raft => |value| if (receipt.position.raft.index <= value.index or receipt.position.raft.term < value.term)
                return error.GenerationPublicationChanged,
            .native => |value| if (receipt.position.native.sequence <= value.sequence)
                return error.GenerationPublicationChanged,
        }
    }
    const allowed = switch (action) {
        .fenced => head.phase == .fencing,
        .reserved => head.phase == .reserving and owner.has(.fenced),
        .validated => head.phase == .building and owner.has(.reserved),
        .installed => head.phase == .installing and owner.has(.validated),
        .released => head.phase == .releasing and owner.has(.installed),
        .canceled => head.phase == .canceling and !owner.has(.installed),
        .cancel_released => head.phase == .cancel_releasing and owner.has(.canceled),
    };
    if (!allowed) return error.GenerationPublicationChanged;
    var next: Update = .{ .head = head, .owner = owner, .changed = true };
    if (action == .canceled) for ([_]Action{ .fenced, .reserved }) |implicit| {
        if (!next.owner.has(implicit)) {
            next.owner.flags |= bit(implicit);
            next.head.counts[@backingInt(implicit)] += 1;
        }
    };
    next.owner.flags |= bit(action);
    next.owner.receipts[index] = digest;
    next.owner.position = receipt.position;
    next.head.counts[index] += 1;
    if (next.head.counts[index] == next.head.owner_count) next.head.phase = switch (action) {
        .fenced => .reserving,
        .reserved => .building,
        .validated => .building, // A distinct metadata decision publishes.
        .installed => .releasing,
        .released => .published,
        .canceled => .cancel_releasing,
        .cancel_released => .canceled,
    };
    try next.head.bump();
    try next.head.validate();
    try next.owner.validate();
    return next;
}

fn testHead(count: u16) Head {
    return .{ .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .table_id = 9, .owner_count = count };
}
fn testOwner(ordinal: u16) OwnerState {
    return .{ .owner = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .ordinal = ordinal, .group_id = @as(u64, ordinal) + 100, .namespace = .{ .table_id = 9, .shard_id = @as(u64, ordinal) + 100, .range_id = @as(u64, ordinal) + 200 }, .authority = .raft, .fence_digest = @splat(5) } };
}
fn testReceipt(head: Head, owner: OwnerState, action: Action) Receipt {
    const next = if (owner.position) |position| switch (position) {
        .raft => position.raft.index + 1,
        .native => position.native.sequence + 1,
    } else 1;
    const position: positions.Position = if (owner.owner.authority == .native)
        .{ .native = .{ .namespace = owner.owner.namespace, .sequence = next } }
    else
        .{ .raft = .{ .term = if (owner.position) |previous| previous.raft.term else 1, .index = next } };
    return .{ .owner = owner.owner, .action = action, .position = position, .candidate_digest = head.candidate_digest, .reservation_digest = head.reservation_digest, .evidence_digest = @splat(6) };
}
fn testAck(head: *Head, owner: *OwnerState, action: Action) !void {
    const update = try acknowledge(head.*, owner.*, head.revision, testReceipt(head.*, owner.*, action));
    // Reopen exact durable bytes after every transition, as on restart.
    head.* = try Head.decode(&(try update.head.encode()));
    owner.* = try OwnerState.decode(&(try update.owner.encode()));
}

test "index publication waits for every owner before build publication and release" {
    var head = testHead(2);
    var owners = [_]OwnerState{ testOwner(0), testOwner(1) };
    try std.testing.expectError(error.GenerationPublicationChanged, head.decidePublish(head.revision));
    for ([_]Action{ .fenced, .reserved, .validated }) |action| {
        try testAck(&head, &owners[1], action);
        try std.testing.expectError(error.GenerationPublicationChanged, head.decidePublish(head.revision));
        try testAck(&head, &owners[0], action);
    }
    try std.testing.expectEqual(Phase.building, head.phase);
    head = try head.decidePublish(head.revision);
    try std.testing.expectError(error.GenerationPublicationChanged, head.cancel(head.revision, .requested));
    try testAck(&head, &owners[0], .installed);
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owners[0], head.revision, testReceipt(head, owners[0], .released)));
    try testAck(&head, &owners[1], .installed);
    try testAck(&head, &owners[1], .released);
    try std.testing.expectEqual(Phase.releasing, head.phase);
    try testAck(&head, &owners[0], .released);
    try std.testing.expectEqual(Phase.published, head.phase);
}

test "index publication completed sources count each owner once and preserve a stable final barrier" {
    var head = testHead(3);
    var owners = [_]OwnerState{ testOwner(0), testOwner(1), testOwner(2) };
    for (&owners) |*owner| owner.owner.authority = .native;
    for ([_]Action{ .fenced, .reserved }) |action| for (&owners) |*owner| try testAck(&head, owner, action);
    for (&owners, 0..) |*owner, index| {
        // Pure counter test: metadata, not this helper, verifies consumption.
        owner.source_delivery = @splat(7);
        head = try head.completeSource(owner.*);
        owner.source_completed = true;
        try std.testing.expectEqual(@as(u16, @intCast(index + 1)), head.completed_sources);
        try std.testing.expectEqual(@as(u64, if (index == 2) head.revision else 0), head.completion_revision);
        try std.testing.expectError(error.GenerationPublicationChanged, head.completeSource(owner.*));
        head = try Head.decode(&try head.encode());
        owner.* = try OwnerState.decode(&try owner.encode());
    }
    const barrier = head.completion_revision;
    try testAck(&head, &owners[0], .validated);
    try std.testing.expectEqual(barrier, head.completion_revision);
    var corrupt = head;
    corrupt.completed_sources = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, corrupt.validate());
}

test "index cancellation burns every owner including unvisited ones before any release" {
    var head = testHead(3);
    var owners = [_]OwnerState{ testOwner(0), testOwner(1), testOwner(2) };
    try testAck(&head, &owners[0], .fenced);
    head = try head.cancel(head.revision, .duplicate_key);
    for (0..owners.len) |i| {
        try testAck(&head, &owners[i], .canceled);
        if (i + 1 < owners.len) try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owners[i], head.revision, testReceipt(head, owners[i], .cancel_released)));
    }
    try std.testing.expectEqual(@as(u16, 3), head.counts[0]);
    try std.testing.expectEqual(@as(u16, 3), head.counts[1]);
    try std.testing.expectEqual(Phase.cancel_releasing, head.phase);
    for (&owners) |*owner| try testAck(&head, owner, .cancel_released);
    try std.testing.expectEqual(Phase.canceled, head.phase);
    try std.testing.expectEqual(Failure.duplicate_key, head.failure);
    try std.testing.expectError(error.GenerationPublicationChanged, head.decidePublish(head.revision));
}

test "index receipts reject stale revisions conflicting replay and owner or candidate substitution" {
    var head = testHead(1);
    var owner = testOwner(0);
    const receipt = testReceipt(head, owner, .fenced);
    const prior = head;
    try testAck(&head, &owner, .fenced);
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, prior.revision, receipt));
    const replay = try acknowledge(head, owner, head.revision, receipt);
    try std.testing.expect(!replay.changed);
    try std.testing.expectEqual(head.revision, replay.head.revision);
    var altered = receipt;
    altered.evidence_digest[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, head.revision, altered));
    altered = testReceipt(head, owner, .reserved);
    altered.owner.namespace.range_id += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, owner, head.revision, altered));
    altered = testReceipt(head, owner, .reserved);
    altered.reservation_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, owner, head.revision, altered));
    altered = testReceipt(head, owner, .reserved);
    altered.position = owner.position.?;
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, head.revision, altered));
}

test "index native and Raft owners publish and cancel without mixing receipt domains" {
    for ([_]bool{ false, true }) |cancel| {
        var head = testHead(2);
        var owners = [_]OwnerState{ testOwner(0), testOwner(1) };
        owners[0].owner.authority = .native;
        var native = testReceipt(head, owners[0], .fenced);
        native.position = .{ .native = .{ .namespace = owners[0].owner.namespace, .sequence = 1 } };
        const first = try acknowledge(head, owners[0], head.revision, native);
        head = try Head.decode(&try first.head.encode());
        owners[0] = try OwnerState.decode(&try first.owner.encode());
        try testAck(&head, &owners[1], .fenced);
        for ([_]Action{ .reserved, .validated }) |action| for (&owners) |*owner| try testAck(&head, owner, action);
        if (cancel) {
            head = try head.cancel(head.revision, .requested);
            for ([_]Action{ .canceled, .cancel_released }) |action| for (&owners) |*owner| try testAck(&head, owner, action);
            try std.testing.expectEqual(Phase.canceled, head.phase);
        } else {
            head = try head.decidePublish(head.revision);
            for ([_]Action{ .installed, .released }) |action| for (&owners) |*owner| try testAck(&head, owner, action);
            try std.testing.expectEqual(Phase.published, head.phase);
        }
        try std.testing.expect(owners[0].position.? == .native);
        try std.testing.expect(owners[1].position.? == .raft);
        try std.testing.expect(owners[0].position.?.native.namespace.eql(owners[0].owner.namespace));
    }
}

test "index receipts reject authority namespace and regressing Raft term substitutions" {
    inline for (.{ false, true }) |native| {
        var head = testHead(1);
        var owner = testOwner(0);
        owner.owner.authority = if (native) .native else .raft;
        var first = testReceipt(head, owner, .fenced);
        first.position = if (native) .{ .native = .{ .namespace = owner.owner.namespace, .sequence = 1 } } else .{ .raft = .{ .term = 2, .index = 1 } };
        const accepted = try acknowledge(head, owner, head.revision, first);
        head = accepted.head;
        owner = accepted.owner;
        var next = testReceipt(head, owner, .reserved);
        next.position = if (native) .{ .raft = .{ .term = 2, .index = 2 } } else .{ .native = .{ .namespace = owner.owner.namespace, .sequence = 2 } };
        try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, owner, head.revision, next));
        next = testReceipt(head, owner, .reserved);
        if (native) {
            next.position.native.namespace.shard_id += 1;
            try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, owner, head.revision, next));
            next = testReceipt(head, owner, .reserved);
            next.position.native.sequence = owner.position.?.native.sequence;
        } else {
            next.position.raft.term = 1;
            try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, head.revision, next));
            next = testReceipt(head, owner, .reserved);
            next.position.raft.term += 1;
            next.position.raft.index = owner.position.?.raft.index;
        }
        try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, head.revision, next));
        next = testReceipt(head, owner, .reserved);
        next.owner.namespace.shard_id += 1;
        try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, owner, head.revision, next));
        try testAck(&head, &owner, .reserved);
        const replay = try acknowledge(head, owner, head.revision, first);
        try std.testing.expect(!replay.changed);
        try std.testing.expectEqualDeep(owner.position, replay.owner.position);
    }
}

test "index owner codec rejects legacy layout and checksummed position padding" {
    const owner = testOwner(0);
    const bytes = try owner.encode();
    var malformed = bytes;
    malformed[4] = 1;
    @memcpy(malformed[680..712], &checksum(malformed[0..680]));
    try std.testing.expectError(error.InvalidGenerationPublication, OwnerState.decode(&malformed));
    malformed = bytes;
    malformed[161] = 1;
    @memcpy(malformed[680..712], &checksum(malformed[0..680]));
    try std.testing.expectError(error.InvalidGenerationPublication, OwnerState.decode(&malformed));
    malformed = bytes;
    const position: positions.Position = .{ .native = .{ .namespace = owner.owner.namespace, .sequence = 1 } };
    @memcpy(malformed[128..161], &try position.encode());
    @memcpy(malformed[680..712], &checksum(malformed[0..680]));
    try std.testing.expectError(error.InvalidGenerationPublication, OwnerState.decode(&malformed));
}

test "index cancellation recovers every prepublication cut without releasing an unburned owner" {
    const actions = [_]Action{ .fenced, .reserved, .validated };
    for (0..10) |cut| {
        var head = testHead(3);
        var owners = [_]OwnerState{ testOwner(0), testOwner(1), testOwner(2) };
        for (0..cut) |step| try testAck(&head, &owners[step % 3], actions[step / 3]);
        head = try head.cancel(head.revision, .requested);
        // Cleanup/release order differs from preparation, as with restarted
        // supervisors and owners returning receipts out of order.
        for (0..3) |step| {
            const i = 2 - step;
            try testAck(&head, &owners[i], .canceled);
            try std.testing.expectError(error.GenerationPublicationChanged, head.decidePublish(head.revision));
            if (step < 2) try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owners[i], head.revision, testReceipt(head, owners[i], .cancel_released)));
        }
        for (0..3) |step| try testAck(&head, &owners[2 - step], .cancel_released);
        try std.testing.expectEqual(Phase.canceled, head.phase);
        try std.testing.expectEqual(@as(u16, 3), head.counts[1]);
        try std.testing.expectEqual(@as(u16, 0), head.counts[3]);
        try std.testing.expectEqual(@as(u16, 0), head.counts[4]);
        try std.testing.expectEqualSlices(u8, &testHead(3).reservation_digest, &head.reservation_digest);
        try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owners[0], head.revision, testReceipt(head, owners[0], .installed)));
    }
}

test "index ACK rejects inconsistent head and owner progress even with individually valid records" {
    var head = testHead(2);
    var owner = testOwner(0);
    try testAck(&head, &owner, .fenced);
    const receipt = testReceipt(head, owner, .reserved);
    var inconsistent = head;
    inconsistent.counts[0] = 0;
    try inconsistent.validate();
    try owner.validate();
    try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(inconsistent, owner, inconsistent.revision, receipt));
    var other = testOwner(1);
    try testAck(&head, &other, .fenced);
    const absent = testOwner(1);
    try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(head, absent, head.revision, testReceipt(head, absent, .fenced)));
}

test "index publication and cancellation decisions are mutually exclusive at the same revision" {
    var ready = testHead(1);
    var owner = testOwner(0);
    for ([_]Action{ .fenced, .reserved, .validated }) |action| try testAck(&ready, &owner, action);
    const published = try ready.decidePublish(ready.revision);
    const canceled = try ready.cancel(ready.revision, .requested);
    try std.testing.expectError(error.GenerationPublicationChanged, published.cancel(ready.revision, .requested));
    try std.testing.expectError(error.GenerationPublicationChanged, canceled.decidePublish(ready.revision));
    try std.testing.expectError(error.GenerationPublicationChanged, canceled.decidePublish(canceled.revision));
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(canceled, owner, canceled.revision, testReceipt(canceled, owner, .installed)));
}

test "index progress codecs reject every byte corruption and invalid checksummed state" {
    const head = testHead(1);
    const owner = testOwner(0);
    const head_bytes = try head.encode();
    const owner_bytes = try owner.encode();
    for (0..head_bytes.len) |i| {
        var damaged = head_bytes;
        damaged[i] ^= 1;
        try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(&damaged));
    }
    for (0..owner_bytes.len) |i| {
        var damaged = owner_bytes;
        damaged[i] ^= 1;
        try std.testing.expectError(error.InvalidGenerationPublication, OwnerState.decode(&damaged));
    }
    var impossible = head_bytes;
    impossible[5] = @backingInt(Phase.published);
    @memcpy(impossible[176..208], &checksum(impossible[0..176]));
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(&impossible));
}

test "index publication reader requirements survive ACKs and reject partial checksummed pairs" {
    var head = testHead(1);
    head.reader_requirements = .{ .semantic_version = 30, .encoding_version = 17 };
    var owner = testOwner(0);
    try testAck(&head, &owner, .fenced);
    try std.testing.expectEqualDeep(@as(?@import("../../common/schema_readers.zig").ReaderRequirements, .{ .semantic_version = 30, .encoding_version = 17 }), head.reader_requirements);
    const canonical = try head.encode();
    try std.testing.expectEqualDeep(head, try Head.decode(&canonical));
    for ([_][2]u32{ .{ 0, 17 }, .{ 30, 0 }, .{ 32, 17 }, .{ 30, 32 } }) |pair| {
        var invalid = canonical;
        std.mem.writeInt(u32, invalid[152..156], pair[0], .little);
        std.mem.writeInt(u32, invalid[156..160], pair[1], .little);
        @memcpy(invalid[176..208], &checksum(invalid[0..176]));
        try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(&invalid));
    }
    var old_format = canonical;
    old_format[4] = 1;
    @memcpy(old_format[176..208], &checksum(old_format[0..176]));
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(&old_format));
}

test "index progress stays fixed size at maximum owner count and fails closed on revision exhaustion" {
    var head = testHead(max_owners);
    // Acknowledge one point-read owner at a time; no plan-sized allocation.
    for (0..max_owners) |i| {
        var owner = testOwner(@intCast(i));
        try testAck(&head, &owner, .fenced);
    }
    try std.testing.expectEqual(Phase.reserving, head.phase);
    try std.testing.expectEqual(@as(usize, 208), (try head.encode()).len);
    head.revision = std.math.maxInt(u64);
    try std.testing.expectError(error.GenerationPublicationChanged, head.cancel(head.revision, .requested));
    head = testHead(1);
    var owner = testOwner(0);
    try testAck(&head, &owner, .fenced);
    head.revision = std.math.maxInt(u64);
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owner, head.revision, testReceipt(head, owner, .reserved)));
}

test "index ACK staging compares both durable predecessors before writing either" {
    const MockTxn = struct {
        head: [Head.encoded_len]u8,
        owner: [OwnerState.encoded_len]u8,
        writes: usize = 0,
        read_error: bool = false,
        scratch: [OwnerState.encoded_len]u8 = undefined,
        fn get(self: *@This(), key: []const u8) error{ NotFound, StorageReadFailed }![]const u8 {
            if (self.read_error) return error.StorageReadFailed;
            const bytes: []const u8 = if (std.mem.eql(u8, key, "head")) &self.head else if (std.mem.eql(u8, key, "owner")) &self.owner else return error.NotFound;
            // A legal reader may reuse its borrowed-result buffer on get.
            @memcpy(self.scratch[0..bytes.len], bytes);
            return self.scratch[0..bytes.len];
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            self.writes += 1;
            if (std.mem.eql(u8, key, "head")) @memcpy(&self.head, value) else if (std.mem.eql(u8, key, "owner")) @memcpy(&self.owner, value) else return error.NotFound;
        }
    };
    const head = testHead(1);
    const owner = testOwner(0);
    const receipt = testReceipt(head, owner, .fenced);
    const prepared = try PreparedAck.prepare(&(try head.encode()), &(try owner.encode()), head.revision, receipt);
    var txn: MockTxn = .{ .head = prepared.expected_head, .owner = prepared.expected_owner };
    txn.read_error = true;
    try std.testing.expectError(error.StorageReadFailed, prepared.stage(&txn, "head", "owner"));
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    txn.read_error = false;
    txn.owner[80] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, prepared.stage(&txn, "head", "owner"));
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    txn.owner = prepared.expected_owner;
    try std.testing.expectError(error.GenerationPublicationChanged, prepared.stage(&txn, "head", "missing"));
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    try prepared.stage(&txn, "head", "owner");
    try std.testing.expectEqual(@as(usize, 2), txn.writes);
    try std.testing.expectEqualSlices(u8, &prepared.next_head, &txn.head);
    try std.testing.expectEqualSlices(u8, &prepared.next_owner, &txn.owner);
    try std.testing.expectError(error.GenerationPublicationChanged, prepared.stage(&txn, "head", "owner"));
    const installed = try Head.decode(&txn.head);
    const replay = try PreparedAck.prepare(&txn.head, &txn.owner, installed.revision, receipt);
    try replay.stage(&txn, "head", "owner");
    try std.testing.expectEqual(@as(usize, 2), txn.writes);
}
