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

//! Native source attempt fencing. Reducers return replacement values; callers
//! must persist the header, record, transaction and effects in one NativeKV commit.
const std = @import("std");
const completion = @import("transaction_completion.zig");
pub const Digest = [32]u8;
pub const prefix = "\x00\x00__metadata__:source_scope:";
pub const max_domains = 1024;
fn nonzero(v: anytype) bool {
    return !std.mem.allEqual(u8, &v, 0);
}
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.meta.eql(a, b);
}
pub const Basis = struct {
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    namespace: [24]u8,
    root_generation: u64,
    group_id: u64,
    pub fn fromOwner(owner: completion.RoutedOwner) !Basis {
        try owner.validate();
        return .{ .metadata_group_id = owner.metadata_group_id, .metadata_incarnation = owner.metadata_incarnation, .namespace = owner.namespace, .root_generation = owner.root_generation, .group_id = owner.group_id };
    }
    pub fn validate(self: Basis) !void {
        if (self.metadata_group_id == 0 or self.group_id == 0 or !nonzero(self.namespace) or std.mem.readInt(u64, self.namespace[0..8], .big) == 0 or std.mem.allEqual(u8, &self.metadata_incarnation, '0')) return error.InvalidSourceAdmission;
        for (self.metadata_incarnation) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return error.InvalidSourceAdmission;
    }
};
pub const Spec = struct {
    actor_table_id: u64,
    source_id: Digest,
    generation: u64,
    binding: Digest,
    checkpoint_owner: Basis,
    pub fn validate(self: Spec) !void {
        try self.checkpoint_owner.validate();
        if (self.actor_table_id == 0 or self.generation == 0 or !nonzero(self.source_id) or !nonzero(self.binding)) return error.InvalidSourceAdmission;
    }
    pub fn domain(self: Spec) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-scope-domain:v1");
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, self.actor_table_id, .little);
        hash.update(&id);
        hash.update(&self.source_id);
        return hash.finalResult();
    }
};
pub const Reference = struct {
    domain: Digest,
    binding: Digest,
    generation: u64,
    token: u64,
    sequence: u64,
    pub const encoded_len = 88;
    pub fn validate(self: Reference) !void {
        if (!nonzero(self.domain) or !nonzero(self.binding) or self.generation == 0 or self.token == 0 or self.sequence == 0) return error.InvalidSourceAdmission;
    }
    pub fn encode(self: Reference) ![encoded_len]u8 {
        try self.validate();
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..32], &self.domain);
        @memcpy(out[32..64], &self.binding);
        std.mem.writeInt(u64, out[64..72], self.generation, .little);
        std.mem.writeInt(u64, out[72..80], self.token, .little);
        std.mem.writeInt(u64, out[80..88], self.sequence, .little);
        return out;
    }
    pub fn decode(bytes: []const u8) !Reference {
        if (bytes.len != encoded_len) return error.SourceAdmissionCorrupt;
        const value: Reference = .{ .domain = bytes[0..32].*, .binding = bytes[32..64].*, .generation = std.mem.readInt(u64, bytes[64..72], .little), .token = std.mem.readInt(u64, bytes[72..80], .little), .sequence = std.mem.readInt(u64, bytes[80..88], .little) };
        value.validate() catch return error.SourceAdmissionCorrupt;
        return value;
    }
};
pub const MetadataCut = struct {
    group_id: u64,
    incarnation: [32]u8,
    index: u64,
    fn require(self: MetadataCut, root: Basis) !void {
        if (self.index == 0 or self.group_id != root.metadata_group_id or !same(self.incarnation, root.metadata_incarnation)) return error.SourceCheckpointConflict;
    }
};
pub const Cut = struct { term: u64, index: u64 };
pub const Slot = struct { reference: Reference, transaction_id: [16]u8, source_purpose: Digest, admission_digest: Digest };
pub const Phase = enum { active, closing, closed };
pub const Record = struct {
    spec: Spec,
    bound_metadata_index: u64,
    phase: Phase = .active,
    closed_through_token: u64 = 0,
    last_committed_sequence: u64 = 0,
    last_committed_token: u64 = 0,
    slot: ?Slot = null,
    close_metadata_index: u64 = 0,
    closed_at: ?Cut = null,
    fn matches(self: Record, ref: Reference) !void {
        try ref.validate();
        if (!same(ref.domain, self.spec.domain()) or !same(ref.binding, self.spec.binding) or ref.generation != self.spec.generation) return error.SourceCheckpointConflict;
    }
    pub fn enter(self: *Record, slot: Slot, coordinator: Basis, local: Basis) !void {
        try self.matches(slot.reference);
        if (!nonzero(slot.transaction_id) or !nonzero(slot.source_purpose) or !nonzero(slot.admission_digest)) return error.InvalidSourceAdmission;
        if (self.phase != .active or !same(coordinator, self.spec.checkpoint_owner)) return error.SourceCheckpointConflict;
        if (self.slot) |existing| {
            if (same(existing, slot)) return;
            return error.SourceTransactionIdentityConflict;
        }
        if (slot.reference.token <= self.closed_through_token or slot.reference.sequence <= self.last_committed_sequence) return error.SourceCheckpointConflict;
        if (same(local, coordinator) and (self.last_committed_sequence == std.math.maxInt(u64) or slot.reference.sequence != self.last_committed_sequence + 1)) return error.SourceCheckpointConflict;
        self.slot = slot;
    }
    /// Only an independently observed original coordinator ABORT may call this.
    /// No local BEGIN/outcome is fabricated. One cumulative floor bounds storage.
    pub fn retireUnenlisted(self: *Record, slot: Slot, coordinator: Basis, local: Basis, applied: Cut) !bool {
        try self.matches(slot.reference);
        if (!nonzero(slot.transaction_id) or !nonzero(slot.source_purpose) or !nonzero(slot.admission_digest) or applied.term == 0 or applied.index == 0 or !same(coordinator, self.spec.checkpoint_owner)) return error.InvalidSourceAdmission;
        if (slot.reference.token <= self.closed_through_token) return false;
        // A missing original coordinator record cannot establish its own abort.
        if (same(local, coordinator)) return error.SourceOutcomeUnresolved;
        if (self.slot) |active| {
            if (active.reference.token <= slot.reference.token) return error.SourceTransactionIdentityConflict;
        }
        if (slot.reference.sequence <= self.last_committed_sequence) return error.SourceCheckpointConflict;
        self.closed_through_token = slot.reference.token;
        if (self.phase == .closing and self.slot == null) {
            self.phase = .closed;
            self.closed_at = applied;
        }
        return true;
    }
    pub fn prepareAllowed(self: Record, slot: Slot) !void {
        if (self.phase != .active or self.slot == null or !same(self.slot.?, slot)) return error.SourceCheckpointConflict;
    }
    pub fn close(self: *Record, root: Basis, cut: MetadataCut, applied: Cut) !void {
        try cut.require(root);
        if (cut.index <= self.bound_metadata_index or applied.term == 0 or applied.index == 0) return error.SourceCheckpointConflict;
        if (self.phase == .closed) return;
        if (self.phase == .active) self.close_metadata_index = cut.index;
        self.phase = .closing;
        if (self.slot == null) {
            self.phase = .closed;
            self.closed_at = applied;
        }
    }
    /// Positive follower decisions must already be authenticated by the retained
    /// completion coordinator. This reducer never manufactures that evidence.
    pub fn finish(self: *Record, slot: Slot, committed: bool, is_coordinator: bool, applied: Cut) !void {
        try self.matches(slot.reference);
        if (self.slot == null or !same(self.slot.?, slot) or applied.term == 0 or applied.index == 0) return error.SourceTransactionIdentityConflict;
        if (committed and is_coordinator and self.phase != .active) return error.SourceCheckpointConflict;
        self.closed_through_token = slot.reference.token;
        if (committed) {
            self.last_committed_sequence = slot.reference.sequence;
            self.last_committed_token = slot.reference.token;
        }
        self.slot = null;
        if (self.phase == .closing) {
            self.phase = .closed;
            self.closed_at = applied;
        }
    }
};
pub const Header = struct {
    version: u8 = 1,
    root: Basis,
    metadata_floor: u64 = 0,
    domains: u16 = 0,
    strict: bool = false,
    pub fn requireLegacyAllowed(self: Header) !void {
        if (self.strict) return error.SourceCheckpointConflict;
    }
    /// `cut` must come from a qualified metadata read of the active actor.
    /// Numeric cuts supplied by an untrusted caller are not authorization.
    pub fn bind(self: *Header, old: ?Record, spec: Spec, cut: MetadataCut, checkpoint_sequence: u64) !Record {
        try self.root.validate();
        try spec.validate();
        try cut.require(self.root);
        if (cut.index < self.metadata_floor) return error.SourceCheckpointConflict;
        if (old) |record| {
            if (!self.strict or self.domains == 0) return error.SourceAdmissionCorrupt;
            if (!same(record.spec.domain(), spec.domain())) return error.SourceAdmissionCorrupt;
            if (same(record.spec, spec) and record.phase == .active) return record;
            if (spec.generation <= record.spec.generation or record.phase != .closed or record.slot != null or cut.index <= record.close_metadata_index) return error.SourceCheckpointConflict;
        } else if (self.domains >= max_domains) return error.SourceAdmissionBackpressure;
        if (old == null) self.domains += 1;
        self.strict = true;
        self.metadata_floor = @max(self.metadata_floor, cut.index);
        return .{ .spec = spec, .bound_metadata_index = cut.index, .last_committed_sequence = checkpoint_sequence };
    }
    /// A qualified retired-actor read must prove retirement through this generation.
    /// Persist header advancement and record deletion atomically.
    pub fn drop(self: *Header, record: Record, cut: MetadataCut, retired_through_generation: u64, expected_closed_at: Cut) !void {
        try cut.require(self.root);
        if (record.phase != .closed or record.slot != null or record.closed_at == null or !same(record.closed_at.?, expected_closed_at) or retired_through_generation < record.spec.generation or cut.index <= record.close_metadata_index or cut.index <= record.bound_metadata_index or self.domains == 0) return error.SourceCheckpointConflict;
        self.metadata_floor = @max(self.metadata_floor, cut.index);
        self.domains -= 1;
    }
};

/// All buffers use the enclosing attempt arena. Any write failure requires
/// aborting the enclosing NativeKV transaction, including earlier writes.
pub const Store = struct {
    pub const max_record_bytes = 8192;
    const header_key = prefix ++ "header";
    fn recordKey(domain: Digest) [prefix.len + 32]u8 {
        var key: [prefix.len + 32]u8 = undefined;
        @memcpy(key[0..prefix.len], prefix);
        @memcpy(key[prefix.len..], &domain);
        return key;
    }
    fn checksum(comptime T: type, payload: []const u8) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(if (T == Header) "antfly:source-scope-header:v1" else "antfly:source-scope-record:v1");
        hash.update(payload);
        return hash.finalResult();
    }
    pub fn encode(a: std.mem.Allocator, value: anytype) ![]u8 {
        const payload = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(payload);
        if (payload.len + 36 > max_record_bytes) return error.SourceAdmissionCorrupt;
        const result = try a.alloc(u8, payload.len + 36);
        @memcpy(result[0..4], "SSC1");
        @memcpy(result[4..36], &checksum(@TypeOf(value), payload));
        @memcpy(result[36..], payload);
        return result;
    }
    pub fn decode(comptime T: type, a: std.mem.Allocator, bytes: []const u8) !T {
        if (bytes.len <= 36 or bytes.len > max_record_bytes or !std.mem.eql(u8, bytes[0..4], "SSC1") or !std.mem.eql(u8, bytes[4..36], &checksum(T, bytes[36..]))) return error.SourceAdmissionCorrupt;
        var parsed = std.json.parseFromSlice(T, a, bytes[36..], .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
        defer parsed.deinit();
        const value = parsed.value;
        if (T == Header) {
            value.root.validate() catch return error.SourceAdmissionCorrupt;
            if (value.version != 1 or value.domains > max_domains or (!value.strict and (value.domains != 0 or value.metadata_floor != 0))) return error.SourceAdmissionCorrupt;
        } else if (T == Record) {
            value.spec.validate() catch return error.SourceAdmissionCorrupt;
            if (value.bound_metadata_index == 0 or value.last_committed_token > value.closed_through_token or (value.last_committed_sequence == 0 and value.last_committed_token != 0)) return error.SourceAdmissionCorrupt;
            if ((value.phase == .closed) != (value.closed_at != null) or (value.phase == .active) != (value.close_metadata_index == 0) or (value.phase != .active and value.close_metadata_index <= value.bound_metadata_index)) return error.SourceAdmissionCorrupt;
            if (value.closed_at) |cut| if (cut.term == 0 or cut.index == 0 or value.slot != null) return error.SourceAdmissionCorrupt;
            if (value.slot) |slot| {
                value.matches(slot.reference) catch return error.SourceAdmissionCorrupt;
                if (value.phase == .closed or slot.reference.token <= value.closed_through_token or slot.reference.sequence <= value.last_committed_sequence or !nonzero(slot.transaction_id) or !nonzero(slot.source_purpose) or !nonzero(slot.admission_digest)) return error.SourceAdmissionCorrupt;
            }
        } else @compileError("unsupported source scope record type");
        return value;
    }
    /// Reject fresh legacy source admission once this root enables scoped work.
    /// A retained old transaction is handled by its own immutable record instead.
    pub fn requireLegacyAllowed(a: std.mem.Allocator, txn: anytype) !void {
        const bytes = txn.get(header_key) catch |err| switch (@as(anyerror, err)) {
            error.NotFound => return,
            else => return err,
        };
        try (try decode(Header, a, bytes)).requireLegacyAllowed();
    }
    pub fn readHeader(a: std.mem.Allocator, txn: anytype, root: Basis) !Header {
        try root.validate();
        const bytes = txn.get(header_key) catch |err| switch (@as(anyerror, err)) {
            error.NotFound => return .{ .root = root },
            else => return err,
        };
        const value = try decode(Header, a, bytes);
        if (!same(value.root, root)) return error.SourceCheckpointConflict;
        return value;
    }
    pub fn readRecord(a: std.mem.Allocator, txn: anytype, domain: Digest) !?Record {
        const bytes = txn.get(&recordKey(domain)) catch |err| switch (@as(anyerror, err)) {
            error.NotFound => return null,
            else => return err,
        };
        const value = try decode(Record, a, bytes);
        if (!same(value.spec.domain(), domain)) return error.SourceAdmissionCorrupt;
        return value;
    }
    pub fn writeHeader(a: std.mem.Allocator, txn: anytype, header: Header) !void {
        const bytes = try encode(a, header);
        try txn.put(header_key, bytes);
    }
    pub fn writeRecord(a: std.mem.Allocator, txn: anytype, record: Record) !void {
        const bytes = try encode(a, record);
        try txn.put(&recordKey(record.spec.domain()), bytes);
    }
    pub fn bind(a: std.mem.Allocator, txn: anytype, root: Basis, spec: Spec, cut: MetadataCut) !Record {
        var header = try readHeader(a, txn, root);
        const old = try readRecord(a, txn, spec.domain());
        var sequence: u64 = 0;
        if (same(root, spec.checkpoint_owner)) {
            const checkpoint = @import("source_checkpoint.zig");
            const bytes = txn.get(&checkpoint.key(spec.binding)) catch |err| switch (@as(anyerror, err)) {
                error.NotFound => null,
                else => return err,
            };
            if (bytes) |retained| {
                const head = try checkpoint.Head.decode(retained);
                if (!same(head.binding, spec.binding)) return error.SourceAdmissionCorrupt;
                sequence = head.sequence;
            }
        }
        const record = try header.bind(old, spec, cut, sequence);
        try writeRecord(a, txn, record);
        try writeHeader(a, txn, header);
        return record;
    }
    pub fn requireRecord(a: std.mem.Allocator, txn: anytype, root: Basis, domain: Digest) !Record {
        const header = try readHeader(a, txn, root);
        const record = (try readRecord(a, txn, domain)) orelse return error.SourceCheckpointConflict;
        if (!header.strict or header.domains == 0) return error.SourceAdmissionCorrupt;
        return record;
    }
    pub fn enter(a: std.mem.Allocator, txn: anytype, root: Basis, slot: Slot, coordinator: Basis) !void {
        var record = try requireRecord(a, txn, root, slot.reference.domain);
        try record.enter(slot, coordinator, root);
        try writeRecord(a, txn, record);
    }
    pub fn retireUnenlisted(a: std.mem.Allocator, txn: anytype, root: Basis, slot: Slot, coordinator: Basis, applied: Cut) !struct { changed: bool, floor: u64 } {
        var record = try requireRecord(a, txn, root, slot.reference.domain);
        const changed = try record.retireUnenlisted(slot, coordinator, root, applied);
        if (changed) try writeRecord(a, txn, record);
        return .{ .changed = changed, .floor = record.closed_through_token };
    }
    pub fn prepareAllowed(a: std.mem.Allocator, txn: anytype, root: Basis, slot: Slot) !void {
        const record = try requireRecord(a, txn, root, slot.reference.domain);
        try record.prepareAllowed(slot);
    }
    /// A qualified closing/retired actor cut also fences an original bind that
    /// has not arrived yet. No per-domain tombstone or budget charge is needed:
    /// active-actor bind captures necessarily precede the closing metadata cut.
    pub fn closeAbsent(a: std.mem.Allocator, txn: anytype, root: Basis, spec: Spec, cut: MetadataCut) !u64 {
        try spec.validate();
        try cut.require(root);
        if (spec.checkpoint_owner.metadata_group_id != root.metadata_group_id or !same(spec.checkpoint_owner.metadata_incarnation, root.metadata_incarnation)) return error.SourceCheckpointConflict;
        var header = try readHeader(a, txn, root);
        if (try readRecord(a, txn, spec.domain()) != null) return error.SourcePendingWork;
        header.strict = true;
        header.metadata_floor = @max(header.metadata_floor, cut.index);
        try writeHeader(a, txn, header);
        return header.metadata_floor;
    }
    pub fn close(a: std.mem.Allocator, txn: anytype, root: Basis, domain: Digest, expected_spec: Spec, cut: MetadataCut, applied: Cut) !Record {
        var record = try requireRecord(a, txn, root, domain);
        if (!same(record.spec, expected_spec)) return error.SourceCheckpointConflict;
        try record.close(root, cut, applied);
        try writeRecord(a, txn, record);
        return record;
    }
    pub fn finish(a: std.mem.Allocator, txn: anytype, root: Basis, slot: Slot, committed: bool, applied: Cut) !void {
        var record = try requireRecord(a, txn, root, slot.reference.domain);
        try record.finish(slot, committed, same(root, record.spec.checkpoint_owner), applied);
        try writeRecord(a, txn, record);
    }
    pub fn drop(a: std.mem.Allocator, txn: anytype, root: Basis, domain: Digest, cut: MetadataCut, retired_through_generation: u64, closed_at: Cut) !void {
        var header = try readHeader(a, txn, root);
        try cut.require(root);
        const record = (try readRecord(a, txn, domain)) orelse {
            if (header.strict and header.metadata_floor >= cut.index) return;
            return error.SourceCheckpointConflict;
        };
        try header.drop(record, cut, retired_through_generation, closed_at);
        try txn.delete(&recordKey(domain));
        try writeHeader(a, txn, header);
    }
};

test "scope persistent envelopes reject corruption and cross-type substitution" {
    const a = std.testing.allocator;
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const header: Header = .{ .root = root };
    const bytes = try Store.encode(a, header);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(header, try Store.decode(Header, a, bytes));
    try std.testing.expectError(error.SourceAdmissionCorrupt, Store.decode(Record, a, bytes));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, Store.decode(Header, a, bytes));
}

test "scope close drains original attempt, forbids late admission, and bounds retirement" {
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    var header: Header = .{ .root = root };
    const active: MetadataCut = .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 };
    var record = try header.bind(null, spec, active, 0);
    const slot: Slot = .{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 7, .sequence = 1 }, .transaction_id = @splat(8), .source_purpose = @splat(9), .admission_digest = @splat(10) };
    const encoded = try slot.reference.encode();
    try std.testing.expectEqualDeep(slot.reference, try Reference.decode(&encoded));
    try record.enter(slot, root, root);
    try record.enter(slot, root, root);
    try record.prepareAllowed(slot);
    var closing = active;
    closing.index = 11;
    try record.close(root, closing, .{ .term = 1, .index = 20 });
    try std.testing.expectEqual(Phase.closing, record.phase);
    try std.testing.expectError(error.SourceCheckpointConflict, record.prepareAllowed(slot));
    try std.testing.expectError(error.SourceCheckpointConflict, record.finish(slot, true, true, .{ .term = 1, .index = 21 }));
    try record.finish(slot, false, true, .{ .term = 1, .index = 22 });
    try std.testing.expectEqual(Phase.closed, record.phase);
    var retired = active;
    retired.index = 12;
    try header.drop(record, retired, 1, .{ .term = 1, .index = 22 });
    try std.testing.expectEqual(@as(u16, 0), header.domains);
    try std.testing.expectError(error.SourceCheckpointConflict, header.bind(null, spec, active, 0));
    try std.testing.expectError(error.SourceCheckpointConflict, header.requireLegacyAllowed());
}

test "scope committed token floors survive retries and checkpoint owner cannot skip sequences" {
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    var record: Record = .{ .spec = spec, .bound_metadata_index = 10 };
    var slot: Slot = .{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 7, .sequence = 2 }, .transaction_id = @splat(8), .source_purpose = @splat(9), .admission_digest = @splat(10) };
    try std.testing.expectError(error.SourceCheckpointConflict, record.enter(slot, root, root));
    slot.reference.sequence = 1;
    try record.enter(slot, root, root);
    try record.finish(slot, true, true, .{ .term = 1, .index = 20 });
    try std.testing.expectError(error.SourceCheckpointConflict, record.enter(slot, root, root));
    slot.reference.token += 1;
    slot.reference.sequence += 1;
    try record.enter(slot, root, root);
}

test "retired scope replacement preserves domain budget and rejects reopened generations" {
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    var spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    var header: Header = .{ .root = root };
    var cut: MetadataCut = .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 };
    var record = try header.bind(null, spec, cut, 0);
    cut.index = 11;
    try record.close(root, cut, .{ .term = 1, .index = 20 });
    try std.testing.expectError(error.SourceCheckpointConflict, header.bind(record, spec, cut, 0));
    spec.generation = 2;
    spec.binding = @splat(6);
    try std.testing.expectError(error.SourceCheckpointConflict, header.bind(record, spec, cut, 0));
    cut.index = 12;
    const next = try header.bind(record, spec, cut, 0);
    try std.testing.expectEqual(@as(u16, 1), header.domains);
    try std.testing.expectEqual(Phase.active, next.phase);
    const bytes = try Store.encode(std.testing.allocator, next);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualDeep(next, try Store.decode(Record, std.testing.allocator, bytes));
    header.domains = max_domains;
    spec.source_id = @splat(7);
    try std.testing.expectError(error.SourceAdmissionBackpressure, header.bind(null, spec, cut, 0));
}

test "closing follower drains retained positive decision and keeps first close cut" {
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    var record: Record = .{ .spec = spec, .bound_metadata_index = 10 };
    const slot: Slot = .{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 7, .sequence = 4 }, .transaction_id = @splat(8), .source_purpose = @splat(9), .admission_digest = @splat(10) };
    var follower = root;
    follower.group_id = 8;
    try record.enter(slot, root, follower);
    const cut: MetadataCut = .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 11 };
    try record.close(root, cut, .{ .term = 1, .index = 20 });
    try record.finish(slot, true, false, .{ .term = 1, .index = 21 });
    try record.close(root, cut, .{ .term = 2, .index = 30 });
    try std.testing.expectEqualDeep(Cut{ .term = 1, .index = 21 }, record.closed_at.?);
    try std.testing.expectEqual(@as(u64, 4), record.last_committed_sequence);
}

const TestTxn = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    pub fn deinit(self: *@This()) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
            std.testing.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit(std.testing.allocator);
    }
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.map.get(key) orelse error.NotFound;
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const a = std.testing.allocator;
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const v = try a.dupe(u8, value);
        errdefer a.free(v);
        const slot = try self.map.getOrPut(a, k);
        if (slot.found_existing) {
            a.free(k);
            a.free(slot.value_ptr.*);
        }
        slot.value_ptr.* = v;
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        const entry = self.map.fetchRemove(key) orelse return error.NotFound;
        std.testing.allocator.free(entry.key);
        std.testing.allocator.free(entry.value);
    }
};

test "scope store persists drain and retirement floor across reloads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    var cut: MetadataCut = .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 };
    _ = try Store.bind(a, &txn, root, spec, cut);
    const slot: Slot = .{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 7, .sequence = 1 }, .transaction_id = @splat(8), .source_purpose = @splat(9), .admission_digest = @splat(10) };
    try Store.enter(a, &txn, root, slot, root);
    try Store.prepareAllowed(a, &txn, root, slot);
    cut.index = 11;
    _ = try Store.close(a, &txn, root, spec.domain(), spec, cut, .{ .term = 1, .index = 20 });
    try Store.finish(a, &txn, root, slot, false, .{ .term = 1, .index = 21 });
    cut.index = 12;
    try Store.drop(a, &txn, root, spec.domain(), cut, 1, .{ .term = 1, .index = 21 });
    try Store.drop(a, &txn, root, spec.domain(), cut, 1, .{ .term = 1, .index = 21 });
    try std.testing.expect((try Store.readRecord(a, &txn, spec.domain())) == null);
    const header = try Store.readHeader(a, &txn, root);
    try std.testing.expectEqual(@as(u16, 0), header.domains);
    try std.testing.expectEqual(@as(u64, 12), header.metadata_floor);
    try std.testing.expectError(error.SourceCheckpointConflict, Store.enter(a, &txn, root, slot, root));
    cut.index = 10;
    try std.testing.expectError(error.SourceCheckpointConflict, Store.bind(a, &txn, root, spec, cut));
}

test "scope absent closure fences queued old binding without allocating domain tombstones" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root: Basis = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .root_generation = 0, .group_id = 2 };
    const spec: Spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = root };
    const closing: MetadataCut = .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 11 };
    try std.testing.expectEqual(@as(u64, 11), try Store.closeAbsent(a, &txn, root, spec, closing));
    try std.testing.expectEqual(@as(usize, 1), txn.map.count());
    const header = try Store.readHeader(a, &txn, root);
    try std.testing.expect(header.strict);
    try std.testing.expectEqual(@as(u16, 0), header.domains);
    try std.testing.expect((try Store.readRecord(a, &txn, spec.domain())) == null);
    var old = closing;
    old.index = 10;
    try std.testing.expectError(error.SourceCheckpointConflict, Store.bind(a, &txn, root, spec, old));
    var another = spec;
    another.source_id = @splat(6);
    _ = try Store.bind(a, &txn, root, another, closing);
    try std.testing.expectError(error.SourcePendingWork, Store.closeAbsent(a, &txn, root, another, closing));
    try std.testing.expectEqual(@as(u64, 11), try Store.closeAbsent(a, &txn, root, spec, old));
    try std.testing.expectEqual(@as(u16, 1), (try Store.readHeader(a, &txn, root)).domains);
}
