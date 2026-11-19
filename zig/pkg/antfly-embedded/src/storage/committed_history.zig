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

//! Root-local committed history. This is part of the primary transaction, not
//! a second WAL or a notification queue. Global feeds coordinate these roots;
//! a root sequence alone never claims cross-shard transaction completion.
const std = @import("std");
const keys = @import("internal_keys.zig");
const authority = @import("source_authority.zig");
pub const frame = @import("committed_frame.zig");
const intent_value = @import("transaction_intent_value.zig");
const A = std.mem.Allocator;
pub const state_key = "\x00\x00__committed_history__:state";
pub const consumer_authority_key = "\x00\x00__committed_history__:consumer_authority";
const record_prefix = "\x00\x00__committed_history__:record:";
const chunk_prefix = "\x00\x00__committed_history__:chunk:";
const member_prefix = "\x00\x00__committed_history__:member:";
const reservation_prefix = "\x00\x00__committed_history__:reservation:";
const intents_prefix = "\x00\x00__txn_intents__:";
const completion = frame.completion;
const completion_credit_prefix = "\x00\x00__committed_history__:completion_credit:";
pub const completion_certificate_prefix = "\x00\x00__committed_history__:completion_certificate:";
const completion_retained_prefix = "\x00\x00__committed_history__:completion_retained:";
const publication_wire = @import("history_publication_wire.zig");
pub const CompletionPublicationPosition = publication_wire.CompletionPublicationPosition;
pub const CompletionPublication = publication_wire.CompletionPublication;
pub const CompletionPublicationQuery = publication_wire.CompletionPublicationQuery;
pub const CompletionPublicationObservation = publication_wire.CompletionPublicationObservation;
pub const publication_read_max_bytes = publication_wire.publication_read_max_bytes;
pub const decodeCompletionPublicationQuery = publication_wire.decodeCompletionPublicationQuery;
pub const decodeCompletionPublicationObservation = publication_wire.decodeCompletionPublicationObservation;
pub const completion_publication_prefix = "\x00\x00__committed_history__:completion_publication:";
pub fn completionPublicationKey(index: u64) [completion_publication_prefix.len + 8]u8 {
    var key: [completion_publication_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..completion_publication_prefix.len], completion_publication_prefix);
    std.mem.writeInt(u64, key[completion_publication_prefix.len..], index, .big);
    return key;
}
pub const CompletionPublicationRecord = struct {
    transaction_id: [16]u8,
    position: CompletionPublicationPosition,
    certificate_digest: completion.Digest,
    pub const encoded_len = 96;
    pub fn encode(self: CompletionPublicationRecord) [encoded_len]u8 {
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..16], &self.transaction_id);
        std.mem.writeInt(u64, raw[16..24], self.position.term, .little);
        std.mem.writeInt(u64, raw[24..32], self.position.index, .little);
        @memcpy(raw[32..64], &self.certificate_digest);
        @memcpy(raw[64..96], &frame.digest(raw[0..64]));
        return raw;
    }
    pub fn decode(raw: []const u8) !CompletionPublicationRecord {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[64..96], &frame.digest(raw[0..64]))) return error.HistoryCorrupt;
        const record: CompletionPublicationRecord = .{ .transaction_id = raw[0..16].*, .position = .{ .term = std.mem.readInt(u64, raw[16..24], .little), .index = std.mem.readInt(u64, raw[24..32], .little) }, .certificate_digest = raw[32..64].* };
        if (record.position.term == 0 or record.position.index == 0 or std.mem.allEqual(u8, &record.transaction_id, 0) or std.mem.allEqual(u8, &record.certificate_digest, 0)) return error.HistoryCorrupt;
        return record;
    }
};
pub fn completionCertificateKey(id: [16]u8) [completion_certificate_prefix.len + 16]u8 {
    return completion_certificate_prefix.* ++ id;
}
fn completionRetainedKey(id: [16]u8) [completion_retained_prefix.len + 16]u8 {
    return completion_retained_prefix.* ++ id;
}

pub const default_limit: u64 = 256 * 1024 * 1024;
pub const max_consumers = 32;
const transaction_overhead: u64 = 2 * frame.max_schema_bytes + 1024 + 256 * 32;
pub const Consumer = struct {
    id: [16]u8 = @splat(0),
    epoch: u64 = 0,
    start: u64 = 0,
    acknowledged: u64 = 0,
    expires_at_seconds: u64 = 0,
    policy_revision: u64 = 0,
};
pub const policy_key = "\x00\x00__committed_history__:policy";
pub const Policy = struct {
    namespace: frame.Namespace,
    authority: [32]u8,
    previous_revision: u64,
    /// Root retention configuration generation; not a lease authorization revision.
    revision: u64,
    limit: u64,
    pub const encoded_size = 116;
    pub fn validate(self: Policy) !void {
        if (std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.authority, 0) or self.revision <= self.previous_revision or self.limit < transaction_overhead + frame.chunk_bytes) return error.InvalidHistoryAdmission;
    }
    pub fn encode(self: Policy) ![encoded_size]u8 {
        try self.validate();
        var raw: [encoded_size]u8 = @splat(0);
        @memcpy(raw[0..4], "HCP1");
        @memcpy(raw[4..28], &self.namespace);
        @memcpy(raw[28..60], &self.authority);
        std.mem.writeInt(u64, raw[60..68], self.previous_revision, .little);
        std.mem.writeInt(u64, raw[68..76], self.revision, .little);
        std.mem.writeInt(u64, raw[76..84], self.limit, .little);
        @memcpy(raw[84..116], &frame.digest(raw[0..84]));
        return raw;
    }
    pub fn decode(raw: []const u8) !Policy {
        if (raw.len != encoded_size or !std.mem.eql(u8, raw[0..4], "HCP1") or !std.mem.eql(u8, raw[84..116], &frame.digest(raw[0..84]))) return error.HistoryCorrupt;
        const value: Policy = .{ .namespace = raw[4..28].*, .authority = raw[28..60].*, .previous_revision = std.mem.readInt(u64, raw[60..68], .little), .revision = std.mem.readInt(u64, raw[68..76], .little), .limit = std.mem.readInt(u64, raw[76..84], .little) };
        value.validate() catch return error.HistoryCorrupt;
        return value;
    }
};
pub fn loadPolicy(txn: anytype) !?Policy {
    return if (try optional(txn, policy_key)) |raw| try Policy.decode(raw) else null;
}
/// Ordered root capture activation does not allocate a consumer or move any
/// consumer ACK. The fixed policy record itself remains charged after GC.
pub fn configurePolicy(txn: anytype, value: Policy) !void {
    const encoded = try value.encode();
    const previous = try loadPolicy(txn);
    var state = (try load(txn)) orelse State{ .namespace = value.namespace, .limit = value.limit };
    try requireNamespace(txn, state, value.namespace);
    if (previous) |current| {
        if (!std.mem.eql(u8, &current.namespace, &value.namespace) or !std.mem.eql(u8, &current.authority, &value.authority)) return error.HistoryResynchronizationRequired;
        if (current.revision == value.revision) {
            if (!std.mem.eql(u8, &(try current.encode()), &encoded)) return error.HistoryCheckpointConflict;
            return;
        }
        if (current.revision != value.previous_revision) return error.HistoryCheckpointConflict;
    } else {
        if (value.previous_revision != 0) return error.HistoryCheckpointConflict;
        // Existing prepared legacy effects cannot straddle activation. Legacy
        // BEGIN/PREPARE admission is rejected after this ordered policy exists.
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(intents_prefix)) |entry| if (std.mem.startsWith(u8, entry.key, intents_prefix)) return error.HistoryPreparedTransactionsPresent;
        state.retained_bytes = std.math.add(u64, state.retained_bytes, policy_key.len + Policy.encoded_size) catch return error.HistoryFull;
    }
    if (state.retained_bytes > value.limit or state.reserved_bytes > value.limit - state.retained_bytes) return error.HistoryFull;
    state.limit = value.limit;
    try save(txn, state);
    try txn.put(policy_key, &encoded);
}

pub const State = struct {
    namespace: frame.Namespace,
    latest: u64 = 0,
    reclaimed: u64 = 0,
    retained_bytes: u64 = 0,
    reserved_bytes: u64 = 0,
    limit: u64 = default_limit,
    epoch: u64 = 0,
    consumers: [max_consumers]Consumer = @splat(.{}),
    pub fn active(self: State) bool {
        for (self.consumers) |consumer| if (consumer.epoch != 0) return true;
        return false;
    }
    pub fn floor(self: State) u64 {
        var result = self.latest;
        for (self.consumers) |consumer| if (consumer.epoch != 0) {
            result = @min(result, consumer.acknowledged);
        };
        return result;
    }
    pub fn findConsumer(self: *State, id: [16]u8, epoch: u64) !*Consumer {
        for (&self.consumers) |*entry| if (entry.epoch != 0 and std.mem.eql(u8, &entry.id, &id)) {
            if (entry.epoch != epoch) return error.HistoryResynchronizationRequired;
            return entry;
        };
        return error.HistoryResynchronizationRequired;
    }
};
const state_size = 4 + 24 + 6 * 8 + max_consumers * 64 + 32;
fn encode(state: State) [state_size]u8 {
    var out: [state_size]u8 = undefined;
    @memcpy(out[0..4], "CHS1");
    @memcpy(out[4..28], &state.namespace);
    var offset: usize = 28;
    for ([_]u64{ state.latest, state.reclaimed, state.retained_bytes, state.reserved_bytes, state.limit, state.epoch }) |value| {
        std.mem.writeInt(u64, out[offset..][0..8], value, .little);
        offset += 8;
    }
    for (state.consumers) |consumer| {
        @memcpy(out[offset..][0..16], &consumer.id);
        offset += 16;
        for ([_]u64{ consumer.epoch, consumer.start, consumer.acknowledged, consumer.expires_at_seconds, consumer.policy_revision }) |value| {
            std.mem.writeInt(u64, out[offset..][0..8], value, .little);
            offset += 8;
        }
        @memset(out[offset..][0..8], 0);
        offset += 8;
    }
    @memcpy(out[offset..], &frame.digest(out[0..offset]));
    return out;
}
fn decode(raw: []const u8) !State {
    if (raw.len != state_size or !std.mem.eql(u8, raw[0..4], "CHS1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &frame.digest(raw[0 .. raw.len - 32]))) return error.HistoryCorrupt;
    var state: State = .{ .namespace = raw[4..28].* };
    var offset: usize = 28;
    inline for (.{ "latest", "reclaimed", "retained_bytes", "reserved_bytes", "limit", "epoch" }) |field| {
        @field(state, field) = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
    }
    for (&state.consumers, 0..) |*consumer, index| {
        consumer.id = raw[offset..][0..16].*;
        offset += 16;
        inline for (.{ "epoch", "start", "acknowledged", "expires_at_seconds", "policy_revision" }) |field| {
            @field(consumer, field) = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
        }
        if (!std.mem.allEqual(u8, raw[offset..][0..8], 0)) return error.HistoryCorrupt;
        offset += 8;
        if (consumer.epoch == 0) {
            if (!std.meta.eql(consumer.*, Consumer{})) return error.HistoryCorrupt;
        } else {
            if (std.mem.allEqual(u8, &consumer.id, 0) or consumer.start > consumer.acknowledged or consumer.acknowledged > state.latest or consumer.acknowledged < state.reclaimed or consumer.epoch > state.epoch) return error.HistoryCorrupt;
            for (state.consumers[0..index]) |prior| if (prior.epoch != 0 and std.mem.eql(u8, &prior.id, &consumer.id)) return error.HistoryCorrupt;
        }
    }
    if (std.mem.allEqual(u8, &state.namespace, 0) or state.limit < transaction_overhead + frame.chunk_bytes or state.reclaimed > state.latest or state.retained_bytes > state.limit or state.reserved_bytes > state.limit - state.retained_bytes) return error.HistoryCorrupt;
    return state;
}
pub fn load(txn: anytype) !?State {
    const raw = txn.get(state_key) catch |err| if (err == error.NotFound) return null else return err;
    return try decode(raw);
}
fn save(txn: anytype, state: State) !void {
    try txn.put(state_key, &encode(state));
}
fn requireNamespace(txn: anytype, state: State, namespace: frame.Namespace) !void {
    if (!std.mem.eql(u8, &state.namespace, &namespace)) return error.HistoryResynchronizationRequired;
    const raw = txn.get(&keys.identity_namespace_key) catch |err| if (err == error.NotFound) return error.HistoryResynchronizationRequired else return err;
    if (!std.mem.eql(u8, raw, &namespace)) return error.HistoryResynchronizationRequired;
    const owner = try authority.load(txn) orelse return error.HistoryAuthorityRequired;
    if (!std.mem.eql(u8, &owner.namespace, &namespace)) return error.HistoryResynchronizationRequired;
}
/// Read while holding the same ordered write transaction that installs the
/// bootstrap pin. A partially resolved coordinated commit can already have
/// changed rows while its completion credit still owns the publication debt.
pub fn requireBootstrapCut(txn: anytype, namespace: frame.Namespace) !u64 {
    const state = (try load(txn)) orelse State{ .namespace = namespace };
    try requireNamespace(txn, state, namespace);
    if (state.reserved_bytes != 0) return error.HistoryPreparedTransactionsPresent;
    var cursor = try txn.openCursor();
    defer cursor.close();
    if (try cursor.seekAtOrAfter(intents_prefix)) |entry| {
        if (std.mem.startsWith(u8, entry.key, intents_prefix)) return error.HistoryPreparedTransactionsPresent;
    }
    return state.latest;
}

pub fn admit(txn: anytype, namespace: frame.Namespace, id: [16]u8, limit: u64, expires: u64, policy_revision: u64) !Consumer {
    if (std.mem.allEqual(u8, &id, 0) or limit < transaction_overhead + frame.chunk_bytes) return error.InvalidHistoryAdmission;
    var state = (try load(txn)) orelse State{ .namespace = namespace, .limit = limit };
    try requireNamespace(txn, state, namespace);
    if (state.limit != limit) return error.InvalidHistoryAdmission;
    for (state.consumers) |consumer| if (consumer.epoch != 0 and std.mem.eql(u8, &consumer.id, &id)) {
        if (consumer.expires_at_seconds != expires or consumer.policy_revision != policy_revision) return error.HistoryConsumerConflict;
        return consumer;
    };
    if (!state.active() and try loadPolicy(txn) == null) {
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(intents_prefix)) |entry| if (std.mem.startsWith(u8, entry.key, intents_prefix)) return error.HistoryPreparedTransactionsPresent;
        if (try cursor.seekAtOrAfter(completion.admission_prefix)) |entry| if (std.mem.startsWith(u8, entry.key, completion.admission_prefix)) return error.HistoryPreparedTransactionsPresent;
    }
    for (&state.consumers) |*consumer| if (consumer.epoch == 0) {
        state.epoch = std.math.add(u64, state.epoch, 1) catch return error.HistoryCorrupt;
        consumer.* = .{ .id = id, .epoch = state.epoch, .start = state.latest, .acknowledged = state.latest, .expires_at_seconds = expires, .policy_revision = policy_revision };
        try save(txn, state);
        return consumer.*;
    };
    return error.HistoryConsumerLimit;
}
pub fn acknowledge(txn: anytype, namespace: frame.Namespace, id: [16]u8, epoch: u64, previous: u64, next: u64) !void {
    var state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, namespace);
    const consumer = try state.findConsumer(id, epoch);
    if (consumer.acknowledged != previous or next < previous or next > state.latest) return error.HistoryCheckpointConflict;
    consumer.acknowledged = next;
    try save(txn, state);
}
/// Private metadata-authorized lease extension. Expiry and release are separate
/// decisions; an expired lease remains a retention obligation until fenced close.
pub fn renew(txn: anytype, namespace: frame.Namespace, id: [16]u8, epoch: u64, previous: u64, next: u64) !void {
    if (next <= previous) return error.InvalidHistoryAdmission;
    var state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, namespace);
    const consumer = try state.findConsumer(id, epoch);
    if (consumer.expires_at_seconds != previous) return error.HistoryConsumerConflict;
    consumer.expires_at_seconds = next;
    try save(txn, state);
}
pub fn release(txn: anytype, namespace: frame.Namespace, id: [16]u8, epoch: u64) !void {
    var state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, namespace);
    (try state.findConsumer(id, epoch)).* = .{};
    try save(txn, state);
}
pub fn recordKey(sequence: u64) [record_prefix.len + 8]u8 {
    var out: [record_prefix.len + 8]u8 = undefined;
    @memcpy(out[0..record_prefix.len], record_prefix);
    std.mem.writeInt(u64, out[record_prefix.len..], sequence, .big);
    return out;
}
pub fn chunkKey(sequence: u64, ordinal: u32) [chunk_prefix.len + 12]u8 {
    var out: [chunk_prefix.len + 12]u8 = undefined;
    @memcpy(out[0..chunk_prefix.len], chunk_prefix);
    std.mem.writeInt(u64, out[chunk_prefix.len..][0..8], sequence, .big);
    std.mem.writeInt(u32, out[chunk_prefix.len + 8 ..][0..4], ordinal, .big);
    return out;
}
/// Reclaims at most one bounded transaction per call. Consumer cursors never
/// authorize reclaiming an unacknowledged transaction or a prepared credit.
pub fn reclaim(txn: anytype, namespace: frame.Namespace) !bool {
    var state = (try load(txn)) orelse return true;
    try requireNamespace(txn, state, namespace);
    if (state.reclaimed == state.floor()) return true;
    const sequence = state.reclaimed + 1;
    const descriptor = try frame.Descriptor.decode((try optional(txn, &recordKey(sequence))) orelse return error.HistoryCorrupt, namespace, sequence);
    const stored = descriptor.storedBytes();
    const count = descriptor.count;
    for (0..count) |i| try txn.delete(&chunkKey(sequence, @intCast(i)));
    try txn.delete(&recordKey(sequence));
    state.retained_bytes = std.math.sub(u64, state.retained_bytes, stored) catch return error.HistoryCorrupt;
    state.reclaimed = sequence;
    try save(txn, state);
    return state.reclaimed == state.floor();
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
}
fn memberKey(key: []const u8) [member_prefix.len + 32]u8 {
    return member_prefix.* ++ frame.digest(key);
}
fn reservationKey(id: [16]u8) [reservation_prefix.len + 16]u8 {
    return reservation_prefix.* ++ id;
}
fn completionCreditKey(id: [16]u8) [completion_credit_prefix.len + 16]u8 {
    return completion_credit_prefix.* ++ id;
}
const CompletionCredit = struct {
    bytes: u64 = 0,
    digest: completion.Digest = @splat(0),
    fn load(txn: anytype, id: [16]u8) !CompletionCredit {
        const raw = (try optional(txn, &completionCreditKey(id))) orelse return .{};
        if (raw.len != 76 or !std.mem.eql(u8, raw[0..4], "CCV1") or !std.mem.eql(u8, raw[44..76], &frame.digest(raw[0..44]))) return error.HistoryCorrupt;
        const result: CompletionCredit = .{ .bytes = std.mem.readInt(u64, raw[4..12], .little), .digest = raw[12..44].* };
        if (result.bytes == 0 or result.bytes > 2 * completion.max_bytes + 128) return error.HistoryCorrupt;
        return result;
    }
    fn encode(self: CompletionCredit) [76]u8 {
        var raw: [76]u8 = undefined;
        @memcpy(raw[0..4], "CCV1");
        std.mem.writeInt(u64, raw[4..12], self.bytes, .little);
        @memcpy(raw[12..44], &self.digest);
        @memcpy(raw[44..76], &frame.digest(raw[0..44]));
        return raw;
    }
};
/// Exact charge for retained evidence. The primary coordinator record is not
/// the retention authority; this ledger survives its eventual cleanup.
const RetainedCompletion = struct {
    bytes: u64,
    plan_bytes: u64,
    receipt_bytes: u64,
    certificate_bytes: u64,
    position: CompletionPublicationPosition,
    admission_digest: completion.Digest,
    certificate_digest: completion.Digest,
    const encoded_len = 148;
    fn encode(self: RetainedCompletion) [encoded_len]u8 {
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..4], "CRT1");
        for ([_]u64{ self.bytes, self.plan_bytes, self.receipt_bytes, self.certificate_bytes }, 0..) |value, i| std.mem.writeInt(u64, raw[4 + i * 8 ..][0..8], value, .little);
        std.mem.writeInt(u64, raw[36..44], self.position.term, .little);
        std.mem.writeInt(u64, raw[44..52], self.position.index, .little);
        @memcpy(raw[52..84], &self.admission_digest);
        @memcpy(raw[84..116], &self.certificate_digest);
        @memcpy(raw[116..148], &frame.digest(raw[0..116]));
        return raw;
    }
    fn decode(raw: []const u8) !RetainedCompletion {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "CRT1") or !std.mem.eql(u8, raw[116..148], &frame.digest(raw[0..116]))) return error.HistoryCorrupt;
        return .{ .bytes = std.mem.readInt(u64, raw[4..12], .little), .plan_bytes = std.mem.readInt(u64, raw[12..20], .little), .receipt_bytes = std.mem.readInt(u64, raw[20..28], .little), .certificate_bytes = std.mem.readInt(u64, raw[28..36], .little), .position = .{ .term = std.mem.readInt(u64, raw[36..44], .little), .index = std.mem.readInt(u64, raw[44..52], .little) }, .admission_digest = raw[52..84].*, .certificate_digest = raw[84..116].* };
    }
};

/// Trusted ordered owner publication only. The caller must authenticate every
/// remote receipt through qualified original-owner reads before this reducer.
/// The caller holds native apply admission and commits this transaction together
/// with its ordered marker; failures require aborting the entire transaction.
pub fn publishCompletion(a: A, txn: anytype, physical_namespace: frame.Namespace, physical_root_generation: u64, owner: completion.RoutedOwner, raw: []const u8, position: CompletionPublicationPosition, allow_create: bool) !CompletionPublication {
    return completionPublication(a, txn, physical_namespace, physical_root_generation, owner, raw, position, allow_create, true);
}

/// A read snapshot can verify the same immutable evidence without acquiring a
/// write transaction, advancing a marker or recreating a missing publication.
pub fn readCompletionPublication(a: A, txn: anytype, physical_namespace: frame.Namespace, physical_root_generation: u64, query: CompletionPublicationQuery) !CompletionPublicationObservation {
    try query.validate();
    if (!std.mem.eql(u8, &physical_namespace, &query.owner.namespace) or query.owner.root_generation != physical_root_generation) return error.HistoryResynchronizationRequired;
    const state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, physical_namespace);
    _ = try authority.require(txn, .raft, physical_namespace);
    const marker = (try optional(txn, &keys.ordered_document_applied_entry_key)) orelse return error.StorageReadTemporarilyUnavailable;
    if (marker.len != 16) return error.HistoryCorrupt;
    const cut: CompletionPublicationPosition = .{ .term = std.mem.readInt(u64, marker[0..8], .little), .index = std.mem.readInt(u64, marker[8..16], .little) };
    if (cut.term == 0 or cut.index == 0) return error.HistoryCorrupt;
    if (cut.index < query.minimum_applied_index) return error.StorageReadTemporarilyUnavailable;
    const raw = (try optional(txn, &completionCertificateKey(query.transaction_id))) orelse {
        if (try optional(txn, &completionRetainedKey(query.transaction_id)) != null) return error.HistoryCorrupt;
        return CompletionPublicationObservation.init(query, cut, null);
    };
    var certificate = try completion.parse(a, raw);
    defer certificate.deinit();
    const manifest = certificate.value.certificate.manifest;
    if (!std.mem.eql(u8, &manifest.transaction_id, &query.transaction_id) or !std.mem.eql(u8, &manifest.digest(), &query.manifest_digest) or manifest.commit_version != query.commit_version) return error.HistoryCorrupt;
    return CompletionPublicationObservation.init(query, cut, try completionPublication(a, txn, physical_namespace, physical_root_generation, query.owner, raw, cut, false, false));
}

pub const FrameCompletion = struct {
    certificate_digest: frame.Digest,
    commit_version: u64,
};

pub const CompletionProjection = struct {
    roots: []const frame.Namespace,
    effects: []?completion.Effects,
};

/// Qualify the exact frame against its retained all-participant certificate
/// under the same read transaction that supplied the authenticated descriptor.
/// The caller decides which effects belong to the subscription.
pub fn readFrameCompletion(a: A, txn: anytype, root_generation: u64, header: frame.Header, descriptor: frame.Descriptor) !?FrameCompletion {
    return readProjectedFrameCompletion(a, txn, root_generation, header, descriptor, null);
}

/// Return selected effects only after proving that the local frame is bound
/// to the retained certificate in this read transaction.
pub fn readProjectedFrameCompletion(a: A, txn: anytype, root_generation: u64, header: frame.Header, descriptor: frame.Descriptor, projection: ?CompletionProjection) !?FrameCompletion {
    if (header.transaction_scope != .coordinated or !std.mem.eql(u8, &header.namespace, &descriptor.namespace) or
        header.sequence != descriptor.sequence) return error.InvalidHistoryAdmission;
    const raw = (try optional(txn, &completionCertificateKey(header.transaction_id))) orelse {
        if (try optional(txn, &completionRetainedKey(header.transaction_id)) != null) return error.HistoryCorrupt;
        return null;
    };
    var parsed = try completion.parse(a, raw);
    defer parsed.deinit();
    const certificate = parsed.value.certificate;
    const manifest = certificate.manifest;
    if (!std.mem.eql(u8, &manifest.transaction_id, &header.transaction_id)) return error.HistoryCorrupt;
    const owner = try manifest.admission().recoveryOwner(header.namespace);
    const acknowledgement = try readCompletionPublication(a, txn, header.namespace, root_generation, .{
        .version = 1,
        .owner = owner,
        .transaction_id = header.transaction_id,
        .manifest_digest = manifest.digest(),
        .commit_version = manifest.commit_version,
        .minimum_applied_index = 1,
    });
    const publication = acknowledgement.publication orelse return error.HistoryCorrupt;
    for (certificate.resolutions) |resolution| {
        if (!std.mem.eql(u8, &resolution.namespace, &header.namespace)) continue;
        const effects = switch (resolution.outcome) {
            .effects => |effects| effects,
            .no_effect => return error.HistoryCorrupt,
        };
        if (effects.sequence != header.sequence or
            !std.mem.eql(u8, &effects.frame_digest, &frame.digest(descriptor.raw)) or
            !std.mem.eql(u8, &resolution.source_position, &header.source_position)) return error.HistoryCorrupt;
        if (projection) |selected| try certificate.projectEffects(selected.roots, selected.effects);
        return .{ .certificate_digest = publication.digest, .commit_version = manifest.commit_version };
    }
    return error.HistoryCorrupt;
}

fn completionPublication(a: A, txn: anytype, physical_namespace: frame.Namespace, physical_root_generation: u64, owner: completion.RoutedOwner, raw: []const u8, position: CompletionPublicationPosition, allow_create: bool, comptime writable: bool) !CompletionPublication {
    try owner.validate();
    if (position.term == 0 or position.index == 0) return error.InvalidHistoryAdmission;
    if (!std.mem.eql(u8, &physical_namespace, &owner.namespace) or owner.root_generation != physical_root_generation) return error.HistoryResynchronizationRequired;
    var state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, physical_namespace);
    _ = try authority.require(txn, .raft, physical_namespace);
    var parsed = try completion.parse(a, raw);
    defer parsed.deinit();
    const certificate = parsed.value.certificate;
    const manifest = certificate.manifest;
    try manifest.admission().requireRoutedOwner(owner);
    const id = manifest.transaction_id;
    const plan_key = completion.admission_prefix.* ++ id;
    const plan_raw = (try optional(txn, &plan_key)) orelse return error.HistoryResynchronizationRequired;
    var plan = try completion.parseAdmission(a, plan_raw);
    defer plan.deinit();
    if (!std.mem.eql(u8, &plan.value.digest, &manifest.admission().digest())) return error.DecisionConflict;
    const receipt_key = completion.resolutionKey(id);
    const receipt_raw = (try optional(txn, &receipt_key)) orelse return error.SourceOutcomeUnresolved;
    var local = try completion.parseResolution(a, manifest, receipt_raw);
    defer local.deinit();
    const marker = (try optional(txn, &keys.ordered_document_applied_entry_key)) orelse return error.HistoryAuthorityRequired;
    if (marker.len != 16) return error.HistoryCorrupt;
    const wire = @import("history_resolution_wire.zig");
    const request: wire.Request = .{ .authority = .raft, .namespace = physical_namespace, .transaction_id = id, .owner_fence = owner.digest(), .manifest_digest = manifest.digest(), .commit_version = manifest.commit_version, .minimum_applied_index = 1 };
    _ = try wire.Reply.init(request, .{ .term = std.mem.readInt(u64, marker[0..8], .little), .index = std.mem.readInt(u64, marker[8..16], .little) }, local.value.resolution);
    var found = false;
    for (certificate.resolutions) |receipt| if (std.mem.eql(u8, &receipt.namespace, &physical_namespace)) {
        if (!std.mem.eql(u8, &receipt.digest(), &local.value.digest)) return error.DecisionConflict;
        found = true;
        break;
    };
    if (!found) return error.InvalidParticipant;
    const canonical = try completion.encode(a, certificate);
    defer a.free(canonical);
    const certificate_key = completionCertificateKey(id);
    const retained_key = completionRetainedKey(id);
    const stored_certificate = try optional(txn, &certificate_key);
    const stored_retained = try optional(txn, &retained_key);
    const credit = try CompletionCredit.load(txn, id);
    const publication_key = completionPublicationKey(position.index);
    const overhead = plan_key.len + receipt_key.len + certificate_key.len + retained_key.len + publication_key.len + CompletionPublicationRecord.encoded_len + RetainedCompletion.encoded_len + 128;
    const charge: u64 = std.math.add(u64, overhead, plan_raw.len + receipt_raw.len + canonical.len) catch return error.HistoryTooLarge;
    const retained: RetainedCompletion = .{ .bytes = charge, .plan_bytes = plan_raw.len, .receipt_bytes = receipt_raw.len, .certificate_bytes = canonical.len, .position = position, .admission_digest = plan.value.digest, .certificate_digest = parsed.value.digest };
    if (stored_certificate != null or stored_retained != null) {
        if (stored_certificate == null or stored_retained == null) return error.HistoryCorrupt;
        const previous = try RetainedCompletion.decode(stored_retained.?);
        if (!std.mem.eql(u8, stored_certificate.?, canonical)) return error.DecisionConflict;
        var expected = retained;
        expected.position = previous.position;
        if (!std.meta.eql(previous, expected) or credit.bytes != 0 or state.retained_bytes < charge) return error.HistoryCorrupt;
        const index_raw = (try optional(txn, &completionPublicationKey(previous.position.index))) orelse return error.HistoryCorrupt;
        const index_record = try CompletionPublicationRecord.decode(index_raw);
        if (!std.mem.eql(u8, &index_record.transaction_id, &id) or !std.mem.eql(u8, &index_record.certificate_digest, &previous.certificate_digest) or !std.meta.eql(index_record.position, previous.position) or previous.position.index > std.mem.readInt(u64, marker[8..16], .little) or previous.position.term > std.mem.readInt(u64, marker[0..8], .little)) return error.HistoryCorrupt;
        return .{ .digest = previous.certificate_digest, .retained_bytes = previous.bytes, .position = previous.position };
    }
    if (comptime writable) {
        if (!allow_create) return error.SourceOutcomeUnresolved;
        if (position.index <= std.mem.readInt(u64, marker[8..16], .little) or position.term < std.mem.readInt(u64, marker[0..8], .little)) return error.HistoryAuthorityRequired;
        if (try optional(txn, &publication_key) != null) return error.HistoryCorrupt;
        const reserved = try plan.value.admission.reservationBytes(plan_raw.len);
        if (credit.bytes != reserved or !std.mem.eql(u8, &credit.digest, &plan.value.digest) or charge > reserved or state.reserved_bytes < reserved) return error.HistoryCorrupt;
        state.reserved_bytes -= reserved;
        state.retained_bytes = std.math.add(u64, state.retained_bytes, charge) catch return error.HistoryFull;
        if (state.retained_bytes > state.limit or state.reserved_bytes > state.limit - state.retained_bytes) return error.HistoryFull;
        const index_record: CompletionPublicationRecord = .{ .transaction_id = id, .position = position, .certificate_digest = retained.certificate_digest };
        try txn.put(&publication_key, &index_record.encode());
        try txn.put(&certificate_key, canonical);
        try txn.put(&retained_key, &retained.encode());
        try txn.delete(&completionCreditKey(id));
        try save(txn, state);
        return .{ .digest = retained.certificate_digest, .retained_bytes = retained.bytes, .position = position };
    } else return error.SourceOutcomeUnresolved;
}

/// Called under the native prepare/apply fence, before publishing the vote.
/// BEGIN's durable credit must still bind this exact immutable admission.
pub fn requireCompletionReservation(txn: anytype, namespace: frame.Namespace, admission: completion.Admission) !void {
    const state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, namespace);
    const plan_key = completion.admission_prefix.* ++ admission.transaction_id;
    const plan = (try optional(txn, &plan_key)) orelse return error.HistoryCorrupt;
    const expected = try admission.reservationBytes(plan.len);
    const credit = try CompletionCredit.load(txn, admission.transaction_id);
    if (credit.bytes != expected or state.reserved_bytes < expected or !std.mem.eql(u8, &credit.digest, &admission.digest())) return error.HistoryCorrupt;
}
const Reservation = struct {
    members: u64 = 0,
    bytes: u64 = 0,
    fn load(txn: anytype, id: [16]u8) !Reservation {
        const raw = (try optional(txn, &reservationKey(id))) orelse return .{};
        if (raw.len != 16) return error.HistoryCorrupt;
        const result: Reservation = .{ .members = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little) };
        if (result.members == 0 or result.members > frame.max_events / 2 or result.bytes > frame.max_bytes - transaction_overhead) return error.HistoryCorrupt;
        return result;
    }
    fn total(self: Reservation) !u64 {
        return if (self.members == 0) 0 else std.math.add(u64, self.bytes, transaction_overhead) catch error.HistoryFull;
    }
};

pub const Capture = struct {
    checked: bool = false,
    enabled: bool = false,
    control: bool = false,
    touched_primary: bool = false,
    staging: bool = false,
    poisoned: bool = false,
    namespace: frame.Namespace = @splat(0),
    initial_authority: ?authority.State = null,
    rows: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    row_bytes: usize = 0,
    schema_before: ?[]const u8 = null,
    history_commit_time_ns: i64 = 0,
    schema_checked: bool = false,
    raft_marker: ?[16]u8 = null,
    reservations: std.AutoHashMapUnmanaged([16]u8, Reservation) = .empty,
    reservation_previous: std.AutoHashMapUnmanaged([16]u8, Reservation) = .empty,
    members: std.StringHashMapUnmanaged(u64) = .empty,
    transaction_id: ?[16]u8 = null,
    resolution_request: ?completion.ResolutionRequest = null,
    completion_credits: std.AutoHashMapUnmanaged([16]u8, CompletionCreditChange) = .empty,

    const CompletionCreditChange = struct { previous: u64, next: u64, digest: completion.Digest };

    pub fn deinit(self: *Capture, a: A) void {
        self.completion_credits.deinit(a);
        var rows = self.rows.iterator();
        while (rows.next()) |entry| {
            a.free(entry.key_ptr.*);
            if (entry.value_ptr.*) |bytes| a.free(bytes);
        }
        self.rows.deinit(a);
        if (self.schema_before) |bytes| a.free(bytes);
        self.reservations.deinit(a);
        self.reservation_previous.deinit(a);
        var members = self.members.keyIterator();
        while (members.next()) |key| a.free(key.*);
        self.members.deinit(a);
    }
    pub fn touch(self: *Capture, a: A, txn: anytype, key: []const u8, value: ?[]const u8, cache: ?*std.atomic.Value(u8)) !void {
        if (self.staging) return;
        errdefer self.poisoned = true;
        if (std.mem.eql(u8, key, state_key) or std.mem.eql(u8, key, consumer_authority_key) or std.mem.eql(u8, key, policy_key)) {
            if (self.touched_primary or self.rows.count() != 0 or self.reservations.count() != 0 or self.completion_credits.count() != 0) return error.HistoryMixedControl;
            self.control = true;
            if (cache) |known| known.store(2, .release);
            return;
        }
        if (std.mem.eql(u8, key, &keys.ordered_document_applied_entry_key)) {
            self.raft_marker = if (value) |raw| if (raw.len == 16) raw[0..16].* else null else null;
            return;
        }
        const intent = std.mem.startsWith(u8, key, intents_prefix);
        const completion_admission = std.mem.startsWith(u8, key, completion.admission_prefix);
        const resolution_request = std.mem.startsWith(u8, key, completion.resolution_request_prefix);
        const primary = keys.isStoredDocumentRowKey(key);
        const schema_change = std.mem.eql(u8, key, keys.public_schema_json_key);
        if (!primary and !intent and !schema_change and !completion_admission and !resolution_request) return;
        if (primary) self.touched_primary = true;
        if (!self.checked) {
            self.checked = true;
            if (cache == null or cache.?.load(.acquire) != 1) {
                const state = try load(txn);
                // Consumer removal cannot discard credits already promised
                // to a prepared transaction. Finish those resolutions even
                // when the last public consumer has been released.
                const policy = try loadPolicy(txn);
                if (policy != null and state == null) return error.HistoryCorrupt;
                self.enabled = if (state) |current| policy != null or current.active() or current.reserved_bytes != 0 or completion_admission or resolution_request else false;
                if (policy) |configured| if (!std.mem.eql(u8, &configured.namespace, &state.?.namespace) or configured.limit != state.?.limit) return error.HistoryCorrupt;
                if (state) |current| {
                    try requireNamespace(txn, current, current.namespace);
                    self.namespace = current.namespace;
                    self.initial_authority = try authority.load(txn);
                }
                if (cache) |known| {
                    if (state != null) known.store(2, .release) else _ = known.cmpxchgStrong(0, 1, .acq_rel, .acquire);
                }
            }
        }
        if (!self.enabled) {
            if (resolution_request) return error.HistoryAuthorityRequired;
            return;
        }
        if (self.control or self.poisoned) return error.HistoryMixedControl;
        if (resolution_request) {
            if (key.len != completion.resolution_request_prefix.len + 16 or value == null or self.resolution_request != null) return error.InvalidTxnRecord;
            const request = try completion.ResolutionRequest.decode(value.?);
            if (!std.mem.eql(u8, key[completion.resolution_request_prefix.len..], &request.transaction_id)) return error.InvalidTxnRecord;
            if (self.transaction_id) |id| if (!std.mem.eql(u8, &id, &request.transaction_id)) return error.HistoryMultipleTransactions;
            self.transaction_id = request.transaction_id;
            self.resolution_request = request;
            return;
        }
        if (completion_admission) return self.touchCompletion(a, txn, key, value);
        if (intent) return self.touchIntent(a, txn, key, value);
        if (!self.schema_checked) {
            self.schema_checked = true;
            if (try optional(txn, keys.public_schema_json_key)) |schema| {
                if (schema.len > frame.max_schema_bytes) return error.HistorySchemaTooLarge;
                self.schema_before = try a.dupe(u8, schema);
            }
        }
        if (schema_change) {
            if (value) |raw| if (raw.len > frame.max_schema_bytes) return error.HistorySchemaTooLarge;
            return;
        }
        if (self.rows.contains(key)) return;
        if (self.rows.count() >= frame.max_events or key.len > frame.max_bytes - @min(self.row_bytes, frame.max_bytes)) return error.HistoryTooLarge;
        const before = try optional(txn, key);
        const before_bytes = if (before) |raw| raw.len else 0;
        const size = std.math.add(usize, before_bytes, key.len) catch return error.HistoryTooLarge;
        if (size > frame.max_bytes - @min(self.row_bytes, frame.max_bytes)) return error.HistoryTooLarge;
        const owned_key = try a.dupe(u8, key);
        errdefer a.free(owned_key);
        const owned_value = if (before) |raw| try a.dupe(u8, raw) else null;
        errdefer if (owned_value) |raw| a.free(raw);
        try self.rows.put(a, owned_key, owned_value);
        self.row_bytes += size;
    }
    fn touchCompletion(self: *Capture, a: A, txn: anytype, key: []const u8, value: ?[]const u8) !void {
        if (key.len != completion.admission_prefix.len + 16) return error.InvalidTxnRecord;
        const id = key[completion.admission_prefix.len..][0..16].*;
        const entry = try self.completion_credits.getOrPut(a, id);
        if (!entry.found_existing) {
            const previous = try CompletionCredit.load(txn, id);
            entry.value_ptr.* = .{ .previous = previous.bytes, .next = previous.bytes, .digest = previous.digest };
        }
        if (value) |raw| {
            var parsed = try completion.parseAdmission(a, raw);
            defer parsed.deinit();
            const admission = parsed.value.admission;
            if (!std.mem.eql(u8, &admission.transaction_id, &id)) return error.InvalidTxnRecord;
            _ = try admission.member(self.namespace);
            if (entry.value_ptr.previous != 0 and !std.mem.eql(u8, &entry.value_ptr.digest, &parsed.value.digest)) return error.InvalidTxnRecord;
            entry.value_ptr.next = try admission.reservationBytes(raw.len);
            entry.value_ptr.digest = parsed.value.digest;
        } else entry.value_ptr.next = 0;
    }

    fn touchIntent(self: *Capture, a: A, txn: anytype, key: []const u8, value: ?[]const u8) !void {
        if (key.len <= intents_prefix.len + 17 or key[intents_prefix.len + 16] != ':') return error.InvalidTxnRecord;
        const id = key[intents_prefix.len..][0..16].*;
        const logical = key[intents_prefix.len + 17 ..];
        // Metadata/integrity intents have no public tuple image.
        if (@import("transaction_metadata_keys.zig").isRawMetadataKey(logical)) return;
        const item = try self.members.getOrPut(a, key);
        if (!item.found_existing) {
            item.key_ptr.* = a.dupe(u8, key) catch |err| {
                _ = self.members.remove(key);
                return err;
            };
            const raw = try optional(txn, &memberKey(key));
            item.value_ptr.* = if (raw) |bytes| blk: {
                if (bytes.len != 8) return error.HistoryCorrupt;
                break :blk std.mem.readInt(u64, bytes[0..8], .little);
            } else 0;
        }
        const old_cost = item.value_ptr.*;
        var next_cost: u64 = 0;
        if (value) |raw| {
            const decoded = try intent_value.Value.decode(raw);
            const document = try keys.documentKeyAlloc(a, logical);
            defer a.free(document);
            const relational = try keys.relationalRowKeyAlloc(a, logical);
            defer a.free(relational);
            const before_document = try optional(txn, document);
            const before_document_size = if (before_document) |bytes| bytes.len else 0;
            const before_relational = try optional(txn, relational);
            const before_relational_size = if (before_relational) |bytes| bytes.len else 0;
            if (before_document_size != 0 and before_relational_size != 0) return error.HistoryCorrupt;
            const image: u64 = if (decoded.prepared_row) |bytes| bytes.len else if (decoded.value) |bytes| try intent_value.documentImageBytes(a, bytes) else 0;
            // A schema rewrite can replace a document primary with a typed
            // primary under the same logical key: reserve both physical keys.
            next_cost = std.math.add(u64, image, before_document_size + before_relational_size + 2 * (document.len + 12) + 64) catch return error.HistoryTooLarge;
        } else if (old_cost != 0) {
            if (self.transaction_id) |current| if (!std.mem.eql(u8, &current, &id)) return error.HistoryMultipleTransactions;
            self.transaction_id = id;
        }
        const pending = try self.reservations.getOrPut(a, id);
        if (!pending.found_existing) {
            const previous = try Reservation.load(txn, id);
            try self.reservation_previous.put(a, id, previous);
            pending.value_ptr.* = previous;
        }
        pending.value_ptr.bytes = std.math.add(u64, std.math.sub(u64, pending.value_ptr.bytes, old_cost) catch return error.HistoryCorrupt, next_cost) catch return error.HistoryFull;
        if (old_cost == 0 and next_cost != 0) pending.value_ptr.members += 1;
        if (old_cost != 0 and next_cost == 0) pending.value_ptr.members = std.math.sub(u64, pending.value_ptr.members, 1) catch return error.HistoryCorrupt;
        item.value_ptr.* = next_cost;
    }
    fn sourcePosition(self: *Capture, txn: anytype) !completion.Digest {
        var owner = (try authority.load(txn)) orelse return error.HistoryAuthorityRequired;
        const initial = self.initial_authority orelse return error.HistoryAuthorityRequired;
        if (owner.kind != initial.kind) return error.HistoryAuthorityRequired;
        if (owner.kind == .native) {
            if (owner.sequence == initial.sequence) {
                owner.sequence = try authority.advance(txn, self.namespace, null);
            } else if (owner.sequence != initial.sequence +| 1) return error.HistoryAuthorityRequired;
        }
        var position: completion.Digest = @splat(0);
        if (owner.kind == .raft) {
            const marker = self.raft_marker orelse return error.HistoryAuthorityRequired;
            @memcpy(position[0..16], &marker);
            position[31] = 1;
        } else {
            std.mem.writeInt(u64, position[0..8], owner.sequence, .little);
            position[31] = 2;
        }
        completion.validatePosition(position) catch return error.HistoryAuthorityRequired;
        return position;
    }

    pub fn stage(self: *Capture, a: A, txn: anytype) !void {
        if (self.poisoned) return error.HistoryTransactionFailed;
        if (!self.enabled or (self.rows.count() == 0 and self.reservations.count() == 0 and self.completion_credits.count() == 0 and self.resolution_request == null)) return;
        errdefer self.poisoned = true;
        self.staging = true;
        defer self.staging = false;
        var state = (try load(txn)) orelse return error.HistoryCorrupt;
        try requireNamespace(txn, state, self.namespace);
        var produced: ?completion.Effects = null;
        var produced_position: ?completion.Digest = null;
        var completions = self.completion_credits.iterator();
        while (completions.next()) |entry| {
            const change = entry.value_ptr.*;
            state.reserved_bytes = std.math.add(u64, std.math.sub(u64, state.reserved_bytes, change.previous) catch return error.HistoryCorrupt, change.next) catch return error.HistoryFull;
            if (change.next == 0) {
                txn.delete(&completionCreditKey(entry.key_ptr.*)) catch |err| if (err != error.NotFound) return err;
            } else {
                const credit: CompletionCredit = .{ .bytes = change.next, .digest = change.digest };
                try txn.put(&completionCreditKey(entry.key_ptr.*), &credit.encode());
            }
        }
        var reservations = self.reservations.iterator();
        while (reservations.next()) |entry| {
            const previous = self.reservation_previous.get(entry.key_ptr.*) orelse return error.HistoryCorrupt;
            state.reserved_bytes = std.math.add(u64, std.math.sub(u64, state.reserved_bytes, try previous.total()) catch return error.HistoryCorrupt, try entry.value_ptr.total()) catch return error.HistoryFull;
            if (entry.value_ptr.members == 0) {
                txn.delete(&reservationKey(entry.key_ptr.*)) catch |err| if (err != error.NotFound) return err;
            } else {
                if (entry.value_ptr.members > frame.max_events / 2) return error.HistoryTooLarge;
                if (try entry.value_ptr.total() > frame.max_bytes) return error.HistoryTooLarge;
                var raw: [16]u8 = undefined;
                std.mem.writeInt(u64, raw[0..8], entry.value_ptr.members, .little);
                std.mem.writeInt(u64, raw[8..16], entry.value_ptr.bytes, .little);
                try txn.put(&reservationKey(entry.key_ptr.*), &raw);
            }
        }
        var members = self.members.iterator();
        while (members.next()) |entry| {
            if (entry.value_ptr.* == 0) {
                txn.delete(&memberKey(entry.key_ptr.*)) catch |err| if (err != error.NotFound) return err;
            } else {
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, entry.value_ptr.*, .little);
                try txn.put(&memberKey(entry.key_ptr.*), &raw);
            }
        }
        if (state.reserved_bytes > state.limit - state.retained_bytes) return error.HistoryFull;
        if (self.rows.count() != 0) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            var events: std.ArrayList(frame.Event) = .empty;
            var rows = self.rows.iterator();
            while (rows.next()) |entry| {
                const after = try optional(txn, entry.key_ptr.*);
                if (after == null and entry.value_ptr.* == null) continue;
                // Readers can reuse scratch on each lookup; own every image
                // before asking the backend for another row or schema.
                try events.append(scratch, .{ .key = entry.key_ptr.*, .before = entry.value_ptr.*, .after = if (after) |raw| try scratch.dupe(u8, raw) else null });
            }
            if (events.items.len != 0) {
                std.mem.sort(frame.Event, events.items, {}, struct {
                    fn less(_: void, left: frame.Event, right: frame.Event) bool {
                        return std.mem.order(u8, left.key, right.key) == .lt;
                    }
                }.less);
                const sequence = std.math.add(u64, state.latest, 1) catch return error.HistoryCorrupt;
                const position = try self.sourcePosition(txn);
                var transaction_id: [16]u8 = undefined;
                if (self.transaction_id) |id| transaction_id = id else {
                    var identity: [32]u8 = undefined;
                    @memcpy(identity[0..24], &self.namespace);
                    std.mem.writeInt(u64, identity[24..32], sequence, .little);
                    transaction_id = frame.digest(&identity)[0..16].*;
                }
                const schema_after = try optional(txn, keys.public_schema_json_key);
                const encoded = try frame.encode(scratch, .{ .namespace = state.namespace, .sequence = sequence, .transaction_id = transaction_id, .transaction_scope = if (self.transaction_id == null) .root_local else .coordinated, .source_position = position, .committed_at_ns = if (self.transaction_id == null) self.history_commit_time_ns else 0, .schema_before = self.schema_before, .schema_after = schema_after }, events.items);
                const descriptor_raw = try frame.Descriptor.encode(scratch, state.namespace, sequence, encoded);
                const descriptor = try frame.Descriptor.decode(descriptor_raw, state.namespace, sequence);
                if (descriptor.storedBytes() > state.limit - state.retained_bytes - state.reserved_bytes) return error.HistoryFull;
                for (0..descriptor.count) |i| try txn.put(&chunkKey(sequence, @intCast(i)), encoded[i * frame.chunk_bytes .. @min(encoded.len, (i + 1) * frame.chunk_bytes)]);
                try txn.put(&recordKey(sequence), descriptor_raw);
                produced = .{ .sequence = sequence, .frame_digest = frame.digest(descriptor_raw) };
                produced_position = position;
                state.latest = sequence;
                state.retained_bytes += descriptor.storedBytes();
            }
        }
        if (self.resolution_request) |request| {
            const plan_key = completion.admission_prefix.* ++ request.transaction_id;
            const plan_raw = (try optional(txn, &plan_key)) orelse return error.HistoryCorrupt;
            var plan = try completion.parseAdmission(a, plan_raw);
            defer plan.deinit();
            if (!std.mem.eql(u8, &plan.value.digest, &request.admission_digest)) return error.InvalidTxnRecord;
            try requireCompletionReservation(txn, state.namespace, plan.value.admission);
            const record_key = completion.record_prefix.* ++ request.transaction_id;
            const record = (try optional(txn, &record_key)) orelse return error.HistoryCorrupt;
            if (!std.mem.eql(u8, &completion.bytesDigest(record), &request.record_digest)) return error.InvalidTxnRecord;
            try @import("transactions.zig").requireCompletionResolvedRecord(record, request.commit_version);
            const manifest = try plan.value.admission.manifest(request.commit_version);
            const member = try plan.value.admission.member(state.namespace);
            const receipt_key = completion.resolutionKey(request.transaction_id);
            if (try optional(txn, &receipt_key) != null) return error.InvalidTxnRecord;
            const position = produced_position orelse try self.sourcePosition(txn);
            const receipt: completion.Resolution = .{ .namespace = state.namespace, .owner_fence = member.owner_fence, .manifest_digest = manifest.digest(), .commit_version = request.commit_version, .source_position = position, .outcome = if (produced) |effects| .{ .effects = effects } else .{ .no_effect = completion.noEffectProof(manifest, state.namespace, position) } };
            const encoded_receipt = try completion.encodeResolution(a, manifest, receipt);
            defer a.free(encoded_receipt);
            try txn.put(&receipt_key, encoded_receipt);
            try txn.delete(&completion.requestKey(request.transaction_id));
        }
        try save(txn, state);
    }
};

pub fn read(a: A, txn: anytype, namespace: frame.Namespace, id: [16]u8, epoch: u64, after: u64) !?frame.Descriptor {
    var state = (try load(txn)) orelse return error.HistoryResynchronizationRequired;
    try requireNamespace(txn, state, namespace);
    const consumer = try state.findConsumer(id, epoch);
    if (after < consumer.start or after < state.reclaimed or after > state.latest) return error.HistoryResynchronizationRequired;
    if (after == state.latest) return null;
    const raw = try a.dupe(u8, (try optional(txn, &recordKey(after + 1))) orelse return error.HistoryCorrupt);
    errdefer a.free(raw);
    return try frame.Descriptor.decode(raw, namespace, after + 1);
}

/// A reader owns one reusable chunk buffer, so backend point-read scratch and
/// unrelated metadata checks cannot invalidate its authenticated cached chunk.
pub fn ChunkSource(comptime Txn: type) type {
    return struct {
        alloc: A,
        txn: *Txn,
        sequence: u64,
        buffer: []u8,
        pub fn init(a: A, txn: *Txn, descriptor: frame.Descriptor) !@This() {
            return .{ .alloc = a, .txn = txn, .sequence = descriptor.sequence, .buffer = try a.alloc(u8, @min(frame.chunk_bytes, descriptor.total)) };
        }
        pub fn deinit(self: *@This()) void {
            self.alloc.free(self.buffer);
            self.* = undefined;
        }
        fn get(ptr: *anyopaque, ordinal: u32) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const raw = self.txn.get(&chunkKey(self.sequence, ordinal)) catch |err| if (err == error.NotFound) return error.HistoryCorrupt else return err;
            if (raw.len > self.buffer.len) return error.HistoryCorrupt;
            @memcpy(self.buffer[0..raw.len], raw);
            return self.buffer[0..raw.len];
        }
        pub fn source(self: *@This()) frame.Source {
            return .{ .context = self, .read = get };
        }
    };
}

test "native history consumer control coordinated frame completion distinguishes absent evidence from a corrupt publication" {
    const Store = struct {
        retained_only: bool = false,

        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (self.retained_only and std.mem.startsWith(u8, key, completion_retained_prefix)) return "invalid retained record";
            return error.NotFound;
        }
    };
    var store: Store = .{};
    const header: frame.Header = .{ .namespace = @splat(1), .sequence = 7, .transaction_id = @splat(2), .transaction_scope = .coordinated };
    var descriptor: frame.Descriptor = .{ .raw = "", .namespace = header.namespace, .sequence = header.sequence, .total = 0, .count = 0, .events = 0 };
    try std.testing.expectEqual(null, try readFrameCompletion(std.testing.allocator, &store, 1, header, descriptor));
    var effects = [_]?completion.Effects{.{ .sequence = 11, .frame_digest = @splat(3) }};
    const projection: CompletionProjection = .{ .roots = &.{header.namespace}, .effects = &effects };
    try std.testing.expectEqual(null, try readProjectedFrameCompletion(std.testing.allocator, &store, 1, header, descriptor, projection));
    try std.testing.expectEqual(@as(u64, 11), effects[0].?.sequence);
    store.retained_only = true;
    try std.testing.expectError(error.HistoryCorrupt, readFrameCompletion(std.testing.allocator, &store, 1, header, descriptor));
    descriptor.sequence += 1;
    try std.testing.expectError(error.InvalidHistoryAdmission, readFrameCompletion(std.testing.allocator, &store, 1, header, descriptor));
}
