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
const record_prefix = "\x00\x00__committed_history__:record:";
const chunk_prefix = "\x00\x00__committed_history__:chunk:";
const member_prefix = "\x00\x00__committed_history__:member:";
const reservation_prefix = "\x00\x00__committed_history__:reservation:";
const intents_prefix = "\x00\x00__txn_intents__:";
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
pub fn admit(txn: anytype, namespace: frame.Namespace, id: [16]u8, limit: u64, expires: u64, policy_revision: u64) !Consumer {
    if (std.mem.allEqual(u8, &id, 0) or limit < transaction_overhead + frame.chunk_bytes) return error.InvalidHistoryAdmission;
    var state = (try load(txn)) orelse State{ .namespace = namespace, .limit = limit };
    try requireNamespace(txn, state, namespace);
    if (state.limit != limit) return error.InvalidHistoryAdmission;
    for (state.consumers) |consumer| if (consumer.epoch != 0 and std.mem.eql(u8, &consumer.id, &id)) {
        if (consumer.expires_at_seconds != expires or consumer.policy_revision != policy_revision) return error.HistoryConsumerConflict;
        return consumer;
    };
    if (!state.active()) {
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(intents_prefix)) |entry| if (std.mem.startsWith(u8, entry.key, intents_prefix)) return error.HistoryPreparedTransactionsPresent;
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
    schema_checked: bool = false,
    raft_marker: ?[16]u8 = null,
    reservations: std.AutoHashMapUnmanaged([16]u8, Reservation) = .empty,
    reservation_previous: std.AutoHashMapUnmanaged([16]u8, Reservation) = .empty,
    members: std.StringHashMapUnmanaged(u64) = .empty,
    transaction_id: ?[16]u8 = null,

    pub fn deinit(self: *Capture, a: A) void {
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
        if (std.mem.eql(u8, key, state_key)) {
            if (self.touched_primary or self.rows.count() != 0 or self.reservations.count() != 0) return error.HistoryMixedControl;
            self.control = true;
            if (cache) |known| known.store(2, .release);
            return;
        }
        if (std.mem.eql(u8, key, &keys.ordered_document_applied_entry_key)) {
            self.raft_marker = if (value) |raw| if (raw.len == 16) raw[0..16].* else null else null;
            return;
        }
        const intent = std.mem.startsWith(u8, key, intents_prefix);
        const primary = keys.isStoredDocumentRowKey(key);
        const schema_change = std.mem.eql(u8, key, keys.public_schema_json_key);
        if (!primary and !intent and !schema_change) return;
        if (primary) self.touched_primary = true;
        if (!self.checked) {
            self.checked = true;
            if (cache == null or cache.?.load(.acquire) != 1) {
                const state = try load(txn);
                // Consumer removal cannot discard credits already promised
                // to a prepared transaction. Finish those resolutions even
                // when the last public consumer has been released.
                self.enabled = if (state) |current| current.active() or current.reserved_bytes != 0 else false;
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
        if (!self.enabled) return;
        if (self.control or self.poisoned) return error.HistoryMixedControl;
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
    pub fn stage(self: *Capture, a: A, txn: anytype) !void {
        if (self.poisoned) return error.HistoryTransactionFailed;
        if (!self.enabled or (self.rows.count() == 0 and self.reservations.count() == 0)) return;
        errdefer self.poisoned = true;
        self.staging = true;
        defer self.staging = false;
        var state = (try load(txn)) orelse return error.HistoryCorrupt;
        try requireNamespace(txn, state, self.namespace);
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
                var owner = (try authority.load(txn)) orelse return error.HistoryAuthorityRequired;
                const initial = self.initial_authority orelse return error.HistoryAuthorityRequired;
                if (owner.kind != initial.kind) return error.HistoryAuthorityRequired;
                if (owner.kind == .native) {
                    if (owner.sequence == initial.sequence) {
                        owner.sequence = try authority.advance(txn, state.namespace, null);
                    } else if (owner.sequence != initial.sequence +| 1) return error.HistoryAuthorityRequired;
                }
                var position: [32]u8 = @splat(0);
                if (owner.kind == .raft) {
                    const marker = self.raft_marker orelse return error.HistoryAuthorityRequired;
                    if (std.mem.readInt(u64, marker[0..8], .little) == 0 or std.mem.readInt(u64, marker[8..16], .little) == 0) return error.HistoryAuthorityRequired;
                    @memcpy(position[0..16], &marker);
                    position[31] = 1;
                } else {
                    std.mem.writeInt(u64, position[0..8], owner.sequence, .little);
                    position[31] = 2;
                }
                var transaction_id: [16]u8 = undefined;
                if (self.transaction_id) |id| transaction_id = id else {
                    var identity: [32]u8 = undefined;
                    @memcpy(identity[0..24], &self.namespace);
                    std.mem.writeInt(u64, identity[24..32], sequence, .little);
                    transaction_id = frame.digest(&identity)[0..16].*;
                }
                const schema_after = try optional(txn, keys.public_schema_json_key);
                const encoded = try frame.encode(scratch, .{ .namespace = state.namespace, .sequence = sequence, .transaction_id = transaction_id, .source_position = position, .schema_before = self.schema_before, .schema_after = schema_after }, events.items);
                const descriptor_raw = try frame.Descriptor.encode(scratch, state.namespace, sequence, encoded);
                const descriptor = try frame.Descriptor.decode(descriptor_raw, state.namespace, sequence);
                if (descriptor.storedBytes() > state.limit - state.retained_bytes - state.reserved_bytes) return error.HistoryFull;
                for (0..descriptor.count) |i| try txn.put(&chunkKey(sequence, @intCast(i)), encoded[i * frame.chunk_bytes .. @min(encoded.len, (i + 1) * frame.chunk_bytes)]);
                try txn.put(&recordKey(sequence), descriptor_raw);
                state.latest = sequence;
                state.retained_bytes += descriptor.storedBytes();
            }
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
