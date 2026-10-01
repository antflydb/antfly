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

//! Shared source-side row/integrity after-image retention. Control operations and captured
//! effects MUST commit in the same store transaction as their respective state.
//! This is a tail substrate, not snapshot certification: admission callers still
//! need an authenticated, transferable immutable source cut before releasing it.
//! Physical row envelopes retain their schema version; consumers must possess
//! that immutable schema registry. Derived indexes/artifacts are not this log.
const std = @import("std");
const internal_keys = @import("internal_keys.zig");
const integrity = @import("db/relational_integrity_contract.zig");
const retained_frame = @import("retained_frame.zig");
const Allocator = std.mem.Allocator;
pub const state_key = "\x00\x00__retained_rows__:state";
const record_prefix = "\x00\x00__retained_rows__:record:";
const chunk_prefix = "\x00\x00__retained_rows__:chunk:";
const gc_cursor_key = "\x00\x00__retained_rows__:gc_cursor";
pub const max_consumers = 16;
pub const max_frame_bytes = 16 * 1024 * 1024;
pub const max_keys = 65536;
pub const default_limit: u64 = 256 * 1024 * 1024;
pub const Consumer = struct { epoch: u64 = 0, pin: [32]u8 = @splat(0), start: u64 = 0, acknowledged: u64 = 0 };
pub const Namespace = [24]u8;
pub const reservation_key = "\x00\x00__retained_rows__:reserved";
const intent_prefix = "\x00\x00__txn_intents__:";
/// A single aggregate protects prepared votes without adding work to inactive
/// ordinary mutations. Per-transaction credits live in the existing intent
/// admission ledger and are retired in the same atomic batch as resolution.
pub const Reservations = struct {
    namespace: Namespace = @splat(0),
    bytes: u64 = 0,
    oversized: u64 = 0,
    complete: bool = false,
};
pub fn loadReservations(txn: anytype) !?Reservations {
    const raw = txn.get(reservation_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 77 or !std.mem.eql(u8, raw[0..4], "RRV1") or raw[44] > 1 or
        !std.mem.eql(u8, raw[45..77], &checksum(raw[0..45]))) return error.RetainedEffectsCorrupt;
    return .{ .namespace = raw[4..28].*, .bytes = std.mem.readInt(u64, raw[28..36], .little), .oversized = std.mem.readInt(u64, raw[36..44], .little), .complete = raw[44] == 1 };
}
fn saveReservations(txn: anytype, value: Reservations) !void {
    var raw: [77]u8 = undefined;
    @memcpy(raw[0..4], "RRV1");
    @memcpy(raw[4..28], &value.namespace);
    std.mem.writeInt(u64, raw[28..36], value.bytes, .little);
    std.mem.writeInt(u64, raw[36..44], value.oversized, .little);
    raw[44] = @intFromBool(value.complete);
    @memcpy(raw[45..77], &checksum(raw[0..45]));
    try txn.put(reservation_key, &raw);
}
fn noExistingIntents(txn: anytype) !bool {
    var cursor = try txn.openCursor();
    defer cursor.close();
    const entry = (try cursor.seekAtOrAfter(intent_prefix)) orelse return true;
    return !std.mem.startsWith(u8, entry.key, intent_prefix);
}
fn bindReservations(txn: anytype, value: *Reservations) !void {
    if (try namespace(txn)) |current| {
        if (std.mem.eql(u8, &value.namespace, &@as(Namespace, @splat(0)))) value.namespace = current else if (!std.mem.eql(u8, &value.namespace, &current)) return error.RetainedEffectsNamespaceMismatch;
    } else if (!std.mem.eql(u8, &value.namespace, &@as(Namespace, @splat(0)))) return error.RetainedEffectsNamespaceMismatch;
}
/// Replacement-aware, called before writing the new intent ledger. A legacy
/// root without complete accounting cannot admit a source until its prepares
/// drain; checking that boundary is a single ordered prefix seek, not a scan.
pub fn replaceReservation(txn: anytype, previous: u64, next: u64) !void {
    var value = (try loadReservations(txn)) orelse Reservations{ .complete = try noExistingIntents(txn) };
    try bindReservations(txn, &value);
    value.bytes = std.math.add(u64, std.math.sub(u64, value.bytes, previous) catch return error.RetainedEffectsCorrupt, next) catch return error.RetainedEffectsFull;
    value.oversized = std.math.sub(u64, value.oversized, @intFromBool(previous > max_frame_bytes)) catch return error.RetainedEffectsCorrupt;
    value.oversized = std.math.add(u64, value.oversized, @intFromBool(next > max_frame_bytes)) catch return error.RetainedEffectsCorrupt;
    if (try load(txn)) |state| if (state.active()) {
        try requireNamespace(txn, state, value.namespace);
        if (!value.complete or value.oversized != 0 or value.bytes > state.limit - state.retained_bytes) return error.RetainedEffectsFull;
    };
    try saveReservations(txn, value);
}

fn admissionReservations(txn: anytype, current: Namespace) !Reservations {
    var value = (try loadReservations(txn)) orelse Reservations{};
    try bindReservations(txn, &value);
    if (!value.complete) {
        if (!try noExistingIntents(txn)) return error.RetainedEffectsFull;
        if (value.bytes != 0 or value.oversized != 0) return error.RetainedEffectsCorrupt;
        value.complete = true;
    }
    value.namespace = current;
    try saveReservations(txn, value);
    return value;
}
pub const State = struct {
    direct_vectors: bool = false,
    chunked_frames: bool = false,
    graph_artifacts: bool = false,
    namespace: Namespace = @splat(0),
    latest: u64 = 0,
    reclaimed: u64 = 0,
    retained_bytes: u64 = 0,
    limit: u64 = default_limit,
    epoch: u64 = 0,
    consumers: [max_consumers]Consumer = @splat(.{}),

    pub fn active(self: State) bool {
        for (self.consumers) |value| if (value.epoch != 0) return true;
        return false;
    }
    pub fn reclaimableThrough(self: State) u64 {
        var floor = self.latest;
        for (self.consumers) |value| if (value.epoch != 0) {
            floor = @min(floor, value.acknowledged);
        };
        return floor;
    }
    fn consumer(self: *State, epoch: u64, pin: [32]u8) !*Consumer {
        for (&self.consumers) |*value| if (value.epoch == epoch and epoch != 0) {
            if (!std.mem.eql(u8, &value.pin, &pin)) return error.RetainedEffectsFenceMismatch;
            return value;
        };
        return error.RetainedEffectsFenceMismatch;
    }
};
const legacy_state_size = 4 + 24 + 5 * 8 + max_consumers * 56 + 32;
const state_size = legacy_state_size + 1;
const chunked_state_size = state_size + 1;
const graph_state_size = chunked_state_size + 1;

fn checksum(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn encode(state: State) [state_size]u8 {
    var bytes: [state_size]u8 = undefined;
    @memcpy(bytes[0..4], "RER3");
    @memcpy(bytes[4..28], &state.namespace);
    var pos: usize = 28;
    for ([_]u64{ state.latest, state.reclaimed, state.retained_bytes, state.limit, state.epoch }) |value| {
        std.mem.writeInt(u64, bytes[pos..][0..8], value, .little);
        pos += 8;
    }
    for (state.consumers) |value| {
        std.mem.writeInt(u64, bytes[pos..][0..8], value.epoch, .little);
        @memcpy(bytes[pos + 8 ..][0..32], &value.pin);
        std.mem.writeInt(u64, bytes[pos + 40 ..][0..8], value.acknowledged, .little);
        std.mem.writeInt(u64, bytes[pos + 48 ..][0..8], value.start, .little);
        pos += 56;
    }
    bytes[pos] = @intFromBool(state.direct_vectors);
    pos += 1;
    @memcpy(bytes[pos..][0..32], &checksum(bytes[0..pos]));
    return bytes;
}
fn encodeChunked(state: State) [chunked_state_size]u8 {
    var bytes: [chunked_state_size]u8 = undefined;
    const previous = encode(state);
    @memcpy(bytes[0 .. state_size - 32], previous[0 .. state_size - 32]);
    @memcpy(bytes[0..4], "RER4");
    bytes[state_size - 32] = @intFromBool(state.chunked_frames);
    @memcpy(bytes[chunked_state_size - 32 ..], &checksum(bytes[0 .. chunked_state_size - 32]));
    return bytes;
}
fn encodeGraph(state: State) [graph_state_size]u8 {
    var bytes: [graph_state_size]u8 = undefined;
    const previous = encodeChunked(state);
    @memcpy(bytes[0 .. chunked_state_size - 32], previous[0 .. chunked_state_size - 32]);
    @memcpy(bytes[0..4], "RER5");
    bytes[chunked_state_size - 32] = @intFromBool(state.graph_artifacts);
    @memcpy(bytes[graph_state_size - 32 ..], &checksum(bytes[0 .. graph_state_size - 32]));
    return bytes;
}
pub fn decode(bytes: []const u8) !State {
    const legacy = bytes.len == legacy_state_size;
    const chunked = bytes.len == chunked_state_size;
    const graph = bytes.len == graph_state_size;
    if ((!legacy and bytes.len != state_size and !chunked and !graph) or
        !std.mem.eql(u8, bytes[0..4], if (legacy) "RER2" else if (graph) "RER5" else if (chunked) "RER4" else "RER3") or
        !std.mem.eql(u8, bytes[bytes.len - 32 ..], &checksum(bytes[0 .. bytes.len - 32])))
        return error.RetainedEffectsCorrupt;
    var state: State = .{ .namespace = bytes[4..28].* };
    var pos: usize = 28;
    inline for (.{ "latest", "reclaimed", "retained_bytes", "limit", "epoch" }) |field| {
        @field(state, field) = std.mem.readInt(u64, bytes[pos..][0..8], .little);
        pos += 8;
    }
    if (state.reclaimed > state.latest or state.retained_bytes > state.limit or state.limit < max_frame_bytes)
        return error.RetainedEffectsCorrupt;
    for (&state.consumers, 0..) |*value, i| {
        value.* = .{
            .epoch = std.mem.readInt(u64, bytes[pos..][0..8], .little),
            .pin = bytes[pos + 8 ..][0..32].*,
            .acknowledged = std.mem.readInt(u64, bytes[pos + 40 ..][0..8], .little),
            .start = std.mem.readInt(u64, bytes[pos + 48 ..][0..8], .little),
        };
        pos += 56;
        if (value.epoch > state.epoch or value.acknowledged > state.latest or value.start > value.acknowledged or
            (value.epoch != 0 and value.acknowledged < state.reclaimed)) return error.RetainedEffectsCorrupt;
        for (state.consumers[0..i]) |prior| if (value.epoch != 0 and prior.epoch == value.epoch)
            return error.RetainedEffectsCorrupt;
    }
    if (!legacy) {
        if (bytes[pos] > 1) return error.RetainedEffectsCorrupt;
        state.direct_vectors = bytes[pos] == 1;
        pos += 1;
    }
    if (chunked or graph) {
        if (bytes[pos] > 1) return error.RetainedEffectsCorrupt;
        state.chunked_frames = bytes[pos] == 1;
        pos += 1;
    }
    if (graph) {
        if (bytes[pos] > 1) return error.RetainedEffectsCorrupt;
        state.graph_artifacts = bytes[pos] == 1;
        if (state.graph_artifacts and (!state.chunked_frames or !state.direct_vectors)) return error.RetainedEffectsCorrupt;
    }
    return state;
}
/// Diagnostic catalog read, including foreign state awaiting adoption cleanup.
/// This does not authorize a consumer operation; those require explicit scope.
pub fn load(txn: anytype) !?State {
    const raw = txn.get(state_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decode(raw);
}
fn save(txn: anytype, state: State) !void {
    if (state.graph_artifacts) {
        try txn.put(state_key, &encodeGraph(state));
    } else if (state.chunked_frames) {
        try txn.put(state_key, &encodeChunked(state));
    } else try txn.put(state_key, &encode(state));
}

fn namespace(txn: anytype) !?Namespace {
    const raw = txn.get(&internal_keys.identity_namespace_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 24) return error.RetainedEffectsCorrupt;
    return raw[0..24].*;
}

fn matchesNamespace(txn: anytype, state: State) !bool {
    const current = (try namespace(txn)) orelse return false;
    return std.mem.eql(u8, &current, &state.namespace);
}

fn requireNamespace(txn: anytype, state: State, expected: Namespace) !void {
    if (!std.mem.eql(u8, &state.namespace, &expected) or !try matchesNamespace(txn, state)) return error.RetainedEffectsNamespaceMismatch;
}

/// Epochs are monotonically allocated by the source coordinator, never reused.
/// Retired epochs remain fenced without an unbounded tombstone collection.
/// Returned sequence must be bound to the immutable snapshot publication.
/// expected_namespace is the authenticated caller scope, not inferred from a
/// newly adopted store; delayed source admission cannot silently bind a target.
pub fn admit(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, limit: u64) !u64 {
    return admitWithDirectVectors(txn, expected_namespace, epoch, pin, limit, false);
}

pub fn admitWithDirectVectors(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, limit: u64, direct_vectors: bool) !u64 {
    return admitWithCapabilities(txn, expected_namespace, epoch, pin, limit, direct_vectors, false);
}

/// Only an all-voter protocol-15 admission may request chunked frames. The
/// existing route always passes false, so an upgraded writer cannot emit REF5
/// to an older merge/rewrite receiver merely because it sees a large row.
pub fn admitWithCapabilities(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, limit: u64, direct_vectors: bool, chunked_frames: bool) !u64 {
    return admitWithArtifactCapabilities(txn, expected_namespace, epoch, pin, limit, direct_vectors, chunked_frames, false);
}

/// Graph afterimages use a separately negotiated effect language. No public
/// source admission calls this until graph transfer is certified end to end.
pub fn admitWithArtifactCapabilities(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, limit: u64, direct_vectors: bool, chunked_frames: bool, graph_artifacts: bool) !u64 {
    if (epoch == 0 or std.mem.eql(u8, &pin, &@as([32]u8, @splat(0))) or limit < max_frame_bytes)
        return error.InvalidRetainedEffectsAdmission;
    if (graph_artifacts and (!chunked_frames or !direct_vectors)) return error.InvalidRetainedEffectsAdmission;
    const current = (try namespace(txn)) orelse return error.RetainedEffectsIdentityRequired;
    if (!std.mem.eql(u8, &current, &expected_namespace)) return error.RetainedEffectsNamespaceMismatch;
    var state = (try load(txn)) orelse State{ .namespace = current, .limit = limit };
    try requireNamespace(txn, state, expected_namespace);
    if (state.limit != limit) return error.InvalidRetainedEffectsAdmission;
    // Prepare and admission serialize through the same primary transaction.
    // Pre-admission prepares may predate the artifact catalog/accounting; let
    // them resolve before activating this effect language. New prepares after
    // activation reserve their artifacts while catalog DDL is fenced.
    if (direct_vectors and !state.active() and !try noExistingIntents(txn)) return error.RetainedEffectsFull;
    // A retained interval has one effect language. Never silently widen an
    // existing consumer's proof. Released intervals are self-describing and
    // new consumers start at the latest sequence, beyond unreclaimed frames.
    if (state.direct_vectors != direct_vectors) {
        if (state.active()) return error.InvalidRetainedEffectsAdmission;
        state.direct_vectors = direct_vectors;
    }
    if (state.chunked_frames != chunked_frames) {
        if (state.active()) return error.InvalidRetainedEffectsAdmission;
        state.chunked_frames = chunked_frames;
    }
    if (state.graph_artifacts != graph_artifacts) {
        if (state.active()) return error.InvalidRetainedEffectsAdmission;
        state.graph_artifacts = graph_artifacts;
    }
    for (state.consumers) |value| if (value.epoch == epoch) {
        if (!std.mem.eql(u8, &value.pin, &pin)) return error.RetainedEffectsFenceMismatch;
        return value.start;
    };
    if (epoch <= state.epoch) return error.RetainedEffectsFenceMismatch;
    const reserved = try admissionReservations(txn, current);
    if (reserved.oversized != 0 or reserved.bytes > state.limit - state.retained_bytes or
        state.limit - state.retained_bytes - reserved.bytes < max_frame_bytes) return error.RetainedEffectsFull;
    for (&state.consumers) |*value| if (value.epoch == 0) {
        value.* = .{ .epoch = epoch, .pin = pin, .start = state.latest, .acknowledged = state.latest };
        state.epoch = epoch;
        try save(txn, state);
        return state.latest;
    };
    return error.RetainedEffectsConsumerLimit;
}

/// Only an authenticated durable receiver receipt may authorize acknowledgement.
/// The CAS is exact: a stale control RPC cannot skip an unacknowledged interval.
pub fn acknowledge(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, previous: u64, next: u64) !void {
    var state = (try load(txn)) orelse return error.RetainedEffectsFenceMismatch;
    try requireNamespace(txn, state, expected_namespace);
    const value = try state.consumer(epoch, pin);
    if (next < previous or next > state.latest) return error.RetainedEffectsCursorMismatch;
    if (value.acknowledged == next) return;
    if (value.acknowledged != previous) return error.RetainedEffectsCursorMismatch;
    value.acknowledged = next;
    try save(txn, state);
}

/// Terminal receipt/cancellation fencing belongs to the authenticated driver.
pub fn release(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8) !void {
    var state = (try load(txn)) orelse return error.RetainedEffectsFenceMismatch;
    try requireNamespace(txn, state, expected_namespace);
    for (&state.consumers) |*value| if (value.epoch == epoch and epoch != 0) {
        if (!std.mem.eql(u8, &value.pin, &pin)) return error.RetainedEffectsFenceMismatch;
        value.* = .{};
        try save(txn, state);
        return;
    };
    // A retired epoch cannot affect any current consumer. Retry acknowledgement
    // after a lost terminal response without storing an unbounded receipt set.
    if (epoch == 0 or epoch > state.epoch) return error.RetainedEffectsFenceMismatch;
}

pub fn recordKey(sequence: u64) [record_prefix.len + 8]u8 {
    var key: [record_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..record_prefix.len], record_prefix);
    std.mem.writeInt(u64, key[record_prefix.len..][0..8], sequence, .big);
    return key;
}

pub fn chunkKey(sequence: u64, ordinal: u32) [chunk_prefix.len + 12]u8 {
    var key: [chunk_prefix.len + 12]u8 = undefined;
    @memcpy(key[0..chunk_prefix.len], chunk_prefix);
    std.mem.writeInt(u64, key[chunk_prefix.len..][0..8], sequence, .big);
    std.mem.writeInt(u32, key[chunk_prefix.len + 8 ..][0..4], ordinal, .big);
    return key;
}

fn chunkedRecord(raw: []const u8) bool {
    return raw.len >= 4 and std.mem.eql(u8, raw[0..4], "RFV5");
}

const GcCursor = struct { sequence: u64, next_chunk: u32 };
fn loadGcCursor(txn: anytype) !?GcCursor {
    const raw = txn.get(gc_cursor_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 48 or !std.mem.eql(u8, raw[0..4], "RGC1") or
        !std.mem.eql(u8, raw[16..48], &checksum(raw[0..16]))) return error.RetainedEffectsCorrupt;
    return .{ .sequence = std.mem.readInt(u64, raw[4..12], .little), .next_chunk = std.mem.readInt(u32, raw[12..16], .little) };
}
fn saveGcCursor(txn: anytype, value: GcCursor) !void {
    var raw: [48]u8 = undefined;
    @memcpy(raw[0..4], "RGC1");
    std.mem.writeInt(u64, raw[4..12], value.sequence, .little);
    std.mem.writeInt(u32, raw[12..16], value.next_chunk, .little);
    @memcpy(raw[16..48], &checksum(raw[0..16]));
    try txn.put(gc_cursor_key, &raw);
}

fn descriptorView(raw: []const u8, sequence: u64) !retained_frame.View {
    const Dummy = struct {
        fn read(_: *anyopaque, _: u64, _: u32, _: []u8) !usize {
            return error.RetainedEffectsCorrupt;
        }
    };
    // fromDescriptor deliberately performs no chunk IO.
    return retained_frame.View.fromDescriptor(raw, sequence, .{ .context = @ptrFromInt(1), .read_chunk = Dummy.read });
}

/// One checksummed atomic transaction frame, sorted by physical key. Integrity
/// claims, references and action jobs share the row's committed transaction;
/// they are never reconstructed from a later live owner read.
/// Iteration borrows the frame and never materializes a row or JSON value.
pub const Reader = struct {
    /// Complete checksum-verified REF3 frame borrowed for the read transaction.
    /// `bytes` excludes its checksum and is the effect parser's window.
    encoded_frame: []const u8 = "",
    bytes: []const u8,
    frame_digest: [32]u8 = @splat(0),
    pos: usize = 16,
    remaining: u32,
    pub const Effect = struct {
        key: []const u8,
        value: ?[]const u8,
        timestamp: u64,
        pub fn isIntegrity(self: Effect) bool {
            return integrity.isKey(self.key);
        }
        pub fn isVector(self: Effect) bool {
            return @import("db/online_vector_artifacts.zig").isKey(self.key);
        }
    };
    pub fn init(bytes: []const u8, sequence: u64) !Reader {
        if (bytes.len < 48 or bytes.len > max_frame_bytes or (!std.mem.eql(u8, bytes[0..4], "REF3") and !std.mem.eql(u8, bytes[0..4], "REF4")) or
            std.mem.readInt(u64, bytes[4..12], .little) != sequence)
            return error.RetainedEffectsCorrupt;
        // Share the payload hash pass between checksum validation and complete
        // frame identity. peek clones the hash state, so extending it
        // with the stored checksum avoids hashing a 16 MiB payload twice.
        var frame_hash = std.crypto.hash.sha2.Sha256.init(.{});
        frame_hash.update(bytes[0 .. bytes.len - 32]);
        if (!std.mem.eql(u8, bytes[bytes.len - 32 ..], &frame_hash.peek())) return error.RetainedEffectsCorrupt;
        frame_hash.update(bytes[bytes.len - 32 ..]);
        const count = std.mem.readInt(u32, bytes[12..16], .little);
        if (count == 0 or count > max_keys) return error.RetainedEffectsCorrupt;
        var reader: Reader = .{ .bytes = bytes[0 .. bytes.len - 32], .remaining = count };
        // Verify framing and canonical order before exposing any prefix.
        var last: ?[]const u8 = null;
        while (try reader.next()) |effect| {
            if (effect.isIntegrity()) {
                _ = integrity.parseKey(effect.key) catch return error.RetainedEffectsCorrupt;
                if (effect.timestamp != 0) return error.RetainedEffectsCorrupt;
                if (effect.value) |value| _ = integrity.validateTransferRecord(effect.key, value) catch return error.RetainedEffectsCorrupt;
            } else if (effect.isVector()) {
                if (!std.mem.eql(u8, bytes[0..4], "REF4") or effect.timestamp != 0) return error.RetainedEffectsCorrupt;
                @import("db/online_vector_artifacts.zig").validate(effect.key, effect.value) catch return error.RetainedEffectsCorrupt;
            } else if (!internal_keys.isStoredDocumentRowKey(effect.key)) return error.RetainedEffectsCorrupt;
            if (last) |previous| if (std.mem.order(u8, previous, effect.key) != .lt) return error.RetainedEffectsCorrupt;
            last = effect.key;
        }
        return .{ .encoded_frame = bytes, .bytes = bytes[0 .. bytes.len - 32], .remaining = count, .frame_digest = frame_hash.finalResult() };
    }
    pub fn next(self: *Reader) !?Effect {
        if (self.remaining == 0) {
            if (self.pos != self.bytes.len) return error.RetainedEffectsCorrupt;
            return null;
        }
        if (self.pos > self.bytes.len or self.bytes.len - self.pos < 16) return error.RetainedEffectsCorrupt;
        const key_len = std.mem.readInt(u32, self.bytes[self.pos..][0..4], .little);
        const value_len = std.mem.readInt(u32, self.bytes[self.pos + 4 ..][0..4], .little);
        const timestamp = std.mem.readInt(u64, self.bytes[self.pos + 8 ..][0..8], .little);
        self.pos += 16;
        if (key_len == 0 or key_len > self.bytes.len - self.pos) return error.RetainedEffectsCorrupt;
        const key = self.bytes[self.pos..][0..key_len];
        self.pos += key_len;
        var value: ?[]const u8 = null;
        if (value_len != std.math.maxInt(u32)) {
            if (value_len > self.bytes.len - self.pos) return error.RetainedEffectsCorrupt;
            value = self.bytes[self.pos..][0..value_len];
            self.pos += value_len;
        }
        if (value == null and timestamp != 0) return error.RetainedEffectsCorrupt;
        self.remaining -= 1;
        return .{ .key = key, .value = value, .timestamp = timestamp };
    }
};

/// Borrow one contiguous, scope-bound frame from a caller-owned read view.
/// Missing retained history is an error, never a fallback to current rows.
pub fn read(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, after: u64) !?Reader {
    var state = (try load(txn)) orelse return error.RetainedEffectsFenceMismatch;
    try requireNamespace(txn, state, expected_namespace);
    const value = try state.consumer(epoch, pin);
    if (after < value.start or after < state.reclaimed or after > state.latest) return error.RetainedEffectsCursorMismatch;
    if (after == state.latest) return null;
    const sequence = after + 1;
    const raw = txn.get(&recordKey(sequence)) catch |err| switch (err) {
        error.NotFound => return error.RetainedEffectsCorrupt,
        else => return err,
    };
    if (chunkedRecord(raw)) return error.RetainedEffectsUnsupported;
    return try Reader.init(raw, sequence);
}

pub const Frame = union(enum) {
    contiguous: Reader,
    chunked: retained_frame.View,

    pub fn digest(self: Frame) retained_frame.Digest {
        return switch (self) {
            .contiguous => |reader| reader.frame_digest,
            .chunked => |view| view.descriptor_digest,
        };
    }
};

pub fn chunkSource(txn: anytype) retained_frame.Source {
    const Txn = @TypeOf(txn.*);
    return .{ .context = txn, .read_chunk = struct {
        fn read(ptr: *anyopaque, sequence: u64, ordinal: u32, out: []u8) !usize {
            const owner: *Txn = @ptrCast(@alignCast(ptr));
            const key = chunkKey(sequence, ordinal);
            if (@hasDecl(Txn, "forkRead")) {
                if (owner.forkRead()) |forked| {
                    var fork = forked;
                    defer fork.abort();
                    const raw = fork.get(&key) catch |err| switch (err) {
                        error.NotFound => return error.RetainedEffectsCorrupt,
                        else => return err,
                    };
                    if (raw.len != out.len) return error.RetainedEffectsCorrupt;
                    @memcpy(out, raw);
                    return out.len;
                } else |err| if (err != error.ReadSnapshotForkUnsupported) return err;
            }
            const raw = owner.get(&key) catch |err| switch (err) {
                error.NotFound => return error.RetainedEffectsCorrupt,
                else => return err,
            };
            if (raw.len != out.len) return error.RetainedEffectsCorrupt;
            @memcpy(out, raw);
            return out.len;
        }
    }.read };
}

/// One retained sequence at the caller's immutable read cut. The caller owns
/// the txn and one reusable 1 MiB cache; a chunked View must not outlive them.
pub fn readFrame(txn: anytype, expected_namespace: Namespace, epoch: u64, pin: [32]u8, after: u64, cache: *retained_frame.View.ChunkCache) !?Frame {
    var state = (try load(txn)) orelse return error.RetainedEffectsFenceMismatch;
    try requireNamespace(txn, state, expected_namespace);
    const value = try state.consumer(epoch, pin);
    if (after < value.start or after < state.reclaimed or after > state.latest) return error.RetainedEffectsCursorMismatch;
    if (after == state.latest) return null;
    const sequence = after + 1;
    const raw = txn.get(&recordKey(sequence)) catch |err| switch (err) {
        error.NotFound => return error.RetainedEffectsCorrupt,
        else => return err,
    };
    if (chunkedRecord(raw)) return .{ .chunked = try retained_frame.View.init(raw, sequence, chunkSource(txn), cache) };
    return .{ .contiguous = try Reader.init(raw, sequence) };
}

/// Returns true only after a whole sequence is gone. For REF5, each call
/// removes bounded authenticated chunks and persists its exact next ordinal;
/// a crash cannot advance `reclaimed` past any surviving value chunk.
fn reclaimOne(txn: anytype, state: *State, sequence: u64, byte_limit: usize, bytes: *usize) !bool {
    const key = recordKey(sequence);
    const raw = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return error.RetainedEffectsCorrupt,
        else => return err,
    };
    if (!chunkedRecord(raw)) {
        if ((try loadGcCursor(txn)) != null) return error.RetainedEffectsCorrupt;
        _ = try Reader.init(raw, sequence);
        if (raw.len > state.retained_bytes) return error.RetainedEffectsCorrupt;
        bytes.* = std.math.add(usize, bytes.*, raw.len) catch return error.RetainedEffectsCorrupt;
        state.retained_bytes -= raw.len;
        try txn.delete(&key);
        state.reclaimed = sequence;
        return true;
    }
    const view = try descriptorView(raw, sequence);
    var cursor = (try loadGcCursor(txn)) orelse GcCursor{ .sequence = sequence, .next_chunk = 0 };
    if (cursor.sequence != sequence or cursor.next_chunk > view.chunk_count) return error.RetainedEffectsCorrupt;
    while (cursor.next_chunk < view.chunk_count and (bytes.* < byte_limit or bytes.* == 0)) {
        const ordinal = cursor.next_chunk;
        const chunk_key = chunkKey(sequence, ordinal);
        const chunk = txn.get(&chunk_key) catch |err| switch (err) {
            error.NotFound => return error.RetainedEffectsCorrupt,
            else => return err,
        };
        const start = @as(u64, ordinal) * retained_frame.chunk_bytes;
        const expected_len: usize = @intCast(@min(retained_frame.chunk_bytes, @as(u64, view.total) - start));
        if (chunk.len != expected_len or chunk.len > state.retained_bytes) return error.RetainedEffectsCorrupt;
        var digest: retained_frame.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(chunk, &digest, .{});
        const expected_digest = try view.chunkHashAt(ordinal);
        if (!std.mem.eql(u8, &digest, &expected_digest)) return error.RetainedEffectsCorrupt;
        try txn.delete(&chunk_key);
        state.retained_bytes -= chunk.len;
        bytes.* = std.math.add(usize, bytes.*, chunk.len) catch return error.RetainedEffectsCorrupt;
        cursor.next_chunk += 1;
    }
    if (cursor.next_chunk != view.chunk_count) {
        try saveGcCursor(txn, cursor);
        return false;
    }
    if (raw.len > state.retained_bytes) return error.RetainedEffectsCorrupt;
    try txn.delete(&key);
    txn.delete(gc_cursor_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    state.retained_bytes -= raw.len;
    bytes.* = std.math.add(usize, bytes.*, raw.len) catch return error.RetainedEffectsCorrupt;
    state.reclaimed = sequence;
    return true;
}

/// Point-addressed reclamation. REF5 deletes at most the requested frame
/// count and byte budget worth of 1 MiB chunks, recording partial progress.
pub fn reclaim(txn: anytype, expected_namespace: Namespace, frame_limit: usize, byte_limit: usize) !usize {
    if (frame_limit == 0 or frame_limit > 128 or byte_limit == 0) return error.InvalidRetainedEffectsAdmission;
    var state = (try load(txn)) orelse return 0;
    try requireNamespace(txn, state, expected_namespace);
    const floor = state.reclaimableThrough();
    var count: usize = 0;
    var bytes: usize = 0;
    while (state.reclaimed < floor and count < frame_limit and bytes < byte_limit) {
        const sequence = state.reclaimed + 1;
        if (try reclaimOne(txn, &state, sequence, byte_limit, &bytes)) count += 1 else break;
    }
    if (bytes != 0) try save(txn, state);
    return count;
}

/// Explicit disposal of copied retention belonging to another logical owner.
/// Ordinary target writes ignore foreign consumers, but their records are not
/// silently rebound or deleted. The authenticated adoption driver names the
/// exact old namespace and reclaims bounded contiguous pages. The final page
/// removes the foreign catalog, permitting a fresh local admission. Same-owner
/// native snapshot transfer must use ordinary consumer-controlled reclamation.
pub fn reclaimForeign(txn: anytype, expected_namespace: Namespace, expected_source: Namespace, frame_limit: usize, byte_limit: usize) !usize {
    if (frame_limit == 0 or frame_limit > 128 or byte_limit == 0) return error.InvalidRetainedEffectsAdmission;
    const current = (try namespace(txn)) orelse return error.RetainedEffectsIdentityRequired;
    if (!std.mem.eql(u8, &current, &expected_namespace)) return error.RetainedEffectsNamespaceMismatch;
    if (std.mem.eql(u8, &current, &expected_source)) return error.RetainedEffectsNamespaceMismatch;
    var state = (try load(txn)) orelse return 0;
    if (!std.mem.eql(u8, &state.namespace, &expected_source)) return error.RetainedEffectsNamespaceMismatch;
    if (try loadReservations(txn)) |reserved| {
        if (!std.mem.eql(u8, &reserved.namespace, &expected_source)) return error.RetainedEffectsNamespaceMismatch;
        // Never discard a copied prepared obligation as if it were GC. The
        // adoption authority must resolve it before changing its namespace.
        if (reserved.bytes != 0 or reserved.oversized != 0) return error.RetainedEffectsFull;
        try txn.delete(reservation_key);
    }
    state.consumers = @splat(.{});
    var count: usize = 0;
    var bytes: usize = 0;
    while (state.reclaimed < state.latest and count < frame_limit and bytes < byte_limit) {
        const sequence = state.reclaimed + 1;
        if (try reclaimOne(txn, &state, sequence, byte_limit, &bytes)) count += 1 else break;
    }
    if (state.reclaimed == state.latest) {
        if (state.retained_bytes != 0) return error.RetainedEffectsCorrupt;
        try txn.delete(state_key);
    } else if (bytes != 0) try save(txn, state);
    return count;
}

const ChunkEmitter = struct {
    sequence: u64,
    buffer: []u8,
    hashes: []retained_frame.Digest,
    used: usize = 0,
    ordinal: u32 = 0,
    logical_hash: std.crypto.hash.sha2.Sha256 = std.crypto.hash.sha2.Sha256.init(.{}),

    fn flush(self: *ChunkEmitter, txn: anytype) !void {
        if (self.used == 0) return;
        if (@as(usize, self.ordinal) >= self.hashes.len) return error.RetainedEffectsCorrupt;
        std.crypto.hash.sha2.Sha256.hash(self.buffer[0..self.used], &self.hashes[self.ordinal], .{});
        try txn.put(&chunkKey(self.sequence, self.ordinal), self.buffer[0..self.used]);
        self.ordinal += 1;
        self.used = 0;
    }

    fn append(self: *ChunkEmitter, txn: anytype, raw: []const u8) !void {
        self.logical_hash.update(raw);
        var pos: usize = 0;
        while (pos < raw.len) {
            const n = @min(self.buffer.len - self.used, raw.len - pos);
            @memcpy(self.buffer[self.used..][0..n], raw[pos..][0..n]);
            self.used += n;
            pos += n;
            if (self.used == self.buffer.len) try self.flush(txn);
        }
    }

    fn finish(self: *ChunkEmitter, txn: anytype) !retained_frame.Digest {
        try self.flush(txn);
        if (@as(usize, self.ordinal) != self.hashes.len) return error.RetainedEffectsCorrupt;
        return self.logical_hash.finalResult();
    }
};

/// Transaction-local coalescing; values are read only once, after all writes.
/// The inactive path neither allocates nor copies a row. DocStore caches the
/// absence of a catalog; admission invalidates that cache before publication.
pub const Capture = struct {
    keys: std.StringHashMapUnmanaged(void) = .empty,
    key_bytes: usize = 0,
    checked: bool = false,
    pin_checked: bool = false,
    has_state: bool = false,
    enabled: bool = false,
    direct_vectors: bool = false,
    graph_artifacts: bool = false,
    raft_marker: bool = false,
    touched_vector: bool = false,
    touched_graph: bool = false,
    touched_primary: bool = false,
    control: bool = false,
    staging: bool = false,
    staged: bool = false,
    poisoned: bool = false,
    pub fn deinit(self: *Capture, alloc: Allocator) void {
        var iter = self.keys.keyIterator();
        while (iter.next()) |key| alloc.free(key.*);
        self.keys.deinit(alloc);
    }
    pub fn touch(self: *Capture, alloc: Allocator, txn: anytype, key: []const u8, primary: bool, cache: ?*std.atomic.Value(u8)) !void {
        if (self.staging) return;
        errdefer self.poisoned = true;
        if (std.mem.eql(u8, key, &internal_keys.raft_document_applied_entry_key)) {
            try @import("source_authority.zig").requireRaftMarkerAllowed(txn);
            self.raft_marker = true;
        }
        if (std.mem.eql(u8, key, &internal_keys.identity_namespace_key)) {
            // A foreign->matching switch after a disabled primary capture
            // would otherwise omit those mutations. Namespace adoption may
            // precede row writes, but cannot follow them while a catalog exists.
            // First namespace persistence with no retention remains unchanged.
            if (self.control or (self.touched_primary and self.has_state)) return error.RetainedEffectsMixedControl;
            self.checked = false;
            self.pin_checked = false;
            return;
        }
        if (std.mem.eql(u8, key, state_key) or std.mem.eql(u8, key, @import("source_pin_state.zig").key) or std.mem.eql(u8, key, @import("source_authority.zig").key)) {
            if (self.staged or self.touched_primary) return error.RetainedEffectsMixedControl;
            self.control = true;
            // Provisioning an owner authority is not retention admission.
            // Preserve the no-retention fast path until an actual catalog or
            // prepared-pin control is written in this transaction.
            if (!std.mem.eql(u8, key, @import("source_authority.zig").key)) if (cache) |value| value.store(2, .release);
            return;
        }
        // Fence metadata-only prepares/decisions and applied watermarks too:
        // they can race the DB's optimistic admission check without touching
        // a primary row. Only explicit retention/pin control transactions may
        // close this gap. The ordinary no-retention path keeps its cached skip.
        if (!self.control and !self.pin_checked) {
            self.pin_checked = true;
            if (cache == null or cache.?.load(.acquire) != 1) {
                if (try namespace(txn)) |current| try @import("source_pin_state.zig").requireNoPrepared(txn, current);
            }
        }
        const vector = @import("db/online_vector_artifacts.zig").isKey(key);
        const graph = @import("db/online_graph_artifacts.zig").isKey(key);
        if (!primary and !internal_keys.isTtlKey(key) and !integrity.isKey(key) and !vector and !graph) return;
        if (graph) {
            // Preserve the old zero-retention graph path. Merely recognizing a
            // graph key must not turn ordinary graph work into primary-capture
            // control or add a durable point read on the catalog-less fast path.
            if (cache) |known| if (known.load(.acquire) == 1) return;
            const graph_state = (try load(txn)) orelse return;
            if (!graph_state.active() or !graph_state.graph_artifacts or !try matchesNamespace(txn, graph_state)) return;
        }
        if (self.staged or self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.control) return error.RetainedEffectsMixedControl;
        self.touched_primary = true;
        if (!self.checked) {
            self.checked = true;
            if (cache == null or cache.?.load(.acquire) != 1) {
                const state = try load(txn);
                self.has_state = state != null;
                self.enabled = if (state) |value| value.active() and try matchesNamespace(txn, value) else false;
                self.direct_vectors = if (state) |value| value.direct_vectors else false;
                self.graph_artifacts = if (state) |value| value.graph_artifacts else false;
                if (self.enabled) try @import("source_pin_state.zig").requireNoPrepared(txn, state.?.namespace);
                if (cache) |value| {
                    if (state != null) value.store(2, .release) else _ = value.cmpxchgStrong(0, 1, .acq_rel, .acquire);
                }
            }
        }
        if (!self.enabled or self.keys.contains(key)) return;
        if (vector and !self.direct_vectors) return;
        if (graph and !self.graph_artifacts) return;
        if (vector) self.touched_vector = true;
        if (graph) self.touched_graph = true;
        // Primary and timestamp sidecar keys coexist until normalization. A
        // legal final frame must not fail because its key is temporarily held
        // twice (especially a near-limit delete with a long binary key).
        if (self.keys.count() >= 2 * max_keys or key.len > 2 * max_frame_bytes - @min(self.key_bytes, 2 * max_frame_bytes))
            return error.RetainedEffectsFull;
        const owned = try alloc.dupe(u8, key);
        errdefer alloc.free(owned);
        try self.keys.put(alloc, owned, {});
        self.key_bytes += key.len;
    }
    pub fn stage(self: *Capture, alloc: Allocator, txn: anytype) !void {
        if (self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.staged or self.keys.count() == 0) return;
        errdefer self.poisoned = true;
        // Direct vectors are ordered by the same Raft entry as their rows.
        // A background/local artifact publication cannot enter this stream.
        if ((self.touched_vector or self.touched_graph) and !self.raft_marker) return error.RetainedEffectsFenceMismatch;
        self.staging = true;
        defer self.staging = false;
        // A timestamp-only refresh is a logical row mutation too. Resolve its
        // primary kind from the final transaction view, not at touch time:
        // callers may write TTL before inserting/deleting the primary row.
        // A timestamp for a missing row produces no phantom deletion record.
        const touched = try alloc.alloc([]const u8, self.keys.count());
        defer alloc.free(touched);
        var touched_iter = self.keys.keyIterator();
        for (touched) |*key| key.* = touched_iter.next().?.*;
        for (touched) |key| if (internal_keys.isTtlKey(key)) {
            const candidate = try alloc.dupe(u8, key);
            errdefer alloc.free(candidate);
            candidate[candidate.len - 1] = internal_keys.primary_kind;
            const document_present = (txn.get(candidate) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) != null;
            candidate[candidate.len - 1] = internal_keys.relational_row_kind;
            const relational_present = (txn.get(candidate) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) != null;
            if (document_present and relational_present) return error.RetainedEffectsCorrupt;
            if (document_present) candidate[candidate.len - 1] = internal_keys.primary_kind;
            const removed = self.keys.fetchRemove(key).?;
            self.key_bytes -= key.len;
            alloc.free(removed.key);
            if ((document_present or relational_present) and !self.keys.contains(candidate)) {
                try self.keys.put(alloc, candidate, {});
                self.key_bytes += candidate.len;
            } else alloc.free(candidate);
        };
        if (self.keys.count() == 0) {
            self.staged = true;
            return;
        }
        if (self.keys.count() > max_keys) return error.RetainedEffectsFull;
        var state = (try load(txn)) orelse return error.RetainedEffectsFenceMismatch;
        try requireNamespace(txn, state, state.namespace);
        if (!state.active()) return error.RetainedEffectsFenceMismatch;
        const sequence = std.math.add(u64, state.latest, 1) catch return error.RetainedEffectsFull;
        const keys = try alloc.alloc([]const u8, self.keys.count());
        defer alloc.free(keys);
        var iter = self.keys.keyIterator();
        for (keys) |*key| key.* = iter.next().?.*;
        std.mem.sort([]const u8, keys, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        const values = try alloc.alloc(?[]const u8, keys.len);
        defer alloc.free(values);
        const timestamps = try alloc.alloc(u64, keys.len);
        defer alloc.free(timestamps);
        var size: usize = 48;
        for (keys, values, timestamps) |key, *value, *timestamp| {
            value.* = txn.get(key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            timestamp.* = 0;
            if (integrity.isKey(key)) {
                _ = integrity.parseKey(key) catch return error.RetainedEffectsCorrupt;
                if (value.*) |raw| _ = integrity.validateTransferRecord(key, raw) catch return error.RetainedEffectsCorrupt;
            } else if (@import("db/online_vector_artifacts.zig").isKey(key)) {
                // This is a new afterimage, not a decoded retained frame.
                // Invalid caller-authored vectors reject the mutation rather
                // than wedging its Raft entry as durable-log corruption.
                @import("db/online_vector_artifacts.zig").validate(key, value.*) catch return error.InvalidBatchRequest;
            } else if (@import("db/online_graph_artifacts.zig").isKey(key)) {
                if (!state.graph_artifacts) return error.RetainedEffectsFenceMismatch;
                @import("db/online_graph_artifacts.zig").validate(key, value.*) catch return error.InvalidBatchRequest;
            } else if (value.* != null) {
                // TTL metadata and the row share this transaction's final
                // view. A later overwrite must never change a retained row's
                // timestamp when its fragment is retried after recovery.
                const timestamp_key = try alloc.dupe(u8, key);
                defer alloc.free(timestamp_key);
                timestamp_key[timestamp_key.len - 1] = internal_keys.ttl_kind;
                const raw_timestamp = txn.get(timestamp_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (raw_timestamp) |raw| {
                    if (raw.len != 8) return error.RetainedEffectsCorrupt;
                    timestamp.* = std.mem.readInt(u64, raw[0..8], .little);
                }
                // Typed query/TTL readers use the packed row metadata. Refuse
                // a sidecar-only typed update before commit rather than retain
                // a tail that would silently change the receiver's version.
                if (internal_keys.isRelationalRowKey(key)) {
                    const packed_timestamp = @import("db/algebraic/relational_row_codec.zig").rowWriteTimestampNsTrusted(value.*.?) catch return error.RetainedEffectsCorrupt;
                    if (packed_timestamp != timestamp.*) return error.RetainedEffectsCorrupt;
                }
            }
            const value_len = if (value.*) |raw| raw.len else 0;
            size = std.math.add(usize, size, 16) catch return error.RetainedEffectsFull;
            size = std.math.add(usize, size, key.len) catch return error.RetainedEffectsFull;
            size = std.math.add(usize, size, value_len) catch return error.RetainedEffectsFull;
            if (size > retained_frame.max_logical_bytes) return error.RetainedEffectsFull;
        }
        // REF3/4 stay inline and allocation-light. REF5 is admitted only
        // after all consumers negotiate support; its immutable chunk records
        // are written in this same primary mutation transaction.
        const chunked = size > max_frame_bytes or state.graph_artifacts;
        if (chunked and !state.chunked_frames) return error.RetainedEffectsFull;
        const descriptor_len = if (chunked) try retained_frame.descriptorLength(@intCast(size), @intCast(keys.len)) else 0;
        const stored_bytes = std.math.add(usize, size, descriptor_len) catch return error.RetainedEffectsFull;
        const reserved = (try loadReservations(txn)) orelse return error.RetainedEffectsCorrupt;
        if (!reserved.complete or reserved.oversized != 0 or !std.mem.eql(u8, &reserved.namespace, &state.namespace)) return error.RetainedEffectsCorrupt;
        if (reserved.bytes > state.limit - state.retained_bytes or stored_bytes > state.limit - state.retained_bytes - reserved.bytes) return error.RetainedEffectsFull;
        if (chunked) {
            const chunk_count = std.math.divCeil(usize, size, retained_frame.chunk_bytes) catch unreachable;
            const hashes = try alloc.alloc(retained_frame.Digest, chunk_count);
            defer alloc.free(hashes);
            const offsets = try alloc.alloc(u32, keys.len);
            defer alloc.free(offsets);
            var header: [16]u8 = undefined;
            @memcpy(header[0..4], if (state.graph_artifacts) "REFG" else if (state.direct_vectors) "REF4" else "REF3");
            std.mem.writeInt(u64, header[4..12], sequence, .little);
            std.mem.writeInt(u32, header[12..16], @intCast(keys.len), .little);
            var payload_hash = std.crypto.hash.sha2.Sha256.init(.{});
            payload_hash.update(&header);
            var frame_offset: usize = header.len;
            for (keys, values, timestamps, 0..) |key, value, timestamp, index| {
                offsets[index] = @intCast(frame_offset);
                var entry: [16]u8 = undefined;
                std.mem.writeInt(u32, entry[0..4], @intCast(key.len), .little);
                std.mem.writeInt(u32, entry[4..8], if (value) |raw| @intCast(raw.len) else std.math.maxInt(u32), .little);
                std.mem.writeInt(u64, entry[8..16], timestamp, .little);
                payload_hash.update(&entry);
                payload_hash.update(key);
                if (value) |raw| payload_hash.update(raw);
                frame_offset += 16 + key.len + if (value) |raw| raw.len else 0;
            }
            if (frame_offset + 32 != size) return error.RetainedEffectsCorrupt;
            const payload_checksum = payload_hash.finalResult();
            const chunk_buffer = try alloc.alloc(u8, retained_frame.chunk_bytes);
            defer alloc.free(chunk_buffer);
            var emitter: ChunkEmitter = .{ .sequence = sequence, .buffer = chunk_buffer, .hashes = hashes };
            try emitter.append(txn, &header);
            for (keys, values, timestamps) |key, value, timestamp| {
                var entry: [16]u8 = undefined;
                std.mem.writeInt(u32, entry[0..4], @intCast(key.len), .little);
                std.mem.writeInt(u32, entry[4..8], if (value) |raw| @intCast(raw.len) else std.math.maxInt(u32), .little);
                std.mem.writeInt(u64, entry[8..16], timestamp, .little);
                try emitter.append(txn, &entry);
                try emitter.append(txn, key);
                if (value) |raw| try emitter.append(txn, raw);
            }
            try emitter.append(txn, &payload_checksum);
            const logical_digest = try emitter.finish(txn);
            const descriptor = try (retained_frame.Descriptor{
                .sequence = sequence,
                .total = @intCast(size),
                .direct_vectors = state.direct_vectors,
                .graph_artifacts = state.graph_artifacts,
                .logical_digest = logical_digest,
                .payload_checksum = payload_checksum,
                .chunk_hashes = hashes,
                .effect_offsets = offsets,
            }).encodeAlloc(alloc);
            defer alloc.free(descriptor);
            if (descriptor.len != descriptor_len) return error.RetainedEffectsCorrupt;
            try txn.put(&recordKey(sequence), descriptor);
        } else {
            const bytes = try alloc.alloc(u8, size);
            defer alloc.free(bytes);
            @memcpy(bytes[0..4], if (state.direct_vectors) "REF4" else "REF3");
            std.mem.writeInt(u64, bytes[4..12], sequence, .little);
            std.mem.writeInt(u32, bytes[12..16], @intCast(keys.len), .little);
            var pos: usize = 16;
            for (keys, values, timestamps) |key, value, timestamp| {
                std.mem.writeInt(u32, bytes[pos..][0..4], @intCast(key.len), .little);
                std.mem.writeInt(u32, bytes[pos + 4 ..][0..4], if (value) |raw| @intCast(raw.len) else std.math.maxInt(u32), .little);
                std.mem.writeInt(u64, bytes[pos + 8 ..][0..8], timestamp, .little);
                pos += 16;
                @memcpy(bytes[pos..][0..key.len], key);
                pos += key.len;
                if (value) |raw| {
                    @memcpy(bytes[pos..][0..raw.len], raw);
                    pos += raw.len;
                }
            }
            @memcpy(bytes[pos..][0..32], &checksum(bytes[0..pos]));
            try txn.put(&recordKey(sequence), bytes);
        }
        state.latest = sequence;
        state.retained_bytes += stored_bytes;
        try save(txn, state);
        try @import("source_authority.zig").advanceCaptured(txn, state.namespace);
        self.staged = true;
    }
};

test "retained effects control codec corruption fails closed" {
    var state: State = .{ .latest = 2, .epoch = 3 };
    state.consumers[0] = .{ .epoch = 3, .pin = @splat(4), .start = 0, .acknowledged = 1 };
    var bytes = encode(state);
    try std.testing.expectEqualDeep(state, try decode(&bytes));
    bytes[20] ^= 1;
    try std.testing.expectError(error.RetainedEffectsCorrupt, decode(&bytes));
    state.consumers[0].acknowledged = 4;
    try std.testing.expectError(error.RetainedEffectsCorrupt, decode(&encode(state)));
}

test "REF5 capability is encoded only after explicit chunked admission" {
    const ordinary: State = .{ .namespace = @splat(1) };
    try std.testing.expectEqual(@as(usize, state_size), encode(ordinary).len);
    const decoded_ordinary = try decode(&encode(ordinary));
    try std.testing.expect(!decoded_ordinary.chunked_frames);
    var chunked = ordinary;
    chunked.chunked_frames = true;
    const raw = encodeChunked(chunked);
    try std.testing.expectEqual(@as(usize, chunked_state_size), raw.len);
    try std.testing.expect((try decode(&raw)).chunked_frames);
    var bad = raw;
    bad[chunked_state_size - 33] = 2;
    try std.testing.expectError(error.RetainedEffectsCorrupt, decode(&bad));
    chunked.direct_vectors = true;
    chunked.graph_artifacts = true;
    const graph = encodeGraph(chunked);
    try std.testing.expectEqual(@as(usize, graph_state_size), graph.len);
    try std.testing.expect((try decode(&graph)).graph_artifacts);
    chunked.direct_vectors = false;
    try std.testing.expectError(error.RetainedEffectsCorrupt, decode(&encodeGraph(chunked)));
}

test "retained reservation work stays constant and inactive mutations make zero probes" {
    const Probe = struct {
        reads: usize = 0,
        writes: usize = 0,
        seeks: usize = 0,
        reserved: ?[77]u8 = null,
        state: ?[state_size]u8 = null,
        const Self = @This();
        pub fn get(self: *Self, key: []const u8) anyerror![]const u8 {
            self.reads += 1;
            if (std.mem.eql(u8, key, &internal_keys.identity_namespace_key)) return &@as(Namespace, @splat(1));
            if (std.mem.eql(u8, key, reservation_key)) return if (self.reserved) |*value| value else error.NotFound;
            if (std.mem.eql(u8, key, state_key)) return if (self.state) |*value| value else error.NotFound;
            return error.NotFound;
        }
        pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
            self.writes += 1;
            if (!std.mem.eql(u8, key, reservation_key) or value.len != 77) return error.UnexpectedWrite;
            self.reserved = value[0..77].*;
        }
        const Cursor = struct {
            owner: *Self,
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), _: []const u8) !?struct { key: []const u8 } {
                self.owner.seeks += 1;
                return null;
            }
        };
        pub fn openCursor(self: *Self) !Cursor {
            return .{ .owner = self };
        }
    };
    var probe: Probe = .{};
    try replaceReservation(&probe, 0, 128);
    try std.testing.expectEqual(@as(usize, 1), probe.seeks);
    probe.reads = 0;
    probe.writes = 0;
    probe.seeks = 0;
    for (0..1000) |_| try replaceReservation(&probe, 128, 128);
    try std.testing.expectEqual(@as(usize, 3000), probe.reads);
    try std.testing.expectEqual(@as(usize, 1000), probe.writes);
    try std.testing.expectEqual(@as(usize, 0), probe.seeks);
    var absent_cache = std.atomic.Value(u8).init(1);
    var inactive: Capture = .{};
    defer inactive.deinit(std.testing.allocator);
    probe.reads = 0;
    for (0..10_000) |_| try inactive.touch(std.testing.allocator, &probe, "row", true, &absent_cache);
    try std.testing.expectEqual(@as(usize, 0), probe.reads);
    try std.testing.expectEqual(@as(usize, 0), inactive.keys.count());
    var state: State = .{ .namespace = @splat(1), .epoch = 1 };
    state.consumers[0] = .{ .epoch = 1, .pin = @splat(2) };
    probe.state = encode(state);
    var active: Capture = .{};
    defer active.deinit(std.testing.allocator);
    for (0..10_000) |_| try active.touch(std.testing.allocator, &probe, "row", true, null);
    try std.testing.expect(probe.reads <= 6);
    try std.testing.expectEqual(@as(usize, 1), active.keys.count());
}

test "retained effects capture allocation failures poison commit and release owned keys" {
    const Fixture = struct {
        const Self = @This();
        alloc: Allocator,
        records: std.StringHashMapUnmanaged([]u8) = .empty,
        fn deinit(self: *@This()) void {
            var iter = self.records.iterator();
            while (iter.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            self.records.deinit(self.alloc);
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.records.get(key) orelse error.NotFound;
        }
        const Cursor = struct {
            owner: *Self,
            fn close(_: *@This()) void {}
            fn seekAtOrAfter(self: *@This(), prefix: []const u8) !?struct { key: []const u8 } {
                var iter = self.owner.records.keyIterator();
                while (iter.next()) |key| if (std.mem.startsWith(u8, key.*, prefix)) return .{ .key = key.* };
                return null;
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            return .{ .owner = self };
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const owned = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(owned);
            if (self.records.getPtr(key)) |old| {
                self.alloc.free(old.*);
                old.* = owned;
            } else {
                const owned_key = try self.alloc.dupe(u8, key);
                errdefer self.alloc.free(owned_key);
                try self.records.put(self.alloc, owned_key, owned);
            }
        }
        fn run(alloc: Allocator) !void {
            var fixture: @This() = .{ .alloc = alloc };
            defer fixture.deinit();
            try fixture.put(&internal_keys.identity_namespace_key, &@as(Namespace, @splat(1)));
            _ = try admit(&fixture, @splat(1), 1, @splat(1), default_limit);
            var capture: Capture = .{};
            defer capture.deinit(alloc);
            const key = try internal_keys.documentKeyAlloc(alloc, "row");
            defer alloc.free(key);
            capture.touch(alloc, &fixture, key, true, null) catch |err| {
                try std.testing.expectError(error.RetainedEffectsTransactionFailed, capture.stage(alloc, &fixture));
                return err;
            };
            try fixture.put(key, "exact final value");
            capture.stage(alloc, &fixture) catch |err| {
                try std.testing.expectError(error.RetainedEffectsTransactionFailed, capture.stage(alloc, &fixture));
                return err;
            };
            var reader = (try read(&fixture, @splat(1), 1, @splat(1), 0)).?;
            try std.testing.expectEqualStrings("exact final value", (try reader.next()).?.value.?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "retained effects reject checksummed non-primary control keys before exposing any effect" {
    const key = state_key;
    var frame: [48 + 16 + key.len]u8 = undefined;
    @memcpy(frame[0..4], "REF3");
    std.mem.writeInt(u64, frame[4..12], 1, .little);
    std.mem.writeInt(u32, frame[12..16], 1, .little);
    std.mem.writeInt(u32, frame[16..20], key.len, .little);
    std.mem.writeInt(u32, frame[20..24], std.math.maxInt(u32), .little);
    std.mem.writeInt(u64, frame[24..32], 0, .little);
    @memcpy(frame[32..][0..key.len], key);
    @memcpy(frame[32 + key.len ..][0..32], &checksum(frame[0 .. 32 + key.len]));
    try std.testing.expectError(error.RetainedEffectsCorrupt, Reader.init(&frame, 1));
}
