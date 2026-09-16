// Copyright 2026 Antfly, Inc.
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

//! Shared source-side row/integrity after-image retention. Control operations and captured
//! effects MUST commit in the same store transaction as their respective state.
//! This is a tail substrate, not snapshot certification: admission callers still
//! need an authenticated, transferable immutable source cut before releasing it.
//! Physical row envelopes retain their schema version; consumers must possess
//! that immutable schema registry. Derived indexes/artifacts are not this log.
const std = @import("std");
const internal_keys = @import("internal_keys.zig");
const integrity = @import("db/relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;
pub const state_key = "\x00\x00__retained_rows__:state";
const record_prefix = "\x00\x00__retained_rows__:record:";
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
const state_size = 4 + 24 + 5 * 8 + max_consumers * 56 + 32;

fn checksum(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn encode(state: State) [state_size]u8 {
    var bytes: [state_size]u8 = undefined;
    @memcpy(bytes[0..4], "RER2");
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
    @memcpy(bytes[pos..][0..32], &checksum(bytes[0..pos]));
    return bytes;
}
pub fn decode(bytes: []const u8) !State {
    if (bytes.len != state_size or !std.mem.eql(u8, bytes[0..4], "RER2") or
        !std.mem.eql(u8, bytes[state_size - 32 ..], &checksum(bytes[0 .. state_size - 32])))
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
    try txn.put(state_key, &encode(state));
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
    if (epoch == 0 or std.mem.eql(u8, &pin, &@as([32]u8, @splat(0))) or limit < max_frame_bytes)
        return error.InvalidRetainedEffectsAdmission;
    const current = (try namespace(txn)) orelse return error.RetainedEffectsIdentityRequired;
    if (!std.mem.eql(u8, &current, &expected_namespace)) return error.RetainedEffectsNamespaceMismatch;
    var state = (try load(txn)) orelse State{ .namespace = current, .limit = limit };
    try requireNamespace(txn, state, expected_namespace);
    if (state.limit != limit) return error.InvalidRetainedEffectsAdmission;
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
    };
    pub fn init(bytes: []const u8, sequence: u64) !Reader {
        if (bytes.len < 48 or bytes.len > max_frame_bytes or !std.mem.eql(u8, bytes[0..4], "REF3") or
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
    return try Reader.init(raw, sequence);
}

/// Reads only point-addressed contiguous frames. No full log scan or sorted
/// catalog allocation is needed. Budget may admit one oversized frame to make
/// progress, but every frame is independently bounded to max_frame_bytes.
pub fn reclaim(txn: anytype, expected_namespace: Namespace, frame_limit: usize, byte_limit: usize) !usize {
    if (frame_limit == 0 or frame_limit > 128 or byte_limit == 0) return error.InvalidRetainedEffectsAdmission;
    var state = (try load(txn)) orelse return 0;
    try requireNamespace(txn, state, expected_namespace);
    const floor = state.reclaimableThrough();
    var count: usize = 0;
    var bytes: usize = 0;
    while (state.reclaimed < floor and count < frame_limit and bytes < byte_limit) {
        const sequence = state.reclaimed + 1;
        const key = recordKey(sequence);
        const raw = txn.get(&key) catch |err| switch (err) {
            error.NotFound => return error.RetainedEffectsCorrupt,
            else => return err,
        };
        _ = try Reader.init(raw, sequence);
        if (raw.len > state.retained_bytes) return error.RetainedEffectsCorrupt;
        bytes += raw.len;
        state.retained_bytes -= raw.len;
        try txn.delete(&key);
        state.reclaimed = sequence;
        count += 1;
    }
    if (count != 0) try save(txn, state);
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
        const key = recordKey(sequence);
        const raw = txn.get(&key) catch |err| switch (err) {
            error.NotFound => return error.RetainedEffectsCorrupt,
            else => return err,
        };
        _ = try Reader.init(raw, sequence);
        if (raw.len > state.retained_bytes) return error.RetainedEffectsCorrupt;
        bytes += raw.len;
        state.retained_bytes -= raw.len;
        try txn.delete(&key);
        state.reclaimed = sequence;
        count += 1;
    }
    if (state.reclaimed == state.latest) {
        if (state.retained_bytes != 0) return error.RetainedEffectsCorrupt;
        try txn.delete(state_key);
    } else if (count != 0) try save(txn, state);
    return count;
}

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
        if (std.mem.eql(u8, key, &internal_keys.raft_document_applied_entry_key))
            try @import("source_authority.zig").requireRaftMarkerAllowed(txn);
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
        if (!primary and !internal_keys.isTtlKey(key) and !integrity.isKey(key)) return;
        if (self.staged or self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.control) return error.RetainedEffectsMixedControl;
        self.touched_primary = true;
        if (!self.checked) {
            self.checked = true;
            if (cache == null or cache.?.load(.acquire) != 1) {
                const state = try load(txn);
                self.has_state = state != null;
                self.enabled = if (state) |value| value.active() and try matchesNamespace(txn, value) else false;
                if (self.enabled) try @import("source_pin_state.zig").requireNoPrepared(txn, state.?.namespace);
                if (cache) |value| {
                    if (state != null) value.store(2, .release) else _ = value.cmpxchgStrong(0, 1, .acq_rel, .acquire);
                }
            }
        }
        if (!self.enabled or self.keys.contains(key)) return;
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
            if (size > max_frame_bytes) return error.RetainedEffectsFull;
        }
        const bytes = try alloc.alloc(u8, size);
        defer alloc.free(bytes);
        @memcpy(bytes[0..4], "REF3");
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
        const reserved = (try loadReservations(txn)) orelse return error.RetainedEffectsCorrupt;
        if (!reserved.complete or reserved.oversized != 0 or !std.mem.eql(u8, &reserved.namespace, &state.namespace)) return error.RetainedEffectsCorrupt;
        if (reserved.bytes > state.limit - state.retained_bytes or size > state.limit - state.retained_bytes - reserved.bytes) return error.RetainedEffectsFull;
        state.latest = sequence;
        state.retained_bytes += size;
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
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
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
