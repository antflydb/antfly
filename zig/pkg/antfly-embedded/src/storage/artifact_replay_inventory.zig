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

//! Physical derived-key totals for future replay admission. Counts are exact
//! for reconciled documents; missing legacy facts remain unknown. This is not
//! a prediction of new keys produced by a prepared transaction.
const std = @import("std");
const keys = @import("internal_keys.zig");
const footprint = @import("artifact_footprint.zig");
const revisions = @import("document_mutation_revision.zig");
const read_control = @import("../common/read_admission.zig");
const backend = @import("backend_erased.zig");
pub const marker_key = "\x00\x00__metadata__:artifact_replay_inventory_v1";
const state_prefix = "\x00\x00__metadata__:artifact_replay_keys_v1:";
const progress_prefix = "\x00\x00__metadata__:artifact_replay_scan_v1:";
const max_cursor = 1024 * 1024;

pub const State = struct {
    count: u64 = 0,
    key_bytes: u64 = 0,
    fn valid(self: State) bool {
        return (self.count == 0) == (self.key_bytes == 0) and self.key_bytes >= self.count;
    }
    pub fn encode(self: State) [52]u8 {
        var raw: [52]u8 = undefined;
        @memcpy(raw[0..4], "ARK1");
        std.mem.writeInt(u64, raw[4..12], self.count, .little);
        std.mem.writeInt(u64, raw[12..20], self.key_bytes, .little);
        std.crypto.hash.Blake3.hash(raw[0..20], raw[20..52], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != 52 or !std.mem.eql(u8, raw[0..4], "ARK1")) return error.InvalidArtifactReplayInventory;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..20], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[20..52])) return error.InvalidArtifactReplayInventory;
        const result: State = .{ .count = std.mem.readInt(u64, raw[4..12], .little), .key_bytes = std.mem.readInt(u64, raw[12..20], .little) };
        if (!result.valid()) return error.InvalidArtifactReplayInventory;
        return result;
    }
    /// Conservative CJ2 contribution of retiring this physical inventory.
    /// A removed key can occupy both deleted_doc_keys and changed_artifact_keys.
    /// Graph removals can also name owner/source/target in changed_doc_keys;
    /// their combined decoded lengths fit inside the encoded relationship key.
    /// Charging every physical key for these lanes safely includes non-graph
    /// metadata too. Deduplication may only reduce the actual record.
    /// Request keys, new generated effects and persistence envelopes are extra.
    /// Missing/invalidated inventory must be reconciled, never passed as zero.
    pub fn retirementReplayShape(self: State) !@import("replay_record_shape.zig").Record {
        if (!self.valid()) return error.InvalidArtifactReplayInventory;
        const shape: @import("replay_record_shape.zig").Record = .{
            .changed_doc_keys = .{ .count = std.math.mul(u64, self.count, 3) catch return error.RecordTooLarge, .key_bytes = self.key_bytes },
            .deleted_doc_keys = .{ .count = self.count, .key_bytes = self.key_bytes },
            .changed_artifact_keys = .{ .count = self.count, .key_bytes = self.key_bytes },
        };
        _ = try shape.encodedBytes();
        return shape;
    }

    fn change(self: State, len: usize, present: bool) !State {
        const result: State = if (present) .{
            .count = std.math.add(u64, self.count, 1) catch return error.InvalidArtifactReplayInventory,
            .key_bytes = std.math.add(u64, self.key_bytes, len) catch return error.InvalidArtifactReplayInventory,
        } else .{
            .count = std.math.sub(u64, self.count, 1) catch return error.InvalidArtifactReplayInventory,
            .key_bytes = std.math.sub(u64, self.key_bytes, len) catch return error.InvalidArtifactReplayInventory,
        };
        if (!result.valid()) return error.InvalidArtifactReplayInventory;
        return result;
    }
};

const Marker = struct {
    generation: u64,
    epochs: [4]u64,
    complete: bool,
    fn encode(self: Marker) [80]u8 {
        var raw: [80]u8 = @splat(0);
        @memcpy(raw[0..4], "ARI1");
        std.mem.writeInt(u64, raw[4..12], self.generation, .little);
        for (self.epochs, 0..) |epoch, i| std.mem.writeInt(u64, raw[12 + i * 8 ..][0..8], epoch, .little);
        raw[44] = @intFromBool(self.complete);
        std.crypto.hash.Blake3.hash(raw[0..48], raw[48..80], .{});
        return raw;
    }
    fn decode(raw: []const u8) !Marker {
        if (raw.len != 80 or !std.mem.eql(u8, raw[0..4], "ARI1") or raw[44] > 1 or !std.mem.allEqual(u8, raw[45..48], 0)) return error.InvalidArtifactReplayInventory;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..48], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[48..80])) return error.InvalidArtifactReplayInventory;
        var result: Marker = .{ .generation = std.mem.readInt(u64, raw[4..12], .little), .epochs = undefined, .complete = raw[44] != 0 };
        if (result.generation == 0) return error.InvalidArtifactReplayInventory;
        for (&result.epochs, 0..) |*epoch, i| epoch.* = std.mem.readInt(u64, raw[12 + i * 8 ..][0..8], .little);
        return result;
    }
};
fn loadMarker(txn: anytype) !?Marker {
    const raw = txn.get(marker_key) catch |err| if (err == error.NotFound) return null else return err;
    return try Marker.decode(raw);
}
fn currentMarker(txn: anytype) !?Marker {
    const marker = (try loadMarker(txn)) orelse return null;
    const current = try footprint.load(txn);
    // Old writers and import invalidation advance footprint epochs without
    // refreshing this marker. Their unobserved mutations invalidate all totals.
    return if (std.meta.eql(marker.epochs, current.epochs)) marker else null;
}
pub fn docPrefix(physical: []const u8) ![]const u8 {
    if (!keys.isInternalUserKey(physical)) return error.InvalidArgument;
    const end = (keys.findComponentTerminator(physical, 1) orelse return error.InvalidArgument) + 2;
    return physical[0..end];
}
fn address(comptime prefix: []const u8, generation: u64, physical: []const u8) ![prefix.len + 40]u8 {
    var result: [prefix.len + 40]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.mem.writeInt(u64, result[prefix.len..][0..8], generation, .little);
    std.crypto.hash.Blake3.hash(try docPrefix(physical), result[prefix.len + 8 ..][0..32], .{});
    return result;
}
fn loadState(txn: anytype, marker: Marker, physical: []const u8) !?State {
    const key = try address(state_prefix, marker.generation, physical);
    const raw = txn.get(&key) catch |err| if (err == error.NotFound) return if (marker.complete) State{} else null else return err;
    return try State.decode(raw);
}
/// Physical document/derived key, not a raw public document ID. No payload read.
pub fn loadDocument(txn: anytype, physical: []const u8) !?State {
    const marker = (try currentMarker(txn)) orelse return null;
    return loadState(txn, marker, physical);
}
/// Fixed-size address lookup for a document whose prefix hash is already owned.
pub fn loadDocumentHash(txn: anytype, hash: [32]u8) !?State {
    const marker = (try currentMarker(txn)) orelse return null;
    var key: [state_prefix.len + 40]u8 = undefined;
    @memcpy(key[0..state_prefix.len], state_prefix);
    std.mem.writeInt(u64, key[state_prefix.len..][0..8], marker.generation, .little);
    @memcpy(key[state_prefix.len + 8 ..], &hash);
    const raw = txn.get(&key) catch |err| if (err == error.NotFound) return if (marker.complete) State{} else null else return err;
    return try State.decode(raw);
}
const replay_credit = @import("prepared_replay_credit.zig");
const reader_floors = @import("db/catalog/reader_versions.zig");

pub fn isTrackedKey(candidate: []const u8) bool {
    return footprint.classify(candidate) != null;
}
pub fn isMetadataKey(candidate: []const u8) bool {
    return std.mem.eql(u8, candidate, marker_key) or std.mem.startsWith(u8, candidate, state_prefix) or std.mem.startsWith(u8, candidate, progress_prefix);
}

const PreparedInputs = struct {
    contract: @import("db/prepared_transaction_effects.zig").Contract,
    frame: [32]u8,
};

pub const Capture = struct {
    checked: bool = false,
    marker: ?Marker = null,
    staging: bool = false,
    staged: bool = false,
    poisoned: bool = false,
    touched: std.StringHashMapUnmanaged(void) = .empty,
    prepared_inputs: std.AutoHashMapUnmanaged([16]u8, PreparedInputs) = .empty,
    pub fn deinit(self: *Capture, a: std.mem.Allocator) void {
        var iter = self.touched.keyIterator();
        while (iter.next()) |key| a.free(key.*);
        self.touched.deinit(a);
        self.prepared_inputs.deinit(a);
    }
    /// Retain fixed-size input authority until the writer's final validation.
    /// No borrowed request controls, frame payloads or producer buffers escape.
    pub fn retainPreparedInputs(self: *Capture, a: std.mem.Allocator, expected: @import("db/prepared_transaction_effects.zig").Contract, frame: [32]u8) !void {
        errdefer self.poisoned = true;
        if (self.staged or self.poisoned) return error.InvalidArtifactReplayInventory;
        const entry = try self.prepared_inputs.getOrPut(a, expected.txn_id);
        const selected: PreparedInputs = .{ .contract = expected, .frame = frame };
        if (entry.found_existing) {
            if (!std.meta.eql(entry.value_ptr.*, selected)) return error.PreparedEffectsConflict;
        } else entry.value_ptr.* = selected;
    }
    pub fn touch(self: *Capture, a: std.mem.Allocator, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, candidate: []const u8, present: bool, value: ?[]const u8) !void {
        if (self.staging) return;
        if (replay_credit.isGrant(candidate) or replay_credit.isClaim(candidate) or replay_credit.isOwner(candidate)) {
            errdefer self.poisoned = true;
            try debt.acquireMutationGate(gate);
            if (self.staged or self.poisoned) return error.InvalidHAOutbox;
            if (replay_credit.isGrant(candidate)) {
                _ = try replay_credit.idFromKey(candidate, replay_credit.grant_prefix);
                try self.remember(a, candidate);
            } else if (replay_credit.isClaim(candidate)) {
                if (candidate.len != replay_credit.claim_prefix.len + 48) return error.InvalidHAOutbox;
                const id = candidate[replay_credit.claim_prefix.len..][0..16].*;
                try self.remember(a, &replay_credit.grantKey(id));
            } else {
                if (candidate.len != replay_credit.owner_prefix.len + 32) return error.InvalidHAOutbox;
                const hash = candidate[replay_credit.owner_prefix.len..][0..32].*;
                if (try replay_credit.owner(txn, hash)) |id| try self.remember(a, &replay_credit.grantKey(id));
                if (value) |raw| {
                    if (raw.len != 16) return error.InvalidHAOutbox;
                    try self.remember(a, &replay_credit.grantKey(raw[0..16].*));
                }
                try self.remember(a, candidate);
            }
            return;
        }
        if (std.mem.eql(u8, candidate, reader_floors.minimum_reader_key) or std.mem.eql(u8, candidate, reader_floors.replacement_reader_base_key) or std.mem.eql(u8, candidate, replay_credit.feature_key)) {
            errdefer self.poisoned = true;
            try debt.acquireMutationGate(gate);
            try replay_credit.checkFloorMutation(txn, candidate, value);
            return;
        }
        if (isMetadataKey(candidate)) {
            if (!present and !std.mem.eql(u8, candidate, marker_key)) {
                try debt.acquireMutationGate(gate);
                const current = try currentMarker(txn);
                const prefix_len = if (std.mem.startsWith(u8, candidate, state_prefix)) state_prefix.len else progress_prefix.len;
                if (current == null or (candidate.len == prefix_len + 40 and std.mem.readInt(u64, candidate[prefix_len..][0..8], .little) != current.?.generation)) return;
            }
            self.poisoned = true;
            return error.InvalidArtifactReplayInventory;
        }
        // Invalidation/reconciliation writes share this serialization too:
        // a concurrent certificate update must not be overwritten at commit.
        if (footprint.isKey(candidate)) {
            errdefer self.poisoned = true;
            try debt.acquireMutationGate(gate);
            if (try replay_credit.hasClaims(txn)) return error.Backpressured;
            return;
        }
        if (!isTrackedKey(candidate)) return;
        errdefer self.poisoned = true;
        if (self.staged or self.poisoned) return error.InvalidArtifactReplayInventory;
        // Shared across namespace views and retained until this writer ends.
        try debt.acquireMutationGate(gate);
        if (!self.checked) {
            self.marker = try currentMarker(txn);
            self.checked = true;
        }
        if (self.marker == null) {
            if (try replay_credit.owner(txn, try replay_credit.documentHash(candidate)) != null) return error.InvalidArtifactReplayInventory;
            return;
        }
        try self.remember(a, candidate);
    }
    fn remember(self: *Capture, a: std.mem.Allocator, candidate: []const u8) !void {
        if (self.touched.contains(candidate)) return;
        const owned = try a.dupe(u8, candidate);
        errdefer a.free(owned);
        try self.touched.put(a, owned, {});
    }
    fn putMetadata(self: *Capture, txn: anytype, key: []const u8, raw: []const u8) !void {
        self.staging = true;
        defer self.staging = false;
        try txn.put(key, raw);
    }
    pub fn stage(self: *Capture, txn: anytype, base: ?*backend.Store) !void {
        if (self.poisoned) return error.InvalidArtifactReplayInventory;
        if (self.staged or (self.touched.count() == 0 and self.prepared_inputs.count() == 0)) return;
        errdefer self.poisoned = true;
        var physical_keys: usize = 0;
        var selected_keys = self.touched.keyIterator();
        while (selected_keys.next()) |key| if (isTrackedKey(key.*)) {
            physical_keys += 1;
        };
        if (physical_keys != 0) {
            const selected = self.marker orelse return error.InvalidArtifactReplayInventory;
            const current = (try loadMarker(txn)) orelse return error.InvalidArtifactReplayInventory;
            if (!std.meta.eql(selected, current)) return error.InvalidArtifactReplayInventory;
            const observed = try footprint.load(txn);
            var families: u8 = 0;
            var owners = self.touched.keyIterator();
            while (owners.next()) |physical| if (footprint.classify(physical.*)) |family| {
                families |= footprint.bit(family);
            };
            for (selected.epochs, observed.epochs, 0..) |old, now, i| {
                const expected = if (families & (@as(u8, 1) << @as(u3, @intCast(i))) != 0)
                    std.math.add(u64, old, 1) catch return error.InvalidArtifactReplayInventory
                else
                    old;
                if (now != expected) return error.InvalidArtifactReplayInventory;
            }
            const store = base orelse return error.InvalidArtifactReplayInventory;
            var probe = try store.beginProbeWithBlockCacheAdmission(.transient);
            defer probe.abort();
            var iter = self.touched.keyIterator();
            while (iter.next()) |physical| {
                if (!isTrackedKey(physical.*)) continue;
                // A late Unsupported can be handled by fallback, or ignored by a
                // low-level caller. Only actual staged membership is authoritative.
                const after = try txn.contains(physical.*);
                const before = try probe.contains(physical.*);
                if (before == after) continue;
                const old = (try loadState(txn, selected, physical.*)) orelse {
                    if (try replay_credit.owner(txn, try replay_credit.documentHash(physical.*)) != null) return error.InvalidArtifactReplayInventory;
                    continue;
                };
                const next = try old.change(physical.*.len, after);
                const key = try address(state_prefix, selected.generation, physical.*);
                try self.putMetadata(txn, &key, &next.encode());
                try replay_credit.inventoryChanged(txn, self, try replay_credit.documentHash(physical.*), old, next);
            }
            var next = selected;
            next.epochs = observed.epochs;
            try self.putMetadata(txn, marker_key, &next.encode());
        }
        var validations = self.touched.keyIterator();
        while (validations.next()) |key| {
            if (replay_credit.isGrant(key.*)) try replay_credit.validateGrant(txn, try replay_credit.idFromKey(key.*, replay_credit.grant_prefix));
            if (replay_credit.isOwner(key.*)) try replay_credit.validateOwner(txn, key.*);
        }
        // Reservation-time validation alone cannot exclude later mutations
        // staged by this same writer. Recheck its original authority without
        // rereading the accepted frame or retaining a caller's borrowed controls.
        var inputs = self.prepared_inputs.iterator();
        while (inputs.next()) |input| {
            const grant = (try replay_credit.load(txn, input.key_ptr.*)) orelse continue;
            if (!std.meta.eql(grant.frame, input.value_ptr.frame)) return error.PreparedEffectsMismatch;
            const T = switch (@typeInfo(@TypeOf(txn))) {
                .pointer => |pointer| pointer.child,
                else => @TypeOf(txn),
            };
            if (comptime @hasDecl(T, "openPhysicalCursorAdapter") and @hasField(T, "alloc")) {
                var plan = try @import("transactions.zig").derivePreparedReplayPlanFromTxn(txn.alloc, txn, input.value_ptr.contract, .{}, .{}, null);
                defer plan.deinit();
                if (grant.phase == .prepared) try replay_credit.validateMutationPlan(txn, input.key_ptr.*, plan, null);
            } else return error.InvalidArtifactReplayInventory;
        }
        self.staged = true;
    }
};

/// The absence certificate admits a fully tracked empty root. Existing data
/// starts partial; absent per-document totals then mean unknown, never zero.
/// Invalidated generations get new addresses, so stale counters cannot revive.
pub fn initialize(store: anytype) !void {
    return initializeWithAdmission(store, null);
}

fn initializeWithAdmission(store: anytype, control: ?read_control.Port) !void {
    if (control) |port| try port.checkpoint();
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        if (try currentMarker(&read) != null) {
            if (control) |port| try port.checkpoint();
            return;
        }
    }
    var write = try store.beginWriteTxn();
    errdefer write.abort();
    try write.publication_backlog.acquireMutationGate(store.runtime_store.publication_gate orelse &store.publication_backlog_mutex);
    if (try currentMarker(&write) != null) {
        write.abort();
        return;
    }
    if (try replay_credit.hasClaims(&write)) return error.Backpressured;
    const old = try loadMarker(&write);
    const observed = try footprint.load(&write);
    const marker: Marker = .{
        .generation = if (old) |previous| std.math.add(u64, previous.generation, 1) catch return error.InvalidArtifactReplayInventory else 1,
        .epochs = observed.epochs,
        .complete = observed.certified == footprint.all and observed.present == 0,
    };
    if (control) |port| try port.checkpoint();
    try write.artifact_inventory.putMetadata(&write, marker_key, &marker.encode());
    if (control) |port| try port.checkpoint();
    try write.commit();
}

fn copyControlled(out: []u8, input: []const u8, control: ?read_control.Port) !void {
    var offset: usize = 0;
    while (offset < input.len) {
        if (control) |port| try port.checkpoint();
        const n = @min(4096, input.len - offset);
        @memcpy(out[offset..][0..n], input[offset..][0..n]);
        offset += n;
    }
}
fn hashControlled(input: []const u8, control: ?read_control.Port) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: usize = 0;
    while (offset < input.len) {
        if (control) |port| try port.checkpoint();
        const n = @min(4096, input.len - offset);
        hash.update(input[offset..][0..n]);
        offset += n;
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
const Progress = struct {
    revision: u64,
    state: State = .{},
    cursor: []const u8 = "",
    fn encode(self: Progress, a: std.mem.Allocator, control: ?read_control.Port) ![]u8 {
        if (self.cursor.len > max_cursor) return error.InvalidArtifactReplayInventory;
        const raw = a.alloc(u8, 64 + self.cursor.len) catch |cause| {
            if (control) |port| try port.checkpoint();
            return cause;
        };
        errdefer a.free(raw);
        @memcpy(raw[0..4], "AIRP");
        std.mem.writeInt(u64, raw[4..12], self.revision, .little);
        std.mem.writeInt(u64, raw[12..20], self.state.count, .little);
        std.mem.writeInt(u64, raw[20..28], self.state.key_bytes, .little);
        std.mem.writeInt(u32, raw[28..32], @intCast(self.cursor.len), .little);
        try copyControlled(raw[32 .. raw.len - 32], self.cursor, control);
        raw[raw.len - 32 ..][0..32].* = try hashControlled(raw[0 .. raw.len - 32], control);
        if (control) |port| try port.checkpoint();
        return raw;
    }
    fn decode(raw: []const u8, control: ?read_control.Port) !Progress {
        if (raw.len < 64 or raw.len > 64 + max_cursor or !std.mem.eql(u8, raw[0..4], "AIRP") or std.mem.readInt(u32, raw[28..32], .little) != raw.len - 64) return error.InvalidArtifactReplayInventory;
        const digest = try hashControlled(raw[0 .. raw.len - 32], control);
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.InvalidArtifactReplayInventory;
        const state: State = .{ .count = std.mem.readInt(u64, raw[12..20], .little), .key_bytes = std.mem.readInt(u64, raw[20..28], .little) };
        if (!state.valid()) return error.InvalidArtifactReplayInventory;
        return .{ .revision = std.mem.readInt(u64, raw[4..12], .little), .state = state, .cursor = raw[32 .. raw.len - 32] };
    }
};

/// One keys-only page. No cursor/snapshot survives the call. Document revision,
/// generation and progress CAS fence publication; concurrent mutation restarts
/// the affected document without invalidating unrelated reconciled documents.
pub fn reconcileDocumentPage(a: std.mem.Allocator, store: anytype, physical: []const u8, control: ?read_control.Port) !bool {
    if (control) |port| try port.checkpoint();
    try initializeWithAdmission(store, control);
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const marker = (try currentMarker(&read)) orelse return false;
    if (try loadState(&read, marker, physical) != null) return true;
    const prefix = try docPrefix(physical);
    const revision_key = revisions.keyForPhysical(physical) orelse return error.InvalidArgument;
    const revision = try revisions.load(&read, revision_key);
    const progress_key = try address(progress_prefix, marker.generation, physical);
    const previous = read.get(&progress_key) catch |err| if (err == error.NotFound) null else return err;
    const before = if (previous) |raw| owned: {
        if (raw.len < 64 or raw.len > 64 + max_cursor) return error.InvalidArtifactReplayInventory;
        break :owned a.alloc(u8, raw.len) catch |cause| {
            if (control) |port| try port.checkpoint();
            return cause;
        };
    } else null;
    defer if (before) |owned| a.free(owned);
    if (before) |owned| try copyControlled(owned, previous.?, control);
    var progress: Progress = if (before) |raw| try Progress.decode(raw, control) else .{ .revision = revision };
    if (progress.revision != revision) progress = .{ .revision = revision };
    if (progress.cursor.len != 0 and !std.mem.startsWith(u8, progress.cursor, prefix)) return error.InvalidArtifactReplayInventory;
    var last: std.ArrayList(u8) = .empty;
    defer last.deinit(a);
    var count: usize = 0;
    var bytes: usize = 0;
    var complete = false;
    {
        var cursor = try read.openPhysicalKeyCursorAdapter();
        defer cursor.close();
        var item = try cursor.seekAtOrAfter(if (progress.cursor.len == 0) prefix else progress.cursor);
        if (item) |row| {
            if (progress.cursor.len != 0 and std.mem.eql(u8, row.key, progress.cursor)) item = try cursor.next();
        }
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        while (item) |row| {
            if (control) |port| try port.checkpoint();
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            if (count != 0 and (count >= 128 or bytes >= 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
            if (row.key.len > max_cursor) return error.InvalidArtifactReplayInventory;
            if (isTrackedKey(row.key)) progress.state = try progress.state.change(row.key.len, true);
            last.resize(a, row.key.len) catch |cause| {
                if (control) |port| try port.checkpoint();
                return cause;
            };
            try copyControlled(last.items, row.key, control);
            count += 1;
            bytes += row.key.len;
            item = try cursor.next();
        }
        complete = if (item) |row| !std.mem.startsWith(u8, row.key, prefix) else true;
    }
    if (count != 0) progress.cursor = last.items;
    const after = try progress.encode(a, control);
    defer a.free(after);
    if (control) |port| try port.checkpoint();
    var write = try store.beginWriteTxn();
    errdefer write.abort();
    try write.publication_backlog.acquireMutationGate(store.runtime_store.publication_gate orelse &store.publication_backlog_mutex);
    const current = try currentMarker(&write);
    const now = write.get(&progress_key) catch |err| if (err == error.NotFound) null else return err;
    const same_progress = if (before) |old| if (now) |raw| std.mem.eql(u8, old, raw) else false else now == null;
    if (current == null or current.?.generation != marker.generation or !same_progress or try revisions.load(&write, revision_key) != revision) {
        write.abort();
        return false;
    }
    if (control) |port| try port.checkpoint();
    if (complete) {
        const state_key = try address(state_prefix, marker.generation, physical);
        try write.artifact_inventory.putMetadata(&write, &state_key, &progress.state.encode());
        write.artifact_inventory.staging = true;
        defer write.artifact_inventory.staging = false;
        write.delete(&progress_key) catch |err| if (err != error.NotFound) return err;
    } else try write.artifact_inventory.putMetadata(&write, &progress_key, after);
    try write.commit();
    return complete;
}

test "artifact replay inventory checksums arithmetic and controlled large cursor retirement" {
    const a = std.testing.allocator;
    var marker_raw = (Marker{ .generation = 1, .epochs = .{ 1, 2, 3, 4 }, .complete = true }).encode();
    try std.testing.expectEqual(@as(u64, 1), (try Marker.decode(&marker_raw)).generation);
    marker_raw[12] ^= 1;
    try std.testing.expectError(error.InvalidArtifactReplayInventory, Marker.decode(&marker_raw));
    const invalid_marker = (Marker{ .generation = 0, .epochs = @splat(0), .complete = false }).encode();
    try std.testing.expectError(error.InvalidArtifactReplayInventory, Marker.decode(&invalid_marker));
    const state: State = .{ .count = 3, .key_bytes = 99 };
    var encoded = state.encode();
    try std.testing.expectEqualDeep(state, try State.decode(&encoded));
    encoded[12] ^= 1;
    try std.testing.expectError(error.InvalidArtifactReplayInventory, State.decode(&encoded));
    try std.testing.expectError(error.InvalidArtifactReplayInventory, (State{}).change(3, false));
    try std.testing.expectError(error.InvalidArtifactReplayInventory, (State{ .count = 1, .key_bytes = 3 }).change(2, false));
    try std.testing.expectError(error.InvalidArtifactReplayInventory, (State{ .count = 1, .key_bytes = std.math.maxInt(u64) }).change(1, true));
    const large = try a.alloc(u8, max_cursor);
    defer a.free(large);
    @memset(large, 17);
    const progress: Progress = .{ .revision = 9, .state = state, .cursor = large };
    const Control = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return error.DeadlineExceeded;
        }
    };
    var control: Control = .{};
    const payload: read_control.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
    const port = read_control.Port.init(&payload);
    const raw = try progress.encode(a, port);
    defer a.free(raw);
    const checks = control.checks;
    try std.testing.expect(checks > 500);
    const decoded = try Progress.decode(raw, null);
    try std.testing.expectEqual(progress.revision, decoded.revision);
    try std.testing.expectEqualDeep(state, decoded.state);
    try std.testing.expectEqualSlices(u8, large, decoded.cursor);
    // Exercise copy, hash and final ownership refusal without quadratic work.
    for ([_]usize{ 1, 2, 128, 256, checks - 1, checks }) |reject| {
        control = .{ .reject = reject };
        try std.testing.expectError(error.DeadlineExceeded, progress.encode(a, port));
        try std.testing.expectEqual(reject, control.checks);
    }
    raw[40] ^= 1;
    try std.testing.expectError(error.InvalidArtifactReplayInventory, Progress.decode(raw, null));
}

test "artifact replay inventory capture owns all allocation failures and refuses metadata forgery" {
    const Case = struct {
        const Fake = struct {
            marker: [80]u8,
            footprint_raw: [80]u8,
            const EmptyCursor = struct {
                const Row = struct { key: []const u8 };
                pub fn close(_: *@This()) void {}
                pub fn setKeysOnly(_: *@This()) void {}
                pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Row {
                    return null;
                }
                pub fn next(_: *@This()) !?Row {
                    return null;
                }
            };
            pub fn openCursor(_: *@This()) !EmptyCursor {
                return .{};
            }
            pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
                if (std.mem.eql(u8, key, marker_key)) return &self.marker;
                try std.testing.expectEqualStrings(footprint.key, key);
                return &self.footprint_raw;
            }
            pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {
                return error.TestUnexpectedInventoryWrite;
            }
            pub fn contains(_: *@This(), _: []const u8) anyerror!bool {
                return error.TestUnexpectedInventoryRead;
            }
        };
        fn run(a: std.mem.Allocator) !void {
            var capture: Capture = .{};
            defer capture.deinit(a);
            var debt: @import("publication_backlog.zig").Capture = .{};
            defer debt.release();
            var gate: std.atomic.Mutex = .unlocked;
            var txn: Fake = .{
                .marker = (Marker{ .generation = 1, .epochs = @splat(0), .complete = true }).encode(),
                .footprint_raw = (footprint.State{ .certified = footprint.all }).encode(),
            };
            const one = "\x01owner\x00\x00\x20artifact-one";
            const two = "\x01owner\x00\x00\x20artifact-two";
            for ([_][]const u8{ one, two, one }) |key| capture.touch(a, &txn, &debt, &gate, key, true, "x") catch |cause| {
                try std.testing.expectError(error.InvalidArtifactReplayInventory, capture.stage(&txn, null));
                return cause;
            };
            try std.testing.expectEqual(@as(usize, 2), capture.touched.count());
            try std.testing.expectError(error.InvalidArtifactReplayInventory, capture.touch(a, &txn, &debt, &gate, marker_key, true, "x"));
            try std.testing.expectError(error.InvalidArtifactReplayInventory, capture.stage(&txn, null));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "artifact replay inventory retirement shape preserves additive growth and rejects overflow" {
    const first: State = .{ .count = 2, .key_bytes = 20 };
    const second: State = .{ .count = 3, .key_bytes = 45 };
    const combined: State = .{ .count = 5, .key_bytes = 65 };
    const shape = try (try first.retirementReplayShape()).plus(try second.retirementReplayShape());
    try std.testing.expectEqualDeep(try combined.retirementReplayShape(), shape);
    try std.testing.expect((try shape.encodedBytes()) > (try (try first.retirementReplayShape()).encodedBytes()));
    try std.testing.expectEqual(@as(u64, 16), try (try (State{}).retirementReplayShape()).encodedBytes());
    try std.testing.expectError(error.InvalidArtifactReplayInventory, (State{ .key_bytes = 1 }).retirementReplayShape());
    const too_many = @as(u64, std.math.maxInt(u32)) / 3 + 1;
    try std.testing.expectError(error.RecordTooLarge, (State{ .count = too_many, .key_bytes = too_many }).retirementReplayShape());
    try std.testing.expectError(error.RecordTooLarge, (State{ .count = 1, .key_bytes = std.math.maxInt(u64) }).retirementReplayShape());
}
