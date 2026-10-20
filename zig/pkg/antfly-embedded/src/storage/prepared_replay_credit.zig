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

//! Durable, per-transaction HA replay retention credits. These credits do not
//! fund derived-log RAM, mandatory providers or an executable recovery bundle.
//! The caller must bind a real accepted frame and include all affected roots.
const std = @import("std");
const shape = @import("replay_record_shape.zig");
const inventory = @import("artifact_replay_inventory.zig");
const backlog = @import("publication_backlog.zig");
const floors = @import("db/catalog/reader_versions.zig");
const control = @import("../common/read_admission.zig");
pub const grant_prefix = "\x00\x00__txn_replay_credit_v1__:";
pub const claim_prefix = "\x00\x00__txn_replay_claim_v1__:";
pub const owner_prefix = "\x00\x00__metadata__:replay_credit_owner_v1:";
pub const replay_prefix = "\x00\x00__txn_ha_replay_outbox__:";
pub const feature_key = "\x00\x00__metadata__:replay_credit_protocol_v1";
pub const Id = [16]u8;
pub const Hash = [32]u8;
pub const Phase = enum(u8) { prepared, materialized };
pub fn grantKey(id: Id) [grant_prefix.len + 16]u8 {
    return grant_prefix.* ++ id;
}
pub fn claimKey(id: Id, hash: Hash) [claim_prefix.len + 48]u8 {
    return claim_prefix.* ++ id ++ hash;
}
pub fn ownerKey(hash: Hash) [owner_prefix.len + 32]u8 {
    return owner_prefix.* ++ hash;
}
pub fn isGrant(key: []const u8) bool {
    return std.mem.startsWith(u8, key, grant_prefix);
}
pub fn isClaim(key: []const u8) bool {
    return std.mem.startsWith(u8, key, claim_prefix);
}
pub fn isOwner(key: []const u8) bool {
    return std.mem.startsWith(u8, key, owner_prefix);
}
pub fn isReplay(key: []const u8) bool {
    return std.mem.startsWith(u8, key, replay_prefix);
}
pub fn isObligation(key: []const u8) bool {
    return isGrant(key) or isClaim(key) or isOwner(key) or isReplay(key);
}
pub fn idFromKey(key: []const u8, prefix: []const u8) !Id {
    if (!std.mem.startsWith(u8, key, prefix) or key.len != prefix.len + 16) return error.InvalidHAOutbox;
    return key[prefix.len..][0..16].*;
}
fn readOptional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) null else return err;
}
pub fn requireFloors(txn: anytype) !void {
    for ([_][]const u8{ floors.minimum_reader_key, floors.replacement_reader_base_key }) |key| {
        const raw = (try readOptional(txn, key)) orelse return error.PreparedTransactionProtocolDisabled;
        const version = std.fmt.parseInt(u32, raw, 10) catch return error.InvalidHAOutbox;
        if (version < floors.replay_credit_reader_version or version > floors.supported_reader_version) return error.PreparedTransactionProtocolDisabled;
    }
}
pub fn requireProtocol(txn: anytype) !void {
    try requireFloors(txn);
    const feature = (try readOptional(txn, feature_key)) orelse return error.PreparedTransactionProtocolDisabled;
    if (!std.mem.eql(u8, feature, "RPC1")) return error.InvalidHAOutbox;
}
fn inventoryOwned(txn: anytype) bool {
    const T = switch (@typeInfo(@TypeOf(txn))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(txn),
    };
    if (comptime @hasField(T, "artifact_inventory")) return txn.artifact_inventory.staging;
    return false;
}
pub fn checkGrantMutation(txn: anytype, id: Id, next: Grant) !void {
    try requireProtocol(txn);
    if (try load(txn, id)) |previous| {
        if (!std.meta.eql(previous.owner, next.owner) or !std.meta.eql(previous.frame, next.frame) or !std.meta.eql(previous.roots, next.roots) or previous.root_count != next.root_count or !std.meta.eql(previous.base, next.base)) return error.PreparedEffectsConflict;
        if (previous.phase == .materialized) {
            if (!std.meta.eql(previous, next)) return error.PreparedEffectsConflict;
        } else if (next.phase == .materialized or !inventoryOwned(txn)) {
            if (!std.meta.eql(previous.current, next.current)) return error.PreparedEffectsConflict;
        }
    } else {
        if (next.phase != .prepared) return error.InvalidHAOutbox;
        const raw = (try readOptional(txn, &(backlog.prepared_effects_prefix.* ++ id))) orelse return error.PreparedEffectsRequired;
        const inspected = try @import("db/prepared_transaction_effects.zig").inspect(raw, null);
        if (!std.meta.eql(inspected.contract.txn_id, id) or !std.meta.eql(inspected.fingerprint, next.frame)) return error.PreparedEffectsMismatch;
    }
}
pub fn checkFloorMutation(txn: anytype, key: []const u8, value: ?[]const u8) !void {
    if (std.mem.eql(u8, key, feature_key)) {
        if (value == null or !std.mem.eql(u8, value.?, "RPC1")) return error.InvalidHAOutbox;
        return requireFloors(txn);
    }
    if (try readOptional(txn, feature_key) != null) {
        const raw = value orelse return error.InvalidHAOutbox;
        const version = std.fmt.parseInt(u32, raw, 10) catch return error.InvalidHAOutbox;
        if (version < floors.replay_credit_reader_version or version > floors.supported_reader_version) return error.InvalidHAOutbox;
    }
}
fn encodeShape(raw: []u8, value: shape.Record) void {
    inline for (comptime std.meta.fieldNames(shape.Record), 0..) |name, i| {
        std.mem.writeInt(u64, raw[i * 16 ..][0..8], @field(value, name).count, .little);
        std.mem.writeInt(u64, raw[i * 16 + 8 ..][0..8], @field(value, name).key_bytes, .little);
    }
}
fn decodeShape(raw: []const u8) !shape.Record {
    var value: shape.Record = .{};
    inline for (comptime std.meta.fieldNames(shape.Record), 0..) |name, i| @field(value, name) = .{ .count = std.mem.readInt(u64, raw[i * 16 ..][0..8], .little), .key_bytes = std.mem.readInt(u64, raw[i * 16 + 8 ..][0..8], .little) };
    _ = try value.encodedBytes();
    return value;
}
fn digest(raw: []const u8) Hash {
    var out: Hash = undefined;
    std.crypto.hash.Blake3.hash(raw, &out, .{});
    return out;
}
pub const Grant = struct {
    owner: Id,
    frame: Hash,
    roots: Hash,
    root_count: u64,
    base: shape.Record,
    current: shape.Record,
    phase: Phase = .prepared,
    payload: Hash = @splat(0),
    pub const bytes = 288;
    pub fn budget(self: Grant) !u64 {
        return (try self.base.plus(self.current)).encodedBytes();
    }
    pub fn encode(self: Grant) ![bytes]u8 {
        _ = try self.budget();
        if (self.root_count > backlog.default_max_records) return error.InvalidHAOutbox;
        if (self.phase == .prepared and !std.mem.allEqual(u8, &self.payload, 0)) return error.InvalidHAOutbox;
        var raw: [bytes]u8 = @splat(0);
        @memcpy(raw[0..4], "RCG1");
        std.mem.writeInt(u16, raw[4..6], 1, .little);
        raw[6] = @backingInt(self.phase);
        @memcpy(raw[8..24], &self.owner);
        @memcpy(raw[24..56], &self.frame);
        @memcpy(raw[56..88], &self.roots);
        std.mem.writeInt(u64, raw[88..96], self.root_count, .little);
        encodeShape(raw[96..160], self.base);
        encodeShape(raw[160..224], self.current);
        @memcpy(raw[224..256], &self.payload);
        @memcpy(raw[256..288], &digest(raw[0..256]));
        return raw;
    }
    pub fn decode(raw: []const u8) !Grant {
        if (raw.len != bytes or !std.mem.eql(u8, raw[0..4], "RCG1") or std.mem.readInt(u16, raw[4..6], .little) != 1 or raw[7] != 0 or !std.mem.eql(u8, &digest(raw[0..256]), raw[256..288])) return error.InvalidHAOutbox;
        const value: Grant = .{ .owner = raw[8..24].*, .frame = raw[24..56].*, .roots = raw[56..88].*, .root_count = std.mem.readInt(u64, raw[88..96], .little), .base = decodeShape(raw[96..160]) catch return error.InvalidHAOutbox, .current = decodeShape(raw[160..224]) catch return error.InvalidHAOutbox, .phase = std.enums.fromInt(Phase, raw[6]) orelse return error.InvalidHAOutbox, .payload = raw[224..256].* };
        _ = value.encode() catch return error.InvalidHAOutbox;
        return value;
    }
};
const Claim = struct {
    owner: Id,
    frame: Hash,
    state: inventory.State,
    fn encode(self: Claim) [100]u8 {
        var raw: [100]u8 = undefined;
        @memcpy(raw[0..4], "RCL1");
        @memcpy(raw[4..20], &self.owner);
        @memcpy(raw[20..52], &self.frame);
        std.mem.writeInt(u64, raw[52..60], self.state.count, .little);
        std.mem.writeInt(u64, raw[60..68], self.state.key_bytes, .little);
        @memcpy(raw[68..100], &digest(raw[0..68]));
        return raw;
    }
    fn decode(raw: []const u8) !Claim {
        if (raw.len != 100 or !std.mem.eql(u8, raw[0..4], "RCL1") or !std.mem.eql(u8, &digest(raw[0..68]), raw[68..100])) return error.InvalidHAOutbox;
        const value: Claim = .{ .owner = raw[4..20].*, .frame = raw[20..52].*, .state = .{ .count = std.mem.readInt(u64, raw[52..60], .little), .key_bytes = std.mem.readInt(u64, raw[60..68], .little) } };
        _ = value.state.retirementReplayShape() catch return error.InvalidHAOutbox;
        return value;
    }
};
pub fn load(txn: anytype, id: Id) !?Grant {
    const raw = (try readOptional(txn, &grantKey(id))) orelse return null;
    const value = try Grant.decode(raw);
    if (!std.meta.eql(value.owner, id)) return error.InvalidHAOutbox;
    return value;
}
pub fn documentHash(physical: []const u8) !Hash {
    return digest(try inventory.docPrefix(physical));
}
pub fn owner(txn: anytype, hash: Hash) !?Id {
    const raw = (try readOptional(txn, &ownerKey(hash))) orelse return null;
    if (raw.len != 16) return error.InvalidHAOutbox;
    return raw[0..16].*;
}
pub fn hasClaims(txn: anytype) !bool {
    var cursor = try txn.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    const row = (try cursor.seekAtOrAfter(claim_prefix)) orelse return false;
    return std.mem.startsWith(u8, row.key, claim_prefix);
}
fn less(_: void, a: Hash, b: Hash) bool {
    return std.mem.order(u8, &a, &b) == .lt;
}
/// Caller owns this DocStore writer and original bounded metadata allocator.
/// Failures poison capture even if a low-level caller ignores the error.
pub fn reserve(txn: anytype, a: std.mem.Allocator, capture: *inventory.Capture, id: Id, frame: Hash, base: shape.Record, physical: []const []const u8, original: ?control.Port) !void {
    errdefer capture.poisoned = true;
    try requireFloors(txn);
    const frame_raw = (try readOptional(txn, &(backlog.prepared_effects_prefix.* ++ id))) orelse return error.PreparedEffectsRequired;
    const accepted = try @import("db/prepared_transaction_effects.zig").inspect(frame_raw, original);
    if (!std.meta.eql(accepted.contract.txn_id, id) or !std.meta.eql(accepted.fingerprint, frame)) return error.PreparedEffectsMismatch;
    if (physical.len > backlog.default_max_records) return error.Backpressured;
    const hashes = try a.alloc(Hash, physical.len);
    defer a.free(hashes);
    for (physical, 0..) |key, i| {
        if (original) |port| try port.checkpoint();
        hashes[i] = try documentHash(key);
    }
    std.mem.sort(Hash, hashes, {}, less);
    var count: usize = 0;
    for (hashes) |hash| {
        if (count != 0 and std.meta.eql(hashes[count - 1], hash)) continue;
        hashes[count] = hash;
        count += 1;
    }
    var roots_hash = std.crypto.hash.Blake3.init(.{});
    for (hashes[0..count]) |hash| roots_hash.update(&hash);
    var roots: Hash = undefined;
    roots_hash.final(&roots);
    if (try load(txn, id)) |previous| {
        if (!std.meta.eql(previous.frame, frame) or !std.meta.eql(previous.roots, roots) or previous.root_count != count or !std.meta.eql(previous.base, base) or previous.phase != .prepared) return error.PreparedEffectsConflict;
        // Queue final-view validation even when a retry does not change debt.
        try txn.put(&grantKey(id), &try previous.encode());
        return;
    }
    var current: shape.Record = .{};
    // Validate every root before publishing any claim. The same ordered writer
    // view is revalidated at commit after all physical inventory deltas stage.
    for (hashes[0..count]) |hash| {
        if (original) |port| try port.checkpoint();
        if (try owner(txn, hash) != null) return error.Backpressured;
        const fact = (try inventory.loadDocumentHash(txn, hash)) orelse return error.InvalidArtifactReplayInventory;
        current = try current.plus(try fact.retirementReplayShape());
    }
    const grant: Grant = .{ .owner = id, .frame = frame, .roots = roots, .root_count = count, .base = base, .current = current };
    try txn.put(feature_key, "RPC1");
    try txn.put(&grantKey(id), &try grant.encode());
    for (hashes[0..count]) |hash| {
        if (original) |port| try port.checkpoint();
        const fact = (try inventory.loadDocumentHash(txn, hash)) orelse return error.InvalidArtifactReplayInventory;
        try txn.put(&claimKey(id, hash), &(Claim{ .owner = id, .frame = frame, .state = fact }).encode());
        try txn.put(&ownerKey(hash), &id);
    }
    if (original) |port| try port.checkpoint();
}
/// Final writer-view proof. No payload scan and no independent resource grant.
pub fn validateGrant(txn: anytype, id: Id) !void {
    const grant = try load(txn, id);
    if (grant) |selected| {
        try requireProtocol(txn);
        if (selected.phase == .prepared) {
            // Creation inspected the complete frame, and its mutation hook
            // preserves that identity. Do not reread a cold frame at resolution.
            if (!try txn.contains(&(backlog.prepared_effects_prefix.* ++ id))) return error.PreparedEffectsRequired;
        } else if (!try txn.contains(&(replay_prefix.* ++ id))) return error.InvalidHAOutbox;
    }
    var cursor = try txn.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    const prefix = claim_prefix.* ++ id;
    var next = try cursor.seekAtOrAfter(&prefix);
    var count: u64 = 0;
    var total: shape.Record = .{};
    var hasher = std.crypto.hash.Blake3.init(.{});
    while (next) |row| {
        if (!std.mem.startsWith(u8, row.key, &prefix)) break;
        if (row.key.len != claim_prefix.len + 48) return error.InvalidHAOutbox;
        const hash = row.key[claim_prefix.len + 16 ..][0..32].*;
        const selected = grant orelse return error.InvalidHAOutbox;
        if (selected.phase != .prepared) return error.InvalidHAOutbox;
        const raw = (try readOptional(txn, &claimKey(id, hash))) orelse return error.InvalidHAOutbox;
        const claim = try Claim.decode(raw);
        const claimed_owner = (try owner(txn, hash)) orelse return error.InvalidHAOutbox;
        if (!std.meta.eql(claim.owner, id) or !std.meta.eql(claim.frame, selected.frame) or !std.meta.eql(claimed_owner, id)) return error.InvalidHAOutbox;
        const fact = (try inventory.loadDocumentHash(txn, hash)) orelse return error.InvalidArtifactReplayInventory;
        if (!std.meta.eql(fact, claim.state)) return error.InvalidArtifactReplayInventory;
        total = try total.plus(try fact.retirementReplayShape());
        count += 1;
        if (count > selected.root_count) return error.InvalidHAOutbox;
        hasher.update(&hash);
        next = try cursor.next();
    }
    if (grant == null and try txn.contains(&(replay_prefix.* ++ id))) return error.InvalidHAOutbox;
    if (grant) |selected| {
        if (selected.phase == .materialized) {
            if (count != 0) return error.InvalidHAOutbox;
            return;
        }
        var roots: Hash = undefined;
        hasher.final(&roots);
        if (count != selected.root_count or !std.meta.eql(roots, selected.roots) or !std.meta.eql(total, selected.current)) return error.InvalidHAOutbox;
    }
}
pub fn validateOwner(txn: anytype, key: []const u8) !void {
    if (key.len != owner_prefix.len + 32) return error.InvalidHAOutbox;
    const hash = key[owner_prefix.len..][0..32].*;
    const id = (try owner(txn, hash)) orelse return;
    const grant = (try load(txn, id)) orelse return error.InvalidHAOutbox;
    const raw = (try readOptional(txn, &claimKey(id, hash))) orelse return error.InvalidHAOutbox;
    const claim = try Claim.decode(raw);
    if (grant.phase != .prepared or !std.meta.eql(claim.owner, id) or !std.meta.eql(claim.frame, grant.frame)) return error.InvalidHAOutbox;
}
/// All arithmetic uses the old exact claim, never a guessed artifact count.
pub fn inventoryChanged(txn: anytype, capture: *inventory.Capture, hash: Hash, before: inventory.State, after: inventory.State) !void {
    const id = (try owner(txn, hash)) orelse return;
    var grant = (try load(txn, id)) orelse return error.InvalidHAOutbox;
    const raw = (try readOptional(txn, &claimKey(id, hash))) orelse return error.InvalidHAOutbox;
    var claim = try Claim.decode(raw);
    if (grant.phase != .prepared or !std.meta.eql(claim.owner, id) or !std.meta.eql(claim.frame, grant.frame) or !std.meta.eql(claim.state, before)) return error.InvalidHAOutbox;
    const old = try before.retirementReplayShape();
    const new = try after.retirementReplayShape();
    inline for (comptime std.meta.fieldNames(shape.Record)) |name| {
        const part = &@field(grant.current, name);
        const removed = @field(old, name);
        const added = @field(new, name);
        part.count = std.math.sub(u64, part.count, removed.count) catch return error.InvalidHAOutbox;
        part.key_bytes = std.math.sub(u64, part.key_bytes, removed.key_bytes) catch return error.InvalidHAOutbox;
        part.* = try part.plus(added);
    }
    claim.state = after;
    _ = try grant.budget();
    // Already inside the inventory work iterator: trusted metadata updates must
    // not resize that queue. Publication debt capture still sees both writes.
    capture.staging = true;
    defer capture.staging = false;
    try txn.put(&claimKey(id, hash), &claim.encode());
    try txn.put(&grantKey(id), &try grant.encode());
}
fn releaseClaims(txn: anytype, id: Id) !void {
    const prefix = claim_prefix.* ++ id;
    var retired: usize = 0;
    while (true) {
        // Reopen against the writer's current view after each deletion. A
        // cursor may otherwise retain a snapshot containing the deleted row.
        const hash: Hash = selected: {
            var cursor = try txn.openCursor();
            defer cursor.close();
            cursor.setKeysOnly();
            const row = (try cursor.seekAtOrAfter(&prefix)) orelse break;
            if (!std.mem.startsWith(u8, row.key, &prefix)) break;
            if (row.key.len != claim_prefix.len + 48) return error.InvalidHAOutbox;
            break :selected row.key[claim_prefix.len + 16 ..][0..32].*;
        };
        if (retired == backlog.default_max_records) return error.InvalidHAOutbox;
        retired += 1;
        const claimed_owner = (try owner(txn, hash)) orelse return error.InvalidHAOutbox;
        if (!std.meta.eql(claimed_owner, id)) return error.InvalidHAOutbox;
        try txn.delete(&ownerKey(hash));
        try txn.delete(&claimKey(id, hash));
    }
}
pub fn materialize(txn: anytype, id: Id, payload: []const u8) !bool {
    var grant = (try load(txn, id)) orelse return false;
    _ = try @import("db/derived/change_journal.zig").inspectBinaryRecord(payload, null);
    if (payload.len > try grant.budget()) return error.InvalidHAOutbox;
    const fingerprint = digest(payload);
    if (grant.phase == .materialized) {
        if (!std.meta.eql(grant.payload, fingerprint)) return error.PreparedEffectsConflict;
        return true;
    }
    try validateGrant(txn, id);
    try releaseClaims(txn, id);
    grant.phase = .materialized;
    grant.payload = fingerprint;
    try txn.put(&grantKey(id), &try grant.encode());
    return true;
}
pub fn acknowledge(txn: anytype, id: Id) !void {
    const grant = (try load(txn, id)) orelse return;
    if (grant.phase != .materialized) return error.InvalidHAOutbox;
    try txn.delete(&grantKey(id));
}
pub fn cancel(txn: anytype, capture: *inventory.Capture, id: Id) !void {
    errdefer capture.poisoned = true;
    const grant = (try load(txn, id)) orelse return;
    if (grant.phase != .prepared or (try readOptional(txn, &(replay_prefix.* ++ id))) != null) return error.DecisionConflict;
    try releaseClaims(txn, id);
    try txn.delete(&grantKey(id));
}

test "prepared replay credit codecs reject truncation corruption and impossible wire shapes" {
    const grant: Grant = .{ .owner = @splat(1), .frame = @splat(2), .roots = @splat(3), .root_count = 1, .base = .{ .changed_doc_keys = .{ .count = 1, .key_bytes = 5 } }, .current = try (inventory.State{ .count = 2, .key_bytes = 23 }).retirementReplayShape() };
    const encoded = try grant.encode();
    try std.testing.expectEqualDeep(grant, try Grant.decode(&encoded));
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidHAOutbox, Grant.decode(encoded[0..len]));
    for (0..encoded.len) |i| {
        var changed = encoded;
        changed[i] ^= 1;
        try std.testing.expectError(error.InvalidHAOutbox, Grant.decode(&changed));
    }
    var changed = encoded;
    changed[6] = 99;
    @memcpy(changed[256..], &digest(changed[0..256]));
    try std.testing.expectError(error.InvalidHAOutbox, Grant.decode(&changed));
    const claim: Claim = .{ .owner = grant.owner, .frame = grant.frame, .state = .{ .count = 2, .key_bytes = 23 } };
    const bytes = claim.encode();
    try std.testing.expectEqualDeep(claim, try Claim.decode(&bytes));
    for (0..bytes.len) |len| try std.testing.expectError(error.InvalidHAOutbox, Claim.decode(bytes[0..len]));
    var impossible = grant;
    impossible.base.changed_doc_keys = .{ .key_bytes = 1 };
    try std.testing.expectError(error.RecordTooLarge, impossible.encode());
}
