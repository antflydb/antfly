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

//! Generation-owned inventory for unpublished UNIQUE claims. Callers retain
//! apply exclusion and commit these primitives with their ordered durable
//! effect/outbox. No API here authenticates metadata or acknowledges a cohort.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const catalog = @import("relational_integrity_catalog.zig");
const integrity = @import("relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;
pub const prefix = jobs.key ++ ":claim-inventory:";
pub const registration_prefix = jobs.key ++ ":claim-registration:";
const authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
pub const max_claims = 128;
pub const max_bytes = 1024 * 1024;
const key_len = prefix.len + 16 + 32 + 32;
// AFC1 fixed fields/checksum, both paired keys, and the provenance value.
// Count the actual bytes GC must carry, not just request string lengths.
pub const claim_entry_overhead = 76 + key_len + integrity.key_len + inventory_value_len;

fn hash(bytes: []const u8) integrity.Digest {
    return integrity.hash(bytes);
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

fn scope(txn: anytype, binding: jobs.Binding) !catalog.PrivateIndexAllocation {
    return catalog.privateIndexAllocationFromReservation(try txn.get(catalog.key), binding.integrity_reserved, binding.fence.namespace.table_id);
}

pub fn registrationKey(generation: integrity.Generation) [registration_prefix.len + 16]u8 {
    var key: [registration_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..registration_prefix.len], registration_prefix);
    @memcpy(key[registration_prefix.len..], &generation);
    return key;
}

fn registration(binding: jobs.Binding, generation: integrity.Generation) !integrity.Digest {
    var current = binding;
    current.phase = .reserved;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly private UNIQUE claim registration v1");
    state.update(&try current.encode());
    state.update(&generation);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

fn generationPrefix(generation: integrity.Generation) [prefix.len + 16]u8 {
    var key: [prefix.len + 16]u8 = undefined;
    @memcpy(key[0..prefix.len], prefix);
    @memcpy(key[prefix.len..], &generation);
    return key;
}

pub fn inventoryKey(address: integrity.Address) [key_len]u8 {
    var key: [key_len]u8 = undefined;
    @memcpy(key[0 .. prefix.len + 16], &generationPrefix(address.generation));
    @memcpy(key[prefix.len + 16 ..][0..32], &address.routing);
    @memcpy(key[prefix.len + 48 ..], &address.tuple_digest);
    return key;
}

fn addressFromInventory(key: []const u8) !integrity.Address {
    if (key.len != key_len or !std.mem.startsWith(u8, key, prefix)) return error.InvalidPrivateIndexClaimInventory;
    const address: integrity.Address = .{
        .generation = key[prefix.len..][0..16].*,
        .routing = key[prefix.len + 16 ..][0..32].*,
        .tuple_digest = key[prefix.len + 48 ..][0..32].*,
    };
    _ = integrity.parseKey(&address.claimKey()) catch return error.InvalidPrivateIndexClaimInventory;
    return address;
}

/// Reservation creates the empty inventory before private claim writes become
/// admissible. This is staged with both allocation burns, not lazily inferred
/// from the absence of inventory entries after a restart.
pub fn stageRegistration(txn: anytype, binding: jobs.Binding) !void {
    const allocation = try scope(txn, binding);
    const key = registrationKey(allocation.generation);
    if (try optional(txn, &key) != null) return error.InvalidPrivateIndexClaimInventory;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const start = generationPrefix(allocation.generation);
        if (try cursor.seekAtOrAfter(&start)) |entry| if (std.mem.startsWith(u8, entry.key, &start)) return error.InvalidPrivateIndexClaimInventory;
    }
    try txn.put(&key, &try registration(binding, allocation.generation));
}

pub fn requireRegistered(txn: anytype, binding: jobs.Binding) !catalog.PrivateIndexAllocation {
    const allocation = try scope(txn, binding);
    const expected = try registration(binding, allocation.generation);
    const actual = (try optional(txn, &registrationKey(allocation.generation))) orelse return error.InvalidPrivateIndexClaimInventory;
    if (!std.mem.eql(u8, actual, &expected)) return error.InvalidPrivateIndexClaimInventory;
    return allocation;
}

const Entry = struct {
    address: integrity.Address,
    value: []const u8,
    expected: ?[]const u8,
    sequence: u64 = 0,
};

fn bindingDigest(binding: jobs.Binding) !integrity.Digest {
    var normalized = binding;
    normalized.phase = .reserved;
    return hash(&try normalized.encode());
}

// One immutable cut plus a binding/claim/key/cut digest. The shared generation
// registration already owns binding identity; duplicating that identity and a
// claim hash as plaintext in every row adds bytes without additional integrity.
pub const inventory_value_len = 40;

fn inventoryChecksum(key: []const u8, binding_digest: integrity.Digest, value_digest: integrity.Digest, cut: []const u8) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly private claim provenance v1");
    state.update(key);
    state.update(&binding_digest);
    state.update(&value_digest);
    state.update(cut);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

fn encodeInventory(key: []const u8, binding_digest: integrity.Digest, value_digest: integrity.Digest, sequence: u64) [inventory_value_len]u8 {
    var result: [inventory_value_len]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], sequence, .little);
    @memcpy(result[8..], &inventoryChecksum(key, binding_digest, value_digest, result[0..8]));
    return result;
}

fn decodeInventory(key: []const u8, value: []const u8, provenance: Provenance, value_digest: integrity.Digest) !u64 {
    _ = try addressFromInventory(key);
    if (value.len != inventory_value_len or
        !std.mem.eql(u8, value[8..], &inventoryChecksum(key, provenance.binding_digest, value_digest, value[0..8]))) return error.InvalidPrivateIndexClaimInventory;
    const sequence = std.mem.readInt(u64, value[0..8], .little);
    if (sequence <= provenance.reserved or sequence > provenance.maximum) return error.InvalidPrivateIndexClaimInventory;
    return sequence;
}

const Provenance = struct {
    binding_digest: integrity.Digest,
    reserved: u64,
    maximum: u64,

    fn init(txn: anytype, binding: jobs.Binding) !Provenance {
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
        const current = try authority.require(txn, .native, namespace);
        const reserved = (try jobs.nativeEffectPosition(txn, binding, .reserved)) orelse return error.InvalidPrivateIndexNativeReceipt;
        var maximum = current.sequence;
        const cancellation = @import("relational_index_cancellation_intent.zig");
        if (try cancellation.load(txn)) |canceled| {
            if (canceled.intent.fence.admission_epoch < binding.fence.admission_epoch) {
                try cancellation.requireOpen(txn, binding);
            } else {
                try cancellation.requireCanceled(txn, binding);
                maximum = @min(maximum, canceled.position.sequence -| 1);
            }
        }
        if (maximum < reserved.sequence) return error.InvalidPrivateIndexClaimInventory;
        return .{ .binding_digest = try bindingDigest(binding), .reserved = reserved.sequence, .maximum = maximum };
    }
};

pub const OrderedOutcome = struct { position: Native, duplicate: bool, claims_added: usize };

/// Own all tuple/claim bytes before serialized apply. A prepared batch does not
/// retain a store snapshot or borrow request memory. Cancellation is checked
/// again in stage, including batches prepared before the intent committed.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn init(alloc: Allocator, txn: anytype, binding: jobs.Binding, claims: []const integrity.Claim) !Prepared {
        if (claims.len == 0 or claims.len > max_claims or binding.authority != .native) return error.InvalidPrivateIndexClaimInventory;
        var bytes: usize = 0;
        for (claims) |claim| {
            if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
            bytes = std.math.add(usize, bytes, claim_entry_overhead) catch return error.TransactionTooLarge;
            for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        }
        if (bytes > max_bytes) return error.TransactionTooLarge;
        try jobs.requireReserved(txn, binding);
        const allocation = try requireRegistered(txn, binding);
        const provenance = try Provenance.init(txn, binding);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        var seen: std.AutoHashMapUnmanaged(integrity.Digest, usize) = .empty;
        defer seen.deinit(alloc);
        for (claims) |claim| {
            const address = try integrity.Address.init(allocation.generation, claim.tuple);
            const value = try claim.encode(owned, address);
            const slot = try seen.getOrPut(alloc, address.routing);
            if (slot.found_existing) {
                if (!std.mem.eql(u8, entries.items[slot.value_ptr.*].value, value)) return error.UniqueConstraintViolation;
                continue;
            }
            slot.value_ptr.* = entries.items.len;
            // Validate before owning any store bytes. An exact duplicate can
            // retain the already-owned encoding, avoiding a second allocation
            // and rejecting oversized corrupt records before cloning them.
            const expected = if (try optional(txn, &address.claimKey())) |raw| blk: {
                _ = try integrity.Claim.decode(&address.claimKey(), raw);
                if (!std.mem.eql(u8, raw, value)) return error.UniqueConstraintViolation;
                break :blk value;
            } else null;
            const sequence = try checkInventory(txn, address, expected, provenance);
            try entries.append(owned, .{ .address = address, .value = value, .expected = expected, .sequence = sequence });
        }
        // Stable transport order, independent of request order/coalescing.
        std.mem.sort(Entry, entries.items, {}, struct {
            fn less(_: void, left: Entry, right: Entry) bool {
                return std.mem.order(u8, &left.address.claimKey(), &right.address.claimKey()) == .lt;
            }
        }.less);
        return .{ .arena = arena, .binding = binding, .entries = entries.items };
    }

    /// Caller commits claim pairs, immutable creation cuts, transport marker
    /// and outbox together. The batch cut is the maximum creation cut of its
    /// members: new members receive this effect's cut, existing members never
    /// change. Retries and regrouping therefore add no metadata and recover an
    /// exact cut without a per-batch ledger or a global-clock approximation.
    pub fn stageOrdered(self: *const Prepared, txn: anytype, replay: ?Native) !OrderedOutcome {
        try jobs.requireReserved(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const namespace = @import("online_source_contract.zig").namespaceBytes(self.binding.fence.namespace);
        const current = try authority.require(txn, .native, namespace);
        const provenance = try Provenance.init(txn, self.binding);
        if (replay) |position| {
            try position.validate();
            if (!position.namespace.eql(self.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        }
        var changed: usize = 0;
        var maximum: u64 = 0;
        for (self.entries) |entry| {
            if (!std.mem.eql(u8, &entry.address.generation, &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
            const actual = try optional(txn, &entry.address.claimKey());
            if (actual) |value| {
                if (!std.mem.eql(u8, value, entry.value)) return error.UniqueConstraintViolation;
                const sequence = try checkInventory(txn, entry.address, entry.value, provenance);
                if (entry.expected != null and sequence != entry.sequence) return error.VersionConflict;
                maximum = @max(maximum, sequence);
            } else {
                if (entry.expected != null) return error.VersionConflict;
                _ = try checkInventory(txn, entry.address, null, provenance);
                changed += 1;
            }
        }
        if (changed == 0) {
            if (replay) |position| if (position.sequence != maximum) return error.InvalidPrivateIndexClaimReceipt;
            return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = maximum }, .duplicate = true, .claims_added = 0 };
        }
        const sequence = std.math.add(u64, current.sequence, 1) catch return error.OnlineSourceCorrupt;
        if (replay) |position| if (position.sequence != sequence) return error.OnlineSourceScopeChanged;
        try txn.retained.bindPrivateClaims(allocation.generation, prefix);
        for (self.entries) |entry| {
            if (try optional(txn, &entry.address.claimKey()) != null) continue;
            const key = inventoryKey(entry.address);
            try txn.put(&entry.address.claimKey(), entry.value);
            try txn.put(&key, &encodeInventory(&key, provenance.binding_digest, hash(entry.value), sequence));
        }
        _ = try authority.advance(txn, namespace, sequence);
        return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = sequence }, .duplicate = false, .claims_added = changed };
    }
};

fn optionalEqual(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}

fn checkInventory(txn: anytype, address: integrity.Address, expected: ?[]const u8, provenance: Provenance) !u64 {
    const actual = try optional(txn, &inventoryKey(address));
    if (expected) |value| {
        return decodeInventory(&inventoryKey(address), actual orelse return error.InvalidPrivateIndexClaimInventory, provenance, hash(value));
    } else if (actual != null) return error.InvalidPrivateIndexClaimInventory;
    return 0;
}

/// Pin once in the same source/staging transaction as a transfer page. This
/// avoids decoding the job or hashing its catalog once per claim. The view is
/// information for physical validation, never execution/publication authority.
pub const TransferView = struct {
    generation: ?integrity.Generation = null,
    provenance: ?Provenance = null,

    pub fn init(txn: anytype) !TransferView {
        const binding = (try jobs.load(txn)) orelse return .{};
        if (binding.phase != .reserved) return .{};
        return .{ .generation = (try requireRegistered(txn, binding)).generation, .provenance = if (binding.authority == .native) try Provenance.init(txn, binding) else null };
    }

    pub fn isPrivate(self: TransferView, generation: integrity.Generation) bool {
        return if (self.generation) |current| std.mem.eql(u8, &current, &generation) else false;
    }

    pub fn validateCompanion(self: TransferView, txn: anytype, parsed: integrity.ParsedKey, value_digest: integrity.Digest) !void {
        if (!self.isPrivate(parsed.address.generation)) return;
        // An unpublished UNIQUE generation cannot have FK references/action
        // jobs. Those records must not acquire legitimacy through restoration.
        if (parsed.kind != .claim) return error.InvalidPrivateIndexClaimInventory;
        const actual = (try optional(txn, &inventoryKey(parsed.address))) orelse return error.InvalidPrivateIndexClaimInventory;
        _ = try decodeInventory(&inventoryKey(parsed.address), actual, self.provenance orelse return error.InvalidPrivateIndexClaimInventory, value_digest);
    }
};

/// Local emptiness only; metadata must still prove complete cohort retirement
/// and persist the final cancellation cut before any owner acknowledgement.
pub fn requireRetired(txn: anytype, binding: jobs.Binding) !void {
    _ = try jobs.requireCancellationCleanup(txn, binding);
    const allocation = try requireRegistered(txn, binding);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const start = generationPrefix(allocation.generation);
    if (try cursor.seekAtOrAfter(&start)) |entry| if (std.mem.startsWith(u8, entry.key, &start)) return error.PrivateIndexCancellationPending;
}

/// Deleting the first bounded inventory page is itself the durable continuation:
/// abort leaves every pair present, commit advances both keys atomically. No
/// whole-claim scan, RAM cursor or generation-global rewrite is required.
pub const GcPage = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,

    pub fn deinit(self: *GcPage) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, txn: anytype, binding: jobs.Binding) !?GcPage {
        _ = try jobs.requireCancellationCleanup(txn, binding);
        const allocation = try requireRegistered(txn, binding);
        const provenance = try Provenance.init(txn, binding);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        var cursor = try txn.openCursor();
        defer cursor.close();
        const start = generationPrefix(allocation.generation);
        const time = @import("antfly_platform").time;
        const started = time.monotonicNs();
        var bytes: usize = 0;
        var next = try cursor.seekAtOrAfter(&start);
        while (next) |entry| : (next = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, &start)) break;
            if (io) |runtime| try runtime.checkCancel();
            const address = try addressFromInventory(entry.key);
            if (entry.value.len != inventory_value_len) return error.InvalidPrivateIndexClaimInventory;
            // Copy before the claim read can invalidate borrowed cursor bytes.
            const inventory = entry.value[0..inventory_value_len].*;
            const raw = (try optional(txn, &address.claimKey())) orelse return error.InvalidPrivateIndexClaimInventory;
            if (raw.len > integrity.max_record_bytes) return error.InvalidPrivateIndexClaimInventory;
            const record_bytes = raw.len + key_len + integrity.key_len + inventory_value_len;
            if (record_bytes > max_bytes) return error.InvalidPrivateIndexClaimInventory;
            // Stop BEFORE owning another row; checking after append can admit
            // almost two page budgets when the next claim is large.
            if (record_bytes > max_bytes - bytes) break;
            const sequence = try decodeInventory(&inventoryKey(address), &inventory, provenance, hash(raw));
            const value = try owned.dupe(u8, raw);
            const claim = try integrity.Claim.decode(&address.claimKey(), value);
            if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
            try entries.append(owned, .{ .address = address, .value = value, .expected = value, .sequence = sequence });
            bytes += record_bytes;
            if (entries.items.len >= 256 or bytes >= max_bytes or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) break;
        }
        if (entries.items.len == 0) {
            arena.deinit();
            return null;
        }
        return .{ .arena = arena, .binding = binding, .entries = entries.items };
    }

    pub fn stage(self: *const GcPage, txn: anytype) !void {
        _ = try jobs.requireCancellationCleanup(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const provenance = try Provenance.init(txn, self.binding);
        try txn.retained.bindPrivateClaims(allocation.generation, prefix);
        for (self.entries) |entry| {
            if (!std.mem.eql(u8, &entry.address.generation, &allocation.generation)) return error.InvalidPrivateIndexClaimInventory;
            if (!optionalEqual(try optional(txn, &entry.address.claimKey()), entry.expected)) return error.VersionConflict;
            if (try checkInventory(txn, entry.address, entry.expected, provenance) != entry.sequence) return error.VersionConflict;
            if (try optional(txn, &entry.address.jobKey()) != null) return error.InvalidPrivateIndexClaimInventory;
            var cursor = try txn.openCursor();
            defer cursor.close();
            const reference = entry.address.referencePrefix();
            if (try cursor.seekAtOrAfter(&reference)) |record| if (std.mem.startsWith(u8, record.key, &reference)) return error.InvalidPrivateIndexClaimInventory;
        }
        for (self.entries) |entry| {
            try txn.delete(&entry.address.claimKey());
            try txn.delete(&inventoryKey(entry.address));
        }
    }
};
