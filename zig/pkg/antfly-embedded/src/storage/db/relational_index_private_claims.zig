// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS,
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
pub const receipt_prefix = jobs.key ++ ":claim-receipt:";
const authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
pub const max_claims = 128;
pub const max_bytes = 1024 * 1024;
const key_len = prefix.len + 16 + 32 + 32;

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
};

fn bindingDigest(binding: jobs.Binding) !integrity.Digest {
    var normalized = binding;
    normalized.phase = .reserved;
    return hash(&try normalized.encode());
}

const receipt_key_len = receipt_prefix.len + 16 + 32;
const receipt_value_len = 108;

pub fn receiptKey(generation: integrity.Generation, digest: integrity.Digest) [receipt_key_len]u8 {
    var result: [receipt_key_len]u8 = undefined;
    @memcpy(result[0..receipt_prefix.len], receipt_prefix);
    @memcpy(result[receipt_prefix.len..][0..16], &generation);
    @memcpy(result[receipt_prefix.len + 16 ..], &digest);
    return result;
}

fn receiptChecksum(key: []const u8, body: []const u8) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly private claim receipt v1");
    state.update(key);
    state.update(body);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

fn encodeReceipt(key: []const u8, binding: jobs.Binding, digest: integrity.Digest, sequence: u64) ![receipt_value_len]u8 {
    var result: [receipt_value_len]u8 = undefined;
    @memcpy(result[0..4], "AICR");
    std.mem.writeInt(u64, result[4..12], sequence, .little);
    @memcpy(result[12..44], &try bindingDigest(binding));
    @memcpy(result[44..76], &digest);
    @memcpy(result[76..], &receiptChecksum(key, result[0..76]));
    return result;
}

fn decodeReceipt(key: []const u8, value: []const u8, binding_digest: integrity.Digest, reserved: u64, current: u64) !u64 {
    if (key.len != receipt_key_len or !std.mem.startsWith(u8, key, receipt_prefix) or
        value.len != receipt_value_len or !std.mem.eql(u8, value[0..4], "AICR") or
        !std.mem.eql(u8, value[12..44], &binding_digest) or
        !std.mem.eql(u8, value[44..76], key[receipt_prefix.len + 16 ..]) or
        !std.mem.eql(u8, value[76..], &receiptChecksum(key, value[0..76]))) return error.InvalidPrivateIndexClaimReceipt;
    const sequence = std.mem.readInt(u64, value[4..12], .little);
    if (sequence <= reserved or sequence > current) return error.InvalidPrivateIndexClaimReceipt;
    return sequence;
}

pub const OrderedOutcome = struct { position: Native, duplicate: bool, claims_added: usize };
const ReceiptEntry = struct { key: []const u8, value: []const u8 };

/// Own all tuple/claim bytes before serialized apply. A prepared batch does not
/// retain a store snapshot or borrow request memory. Cancellation is checked
/// again in stage, including batches prepared before the intent committed.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,
    digest: integrity.Digest,

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn init(alloc: Allocator, txn: anytype, binding: jobs.Binding, claims: []const integrity.Claim) !Prepared {
        if (claims.len == 0 or claims.len > max_claims or binding.authority != .native) return error.InvalidPrivateIndexClaimInventory;
        var bytes: usize = 0;
        for (claims) |claim| {
            if (claim.state != .live or claim.schema_version != binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
            bytes = std.math.add(usize, bytes, 256) catch return error.TransactionTooLarge;
            for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
        }
        if (bytes > max_bytes) return error.TransactionTooLarge;
        try jobs.requireReserved(txn, binding);
        const allocation = try requireRegistered(txn, binding);
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
            try checkInventory(txn, address, expected);
            try entries.append(owned, .{ .address = address, .value = value, .expected = expected });
        }
        // Canonical set identity makes reordered/coalesced retries recover the
        // same cut. No request-owned byte or incidental physical encoding is
        // included in the batch receipt identity.
        std.mem.sort(Entry, entries.items, {}, struct {
            fn less(_: void, left: Entry, right: Entry) bool {
                return std.mem.order(u8, &left.address.claimKey(), &right.address.claimKey()) == .lt;
            }
        }.less);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly private claim batch v1");
        state.update(&try bindingDigest(binding));
        for (entries.items) |entry| {
            state.update(&entry.address.claimKey());
            const claim = try integrity.Claim.decode(&entry.address.claimKey(), entry.value);
            var version: [4]u8 = undefined;
            std.mem.writeInt(u32, &version, claim.schema_version, .little);
            state.update(&version);
            for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| {
                var length: [8]u8 = undefined;
                std.mem.writeInt(u64, &length, field.len, .little);
                state.update(&length);
                state.update(field);
            }
        }
        var digest: integrity.Digest = undefined;
        state.final(&digest);
        return .{ .arena = arena, .binding = binding, .entries = entries.items, .digest = digest };
    }

    pub fn stage(self: *const Prepared, txn: anytype) !usize {
        try jobs.requireReserved(txn, self.binding);
        _ = try requireRegistered(txn, self.binding);
        for (self.entries) |entry| {
            const actual = try optional(txn, &entry.address.claimKey());
            if (!optionalEqual(actual, entry.expected)) return error.VersionConflict;
            try checkInventory(txn, entry.address, entry.expected);
        }
        var changed: usize = 0;
        for (self.entries) |entry| {
            if (entry.expected != null) continue;
            try txn.put(&entry.address.claimKey(), entry.value);
            try txn.put(&inventoryKey(entry.address), &hash(entry.value));
            changed += 1;
        }
        return changed;
    }

    /// Caller commits the claim pairs, this exact-cut receipt, transport marker
    /// and outbox together. A duplicate is proved by both receipt and retained
    /// claim postimages, never inferred from a newer global source clock.
    pub fn stageOrdered(self: *const Prepared, txn: anytype, replay: ?Native) !OrderedOutcome {
        try jobs.requireReserved(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        const namespace = @import("online_source_contract.zig").namespaceBytes(self.binding.fence.namespace);
        const current = try authority.require(txn, .native, namespace);
        const reserved = (try jobs.nativeEffectPosition(txn, self.binding, .reserved)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if (replay) |position| {
            try position.validate();
            if (!position.namespace.eql(self.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        }
        const key = receiptKey(allocation.generation, self.digest);
        if (try optional(txn, &key)) |raw| {
            const sequence = try decodeReceipt(&key, raw, try bindingDigest(self.binding), reserved.sequence, current.sequence);
            if (replay) |position| if (position.sequence != sequence) return error.InvalidPrivateIndexClaimReceipt;
            for (self.entries) |entry| {
                if (!optionalEqual(try optional(txn, &entry.address.claimKey()), entry.value)) return error.InvalidPrivateIndexClaimReceipt;
                try checkInventory(txn, entry.address, entry.value);
            }
            return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = sequence }, .duplicate = true, .claims_added = 0 };
        }
        const changed = try self.stage(txn);
        const sequence = try authority.advance(txn, namespace, if (replay) |position| position.sequence else null);
        try txn.put(&key, &try encodeReceipt(&key, self.binding, self.digest, sequence));
        return .{ .position = .{ .namespace = self.binding.fence.namespace, .sequence = sequence }, .duplicate = false, .claims_added = changed };
    }
};

fn optionalEqual(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}

fn checkInventory(txn: anytype, address: integrity.Address, expected: ?[]const u8) !void {
    const actual = try optional(txn, &inventoryKey(address));
    if (expected) |value| {
        if (actual == null or !std.mem.eql(u8, actual.?, &hash(value))) return error.InvalidPrivateIndexClaimInventory;
    } else if (actual != null) return error.InvalidPrivateIndexClaimInventory;
}

/// Pin once in the same source/staging transaction as a transfer page. This
/// avoids decoding the job or hashing its catalog once per claim. The view is
/// information for physical validation, never execution/publication authority.
pub const TransferView = struct {
    generation: ?integrity.Generation = null,

    pub fn init(txn: anytype) !TransferView {
        const binding = (try jobs.load(txn)) orelse return .{};
        if (binding.phase != .reserved) return .{};
        return .{ .generation = (try requireRegistered(txn, binding)).generation };
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
        if (!std.mem.eql(u8, actual, &value_digest)) return error.InvalidPrivateIndexClaimInventory;
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
    const receipt_start = receiptKey(allocation.generation, @splat(0));
    const selected = receipt_start[0 .. receipt_prefix.len + 16];
    if (try cursor.seekAtOrAfter(selected)) |entry| if (std.mem.startsWith(u8, entry.key, selected)) return error.PrivateIndexCancellationPending;
}

/// Deleting the first bounded inventory page is itself the durable continuation:
/// abort leaves every pair present, commit advances both keys atomically. No
/// whole-claim scan, RAM cursor or generation-global rewrite is required.
pub const GcPage = struct {
    arena: std.heap.ArenaAllocator,
    binding: jobs.Binding,
    entries: []const Entry,
    receipts: []const ReceiptEntry = &.{},

    pub fn deinit(self: *GcPage) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, txn: anytype, binding: jobs.Binding) !?GcPage {
        _ = try jobs.requireCancellationCleanup(txn, binding);
        const allocation = try requireRegistered(txn, binding);
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
            if (entry.value.len != 32) return error.InvalidPrivateIndexClaimInventory;
            const expected_hash = entry.value[0..32].*;
            const raw = (try optional(txn, &address.claimKey())) orelse return error.InvalidPrivateIndexClaimInventory;
            if (raw.len > integrity.max_record_bytes or !std.mem.eql(u8, &expected_hash, &hash(raw))) return error.InvalidPrivateIndexClaimInventory;
            const value = try owned.dupe(u8, raw);
            const claim = try integrity.Claim.decode(&address.claimKey(), value);
            if (claim.state != .live) return error.InvalidPrivateIndexClaimInventory;
            try entries.append(owned, .{ .address = address, .value = value, .expected = value });
            bytes +|= value.len + key_len;
            if (entries.items.len >= 256 or bytes >= max_bytes or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) break;
        }
        if (entries.items.len == 0) {
            // Receipts remain until all claims are gone. Their identity and
            // source cuts are self-contained, so this phase never scans rows
            // or attempts to decode claims that earlier pages retired.
            const digest = try bindingDigest(binding);
            const current = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
            const reserved = (try jobs.nativeEffectPosition(txn, binding, .reserved)) orelse return error.InvalidPrivateIndexNativeReceipt;
            var receipts: std.ArrayList(ReceiptEntry) = .empty;
            const receipt_start = receiptKey(allocation.generation, @splat(0));
            const selected = receipt_start[0 .. receipt_prefix.len + 16];
            next = try cursor.seekAtOrAfter(selected);
            while (next) |entry| : (next = try cursor.next()) {
                if (!std.mem.startsWith(u8, entry.key, selected)) break;
                if (io) |runtime| try runtime.checkCancel();
                _ = try decodeReceipt(entry.key, entry.value, digest, reserved.sequence, current.sequence);
                try receipts.append(owned, .{ .key = try owned.dupe(u8, entry.key), .value = try owned.dupe(u8, entry.value) });
                if (receipts.items.len >= 256 or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) break;
            }
            if (receipts.items.len == 0) {
                arena.deinit();
                return null;
            }
            return .{ .arena = arena, .binding = binding, .entries = &.{}, .receipts = receipts.items };
        }
        return .{ .arena = arena, .binding = binding, .entries = entries.items };
    }

    pub fn stage(self: *const GcPage, txn: anytype) !void {
        _ = try jobs.requireCancellationCleanup(txn, self.binding);
        const allocation = try requireRegistered(txn, self.binding);
        if (self.receipts.len != 0) {
            var cursor = try txn.openCursor();
            defer cursor.close();
            const start = generationPrefix(allocation.generation);
            if (try cursor.seekAtOrAfter(&start)) |entry| if (std.mem.startsWith(u8, entry.key, &start)) return error.VersionConflict;
        }
        for (self.receipts) |receipt| {
            if (!optionalEqual(try optional(txn, receipt.key), receipt.value)) return error.VersionConflict;
        }
        for (self.entries) |entry| {
            if (!optionalEqual(try optional(txn, &entry.address.claimKey()), entry.expected)) return error.VersionConflict;
            try checkInventory(txn, entry.address, entry.expected);
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
        for (self.receipts) |receipt| try txn.delete(receipt.key);
    }
};
