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

//! Compare-and-lock against the exact native primary witness. A changed row
//! consumes a false/cancelled effect, never a grant. The caller may refresh
//! and recheck that row without replaying its SQL occurrence. Matching grants
//! reuse the already captured typed row: no row image is copied into the WAL,
//! and compatible later updates cannot change the returned snapshot's values.
const std = @import("std");
const participant = @import("pessimistic_lock_participant.zig");
const scopes = @import("pessimistic_lock_authority.zig");
const ledger = scopes.ledger;
const keys = @import("internal_keys.zig");
const A = std.mem.Allocator;

pub const Witness = struct {
    version: u64,
    digest: [32]u8,
    pub fn validate(self: Witness) !void {
        if (self.version == 0 or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidLockResource;
    }
};
pub const Request = struct {
    acquisition: participant.Acquisition,
    witness: Witness,
    wait: enum { block, try_lock },
    pub fn validate(self: Request) !void {
        try self.acquisition.validate();
        try self.witness.validate();
        if (self.acquisition.physical_key == null or self.acquisition.effect.action != .acquire) return error.InvalidLockEffect;
    }
    fn digest(self: Request, a: A) ![32]u8 {
        try self.validate();
        var normalized = self;
        // Lease extension confers no new effect identity or SQL permission.
        normalized.acquisition.admission.source.lease.expires_at_ms = 1;
        const bytes = try std.json.Stringify.valueAlloc(a, normalized, .{});
        defer a.free(bytes);
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        return hash;
    }
};
pub const Result = struct {
    receipt: ledger.Receipt,
    status: enum { matched, changed, absent, contended, write_pending },
};
const Record = struct { request: [32]u8, result: Result };
fn address(buffer: []u8, admission: participant.Admission, sequence: u64) ![]const u8 {
    const ns = admission.binding.namespace;
    const backend = (try admission.registration()).lease.backend;
    return std.fmt.bufPrint(buffer, "\x00\x00__lock_row_result__:{x:0>16}:{s}:{s}:{x:0>16}:{x:0>16}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence, sequence });
}

/// Read the same physical primary and timestamp used by native version
/// predicates, from the accepting writer itself. Never use a cached SQL row
/// or a second snapshot as admission evidence.
pub fn current(a: A, txn: anytype, mode: @import("schema.zig").StorageMode, key_value: []const u8) !?Witness {
    if (key_value.len == 0 or std.mem.startsWith(u8, key_value, "\x00\x00")) return error.InvalidLockResource;
    const primary = if (mode == .relational) try keys.relationalRowKeyAlloc(a, key_value) else try keys.documentKeyAlloc(a, key_value);
    defer a.free(primary);
    const bytes = txn.get(primary) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const timestamp_key = try keys.ttlKeyAlloc(a, key_value);
    defer a.free(timestamp_key);
    const timestamp = txn.get(timestamp_key) catch |err| switch (err) {
        error.NotFound => return error.InvalidLockRecord,
        else => return err,
    };
    if (timestamp.len < 8) return error.InvalidLockRecord;
    var result: Witness = .{ .version = std.mem.readInt(u64, timestamp[0..8], .little), .digest = undefined };
    std.crypto.hash.sha2.Sha256.hash(bytes, &result.digest, .{});
    try result.validate();
    return result;
}

/// Retained false and granted outcomes are observations only. A caller must
/// still own its global SQL execution permit before consuming a returned row.
pub fn observe(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, request: Request, authority: anytype) !?Result {
    try request.validate();
    const receipt = (try participant.observe(a, txn, receiving, request.acquisition, authority)) orelse return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var buffer: [224]u8 = undefined;
    const raw = txn.get(try address(&buffer, request.acquisition.admission, receipt.sequence)) catch |err| switch (err) {
        error.NotFound => return error.InvalidLockRecord,
        else => return err,
    };
    if (raw.len > 2048 or raw.len <= 32) return error.InvalidLockRecord;
    const body = raw[0 .. raw.len - 32];
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[body.len..])) return error.InvalidLockRecord;
    const record = try std.json.parseFromSliceLeaky(Record, scratch, body, .{});
    if (!std.mem.eql(u8, &record.request, &try request.digest(scratch))) return error.LockEffectChanged;
    try record.result.receipt.requireFor(request.acquisition.admission.binding.namespace, request.acquisition.effect);
    // Independent deadlock/cancellation cleanup can complete a queued effect.
    // Its immutable request stays bound; the ledger owns that terminal result.
    if (record.result.receipt.outcome != receipt.outcome and (record.result.receipt.outcome != .pending or (receipt.outcome != .cancelled and receipt.outcome != .deadlocked))) return error.InvalidLockRecord;
    return .{ .receipt = receipt, .status = record.result.status };
}

/// Runs under the receiving DB apply mutex in one native writer transaction.
/// Abort on every error. Snapshot mismatch consumes the exact effect even if
/// its original RPC is delayed; an old false outcome can never become a grant.
pub fn acquire(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, mode: @import("schema.zig").StorageMode, request: Request, authority: anytype) !Result {
    try request.validate();
    try request.acquisition.admission.binding.requireIdentity(receiving);
    try authority.requireDataResource(txn, request.acquisition.admission, request.acquisition.physical_key);
    if (try observe(a, txn, receiving, request, authority)) |known| if (known.receipt.outcome != .pending) return known;
    const acquisition = request.acquisition;
    const admission = acquisition.admission;
    const found = try current(a, txn, mode, acquisition.physical_key.?);
    // Compare against native write intents under the same apply mutex as
    // their admission. An old committed primary is not lockable while another
    // transaction owns a pending update, even before external COMMIT activation.
    const writing = try @import("transactions.zig").pendingIntentOwner(a, txn, acquisition.physical_key.?, admission.transaction);
    const result: Result = if (writing != null) .{
        .receipt = try participant.cancel(a, txn, receiving, acquisition, authority),
        .status = .write_pending,
    } else if (found == null or !std.meta.eql(found.?, request.witness)) .{
        .receipt = try participant.cancel(a, txn, receiving, acquisition, authority),
        .status = if (found == null) .absent else .changed,
    } else result: {
        if (request.wait == .try_lock) {
            const owner = try ledger.readOwner(a, txn, admission.binding.namespace, acquisition.effect.backend) orelse return error.LockOwnerNotRegistered;
            if (owner.waiting != null or owner.job != null or owner.state != .active) return error.LockOwnerBusy;
            if (owner.transaction) |id| if (!std.mem.eql(u8, &id, &admission.transaction)) return error.InvalidLockRecord;
            const attachment: ?ledger.AccessOwner = if (owner.transaction) |id| .{ .backend = owner.lease.backend, .transaction = id } else null;
            const blocked = try ledger.accessBlocked(a, txn, admission.binding.namespace, acquisition.effect.action.resource(), acquisition.effect.action.acquire.mode, attachment);
            if (blocked) break :result .{ .receipt = try participant.cancel(a, txn, receiving, acquisition, authority), .status = .contended };
        }
        const receipt = try participant.acquire(a, txn, receiving, acquisition, authority);
        // The anonymous/owned admission probe and grant share this writer;
        // a nonblocking request must never publish a queued waiter.
        if (request.wait == .try_lock and receipt.outcome == .pending) return error.InvalidLockRecord;
        break :result .{ .receipt = receipt, .status = .matched };
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const record: Record = .{ .request = try request.digest(scratch), .result = result };
    const body = try std.json.Stringify.valueAlloc(scratch, record, .{});
    if (body.len + 32 > 2048) return error.InvalidLockRecord;
    const encoded = try scratch.alloc(u8, body.len + 32);
    @memcpy(encoded[0..body.len], body);
    std.crypto.hash.Blake3.hash(body, encoded[body.len..][0..32], .{});
    var buffer: [224]u8 = undefined;
    try txn.put(try address(&buffer, admission, acquisition.effect.sequence), encoded);
    if (acquisition.effect.sequence > ledger.receipt_window) try txn.delete(try address(&buffer, admission, acquisition.effect.sequence - ledger.receipt_window));
    return result;
}
