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

//! Durable publication debt admission. The aggregate and the obligations share
//! the backend writer transaction: abort, uncertain commit, and reopen cannot
//! retire a credit independently of its record. No postcommit reservation exists.
const std = @import("std");
const outbox = @import("db/durable_outbox.zig");
pub const state_key = "\x00\x00__metadata__:ha_publication_backlog_v1";
pub const size_prefix = "\x00\x00__metadata__:ha_publication_size_v1:";
pub const transaction_batch_prefix = "\x00\x00__txn_ha_batch_outbox__:";
// Immutable generated effects retained between final preparation and resolution.
pub const preparation_requirement_prefix = "\x00\x00__txn_preparation_requirement_v1__:";
pub const prepared_effects_prefix = "\x00\x00__txn_prepared_effects_v1__:";
pub const transaction_credit_prefix = "\x00\x00__txn_ha_batch_credit_v1__:";
const transaction_credit_initialized = "\x00\x00__metadata__:ha_txn_batch_credit_v1";

pub fn transactionCreditKey(id: [16]u8) [transaction_credit_prefix.len + 16]u8 {
    return transaction_credit_prefix.* ++ id;
}

/// JSON strings expand at most six bytes per input byte; binary byte-array
/// encoding expands at most four. Intent admission also includes 4096 bytes
/// per key and 16 copies of the key. The fixed envelope allowance is checked
/// against the actual transaction request encoder in integration tests.
/// This funds the batch outbox only, not derived replay or append working RAM.
pub fn transactionBatchBudget(intent_bytes: u64) !u64 {
    return std.math.add(u64, 64 * 1024, std.math.mul(u64, intent_bytes, 6) catch return error.TransactionTooLarge) catch error.TransactionTooLarge;
}

pub fn sizeKey(key: []const u8) [size_prefix.len + 32]u8 {
    var result: [size_prefix.len + 32]u8 = undefined;
    @memcpy(result[0..size_prefix.len], size_prefix);
    std.crypto.hash.Blake3.hash(key, result[size_prefix.len..][0..32], .{});
    return result;
}

pub const default_max_records: u64 = 4096;
pub const default_max_bytes: u64 = 256 * 1024 * 1024;

pub fn isObligation(key: []const u8) bool {
    // Conservatively include malformed v2 keys: corruption must not escape
    // accounting simply because recovery cannot decode its kind or identity.
    return std.mem.startsWith(u8, key, preparation_requirement_prefix) or
        std.mem.startsWith(u8, key, prepared_effects_prefix) or
        std.mem.startsWith(u8, key, transaction_credit_prefix) or
        std.mem.startsWith(u8, key, transaction_batch_prefix) or
        std.mem.startsWith(u8, key, outbox.replication_outbox_v2_prefix) or
        std.mem.eql(u8, key, outbox.replication_batch_outbox_key) or
        std.mem.eql(u8, key, outbox.replication_replay_outbox_key) or
        std.mem.eql(u8, key, outbox.replication_schema_outbox_key);
}

pub const State = struct {
    records: u64 = 0,
    bytes: u64 = 0,
    pub fn admit(self: State, records_delta: i128, bytes_delta: i128) !State {
        const records = std.math.cast(u64, @as(i128, self.records) + records_delta) orelse return error.InvalidHAOutbox;
        const bytes = std.math.cast(u64, @as(i128, self.bytes) + bytes_delta) orelse return error.InvalidHAOutbox;
        // Existing over-limit debt may shrink; recovery must never require a
        // fresh backlog grant. Admission checks the final atomic batch, so a
        // delete-and-replace can reuse credit regardless of operation order.
        if ((records > default_max_records and records > self.records) or
            (bytes > default_max_bytes and bytes > self.bytes)) return error.Backpressured;
        return .{ .records = records, .bytes = bytes };
    }
    pub fn encode(self: State) [52]u8 {
        var raw: [52]u8 = undefined;
        @memcpy(raw[0..4], "HPB1");
        std.mem.writeInt(u64, raw[4..12], self.records, .little);
        std.mem.writeInt(u64, raw[12..20], self.bytes, .little);
        std.crypto.hash.Blake3.hash(raw[0..20], raw[20..52], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != 52 or !std.mem.eql(u8, raw[0..4], "HPB1")) return error.InvalidHAOutbox;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..20], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[20..52])) return error.InvalidHAOutbox;
        const state: State = .{ .records = std.mem.readInt(u64, raw[4..12], .little), .bytes = std.mem.readInt(u64, raw[12..20], .little) };
        if ((state.records == 0) != (state.bytes == 0)) return error.InvalidHAOutbox;
        return state;
    }
};

pub fn load(txn: anytype) !?State {
    const raw = txn.get(state_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try State.decode(raw);
}

/// Bounded metadata seeks, never a legacy value scan. Physical index/block work is still
/// synchronous and is not claimed as a cooperative CPU quantum.
fn hasObligations(txn: anytype) !bool {
    var cursor = try txn.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    for ([_][]const u8{ outbox.replication_batch_outbox_key, outbox.replication_replay_outbox_key, outbox.replication_schema_outbox_key }) |key| {
        if (try cursor.seekAtOrAfter(key)) |entry| if (std.mem.eql(u8, entry.key, key)) return true;
    }
    if (try cursor.seekAtOrAfter(outbox.replication_outbox_v2_prefix)) |entry|
        if (std.mem.startsWith(u8, entry.key, outbox.replication_outbox_v2_prefix)) return true;
    for ([_][]const u8{ transaction_credit_prefix, transaction_batch_prefix, prepared_effects_prefix, preparation_requirement_prefix }) |prefix| {
        if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return true;
    }
    return false;
}

fn initializeTransactionCredits(txn: anytype) !void {
    if (txn.get(transaction_credit_initialized)) |raw| {
        if (!std.mem.eql(u8, raw, "HTC1")) return error.InvalidHAOutbox;
        return;
    } else |err| if (err != error.NotFound) return err;
    // Install before the first new intent is staged. Legacy prepared votes
    // must resolve without a new grant; fence new prepares until they drain.
    var cursor = try txn.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    for ([_][]const u8{ "\x00\x00__txn_intents__:", transaction_batch_prefix, transaction_credit_prefix }) |prefix| {
        if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return error.Backpressured;
    }
    try txn.put(transaction_credit_initialized, "HTC1");
}

/// Constant-space capture uses the transaction's own ordered view, including
/// previous writes/deletes in the same batch. Failed obligation mutations poison
/// the capture so ignoring a storage failure cannot commit phantom credits.
pub const Capture = struct {
    gate: ?*std.atomic.Mutex = null,
    initial: State = .{},
    loaded: bool = false,
    complete: bool = false,
    poisoned: bool = false,
    records_delta: i128 = 0,
    bytes_delta: i128 = 0,

    fn acquire(self: *Capture, gate: ?*std.atomic.Mutex) !void {
        if (self.gate != null) return;
        const mutex = gate orelse return error.Backpressured;
        if (!mutex.tryLock()) return error.Backpressured;
        self.gate = mutex;
    }

    /// Serialize related artifact accounting with publication-credit RMW.
    /// The existing transaction owner releases this gate on commit or abort.
    pub fn acquireMutationGate(self: *Capture, gate: ?*std.atomic.Mutex) !void {
        try self.acquire(gate);
    }

    pub fn touch(self: *Capture, txn: anytype, gate: ?*std.atomic.Mutex, key: []const u8, value: ?[]const u8) !void {
        if (!isObligation(key)) return;
        errdefer self.poisoned = true;
        if (self.poisoned) return error.InvalidHAOutbox;
        try self.acquire(gate);
        if (std.mem.startsWith(u8, key, transaction_batch_prefix) and key.len == transaction_batch_prefix.len + 16) {
            if (value) |payload| {
                const credit_key = transactionCreditKey(key[transaction_batch_prefix.len..][0..16].*);
                if (txn.get(&credit_key)) |raw| {
                    if (raw.len != 8 or payload.len > std.mem.readInt(u64, raw[0..8], .little)) return error.InvalidHAOutbox;
                } else |err| {
                    if (err != error.NotFound) return err;
                    // Untracked old prepares retain their prior resolution
                    // policy. They cannot coexist with new tracked prepares.
                    if (txn.get(transaction_credit_initialized)) |_| return error.InvalidHAOutbox else |cause| if (cause != error.NotFound) return cause;
                }
            }
            // The reservation remains charged until successful HA delivery.
            // Materialization never acquires another byte or record credit.
            return;
        }
        const credit = std.mem.startsWith(u8, key, transaction_credit_prefix);
        const charged_size: ?u64 = if (value) |raw| if (credit) blk: {
            if (key.len != transaction_credit_prefix.len + 16 or raw.len != 8) return error.InvalidHAOutbox;
            const budget = std.mem.readInt(u64, raw[0..8], .little);
            if (budget == 0) return error.InvalidHAOutbox;
            try initializeTransactionCredits(txn);
            break :blk budget;
        } else raw.len else null;
        if (!self.loaded) {
            if (try load(txn)) |state| {
                self.initial = state;
                self.complete = true;
            } else self.complete = !try hasObligations(txn);
            self.loaded = true;
        }
        // One fixed-size sidecar per obligation avoids rereading a potentially
        // huge cold payload merely to retire its byte credit. Sidecars and the
        // aggregate are staged in this same writer transaction. A missing
        // sidecar is absent credit in a fully accounted root; raw backend writes
        // are not allowed to bypass the DocStore mutation contract.
        const size_key = sizeKey(key);
        const previous: ?u64 = if (txn.get(&size_key)) |raw| previous: {
            if (raw.len != 8) return error.InvalidHAOutbox;
            break :previous std.mem.readInt(u64, raw[0..8], .little);
        } else |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        // Old roots can drain without allocating a migration scan or guessing
        // a partial count. Their first empty boundary establishes exact credit.
        if (!self.complete and value != null) return error.Backpressured;
        if (value) |raw| {
            var size: [8]u8 = undefined;
            _ = raw;
            std.mem.writeInt(u64, &size, charged_size.?, .little);
            try txn.put(&size_key, &size);
        } else if (previous != null) try txn.delete(&size_key);
        if (!self.complete) return;
        self.records_delta += @as(i128, @intFromBool(value != null)) - @as(i128, @intFromBool(previous != null));
        self.bytes_delta += (if (charged_size) |len| @as(i128, key.len) + len else @as(i128, 0)) -
            (if (previous) |len| @as(i128, key.len) + len else @as(i128, 0));
    }

    pub fn release(self: *Capture) void {
        if (self.gate) |gate| gate.unlock();
        self.gate = null;
    }

    pub fn stage(self: *Capture, txn: anytype) !void {
        if (self.poisoned) return error.InvalidHAOutbox;
        if (!self.loaded) return;
        if (!self.complete) {
            if (!try hasObligations(txn)) try txn.put(state_key, &(State{}).encode());
            return;
        }
        const next = try self.initial.admit(self.records_delta, self.bytes_delta);
        try txn.put(state_key, &next.encode());
    }
};
