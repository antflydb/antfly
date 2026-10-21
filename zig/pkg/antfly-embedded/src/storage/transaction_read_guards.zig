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

//! Existing transaction read-guard contract shared by admission and writers.
//! This does not expand the metadata predicate surface or admit required PREPARE.
const std = @import("std");
const keys = @import("internal_keys.zig");
const range_protection = @import("range_protection.zig");
pub const prefix = "\x00\x00__txn_read_guards__:";
pub const count_key = "\x00\x00__txn_read_guard_count";

pub fn isRawMetadataIntentKey(key: []const u8) bool {
    return range_protection.counterBucket(key) != null or
        range_protection.indexCounterDigest(key) != null or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:relational_integrity:") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_activation") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_generation_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:restore_staging_owner") or
        @import("db/relational_index_maintenance_contract.zig").isControlKey(key);
}

pub fn isRawMetadataPredicateKey(key: []const u8) bool {
    return isRawMetadataIntentKey(key) or
        std.mem.eql(u8, key, @import("db/relational_index_catalog.zig").head_key) or
        (key.len == @import("db/relational_index_jobs.zig").progress_prefix.len + range_protection.index_id_bytes and
            std.mem.startsWith(u8, key, @import("db/relational_index_jobs.zig").progress_prefix));
}

fn haveReaders(txn: anytype) !bool {
    const raw = txn.get(count_key) catch |err| return if (err == error.NotFound) false else err;
    if (raw.len != 8 or std.mem.readInt(u64, raw[0..8], .little) == 0) return error.InvalidTxnRecord;
    return true;
}

/// The shared mutation gate serializes capture with guard admission. Only keys
/// touched while guards exist need copies. A writer cannot install new guards
/// after skipping capture for an unguarded data mutation; split those phases.
/// Final-view validation permits atomic retirement of the resolver's own guards.
pub const Capture = struct {
    active: ?bool = null,
    unguarded_mutation: bool = false,
    failure: ?anyerror = null,
    logical_keys: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        var iterator = self.logical_keys.keyIterator();
        while (iterator.next()) |key| alloc.free(key.*);
        self.logical_keys.deinit(alloc);
    }
    pub fn check(self: *const Capture) !void {
        if (self.failure) |cause| return cause;
    }
    pub fn touch(self: *Capture, alloc: std.mem.Allocator, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, physical: []const u8) !void {
        try self.check();
        self.touchInner(alloc, txn, debt, gate, physical) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn touchInner(self: *Capture, alloc: std.mem.Allocator, txn: anytype, debt: anytype, gate: ?*std.atomic.Mutex, physical: []const u8) !void {
        const guard_metadata = std.mem.eql(u8, physical, count_key) or std.mem.startsWith(u8, physical, prefix);
        const protected = keys.isStoredDocumentRowKey(physical) or isRawMetadataPredicateKey(physical);
        if (!guard_metadata and !protected) return;
        // Acquire before observing even an absent count. Otherwise a concurrent
        // guard install could commit after that fast-path probe but before us.
        try debt.acquireMutationGate(gate);
        if (std.mem.eql(u8, physical, count_key)) {
            if (self.unguarded_mutation) return error.InvalidTxnRecord;
            self.active = null;
            return;
        }
        if (!protected) return;
        if (self.active == null) self.active = try haveReaders(txn);
        if (!self.active.?) {
            self.unguarded_mutation = true;
            return;
        }
        const logical = (try keys.decodeStoredDocumentRowKeyAlloc(alloc, physical)) orelse try alloc.dupe(u8, physical);
        errdefer alloc.free(logical);
        const entry = try self.logical_keys.getOrPut(alloc, logical);
        if (entry.found_existing) alloc.free(logical);
    }
    pub fn validate(self: *Capture, alloc: std.mem.Allocator, txn: anytype) !void {
        try self.check();
        self.validateInner(alloc, txn) catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn validateInner(self: *Capture, alloc: std.mem.Allocator, txn: anytype) !void {
        if (self.logical_keys.count() == 0 or !try haveReaders(txn)) return;
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        var iterator = self.logical_keys.keyIterator();
        while (iterator.next()) |logical| {
            const length = std.math.cast(u32, logical.*.len) orelse return error.TransactionTooLarge;
            const framed = try alloc.alloc(u8, prefix.len + 4 + logical.*.len);
            defer alloc.free(framed);
            @memcpy(framed[0..prefix.len], prefix);
            std.mem.writeInt(u32, framed[prefix.len..][0..4], length, .big);
            @memcpy(framed[prefix.len + 4 ..], logical.*);
            const entry = (try cursor.seekAtOrAfter(framed)) orelse continue;
            if (!std.mem.startsWith(u8, entry.key, framed)) continue;
            if (entry.key.len != framed.len + 16 or entry.value.len != 0) return error.InvalidTxnRecord;
            return error.IntentConflict;
        }
    }
};
