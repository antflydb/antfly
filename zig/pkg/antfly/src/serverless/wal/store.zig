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

const std = @import("std");
const Allocator = std.mem.Allocator;
const wal_types = @import("types.zig");

pub const WalStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        append: *const fn (*anyopaque, []const u8, u64, []const u8) anyerror!u64,
        append_batch: ?*const fn (*anyopaque, []const u8, u64, []const []const u8) anyerror!wal_types.BatchResult = null,
        append_batch_idempotent: ?*const fn (*anyopaque, []const u8, u64, []const []const u8, []const u8) anyerror!wal_types.BatchResult = null,
        append_idempotent: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8) anyerror!u64 = null,
        append_idempotent_if_latest: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8, u64) anyerror!?u64 = null,
        read_from_alloc: *const fn (*anyopaque, Allocator, []const u8, u64) anyerror![]wal_types.Record,
        read_range_alloc: ?*const fn (*anyopaque, Allocator, []const u8, wal_types.ReadRange) anyerror![]wal_types.Record = null,
        read_snapshot: ?*const fn (*anyopaque, []const u8) anyerror!wal_types.ReadSnapshot = null,
        read_point_alloc: ?*const fn (*anyopaque, Allocator, []const u8, []const u8, wal_types.ReadRange) anyerror!wal_types.PointRead = null,
        materialize_pending: ?*const fn (*anyopaque, []const u8, ?@import("objectstore").CancellationToken) anyerror!bool = null,
        latest_lsn: *const fn (*anyopaque, []const u8) anyerror!u64,
        truncate_prefix: *const fn (*anyopaque, []const u8, u64) anyerror!u64,
    };

    pub fn materializePending(self: *WalStore, namespace: []const u8, cancellation: ?@import("objectstore").CancellationToken) !bool {
        const call = self.vtable.materialize_pending orelse return false;
        return call(self.ptr, namespace, cancellation);
    }

    pub fn materializePendingCooperatively(self: *WalStore, namespace: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) !bool {
        try cancellation.check();
        const token = cancellation;
        const result = self.materializePending(namespace, .{ .ptr = &token, .is_cancelled_fn = struct {
            fn check(raw: *const anyopaque) bool {
                const value: *const @import("antfly_cancellation").CancellationToken = @ptrCast(@alignCast(raw));
                value.check() catch return true;
                return false;
            }
        }.check }) catch |err| {
            try cancellation.check();
            return err;
        };
        try cancellation.check();
        return result;
    }

    pub fn deinit(self: *WalStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn append(self: *WalStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        return try self.vtable.append(self.ptr, namespace, timestamp_ns, payload);
    }

    pub fn supportsBatchAppend(self: *const WalStore) bool {
        return self.vtable.append_batch != null;
    }
    /// One bounded atomic append without a retry identity. An ambiguous result
    /// remains ambiguous; callers needing replay must use appendBatchIdempotent.
    pub fn appendBatch(self: *WalStore, namespace: []const u8, timestamp: u64, payloads: []const []const u8) !wal_types.BatchResult {
        const call = self.vtable.append_batch orelse return error.AtomicBatchAppendUnsupported;
        return call(self.ptr, namespace, timestamp, payloads);
    }

    /// Append exactly once for a durable caller-supplied operation identity.
    /// Stores that cannot make that guarantee fail closed rather than silently
    /// degrading to an ambiguous ordinary append.
    pub fn appendIdempotent(
        self: *WalStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
    ) !u64 {
        const append_fn = self.vtable.append_idempotent orelse return error.IdempotentAppendUnsupported;
        return try append_fn(self.ptr, namespace, timestamp_ns, payload, operation_id);
    }

    /// An atomic batch requires a store-provided commit primitive. Never
    /// emulate it with independently acknowledged per-record appends.
    pub fn appendBatchIdempotent(self: *WalStore, namespace: []const u8, timestamp_ns: u64, payloads: []const []const u8, operation_id: []const u8) !wal_types.BatchResult {
        const append_fn = self.vtable.append_batch_idempotent orelse return error.IdempotentBatchAppendUnsupported;
        return append_fn(self.ptr, namespace, timestamp_ns, payloads, operation_id);
    }

    pub fn supportsIdempotentAppend(self: *const WalStore) bool {
        return self.vtable.append_idempotent != null;
    }

    /// Append exactly once only if no WAL record has landed since the caller's
    /// snapshot. The latest-LSN check and append must share the store's atomic
    /// write primitive; returning null means the snapshot fence was lost.
    pub fn appendIdempotentIfLatest(
        self: *WalStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        const append_fn = self.vtable.append_idempotent_if_latest orelse
            return error.ConditionalIdempotentAppendUnsupported;
        return try append_fn(
            self.ptr,
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
    }

    pub fn supportsConditionalIdempotentAppend(self: *const WalStore) bool {
        return self.vtable.append_idempotent_if_latest != null;
    }

    pub fn readFromAlloc(self: *WalStore, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        return try self.vtable.read_from_alloc(self.ptr, self.allocator, namespace, start_lsn);
    }

    /// No unbounded fallback: an old provider must opt into bounded I/O.
    pub fn readRangeAlloc(self: *WalStore, namespace: []const u8, range: wal_types.ReadRange) ![]wal_types.Record {
        try range.check();
        if (range.start_lsn > range.end_lsn) return self.allocator.alloc(wal_types.Record, 0);
        const read = self.vtable.read_range_alloc orelse return error.BoundedWalReadUnsupported;
        return read(self.ptr, self.allocator, namespace, range);
    }

    pub fn readSnapshot(self: *WalStore, namespace: []const u8) !?wal_types.ReadSnapshot {
        const read = self.vtable.read_snapshot orelse return null;
        return try read(self.ptr, namespace);
    }
    pub fn readPointAlloc(self: *WalStore, namespace: []const u8, id: []const u8, range: wal_types.ReadRange) !wal_types.PointRead {
        try range.check();
        const read = self.vtable.read_point_alloc orelse return .{};
        return read(self.ptr, self.allocator, namespace, id, range);
    }

    pub fn latestLsn(self: *WalStore, namespace: []const u8) !u64 {
        return try self.vtable.latest_lsn(self.ptr, namespace);
    }

    pub fn truncatePrefix(self: *WalStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        return try self.vtable.truncate_prefix(self.ptr, namespace, keep_from_lsn);
    }
};
