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
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const Allocator = std.mem.Allocator;
const wal_types = @import("types.zig");
const Execution = @import("antfly_local_sources").common_request_admission.runnable_scheduler.Execution;

pub const WalStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    context: ?@import("antfly_local_sources").serverless_query_lake_read_context.Context = null,

    pub const VTable = struct {
        append_for: ?*const fn (*anyopaque, []const u8, u64, []const u8, Context) anyerror!u64 = null,
        append_idempotent_for: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8, Context) anyerror!u64 = null,
        append_idempotent_if_latest_for: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8, u64, Context) anyerror!?u64 = null,
        latest_lsn_for: ?*const fn (*anyopaque, []const u8, Context) anyerror!u64 = null,
        read_from_alloc_for: ?*const fn (*anyopaque, Allocator, []const u8, u64, Context) anyerror![]wal_types.Record = null,
        deinit: *const fn (Allocator, *anyopaque) void,
        append: *const fn (*anyopaque, []const u8, u64, []const u8) anyerror!u64,
        append_with_execution: ?*const fn (*anyopaque, []const u8, u64, []const u8, *Execution) anyerror!u64 = null,
        append_idempotent: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8) anyerror!u64 = null,
        append_idempotent_if_latest: ?*const fn (*anyopaque, []const u8, u64, []const u8, []const u8, u64) anyerror!?u64 = null,
        read_from_alloc: *const fn (*anyopaque, Allocator, []const u8, u64) anyerror![]wal_types.Record,
        latest_lsn: *const fn (*anyopaque, []const u8) anyerror!u64,
        truncate_prefix_for: ?*const fn (*anyopaque, []const u8, u64, Context) anyerror!u64 = null,
        truncate_prefix: *const fn (*anyopaque, []const u8, u64) anyerror!u64,
    };

    pub fn deinit(self: *WalStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn append(self: *WalStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        if (self.context) |context| if (self.vtable.append_for) |call| return call(self.ptr, namespace, timestamp_ns, payload, context);
        return try self.vtable.append(self.ptr, namespace, timestamp_ns, payload);
    }

    /// Converted stores may loan CPU only at their verified wait boundaries.
    /// Legacy providers retain the grant throughout their physical operation.
    pub fn appendWithExecution(self: *WalStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8, execution: *Execution) !u64 {
        if (self.context) |context| if (self.vtable.append_for) |call| return call(self.ptr, namespace, timestamp_ns, payload, context.withExecution(execution));
        if (self.vtable.append_with_execution) |append_fn| return append_fn(self.ptr, namespace, timestamp_ns, payload, execution);
        return self.append(namespace, timestamp_ns, payload);
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
        if (self.context) |context| if (self.vtable.append_idempotent_for) |call| return call(self.ptr, namespace, timestamp_ns, payload, operation_id, context);
        const append_fn = self.vtable.append_idempotent orelse return error.IdempotentAppendUnsupported;
        return try append_fn(self.ptr, namespace, timestamp_ns, payload, operation_id);
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
        if (self.context) |context| if (self.vtable.append_idempotent_if_latest_for) |call| return call(self.ptr, namespace, timestamp_ns, payload, operation_id, expected_latest_lsn, context);
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
        if (self.context) |context| if (self.vtable.read_from_alloc_for != null) {
            return self.vtable.read_from_alloc_for.?(self.ptr, self.allocator, namespace, start_lsn, context);
        };
        return try self.vtable.read_from_alloc(self.ptr, self.allocator, namespace, start_lsn);
    }

    pub fn latestLsn(self: *WalStore, namespace: []const u8) !u64 {
        if (self.context) |context| if (self.vtable.latest_lsn_for != null) {
            return self.vtable.latest_lsn_for.?(self.ptr, namespace, context);
        };
        return try self.vtable.latest_lsn(self.ptr, namespace);
    }

    pub fn truncatePrefix(self: *WalStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        if (self.context) |context| if (self.vtable.truncate_prefix_for) |call| return call(self.ptr, namespace, keep_from_lsn, context);
        return try self.vtable.truncate_prefix(self.ptr, namespace, keep_from_lsn);
    }
};
