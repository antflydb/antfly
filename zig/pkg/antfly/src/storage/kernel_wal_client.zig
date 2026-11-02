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

//! Storage-free client for a complete durable WAL owned by the compiled
//! storage kernel. Calls remain at logical WAL-operation granularity; no
//! backend transaction, cursor, key, or LSM record crosses this ABI.

const std = @import("std");
const builtin = @import("builtin");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const wire = @import("kernel_wal_wire.zig");

pub const WalEntry = struct {
    lsn: u64,
    data: []const u8,
};

pub const WalScanAction = enum {
    @"continue",
    stop,
};

pub const WalStats = abi.WalStats;
pub const RecoveryPosition = abi.WalRecoveryPosition;
pub const RecoveryAppendResult = union(enum) { changed, busy, appended: u64 };

fn validRecoveryPosition(value: RecoveryPosition) bool {
    return value.origin != 0 and value.owner != 0 and value.revision != 0 and value.next_lsn != 0;
}

const RecoveryRelay = struct {
    original: @import("antfly_local_sources").common_read_admission.Port,
    failure: ?anyerror = null,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |err| return err;
        self.original.checkLifetime() catch |err| {
            self.failure = err;
            return err;
        };
    }
    fn payload(self: *@This()) @import("antfly_local_sources").common_read_admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check } };
    }
};

pub const CommitBackend = enum { sync, worker_thread, async_io, adaptive };
pub const StorageBackend = enum { lmdb, lsm, lsm_memory };

/// The opaque owner accepts logical WAL policy, not native backend handles.
/// Keep the control-side options independent of LMDB and physical LSM types.
pub const WalOptions = struct {
    no_sync: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    backend: ?StorageBackend = null,
    read_only: bool = false,
    clock: @import("antfly_local_sources").storage_sim_runtime.Clock = @import("antfly_local_sources").storage_sim_runtime.real_clock,
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: WalOptions) StorageBackend {
        return self.backend orelse .lsm;
    }
};

const MemoryBridge = struct { allocator: std.mem.Allocator };

const Reply = struct {
    result: abi.WalReadResult = .{},
    allocator: ?std.mem.Allocator,
    fn deinit(self: *@This()) void {
        freeReply(self.allocator, &self.result.buffer);
    }
};
const Page = struct {
    result: abi.WalScanResult = .{},
    allocator: ?std.mem.Allocator,
    fn deinit(self: *@This()) void {
        freeReply(self.allocator, &self.result.entries);
    }
};
fn freeReply(allocator: ?std.mem.Allocator, bytes: *abi.OwnedBytes) void {
    if (allocator) |owner| {
        if (bytes.ptr) |ptr| owner.free(ptr[0..@intCast(bytes.len)]);
        bytes.* = .{};
    } else abi.antfly_storage_owner_buffer_destroy(bytes);
}

pub const WAL = struct {
    memory_bridge: ?*MemoryBridge = null,
    clock: @import("antfly_local_sources").storage_sim_runtime.Clock,
    handle: ?*anyopaque = null,

    pub const ScanAction = WalScanAction;

    pub fn open(path: [*:0]const u8, opts: anytype) !WAL {
        try validateOptions(opts);
        var result = abi.WalOpenResult{};
        try statusToError(abi.antfly_storage_wal_open(&.{
            .path = .fromSlice(std.mem.span(path)),
            .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
            .group_commit_window_ns = opts.group_commit_window_ns,
            .group_commit_max_requests = @intCast(opts.group_commit_max_requests),
            .commit_backend = commitBackendRaw(opts.commit_backend),
            .no_sync = @intFromBool(opts.no_sync),
            .read_only = @intFromBool(opts.read_only),
            .model_commit_backend_completions = @intFromBool(opts.model_commit_backend_completions),
        }, &result));
        return .{
            .handle = result.handle orelse return error.StorageKernelFailure,
            .clock = opts.clock,
        };
    }

    /// The allocator context must outlive close. The bridge is rooted in that
    /// allocator and keeps the callback context stable across WAL value moves.
    pub fn openWithAllocator(allocator: std.mem.Allocator, path: [*:0]const u8, opts: anytype) !WAL {
        try validateOptions(opts);
        const bridge = try allocator.create(MemoryBridge);
        errdefer allocator.destroy(bridge);
        bridge.* = .{ .allocator = allocator };
        const memory = @import("runtime_memory_abi").Allocator.fromStd(&bridge.allocator);
        const options: abi.WalOpenRequest = .{
            .path = .fromSlice(std.mem.span(path)),
            .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
            .group_commit_window_ns = opts.group_commit_window_ns,
            .group_commit_max_requests = @intCast(opts.group_commit_max_requests),
            .commit_backend = commitBackendRaw(opts.commit_backend),
            .no_sync = @intFromBool(opts.no_sync),
            .read_only = @intFromBool(opts.read_only),
            .model_commit_backend_completions = @intFromBool(opts.model_commit_backend_completions),
        };
        var result: abi.WalOpenResult = .{};
        try statusToError(abi.antfly_storage_wal_open_with_allocator(&.{ .options = &options, .allocator = &memory }, &result));
        return .{ .handle = result.handle orelse return error.StorageKernelFailure, .clock = opts.clock, .memory_bridge = bridge };
    }

    pub fn close(self: *WAL) void {
        abi.antfly_storage_wal_close(self.handle);
        if (self.memory_bridge) |bridge| bridge.allocator.destroy(bridge);
        self.* = undefined;
    }

    /// This stable bridge owns its metadata independently of WAL.close. The
    /// caller allocator context must outlive destroy; no control is retained.
    /// Serialize calls on this client handle. Concurrent WAL.close is allowed.
    pub const OwnedRead = struct {
        pub const Status = enum { pending, ready };
        pub const Chunk = struct { len: usize, done: bool };
        allocator: std.mem.Allocator,
        handle: ?*anyopaque = null,
        failure: ?anyerror = null,

        fn fail(self: *@This(), cause: anyerror) anyerror {
            if (self.handle) |handle| abi.antfly_storage_wal_owned_read_destroy(handle);
            self.handle = null;
            self.failure = cause;
            return cause;
        }
        fn receive(self: *@This(), status: abi.Status, relay: *const @import("antfly_local_sources").common_read_admission.ErrorRelay) !void {
            if (relay.rejection) |err| return self.fail(err);
            statusToError(status) catch |err| {
                if (err == error.Backpressured or err == error.InvalidArgument) return err;
                return self.fail(err);
            };
        }
        pub fn step(self: *@This(), quantum: usize, original: @import("antfly_local_sources").common_read_admission.Port) !Status {
            if (quantum == 0 or quantum > 64 * 1024) return error.InvalidArgument;
            if (self.failure) |err| return err;
            var relay: @import("antfly_local_sources").common_read_admission.ErrorRelay = .{};
            const control = relay.bind(original) catch |err| return self.fail(err);
            control.checkpoint() catch |err| return self.fail(relay.rejection orelse err);
            var output: abi.WalOwnedReadStepResult = .{};
            try self.receive(abi.antfly_storage_wal_owned_read_step(self.handle, &.{ .quantum = quantum, .port = &control }, &output), &relay);
            control.checkpoint() catch |err| return self.fail(relay.rejection orelse err);
            if (output.ready > 1 or !std.mem.allEqual(u8, &output._reserved, 0)) return self.fail(error.StorageKernelFailure);
            return if (output.ready == 1) .ready else .pending;
        }
        pub fn readChunk(self: *@This(), offset: usize, out: []u8, original: @import("antfly_local_sources").common_read_admission.Port) !Chunk {
            if (out.len > 64 * 1024) return error.InvalidArgument;
            if (self.failure) |err| return err;
            var relay: @import("antfly_local_sources").common_read_admission.ErrorRelay = .{};
            const control = relay.bind(original) catch |err| return self.fail(err);
            control.checkpoint() catch |err| return self.fail(relay.rejection orelse err);
            var output: abi.WalOwnedReadChunkResult = .{};
            try self.receive(abi.antfly_storage_wal_owned_read_chunk(self.handle, &.{ .offset = offset, .output = .{ .ptr = out.ptr, .len = out.len }, .port = &control }, &output), &relay);
            control.checkpoint() catch |err| return self.fail(relay.rejection orelse err);
            if (output.done > 1 or output.len > out.len or !std.mem.allEqual(u8, &output._reserved, 0) or
                (output.done == 0 and (output.len == 0 or output.len != out.len))) return self.fail(error.StorageKernelFailure);
            return .{ .len = @intCast(output.len), .done = output.done == 1 };
        }
        pub fn destroy(self: *@This()) void {
            if (self.handle) |handle| abi.antfly_storage_wal_owned_read_destroy(handle);
            const allocator = self.allocator;
            allocator.destroy(self);
        }
    };

    pub fn beginOwnedRead(self: *WAL, allocator: std.mem.Allocator, lsn: u64, original: @import("antfly_local_sources").common_read_admission.Port) !*OwnedRead {
        if (lsn == 0) return error.InvalidLsn;
        var relay: @import("antfly_local_sources").common_read_admission.ErrorRelay = .{};
        const control = try relay.bind(original);
        control.checkpoint() catch |err| return relay.rejection orelse err;
        const operation = try allocator.create(OwnedRead);
        operation.* = .{ .allocator = allocator };
        errdefer operation.destroy();
        const memory = @import("runtime_memory_abi").Allocator.fromStd(&operation.allocator);
        var output: abi.WalOwnedReadBeginResult = .{};
        const status = abi.antfly_storage_wal_owned_read_begin(self.handle, &.{ .lsn = lsn, .allocator = &memory, .port = &control }, &output);
        // Retire even a populated failure reply or ignored original rejection.
        operation.handle = output.handle;
        if (relay.rejection) |err| return err;
        try statusToError(status);
        control.checkpoint() catch |err| return relay.rejection orelse err;
        if (operation.handle == null) return error.StorageKernelFailure;
        return operation;
    }

    pub fn append(self: *WAL, data: []const u8) !u64 {
        var result = abi.WalAppendResult{};
        try statusToError(abi.antfly_storage_wal_append(self.handle, &.{
            .expected_next_lsn = 0,
            .data = .fromSlice(data),
        }, &result));
        const expected_next_lsn = try std.math.add(u64, result.assigned_lsn, 1);
        if (result.next_lsn != expected_next_lsn)
            return error.StorageKernelFailure;
        return result.assigned_lsn;
    }

    /// The WAL and allocator descriptor outlive the grant. Retire it only
    /// after publish has joined; the owner enforces a single publication.
    pub const PreparedAppend = struct {
        handle: ?*anyopaque,
        pub fn publish(self: *PreparedAppend, prefix: []const u8) !u64 {
            var output: abi.WalAppendResult = .{};
            try statusToError(abi.antfly_storage_wal_prepared_publish(self.handle, &.{ .prefix = .fromSlice(prefix) }, &output));
            if (output.assigned_lsn == 0 or output.next_lsn != (std.math.add(u64, output.assigned_lsn, 1) catch return error.StorageKernelFailure)) return error.StorageKernelFailure;
            return output.assigned_lsn;
        }
        pub fn deinit(self: *PreparedAppend) void {
            abi.antfly_storage_wal_prepared_destroy(self.handle);
            self.* = undefined;
        }
    };

    pub fn prepareAppend(self: *WAL, prefix_len: usize, tail: []const u8, original: ?@import("antfly_local_sources").common_read_admission.Port) !PreparedAppend {
        if (prefix_len > 4096) return error.InvalidArgument;
        var relay: RecoveryRelay = .{ .original = original orelse undefined };
        const payload = relay.payload();
        const port = @import("antfly_local_sources").common_read_admission.Port.init(&payload);
        if (original != null) try port.checkpoint();
        var output: abi.WalPrepareAppendResult = .{};
        const status = abi.antfly_storage_wal_prepare_append(self.handle, &.{ .prefix_len = @intCast(prefix_len), .tail = .fromSlice(tail), .port = if (original != null) &port else null }, &output);
        if (relay.failure) |cause| {
            if (output.handle) |owned| abi.antfly_storage_wal_prepared_destroy(owned);
            return cause;
        }
        if (original != null) port.checkpoint() catch |cause| {
            if (output.handle) |owned| abi.antfly_storage_wal_prepared_destroy(owned);
            return cause;
        };
        try statusToError(status);
        return .{ .handle = output.handle orelse return error.StorageKernelFailure };
    }

    pub fn appendAt(self: *WAL, expected_lsn: u64, data: []const u8) !u64 {
        const expected_next_lsn = try std.math.add(u64, expected_lsn, 1);
        var result = abi.WalAppendResult{};
        try statusToError(abi.antfly_storage_wal_append(self.handle, &.{
            .reposition_if_empty = 1,
            .expected_next_lsn = expected_lsn,
            .data = .fromSlice(data),
        }, &result));
        if (result.assigned_lsn != expected_lsn or result.next_lsn != expected_next_lsn)
            return error.StorageKernelFailure;
        return result.assigned_lsn;
    }

    pub fn tryRecoveryPosition(self: *WAL, original: ?@import("antfly_local_sources").common_read_admission.Port) !?RecoveryPosition {
        const read_control = @import("antfly_local_sources").common_read_admission;
        var relay: RecoveryRelay = .{ .original = original orelse return error.ExecutionWaitUnsupported };
        const payload = relay.payload();
        const control = read_control.Port.init(&payload);
        try control.checkpoint();
        var output: abi.WalRecoveryPositionResult = .{};
        const status = abi.antfly_storage_wal_recovery_position(self.handle, &.{ .port = &control }, &output);
        if (relay.failure) |err| return err;
        try statusToError(status);
        try control.checkpoint();
        if (!std.mem.allEqual(u8, &output._reserved, 0) or output.busy > 1) return error.StorageKernelFailure;
        if (output.busy == 1) {
            if (!std.meta.eql(output.position, RecoveryPosition{})) return error.StorageKernelFailure;
            return null;
        }
        if (!validRecoveryPosition(output.position)) return error.StorageKernelFailure;
        return output.position;
    }

    pub fn appendIfRecoveryPosition(self: *WAL, expected: RecoveryPosition, data: []const u8, original: ?@import("antfly_local_sources").common_read_admission.Port) !RecoveryAppendResult {
        if (!validRecoveryPosition(expected)) return error.InvalidArgument;
        const read_control = @import("antfly_local_sources").common_read_admission;
        var relay: RecoveryRelay = .{ .original = original orelse return error.ExecutionWaitUnsupported };
        const payload = relay.payload();
        const control = read_control.Port.init(&payload);
        try control.checkpoint();
        var output: abi.WalRecoveryAppendResult = .{};
        const status = abi.antfly_storage_wal_append_if_recovery_position(self.handle, &.{ .expected = expected, .data = .fromSlice(data), .port = &control }, &output);
        if (relay.failure) |err| return err;
        try statusToError(status);
        // No caller checkpoint here: the owner may have committed the record.
        if (!std.mem.allEqual(u8, &output._reserved, 0)) return error.StorageKernelFailure;
        switch (output.outcome) {
            0, 1 => {
                if (output.assigned_lsn != 0 or output.next_lsn != 0) return error.StorageKernelFailure;
                return if (output.outcome == 0) .changed else .busy;
            },
            2 => {
                const next = std.math.add(u64, expected.next_lsn, 1) catch return error.StorageKernelFailure;
                if (output.assigned_lsn != expected.next_lsn or output.next_lsn != next) return error.StorageKernelFailure;
                return .{ .appended = output.assigned_lsn };
            },
            else => return error.StorageKernelFailure,
        }
    }

    pub fn sync(self: *WAL, force: bool) !void {
        try statusToError(abi.antfly_storage_wal_sync(self.handle, &.{
            .force = @intFromBool(force),
        }));
    }

    pub fn appendIdempotent(self: *WAL, key: []const u8, digest: []const u8, data: []const u8) !struct { lsn: u64, appended: bool } {
        var result = abi.WalIdempotentAppendResult{};
        try statusToError(abi.antfly_storage_wal_append_idempotent(self.handle, &.{
            .key = .fromSlice(key),
            .digest = .fromSlice(digest),
            .data = .fromSlice(data),
        }, &result));
        if (result.lsn == 0 or result.appended > 1) return error.StorageKernelFailure;
        return .{ .lsn = result.lsn, .appended = result.appended != 0 };
    }

    pub fn truncate(self: *WAL, up_to_lsn: u64) !void {
        try statusToError(abi.antfly_storage_wal_truncate_prefix(self.handle, &.{
            .lsn = up_to_lsn,
        }));
    }

    pub fn truncateAfter(self: *WAL, keep_lsn: u64) !void {
        _ = try std.math.add(u64, keep_lsn, 1);
        var next_lsn: u64 = 0;
        try statusToError(abi.antfly_storage_wal_truncate_suffix(self.handle, &.{
            .lsn = keep_lsn,
        }, &next_lsn));
        if (next_lsn == 0) return error.StorageKernelFailure;
    }

    pub fn statsSnapshot(self: *WAL) WalStats {
        var stats = WalStats{};
        statusToError(abi.antfly_storage_wal_stats_snapshot(self.handle, &stats)) catch |err|
            std.debug.panic("storage WAL stats snapshot failed: {s}", .{@errorName(err)});
        return stats;
    }

    pub fn iterateFrom(self: *WAL, alloc: std.mem.Allocator, from_lsn: u64) ![]WalEntry {
        var entries = std.ArrayListUnmanaged(WalEntry).empty;
        errdefer {
            for (entries.items) |entry| alloc.free(@constCast(entry.data));
            entries.deinit(alloc);
        }
        var cursor = from_lsn;
        while (true) {
            var page = try self.scanPage(cursor);
            defer page.deinit();
            const parsed = try parsePage(alloc, page.result.entries.slice());
            defer alloc.free(parsed);
            for (parsed) |source| {
                const data = try alloc.dupe(u8, source.data);
                errdefer alloc.free(data);
                try entries.append(alloc, .{ .lsn = source.lsn, .data = data });
            }
            if (page.result.done != 0) break;
            if (page.result.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.result.next_lsn;
        }
        return entries.toOwnedSlice(alloc);
    }

    pub fn readAt(self: *WAL, alloc: std.mem.Allocator, lsn: u64) !?WalEntry {
        var reply: Reply = .{ .allocator = if (self.memory_bridge) |bridge| bridge.allocator else null };
        defer reply.deinit();
        try statusToError(self.dispatchRead(lsn, null, &reply));
        const result = &reply.result;
        if (result.buffer.len != 0 and result.buffer.ptr == null) return error.StorageKernelFailure;
        if (result.found == 0) {
            if (result.buffer.len != 0) return error.StorageKernelFailure;
            return null;
        }
        if (result.found != 1) return error.StorageKernelFailure;
        return .{
            .lsn = lsn,
            .data = try alloc.dupe(u8, result.buffer.slice()),
        };
    }

    /// Borrow the compiled owner's result through the callback, avoiding a
    /// second payload allocation in the consumer. The owner buffer is retired
    /// on success, missing entries, malformed replies and callback failure.
    pub fn withEntryAt(self: *WAL, lsn: u64, context: anytype, comptime callback: fn (@TypeOf(context), WalEntry) anyerror!void) !bool {
        return self.withEntryAtWithAdmission(lsn, null, context, callback);
    }

    pub fn withEntryAtWithAdmission(self: *WAL, lsn: u64, original: ?@import("antfly_local_sources").common_read_admission.Port, context: anytype, comptime callback: fn (@TypeOf(context), WalEntry) anyerror!void) !bool {
        const read_control = @import("antfly_local_sources").common_read_admission;
        const Relay = struct {
            original: read_control.Port,
            failure: ?anyerror = null,
            fn check(raw: *anyopaque) !void {
                const self_relay: *@This() = @ptrCast(@alignCast(raw));
                if (self_relay.failure) |err| return err;
                self_relay.original.checkLifetime() catch |err| {
                    self_relay.failure = err;
                    return err;
                };
            }
        };
        var reply: Reply = .{ .allocator = if (self.memory_bridge) |bridge| bridge.allocator else null };
        defer reply.deinit();
        const result = &reply.result;
        if (original) |port| {
            var relay: Relay = .{ .original = port };
            const payload: read_control.Payload = .{ .ptr = &relay, .vtable = &.{ .checkpoint = Relay.check, .check_lifetime = Relay.check } };
            const control = read_control.Port.init(&payload);
            try control.checkpoint();
            const status = self.dispatchRead(lsn, &control, &reply);
            if (relay.failure) |err| return err;
            try statusToError(status);
            try control.checkpoint();
        } else try statusToError(self.dispatchRead(lsn, null, &reply));
        if (result.buffer.len != 0 and result.buffer.ptr == null) return error.StorageKernelFailure;
        if (result.found == 0) {
            if (result.buffer.len != 0) return error.StorageKernelFailure;
            return false;
        }
        if (result.found != 1) return error.StorageKernelFailure;
        try callback(context, .{ .lsn = lsn, .data = result.buffer.slice() });
        if (original) |port| try port.checkLifetime();
        return true;
    }

    pub fn iterateFromStreaming(
        self: *WAL,
        from_lsn: u64,
        callback: *const fn (entry: WalEntry) anyerror!ScanAction,
    ) !void {
        var cursor = from_lsn;
        while (true) {
            var page = try self.scanPage(cursor);
            defer page.deinit();
            const entries = try parsePage(page.allocator orelse std.heap.page_allocator, page.result.entries.slice());
            defer (page.allocator orelse std.heap.page_allocator).free(entries);
            for (entries) |entry| if (try callback(.{ .lsn = entry.lsn, .data = entry.data }) == .stop) return;
            if (page.result.done != 0) return;
            if (page.result.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.result.next_lsn;
        }
    }

    pub fn iterateFromStreamingWithContext(
        self: *WAL,
        from_lsn: u64,
        context: anytype,
        comptime callback: fn (@TypeOf(context), WalEntry) anyerror!ScanAction,
    ) !void {
        var cursor = from_lsn;
        while (true) {
            var page = try self.scanPage(cursor);
            defer page.deinit();
            const entries = try parsePage(page.allocator orelse std.heap.page_allocator, page.result.entries.slice());
            defer (page.allocator orelse std.heap.page_allocator).free(entries);
            for (entries) |entry| if (try callback(context, .{ .lsn = entry.lsn, .data = entry.data }) == .stop) return;
            if (page.result.done != 0) return;
            if (page.result.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.result.next_lsn;
        }
    }

    pub fn lastLsn(self: *const WAL) u64 {
        var last_lsn: u64 = 0;
        statusToError(abi.antfly_storage_wal_last_lsn(self.handle, &last_lsn)) catch |err|
            std.debug.panic("storage WAL last-LSN read failed: {s}", .{@errorName(err)});
        return last_lsn;
    }

    pub fn lastLsnWithAdmission(self: *const WAL, original: @import("antfly_local_sources").common_read_admission.Port) !u64 {
        const read_control = @import("antfly_local_sources").common_read_admission;
        const Relay = struct {
            original: read_control.Port,
            failure: ?anyerror = null,
            pub fn checkpoint(scope: *@This()) !void {
                scope.original.checkpoint() catch |err| {
                    scope.failure = err;
                    return err;
                };
            }
            pub fn waitForProgress(scope: *@This()) !void {
                scope.original.waitForProgress() catch |err| {
                    scope.failure = err;
                    return err;
                };
            }
        };
        var relay: Relay = .{ .original = original };
        var adapter: read_control.Adapter(*Relay) = .{ .budget = &relay };
        const payload = adapter.payload();
        const control = read_control.Port.init(&payload);
        var last_lsn: u64 = 0;
        const status = abi.antfly_storage_wal_last_lsn_with_admission(self.handle, &.{ .port = &control }, &last_lsn);
        // Preserve domain errors across the coarse owner status boundary.
        if (relay.failure) |err| return err;
        try statusToError(status);
        return last_lsn;
    }

    fn dispatchRead(self: *WAL, lsn: u64, port: ?*const anyopaque, reply: *Reply) abi.Status {
        if (reply.allocator) |*allocator| {
            const memory = @import("runtime_memory_abi").Allocator.fromStd(allocator);
            return abi.antfly_storage_wal_read_with_allocator(self.handle, &.{ .lsn = lsn, .port = port, .allocator = &memory }, &reply.result);
        }
        if (port) |control| return abi.antfly_storage_wal_read_with_admission(self.handle, &.{ .lsn = lsn, .port = control }, &reply.result);
        return abi.antfly_storage_wal_read(self.handle, &.{ .lsn = lsn }, &reply.result);
    }

    fn scanPage(self: *WAL, from_lsn: u64) !Page {
        var page: Page = .{ .allocator = if (self.memory_bridge) |bridge| bridge.allocator else null };
        errdefer page.deinit();
        const options: abi.WalScanRequest = .{
            .from_lsn = from_lsn,
            .max_entries = if (builtin.is_test) 1 else 512,
            .max_bytes = 4 * 1024 * 1024,
        };
        const status = if (page.allocator) |*allocator| blk: {
            const memory = @import("runtime_memory_abi").Allocator.fromStd(allocator);
            break :blk abi.antfly_storage_wal_iterate_with_allocator(self.handle, &.{ .options = &options, .allocator = &memory }, &page.result);
        } else abi.antfly_storage_wal_iterate(self.handle, &options, &page.result);
        try statusToError(status);
        if (page.result.done > 1 or (page.result.entries.len != 0 and page.result.entries.ptr == null)) return error.StorageKernelFailure;
        return page;
    }
};

fn freeEntries(alloc: std.mem.Allocator, entries: []WalEntry) void {
    for (entries) |entry| alloc.free(@constCast(entry.data));
    alloc.free(entries);
}

fn parsePage(alloc: std.mem.Allocator, encoded: []const u8) ![]wire.Entry {
    return wire.parseEntriesAlloc(alloc, encoded) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.StorageKernelFailure,
    };
}

fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}

fn commitBackendRaw(value: anytype) u32 {
    return switch (value) {
        .sync => 0,
        .worker_thread => 1,
        .async_io => 2,
        .adaptive => 3,
    };
}

fn validateOptions(opts: anytype) !void {
    if (opts.resolvedBackend() != .lsm)
        return error.UnsupportedKernelWalOptions;
    if (@hasField(@TypeOf(opts), "storage")) {
        if (opts.storage != null) return error.UnsupportedKernelWalOptions;
    }

    // Every supported scalar is copied into the ABI request above. Reject
    // simulation hooks and non-default physical-LSM tuning rather than
    // silently changing their semantics at the compiled boundary.
    if (@hasField(@TypeOf(opts), "lsm_options")) {
        if (!std.meta.eql(opts.lsm_options, @TypeOf(opts.lsm_options){})) return error.UnsupportedKernelWalOptions;
    }
    if (@hasField(@TypeOf(opts), "commit_scheduler")) {
        if (!std.meta.eql(opts.commit_scheduler, @import("antfly_local_sources").storage_sim_runtime.real_completion_scheduler)) return error.UnsupportedKernelWalOptions;
    }
    if (!std.meta.eql(opts.clock, @import("antfly_local_sources").storage_sim_runtime.real_clock)) return error.UnsupportedKernelWalOptions;
}
