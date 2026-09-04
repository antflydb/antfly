// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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

pub const WAL = struct {
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
        };
    }

    pub fn close(self: *WAL) void {
        abi.antfly_storage_wal_close(self.handle);
        self.* = undefined;
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

    pub fn sync(self: *WAL, force: bool) !void {
        try statusToError(abi.antfly_storage_wal_sync(self.handle, &.{
            .force = @intFromBool(force),
        }));
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
            defer abi.antfly_storage_owner_buffer_destroy(&page.entries);
            const parsed = try parsePage(alloc, page.entries.slice());
            defer alloc.free(parsed);
            for (parsed) |source| {
                const data = try alloc.dupe(u8, source.data);
                errdefer alloc.free(data);
                try entries.append(alloc, .{ .lsn = source.lsn, .data = data });
            }
            if (page.done != 0) break;
            if (page.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.next_lsn;
        }
        return entries.toOwnedSlice(alloc);
    }

    pub fn readAt(self: *WAL, alloc: std.mem.Allocator, lsn: u64) !?WalEntry {
        var result = abi.WalReadResult{};
        try statusToError(abi.antfly_storage_wal_read(self.handle, &.{
            .lsn = lsn,
        }, &result));
        defer abi.antfly_storage_owner_buffer_destroy(&result.buffer);
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

    pub fn iterateFromStreaming(
        self: *WAL,
        from_lsn: u64,
        callback: *const fn (entry: WalEntry) anyerror!ScanAction,
    ) !void {
        var cursor = from_lsn;
        while (true) {
            var page = try self.scanPage(cursor);
            defer abi.antfly_storage_owner_buffer_destroy(&page.entries);
            const entries = try parsePage(std.heap.page_allocator, page.entries.slice());
            defer std.heap.page_allocator.free(entries);
            for (entries) |entry| if (try callback(.{ .lsn = entry.lsn, .data = entry.data }) == .stop) return;
            if (page.done != 0) return;
            if (page.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.next_lsn;
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
            defer abi.antfly_storage_owner_buffer_destroy(&page.entries);
            const entries = try parsePage(std.heap.page_allocator, page.entries.slice());
            defer std.heap.page_allocator.free(entries);
            for (entries) |entry| if (try callback(context, .{ .lsn = entry.lsn, .data = entry.data }) == .stop) return;
            if (page.done != 0) return;
            if (page.next_lsn <= cursor) return error.StorageKernelFailure;
            cursor = page.next_lsn;
        }
    }

    pub fn lastLsn(self: *const WAL) u64 {
        var last_lsn: u64 = 0;
        statusToError(abi.antfly_storage_wal_last_lsn(self.handle, &last_lsn)) catch |err|
            std.debug.panic("storage WAL last-LSN read failed: {s}", .{@errorName(err)});
        return last_lsn;
    }

    fn scanPage(self: *WAL, from_lsn: u64) !abi.WalScanResult {
        var result = abi.WalScanResult{};
        try statusToError(abi.antfly_storage_wal_iterate(self.handle, &.{
            .from_lsn = from_lsn,
            .max_entries = if (builtin.is_test) 1 else 512,
            .max_bytes = 4 * 1024 * 1024,
        }, &result));
        if (result.done > 1) return error.StorageKernelFailure;
        return result;
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
    if (opts.resolvedBackend() != .lsm or opts.storage != null)
        return error.UnsupportedKernelWalOptions;

    // Every supported scalar is copied into the ABI request above. Reject
    // simulation hooks and non-default physical-LSM tuning rather than
    // silently changing their semantics at the compiled boundary.
    if (!std.meta.eql(opts.lsm_options, @TypeOf(opts.lsm_options){}) or
        opts.clock.ctx != null or opts.commit_scheduler.ctx != null)
        return error.UnsupportedKernelWalOptions;
}
