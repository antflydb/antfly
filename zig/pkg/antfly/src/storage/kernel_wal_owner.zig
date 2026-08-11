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

//! Physical provider for the opaque runtime WAL boundary.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const wal_mod = @import("wal.zig");
const wire = @import("kernel_wal_wire.zig");

const alloc = std.heap.c_allocator;

const Handle = struct {
    wal: wal_mod.WAL,
};

pub fn open(
    request: *const abi.WalOpenRequest,
    out_result: *abi.WalOpenResult,
) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (!validBool(request.no_sync) or !validBool(request.read_only) or
        !validBool(request.model_commit_backend_completions) or request.path.len == 0)
        return .invalid_argument;

    const path = alloc.dupeZ(u8, request.path.slice()) catch return .out_of_memory;
    defer alloc.free(path);
    const group_commit_max_requests = std.math.cast(usize, request.group_commit_max_requests) orelse
        return .invalid_argument;
    const commit_backend = decodeCommitBackend(request.commit_backend) orelse
        return .invalid_argument;
    const handle = alloc.create(Handle) catch return .out_of_memory;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .wal = wal_mod.WAL.open(path.ptr, .{
            .backend = .lsm,
            .no_sync = request.no_sync != 0,
            .read_only = request.read_only != 0,
            .artificial_sync_delay_ns = request.artificial_sync_delay_ns,
            .group_commit_window_ns = request.group_commit_window_ns,
            .group_commit_max_requests = group_commit_max_requests,
            .commit_backend = commit_backend,
            .model_commit_backend_completions = request.model_commit_backend_completions != 0,
        }) catch |err| return statusFromError(err),
    };
    out_result.* = .{
        .handle = handle,
        .next_lsn = handle.wal.next_lsn,
    };
    return .ok;
}

pub fn close(raw: ?*anyopaque) callconv(.c) void {
    const handle = asHandle(raw) orelse return;
    handle.wal.close();
    alloc.destroy(handle);
}

pub fn append(
    raw: ?*anyopaque,
    request: *const abi.WalAppendRequest,
    out_result: *abi.WalAppendResult,
) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (!validBool(request.reposition_if_empty) or
        (request.reposition_if_empty != 0 and request.expected_next_lsn == 0))
        return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;

    const original_next_lsn = handle.wal.next_lsn;
    if (request.reposition_if_empty != 0) {
        if (handle.wal.lastLsn() != 0 or original_next_lsn != 1)
            return .wal_lsn_mismatch;
        handle.wal.next_lsn = request.expected_next_lsn;
    }
    const assigned_lsn = handle.wal.append(request.data.slice()) catch |err| {
        if (request.reposition_if_empty != 0) handle.wal.next_lsn = original_next_lsn;
        return statusFromError(err);
    };
    if (request.reposition_if_empty != 0 and assigned_lsn != request.expected_next_lsn)
        return .wal_lsn_mismatch;
    const assigned_next_lsn = std.math.add(u64, assigned_lsn, 1) catch return .arithmetic_overflow;
    out_result.* = .{
        .assigned_lsn = assigned_lsn,
        .next_lsn = assigned_next_lsn,
    };
    return .ok;
}

pub fn sync(raw: ?*anyopaque, request: *const abi.WalSyncRequest) callconv(.c) abi.Status {
    if (request.version != abi.abi_version) return .invalid_abi;
    if (!validBool(request.force)) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    handle.wal.sync(request.force != 0) catch |err| return statusFromError(err);
    return .ok;
}

pub fn truncatePrefix(raw: ?*anyopaque, request: *const abi.WalPositionRequest) callconv(.c) abi.Status {
    if (request.version != abi.abi_version) return .invalid_abi;
    const handle = asHandle(raw) orelse return .invalid_argument;
    handle.wal.truncate(request.lsn) catch |err| return statusFromError(err);
    return .ok;
}

pub fn truncateSuffix(
    raw: ?*anyopaque,
    request: *const abi.WalPositionRequest,
    out_next_lsn: *u64,
) callconv(.c) abi.Status {
    out_next_lsn.* = 0;
    if (request.version != abi.abi_version) return .invalid_abi;
    const handle = asHandle(raw) orelse return .invalid_argument;
    handle.wal.truncateAfter(request.lsn) catch |err| return statusFromError(err);
    out_next_lsn.* = handle.wal.next_lsn;
    return .ok;
}

pub fn iterate(
    raw: ?*anyopaque,
    request: *const abi.WalScanRequest,
    out_result: *abi.WalScanResult,
) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    const max_entries = std.math.cast(usize, request.max_entries) orelse return .invalid_argument;
    const max_bytes = std.math.cast(usize, request.max_bytes) orelse return .invalid_argument;
    if (max_entries == 0 or max_entries > 4096 or max_bytes < 16 or max_bytes > 16 * 1024 * 1024)
        return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    var encoder = wire.Encoder{};
    defer encoder.deinit(alloc);
    encoder.init(alloc) catch |err| return statusFromError(err);
    var context = ScanContext{
        .encoder = &encoder,
        .max_entries = request.max_entries,
        .max_bytes = max_bytes,
        .next_lsn = request.from_lsn,
    };
    handle.wal.iterateFromStreamingWithContext(
        request.from_lsn,
        &context,
        appendScanEntry,
    ) catch |err| return statusFromError(err);
    const encoded = encoder.finish(alloc) catch |err| return statusFromError(err);
    out_result.* = .{
        .entries = .{
            .ptr = if (encoded.len == 0) null else encoded.ptr,
            .len = @intCast(encoded.len),
        },
        .next_lsn = context.next_lsn,
        .done = @intFromBool(context.done),
    };
    return .ok;
}

const ScanContext = struct {
    encoder: *wire.Encoder,
    max_entries: u64,
    max_bytes: usize,
    next_lsn: u64,
    done: bool = true,
};

fn appendScanEntry(context: *ScanContext, entry: wal_mod.WalEntry) !wal_mod.WAL.ScanAction {
    const encoded_len = try wire.encodedEntryLen(entry.data.len);
    const next_encoded_len: ?usize = std.math.add(usize, context.encoder.bytes.items.len, encoded_len) catch null;
    if (context.encoder.count != 0 and
        (context.encoder.count >= context.max_entries or
            next_encoded_len == null or next_encoded_len.? > context.max_bytes))
    {
        context.next_lsn = entry.lsn;
        context.done = false;
        return .stop;
    }
    try context.encoder.append(alloc, .{ .lsn = entry.lsn, .data = entry.data });
    context.next_lsn = try std.math.add(u64, entry.lsn, 1);
    return .@"continue";
}

pub fn read(
    raw: ?*anyopaque,
    request: *const abi.WalPositionRequest,
    out_result: *abi.WalReadResult,
) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const entry = (handle.wal.readAt(alloc, request.lsn) catch |err| return statusFromError(err)) orelse
        return .ok;
    out_result.* = .{
        .buffer = .{
            .ptr = if (entry.data.len == 0) null else @constCast(entry.data.ptr),
            .len = @intCast(entry.data.len),
        },
        .found = 1,
    };
    return .ok;
}

pub fn statsSnapshot(
    raw: ?*anyopaque,
    out_stats: *abi.WalStats,
) callconv(.c) abi.Status {
    out_stats.* = .{};
    const handle = asHandle(raw) orelse return .invalid_argument;
    const stats = handle.wal.statsSnapshot();
    out_stats.* = .{
        .append_calls = stats.append_calls,
        .append_batch_calls = stats.append_batch_calls,
        .logical_entries = stats.logical_entries,
        .physical_commits = stats.physical_commits,
        .grouped_commits = stats.grouped_commits,
        .grouped_requests = stats.grouped_requests,
        .max_requests_per_commit = stats.max_requests_per_commit,
        .max_entries_per_commit = stats.max_entries_per_commit,
        .total_wait_ns = stats.total_wait_ns,
        .total_coalesce_ns = stats.total_coalesce_ns,
        .total_txn_open_ns = stats.total_txn_open_ns,
        .total_put_ns = stats.total_put_ns,
        .total_commit_ns = stats.total_commit_ns,
        .inner_segment_syncs = stats.inner_segment_syncs,
        .inner_index_syncs = stats.inner_index_syncs,
        .post_commit_segment_syncs = stats.post_commit_segment_syncs,
        .post_commit_index_syncs = stats.post_commit_index_syncs,
    };
    return .ok;
}

pub fn lastLsn(raw: ?*anyopaque, out_last_lsn: *u64) callconv(.c) abi.Status {
    out_last_lsn.* = 0;
    const handle = asHandle(raw) orelse return .invalid_argument;
    out_last_lsn.* = handle.wal.lastLsnSnapshot();
    return .ok;
}

fn asHandle(raw: ?*anyopaque) ?*Handle {
    return @ptrCast(@alignCast(raw orelse return null));
}

fn validBool(value: u8) bool {
    return value <= 1;
}

fn decodeCommitBackend(raw: u32) ?wal_mod.CommitBackend {
    return switch (raw) {
        0 => .sync,
        1 => .worker_thread,
        2 => .async_io,
        3 => .adaptive,
        else => null,
    };
}

fn statusFromError(err: anyerror) abi.Status {
    return error_identity.statusFromError(err);
}
