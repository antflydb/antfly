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

//! Physical provider for the opaque runtime WAL boundary.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const wal_mod = @import("antfly_local_sources").storage_wal;
const wire = @import("kernel_wal_wire.zig");

const alloc = std.heap.c_allocator;

const Handle = struct {
    memory: ?@import("runtime_memory_abi").Allocator = null,
    wal: wal_mod.WAL,
};

pub fn open(request: *const abi.WalOpenRequest, out_result: *abi.WalOpenResult) callconv(.c) abi.Status {
    return openOwned(request, out_result, null);
}

pub fn openWithAllocator(request: *const abi.WalOpenAllocatorRequest, out_result: *abi.WalOpenResult) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const options = request.options orelse return .invalid_argument;
    const memory = request.allocator orelse return .invalid_argument;
    if (@intFromPtr(options) % @alignOf(abi.WalOpenRequest) != 0 or
        @intFromPtr(memory) % @alignOf(@import("runtime_memory_abi").Allocator) != 0) return .invalid_argument;
    if (!memory.valid()) return .invalid_abi;
    return openOwned(options, out_result, memory.*);
}

fn openOwned(request: *const abi.WalOpenRequest, out_result: *abi.WalOpenResult, memory: ?@import("runtime_memory_abi").Allocator) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (!validBool(request.no_sync) or !validBool(request.read_only) or
        !validBool(request.model_commit_backend_completions) or request.path.len == 0 or request.path.ptr == null or
        !std.mem.allEqual(u8, &request._reserved0, 0)) return .invalid_argument;
    const group_commit_max_requests = std.math.cast(usize, request.group_commit_max_requests) orelse return .invalid_argument;
    const commit_backend = decodeCommitBackend(request.commit_backend) orelse return .invalid_argument;
    // The descriptor is placed in stable owner storage before asStd creates
    // self-relative allocator pointers. Never move this Handle after opening.
    var temporary = memory;
    const allocation = if (temporary) |*value| value.asStd() else alloc;
    const handle = allocation.create(Handle) catch return .out_of_memory;
    var success = false;
    defer if (!success) allocation.destroy(handle);
    handle.memory = memory;
    const owner_allocator = if (handle.memory) |*value| value.asStd() else alloc;
    const path = owner_allocator.dupeSentinel(u8, request.path.slice(), 0) catch return .out_of_memory;
    defer owner_allocator.free(path);
    handle.wal = wal_mod.WAL.openWithAllocator(owner_allocator, path.ptr, .{
        .backend = .lsm,
        .no_sync = request.no_sync != 0,
        .read_only = request.read_only != 0,
        .artificial_sync_delay_ns = request.artificial_sync_delay_ns,
        .group_commit_window_ns = request.group_commit_window_ns,
        .group_commit_max_requests = group_commit_max_requests,
        .commit_backend = commit_backend,
        .model_commit_backend_completions = request.model_commit_backend_completions != 0,
    }) catch |err| return statusFromError(err);
    success = true;
    out_result.* = .{ .handle = handle, .next_lsn = handle.wal.next_lsn };
    return .ok;
}

pub fn close(raw: ?*anyopaque) callconv(.c) void {
    const handle = asHandle(raw) orelse return;
    handle.wal.close();
    // Copy before freeing the descriptor stored in the owner itself.
    var memory = handle.memory;
    const allocation = if (memory) |*value| value.asStd() else alloc;
    allocation.destroy(handle);
}

// Stable per-read descriptor: neither the open descriptor nor a request stack
// frame may back the allocator of metadata surviving WAL.close.
const ReadHandle = struct {
    memory: @import("runtime_memory_abi").Allocator,
    operation: *wal_mod.WAL.OwnedRead,
};
fn asReadHandle(raw: ?*anyopaque) ?*ReadHandle {
    const ptr = raw orelse return null;
    if (@intFromPtr(ptr) % @alignOf(ReadHandle) != 0) return null;
    return @ptrCast(@alignCast(ptr));
}
pub fn ownedReadBegin(raw: ?*anyopaque, request: *const abi.WalOwnedReadBeginRequest, output: *abi.WalOwnedReadBeginResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or request.lsn == 0) return .invalid_argument;
    const wal = asHandle(raw) orelse return .invalid_argument;
    const memory = replyAllocator(request.allocator) catch |err| return statusFromError(err);
    const control = recoveryControl(request.port) catch |err| return statusFromError(err);
    control.checkpoint() catch |err| return statusFromError(err);
    const handle = memory.asStd().create(ReadHandle) catch return .out_of_memory;
    handle.memory = memory.*;
    handle.operation = wal.wal.beginOwnedRead(handle.memory.asStd(), request.lsn, control) catch |err| {
        memory.asStd().destroy(handle);
        return statusFromError(err);
    };
    control.checkpoint() catch |err| {
        ownedReadDestroy(handle);
        return statusFromError(err);
    };
    output.handle = handle;
    return .ok;
}
pub fn ownedReadStep(raw: ?*anyopaque, request: *const abi.WalOwnedReadStepRequest, output: *abi.WalOwnedReadStepResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or request.quantum == 0 or request.quantum > 64 * 1024) return .invalid_argument;
    const handle = asReadHandle(raw) orelse return .invalid_argument;
    const control = recoveryControl(request.port) catch |err| return statusFromError(err);
    const result = handle.operation.step(@intCast(request.quantum), control) catch |err| return statusFromError(err);
    output.ready = @intFromBool(result == .ready);
    return .ok;
}
pub fn ownedReadChunk(raw: ?*anyopaque, request: *const abi.WalOwnedReadChunkRequest, output: *abi.WalOwnedReadChunkResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or request.output.len > 64 * 1024 or
        (request.output.len != 0 and request.output.ptr == null)) return .invalid_argument;
    const offset = std.math.cast(usize, request.offset) orelse return .invalid_argument;
    const handle = asReadHandle(raw) orelse return .invalid_argument;
    const control = recoveryControl(request.port) catch |err| return statusFromError(err);
    const result = handle.operation.readChunk(offset, if (request.output.ptr) |ptr| ptr[0..@intCast(request.output.len)] else &.{}, control) catch |err| return statusFromError(err);
    output.* = .{ .len = result.len, .done = @intFromBool(result.done) };
    return .ok;
}
pub fn ownedReadDestroy(raw: ?*anyopaque) callconv(.c) void {
    const handle = asReadHandle(raw) orelse return;
    handle.operation.destroy();
    // Native retirement can still call into handle.memory. Free it last, using
    // a copy whose own descriptor remains alive until the free callback ends.
    var memory = handle.memory;
    memory.asStd().destroy(handle);
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

    const assigned_lsn = (if (request.reposition_if_empty != 0)
        handle.wal.appendAt(request.expected_next_lsn, request.data.slice())
    else
        handle.wal.append(request.data.slice())) catch |err| return statusFromError(err);
    const assigned_next_lsn = std.math.add(u64, assigned_lsn, 1) catch return .arithmetic_overflow;
    out_result.* = .{
        .assigned_lsn = assigned_lsn,
        .next_lsn = assigned_next_lsn,
    };
    return .ok;
}

const PreparedHandle = struct {
    allocator: std.mem.Allocator,
    operation: wal_mod.WAL.PreparedAppend,
};
fn asPreparedHandle(raw: ?*anyopaque) ?*PreparedHandle {
    const ptr = raw orelse return null;
    if (@intFromPtr(ptr) % @alignOf(PreparedHandle) != 0) return null;
    return @ptrCast(@alignCast(ptr));
}
pub fn prepareAppend(raw: ?*anyopaque, request: *const abi.WalPrepareAppendRequest, output: *abi.WalPrepareAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request.prefix_len > 4096 or (request.tail.len != 0 and request.tail.ptr == null) or request.tail.len > std.math.maxInt(u32) - request.prefix_len) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const control = if (request.port != null) recoveryControl(request.port) catch |err| return statusFromError(err) else null;
    if (control) |port| port.checkpoint() catch |err| return statusFromError(err);
    const owned = handle.wal.allocator.create(PreparedHandle) catch return .out_of_memory;
    owned.allocator = handle.wal.allocator;
    owned.operation = handle.wal.prepareAppend(request.prefix_len, request.tail.slice(), control) catch |err| {
        owned.allocator.destroy(owned);
        return statusFromError(err);
    };
    output.handle = owned;
    return .ok;
}
pub fn preparedPublish(raw: ?*anyopaque, request: *const abi.WalPreparedPublishRequest, output: *abi.WalAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or request.prefix.len > 4096 or (request.prefix.len != 0 and request.prefix.ptr == null)) return .invalid_argument;
    const handle = asPreparedHandle(raw) orelse return .invalid_argument;
    const assigned = handle.operation.publish(request.prefix.slice()) catch |err| return statusFromError(err);
    output.* = .{ .assigned_lsn = assigned, .next_lsn = assigned + 1 };
    return .ok;
}
pub fn preparedDestroy(raw: ?*anyopaque) callconv(.c) void {
    const handle = asPreparedHandle(raw) orelse return;
    const allocator = handle.allocator;
    handle.operation.deinit();
    allocator.destroy(handle);
}

fn recoveryControl(raw: ?*const anyopaque) !@import("antfly_local_sources").common_read_admission.Port {
    const read_control = @import("antfly_local_sources").common_read_admission;
    const ptr = raw orelse return error.InvalidArgument;
    if (@intFromPtr(ptr) % @alignOf(read_control.Port) != 0) return error.InvalidArgument;
    const port: *const read_control.Port = @ptrCast(@alignCast(ptr));
    try port.validate();
    return port.*;
}

pub fn recoveryPosition(raw: ?*anyopaque, request: *const abi.WalRecoveryPositionRequest, output: *abi.WalRecoveryPositionResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const control = recoveryControl(request.port) catch |err| return statusFromError(err);
    const position = handle.wal.tryRecoveryPosition(control) catch |err| return statusFromError(err);
    if (position) |value| output.position = .{ .origin = value.origin, .owner = value.owner, .revision = value.revision, .next_lsn = value.next_lsn } else output.busy = 1;
    return .ok;
}

pub fn appendIfRecoveryPosition(raw: ?*anyopaque, request: *const abi.WalRecoveryAppendRequest, output: *abi.WalRecoveryAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    if ((request.data.len != 0 and request.data.ptr == null) or request.data.len > std.math.maxInt(usize)) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const control = recoveryControl(request.port) catch |err| return statusFromError(err);
    const result = handle.wal.appendIfRecoveryPosition(.{ .origin = request.expected.origin, .owner = request.expected.owner, .revision = request.expected.revision, .next_lsn = request.expected.next_lsn }, request.data.slice(), control) catch |err| return statusFromError(err);
    switch (result) {
        .changed => {},
        .busy => output.outcome = 1,
        .appended => |lsn| output.* = .{ .outcome = 2, .assigned_lsn = lsn, .next_lsn = lsn + 1 },
    }
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
    return iterateOwned(raw, request, out_result, alloc);
}

pub fn iterateWithAllocator(raw: ?*anyopaque, request: *const abi.WalScanAllocatorRequest, out_result: *abi.WalScanResult) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const options = request.options orelse return .invalid_argument;
    if (@intFromPtr(options) % @alignOf(abi.WalScanRequest) != 0) return .invalid_argument;
    if (options.version != abi.abi_version) return .invalid_abi;
    const memory = replyAllocator(request.allocator) catch |err| return statusFromError(err);
    return iterateOwned(raw, options, out_result, memory.asStd());
}

fn replyAllocator(raw: ?*const @import("runtime_memory_abi").Allocator) !*const @import("runtime_memory_abi").Allocator {
    const memory = raw orelse return error.InvalidArgument;
    if (@intFromPtr(memory) % @alignOf(@import("runtime_memory_abi").Allocator) != 0) return error.InvalidArgument;
    if (!memory.valid()) return error.UnsupportedVersion;
    return memory;
}

fn iterateOwned(raw: ?*anyopaque, request: *const abi.WalScanRequest, out_result: *abi.WalScanResult, allocator: std.mem.Allocator) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved0 != 0) return .invalid_argument;
    const max_entries = std.math.cast(usize, request.max_entries) orelse return .invalid_argument;
    const max_bytes = std.math.cast(usize, request.max_bytes) orelse return .invalid_argument;
    if (max_entries == 0 or max_entries > 4096 or max_bytes < 16 or max_bytes > 16 * 1024 * 1024)
        return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    var encoder = wire.Encoder{};
    defer encoder.deinit(allocator);
    encoder.init(allocator) catch |err| return statusFromError(err);
    var context = ScanContext{
        .encoder = &encoder,
        .allocator = allocator,
        .max_entries = request.max_entries,
        .max_bytes = max_bytes,
        .next_lsn = request.from_lsn,
    };
    handle.wal.iterateFromStreamingWithContext(
        request.from_lsn,
        &context,
        appendScanEntry,
    ) catch |err| return statusFromError(err);
    const encoded = encoder.finish(allocator) catch |err| return statusFromError(err);
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
    allocator: std.mem.Allocator,
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
    try context.encoder.append(context.allocator, .{ .lsn = entry.lsn, .data = entry.data });
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

/// Keep allocation and buffer ownership in this archive. No borrowed snapshot
/// or callback result can escape; failed copying frees the partial owner buffer.
pub fn readWithAdmission(
    raw: ?*anyopaque,
    request: *const abi.WalReadAdmissionRequest,
    out_result: *abi.WalReadResult,
) callconv(.c) abi.Status {
    return readControlled(raw, request, out_result, alloc);
}

pub fn readWithAllocator(raw: ?*anyopaque, request: *const abi.WalReadAllocatorRequest, out_result: *abi.WalReadResult) callconv(.c) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const memory = replyAllocator(request.allocator) catch |err| return statusFromError(err);
    if (request.port != null) return readControlled(raw, &.{ .lsn = request.lsn, .port = request.port }, out_result, memory.asStd());
    const handle = asHandle(raw) orelse return .invalid_argument;
    const entry = (handle.wal.readAt(memory.asStd(), request.lsn) catch |err| return statusFromError(err)) orelse return .ok;
    out_result.* = .{ .found = 1, .buffer = .{ .ptr = if (entry.data.len == 0) null else @constCast(entry.data.ptr), .len = entry.data.len } };
    return .ok;
}

fn readControlled(raw: ?*anyopaque, request: *const abi.WalReadAdmissionRequest, out_result: *abi.WalReadResult, allocator: std.mem.Allocator) abi.Status {
    out_result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const read_control = @import("antfly_local_sources").common_read_admission;
    const port = request.port orelse return .invalid_argument;
    if (@intFromPtr(port) % @alignOf(read_control.Port) != 0) return .invalid_argument;
    const control: *const read_control.Port = @ptrCast(@alignCast(port));
    control.validate() catch |err| return statusFromError(err);
    var retained: read_control.RetainedDependency = .{};
    const lifetime = retained.bind(control.*).?;
    const Copy = struct {
        control: read_control.Port,
        allocator: std.mem.Allocator,
        bytes: ?[]u8 = null,
        fn visit(self: *@This(), entry: wal_mod.WalEntry) !void {
            try self.control.checkpoint();
            const bytes = try self.allocator.alloc(u8, entry.data.len);
            errdefer self.allocator.free(bytes);
            var offset: usize = 0;
            while (offset < bytes.len) {
                try self.control.checkpoint();
                const end = offset + @min(64 * 1024, bytes.len - offset);
                @memcpy(bytes[offset..end], entry.data[offset..end]);
                offset = end;
            }
            try self.control.checkpoint();
            self.bytes = bytes;
        }
    };
    var copy: Copy = .{ .control = lifetime, .allocator = allocator };
    defer if (copy.bytes) |bytes| allocator.free(bytes);
    const found = handle.wal.withEntryAtWithAdmission(request.lsn, lifetime, &copy, Copy.visit) catch |err| return statusFromError(err);
    if (!found) return .ok;
    const bytes = copy.bytes orelse return statusFromError(error.StorageKernelFailure);
    out_result.* = .{ .found = 1, .buffer = .{ .ptr = if (bytes.len == 0) null else bytes.ptr, .len = @intCast(bytes.len) } };
    copy.bytes = null;
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

pub fn lastLsnWithAdmission(raw: ?*anyopaque, request: *const abi.WalSnapshotAdmissionRequest, out_last_lsn: *u64) callconv(.c) abi.Status {
    out_last_lsn.* = 0;
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const read_control = @import("antfly_local_sources").common_read_admission;
    const port = request.port orelse return .invalid_argument;
    if (@intFromPtr(port) % @alignOf(read_control.Port) != 0) return .invalid_argument;
    const control: *const read_control.Port = @ptrCast(@alignCast(port));
    control.validate() catch |err| return statusFromError(err);
    const lsn = handle.wal.lastLsnSnapshotWithAdmission(control.*) catch |err| return statusFromError(err);
    out_last_lsn.* = lsn;
    return .ok;
}

fn asHandle(raw: ?*anyopaque) ?*Handle {
    return @ptrCast(@alignCast(raw orelse return null));
}

test "controlled WAL snapshot native owner and opaque client preserve original authority" {
    const read_control = @import("antfly_local_sources").common_read_admission;
    var handle: Handle = undefined;
    // Snapshot must access only the mutex, its IO owner and the LSN counter.
    // No store, filesystem, worker or durability state is needed by this read.
    handle.wal.mutex = .init;
    handle.wal.sync_io = std.testing.io;
    handle.wal.next_lsn = 42;
    var client: @import("kernel_wal_client.zig").WAL = .{ .clock = undefined, .handle = &handle };
    const Authority = struct {
        handle: *Handle,
        held: *bool,
        checks: usize = 0,
        waits: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            try std.testing.expect(self.held.*);
            self.handle.wal.mutex.unlock(std.testing.io);
            self.held.* = false;
        }
    };
    var held = false;
    defer if (held) handle.wal.mutex.unlock(std.testing.io);
    var authority: Authority = .{ .handle = &handle, .held = &held };
    const payload: read_control.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .wait_for_progress = Authority.wait } };
    const control = read_control.Port.init(&payload);
    for ([_]bool{ false, true }) |opaque_client| {
        try std.testing.expect(handle.wal.mutex.tryLock());
        held = true;
        authority = .{ .handle = &handle, .held = &held };
        const lsn = if (opaque_client) try client.lastLsnWithAdmission(control) else try handle.wal.lastLsnSnapshotWithAdmission(control);
        try std.testing.expectEqual(@as(u64, 41), lsn);
        try std.testing.expectEqual(@as(usize, 1), authority.waits);
        const checks = authority.checks;
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
            for (1..checks + 1) |fail_at| {
                try std.testing.expect(handle.wal.mutex.tryLock());
                held = true;
                authority = .{ .handle = &handle, .held = &held, .fail_at = fail_at, .cause = cause };
                const result = if (opaque_client) client.lastLsnWithAdmission(control) else handle.wal.lastLsnSnapshotWithAdmission(control);
                try std.testing.expectError(cause, result);
                if (held) {
                    try std.testing.expect(!handle.wal.mutex.tryLock());
                    handle.wal.mutex.unlock(std.testing.io);
                    held = false;
                } else {
                    try std.testing.expect(handle.wal.mutex.tryLock());
                    handle.wal.mutex.unlock(std.testing.io);
                }
                try std.testing.expectEqual(@as(u64, 42), handle.wal.next_lsn);
            }
        }
    }
    var out: u64 = 99;
    try std.testing.expectEqual(abi.Status.invalid_abi, lastLsnWithAdmission(&handle, &.{ .version = abi.abi_version + 1, .port = &control }, &out));
    try std.testing.expectEqual(@as(u64, 0), out);
    try std.testing.expectEqual(abi.Status.invalid_argument, lastLsnWithAdmission(&handle, &.{}, &out));
    try std.testing.expectEqual(@as(u64, 0), out);
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

pub fn appendIdempotent(
    raw: ?*anyopaque,
    request: *const abi.WalIdempotentAppendRequest,
    result: *abi.WalIdempotentAppendResult,
) callconv(.c) abi.Status {
    result.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    const handle = asHandle(raw) orelse return .invalid_argument;
    const appended = handle.wal.appendIdempotent(request.key.slice(), request.digest.slice(), request.data.slice()) catch |err| return statusFromError(err);
    result.* = .{ .lsn = appended.lsn, .appended = @intFromBool(appended.appended) };
    return .ok;
}

test "controlled WAL read native owner and opaque client release copied buffers on every rejection" {
    const read_control = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * 64 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 71);
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/controlled-wal-owner-{d}", .{std.testing.random_seed}, 0);
    defer a.free(path);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, path) catch {};
    var handle: Handle = .{ .wal = try wal_mod.WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync }) };
    defer handle.wal.close();
    _ = try handle.wal.append(bytes);
    var client: @import("kernel_wal_client.zig").WAL = .{ .clock = undefined, .handle = &handle };
    var state: State = .{};
    const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read_control.Port.init(&payload);
    const Visit = struct {
        expected: []const u8,
        calls: usize = 0,
        fn run(self: *@This(), entry: @import("kernel_wal_client.zig").WalEntry) !void {
            try std.testing.expectEqualSlices(u8, self.expected, entry.data);
            self.calls += 1;
        }
    };
    var visit: Visit = .{ .expected = bytes };
    try std.testing.expect(try client.withEntryAtWithAdmission(1, control, &visit, Visit.run));
    const checks = state.checks;
    try std.testing.expect(checks > 10);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            state = .{ .reject_at = reject_at, .cause = cause };
            try std.testing.expectError(cause, client.withEntryAtWithAdmission(1, control, &visit, Visit.run));
            try std.testing.expectEqual(reject_at, state.checks);
            _ = try handle.wal.append("after rejection");
        }
    }
    state = .{};
    try std.testing.expect(!try client.withEntryAtWithAdmission(std.math.maxInt(u64), control, &visit, Visit.run));
    var output = abi.WalReadResult{ .found = 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, readWithAdmission(&handle, &.{ .version = abi.abi_version + 1, .port = &control }, &output));
    try std.testing.expectEqual(@as(u8, 0), output.found);
    try std.testing.expectEqual(abi.Status.invalid_argument, readWithAdmission(&handle, &.{}, &output));
    try std.testing.expectEqual(abi.Status.invalid_argument, readWithAdmission(&handle, &.{ ._reserved = 1, .port = &control }, &output));
}

test "controlled WAL position native owner and opaque client preserve witnesses and original failures" {
    const read_control = @import("antfly_local_sources").common_read_admission;
    const client_mod = @import("kernel_wal_client.zig");
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/controlled-wal-position-{d}", .{std.testing.random_seed}, 0);
    defer a.free(path);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, path) catch {};
    var handle: Handle = .{ .wal = try wal_mod.WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync }) };
    defer handle.wal.close();
    var client: client_mod.WAL = .{ .handle = &handle, .clock = undefined };
    const State = struct {
        next: *u64,
        expected_next: u64 = 1,
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        after_commit: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.next.* != self.expected_next) {
                self.after_commit = true;
                return error.Canceled;
            }
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    var state: State = .{ .next = &handle.wal.next_lsn };
    const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read_control.Port.init(&payload);
    const position = (try client.tryRecoveryPosition(control)).?;
    const position_checks = state.checks;
    try std.testing.expect(position_checks >= 4);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..position_checks + 1) |reject_at| {
            state = .{ .next = &handle.wal.next_lsn, .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, client.tryRecoveryPosition(control));
        }
    }
    state = .{ .next = &handle.wal.next_lsn };
    try std.testing.expectEqual(@as(u64, 1), (try client.appendIfRecoveryPosition(position, "owner committed", control)).appended);
    const append_checks = state.checks;
    try std.testing.expect(!state.after_commit);
    state = .{ .next = &handle.wal.next_lsn, .expected_next = 2 };
    const next = (try client.tryRecoveryPosition(control)).?;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..append_checks + 1) |reject_at| {
            state = .{ .next = &handle.wal.next_lsn, .expected_next = 2, .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, client.appendIfRecoveryPosition(next, "rejected", control));
            try std.testing.expectEqual(@as(u64, 2), handle.wal.next_lsn);
        }
    }
    state = .{ .next = &handle.wal.next_lsn, .expected_next = 2 };
    try std.testing.expectEqual(@as(client_mod.RecoveryAppendResult, .changed), try client.appendIfRecoveryPosition(position, "stale", control));
    try std.testing.expect(handle.wal.mutex.tryLock());
    const busy_position = client.tryRecoveryPosition(control);
    const busy_append = client.appendIfRecoveryPosition(next, "busy", control);
    handle.wal.mutex.unlock(handle.wal.sync_io);
    try std.testing.expectEqual(@as(?client_mod.RecoveryPosition, null), try busy_position);
    try std.testing.expectEqual(@as(client_mod.RecoveryAppendResult, .busy), try busy_append);
    var output: abi.WalRecoveryPositionResult = .{ .busy = 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, recoveryPosition(&handle, &.{ .version = abi.abi_version + 1, .port = &control }, &output));
    try std.testing.expectEqual(@as(u8, 0), output.busy);
    try std.testing.expectEqual(abi.Status.invalid_argument, recoveryPosition(&handle, &.{}, &output));
    var append_output: abi.WalRecoveryAppendResult = .{ .outcome = 2 };
    try std.testing.expectEqual(abi.Status.invalid_argument, appendIfRecoveryPosition(&handle, &.{ ._reserved = 1, .port = &control }, &append_output));
    try std.testing.expectEqual(@as(u8, 0), append_output.outcome);
}

test "WAL allocator owner opaque client keeps its root stable through open append and close" {
    const client = @import("kernel_wal_client.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, ".zig-cache/tmp/{s}/rooted-wal", .{tmp.sub_path}, 0);
    defer std.testing.allocator.free(path);
    // Bridge, owner and path allocation failures must each unwind through the
    // originating allocator before publishing a handle.
    for (0..3) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, client.WAL.openWithAllocator(failing.allocator(), path, client.WalOptions{ .no_sync = true }));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    {
        // Returning this WAL by value must not leave a callback pointing at an
        // openWithAllocator stack frame or a moved WAL field.
        var wal = try client.WAL.openWithAllocator(tracking.allocator(), path, client.WalOptions{ .no_sync = true });
        defer wal.close();
        try std.testing.expect(tracking.allocated_bytes > 0);
        const native_handle = asHandle(wal.handle).?;
        tracking.fail_index = tracking.alloc_index;
        try std.testing.expectError(error.OutOfMemory, wal.append("first"));
        try std.testing.expectEqual(@as(u64, 0), native_handle.wal.lastLsn());
        tracking.fail_index = std.math.maxInt(usize);
        try std.testing.expectEqual(@as(u64, 1), try wal.append("first"));
    }
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
    // Version rejection precedes dereferencing the borrowed descriptors.
    var result: abi.WalOpenResult = .{ .handle = @ptrFromInt(8), .next_lsn = 77 };
    try std.testing.expectEqual(abi.Status.invalid_abi, openWithAllocator(&.{ .version = abi.abi_version - 1, .options = @ptrFromInt(8), .allocator = @ptrFromInt(8) }, &result));
    try std.testing.expect(result.handle == null and result.next_lsn == 1);
}

test "controlled WAL reply allocator native owner releases snapshots before visitor and bytes after WAL close" {
    const client = @import("kernel_wal_client.zig");
    const read_control = @import("antfly_local_sources").common_read_admission;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, ".zig-cache/tmp/{s}/rooted-replies", .{tmp.sub_path}, 0);
    defer std.testing.allocator.free(path);
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var wal = try client.WAL.openWithAllocator(tracking.allocator(), path, client.WalOptions{ .no_sync = true });
    var open_wal = true;
    defer if (open_wal) wal.close();
    try std.testing.expectEqual(@as(u64, 1), try wal.append("reply-data"));
    const handle = asHandle(wal.handle).?;
    const Control = struct {
        fn check(_: *anyopaque) !void {}
    };
    var unused: u8 = 0;
    const payload: read_control.Payload = .{ .ptr = &unused, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check } };
    const Visit = struct {
        wal: *client.WAL,
        tracking: *std.testing.FailingAllocator,
        open: *bool,
        fn run(self: *@This(), entry: client.WalEntry) !void {
            const native = asHandle(self.wal.handle).?;
            try std.testing.expectEqual(@as(usize, 0), native.wal.store_owner.lsm.backend.active_readers);
            self.wal.close();
            self.open.* = false;
            // Only the detached response bytes remain; no callback needs the
            // retired bridge, WAL handle, snapshot or provider at this point.
            try std.testing.expectEqual(entry.data.len, self.tracking.allocated_bytes - self.tracking.freed_bytes);
            try std.testing.expectEqualStrings("reply-data", entry.data);
        }
    };
    tracking.fail_index = tracking.alloc_index;
    try std.testing.expectError(error.OutOfMemory, wal.withEntryAtWithAdmission(1, read_control.Port.init(&payload), &unused, struct {
        fn run(_: *u8, _: client.WalEntry) !void {
            return error.UnexpectedVisit;
        }
    }.run));
    tracking.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 0), handle.wal.store_owner.lsm.backend.active_readers);
    var visit: Visit = .{ .wal = &wal, .tracking = &tracking, .open = &open_wal };
    try std.testing.expect(try wal.withEntryAtWithAdmission(1, read_control.Port.init(&payload), &visit, Visit.run));
    try std.testing.expect(!open_wal);
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
    var invalid: abi.WalReadResult = .{ .found = 9 };
    try std.testing.expectEqual(abi.Status.invalid_abi, readWithAllocator(null, &.{ .version = abi.abi_version - 1, .allocator = @ptrFromInt(8), .port = @ptrFromInt(8) }, &invalid));
    try std.testing.expectEqual(@as(u8, 0), invalid.found);
}

test "controlled WAL reply allocator native scan retires detached page and parser storage after close" {
    const client = @import("kernel_wal_client.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, ".zig-cache/tmp/{s}/rooted-scan", .{tmp.sub_path}, 0);
    defer std.testing.allocator.free(path);
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var wal = try client.WAL.openWithAllocator(tracking.allocator(), path, client.WalOptions{ .no_sync = true });
    var open_wal = true;
    defer if (open_wal) wal.close();
    _ = try wal.append("page-data");
    const Visit = struct {
        wal: *client.WAL,
        open: *bool,
        fn run(self: *@This(), entry: client.WalEntry) !client.WalScanAction {
            self.wal.close();
            self.open.* = false;
            try std.testing.expectEqualStrings("page-data", entry.data);
            return .stop;
        }
    };
    var visit: Visit = .{ .wal = &wal, .open = &open_wal };
    try wal.iterateFromStreamingWithContext(1, &visit, Visit.run);
    try std.testing.expect(!open_wal);
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
}

test "WAL owned read opaque client keeps independent roots and copied chunks after WAL close" {
    const client_mod = @import("kernel_wal_client.zig");
    const read_control = @import("antfly_local_sources").common_read_admission;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/owned-c-wal", .{tmp.sub_path}, 0);
    defer a.free(path);
    var wal_root = std.testing.FailingAllocator.init(a, .{});
    var read_root = std.testing.FailingAllocator.init(a, .{});
    var client = try client_mod.WAL.openWithAllocator(wal_root.allocator(), path, client_mod.WalOptions{ .no_sync = true });
    var is_open = true;
    defer if (is_open) client.close();
    const expected = try a.alloc(u8, 128 * 1024 + 19);
    defer a.free(expected);
    for (expected, 0..) |*byte, i| byte.* = @truncate(i *% 71);
    const lsn = try client.append(expected);
    var operations: [16]?*client_mod.WAL.OwnedRead = @splat(null);
    defer for (operations) |maybe| if (maybe) |operation| operation.destroy();
    const Authority = struct {
        active: bool = true,
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.active) return error.ExpiredReadStep;
            self.checks += 1;
        }
    };
    {
        var state: Authority = .{};
        const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Authority.check, .check_lifetime = Authority.check } };
        const port = read_control.Port.init(&payload);
        for (&operations) |*operation| operation.* = try client.beginOwnedRead(read_root.allocator(), lsn, port);
        try std.testing.expectError(error.Backpressured, client.beginOwnedRead(read_root.allocator(), lsn, port));
        state.active = false;
    }
    // Every call gets a fresh stack payload; no earlier Port may be retained.
    var steps: usize = 0;
    while (true) {
        var state: Authority = .{};
        const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Authority.check, .check_lifetime = Authority.check } };
        const status = try operations[0].?.step(4093, read_control.Port.init(&payload));
        state.active = false;
        steps += 1;
        if (status == .ready) break;
        try std.testing.expect(steps < 1000);
    }
    try std.testing.expect(steps > 32);
    var state: Authority = .{};
    const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Authority.check, .check_lifetime = Authority.check } };
    const control = read_control.Port.init(&payload);
    const copied = try a.alloc(u8, expected.len);
    defer a.free(copied);
    var offset: usize = 0;
    while (offset < copied.len) {
        const end = offset + @min(997, copied.len - offset);
        const chunk = try operations[0].?.readChunk(offset, copied[offset..end], control);
        try std.testing.expectEqual(end - offset, chunk.len);
        try std.testing.expectEqual(end == copied.len, chunk.done);
        offset += chunk.len;
    }
    client.close();
    is_open = false;
    // The open descriptor/bridge is gone while every opaque read still lives.
    try std.testing.expectEqual(wal_root.allocated_bytes, wal_root.freed_bytes);
    try std.testing.expectEqualSlices(u8, expected, copied);
    for (&operations) |*operation| {
        try std.testing.expectError(error.TransactionClosed, operation.*.?.step(1, control));
        try std.testing.expectError(error.TransactionClosed, operation.*.?.readChunk(0, copied[0..1], control));
        operation.*.?.destroy();
        operation.* = null;
    }
    try std.testing.expectEqual(read_root.allocated_bytes, read_root.freed_bytes);
}

test "WAL owned read opaque client unwinds each allocation failure and original authority rejection" {
    const client_mod = @import("kernel_wal_client.zig");
    const read_control = @import("antfly_local_sources").common_read_admission;
    const a = std.testing.allocator;
    var handle: Handle = .{ .wal = try wal_mod.WAL.open(".zig-cache/tmp/owned-c-rejection", .{ .backend = .lsm_memory, .commit_backend = .sync }) };
    defer handle.wal.close();
    const lsn = try handle.wal.append("caller owned read payload");
    var client: client_mod.WAL = .{ .clock = undefined, .handle = &handle };
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return error.OriginalOwnedReadRejection;
        }
    };
    var state: State = .{};
    const payload: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read_control.Port.init(&payload);
    const Probe = struct {
        fn run(allocation: std.mem.Allocator, wal: *client_mod.WAL, port: read_control.Port) !void {
            const operation = try wal.beginOwnedRead(allocation, 1, port);
            defer operation.destroy();
            while (try operation.step(7, port) != .ready) {}
            var output: [7]u8 = undefined;
            _ = try operation.readChunk(0, &output, port);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ &client, control });
    var root = std.testing.FailingAllocator.init(a, .{});
    state = .{};
    const baseline = try client.beginOwnedRead(root.allocator(), lsn, control);
    const begin_checks = state.checks;
    state = .{};
    _ = try baseline.step(7, control);
    const step_checks = state.checks;
    while (try baseline.step(7, control) != .ready) {}
    state = .{};
    var output: [7]u8 = undefined;
    _ = try baseline.readChunk(0, &output, control);
    const chunk_checks = state.checks;
    baseline.destroy();
    for (1..begin_checks + 1) |reject_at| {
        state = .{ .reject_at = reject_at };
        try std.testing.expectError(error.OriginalOwnedReadRejection, client.beginOwnedRead(root.allocator(), lsn, control));
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
    for (1..step_checks + 1) |reject_at| {
        state = .{};
        const operation = try client.beginOwnedRead(root.allocator(), lsn, control);
        state = .{ .reject_at = reject_at };
        try std.testing.expectError(error.OriginalOwnedReadRejection, operation.step(7, control));
        state = .{};
        try std.testing.expectError(error.OriginalOwnedReadRejection, operation.step(7, control));
        operation.destroy();
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
    for (1..chunk_checks + 1) |reject_at| {
        state = .{};
        const operation = try client.beginOwnedRead(root.allocator(), lsn, control);
        while (try operation.step(7, control) != .ready) {}
        state = .{ .reject_at = reject_at };
        try std.testing.expectError(error.OriginalOwnedReadRejection, operation.readChunk(0, &output, control));
        state = .{};
        try std.testing.expectError(error.OriginalOwnedReadRejection, operation.readChunk(0, &output, control));
        operation.destroy();
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
}

test "WAL owned read owner validates version and bounds before opaque pointer access" {
    try error_identity.validateForTest();
    var begin: abi.WalOwnedReadBeginResult = .{ .handle = @ptrFromInt(8) };
    try std.testing.expectEqual(abi.Status.invalid_abi, ownedReadBegin(@ptrFromInt(1), &.{ .version = abi.abi_version - 1, .allocator = @ptrFromInt(8), .port = @ptrFromInt(1) }, &begin));
    try std.testing.expect(begin.handle == null);
    var step: abi.WalOwnedReadStepResult = .{ .ready = 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, ownedReadStep(@ptrFromInt(1), &.{ .version = abi.abi_version - 1 }, &step));
    try std.testing.expectEqual(@as(u8, 0), step.ready);
    for ([_]u64{ 0, 65537, std.math.maxInt(u64) }) |quantum| {
        try std.testing.expectEqual(abi.Status.invalid_argument, ownedReadStep(@ptrFromInt(1), &.{ .quantum = quantum }, &step));
    }
    var chunk: abi.WalOwnedReadChunkResult = .{ .len = 1, .done = 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, ownedReadChunk(@ptrFromInt(1), &.{ .version = abi.abi_version - 1 }, &chunk));
    try std.testing.expectEqual(@as(u64, 0), chunk.len);
    try std.testing.expectEqual(@as(u8, 0), chunk.done);
    try std.testing.expectEqual(abi.Status.invalid_argument, ownedReadChunk(@ptrFromInt(1), &.{ .output = .{ .len = 1 } }, &chunk));
    try std.testing.expectEqual(abi.Status.invalid_argument, ownedReadChunk(@ptrFromInt(1), &.{ .output = .{ .ptr = @ptrFromInt(1), .len = 65537 } }, &chunk));
    ownedReadDestroy(null);
}

test "WAL owned read opaque client preserves lower codec errors and retires failed resources exactly once" {
    const a = std.testing.allocator;
    const erased = @import("antfly_local_sources").storage_backend_erased;
    const read_control = @import("antfly_local_sources").common_read_admission;
    const client_mod = @import("kernel_wal_client.zig");
    var handle: Handle = .{ .wal = try wal_mod.WAL.open(".zig-cache/tmp/owned-codec-errors", .{ .backend = .lsm_memory, .commit_backend = .sync }) };
    defer handle.wal.close();
    const original = handle.wal.store;
    defer handle.wal.store = original;
    const Fixture = struct {
        var cause: anyerror = error.CorruptInput;
        var opened: usize = 0;
        var retired: usize = 0;
        const Box = struct { cause: anyerror };
        fn begin(allocator: std.mem.Allocator, _: *anyopaque, _: @import("antfly_local_sources").storage_backend_types.Namespace, _: []const u8, control: read_control.Port) !erased.OwnedValueRead {
            try control.checkpoint();
            const box = try allocator.create(Box);
            box.* = .{ .cause = cause };
            opened += 1;
            return .{ .allocator = allocator, .ptr = box, .vtable = &.{ .deinit = deinit, .step = step, .take_output = take } };
        }
        fn deinit(allocator: std.mem.Allocator, raw: *anyopaque) void {
            const box: *Box = @ptrCast(@alignCast(raw));
            allocator.destroy(box);
            retired += 1;
        }
        fn step(raw: *anyopaque, _: usize, control: read_control.Port) !erased.OwnedValueRead.Status {
            try control.checkpoint();
            const box: *Box = @ptrCast(@alignCast(raw));
            return box.cause;
        }
        fn take(_: *anyopaque) !erased.OwnedValue {
            return error.InvalidArgument;
        }
    };
    var table = original.vtable.*;
    table.begin_owned_value_read = Fixture.begin;
    handle.wal.store.vtable = &table;
    var client: client_mod.WAL = .{ .clock = undefined, .handle = &handle };
    const Authority = struct {
        reject: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject) return error.OriginalCodecReadRejection;
        }
    };
    var authority: Authority = .{};
    const payload: read_control.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .check_lifetime = Authority.check } };
    const control = read_control.Port.init(&payload);
    Fixture.opened = 0;
    Fixture.retired = 0;
    for ([_]anyerror{ error.CorruptInput, error.InvalidTableFile, error.TableBlockChecksumMismatch }) |cause| {
        var root = std.testing.FailingAllocator.init(a, .{});
        Fixture.cause = cause;
        {
            const operation = try client.beginOwnedRead(root.allocator(), 1, control);
            defer operation.destroy();
            try std.testing.expectError(cause, operation.step(4096, control));
            // Subsequent calls return the exact sticky cause without redispatch or
            // borrowing live provider resources; cleanup requires no fresh grant.
            try std.testing.expectEqual(Fixture.opened, Fixture.retired);
            authority.reject = true;
            var output: [1]u8 = undefined;
            try std.testing.expectError(cause, operation.step(4096, control));
            try std.testing.expectError(cause, operation.readChunk(0, &output, control));
        }
        authority.reject = false;
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
        // Registry slots remain reusable after every failed operation.
        {
            const canceled = try client.beginOwnedRead(root.allocator(), 1, control);
            defer canceled.destroy();
            authority.reject = true;
            try std.testing.expectError(error.OriginalCodecReadRejection, canceled.step(4096, control));
        }
        authority.reject = false;
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
        try std.testing.expectEqual(Fixture.opened, Fixture.retired);
    }
    try std.testing.expectEqual(@as(usize, 6), Fixture.opened);
    try std.testing.expectEqual(@as(u64, 0), handle.wal.lastLsn());
    try error_identity.validateForTest();
}
