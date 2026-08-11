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

//! Low-volume system-keyspace adapter for an opaque storage context. This is
//! deliberately not a document-store ABI: table reads, writes, queries, and
//! maintenance stay on their existing coarse kernel operations.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const backend = @import("backend_erased.zig");
const error_identity = @import("kernel_error_identity");

pub fn open(
    allocator: std.mem.Allocator,
    context: ?*anyopaque,
    namespace: []const u8,
) !backend.Store {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_context_system_store_open(context, &.{
        .namespace = .fromSlice(namespace),
    }, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &store_vtable,
    };
}

const store_vtable = backend.Store.VTable{
    .deinit = storeDeinit,
    .capabilities = storeCapabilities,
    .begin_read = storeBeginRead,
    .begin_current_scan = storeBeginCurrentScan,
    .begin_write = storeBeginWrite,
    .begin_batch = storeBeginBatch,
    .sync = storeSync,
};

fn storeDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
    abi.antfly_storage_system_store_close(ptr);
}

fn storeCapabilities(_: *anyopaque) backend.types.Capabilities {
    return .{};
}

fn storeBeginRead(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.ReadTxn {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_store_begin_read(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &read_vtable,
    };
}

const read_vtable = backend.ReadTxn.VTable{
    .abort = readAbort,
    .get = readGet,
    .open_cursor = readOpenCursor,
};

fn readAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    abi.antfly_storage_system_read_abort(ptr);
}

fn readGet(ptr: *anyopaque, key: []const u8) ![]const u8 {
    var value: abi.BorrowedBytes = .{};
    try statusToError(abi.antfly_storage_system_read_get(ptr, .fromSlice(key), &value));
    return value.slice();
}

fn readOpenCursor(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.Cursor {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_read_open_cursor(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &cursor_vtable,
    };
}

fn storeBeginCurrentScan(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.CurrentScanTxn {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_store_begin_current_scan(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &current_scan_vtable,
    };
}

const current_scan_vtable = backend.CurrentScanTxn.VTable{
    .abort = currentScanAbort,
    .open_cursor = currentScanOpenCursor,
};

fn currentScanAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    abi.antfly_storage_system_current_scan_abort(ptr);
}

fn currentScanOpenCursor(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.Cursor {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_current_scan_open_cursor(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &cursor_vtable,
    };
}

const cursor_vtable = backend.Cursor.VTable{
    .close = cursorClose,
    .first = cursorFirst,
    .last = cursorLast,
    .next = cursorNext,
    .prev = cursorPrevious,
    .seek_at_or_after = cursorAtOrAfter,
    .seek_at_or_before = cursorAtOrBefore,
};

fn cursorClose(_: std.mem.Allocator, ptr: *anyopaque) void {
    abi.antfly_storage_system_cursor_close(ptr);
}

fn cursorMove(ptr: *anyopaque, operation: abi.SystemCursorSeek, key: []const u8) !?backend.Entry {
    var result: abi.SystemEntryResult = .{};
    try statusToError(abi.antfly_storage_system_cursor_move(
        ptr,
        operation,
        .fromSlice(key),
        &result,
    ));
    if (result.present == 0) return null;
    return .{ .key = result.key.slice(), .value = result.value.slice() };
}

fn cursorFirst(ptr: *anyopaque) !?backend.Entry {
    return try cursorMove(ptr, .first, "");
}

fn cursorLast(ptr: *anyopaque) !?backend.Entry {
    return try cursorMove(ptr, .last, "");
}

fn cursorNext(ptr: *anyopaque) !?backend.Entry {
    return try cursorMove(ptr, .next, "");
}

fn cursorPrevious(ptr: *anyopaque) !?backend.Entry {
    return try cursorMove(ptr, .previous, "");
}

fn cursorAtOrAfter(ptr: *anyopaque, key: []const u8) !?backend.Entry {
    return try cursorMove(ptr, .at_or_after, key);
}

fn cursorAtOrBefore(ptr: *anyopaque, key: []const u8) !?backend.Entry {
    return try cursorMove(ptr, .at_or_before, key);
}

fn storeBeginWrite(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.WriteTxn {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_store_begin_write(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &write_vtable,
    };
}

const write_vtable = backend.WriteTxn.VTable{
    .abort = writeAbort,
    .commit = writeCommit,
    .get = writeGet,
    .put = writePut,
    .delete = writeDelete,
    .open_cursor = writeOpenCursor,
};

fn writeAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    abi.antfly_storage_system_write_abort(ptr);
}

fn writeCommit(_: std.mem.Allocator, ptr: *anyopaque) !void {
    try statusToError(abi.antfly_storage_system_write_commit(ptr));
}

fn writeGet(ptr: *anyopaque, key: []const u8) ![]const u8 {
    var value: abi.BorrowedBytes = .{};
    try statusToError(abi.antfly_storage_system_write_get(ptr, .fromSlice(key), &value));
    return value.slice();
}

fn writePut(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
    try statusToError(abi.antfly_storage_system_write_put(
        ptr,
        .fromSlice(key),
        .fromSlice(value),
    ));
}

fn writeDelete(ptr: *anyopaque, key: []const u8) !void {
    try statusToError(abi.antfly_storage_system_write_delete(ptr, .fromSlice(key)));
}

fn writeOpenCursor(_: std.mem.Allocator, _: *anyopaque) !backend.Cursor {
    return error.Unsupported;
}

fn storeBeginBatch(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.Batch {
    var handle: ?*anyopaque = null;
    try statusToError(abi.antfly_storage_system_store_begin_write(ptr, &handle));
    return .{
        .allocator = allocator,
        .ptr = handle orelse return error.StorageKernelFailure,
        .vtable = &batch_vtable,
    };
}

const batch_vtable = backend.Batch.VTable{
    .abort = writeAbort,
    .commit = writeCommit,
    .get = writeGet,
    .put = writePut,
    .delete = writeDelete,
};

fn storeSync(ptr: *anyopaque, force: bool) !void {
    try statusToError(abi.antfly_storage_system_store_sync(ptr, @intFromBool(force)));
}

const SingleNamespaceState = struct {
    allocator: std.mem.Allocator,
    store: *backend.Store,
    expected: []u8,
};

const SingleNamespaceRead = struct {
    allocator: std.mem.Allocator,
    txn: backend.ReadTxn,
    expected: []const u8,
};

const SingleNamespaceWrite = struct {
    allocator: std.mem.Allocator,
    txn: backend.WriteTxn,
    expected: []const u8,
};

const SingleNamespaceBatch = struct {
    allocator: std.mem.Allocator,
    txn: backend.Batch,
    expected: []const u8,
};

/// Adapts one kernel-owned system store to the namespaced contract expected by
/// the user and policy managers. The adapter validates the one allowed logical
/// namespace and never owns the underlying store.
pub fn singleNamespaceStore(
    allocator: std.mem.Allocator,
    store: *backend.Store,
    expected: []const u8,
) !backend.NamespaceStore {
    const state = try allocator.create(SingleNamespaceState);
    errdefer allocator.destroy(state);
    state.* = .{
        .allocator = allocator,
        .store = store,
        .expected = try allocator.dupe(u8, expected),
    };
    return .{
        .allocator = allocator,
        .ptr = state,
        .vtable = &single_namespace_vtable,
    };
}

const single_namespace_vtable = backend.NamespaceStore.VTable{
    .deinit = singleNamespaceDeinit,
    .capabilities = singleNamespaceCapabilities,
    .begin_read = singleNamespaceBeginRead,
    .begin_write = singleNamespaceBeginWrite,
    .begin_batch = singleNamespaceBeginBatch,
};

fn singleNamespaceState(ptr: *anyopaque) *SingleNamespaceState {
    return @ptrCast(@alignCast(ptr));
}

fn singleNamespaceDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
    const state = singleNamespaceState(ptr);
    state.allocator.free(state.expected);
    state.allocator.destroy(state);
}

fn singleNamespaceCapabilities(ptr: *anyopaque) backend.types.Capabilities {
    return singleNamespaceState(ptr).store.capabilities();
}

fn requireNamespace(expected: []const u8, namespace: backend.types.Namespace) !void {
    const actual = namespace.name orelse return error.InvalidArgument;
    if (!std.mem.eql(u8, expected, actual)) return error.InvalidArgument;
}

fn singleNamespaceBeginRead(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.NamespaceReadTxn {
    const state = singleNamespaceState(ptr);
    var txn = try state.store.beginRead();
    errdefer txn.abort();
    const wrapper = try allocator.create(SingleNamespaceRead);
    wrapper.* = .{ .allocator = allocator, .txn = txn, .expected = state.expected };
    return .{ .allocator = allocator, .ptr = wrapper, .vtable = &single_namespace_read_vtable };
}

const single_namespace_read_vtable = backend.NamespaceReadTxn.VTable{
    .abort = singleNamespaceReadAbort,
    .get = singleNamespaceReadGet,
    .open_cursor = singleNamespaceReadOpenCursor,
};

fn singleNamespaceRead(ptr: *anyopaque) *SingleNamespaceRead {
    return @ptrCast(@alignCast(ptr));
}

fn singleNamespaceReadAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    const wrapper = singleNamespaceRead(ptr);
    wrapper.txn.abort();
    wrapper.allocator.destroy(wrapper);
}

fn singleNamespaceReadGet(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
) ![]const u8 {
    const wrapper = singleNamespaceRead(ptr);
    try requireNamespace(wrapper.expected, namespace);
    return try wrapper.txn.get(key);
}

fn singleNamespaceReadOpenCursor(
    _: std.mem.Allocator,
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
) !backend.Cursor {
    const wrapper = singleNamespaceRead(ptr);
    try requireNamespace(wrapper.expected, namespace);
    return try wrapper.txn.openCursor();
}

fn singleNamespaceBeginWrite(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.NamespaceWriteTxn {
    const state = singleNamespaceState(ptr);
    var txn = try state.store.beginWrite();
    errdefer txn.abort();
    const wrapper = try allocator.create(SingleNamespaceWrite);
    wrapper.* = .{ .allocator = allocator, .txn = txn, .expected = state.expected };
    return .{ .allocator = allocator, .ptr = wrapper, .vtable = &single_namespace_write_vtable };
}

const single_namespace_write_vtable = backend.NamespaceWriteTxn.VTable{
    .abort = singleNamespaceWriteAbort,
    .commit = singleNamespaceWriteCommit,
    .get = singleNamespaceWriteGet,
    .put = singleNamespaceWritePut,
    .delete = singleNamespaceWriteDelete,
};

fn singleNamespaceWrite(ptr: *anyopaque) *SingleNamespaceWrite {
    return @ptrCast(@alignCast(ptr));
}

fn singleNamespaceWriteAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    const wrapper = singleNamespaceWrite(ptr);
    wrapper.txn.abort();
    wrapper.allocator.destroy(wrapper);
}

fn singleNamespaceWriteCommit(_: std.mem.Allocator, ptr: *anyopaque) !void {
    const wrapper = singleNamespaceWrite(ptr);
    try wrapper.txn.commit();
    wrapper.allocator.destroy(wrapper);
}

fn singleNamespaceWriteGet(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
) ![]const u8 {
    const wrapper = singleNamespaceWrite(ptr);
    try requireNamespace(wrapper.expected, namespace);
    return try wrapper.txn.get(key);
}

fn singleNamespaceWritePut(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
    value: []const u8,
) !void {
    const wrapper = singleNamespaceWrite(ptr);
    try requireNamespace(wrapper.expected, namespace);
    try wrapper.txn.put(key, value);
}

fn singleNamespaceWriteDelete(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
) !void {
    const wrapper = singleNamespaceWrite(ptr);
    try requireNamespace(wrapper.expected, namespace);
    try wrapper.txn.delete(key);
}

fn singleNamespaceBeginBatch(allocator: std.mem.Allocator, ptr: *anyopaque) !backend.NamespaceBatch {
    const state = singleNamespaceState(ptr);
    var txn = try state.store.beginBatch();
    errdefer txn.abort();
    const wrapper = try allocator.create(SingleNamespaceBatch);
    wrapper.* = .{ .allocator = allocator, .txn = txn, .expected = state.expected };
    return .{ .allocator = allocator, .ptr = wrapper, .vtable = &single_namespace_batch_vtable };
}

const single_namespace_batch_vtable = backend.NamespaceBatch.VTable{
    .abort = singleNamespaceBatchAbort,
    .commit = singleNamespaceBatchCommit,
    .get = singleNamespaceBatchGet,
    .put = singleNamespaceBatchPut,
    .delete = singleNamespaceBatchDelete,
};

fn singleNamespaceBatch(ptr: *anyopaque) *SingleNamespaceBatch {
    return @ptrCast(@alignCast(ptr));
}

fn singleNamespaceBatchAbort(_: std.mem.Allocator, ptr: *anyopaque) void {
    const wrapper = singleNamespaceBatch(ptr);
    wrapper.txn.abort();
    wrapper.allocator.destroy(wrapper);
}

fn singleNamespaceBatchCommit(_: std.mem.Allocator, ptr: *anyopaque) !void {
    const wrapper = singleNamespaceBatch(ptr);
    try wrapper.txn.commit();
    wrapper.allocator.destroy(wrapper);
}

fn singleNamespaceBatchGet(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
) ![]const u8 {
    const wrapper = singleNamespaceBatch(ptr);
    try requireNamespace(wrapper.expected, namespace);
    return try wrapper.txn.get(key);
}

fn singleNamespaceBatchPut(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
    value: []const u8,
) !void {
    const wrapper = singleNamespaceBatch(ptr);
    try requireNamespace(wrapper.expected, namespace);
    try wrapper.txn.put(key, value);
}

fn singleNamespaceBatchDelete(
    ptr: *anyopaque,
    namespace: backend.types.Namespace,
    key: []const u8,
) !void {
    const wrapper = singleNamespaceBatch(ptr);
    try requireNamespace(wrapper.expected, namespace);
    try wrapper.txn.delete(key);
}

fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}
