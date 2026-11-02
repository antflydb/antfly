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
const bridge = @import("antfly_runtime_abi").io_abi;
var inject = false;
const vtable: std.Io.VTable = blk: {
    var table = std.Options.debug_io.vtable.*;
    table.dirOpenFile = open;
    table.sleep = sleep;
    table.operate = operate;
    table.fileWriteFilePositional = copyFile;
    table.batchAwaitAsync = batchAwait;
    break :blk table;
};

fn open(ptr: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
    if (inject) return error.AccessDenied;
    return std.Options.debug_io.vtable.dirOpenFile(ptr, dir, path, options);
}
fn sleep(ptr: ?*anyopaque, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    if (inject) return error.Canceled;
    return std.Options.debug_io.vtable.sleep(ptr, timeout);
}
fn operate(ptr: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
    if (inject) return switch (operation) {
        .net_send => .{ .net_send = .{ error.NetworkDown, 0 } },
        else => .{ .file_read_streaming = error.InputOutput },
    };
    return std.Options.debug_io.vtable.operate(ptr, operation);
}
fn copyFile(ptr: ?*anyopaque, file: std.Io.File, header: []const u8, reader: *std.Io.File.Reader, limit: std.Io.Limit, offset: u64) std.Io.File.WriteFilePositionalError!usize {
    if (inject) {
        // Both the input and output cached errors must be interpreted in
        // their respective archive, independently of the outer return error.
        std.debug.assert(reader.err.? == error.InputOutput);
        reader.err = error.AccessDenied;
        reader.seek_err = error.EndOfStream;
        return error.ReadFailed;
    }
    return std.Options.debug_io.vtable.fileWriteFilePositional(ptr, file, header, reader, limit, offset);
}
fn batchAwait(ptr: ?*anyopaque, batch: *std.Io.Batch) std.Io.Cancelable!void {
    if (!inject) return std.Options.debug_io.vtable.batchAwaitAsync(ptr, batch);
    // A completed result may be retained by the caller across await calls.
    var index = batch.completed.head;
    while (index != .none) {
        const completion = &batch.storage[index.toIndex()].completion;
        if (completion.result.file_read_streaming) |_| unreachable else |err| std.debug.assert(err == error.InputOutput);
        completion.result = .{ .file_read_streaming = error.AccessDenied };
        index = completion.node.next;
    }
}
export fn runtime_io_abi_test_borrow(output: *bridge.Borrow) callconv(.c) void {
    const runtime = std.heap.page_allocator.create(std.Io.Threaded) catch @panic("test allocator exhausted");
    runtime.* = std.Io.Threaded.init(std.heap.page_allocator, .{});
    const io: std.Io = .{ .userdata = runtime, .vtable = &vtable };
    output.* = bridge.Borrow.init(&io);
}
export fn runtime_io_abi_test_destroy(borrow: *const bridge.Borrow) callconv(.c) void {
    const runtime: *std.Io.Threaded = @ptrCast(@alignCast(borrow.userdata.?));
    runtime.deinit();
    std.heap.page_allocator.destroy(runtime);
}
export fn runtime_io_abi_test_inject(enabled: bool) callconv(.c) void {
    inject = enabled;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

// Real retained storage is constructed here and consumed in another archive.
const retained_storage = @import("antfly_local_sources").storage_db_native_query_remote_storage;
const retained_io = @import("antfly_local_sources").storage_lsm_backend_storage_io;
var retained_marker: u8 = 0;
fn retainedChunk(_: *anyopaque, a: std.mem.Allocator, chunk: retained_storage.Chunk, token: @import("antfly_cancellation").CancellationToken) ![]u8 {
    try token.check();
    return a.dupe(u8, chunk.artifact_id);
}
export fn runtime_io_abi_test_retained_source(output: *retained_io.Storage.Lease) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    const reader = retained_storage.Reader.create(std.heap.c_allocator, std.Options.debug_io, "/retained", &.{.{
        .path = "runs/1.sst",
        .size = 8,
        .chunks = &.{.{ .artifact_id = "abcdefgh", .byte_len = 8, .checksum = "verified-by-provider" }},
    }}, .{ .ptr = &retained_marker, .read = retainedChunk }, .{ .parent = .none, .deadline_ns = std.math.maxInt(u64) }) catch |err|
        return @import("antfly_runtime_abi").error_abi.statusFromError(err);
    output.* = reader.lease();
    return .ok;
}

const CancellationToken = @import("antfly_cancellation").CancellationToken;
var checkpoint_error: u8 = 0;
fn checkpointFailure(_: *const anyopaque) !void {
    return switch (checkpoint_error) {
        0 => error.DeadlineExceeded,
        1 => error.SessionLeaseLost,
        2 => error.ConnectionRefused,
        else => error.Canceled,
    };
}
export fn runtime_io_abi_test_checkpoint(output: *CancellationToken, failure: u8) callconv(.c) void {
    checkpoint_error = failure;
    output.* = .{ .ptr = &checkpoint_error, .check_fn = checkpointFailure };
}
export fn runtime_io_abi_test_consume_checkpoint(token: *const CancellationToken) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    token.check() catch |err| return @import("antfly_runtime_abi").error_abi.statusFromError(err);
    return .ok;
}

const InferenceControl = @import("inference_checkpoint_control").InferenceExecutionControl;
fn inferenceCheckpoint(_: ?*anyopaque) !void {
    if (checkpoint_error == 4) return;
    try checkpointFailure(&checkpoint_error);
}
export fn runtime_io_abi_test_inference_control(output: *InferenceControl, failure: u8) callconv(.c) void {
    checkpoint_error = failure;
    output.* = .{ .check_fn = inferenceCheckpoint };
}
export fn runtime_io_abi_test_consume_inference_control(control: *const InferenceControl) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    control.check() catch |err| return @import("antfly_runtime_abi").error_abi.statusFromError(err);
    return .ok;
}

const RemoteConfig = @import("antfly_scraping").RemoteContentConfig;
var remote_failure: RemoteConfig.RemoteContentRuntime.AcquireStatus = .ok;
var remote_config: RemoteConfig = .{};
fn remoteSnapshot(_: *anyopaque, output: *RemoteConfig.Snapshot) callconv(.c) RemoteConfig.RemoteContentRuntime.AcquireStatus {
    if (remote_failure == .ok) output.* = .{ .config = &remote_config };
    return remote_failure;
}
export fn runtime_io_abi_test_remote_snapshot(output: *RemoteConfig.RemoteContentRuntime, failure: u8) callconv(.c) void {
    remote_failure = @fromBackingInt(@intCast(failure));
    output.* = .{ .context = &remote_failure, .acquire_fn = remoteSnapshot };
}

export fn runtime_io_abi_test_remote_fetch(borrow: *const bridge.Borrow, url: [*]const u8, url_len: usize) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    const errors = @import("antfly_runtime_abi").error_abi;
    var receiver = borrow.receive() catch |err| return errors.statusFromError(err);
    const scraping = @import("antfly_scraping");
    var hosts = [_][]u8{@constCast("127.0.0.1")};
    const security: scraping.ContentSecurityConfig = .{ .block_private_ips = false, .allowed_hosts = &hosts, .max_download_size_bytes = 1024 };
    var outcome = scraping.downloadContentOutcomeAllocWithHeadersAndContext(std.heap.page_allocator, .{ .io = receiver.io(), .timeout_ms = 1000 }, url[0..url_len], &security, null, null) catch |err| return errors.statusFromError(err);
    if (outcome == .ok) outcome.ok.deinit(std.heap.page_allocator);
    return .{};
}

export fn runtime_io_abi_test_threaded_create(output: *?*std.Io.Threaded, output_table: *?*std.Io.VTable) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    const a = std.heap.page_allocator;
    const runtime = a.create(std.Io.Threaded) catch return @import("antfly_runtime_abi").error_abi.statusFromError(error.OutOfMemory);
    runtime.* = std.Io.Threaded.init(a, .{});
    const table = @import("antfly_local_sources").common_threaded_connect_io.createVTable(a, runtime) catch {
        runtime.deinit();
        a.destroy(runtime);
        return @import("antfly_runtime_abi").error_abi.statusFromError(error.OutOfMemory);
    };
    output.* = runtime;
    output_table.* = table;
    return .{};
}
export fn runtime_io_abi_test_threaded_destroy(runtime: *std.Io.Threaded, table: *std.Io.VTable) callconv(.c) void {
    runtime.deinit();
    std.heap.page_allocator.destroy(table);
    std.heap.page_allocator.destroy(runtime);
}

export fn runtime_io_abi_test_durability_storage(contract: *const @import("antfly_runtime_abi").native_abi.TypeContract, output: *retained_io.Storage.Lease, context: *?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
    const abi = @import("antfly_runtime_abi");
    context.* = null;
    if (!contract.matches(abi.native_abi.TypeContract.of(retained_io.Storage.Lease))) return abi.error_abi.statusFromError(error.UnsupportedVersion);
    const allocator = std.heap.c_allocator;
    const native = allocator.create(retained_io.NativeStorage) catch return abi.error_abi.statusFromError(error.OutOfMemory);
    native.* = retained_io.NativeStorage.init(allocator, .threaded) catch |cause| {
        allocator.destroy(native);
        return abi.error_abi.statusFromError(cause);
    };
    output.* = native.storage().acquireLease() catch |cause| {
        native.deinit();
        allocator.destroy(native);
        return abi.error_abi.statusFromError(cause);
    };
    context.* = native;
    return .ok;
}

export fn runtime_io_abi_test_durability_owner_close(raw: *anyopaque) callconv(.c) void {
    const native: *retained_io.NativeStorage = @ptrCast(@alignCast(raw));
    native.deinit();
    std.heap.c_allocator.destroy(native);
}
