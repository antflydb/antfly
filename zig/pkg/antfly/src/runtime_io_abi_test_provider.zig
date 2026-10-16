// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
