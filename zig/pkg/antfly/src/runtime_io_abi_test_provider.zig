// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const bridge = @import("runtime_io_abi.zig");
var inject = false;
const vtable: std.Io.VTable = blk: {
    var table = std.Options.debug_io.vtable.*;
    table.dirOpenFile = open;
    table.sleep = sleep;
    table.operate = operate;
    table.netSend = send;
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
    if (inject) return .{ .file_read_streaming = error.InputOutput };
    return std.Options.debug_io.vtable.operate(ptr, operation);
}
fn send(ptr: ?*anyopaque, socket: std.Io.net.Socket.Handle, messages: []std.Io.net.OutgoingMessage, flags: std.Io.net.SendFlags) struct { ?std.Io.net.Socket.SendError, usize } {
    if (inject) return .{ error.NetworkDown, 0 };
    return std.Options.debug_io.vtable.netSend(ptr, socket, messages, flags);
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
