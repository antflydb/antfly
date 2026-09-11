// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const bridge = @import("runtime_io_abi.zig");
extern fn runtime_io_abi_test_borrow(*bridge.Borrow) callconv(.c) void;
extern fn runtime_io_abi_test_inject(bool) callconv(.c) void;
extern fn runtime_io_abi_test_destroy(*const bridge.Borrow) callconv(.c) void;

test "executor archive boundary cancels futures and drains group ownership" {
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var executor = try borrow.receive();
    const io = executor.io();
    const Worker = struct {
        fn run(task_io: std.Io) std.Io.Cancelable!void {
            try task_io.sleep(.fromSeconds(3600), .awake);
        }

        fn grouped(task_io: std.Io, cleaned: *std.atomic.Value(bool)) void {
            defer cleaned.store(true, .release);
            run(task_io) catch {};
        }
    };
    var future = try io.concurrent(Worker.run, .{io});
    try std.testing.expectError(error.Canceled, future.cancel(io));
    var cleaned: std.atomic.Value(bool) = .init(false);
    var group: std.Io.Group = .init;
    try group.concurrent(io, Worker.grouped, .{ io, &cleaned });
    group.cancel(io);
    try std.testing.expect(cleaned.load(.acquire));
}

test "executor archive boundary preserves file errors and cancellation" {
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var executor = try borrow.receive();
    const io = executor.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(error.FileNotFound, tmp.dir.openFile(io, "missing", .{}));
    runtime_io_abi_test_inject(true);
    defer runtime_io_abi_test_inject(false);
    try std.testing.expectError(error.AccessDenied, tmp.dir.openFile(io, "missing", .{}));
    try std.testing.expectError(error.Canceled, io.sleep(.fromNanoseconds(1), .awake));
    const Worker = struct {
        fn run(task_io: std.Io) std.Io.Cancelable!u32 {
            try task_io.sleep(.fromNanoseconds(1), .awake);
            return 42;
        }
    };
    var future = try io.concurrent(Worker.run, .{io});
    try std.testing.expectError(error.Canceled, future.await(io));
}

test "executor archive boundary translates embedded operation batch and reader errors" {
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var executor = try borrow.receive();
    const io = executor.io();
    runtime_io_abi_test_inject(true);
    defer runtime_io_abi_test_inject(false);
    const result = try io.operate(.{ .file_read_streaming = .{ .file = .stdin(), .data = &.{} } });
    try std.testing.expectError(error.InputOutput, result.file_read_streaming);
    const sent = io.vtable.netSend(io.userdata, undefined, &.{}, .{});
    try std.testing.expectEqual(error.NetworkDown, sent[0].?);
    try std.testing.expectEqual(@as(usize, 0), sent[1]);

    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch = std.Io.Batch.init(&storage);
    _ = batch.add(.{ .file_read_streaming = .{ .file = .stdin(), .data = &.{} } });
    batch.submitted = .empty;
    batch.completed = .{ .head = .fromIndex(0), .tail = .fromIndex(0) };
    storage[0] = .{ .completion = .{ .node = .{ .next = .none }, .result = result } };
    try batch.awaitAsync(io);
    try std.testing.expectError(error.AccessDenied, batch.next().?.result.file_read_streaming);
    batch.cancel(io);

    var reader = std.Io.File.Reader.init(.stdin(), io, &.{});
    reader.err = error.InputOutput;
    try std.testing.expectError(error.ReadFailed, io.vtable.fileWriteFilePositional(io.userdata, .stdout(), "", &reader, .unlimited, 0));
    try std.testing.expectEqual(error.AccessDenied, reader.err.?);
    try std.testing.expectEqual(error.EndOfStream, reader.seek_err.?);
}

test "executor archive boundary round trips real batched file IO and task results" {
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var executor = try borrow.receive();
    const io = executor.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "data", .data = "hello" });
    const file = try tmp.dir.openFile(io, "data", .{});
    defer file.close(io);
    var data: [5]u8 = undefined;
    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch = std.Io.Batch.init(&storage);
    defer batch.cancel(io);
    _ = batch.add(.{ .file_read_streaming = .{ .file = file, .data = &.{&data} } });
    try batch.awaitConcurrent(io, .none);
    try std.testing.expectEqual(@as(usize, 5), try batch.next().?.result.file_read_streaming);
    try std.testing.expectEqualStrings("hello", &data);
    const Worker = struct {
        fn run() error{TaskPrivateError}!u32 {
            return error.TaskPrivateError;
        }
    };
    var future = try io.concurrent(Worker.run, .{});
    try std.testing.expectError(error.TaskPrivateError, future.await(io));
}
