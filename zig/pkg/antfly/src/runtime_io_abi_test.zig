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

test "executor archive boundary wakes shutdown events with cancellation signals blocked" {
    if (comptime std.posix.Sigaction == void or !@hasField(std.posix.SIG, "IO")) return error.SkipZigTest;
    // Host executors may mask signals. Workers inherit that mask, so this
    // test cannot pass by interrupting a sleep with SIGIO.
    var mask = std.posix.sigemptyset();
    std.posix.sigaddset(&mask, .IO);
    var old_mask: std.posix.sigset_t = undefined;
    std.posix.sigprocmask(std.posix.SIG.BLOCK, &mask, &old_mask);
    defer std.posix.sigprocmask(std.posix.SIG.SETMASK, &old_mask, null);
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var executor = try borrow.receive();
    const io = executor.io();
    const Worker = struct {
        fn run(task_io: std.Io, stop: *std.Io.Event, cleaned: *std.atomic.Value(bool)) void {
            defer cleaned.store(true, .release);
            stop.waitTimeout(task_io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } }) catch {};
        }
    };
    var stop: std.Io.Event = .unset;
    var cleaned: std.atomic.Value(bool) = .init(false);
    var group: std.Io.Group = .init;
    try group.concurrent(io, Worker.run, .{ io, &stop, &cleaned });
    defer group.cancel(io);
    defer stop.set(io);
    const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
    while (@atomicLoad(std.Io.Event, &stop, .acquire) != .waiting) {
        try std.testing.expect(std.Io.Clock.awake.now(std.testing.io).nanoseconds < deadline.nanoseconds);
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const start = std.Io.Clock.awake.now(std.testing.io);
    stop.set(io);
    group.cancel(io);
    try std.testing.expect(cleaned.load(.acquire));
    try std.testing.expect(start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toMilliseconds() < 1000);
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
    const sent = (try io.operate(.{ .net_send = .{ .socket_handle = undefined, .messages = &.{}, .flags = .{} } })).net_send;
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

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

const retained_io = @import("antfly_local_sources").storage_lsm_backend_storage_io;
extern fn runtime_io_abi_test_retained_source(output: *retained_io.Storage.Lease) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;

test "executor archive boundary opens retained immutable sources and preserves their leases" {
    var lease: retained_io.Storage.Lease = undefined;
    const status = runtime_io_abi_test_retained_source(&lease);
    if (!status.isOk()) return @import("antfly_runtime_abi").error_abi.errorFromStatus(status);
    var leased = true;
    defer if (leased) lease.deinit();
    var source = try lease.view.openLeasedImmutableSource(std.testing.allocator, "/retained/runs/1.sst");
    defer source.close();
    // The source must retain its immutable owner after the storage view closes.
    lease.deinit();
    leased = false;
    var bytes: [4]u8 = undefined;
    try source.readInto(2, &bytes);
    try std.testing.expectEqualStrings("cdef", &bytes);
    try std.testing.expectError(error.EndOfStream, source.readInto(7, &bytes));
}

const CancellationToken = @import("antfly_cancellation").CancellationToken;
extern fn runtime_io_abi_test_checkpoint(*CancellationToken, u8) callconv(.c) void;
extern fn runtime_io_abi_test_consume_checkpoint(*const CancellationToken) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;

test "executor archive boundary checkpoints retain deadline lease and connection error identity" {
    for ([_]anyerror{ error.DeadlineExceeded, error.SessionLeaseLost, error.ConnectionRefused, error.Canceled }, 0..) |expected, i| {
        var token: CancellationToken = undefined;
        runtime_io_abi_test_checkpoint(&token, @intCast(i));
        try std.testing.expectError(expected, token.check());
        try std.testing.expect(token.isCancelled());
    }
    const Local = struct {
        fn check(_: *const anyopaque) !void {
            return error.ConnectionRefused;
        }
    };
    var marker: u8 = 0;
    const local_token: CancellationToken = .{ .ptr = &marker, .check_fn = Local.check };
    const status = runtime_io_abi_test_consume_checkpoint(&local_token);
    try std.testing.expectEqual(error.ConnectionRefused, @import("antfly_runtime_abi").error_abi.errorFromStatus(status));
}

const InferenceControl = @import("inference_checkpoint_control").InferenceExecutionControl;
extern fn runtime_io_abi_test_inference_control(output: *InferenceControl, failure: u8) callconv(.c) void;
extern fn runtime_io_abi_test_consume_inference_control(control: *const InferenceControl) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;
test "executor archive boundary composes inference checkpoints in both error domains" {
    const abi = @import("antfly_runtime_abi");
    const Combined = struct {
        parent: InferenceControl,
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            try self.parent.check();
            return error.ConnectionRefused;
        }
    };
    for ([_]anyerror{ error.DeadlineExceeded, error.SessionLeaseLost, error.ConnectionRefused, error.Canceled, error.ConnectionRefused }, 0..) |expected, i| {
        var parent: InferenceControl = undefined;
        runtime_io_abi_test_inference_control(&parent, @intCast(i));
        var combined: Combined = .{ .parent = parent };
        const rebound = parent.withCheckpoint(&combined, Combined.check);
        try std.testing.expectError(expected, rebound.check());
        const status = runtime_io_abi_test_consume_inference_control(&rebound);
        try std.testing.expectEqual(expected, abi.error_abi.errorFromStatus(status));
        try std.testing.expectEqual(@as(usize, 2), combined.calls);
    }
}

test "executor archive boundary requests cancellation without joining retained callback" {
    var borrow: bridge.Borrow = undefined;
    runtime_io_abi_test_borrow(&borrow);
    defer runtime_io_abi_test_destroy(&borrow);
    var receiver = try borrow.receive();
    const io = receiver.io();
    const F = struct {
        io: std.Io,
        canceled: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(self: *@This()) std.Io.Cancelable!void {
            self.io.sleep(.fromSeconds(3600), .awake) catch |cause| {
                self.canceled.set(self.io);
                self.release.waitUncancelable(self.io);
                return cause;
            };
        }
    };
    // Request immediately after submission to exercise the queued/start race,
    // then retain the callback after cancellation to prove request != join.
    for (0..64) |_| {
        var f: F = .{ .io = io };
        var future = try io.concurrent(F.run, .{&f});
        defer {
            f.release.set(io);
            _ = future.cancel(io) catch {};
        }
        try std.testing.expect(future.requestCancel(io));
        try f.canceled.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expect(future.any_future != null);
        try std.testing.expect(future.requestCancel(io));
        f.release.set(io);
        try std.testing.expectError(error.Canceled, future.await(io));
        try std.testing.expect(future.requestCancel(io));
    }
    // A cancellation for a completed registration must not poison the next
    // task running on the same physical executor.
    const Healthy = struct {
        fn run(lane: std.Io) !void {
            try lane.sleep(.fromMilliseconds(1), .awake);
        }
    };
    var healthy = try io.concurrent(Healthy.run, .{io});
    try healthy.await(io);
}

const RemoteConfig = @import("antfly_scraping").RemoteContentConfig;
extern fn runtime_io_abi_test_remote_snapshot(output: *RemoteConfig.RemoteContentRuntime, failure: u8) callconv(.c) void;
test "executor archive boundary remote snapshot failures retain stable identities" {
    var runtime: RemoteConfig.RemoteContentRuntime = undefined;
    runtime_io_abi_test_remote_snapshot(&runtime, 0);
    var snapshot = try runtime.acquire();
    snapshot.deinit();
    for ([_]anyerror{ error.OutOfMemory, error.Timeout, error.Canceled, error.ResourceTemporarilyUnavailable }, 1..) |expected, code| {
        runtime_io_abi_test_remote_snapshot(&runtime, @intCast(code));
        try std.testing.expectError(expected, runtime.acquire());
    }
}

extern fn runtime_io_abi_test_remote_fetch(borrow: *const bridge.Borrow, url: [*]const u8, url_len: usize) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;
test "executor archive boundary remote PDF connection refusal retains its error identity" {
    const io = std.testing.io;
    var listener = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(io, .{});
    const url = try std.fmt.allocPrint(std.testing.allocator, "http://{f}/sources/unreachable.pdf", .{listener.socket.address});
    defer std.testing.allocator.free(url);
    listener.deinit(io);
    const borrow = bridge.Borrow.init(&io);
    const result = runtime_io_abi_test_remote_fetch(&borrow, url.ptr, url.len);
    try std.testing.expect(!result.isOk());
    try std.testing.expectEqual(error.ConnectionRefused, @import("antfly_runtime_abi").error_abi.errorFromStatus(result));
}

extern fn runtime_io_abi_test_threaded_create(output: *?*std.Io.Threaded, output_table: *?*std.Io.VTable) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;
extern fn runtime_io_abi_test_threaded_destroy(runtime: *std.Io.Threaded, table: *std.Io.VTable) callconv(.c) void;
test "executor archive boundary outbound view uses the calling archive error domain" {
    var runtime: ?*std.Io.Threaded = null;
    var table: ?*std.Io.VTable = null;
    const status = runtime_io_abi_test_threaded_create(&runtime, &table);
    if (!status.isOk()) return @import("antfly_runtime_abi").error_abi.errorFromStatus(status);
    defer runtime_io_abi_test_threaded_destroy(runtime.?, table.?);
    const io = @import("antfly_local_sources").common_threaded_connect_io.io(runtime.?);
    try std.testing.expect(io.vtable != table.?);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, "/antfly-network-domain-test-missing-file", .{}));
    var listener = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(std.testing.io, .{});
    const url = try std.fmt.allocPrint(std.testing.allocator, "http://{f}/unreachable.pdf", .{listener.socket.address});
    defer std.testing.allocator.free(url);
    listener.deinit(std.testing.io);
    const scraping = @import("antfly_scraping");
    var hosts = [_][]u8{@constCast("127.0.0.1")};
    const security: scraping.ContentSecurityConfig = .{ .block_private_ips = false, .allowed_hosts = &hosts, .max_download_size_bytes = 1024 };
    try std.testing.expectError(error.ConnectionRefused, scraping.downloadContentOutcomeAllocWithHeadersAndContext(std.testing.allocator, .{ .io = io, .timeout_ms = 1000 }, url, &security, null, null));
}

extern fn runtime_io_abi_test_durability_storage(*const @import("antfly_runtime_abi").native_abi.TypeContract, *retained_io.Storage.Lease, *?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;
extern fn runtime_io_abi_test_durability_owner_close(*anyopaque) callconv(.c) void;

test "executor archive boundary durability grant retains physical authority and original errors" {
    const os = @import("builtin").os.tag;
    if (os == .windows or os == .wasi or os == .freestanding) return error.SkipZigTest;
    const abi = @import("antfly_runtime_abi");
    const admission = @import("antfly_local_sources").common_read_admission;
    const Control = struct {
        rejected: bool = false,
        deadline_ns: u64,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejected) return error.SessionLeaseLost;
        }
        fn deadline(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.deadline_ns;
        }
        fn begin(_: *anyopaque) !void {}
        fn end(_: *anyopaque) !void {}
    };
    var lease: retained_io.Storage.Lease = undefined;
    var owner: ?*anyopaque = null;
    defer if (owner) |context| runtime_io_abi_test_durability_owner_close(context);
    const contract = abi.native_abi.TypeContract.of(retained_io.Storage.Lease);
    var older = contract;
    older.version -= 1;
    const refused = runtime_io_abi_test_durability_storage(&older, &lease, &owner);
    try std.testing.expectEqual(error.UnsupportedVersion, abi.error_abi.errorFromStatus(refused));
    try std.testing.expect(owner == null);
    const status = runtime_io_abi_test_durability_storage(&contract, &lease, &owner);
    if (!status.isOk()) return abi.error_abi.errorFromStatus(status);
    var leased = true;
    defer if (leased) lease.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/CURRENT", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const missing = try std.fmt.allocPrint(std.testing.allocator, "{s}.missing", .{path});
    defer std.testing.allocator.free(missing);
    try lease.view.writeFileAbsolute(path, "archive-control");
    var control: Control = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s };
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    var grant = (try lease.view.tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
    defer grant.deinit();
    var read = try grant.beginControlRead(path, 36, original);
    var read_live = true;
    defer if (read_live) read.deinit();
    control.rejected = true;
    try std.testing.expectError(error.SessionLeaseLost, read.waitWithAdmission(original));
    // The producer's original owner and storage lease may both close while a
    // rejected consumer still owns a phase and its reusable completion grant.
    runtime_io_abi_test_durability_owner_close(owner.?);
    owner = null;
    lease.deinit();
    leased = false;
    control.rejected = false;
    const result = try read.waitWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 15), result.file_size);
    try std.testing.expectEqualStrings("archive-control", result.bytes);
    try std.testing.expectError(error.ResourceBudgetExceeded, grant.beginSyncContents(path, original));
    read.deinit();
    read_live = false;
    var sync = try grant.beginSyncContents(path, original);
    try sync.waitWithAdmission(original);
    sync.deinit();
    var absent = try grant.beginSyncContents(missing, original);
    try std.testing.expectError(error.FileNotFound, absent.waitWithAdmission(original));
    try std.testing.expectError(error.FileNotFound, absent.waitWithAdmission(original));
    absent.deinit();
    var parent = try grant.beginSyncParent(path, original);
    try parent.waitWithAdmission(original);
    parent.deinit();
    control.rejected = true;
    try std.testing.expectError(error.SessionLeaseLost, grant.beginControlRead(path, 36, original));
    control.rejected = false;
    var healthy = try grant.beginControlRead(path, 36, original);
    defer healthy.deinit();
    try std.testing.expectEqualStrings("archive-control", (try healthy.waitWithAdmission(original)).bytes);
}

test "executor archive boundary WAL sync distinguishes original missing-file rejection from absent CURRENT" {
    const os = @import("builtin").os.tag;
    if (os == .windows or os == .wasi or os == .freestanding) return error.SkipZigTest;
    const abi = @import("antfly_runtime_abi");
    const local = @import("antfly_local_sources");
    const admission = local.common_read_admission;
    const wal = local.storage_lsm_backend_wal;
    const Publication = struct {
        held: *bool,
        releases: *usize,
        pub fn release(self: *@This()) void {
            self.held.* = false;
            self.releases.* += 1;
        }
    };
    const Shim = struct {
        var upstream: @FieldType(retained_io.ControlFileReadOperation, "boundary_dispatch") = undefined;
        var inside: bool = false;
        var calls: usize = 0;
        fn dispatch(contract: *const abi.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            calls += 1;
            inside = true;
            defer inside = false;
            return upstream(contract, callback, args, output);
        }
    };
    const Control = struct {
        rejected: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Reject inside the independently compiled read implementation,
            // after the coordinator has entered its physical-wait boundary.
            if (self.rejected and Shim.inside) return error.FileNotFound;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(_: *anyopaque) !void {}
        fn end(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var lease: retained_io.Storage.Lease = undefined;
    var owner: ?*anyopaque = null;
    defer if (owner) |context| runtime_io_abi_test_durability_owner_close(context);
    const contract = abi.native_abi.TypeContract.of(retained_io.Storage.Lease);
    const status = runtime_io_abi_test_durability_storage(&contract, &lease, &owner);
    if (!status.isOk()) return abi.error_abi.errorFromStatus(status);
    var leased = true;
    defer if (leased) lease.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(root);
    var state: local.storage_lsm_backend_state.State = .{};
    defer state.deinit(a);
    try state.upsert(a, .{ .name = "docs" }, "doc", "state", false);
    _ = try wal.appendState(lease.view, a, root, &state, false);
    var control: Control = .{};
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    var held = true;
    var releases: usize = 0;
    var operation = try wal.CurrentStateSync(Publication).prepare(root);
    var resources = (try lease.view.tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
    operation.start(resources, .{ .held = &held, .releases = &releases }, original) catch |cause| {
        resources.deinit();
        return cause;
    };
    defer operation.deinit();
    Shim.upstream = operation.pending.read.boundary_dispatch;
    Shim.calls = 0;
    operation.pending.read.boundary_dispatch = Shim.dispatch;
    control.rejected = true;
    try std.testing.expectError(error.FileNotFound, operation.waitWithAdmission(original));
    try std.testing.expect(held and operation.stage == .current);
    try std.testing.expectEqual(@as(usize, 0), releases);
    try std.testing.expectEqual(@as(usize, 1), Shim.calls);
    runtime_io_abi_test_durability_owner_close(owner.?);
    owner = null;
    lease.deinit();
    leased = false;
    control.rejected = false;
    const result = try operation.waitWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 1), result.segment_syncs);
    try std.testing.expectEqual(@as(u64, 1), result.index_syncs);
    try std.testing.expect(!held);
    try std.testing.expectEqual(@as(usize, 1), releases);
    _ = try operation.waitWithAdmission(original);
    try std.testing.expectEqual(@as(usize, 2), Shim.calls);
}
