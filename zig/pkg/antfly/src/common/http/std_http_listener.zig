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

const builtin = @import("builtin");
const std = @import("std");
const common = @import("http_common.zig");

pub const default_max_request_bytes: usize = 32 * 1024 * 1024;
pub const default_request_stack_size: usize = 8 * 1024 * 1024;

fn sleepMs(ms: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

pub const StdHttpListenerConfig = struct {
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    kernel_backlog: u31 = 512,
    reuse_address: bool = true,
    recv_buffer_bytes: usize = 8 * 1024,
    send_buffer_bytes: usize = 8 * 1024,
    max_request_bytes: usize = default_max_request_bytes,
    thread_stack_size: usize = default_request_stack_size,
    serve_in_connection_threads: bool = false,
    connection_thread_stack_size: usize = default_request_stack_size,
    max_connection_threads: u32 = 0,
};

pub const StdHttpListener = struct {
    const IoOwner = enum {
        owned,
        shared,
    };

    alloc: std.mem.Allocator,
    cfg: StdHttpListenerConfig,
    app: common.RequestExecutor,
    streaming_app: ?common.StreamingRequestExecutor = null,
    io_impl: *std.Io.Threaded,
    io_owner: IoOwner,
    server: ?std.Io.net.Server = null,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    active_connection_threads: std.atomic.Value(u32) = .init(0),

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: StdHttpListenerConfig,
        app: common.RequestExecutor,
    ) StdHttpListener {
        const io_impl = alloc.create(std.Io.Threaded) catch @panic("OOM");
        io_impl.* = std.Io.Threaded.init(alloc, .{ .stack_size = cfg.connection_thread_stack_size });
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .app = app,
            .io_impl = io_impl,
            .io_owner = .owned,
        };
    }

    pub fn initShared(
        alloc: std.mem.Allocator,
        cfg: StdHttpListenerConfig,
        app: common.RequestExecutor,
        io_impl: *std.Io.Threaded,
    ) StdHttpListener {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .app = app,
            .io_impl = io_impl,
            .io_owner = .shared,
        };
    }

    pub fn setStreamingExecutor(self: *StdHttpListener, app: common.StreamingRequestExecutor) void {
        self.streaming_app = app;
    }

    pub fn deinit(self: *StdHttpListener) void {
        self.stop();
        if (self.io_owner == .owned) {
            self.io_impl.deinit();
            self.alloc.destroy(self.io_impl);
        }
        self.* = undefined;
    }

    pub fn start(self: *StdHttpListener) !void {
        if (self.server != null) return error.AlreadyListening;
        self.stopping.store(false, .release);

        const listen_addr = try std.Io.net.IpAddress.parse(self.cfg.bind_host, self.cfg.bind_port);
        const TestBindFailure = struct {
            fn handle(err: anyerror) anyerror {
                if (builtin.is_test and err == error.Unexpected) {
                    std.debug.print("skipping listener test: local bind unavailable in this environment\n", .{});
                    return error.SkipZigTest;
                }
                return err;
            }
        };
        self.server = listen_addr.listen(self.io_impl.io(), .{
            .kernel_backlog = self.cfg.kernel_backlog,
            .reuse_address = self.cfg.reuse_address,
        }) catch |err| blk: {
            if (err == error.Unexpected and self.cfg.reuse_address) {
                if (builtin.is_test) {
                    std.debug.print(
                        "std http listener bind retrying without reuse_address host={s} port={d}\n",
                        .{ self.cfg.bind_host, self.cfg.bind_port },
                    );
                } else {
                    std.log.err(
                        "std http listener bind retrying without reuse_address host={s} port={d}",
                        .{ self.cfg.bind_host, self.cfg.bind_port },
                    );
                }
                break :blk listen_addr.listen(self.io_impl.io(), .{
                    .kernel_backlog = self.cfg.kernel_backlog,
                    .reuse_address = false,
                }) catch |retry_err| return TestBindFailure.handle(retry_err);
            }
            return TestBindFailure.handle(err);
        };
        errdefer {
            self.server.?.deinit(self.io_impl.io());
            self.server = null;
        }

        self.thread = try std.Thread.spawn(.{ .stack_size = self.cfg.thread_stack_size }, serve, .{self});
    }

    pub fn stop(self: *StdHttpListener) void {
        const io = self.io_impl.io();
        const bound_addr = self.boundAddress();
        self.stopping.store(true, .release);
        if (self.thread) |thread| {
            if (bound_addr) |addr| {
                const wake_io = std.Io.Threaded.global_single_threaded.io();
                if (addr.connect(wake_io, .{ .mode = .stream })) |stream| {
                    var wake_stream = stream;
                    wake_stream.close(wake_io);
                } else |_| {}
            }
            thread.join();
            self.thread = null;
        }
        while (self.active_connection_threads.load(.acquire) != 0) {
            sleepMs(1);
        }
        if (self.server) |*server| {
            server.deinit(io);
            self.server = null;
        }
    }

    pub fn boundAddress(self: *const StdHttpListener) ?std.Io.net.IpAddress {
        const server = self.server orelse return null;
        return server.socket.address;
    }

    pub fn baseUri(self: *const StdHttpListener, alloc: std.mem.Allocator) ![]u8 {
        const addr = self.boundAddress() orelse return error.NotListening;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{addr});
    }

    fn serve(self: *StdHttpListener) void {
        const io = self.io_impl.io();
        var connection_group = std.Io.Group.init;
        defer connection_group.await(io) catch {};
        while (true) {
            if (self.stopping.load(.acquire)) return;
            const stream = if (self.server) |*server|
                server.accept(io) catch |err| switch (err) {
                    error.SocketNotListening => return,
                    error.Canceled => return,
                    else => {
                        if (self.stopping.load(.acquire)) return;
                        std.log.warn("std http listener accept failed err={}", .{err});
                        sleepMs(1);
                        continue;
                    },
                }
            else
                return;
            if (self.stopping.load(.acquire)) {
                var wake_stream = stream;
                wake_stream.close(io);
                return;
            }
            if (self.cfg.serve_in_connection_threads) {
                if (!self.tryAcquireConnectionThreadSlot()) {
                    var rejected_stream = stream;
                    rejected_stream.close(io);
                    continue;
                }
                connection_group.concurrent(io, serveStreamFiber, .{ self, stream }) catch |err| {
                    _ = self.active_connection_threads.fetchSub(1, .acq_rel);
                    std.log.warn("std http listener connection fiber handoff failed err={}", .{err});
                    self.serveStream(stream);
                    continue;
                };
                continue;
            }
            self.serveStream(stream);
        }
    }

    fn tryAcquireConnectionThreadSlot(self: *StdHttpListener) bool {
        const max_threads = self.cfg.max_connection_threads;
        while (true) {
            const active = self.active_connection_threads.load(.acquire);
            if (max_threads > 0 and active >= max_threads) return false;
            if (self.active_connection_threads.cmpxchgWeak(active, active + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    fn serveStreamFiber(self: *StdHttpListener, stream: std.Io.net.Stream) void {
        defer _ = self.active_connection_threads.fetchSub(1, .acq_rel);
        self.serveStream(stream);
    }

    fn serveStream(self: *StdHttpListener, stream: std.Io.net.Stream) void {
        const io = self.io_impl.io();
        var owned_stream = stream;
        defer owned_stream.close(io);

        const recv_buffer = self.alloc.alloc(u8, self.cfg.recv_buffer_bytes) catch return;
        defer self.alloc.free(recv_buffer);
        const send_buffer = self.alloc.alloc(u8, self.cfg.send_buffer_bytes) catch return;
        defer self.alloc.free(send_buffer);

        var stream_reader = owned_stream.reader(io, recv_buffer);
        var stream_writer = owned_stream.writer(io, send_buffer);
        var server: std.http.Server = .init(&stream_reader.interface, &stream_writer.interface);

        var request = server.receiveHead() catch return;
        self.handleRequest(&request) catch |err| {
            std.log.err("http request handler error: {}", .{err});
            _ = request.respond("internal server error", .{
                .status = .internal_server_error,
                .keep_alive = false,
            }) catch {};
        };
    }

    fn handleRequest(self: *StdHttpListener, request: *std.http.Server.Request) !void {
        const method = mapMethod(request.head.method) orelse {
            try request.respond("method not allowed", .{
                .status = .method_not_allowed,
                .keep_alive = false,
            });
            return;
        };

        const uri = try self.alloc.dupe(u8, request.head.target);
        defer self.alloc.free(uri);
        var authorization: ?[]u8 = null;
        defer if (authorization) |value| self.alloc.free(value);
        var headers = std.ArrayListUnmanaged(common.RequestHeader).empty;
        defer {
            for (headers.items) |header| {
                self.alloc.free(header.name);
                self.alloc.free(header.value);
            }
            headers.deinit(self.alloc);
        }
        var header_it = request.iterateHeaders();
        while (header_it.next()) |header| {
            const name = try self.alloc.dupe(u8, header.name);
            const value = self.alloc.dupe(u8, header.value) catch |err| {
                self.alloc.free(name);
                return err;
            };
            headers.append(self.alloc, .{ .name = name, .value = value }) catch |err| {
                self.alloc.free(name);
                self.alloc.free(value);
                return err;
            };
            if (std.ascii.eqlIgnoreCase(header.name, "authorization") and authorization == null) {
                authorization = try self.alloc.dupe(u8, header.value);
            }
        }
        const content_type = if (request.head.content_type) |value|
            try self.alloc.dupe(u8, value)
        else
            null;
        defer if (content_type) |value| self.alloc.free(value);

        const body = (try self.readRequestBody(request)) orelse return;
        defer self.alloc.free(body);

        const http_req: common.HttpRequest = .{
            .method = method,
            .uri = uri,
            .headers = headers.items,
            .authorization = authorization,
            .content_type = content_type,
            .body = body,
        };

        if (self.streaming_app) |streaming_app| {
            const body_buffer = try self.alloc.alloc(u8, self.cfg.send_buffer_bytes);
            defer self.alloc.free(body_buffer);
            var stream_writer = StreamingBodyWriter{
                .request = request,
                .buffer = body_buffer,
            };
            const handled = streaming_app.execute(self.alloc, http_req, stream_writer.iface()) catch |err| {
                if (stream_writer.started()) {
                    std.log.err("http streaming request handler error after response start: {}", .{err});
                    _ = stream_writer.end() catch {};
                    return;
                }
                return err;
            };
            if (handled) {
                try stream_writer.end();
                return;
            }
        }

        var response = try self.app.execute(self.alloc, http_req);
        defer response.deinit(self.alloc);

        const header_count = response.headers.len + @intFromBool(response.content_type != null);
        var extra_headers: []std.http.Header = if (header_count > 0)
            try self.alloc.alloc(std.http.Header, header_count)
        else
            @constCast((&[_]std.http.Header{})[0..]);
        defer if (header_count > 0) self.alloc.free(extra_headers);

        var header_index: usize = 0;
        if (response.content_type) |value| {
            extra_headers[header_index] = .{ .name = "content-type", .value = value };
            header_index += 1;
        }
        for (response.headers) |header| {
            extra_headers[header_index] = .{ .name = header.name, .value = header.value };
            header_index += 1;
        }

        try request.respond(response.body, .{
            .status = @enumFromInt(response.status),
            .keep_alive = false,
            .extra_headers = extra_headers,
        });
    }

    const StreamingBodyWriter = struct {
        request: *std.http.Server.Request,
        buffer: []u8,
        body_writer: ?std.http.BodyWriter = null,

        fn iface(self: *@This()) common.StreamWriter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .start = startResponse,
                    .write_all = writeAll,
                    .flush = flush,
                },
            };
        }

        fn startResponse(ptr: *anyopaque, alloc: std.mem.Allocator, response: common.StreamingResponse) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer != null) return error.ResponseAlreadyStarted;

            const header_count = response.headers.len + @intFromBool(response.content_type != null);
            var extra_headers: []std.http.Header = if (header_count > 0)
                try alloc.alloc(std.http.Header, header_count)
            else
                @constCast((&[_]std.http.Header{})[0..]);
            defer if (header_count > 0) alloc.free(extra_headers);

            var header_index: usize = 0;
            if (response.content_type) |value| {
                extra_headers[header_index] = .{ .name = "content-type", .value = value };
                header_index += 1;
            }
            for (response.headers) |header| {
                extra_headers[header_index] = .{ .name = header.name, .value = header.value };
                header_index += 1;
            }

            self.body_writer = try self.request.respondStreaming(self.buffer, .{
                .respond_options = .{
                    .status = @enumFromInt(response.status),
                    .keep_alive = false,
                    .extra_headers = extra_headers,
                },
            });
        }

        fn writeAll(ptr: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer) |*body_writer| {
                try body_writer.writer.writeAll(bytes);
                return;
            }
            return error.ResponseNotStarted;
        }

        fn flush(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer) |*body_writer| {
                try body_writer.writer.flush();
                try body_writer.flush();
                return;
            }
            return error.ResponseNotStarted;
        }

        fn started(self: *const @This()) bool {
            return self.body_writer != null;
        }

        fn end(self: *@This()) !void {
            if (self.body_writer) |*body_writer| {
                try body_writer.end();
                self.body_writer = null;
            }
        }
    };

    fn readRequestBody(self: *StdHttpListener, request: *std.http.Server.Request) !?[]u8 {
        const has_body = request.head.content_length != null or request.head.transfer_encoding != .none;
        if (!has_body) return try self.alloc.dupe(u8, &.{});

        if (request.head.content_length) |content_length| {
            if (content_length > self.cfg.max_request_bytes) {
                if (request.head.expect != null) request.head.expect = null;
                try request.respond("request too large", .{
                    .status = .payload_too_large,
                    .keep_alive = false,
                });
                return null;
            }
        }

        const body_reader = if (request.head.expect) |expect| blk: {
            if (!std.ascii.eqlIgnoreCase(expect, "100-continue")) {
                request.head.expect = null;
                try request.respond("expectation failed", .{
                    .status = .expectation_failed,
                    .keep_alive = false,
                });
                return null;
            }

            // Zig stdlib expects the normalized token when writing the interim response.
            request.head.expect = "100-continue";
            break :blk request.readerExpectContinue(&.{}) catch |err| switch (err) {
                error.HttpExpectationFailed => unreachable,
                else => return err,
            };
        } else request.readerExpectNone(&.{});

        return body_reader.allocRemaining(self.alloc, .limited(self.cfg.max_request_bytes)) catch |err| switch (err) {
            error.StreamTooLong => blk: {
                try request.respond("request too large", .{
                    .status = .payload_too_large,
                    .keep_alive = false,
                });
                break :blk null;
            },
            else => return err,
        };
    }

    fn mapMethod(method: std.http.Method) ?common.Method {
        return switch (method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            else => null,
        };
    }
};

test "std http listener and executor round-trip raft batch route" {
    const raft_engine = @import("raft_engine");
    const http_driver = @import("../../raft/transport/http_driver.zig");
    const http_server = @import("../../raft/transport/http_server.zig");
    const std_http_executor = @import("std_http_executor.zig");

    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) http_server.BatchHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += batch.groups.len;
        }
    };

    var handler = Handler{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        handler.iface(),
        null,
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var driver = http_driver.HttpFrameDriver.init(std.testing.allocator, .{}, executor.executor());

    const msg = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 3,
    };
    const batch = raft_engine.runtime.transport_iface.PeerBatch{
        .peer_id = 2,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{
            .{
                .group_id = 55,
                .messages = (&[_]raft_engine.core.Message{msg})[0..],
            },
        })[0..],
    };
    const frame = try raft_engine.runtime.BinaryCodec.codec().encodePeerBatch(std.testing.allocator, batch);
    defer raft_engine.runtime.BinaryCodec.codec().freeFrame(std.testing.allocator, frame);

    try driver.sendBatch(.{
        .peer_id = 2,
        .base_uri = base_uri,
        .body = frame.bytes,
        .content_type = frame.media_type,
    });
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
}

test "std http listener and executor round-trip snapshot routes" {
    const raft_engine = @import("raft_engine");
    const http_server = @import("../../raft/transport/http_server.zig");
    const http_snapshot = @import("../../raft/transport/http_snapshot.zig");
    const std_http_executor = @import("std_http_executor.zig");
    const routes = @import("../../raft/transport/routes.zig");

    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) http_server.SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |existing| alloc.free(existing);
            self.body = try alloc.dupe(u8, body);
        }

        fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(u8, self.body.?);
        }
    };

    const Noop = struct {
        fn iface(_: *@This()) http_server.BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    const Receiver = struct {
        seen: usize = 0,
        index: u64 = 0,

        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .receive_snapshot = receiveSnapshot,
                },
            };
        }

        fn receiveSnapshot(
            ptr: *anyopaque,
            req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            _ = req;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = snapshot;
            defer owned.deinit(std.testing.allocator);
            self.seen += 1;
            self.index = snapshot.metadata.index;
        }
    };

    var store = Store{};
    defer if (store.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        noop.iface(),
        store.iface(),
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var transport = http_snapshot.HttpSnapshotTransport.init(
        std.testing.allocator,
        .{ .root_dir = "/tmp" },
        executor.executor(),
        null,
    );

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(upload_path);
    const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, upload_path });
    defer std.testing.allocator.free(upload_uri);

    const fetch_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(fetch_path);
    const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, fetch_path });
    defer std.testing.allocator.free(fetch_uri);

    var voters = [_]u64{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.alloc(u8, 16 * 1024 + 37);
    defer std.testing.allocator.free(snapshot_bytes);
    for (snapshot_bytes, 0..) |*byte, i| byte.* = @intCast('a' + (i % 26));

    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{
                .index = 12,
                .term = 4,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "snap-1", .uri = upload_uri },
    });

    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 2,
        .locator = .{ .snapshot_id = "snap-1", .uri = fetch_uri },
    }, receiver.iface());
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(u64, 12), receiver.index);
}

test "std http listener accepts Expect 100-continue request bodies" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "Expect: 100-continue\r\n" ++
            "\r\n" ++
            "ping",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    const body = (try listener.readRequestBody(&request)).?;
    defer std.testing.allocator.free(body);
    try std.testing.expectEqualStrings("ping", body);
    try std.testing.expectEqualStrings("HTTP/1.1 100 Continue\r\n\r\n", output_writer.buffered());
}

test "std http listener rejects oversized request body with 413" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "\r\n",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{
            .max_request_bytes = 3,
        },
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expectEqual(null, try listener.readRequestBody(&request));
    try std.testing.expect(std.mem.indexOf(u8, output_writer.buffered(), "HTTP/1.1 413 Payload Too Large\r\n") != null);
}

test "std http listener rejects unsupported Expect header values with 417" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "Expect: wait-for-me\r\n" ++
            "\r\n",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expectEqual(null, try listener.readRequestBody(&request));
    try std.testing.expect(std.mem.indexOf(u8, output_writer.buffered(), "HTTP/1.1 417 Expectation Failed\r\n") != null);
}

test "std http listener can stream a chunked response through optional executor" {
    const App = struct {
        fn streamingExecutor(self: *@This()) common.StreamingRequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = executeStreaming,
                },
            };
        }

        fn executeStreaming(_: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest, writer: common.StreamWriter) !bool {
            if (!std.mem.eql(u8, req.uri, "/events")) return false;
            try writer.start(alloc, .{ .status = 200, .content_type = "text/event-stream" });
            try writer.writeAll("event: message\ndata: {\"ok\":true}\n\n");
            try writer.flush();
            return true;
        }
    };

    var input_reader: std.Io.Reader = .fixed(
        "GET /events HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "\r\n",
    );
    var output_buffer: [1024]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();
    var app = App{};

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .streaming_app = app.streamingExecutor(),
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try listener.handleRequest(&request);
    const output = output_writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "HTTP/1.1 200 OK\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "transfer-encoding: chunked\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "content-type: text/event-stream\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "event: message\ndata: {\"ok\":true}\n\n") != null);
}

test "std http listener caps active connection handoff threads" {
    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{ .max_connection_threads = 1 },
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expect(listener.tryAcquireConnectionThreadSlot());
    try std.testing.expect(!listener.tryAcquireConnectionThreadSlot());
    try std.testing.expectEqual(@as(u32, 1), listener.active_connection_threads.load(.acquire));

    _ = listener.active_connection_threads.fetchSub(1, .acq_rel);
    try std.testing.expect(listener.tryAcquireConnectionThreadSlot());
    _ = listener.active_connection_threads.fetchSub(1, .acq_rel);
    try std.testing.expectEqual(@as(u32, 0), listener.active_connection_threads.load(.acquire));
}

test "std http listener connection handoff serves fast request while slow request is blocked" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        entered_slow: std.atomic.Value(bool) = .init(false),
        release_slow: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, "/slow")) {
                self.entered_slow.store(true, .release);
                while (!self.release_slow.load(.acquire)) sleepMs(1);
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                    .body = try alloc.dupe(u8, "slow"),
                };
            }
            if (std.mem.eql(u8, req.uri, "/fast")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                    .body = try alloc.dupe(u8, "fast"),
                };
            }
            return .{
                .status = 404,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "missing"),
            };
        }
    };

    const RequestThread = struct {
        uri: []const u8,
        executor: common.RequestExecutor,
        status: std.atomic.Value(u16) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
            }) catch {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            };
            defer response.deinit(std.heap.page_allocator);

            self.status.store(response.status, .release);
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const slow_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/slow", .{base_uri});
    defer std.testing.allocator.free(slow_uri);
    const fast_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/fast", .{base_uri});
    defer std.testing.allocator.free(fast_uri);

    var slow_req = RequestThread{ .uri = slow_uri, .executor = request_executor };
    const slow_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&slow_req});
    defer slow_thread.join();
    defer app.release_slow.store(true, .release);

    var saw_slow = false;
    for (0..1000) |_| {
        if (app.entered_slow.load(.acquire)) {
            saw_slow = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slow);

    var fast_req = RequestThread{ .uri = fast_uri, .executor = request_executor };
    const fast_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&fast_req});
    defer fast_thread.join();

    var fast_completed = false;
    for (0..1000) |_| {
        if (fast_req.done.load(.acquire)) {
            fast_completed = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(fast_completed);
    try std.testing.expect(!fast_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), fast_req.status.load(.acquire));

    app.release_slow.store(true, .release);
    var slow_completed = false;
    for (0..1000) |_| {
        if (slow_req.done.load(.acquire)) {
            slow_completed = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(slow_completed);
    try std.testing.expect(!slow_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), slow_req.status.load(.acquire));
}
