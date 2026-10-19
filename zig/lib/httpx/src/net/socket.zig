//! Cross-Platform Socket Abstraction for httpx.zig
//!
//! Pure-Zig networking via std.Io (no libc dependency):
//!
//! - TCP client and server socket operations via std.Io.net
//! - UDP datagram sockets
//! - Backend-neutral logical read and write timeouts
//! - Optional native socket tuning
//! - Io.Reader/Io.Writer adapters for TLS integration

const std = @import("std");
const posix = std.posix;
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const common = @import("../util/common.zig");

const is_windows = builtin.os.tag == .windows;

/// A stream-local writer; connection owners retain descriptor lifetime.
/// Frame hooks distinguish a rejected next frame from uncertain partial output.
pub const ControlledWriter = struct {
    socket: *Socket,
    control: WriteControl,
    dispatched: bool = false,
    header_block: bool = false,
    header_dispatched: bool = false,
    encoder_dirty: bool = false,
    headers_committed: bool = false,
    connection_wait: ?WriteControl.DeferredWait = null,

    /// Connection owners keep an atomic frame/header block locked through IO,
    /// then release that lock before weighted execution reentry can wait.
    pub fn beginConnectionWrite(self: *@This()) void {
        std.debug.assert(self.connection_wait == null);
        self.connection_wait = .{ .control = self.control };
        self.control = self.connection_wait.?.view();
    }
    pub fn finishConnectionWrite(self: *@This()) !void {
        var wait = self.connection_wait.?;
        self.control = wait.control;
        self.connection_wait = null;
        try wait.finish();
    }
    fn retire(self: *@This()) void {
        const previous = self.socket.io.swapCancelProtection(.blocked);
        defer _ = self.socket.io.swapCancelProtection(previous);
        self.socket.shutdown();
    }
    pub fn beginEncoding(self: *@This()) !void {
        self.headers_committed = false;
        try self.control.check();
        self.encoder_dirty = true;
    }
    pub fn abortEncoding(self: *@This()) void {
        if (self.encoder_dirty) self.retire();
    }
    pub fn beginFrame(self: *@This()) void {
        self.dispatched = false;
    }
    pub fn finishFrame(self: *@This()) void {
        self.dispatched = false;
    }
    pub fn beginHeaderBlock(self: *@This()) void {
        self.header_block = true;
        self.header_dispatched = false;
    }
    pub fn finishHeaderBlock(self: *@This()) void {
        self.headers_committed = true;
        self.header_block = false;
        self.header_dispatched = false;
        self.encoder_dirty = false;
    }
    pub fn writeAll(self: *@This(), bytes: []const u8) !void {
        errdefer if (self.dispatched or self.header_dispatched or self.encoder_dirty) self.retire();
        try self.control.check();
        if (bytes.len == 0) return;
        self.dispatched = true;
        if (self.header_block) self.header_dispatched = true;
        try self.socket.sendAllControlled(bytes, self.control);
    }
    pub fn sendAll(self: *@This(), bytes: []const u8) !void {
        return self.writeAll(bytes);
    }
};

/// Borrowed original request controls for one output operation. Callbacks may
/// run on the timeout monitor; they must not checkpoint or park a CPU grant.
/// The remaining timeout must derive from the original absolute deadline.
pub const WriteControl = struct {
    pub const Error = error{ Canceled, Timeout };
    /// Immutable absolute request ceiling, copied with the control value.
    /// This composes with application callbacks without a stack-owned wrapper.
    deadline_ns: ?u64 = null,
    deadline_io: ?Io = null,

    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) Error!void = null,
    remaining_ms_fn: ?*const fn (?*anyopaque) Error!?u64 = null,
    /// Paired ownership transitions run only on the calling task. Original
    /// fence callbacks above may also run on a timeout monitor. Keeping the
    /// interfaces separate prevents a monitor from releasing another task's
    /// execution grant. Formatting between waits retains runnable capacity.
    wait_begin_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    wait_end_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    pub fn withDeadline(self: WriteControl, io: Io, deadline_ns: u64) WriteControl {
        var result = self;
        var candidate = deadline_ns;
        if (self.deadline_ns) |original| {
            const authority = self.deadline_io.?;
            if (authority.userdata != io.userdata or authority.vtable != io.vtable) {
                const target_now = @as(u64, @intCast(@max(0, Io.Clock.now(.awake, authority).nanoseconds)));
                const source_now = @as(u64, @intCast(@max(0, Io.Clock.now(.awake, io).nanoseconds)));
                candidate = target_now +| (deadline_ns -| source_now);
            }
            result.deadline_ns = @min(original, candidate);
        } else {
            result.deadline_ns = deadline_ns;
            result.deadline_io = io;
        }
        return result;
    }

    fn deadlineRemainingMs(self: WriteControl) Error!?u64 {
        const deadline = self.deadline_ns orelse return null;
        const now = @as(u64, @intCast(@max(0, Io.Clock.now(.awake, self.deadline_io.?).nanoseconds)));
        if (now >= deadline) return error.Timeout;
        // Round up so sub-millisecond budgets remain usable, without renewing
        // the original nanosecond deadline at subsequent operations.
        const remaining = deadline - now;
        return (remaining - 1) / std.time.ns_per_ms + 1;
    }

    pub const DeferredWait = struct {
        control: WriteControl,
        parked: bool = false,
        fn check(raw: ?*anyopaque) Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try self.control.check();
        }
        fn remaining(raw: ?*anyopaque) Error!?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.control.timeout(null);
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.parked) return;
            try self.control.wait_begin_fn.?(self.control.ptr);
            self.parked = true;
        }
        fn end(_: ?*anyopaque) !void {}
        pub fn view(self: *@This()) WriteControl {
            if (self.control.wait_begin_fn == null or self.control.wait_end_fn == null) return self.control;
            return .{ .ptr = self, .check_fn = @This().check, .remaining_ms_fn = @This().remaining, .wait_begin_fn = @This().begin, .wait_end_fn = @This().end };
        }
        pub fn finish(self: *@This()) !void {
            if (!self.parked) return;
            self.parked = false;
            try self.control.wait_end_fn.?(self.control.ptr);
            try self.control.check();
        }
    };

    pub fn withWait(self: WriteControl, comptime function: anytype, args: anytype) anyerror!@typeInfo(@TypeOf(@call(.auto, function, args))).error_union.payload {
        try self.check();
        const begin = self.wait_begin_fn orelse {
            if (self.wait_end_fn != null) return error.InvalidOutputWait;
            const result = @call(.auto, function, args);
            try self.check();
            return result;
        };
        const end = self.wait_end_fn orelse return error.InvalidOutputWait;
        try begin(self.ptr);
        const result = blk: {
            self.check() catch |err| break :blk @as(anyerror!@typeInfo(@TypeOf(@call(.auto, function, args))).error_union.payload, err);
            break :blk @call(.auto, function, args);
        };
        // Even failed IO must return or reject the loan before the caller
        // performs cleanup. A rejected resume prevents further formatter CPU.
        try end(self.ptr);
        try self.check();
        return result;
    }

    /// Wait for an operation that transfers ownership. A successful result
    /// belongs to this scope until the original request accepts resumption.
    /// `discard` must physically retire the result without requiring a grant.
    /// `rejected` distinguishes authority failures from retryable IO failures.
    pub fn withOwnedWait(self: WriteControl, rejected: *bool, comptime function: anytype, args: anytype, comptime discard: anytype) anyerror!@typeInfo(@TypeOf(@call(.auto, function, args))).error_union.payload {
        rejected.* = false;
        self.check() catch |err| {
            rejected.* = true;
            return err;
        };
        const paired = self.wait_begin_fn != null;
        if (paired != (self.wait_end_fn != null)) {
            rejected.* = true;
            return error.InvalidOutputWait;
        }
        if (self.wait_begin_fn) |begin| begin(self.ptr) catch |err| {
            rejected.* = true;
            return err;
        };
        const result = blk: {
            self.check() catch |err| {
                rejected.* = true;
                break :blk @as(anyerror!@typeInfo(@TypeOf(@call(.auto, function, args))).error_union.payload, err);
            };
            break :blk @call(.auto, function, args);
        };
        if (self.wait_end_fn) |end| end(self.ptr) catch |err| {
            rejected.* = true;
            if (result) |owned| discard(owned, args) else |_| {}
            return err;
        };
        self.check() catch |err| {
            rejected.* = true;
            if (result) |owned| discard(owned, args) else |_| {}
            return err;
        };
        return result;
    }

    fn sleep(io: Io, ms: u64) !void {
        try io.sleep(.fromMilliseconds(@intCast(ms)), .awake);
    }

    pub fn active(self: WriteControl) bool {
        return self.deadline_ns != null or self.check_fn != null or self.remaining_ms_fn != null;
    }
    pub fn check(self: WriteControl) !void {
        if (self.check_fn) |check_fn| try check_fn(self.ptr);
        if (self.remaining_ms_fn) |remaining| if (try remaining(self.ptr)) |ms| if (ms == 0) return error.Timeout;
        _ = try self.deadlineRemainingMs();
    }
    pub fn lock(self: WriteControl, mutex: *Io.Mutex, io: Io) !void {
        if (!self.active() and self.wait_begin_fn == null and self.wait_end_fn == null) return mutex.lockUncancelable(io);
        while (!mutex.tryLock()) {
            try self.check();
            const ms = (try self.timeout(10)).?;
            try self.withWait(sleep, .{ io, ms });
        }
        errdefer mutex.unlock(io);
        try self.check();
    }
    pub fn timeout(self: WriteControl, configured: ?u64) !?u64 {
        try self.check();
        const remaining = if (self.remaining_ms_fn) |remaining| try remaining(self.ptr) else null;
        var limit = configured;
        if (remaining) |ms| {
            if (ms == 0) return error.Timeout;
            limit = if (limit) |current| @min(ms, current) else ms;
        }
        if (try self.deadlineRemainingMs()) |ms| limit = if (limit) |current| @min(ms, current) else ms;
        return limit;
    }
};

/// Synchronous borrowed setup envelope. Combines the original request controls
/// with its already captured deadline; it creates no execution authority.
/// The view must not outlive this stack owner or be retained by a connection.
pub const DeadlineControl = struct {
    io: Io,
    deadline_ms: ?i64,
    original: WriteControl = .{},

    fn check(raw: ?*anyopaque) WriteControl.Error!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.original.check();
        if (self.deadline_ms) |deadline| if (common.milliTimestamp(self.io) >= deadline) return error.Timeout;
    }
    fn remaining(raw: ?*anyopaque) WriteControl.Error!?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(raw);
        const original = try self.original.timeout(null);
        const deadline = self.deadline_ms orelse return original;
        const ms: u64 = @intCast(@max(0, deadline - common.milliTimestamp(self.io)));
        if (ms == 0) return error.Timeout;
        return if (original) |limit| @min(limit, ms) else ms;
    }
    fn begin(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.original.wait_begin_fn.?(self.original.ptr);
    }
    fn end(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.original.wait_end_fn.?(self.original.ptr);
    }
    pub fn view(self: *@This()) WriteControl {
        return .{
            .ptr = self,
            .check_fn = check,
            .remaining_ms_fn = remaining,
            .wait_begin_fn = if (self.original.wait_begin_fn != null) begin else null,
            .wait_end_fn = if (self.original.wait_end_fn != null) end else null,
        };
    }
};

const WindowsSocket = if (is_windows) struct {
    extern "ws2_32" fn setsockopt(
        socket: net.Socket.Handle,
        level: c_int,
        option_name: c_int,
        option_value: [*]const u8,
        option_len: c_int,
    ) callconv(.winapi) c_int;
    extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
} else struct {};

/// Network address type (std.Io.net.IpAddress).
pub const Address = net.IpAddress;

/// Optional policy applied to every resolved address immediately before
/// connecting. The borrowed context lets callers enforce deployment-specific
/// network policy (for example configured NAT64 prefixes) without global state.
pub const AddressFilter = struct {
    context: ?*const anyopaque = null,
    acceptsFn: *const fn (?*const anyopaque, Address) bool,

    pub fn accepts(self: AddressFilter, address: Address) bool {
        return self.acceptsFn(self.context, address);
    }
};

const HostName = net.HostName;

/// Resolves a host string to an address, trying IP literal parsing first
/// and falling back to DNS lookup.
pub fn resolveAddress(io: Io, host: []const u8, port: u16) !Address {
    return resolveAddressFiltered(io, host, port, null);
}

/// Resolves a host and returns the first address accepted by `filter`.
/// Literal and DNS results pass through the same policy so callers can pin
/// SSRF checks to the address that is actually connected.
pub fn resolveAddressFiltered(io: Io, host: []const u8, port: u16, filter: ?AddressFilter) !Address {
    return resolveAddressFilteredControlled(io, host, port, filter, .{});
}

pub fn resolveAddressFilteredControlled(io: Io, host: []const u8, port: u16, filter: ?AddressFilter, control: WriteControl) !Address {
    try control.check();
    if (Address.resolve(io, host, port)) |addr| {
        if (filter) |policy| {
            if (!policy.accepts(addr)) return error.AddressRejected;
        }
        return addr;
    } else |_| {}

    const host_name = try HostName.init(host);
    var canonical_name_buffer: [HostName.max_len]u8 = undefined;
    var lookup_buffer: [32]HostName.LookupResult = undefined;
    var lookup_queue: Io.Queue(HostName.LookupResult) = .init(&lookup_buffer);
    try control.withWait(HostName.lookup, .{ host_name, io, &lookup_queue, HostName.LookupOptions{
        .port = port,
        .canonical_name_buffer = &canonical_name_buffer,
    } });
    var saw_address = false;
    while (true) {
        const result = lookup_queue.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => return err,
        };
        switch (result) {
            .address => |address| {
                saw_address = true;
                if (filter) |policy| {
                    if (!policy.accepts(address)) continue;
                }
                return address;
            },
            .canonical_name => {},
        }
    }
    if (saw_address) return error.AddressRejected;
    return error.UnknownHostName;
}

test "resolveAddressFiltered applies policy to literal and DNS results" {
    const Policy = struct {
        fn rejectLoopback(_: ?*const anyopaque, address: Address) bool {
            return switch (address) {
                .ip4 => |ip4| ip4.bytes[0] != 127,
                .ip6 => true,
            };
        }

        fn rejectAll(_: ?*const anyopaque, _: Address) bool {
            return false;
        }
    };

    try std.testing.expectError(
        error.AddressRejected,
        resolveAddressFiltered(std.testing.io, "127.0.0.1", 80, .{ .acceptsFn = Policy.rejectLoopback }),
    );
    const accepted = try resolveAddressFiltered(std.testing.io, "8.8.8.8", 53, .{ .acceptsFn = Policy.rejectLoopback });
    try std.testing.expectEqual(@as(u8, 8), accepted.ip4.bytes[0]);
    try std.testing.expectError(
        error.AddressRejected,
        resolveAddressFiltered(std.testing.io, "localhost", 80, .{ .acceptsFn = Policy.rejectAll }),
    );
}

fn firstNonEmptyBuffer(bufs: [][]u8) ?struct { index: usize, buf: []u8 } {
    for (bufs, 0..) |buf, i| {
        if (buf.len != 0) return .{ .index = i, .buf = buf };
    }
    return null;
}

fn readVecOnce(reader: *Io.Reader, buf: []u8) Io.Reader.Error!usize {
    var iov = [_][]u8{buf};
    return reader.readVec(&iov);
}

fn readAtLeastOne(reader: *Io.Reader, buf: []u8) Io.Reader.Error!usize {
    while (true) {
        const n = try readVecOnce(reader, buf);
        if (n != 0) return n;
    }
}

/// True only for handles owned by Zig's host-network Threaded backend. This
/// identity check keeps raw socket options away from deterministic/custom Io
/// implementations whose handles are virtual rather than OS descriptors.
fn isThreadedNetworkIo(io: Io) bool {
    if (comptime builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return false;
    return io.vtable.netListenIp == Io.Threaded.global_single_threaded.io().vtable.netListenIp;
}

/// TCP socket abstraction backed by std.Io.net.
pub const Socket = struct {
    handle: net.Socket.Handle,
    io: Io,
    recv_timeout_ms: ?u64 = null,
    send_timeout_ms: ?u64 = null,
    native_timeouts: bool = false,
    request_deadline_ms: ?i64 = null,
    request_cancel_cb: ?*const fn (context: ?*anyopaque) bool = null,
    request_cancel_ctx: ?*anyopaque = null,

    const Self = @This();

    pub const AcceptResult = struct {
        socket: Socket,
        addr: Address,
    };

    /// Connects to the given address and returns a connected TCP socket.
    pub fn connect(addr: Address, io: Io) !Self {
        // A cancelled POSIX connect may complete before EINTR is observed.
        // Threaded retries connect and treats the legitimate ISCONN completion
        // as a programmer panic. Poll a nonblocking native connect instead;
        // custom/virtual backends keep their own network and cancellation ABI.
        if (comptime !is_windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
            if (isThreadedNetworkIo(io) and io.vtable.netConnectIp == Io.Threaded.global_single_threaded.io().vtable.netConnectIp) return connectPosix(addr, io);
        }
        const stream = try addr.connect(io, .{ .mode = .stream });
        return .{
            .handle = stream.socket.handle,
            .io = io,
            .native_timeouts = isThreadedNetworkIo(io),
        };
    }

    fn connectStream(addr: Address, io: Io) !net.Stream {
        return addr.connect(io, .{ .mode = .stream });
    }

    fn discardStream(stream: net.Stream, args: anytype) void {
        const io: Io = args[1];
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        stream.close(io);
    }

    pub fn connectControlled(addr: Address, io: Io, control: WriteControl, rejected: *bool) !Self {
        const stream = try control.withOwnedWait(rejected, connectStream, .{ addr, io }, discardStream);
        return .{ .handle = stream.socket.handle, .io = io, .native_timeouts = isThreadedNetworkIo(io) };
    }

    /// Resolves a host and tries each concrete address in resolver order.
    ///
    /// This intentionally does not use `HostName.connect`: Zig 0.16 can race
    /// cancellation of the losing happy-eyeballs task against completion of
    /// its nested DNS future. Resolving first also preserves IPv6-to-IPv4
    /// fallback when only one family is listening.
    pub const HostConnectResult = struct {
        socket: Socket,
        address: Address,
    };

    pub fn connectHostResolved(host: []const u8, port: u16, io: Io) !HostConnectResult {
        var rejected = false;
        return connectHostResolvedControlled(host, port, io, .{}, &rejected);
    }

    pub fn connectHostResolvedControlled(host: []const u8, port: u16, io: Io, control: WriteControl, rejected: *bool) !HostConnectResult {
        if (Address.resolve(io, host, port)) |addr| {
            return .{ .socket = try connectControlled(addr, io, control, rejected), .address = addr };
        } else |_| {}

        const host_name = try HostName.init(host);
        var canonical_name_buffer: [HostName.max_len]u8 = undefined;
        var lookup_buffer: [32]HostName.LookupResult = undefined;
        var lookup_queue: Io.Queue(HostName.LookupResult) = .init(&lookup_buffer);
        try control.withWait(HostName.lookup, .{ host_name, io, &lookup_queue, HostName.LookupOptions{
            .port = port,
            .canonical_name_buffer = &canonical_name_buffer,
        } });

        var last_connect_error: ?anyerror = null;
        while (true) {
            const result = lookup_queue.getOne(io) catch |err| switch (err) {
                error.Closed => break,
                else => return err,
            };
            switch (result) {
                .address => |address| {
                    const socket = connectControlled(address, io, control, rejected) catch |err| {
                        if (rejected.*) return err;
                        last_connect_error = err;
                        continue;
                    };
                    return .{ .socket = socket, .address = address };
                },
                .canonical_name => {},
            }
        }
        if (last_connect_error) |err| return err;
        return error.UnknownHostName;
    }

    pub fn connectHost(host: []const u8, port: u16, io: Io) !Self {
        return (try connectHostResolved(host, port, io)).socket;
    }

    /// Creates a socket from a raw handle (e.g. from accept).
    pub fn fromHandle(handle: net.Socket.Handle, io: Io) Self {
        return .{
            .handle = handle,
            .io = io,
            .native_timeouts = isThreadedNetworkIo(io),
        };
    }

    /// Closes the socket.
    pub fn close(self: *Self) void {
        const socket: net.Socket = .{ .handle = self.handle, .address = undefined };
        self.io.vtable.netClose(self.io.userdata, (&socket)[0..1]);
    }

    /// Shuts down reads and writes without releasing the handle. This is used
    /// to wake a blocking connection fiber before its owner performs close.
    pub fn shutdown(self: *Self) void {
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        self.io.vtable.netShutdown(self.io.userdata, self.handle, .both) catch {};
    }

    /// Wake an abandoned HTTP/1 request without first sending an orderly FIN.
    /// Its owner still closes the descriptor after the request task unwinds;
    /// zero linger makes that close a reset the peer can observe as cancellation.
    pub fn abortRequest(self: *Self) void {
        // Cleanup must still wake the owner when its parent task is cancelled.
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        if (isThreadedNetworkIo(self.io)) {
            const Linger = if (is_windows) std.os.windows.ws2_32.linger else posix.linger;
            const linger = Linger{ .onoff = 1, .linger = 0 };
            setSocketOption(self.handle, posix.SOL.SOCKET, posix.SO.LINGER, std.mem.asBytes(&linger)) catch {
                self.shutdown();
                return;
            };
            self.io.vtable.netShutdown(self.io.userdata, self.handle, .recv) catch {};
        } else {
            // Custom Io handles are not necessarily native descriptors.
            self.shutdown();
        }
    }

    /// Half-closes the write side while keeping the read side available for a
    /// response. The operation stays on the supplied std.Io backend so virtual
    /// transports can preserve stream ordering between queued bytes and FIN.
    pub fn shutdownWrite(self: *Self) !void {
        try self.io.vtable.netShutdown(self.io.userdata, self.handle, .send);
    }

    /// Sends data, returning the number of bytes written.
    pub fn send(self: *Self, data: []const u8) !usize {
        return self.sendControlled(data, .{});
    }

    pub fn sendControlled(self: *Self, data: []const u8, control: WriteControl) !usize {
        // Keep ownership transition failures outside native timeout retries:
        // failed weighted reentry must never dispatch another socket write.
        return control.withWait(Socket.sendControlledInner, .{ self, data, control });
    }

    fn sendControlledInner(self: *Self, data: []const u8, control: WriteControl) !usize {
        // A per-stream budget must not become a later sibling's native socket
        // timeout. H2 callers serialize writes with the connection write mutex.
        defer if (control.active() and self.native_timeouts and !is_windows)
            self.setNativeTimeout(posix.SO.SNDTIMEO, self.send_timeout_ms orelse 0) catch {};
        const operation_deadline_ms = self.socketOperationDeadline(.send);
        while (true) {
            try self.checkRequestCancellation();
            try control.check();
            const wait = try self.operationWait(operation_deadline_ms);
            // Abortive cancellation shuts down reads without sending FIN.
            // A blocked native send must also wake to observe cancellation;
            // unlike reads, writes can safely retry after a polling timeout.
            const poll_cancellation = self.native_timeouts and !is_windows and (self.request_cancel_cb != null or control.active());
            const bounded_timeout_ms = try control.timeout(wait.timeout_ms);
            const timeout_ms = if (poll_cancellation)
                @min(bounded_timeout_ms orelse 25, 25)
            else
                bounded_timeout_ms;
            const sent = self.netWriteWithTimeout(data, timeout_ms, control) catch |err| {
                // A monitor may report application cancellation without the
                // executing Io task being cancelled. Preserve that original
                // cause before recancel, which is only valid for runtime IO.
                try control.check();
                if (err == error.Canceled) {
                    self.io.recancel();
                    return error.Canceled;
                }
                try self.checkRequestCancellation();
                if (err == error.Timeout or err == error.WouldBlock) {
                    try self.checkRequestDeadline();
                    if (poll_cancellation) {
                        _ = try self.operationWait(operation_deadline_ms);
                        continue;
                    }
                    return error.Timeout;
                }
                return error.SendFailed;
            };
            try self.checkRequestDeadline();
            try control.check();
            return sent;
        }
    }

    /// Zig 0.16's Threaded.netWrite treats EAGAIN as an internal errno bug,
    /// but a blocking socket with SO_SNDTIMEO legitimately returns EAGAIN
    /// under backpressure. httpx installs that timeout to enforce request
    /// deadlines and cancellation polling, so POSIX writes must translate the
    /// kernel result here instead of crossing the std.Io adapter.
    fn sendPosixOnce(fd: net.Socket.Handle, data: []const u8) !usize {
        if (data.len == 0) return 0;
        var iov = posix.iovec_const{
            .base = data.ptr,
            .len = data.len,
        };
        var msg: posix.msghdr_const = .{
            .name = null,
            .namelen = 0,
            .iov = @ptrCast(&iov),
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };
        while (true) {
            const rc = posix.system.sendmsg(fd, &msg, posix.MSG.NOSIGNAL);
            switch (posix.errno(rc)) {
                .SUCCESS => return @intCast(rc),
                .INTR => continue,
                .AGAIN => return error.WouldBlock,
                .CONNRESET => return error.ConnectionResetByPeer,
                .PIPE, .NOTCONN => return error.SocketUnconnected,
                .NOBUFS, .NOMEM => return error.SystemResources,
                else => |err| return posix.unexpectedErrno(err),
            }
        }
    }

    /// Sends all data, blocking until complete.
    pub fn sendAll(self: *Self, data: []const u8) !void {
        var sent: usize = 0;
        while (sent < data.len) {
            sent += try self.send(data[sent..]);
        }
    }

    pub fn sendAllControlled(self: *Self, data: []const u8, control: WriteControl) !void {
        try control.check();
        var sent: usize = 0;
        while (sent < data.len) sent += try self.sendControlled(data[sent..], control);
    }

    /// Alias for sendAll — provides the `writeAll` interface expected by
    /// duck-typed writers (e.g. H2Connection).
    pub fn writeAll(self: *Self, data: []const u8) !void {
        return self.sendAll(data);
    }

    /// Receives data into the buffer, returning bytes received (0 = EOF).
    pub fn recv(self: *Self, buffer: []u8) !usize {
        return self.recvControlled(buffer, .{});
    }

    /// Borrowed controls belong to this calling task, never to the shared
    /// connection or its reader task. Only the physical receive parks CPU;
    /// TLS processing and response decoding resume after this call returns.
    /// A failed reentry rejects the result even if bytes reached the buffer.
    pub fn recvControlled(self: *Self, buffer: []u8, control: WriteControl) !usize {
        if (buffer.len == 0) return 0;
        return control.withWait(Socket.recvControlledInner, .{ self, buffer, control });
    }

    fn recvControlledInner(self: *Self, buffer: []u8, control: WriteControl) !usize {
        defer if (control.active() and self.native_timeouts and !is_windows)
            self.setNativeTimeout(posix.SO.RCVTIMEO, self.recv_timeout_ms orelse 0) catch {};
        const operation_deadline_ms = self.socketOperationDeadline(.recv);
        while (true) {
            try self.checkRequestCancellation();
            try control.check();
            const wait = try self.operationWait(operation_deadline_ms);
            // Keep a single receive and the original deadline. Polling with
            // competing read futures can consume then discard stream bytes.
            // The connection owner's watchdog interrupts semantic cancellation.
            const bounded_timeout_ms = try control.timeout(wait.timeout_ms);
            // A native read returning EAGAIN consumed no bytes. Poll that same
            // receive to observe borrowed cancellation without competing reads.
            const poll_control = self.native_timeouts and !is_windows and control.active();
            const timeout_ms = if (poll_control) @min(bounded_timeout_ms orelse 25, 25) else bounded_timeout_ms;
            const received = self.netReadWithTimeout(buffer, timeout_ms, control) catch |err| {
                try control.check();
                if (err == error.Canceled) self.io.recancel();
                try self.checkRequestCancellation();
                if (err == error.Timeout or err == error.WouldBlock) {
                    try self.checkRequestDeadline();
                    if (poll_control) {
                        _ = try self.operationWait(operation_deadline_ms);
                        continue;
                    }
                    return error.Timeout;
                }
                return error.RecvFailed;
            };
            try self.checkRequestDeadline();
            try control.check();
            return received;
        }
    }

    pub fn setRequestDeadline(self: *Self, deadline_ms: ?i64) void {
        self.request_deadline_ms = deadline_ms;
    }

    pub fn setRequestCancellation(
        self: *Self,
        callback: ?*const fn (context: ?*anyopaque) bool,
        context: ?*anyopaque,
    ) void {
        self.request_cancel_cb = callback;
        self.request_cancel_ctx = context;
    }

    const DeadlineOperation = enum { recv, send };

    const OperationWait = struct {
        timeout_ms: ?u64,
    };

    fn socketOperationDeadline(self: *Self, operation: DeadlineOperation) ?i64 {
        const socket_timeout_ms = switch (operation) {
            .recv => self.recv_timeout_ms,
            .send => self.send_timeout_ms,
        };
        const timeout_ms = socket_timeout_ms orelse return null;
        const now_ms = common.milliTimestamp(self.io);
        const deadline = @as(i128, now_ms) + @as(i128, timeout_ms);
        return @intCast(@min(deadline, std.math.maxInt(i64)));
    }

    fn operationWait(self: *Self, socket_deadline_ms: ?i64) !OperationWait {
        const now_ms = common.milliTimestamp(self.io);
        var terminal_timeout_ms: ?u64 = null;
        const deadlines = [_]?i64{ socket_deadline_ms, self.request_deadline_ms };
        for (deadlines) |maybe_deadline| {
            const deadline_ms = maybe_deadline orelse continue;
            if (now_ms >= deadline_ms) return error.Timeout;
            const remaining_ms: u64 = @intCast(deadline_ms - now_ms);
            terminal_timeout_ms = if (terminal_timeout_ms) |current|
                @min(current, remaining_ms)
            else
                remaining_ms;
        }

        // Request cancellation is owned by the client's outer watchdog. It
        // cancels the request task and shuts down its published socket, which
        // wakes a blocking operation. Racing a second polling timer against a
        // socket read is unsafe: the read can consume bytes just before the
        // timer wins, after which discarding the losing future loses stream
        // data. Keep only actual socket/request deadlines here.
        return .{ .timeout_ms = if (terminal_timeout_ms) |timeout| @max(timeout, 1) else null };
    }

    fn netRead(self: *Self, buffer: []u8) net.Stream.Reader.Error!usize {
        var bufs = [_][]u8{buffer};
        return (try (try self.io.operate(.{ .net_read = .{ .socket_handle = self.handle, .data = &bufs } })).net_read).data_len;
    }

    fn netWrite(self: *Self, data: []const u8) net.Stream.Writer.Error!usize {
        return try (try self.io.operate(.{ .net_write = .{ .socket_handle = self.handle, .data = &.{data} } })).net_write;
    }

    fn netReadTask(self: *Self, buffer: []u8, result: *net.Stream.Reader.Error!usize) void {
        result.* = self.netRead(buffer);
    }

    fn netWriteTask(self: *Self, data: []const u8, result: *net.Stream.Writer.Error!usize) void {
        result.* = self.netWrite(data);
    }

    fn timeoutTask(io: Io, timeout_ms: u64) Io.Cancelable!void {
        return Io.Timeout.sleep(.{ .duration = .{
            .raw = .fromMilliseconds(@intCast(timeout_ms)),
            .clock = .awake,
        } }, io);
    }

    fn netReadWithTimeout(self: *Self, buffer: []u8, timeout_ms: ?u64, control: WriteControl) !usize {
        if (self.native_timeouts and !is_windows) {
            if (timeout_ms) |timeout| self.setNativeTimeout(posix.SO.RCVTIMEO, timeout) catch |err| {
                // Darwin may reject timeout changes after both FINs have
                // completed. Preserve queued bytes and readable EOF rather
                // than failing a valid half-closed response. A nonblocking
                // peek distinguishes this from reset, invalid-descriptor and
                // live-socket option failures.
                if (comptime builtin.os.tag == .macos) {
                    if (err == error.InvalidSocketOption) {
                        var byte: [1]u8 = undefined;
                        const count = posix.system.recvfrom(self.handle, &byte, byte.len, @intCast(posix.MSG.PEEK | posix.MSG.DONTWAIT), null, null);
                        if (count == 0) return 0;
                        if (count > 0) {
                            // Both FINs can complete while response bytes are
                            // still queued. Drain only bytes available now;
                            // never block without the rejected timeout option.
                            const received = posix.system.recvfrom(self.handle, buffer.ptr, buffer.len, @intCast(posix.MSG.DONTWAIT), null, null);
                            if (received >= 0) return @intCast(received);
                        }
                    }
                }
                return err;
            };
            return posix.read(self.handle, buffer) catch |err| switch (err) {
                error.WouldBlock => error.Timeout,
                else => err,
            };
        }
        if (timeout_ms == null and !control.active()) return self.netRead(buffer);
        const Outcome = union(enum) {
            operation: void,
            timer: anyerror!void,
        };
        var operation_result: net.Stream.Reader.Error!usize = undefined;
        var outcomes: [2]Outcome = undefined;
        var select = Io.Select(Outcome).init(self.io, &outcomes);
        try select.concurrent(.timer, writeControlTimer, .{ self.io, timeout_ms, control });
        select.concurrent(.operation, netReadTask, .{ self, buffer, &operation_result }) catch |err| {
            select.cancelDiscard();
            return err;
        };
        const outcome = select.await() catch |err| {
            select.cancelDiscard();
            return err;
        };
        select.cancelDiscard();
        return switch (outcome) {
            // Child cancellation is not an acknowledged cancellation of this
            // caller. Only a cancellation point in the caller can establish
            // the authority required by recv's recancel operation.
            .operation => operation_result catch |err| {
                if (err == error.Canceled) {
                    try self.io.checkCancel();
                    return error.RecvFailed;
                }
                return err;
            },
            .timer => |result| blk: {
                result catch |err| {
                    if (err == error.Canceled) {
                        try self.io.checkCancel();
                        return error.RecvFailed;
                    }
                    return err;
                };
                break :blk error.Timeout;
            },
        };
    }

    fn writeControlTimer(io: Io, timeout_ms: ?u64, control: WriteControl) !void {
        if (!control.active()) return timeoutTask(io, timeout_ms.?);
        const deadline: ?i128 = if (timeout_ms) |ms| Io.Clock.awake.now(io).nanoseconds + @as(i128, ms) * std.time.ns_per_ms else null;
        while (true) {
            try control.check();
            const remaining = if (deadline) |fence| fence - Io.Clock.awake.now(io).nanoseconds else 25 * std.time.ns_per_ms;
            if (remaining <= 0) return;
            try io.sleep(.fromNanoseconds(@intCast(@min(remaining, 25 * std.time.ns_per_ms))), .awake);
        }
    }

    fn netWriteWithTimeout(self: *Self, data: []const u8, timeout_ms: ?u64, control: WriteControl) !usize {
        if (self.native_timeouts and !is_windows) {
            if (timeout_ms) |timeout| try self.setNativeTimeout(posix.SO.SNDTIMEO, timeout);
            return sendPosixOnce(self.handle, data);
        }
        if (timeout_ms == null and !control.active()) return self.netWrite(data);
        const Outcome = union(enum) {
            operation: void,
            timer: anyerror!void,
        };
        var operation_result: net.Stream.Writer.Error!usize = undefined;
        var outcomes: [2]Outcome = undefined;
        var select = Io.Select(Outcome).init(self.io, &outcomes);
        try select.concurrent(.timer, writeControlTimer, .{ self.io, timeout_ms, control });
        select.concurrent(.operation, netWriteTask, .{ self, data, &operation_result }) catch |err| {
            select.cancelDiscard();
            return err;
        };
        const outcome = select.await() catch |err| {
            select.cancelDiscard();
            return err;
        };
        select.cancelDiscard();
        return switch (outcome) {
            .operation => operation_result catch |err| {
                if (err == error.Canceled) {
                    try self.io.checkCancel();
                    return error.SendFailed;
                }
                return err;
            },
            .timer => |result| blk: {
                result catch |err| {
                    try control.check();
                    if (err == error.Canceled) {
                        try self.io.checkCancel();
                        return error.SendFailed;
                    }
                    return err;
                };
                break :blk error.Timeout;
            },
        };
    }

    fn checkRequestDeadline(self: *Self) !void {
        const deadline_ms = self.request_deadline_ms orelse return;
        if (common.milliTimestamp(self.io) >= deadline_ms) return error.Timeout;
    }

    fn checkRequestCancellation(self: *Self) !void {
        const callback = self.request_cancel_cb orelse return;
        if (callback(self.request_cancel_ctx)) return error.Canceled;
    }

    /// Enables or disables TCP_NODELAY (Nagle's algorithm).
    pub fn setNoDelay(self: *Self, enable: bool) !void {
        const value: u32 = if (enable) 1 else 0;
        try setSocketOption(self.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, std.mem.asBytes(&value));
    }

    /// Sets the receive timeout in milliseconds.
    pub fn setRecvTimeout(self: *Self, ms: u64) !void {
        if (self.native_timeouts) try self.setNativeTimeout(posix.SO.RCVTIMEO, ms);
        self.recv_timeout_ms = if (ms == 0) null else ms;
    }

    /// Sets the send timeout in milliseconds.
    pub fn setSendTimeout(self: *Self, ms: u64) !void {
        if (self.native_timeouts) try self.setNativeTimeout(posix.SO.SNDTIMEO, ms);
        self.send_timeout_ms = if (ms == 0) null else ms;
    }

    /// Select native socket timeout options only when the owner has proven the
    /// handle belongs to the host backend. Reads and writes still use std.Io;
    /// virtual handles retain the logical Select-based timeout path.
    pub fn enableNativeTimeouts(self: *Self) void {
        self.native_timeouts = true;
        self.recv_timeout_ms = null;
        self.send_timeout_ms = null;
    }

    fn setNativeTimeout(self: *Self, opt: u32, ms: u64) !void {
        if (is_windows) {
            const value_ms: u32 = @intCast(@min(ms, @as(u64, std.math.maxInt(u32))));
            try setSocketOption(self.handle, posix.SOL.SOCKET, opt, std.mem.asBytes(&value_ms));
        } else {
            const tv = posix.timeval{
                .sec = @intCast(ms / 1000),
                .usec = @intCast((ms % 1000) * 1000),
            };
            try setSocketOption(self.handle, posix.SOL.SOCKET, opt, std.mem.asBytes(&tv));
        }
    }

    fn setSocketOption(fd: net.Socket.Handle, level: i32, optname: u32, opt: []const u8) !void {
        if (is_windows) {
            if (WindowsSocket.setsockopt(fd, level, @intCast(optname), opt.ptr, @intCast(opt.len)) == 0) return;
            return switch (WindowsSocket.WSAGetLastError()) {
                10013 => error.PermissionDenied, // WSAEACCES
                10022, 10038 => error.InvalidSocketOption, // WSAEINVAL / WSAENOTSOCK
                10042 => error.InvalidProtocolOption, // WSAENOPROTOOPT
                10055 => error.SystemResources, // WSAENOBUFS
                else => error.InvalidSocketOption,
            };
        }
        switch (posix.errno(posix.system.setsockopt(fd, level, optname, opt.ptr, @intCast(opt.len)))) {
            .SUCCESS => {},
            .BADF, .NOTSOCK, .INVAL, .FAULT => return error.InvalidSocketOption,
            .DOM => return error.TimeoutTooBig,
            .ISCONN => return error.AlreadyConnected,
            .NOPROTOOPT => return error.InvalidProtocolOption,
            .NOMEM, .NOBUFS => return error.SystemResources,
            .PERM => return error.PermissionDenied,
            .NODEV => return error.NoDevice,
            .OPNOTSUPP => return error.OperationUnsupported,
            else => |err| return posix.unexpectedErrno(err),
        }
    }

    /// Enables or disables keep-alive probes.
    pub fn setKeepAlive(self: *Self, enable: bool) !void {
        const value: u32 = if (enable) 1 else 0;
        try setSocketOption(self.handle, posix.SOL.SOCKET, posix.SO.KEEPALIVE, std.mem.asBytes(&value));
    }

    /// Returns a reader interface for the socket.
    pub fn reader(self: *Self) SocketReader {
        return .{ .socket = self };
    }

    /// Returns a writer interface for the socket.
    pub fn writer(self: *Self) SocketWriter {
        return .{ .socket = self };
    }

    pub const SocketReader = struct {
        socket: *Socket,

        pub fn read(self: SocketReader, buffer: []u8) !usize {
            return self.socket.recv(buffer);
        }
    };

    pub const SocketWriter = struct {
        socket: *Socket,

        pub fn writeAll(self: SocketWriter, data: []const u8) !void {
            var sent: usize = 0;
            while (sent < data.len) {
                sent += try self.socket.send(data[sent..]);
            }
        }

        pub fn print(self: SocketWriter, comptime fmt: []const u8, args: anytype) !void {
            // 64 KB stack buffer handles all realistic HTTP header/status lines.
            // If the formatted output somehow exceeds this, bufPrint returns
            // NoSpaceLeft and we propagate it rather than silently truncating.
            var buf: [65536]u8 = undefined;
            const slice = try std.fmt.bufPrint(&buf, fmt, args);
            try self.writeAll(slice);
        }
    };
};

/// Shared Io.Reader VTable helpers for custom reader adapters.
/// These generic implementations call through the vtable's `readVec` and
/// can be reused by any adapter that only needs to provide `readVec`.
pub const IoReaderHelpers = struct {
    pub fn stream(r: *Io.Reader, w: *Io.Writer, limit: Io.Limit) Io.Reader.StreamError!usize {
        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);

        while (total < max_limit) {
            const max_to_read = @min(r.buffer.len, max_limit - total);
            var iov = [_][]u8{r.buffer[0..max_to_read]};
            const n = r.vtable.readVec(r, &iov) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;

            try w.writeAll(r.buffer[0..n]);
            total += n;
        }

        return total;
    }

    pub fn discard(r: *Io.Reader, limit: Io.Limit) error{ EndOfStream, ReadFailed }!usize {
        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);

        while (total < max_limit) {
            const max_to_read = @min(r.buffer.len, max_limit - total);
            var iov = [_][]u8{r.buffer[0..max_to_read]};
            const n = r.vtable.readVec(r, &iov) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;
            total += n;
        }

        return total;
    }

    pub fn rebase(r: *Io.Reader, capacity: usize) Io.Reader.RebaseError!void {
        try std.Io.Reader.defaultRebase(r, capacity);
    }

    /// Builds a standard Io.Reader.VTable using the shared helpers and a
    /// custom readVec implementation. Avoids repeating the same four-field
    /// literal in every Io.Reader adapter.
    pub fn makeVTable(comptime readVecFn: *const fn (*Io.Reader, [][]u8) Io.Reader.Error!usize) Io.Reader.VTable {
        return .{
            .stream = stream,
            .discard = discard,
            .readVec = readVecFn,
            .rebase = rebase,
        };
    }
};

/// Adapter that exposes a `std.Io.Reader` backed by a connected `Socket`.
///
/// This is primarily used to integrate with `std.crypto.tls.Client`.
pub const SocketIoReader = struct {
    socket: *Socket,
    reader_iface: Io.Reader,
    last_read_error: ?anyerror = null,
    /// Borrowed by this reader only. A multiplexed connection reader must use
    /// its own authority rather than retain any individual stream's grant.
    control: WriteControl = .{},

    pub fn init(socket: *Socket, buffer: []u8) SocketIoReader {
        return .{
            .socket = socket,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    pub fn initControlled(socket: *Socket, buffer: []u8, control: WriteControl) SocketIoReader {
        var result = init(socket, buffer);
        result.control = control;
        return result;
    }

    fn parent(r: *Io.Reader) *SocketIoReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        if (dest.len == 0 or dest[0].len == 0) return 0;
        // Route TLS transport reads through Socket.recv so absolute request
        // deadlines and per-request kernel timeout resets apply consistently.
        p.last_read_error = null;
        const n = p.socket.recvControlled(dest[0], p.control) catch |err| {
            p.last_read_error = err;
            return error.ReadFailed;
        };
        if (n == 0) return error.EndOfStream;
        if (n > data_size) {
            r.end += n - data_size;
            return data_size;
        }
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that exposes a `std.Io.Reader` backed by a `[]const u8` slice.
///
/// Used to feed in-memory data (e.g. compressed bytes) to APIs that require
/// an `Io.Reader`, such as `std.compress.flate.Decompress`.
pub const SliceIoReader = struct {
    data: []const u8,
    pos: usize = 0,
    reader_iface: Io.Reader,

    pub fn init(data: []const u8, buffer: []u8) SliceIoReader {
        return .{
            .data = data,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *SliceIoReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        const entry = firstNonEmptyBuffer(bufs) orelse return 0;
        const buf = entry.buf;
        const remaining = p.data[p.pos..];
        if (remaining.len == 0) return error.EndOfStream;
        const n = @min(buf.len, remaining.len);
        @memcpy(buf[0..n], remaining[0..n]);
        p.pos += n;
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that exposes a `std.Io.Writer` backed by a connected `Socket`.
///
/// This is primarily used to integrate with `std.crypto.tls.Client`.
pub const SocketIoWriter = struct {
    socket: *Socket,
    writer_iface: Io.Writer,
    control: ?WriteControl = null,
    last_write_error: ?anyerror = null,

    pub fn init(socket: *Socket, buffer: []u8) SocketIoWriter {
        return .{
            .socket = socket,
            .writer_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .end = 0,
            },
        };
    }

    fn parent(w: *Io.Writer) *SocketIoWriter {
        return @fieldParentPtr("writer_iface", w);
    }

    fn send(self: *@This(), bytes: []const u8) Io.Writer.Error!void {
        const result = if (self.control) |control| self.socket.sendAllControlled(bytes, control) else self.socket.sendAll(bytes);
        result catch |err| {
            self.last_write_error = err;
            return error.WriteFailed;
        };
    }

    fn drain(w: *Io.Writer, bufs: []const []const u8, splat: usize) Io.Writer.Error!usize {
        const p = parent(w);
        if (bufs.len == 0) {
            const buffered = w.buffered();
            if (buffered.len == 0) return 0;
            try p.send(buffered);
            return w.consumeAll();
        }
        if (p.control != null) {
            var sent = w.buffered().len;
            try p.send(w.buffered());
            for (bufs[0 .. bufs.len - 1]) |bytes| {
                try p.send(bytes);
                sent += bytes.len;
            }
            for (0..splat) |_| {
                try p.send(bufs[bufs.len - 1]);
                sent += bufs[bufs.len - 1].len;
            }
            return w.consume(sent);
        }
        const n = (p.socket.io.operate(.{ .net_write = .{
            .socket_handle = p.socket.handle,
            .header = w.buffered(),
            .data = bufs,
            .splat = splat,
        } }) catch |err| {
            if (err == error.Canceled) p.socket.io.recancel();
            return error.WriteFailed;
        }).net_write catch return error.WriteFailed;
        return w.consume(n);
    }

    fn sendFile(w: *Io.Writer, file_reader: *Io.File.Reader, limit: Io.Limit) Io.Writer.FileError!usize {
        const p = parent(w);

        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);
        while (total < max_limit) {
            const remaining = max_limit - total;
            const chunk_len = @min(w.buffer.len, remaining);
            if (chunk_len == 0) break;

            const n_read = file_reader.file.readStreaming(file_reader.io, &.{w.buffer[0..chunk_len]}) catch return error.ReadFailed;
            if (n_read == 0) break;

            try p.send(w.buffer[0..n_read]);
            total += n_read;
        }

        return total;
    }

    fn flush(w: *Io.Writer) Io.Writer.Error!void {
        const p = parent(w);
        while (w.end != 0) {
            const buffered = w.buffered();
            try p.send(buffered);
            _ = w.consumeAll();
        }
    }

    fn rebase(w: *Io.Writer, preserve: usize, capacity: usize) Io.Writer.Error!void {
        if (w.buffer.len - w.end >= capacity) return;

        const preserved_len = @min(preserve, w.end);
        const to_flush = w.end - preserved_len;
        if (to_flush > 0) {
            const p = parent(w);
            try p.send(w.buffer[0..to_flush]);
        }
        if (preserved_len > 0 and to_flush > 0) {
            @memmove(w.buffer[0..preserved_len], w.buffer[to_flush..][0..preserved_len]);
        }
        w.end = preserved_len;
        if (w.buffer.len - w.end < capacity) return error.WriteFailed;
    }

    const vtable: Io.Writer.VTable = .{
        .drain = drain,
        .sendFile = sendFile,
        .flush = flush,
        .rebase = rebase,
    };
};

/// Adapter that first serves bytes from a prefix slice, then delegates to an
/// inner `Io.Reader`. Used to drain leftover header-parse bytes before
/// switching to the network stream for the body.
pub const PrefixedReader = struct {
    prefix: []const u8,
    prefix_pos: usize = 0,
    inner: *Io.Reader,
    reader_iface: Io.Reader,

    pub fn init(prefix: []const u8, inner: *Io.Reader, buffer: []u8) PrefixedReader {
        return .{
            .prefix = prefix,
            .inner = inner,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *PrefixedReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        const entry = firstNonEmptyBuffer(dest) orelse return 0;

        const remaining = p.prefix[p.prefix_pos..];
        const n = if (remaining.len > 0) blk: {
            const prefix_n = @min(entry.buf.len, remaining.len);
            @memcpy(entry.buf[0..prefix_n], remaining[0..prefix_n]);
            p.prefix_pos += prefix_n;
            break :blk prefix_n;
        } else try readVecOnce(p.inner, entry.buf);

        if (n > data_size) {
            r.end += n - data_size;
            return data_size;
        }
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that wraps an `Io.Reader` and limits reads to exactly `limit` bytes.
///
/// Used to enforce Content-Length boundaries when streaming HTTP response bodies.
/// After `limit` bytes have been delivered, further reads return `EndOfStream`.
pub const ContentLengthReader = struct {
    inner: *Io.Reader,
    remaining: usize,
    // Io.Reader exposes only ReadFailed/EndOfStream. Preserve the actual
    // transport failure for callers that must decide whether replay is safe.
    read_error: ?anyerror = null,
    reader_iface: Io.Reader,

    pub fn init(inner: *Io.Reader, limit: usize, buffer: []u8) ContentLengthReader {
        return .{
            .inner = inner,
            .remaining = limit,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *ContentLengthReader {
        return @fieldParentPtr("reader_iface", r);
    }

    pub fn checkReadError(self: *const ContentLengthReader) !void {
        if (self.read_error) |err| return err;
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        if (p.remaining == 0) return error.EndOfStream;
        const entry = firstNonEmptyBuffer(bufs) orelse return 0;

        // Clamp the caller's buffer to our remaining byte budget.
        const orig_buf = bufs[entry.index];
        const clamped_len = @min(orig_buf.len, p.remaining);
        bufs[entry.index] = orig_buf[0..clamped_len];
        defer bufs[entry.index] = orig_buf; // restore original slice for caller

        const n = readVecOnce(p.inner, bufs[entry.index]) catch |err| {
            p.read_error = if (err == error.EndOfStream) error.UnexpectedEof else err;
            return error.ReadFailed;
        };
        p.remaining -= n;
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that wraps an `Io.Reader` and decodes HTTP chunked transfer encoding.
///
/// Reads chunk-size lines, delivers chunk data, consumes inter-chunk CRLFs,
/// and returns `EndOfStream` after the terminal `0\r\n\r\n` chunk.
///
/// Uses an internal read-ahead buffer to reduce per-byte syscalls when
/// parsing chunk-size lines and inter-chunk delimiters.
pub const ChunkedBodyReader = struct {
    inner: *Io.Reader,
    read_error: ?anyerror = null,
    chunk_remaining: usize = 0,
    state: ChunkState = .chunk_size,
    line_buf: [32]u8 = undefined,
    line_len: usize = 0,
    // Read-ahead buffer for reducing syscalls during chunk-size parsing.
    ahead_buf: [512]u8 = undefined,
    ahead_start: usize = 0,
    ahead_end: usize = 0,
    reader_iface: Io.Reader,

    const ChunkState = enum {
        chunk_size,
        chunk_data,
        chunk_crlf,
        trailer,
        done,
    };

    pub fn init(inner: *Io.Reader, buffer: []u8) ChunkedBodyReader {
        return .{
            .inner = inner,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *ChunkedBodyReader {
        return @fieldParentPtr("reader_iface", r);
    }

    /// Reads a single byte, consuming from the read-ahead buffer first,
    /// then falling back to a bulk read from the inner reader.
    fn readOneByte(p: *ChunkedBodyReader) Io.Reader.Error!u8 {
        if (p.ahead_start < p.ahead_end) {
            const b = p.ahead_buf[p.ahead_start];
            p.ahead_start += 1;
            return b;
        }
        // Refill the read-ahead buffer in bulk.
        try p.fillAhead();
        if (p.ahead_start >= p.ahead_end) return error.EndOfStream;
        const b = p.ahead_buf[p.ahead_start];
        p.ahead_start += 1;
        return b;
    }

    /// Fills the read-ahead buffer from the inner reader.
    /// Compacts remaining bytes to the front first.
    fn fillAhead(p: *ChunkedBodyReader) Io.Reader.Error!void {
        // Compact: move unconsumed bytes to front.
        const remaining = p.ahead_end - p.ahead_start;
        if (remaining > 0 and p.ahead_start > 0) {
            std.mem.copyForwards(u8, p.ahead_buf[0..remaining], p.ahead_buf[p.ahead_start..p.ahead_end]);
        }
        p.ahead_start = 0;
        p.ahead_end = remaining;

        if (p.ahead_end >= p.ahead_buf.len) return; // buffer full
        const n = readAtLeastOne(p.inner, p.ahead_buf[p.ahead_end..]) catch |err| return err;
        if (n == 0) return error.EndOfStream;
        p.ahead_end += n;
    }

    /// Returns the number of buffered bytes available in the read-ahead buffer.
    fn aheadAvailable(p: *ChunkedBodyReader) usize {
        return p.ahead_end - p.ahead_start;
    }

    pub fn checkReadError(self: *const ChunkedBodyReader) !void {
        if (self.read_error) |err| return err;
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        return readVecInner(r, bufs) catch |err| {
            if (err == error.EndOfStream and p.state == .done) return error.EndOfStream;
            p.read_error = if (err == error.EndOfStream) error.UnexpectedEof else err;
            return error.ReadFailed;
        };
    }

    fn readVecInner(r: *Io.Reader, bufs: [][]u8) !usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        const entry = firstNonEmptyBuffer(dest) orelse return 0;

        while (true) {
            switch (p.state) {
                .done => return error.EndOfStream,

                .chunk_size => {
                    // Read into read-ahead buffer and scan for newline.
                    while (true) {
                        // Scan buffered data for newline.
                        const buffered = p.ahead_buf[p.ahead_start..p.ahead_end];
                        if (std.mem.indexOfScalar(u8, buffered, '\n')) |nl_pos| {
                            // Accumulate line content (excluding \r and \n) into line_buf.
                            for (buffered[0..nl_pos]) |byte| {
                                if (byte == '\r') continue;
                                if (p.line_len >= p.line_buf.len) return error.InvalidResponse;
                                p.line_buf[p.line_len] = byte;
                                p.line_len += 1;
                            }
                            p.ahead_start += nl_pos + 1; // consume through newline
                            break;
                        }
                        // No newline yet — accumulate all buffered bytes into line_buf.
                        for (buffered) |byte| {
                            if (byte == '\r') continue;
                            if (p.line_len >= p.line_buf.len) return error.InvalidResponse;
                            p.line_buf[p.line_len] = byte;
                            p.line_len += 1;
                        }
                        p.ahead_start = p.ahead_end; // consume all

                        // Read more data in bulk.
                        try p.fillAhead();
                    }

                    // Parse hex chunk size (ignore extensions after ';').
                    const line = p.line_buf[0..p.line_len];
                    p.line_len = 0;
                    const hex_end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
                    const hex = std.mem.trim(u8, line[0..hex_end], " \t");
                    p.chunk_remaining = std.fmt.parseInt(usize, hex, 16) catch return error.InvalidResponse;

                    if (p.chunk_remaining == 0) {
                        // Terminal chunk. Consume trailer lines until empty line.
                        p.state = .trailer;
                        continue;
                    }
                    p.state = .chunk_data;
                    continue;
                },

                .chunk_data => {
                    // First drain any read-ahead bytes that belong to this chunk.
                    const ahead_avail = p.aheadAvailable();
                    if (ahead_avail > 0) {
                        const orig_buf = dest[entry.index];
                        const n = @min(@min(orig_buf.len, p.chunk_remaining), ahead_avail);
                        @memcpy(orig_buf[0..n], p.ahead_buf[p.ahead_start..][0..n]);
                        p.ahead_start += n;
                        p.chunk_remaining -= n;
                        if (p.chunk_remaining == 0) {
                            p.state = .chunk_crlf;
                        }
                        if (n > data_size) {
                            r.end += n - data_size;
                            return data_size;
                        }
                        return n;
                    }

                    // Read-ahead empty — read directly from inner reader.
                    const orig_buf = dest[entry.index];
                    const clamped_len = @min(orig_buf.len, p.chunk_remaining);
                    dest[entry.index] = orig_buf[0..clamped_len];
                    defer dest[entry.index] = orig_buf;

                    const n = readVecOnce(p.inner, dest[entry.index]) catch |err| return err;
                    p.chunk_remaining -= n;
                    if (p.chunk_remaining == 0) {
                        p.state = .chunk_crlf;
                    }
                    if (n > data_size) {
                        r.end += n - data_size;
                        return data_size;
                    }
                    return n;
                },

                .chunk_crlf => {
                    // Consume the \r\n after chunk data.
                    const b1 = p.readOneByte() catch |err| return err;
                    if (b1 == '\r') {
                        const b2 = p.readOneByte() catch |err| return err;
                        if (b2 != '\n') return error.InvalidResponse;
                    } else if (b1 != '\n') {
                        return error.InvalidResponse;
                    }
                    p.state = .chunk_size;
                    continue;
                },

                .trailer => {
                    // Read trailer lines until we see an empty line (\r\n or \n).
                    var saw_content = false;
                    while (true) {
                        const byte = p.readOneByte() catch |err| return err;
                        if (byte == '\n') {
                            if (!saw_content) {
                                p.state = .done;
                                return error.EndOfStream;
                            }
                            saw_content = false;
                            continue;
                        }
                        if (byte != '\r') saw_content = true;
                    }
                },
            }
        }
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// TCP listener for accepting incoming connections.
pub const TcpListener = struct {
    server: net.Server,
    io: Io,

    const Self = @This();

    pub const ListenOptions = struct {
        kernel_backlog: u31 = net.default_kernel_backlog,
        /// Allow rebinding after a prior listener has closed without allowing
        /// two live listeners to share the same address.
        reuse_address: bool = false,
        /// Explicitly allow multiple live listeners to bind the same address.
        /// This is intentionally separate from reuse_address and defaults off.
        reuse_port: bool = false,
    };

    /// Creates and binds a TCP listener to the address.
    pub fn init(addr: Address, io: Io) !Self {
        return initWithOptions(addr, io, .{ .reuse_address = true });
    }

    /// Creates and binds with explicit options.
    pub fn initWithOptions(addr: Address, io: Io, options: ListenOptions) !Self {
        const server = try listenWithOptions(addr, io, options);
        return .{ .server = server, .io = io };
    }

    /// Closes the listener.
    pub fn deinit(self: *Self) void {
        self.server.deinit(self.io);
    }

    /// Interrupts a concurrent accept without closing the underlying handle.
    /// Call this before `deinit` when another thread may be blocked in accept.
    pub fn shutdown(self: *Self) void {
        var socket = Socket.fromHandle(self.server.socket.handle, self.io);
        socket.shutdown();
    }

    /// Accepts an incoming connection.
    pub fn accept(self: *Self) !Socket.AcceptResult {
        const stream = try self.server.accept(self.io);
        return .{
            .socket = Socket.fromHandle(stream.socket.handle, self.io),
            .addr = stream.socket.address,
        };
    }

    /// Returns the local address the listener is bound to.
    pub fn getLocalAddress(self: *Self) Address {
        return self.server.socket.address;
    }
};

fn listenWithOptions(addr: Address, io: Io, options: TcpListener.ListenOptions) !net.Server {
    // Custom std.Io backends must retain ownership of listener creation so
    // deterministic transports can model bind, accept, shutdown, descriptor
    // pressure, and packet delivery. Zig 0.16's *threaded POSIX* backend maps
    // `reuse_address` to both SO_REUSEADDR and SO_REUSEPORT, but HTTPX promises
    // restart-only reuse unless callers explicitly opt into shared live
    // listeners. Use the native seam only for that host backend.
    if (isThreadedNetworkIo(io) and (options.reuse_address or options.reuse_port)) {
        if (comptime is_windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
            if (options.reuse_port) return error.OptionUnsupported else return try addr.listen(io, .{
                .kernel_backlog = options.kernel_backlog,
                .reuse_address = false,
            });
        return try listenPosix(addr, io, options);
    }
    if (options.reuse_port) return error.OptionUnsupported;
    return try addr.listen(io, .{
        .kernel_backlog = options.kernel_backlog,
        .reuse_address = options.reuse_address,
    });
}

const PosixAddress = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,
};

fn addressToPosix(address: Address, storage: *PosixAddress) posix.socklen_t {
    return switch (address) {
        .ip4 => |ip4| {
            storage.in = .{
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = std.mem.bytesToValue(u32, &ip4.bytes),
            };
            return @sizeOf(posix.sockaddr.in);
        },
        .ip6 => |ip6| {
            storage.in6 = .{
                .port = std.mem.nativeToBig(u16, ip6.port),
                .flowinfo = ip6.flow,
                .addr = ip6.bytes,
                .scope_id = ip6.interface.index,
            };
            return @sizeOf(posix.sockaddr.in6);
        },
    };
}

fn addressFromPosix(storage: *const PosixAddress) Address {
    return switch (storage.any.family) {
        posix.AF.INET => .{ .ip4 = .{
            .port = std.mem.bigToNative(u16, storage.in.port),
            .bytes = std.mem.toBytes(storage.in.addr),
        } },
        posix.AF.INET6 => .{ .ip6 = .{
            .port = std.mem.bigToNative(u16, storage.in6.port),
            .bytes = storage.in6.addr,
            .flow = storage.in6.flowinfo,
            .interface = .{ .index = storage.in6.scope_id },
        } },
        else => unreachable,
    };
}

fn connectResult(err: posix.E) !void {
    return switch (err) {
        .SUCCESS, .ISCONN => {},
        .ADDRNOTAVAIL => error.AddressUnavailable,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET, .CONNABORTED => error.ConnectionResetByPeer,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .TIMEDOUT => error.Timeout,
        .ACCES, .PERM => error.AccessDenied,
        .NETDOWN => error.NetworkDown,
        .AGAIN => error.WouldBlock,
        else => posix.unexpectedErrno(err),
    };
}
fn socketFlags(fd: posix.socket_t, command: c_int, argument: usize, io: Io) !usize {
    while (true) {
        try io.checkCancel();
        const rc = posix.system.fcntl(fd, command, argument);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}
fn connectPosix(addr: Address, io: Io) !Socket {
    try io.checkCancel();
    const family: posix.sa_family_t = switch (addr) {
        .ip4 => posix.AF.INET,
        .ip6 => posix.AF.INET6,
    };
    const flags = posix.SOCK.STREAM | if (Io.Threaded.socket_flags_unsupported) 0 else posix.SOCK.CLOEXEC;
    const fd: posix.socket_t = while (true) {
        try io.checkCancel();
        const rc = posix.system.socket(family, flags, @backingInt(net.Protocol.tcp));
        switch (posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    };
    var owned: net.Socket = .{ .handle = fd, .address = addr };
    errdefer owned.close(io);
    if (Io.Threaded.socket_flags_unsupported) _ = try socketFlags(fd, posix.F.SETFD, posix.FD_CLOEXEC, io);
    const original = try socketFlags(fd, posix.F.GETFL, 0, io);
    const nonblocking: u32 = @bitCast(posix.O{ .NONBLOCK = true });
    _ = try socketFlags(fd, posix.F.SETFL, original | nonblocking, io);
    var storage: PosixAddress = undefined;
    const len = addressToPosix(addr, &storage);
    var connected = false;
    while (true) {
        try io.checkCancel();
        const err = posix.errno(posix.system.connect(fd, &storage.any, len));
        switch (err) {
            .SUCCESS, .ISCONN => {
                connected = true;
                break;
            },
            .INTR => continue,
            .INPROGRESS, .ALREADY => break,
            else => {
                try connectResult(err);
                break;
            },
        }
    }
    while (!connected) {
        try io.checkCancel();
        var descriptors = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.OUT, .revents = 0 }};
        if (try posix.poll(&descriptors, 0) == 0) {
            // Yield through Io so pending connects do not occupy every worker
            // needed by request deadline/cancellation watchdogs.
            try io.sleep(.fromMilliseconds(10), .awake);
            continue;
        }
        try io.checkCancel();
        var completion: c_int = 0;
        var completion_len: posix.socklen_t = @sizeOf(c_int);
        while (true) {
            const err = posix.errno(posix.system.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, @ptrCast(&completion), &completion_len));
            switch (err) {
                .SUCCESS => break,
                .INTR => try io.checkCancel(),
                else => return error.InvalidSocketOption,
            }
        }
        try connectResult(@fromBackingInt(@intCast(completion)));
        connected = true;
    }
    try io.checkCancel();
    _ = try socketFlags(fd, posix.F.SETFL, original, io);
    return .{ .handle = fd, .io = io, .native_timeouts = true };
}

test "TCP connect completion accepts a connection completed during interruption" {
    if (comptime is_windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    try connectResult(.ISCONN);
    try std.testing.expectError(error.ConnectionRefused, connectResult(.CONNREFUSED));
}

fn listenPosix(addr: Address, io: Io, options: TcpListener.ListenOptions) !net.Server {
    const family: posix.sa_family_t = switch (addr) {
        .ip4 => posix.AF.INET,
        .ip6 => posix.AF.INET6,
    };
    const socket_flags = posix.SOCK.STREAM |
        if (Io.Threaded.socket_flags_unsupported) 0 else posix.SOCK.CLOEXEC;
    const socket_fd: posix.socket_t = socket: while (true) {
        const rc = posix.system.socket(family, socket_flags, @backingInt(net.Protocol.tcp));
        switch (posix.errno(rc)) {
            .SUCCESS => break :socket @intCast(rc),
            .INTR => continue,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .INVAL => return error.ProtocolUnsupportedBySystem,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
            .PROTOTYPE => return error.SocketModeUnsupported,
            else => |err| return posix.unexpectedErrno(err),
        }
    };
    var owned_socket = net.Socket{ .handle = socket_fd, .address = addr };
    errdefer owned_socket.close(io);

    if (Io.Threaded.socket_flags_unsupported) {
        while (true) switch (posix.errno(posix.system.fcntl(
            socket_fd,
            posix.F.SETFD,
            @as(usize, posix.FD_CLOEXEC),
        ))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return posix.unexpectedErrno(err),
        };
    }

    const enabled: c_int = 1;
    if (options.reuse_address)
        try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&enabled));
    if (options.reuse_port) {
        if (comptime !@hasDecl(posix.SO, "REUSEPORT")) return error.OptionUnsupported;
        try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, std.mem.asBytes(&enabled));
    }

    var storage: PosixAddress = undefined;
    const address_len = addressToPosix(addr, &storage);
    while (true) switch (posix.errno(posix.system.bind(socket_fd, &storage.any, address_len))) {
        .SUCCESS => break,
        .INTR => continue,
        .ADDRINUSE => return error.AddressInUse,
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    };
    while (true) switch (posix.errno(posix.system.listen(socket_fd, options.kernel_backlog))) {
        .SUCCESS => break,
        .INTR => continue,
        .ADDRINUSE => return error.AddressInUse,
        .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    };

    var bound_storage: PosixAddress = undefined;
    var bound_len: posix.socklen_t = @sizeOf(PosixAddress);
    while (true) switch (posix.errno(posix.system.getsockname(socket_fd, &bound_storage.any, &bound_len))) {
        .SUCCESS => break,
        .INTR => continue,
        .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    };
    owned_socket.address = addressFromPosix(&bound_storage);
    return .{
        .socket = owned_socket,
        .options = if (net.Server.AcceptOptions != void) .{
            .mode = .stream,
            .protocol = .tcp,
        },
    };
}

/// UDP datagram socket abstraction backed by std.Io.net.
pub const UdpSocket = struct {
    socket: net.Socket,
    io: Io,

    const Self = @This();

    /// Creates a UDP socket bound to the given address.
    pub fn bind(addr: Address, io: Io) !Self {
        const socket = try addr.bind(io, .{ .mode = .dgram });
        return .{ .socket = socket, .io = io };
    }

    /// Closes the socket.
    pub fn close(self: *Self) void {
        self.socket.close(self.io);
    }

    /// Sends a datagram to a specific address.
    /// UDP sends are all-or-nothing; returns data.len on success.
    pub fn sendTo(self: *Self, dest: Address, data: []const u8) !usize {
        self.socket.send(self.io, &dest, data) catch return error.SendFailed;
        return data.len;
    }

    /// Receives a datagram and returns the source address.
    pub fn recvFrom(self: *Self, buffer: []u8) !struct { n: usize, addr: Address } {
        const msg = self.socket.receive(self.io, buffer) catch return error.RecvFailed;
        return .{ .n = msg.data.len, .addr = msg.from };
    }
};

const BufferOnlyReader = struct {
    remaining: []const u8,
    chunk_size: usize,
    reader_iface: Io.Reader,

    fn init(data: []const u8, chunk_size: usize, buffer: []u8) BufferOnlyReader {
        return .{
            .remaining = data,
            .chunk_size = chunk_size,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *BufferOnlyReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, _: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        if (p.remaining.len == 0) return error.EndOfStream;
        const available = r.buffer.len - r.end;
        if (available == 0) return 0;

        const n = @min(@min(available, p.remaining.len), p.chunk_size);
        @memcpy(r.buffer[r.end..][0..n], p.remaining[0..n]);
        r.end += n;
        p.remaining = p.remaining[n..];
        return 0;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

test "Socket connect and close" {
    const io = std.testing.io;

    // Listen on a random port
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();

    const bound_addr = listener.getLocalAddress();
    try std.testing.expect(bound_addr.getPort() != 0);

    // Connect
    var socket = try Socket.connect(bound_addr, io);
    defer socket.close();
}

test "TcpListener accept" {
    const io = std.testing.io;

    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();

    const bound_addr = listener.getLocalAddress();

    // Connect client
    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    // Accept server side
    var result = try listener.accept();
    defer result.socket.close();
}

test "Socket.writer writeAll and print over TCP" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    // Client writes using SocketWriter.
    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    const w = client.writer();
    try w.writeAll("GET / HTTP/1.1\r\n");
    try w.print("Host: {s}\r\n", .{"localhost"});
    try w.writeAll("\r\n");

    // Server reads and verifies.
    var result = try listener.accept();
    defer result.socket.close();

    var buf: [256]u8 = undefined;
    var total: usize = 0;
    while (total < "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".len) {
        const n = try result.socket.recv(buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n", buf[0..total]);

    // Also verify Request.serialize works through SocketWriter.
    const Request = @import("../core/request.zig").Request;
    var req = try Request.init(allocator, .GET, "/test");
    defer req.deinit();
    try req.headers.set("Host", "example.com");

    var client2 = try Socket.connect(bound_addr, io);
    defer client2.close();
    const w2 = client2.writer();
    try req.serialize(w2);

    var result2 = try listener.accept();
    defer result2.socket.close();

    var buf2: [512]u8 = undefined;
    var total2: usize = 0;
    const expected_prefix = "GET /test";
    while (total2 < expected_prefix.len) {
        const n = try result2.socket.recv(buf2[total2..]);
        if (n == 0) break;
        total2 += n;
    }
    // Verify the request line was serialized correctly.
    try std.testing.expect(std.mem.startsWith(u8, buf2[0..total2], expected_prefix));
}

test "UdpSocket send/recv localhost" {
    const io = std.testing.io;

    const bind_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var recv_sock = try UdpSocket.bind(bind_addr, io);
    defer recv_sock.close();

    const recv_addr = recv_sock.socket.address;

    var send_sock = try UdpSocket.bind(bind_addr, io);
    defer send_sock.close();

    const msg = "ping";
    _ = try send_sock.sendTo(recv_addr, msg);

    var buf: [32]u8 = undefined;
    const got = try recv_sock.recvFrom(&buf);
    try std.testing.expectEqualStrings(msg, buf[0..got.n]);
}

test "SocketIoWriter flush sends buffered bytes" {
    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    var write_buf: [64]u8 = undefined;
    var writer = SocketIoWriter.init(&client, &write_buf);
    try writer.writer_iface.writeAll("hello");
    try writer.writer_iface.flush();

    var result = try listener.accept();
    defer result.socket.close();

    var recv_buf: [16]u8 = undefined;
    const got = try result.socket.recv(&recv_buf);
    try std.testing.expectEqualStrings("hello", recv_buf[0..got]);
}

test "Socket recv timeout returns error.Timeout" {
    if (is_windows) return;

    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    var accepted = try listener.accept();
    defer accepted.socket.close();

    // Host Threaded sockets use the kernel deadline rather than consuming two
    // executor tasks to race every read against a sleeping timer. Besides
    // avoiding per-operation scheduling overhead, this keeps a saturated
    // async lane from running the timer eagerly before the read is submitted.
    try std.testing.expect(client.native_timeouts);
    try std.testing.expect(accepted.socket.native_timeouts);
    try accepted.socket.setRecvTimeout(50);

    var recv_buf: [8]u8 = undefined;
    try std.testing.expectError(error.Timeout, accepted.socket.recv(&recv_buf));
}

test "Socket cancellation polling preserves the configured receive timeout" {
    if (is_windows) return;

    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const DelayedSender = struct {
        fn run(task_io: Io, target: *TcpListener) anyerror!void {
            var accepted = try target.accept();
            defer accepted.socket.close();
            try task_io.sleep(Io.Duration.fromMilliseconds(60), .awake);
            try accepted.socket.sendAll("ready");
        }
    };
    var sender = try io.concurrent(DelayedSender.run, .{ io, &listener });
    errdefer _ = sender.cancel(io) catch {};

    var client = try Socket.connect(listener.getLocalAddress(), io);
    defer client.close();
    try client.setRecvTimeout(500);
    client.setRequestCancellation(struct {
        fn requested(_: ?*anyopaque) bool {
            return false;
        }
    }.requested, null);

    var recv_buf: [8]u8 = undefined;
    const received = try client.recv(&recv_buf);
    try std.testing.expectEqualStrings("ready", recv_buf[0..received]);
    try sender.await(io);
}

test "Socket abort interrupts a backpressured native send" {
    if (is_windows) return error.SkipZigTest;
    const io = std.testing.io;
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var sender = try Socket.connect(listener.getLocalAddress(), io);
    defer sender.close();
    var accepted = try listener.accept();
    defer accepted.socket.close();
    const send_buffer: u32 = 4096;
    try Socket.setSocketOption(sender.handle, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&send_buffer));
    try sender.setSendTimeout(5);
    var payload: [64 * 1024]u8 = @splat(0xa5);
    for (0..1024) |_| {
        _ = sender.send(&payload) catch |err| {
            try std.testing.expectEqual(error.Timeout, err);
            break;
        };
    } else return error.TestUnexpectedResult;
    try sender.setSendTimeout(0);
    var cancelled = std.atomic.Value(bool).init(false);
    var done = std.atomic.Value(bool).init(false);
    sender.setRequestCancellation(struct {
        fn check(raw: ?*anyopaque) bool {
            const signal: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return signal.load(.acquire);
        }
    }.check, &cancelled);
    var writer = try io.concurrent(struct {
        fn run(socket: *Socket, data: []const u8, completed: *std.atomic.Value(bool)) anyerror!void {
            defer completed.store(true, .release);
            while (true) try socket.sendAll(data);
        }
    }.run, .{ &sender, &payload, &done });
    defer {
        sender.shutdown();
        writer.cancel(io) catch {};
    }
    try io.sleep(.fromMilliseconds(100), .awake);
    cancelled.store(true, .release);
    sender.abortRequest();
    for (0..250) |_| {
        if (done.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const prompt = done.load(.acquire);
    // Bound failure cleanup even if cancellation cannot wake a native send.
    sender.shutdown();
    try std.testing.expectError(error.Canceled, writer.await(io));
    try std.testing.expect(prompt);
}

test "Socket send timeout reports backpressure without panicking" {
    if (is_windows) return;

    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    var sender = try Socket.connect(listener.getLocalAddress(), io);
    defer sender.close();
    var accepted = try listener.accept();
    defer accepted.socket.close();

    const send_buffer: u32 = 4096;
    try Socket.setSocketOption(sender.handle, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&send_buffer));
    try sender.setSendTimeout(5);
    // Cancellation polling must retain the configured native write deadline.
    sender.setRequestCancellation(struct {
        fn check(_: ?*anyopaque) bool {
            return false;
        }
    }.check, null);

    var payload: [64 * 1024]u8 = @splat(0xa5);
    var attempts: usize = 0;
    while (attempts < 1024) : (attempts += 1) {
        _ = sender.send(&payload) catch |err| {
            try std.testing.expectEqual(error.Timeout, err);
            return;
        };
    }
    return error.TestUnexpectedResult;
}

test "SliceIoReader skips leading empty buffers" {
    const data = "hello";
    var read_buf: [32]u8 = undefined;
    var reader = SliceIoReader.init(data, &read_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try reader.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, data.len), got);
    try std.testing.expectEqualStrings(data, out[0..got]);
}

test "PrefixedReader skips leading empty buffers" {
    var inner_buf: [32]u8 = undefined;
    var inner = SliceIoReader.init("world", &inner_buf);
    var prefixed_buf: [32]u8 = undefined;
    var prefixed = PrefixedReader.init("hello", &inner.reader_iface, &prefixed_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try prefixed.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}

test "ContentLengthReader skips leading empty buffers" {
    var inner_buf: [32]u8 = undefined;
    var inner = SliceIoReader.init("abcdef", &inner_buf);
    var cl_buf: [32]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 3, &cl_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try limited.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 3), got);
    try std.testing.expectEqualStrings("abc", out[0..got]);
}

test "ContentLengthReader handles inner reader that buffers before producing bytes" {
    var inner_buf: [16]u8 = undefined;
    var inner = BufferOnlyReader.init("abcdef", 2, &inner_buf);
    var cl_buf: [16]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 4, &cl_buf);

    var out: [8]u8 = undefined;
    const got = try limited.reader_iface.readSliceShort(out[0..4]);
    try std.testing.expectEqual(@as(usize, 4), got);
    try std.testing.expectEqualStrings("abcd", out[0..got]);
}

test "ContentLengthReader reports premature EOF as ReadFailed" {
    var inner_buf: [16]u8 = undefined;
    var inner = SliceIoReader.init("abc", &inner_buf);
    var cl_buf: [16]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 4, &cl_buf);

    var out: [8]u8 = undefined;
    try std.testing.expectError(error.ReadFailed, limited.reader_iface.readSliceShort(out[0..4]));
    try std.testing.expectError(error.UnexpectedEof, limited.checkReadError());
}

test "ChunkedBodyReader skips leading empty buffers" {
    var inner_buf: [64]u8 = undefined;
    var inner = SliceIoReader.init("5\r\nhello\r\n0\r\n\r\n", &inner_buf);
    var chunked_buf: [64]u8 = undefined;
    var chunked = ChunkedBodyReader.init(&inner.reader_iface, &chunked_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try chunked.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}

test "ChunkedBodyReader handles inner reader that buffers before producing bytes" {
    var inner_buf: [64]u8 = undefined;
    var inner = BufferOnlyReader.init("5\r\nhello\r\n0\r\n\r\n", 3, &inner_buf);
    var chunked_buf: [64]u8 = undefined;
    var chunked = ChunkedBodyReader.init(&inner.reader_iface, &chunked_buf);

    var out: [8]u8 = undefined;
    const got = try chunked.reader_iface.readSliceShort(out[0..5]);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}

const TimedFallbackTest = struct {
    fn pair(io: Io) ![2]Socket {
        var handles: [2]posix.fd_t = undefined;
        const rc = posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, 0, &handles);
        if (posix.errno(rc) != .SUCCESS) return error.FixtureSocketPair;
        return .{ Socket.fromHandle(handles[0], io), Socket.fromHandle(handles[1], io) };
    }

    fn elapsed(io: Io, start: Io.Timestamp) i64 {
        return @intCast(@divTrunc(start.durationTo(Io.Timestamp.now(io, .awake)).toNanoseconds(), std.time.ns_per_ms));
    }

    fn healthy(comptime write: bool) !void {
        var lane = Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(4) });
        defer lane.deinit();
        const io = lane.io();
        var sockets = try pair(io);
        defer for (&sockets) |*socket| socket.close();
        sockets[0].native_timeouts = false;
        try sockets[0].setRecvTimeout(250);
        try sockets[0].setSendTimeout(250);
        if (!write) try sockets[1].sendAll("r");
        var byte: [1]u8 = undefined;
        const start = Io.Timestamp.now(io, .awake);
        const count = if (write) try sockets[0].send("w") else try sockets[0].recv(&byte);
        const ms = elapsed(io, start);
        std.debug.print("HEALTHY direction={s} elapsed_ms={d} timeout_ms=250 count={d}\n", .{ if (write) "write" else "read", ms, count });
        try std.testing.expectEqual(@as(usize, 1), count);
        if (write) {
            try std.testing.expectEqual(@as(usize, 1), try sockets[1].recv(&byte));
            try std.testing.expectEqual(@as(u8, 'w'), byte[0]);
        } else try std.testing.expectEqual(@as(u8, 'r'), byte[0]);
        try std.testing.expect(ms < 125);
    }

    fn fill(handle: posix.fd_t) !void {
        var data: [65536]u8 = @splat(0x61);
        var iov: posix.iovec_const = .{ .base = &data, .len = data.len };
        var message: posix.msghdr_const = .{ .name = null, .namelen = 0, .iov = @ptrCast(&iov), .iovlen = 1, .control = null, .controllen = 0, .flags = 0 };
        for (0..1024) |_| {
            const rc = posix.system.sendmsg(handle, &message, posix.MSG.DONTWAIT | posix.MSG.NOSIGNAL);
            switch (posix.errno(rc)) {
                .SUCCESS, .INTR => {},
                .AGAIN => return,
                else => return error.FixtureBackpressure,
            }
        }
        return error.FixtureBufferDidNotFill;
    }

    fn stalled(comptime write: bool) !void {
        var lane = Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(4) });
        defer lane.deinit();
        const io = lane.io();
        var sockets = try pair(io);
        defer for (&sockets) |*socket| socket.close();
        sockets[0].native_timeouts = false;
        try sockets[0].setRecvTimeout(250);
        try sockets[0].setSendTimeout(250);
        if (write) try fill(sockets[0].handle);
        var byte: [1]u8 = undefined;
        std.debug.print("STALLED_WAIT direction={s}\n", .{if (write) "write" else "read"});
        const start = Io.Timestamp.now(io, .awake);
        if (write) try std.testing.expectError(error.Timeout, sockets[0].send("x")) else try std.testing.expectError(error.Timeout, sockets[0].recv(&byte));
        const ms = elapsed(io, start);
        std.debug.print("STALLED_TIMEOUT direction={s} elapsed_ms={d}\n", .{ if (write) "write" else "read", ms });
        try std.testing.expect(ms >= 125 and ms < 750);
    }

    fn denied(comptime write: bool, limit: usize) !void {
        var lane = Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(limit) });
        defer lane.deinit();
        const io = lane.io();
        var sockets = try pair(io);
        defer for (&sockets) |*socket| socket.close();
        sockets[0].native_timeouts = false;
        try sockets[0].setRecvTimeout(250);
        try sockets[0].setSendTimeout(250);
        if (!write) try sockets[1].sendAll("r");
        var byte: [1]u8 = undefined;
        std.debug.print("RESIDUAL_RED admission direction={s} limit={d}\n", .{ if (write) "write" else "read", limit });
        const start = Io.Timestamp.now(io, .awake);
        if (write) try std.testing.expectError(error.SendFailed, sockets[0].send("w")) else try std.testing.expectError(error.RecvFailed, sockets[0].recv(&byte));
        const ms = elapsed(io, start);
        std.debug.print("ADMISSION_DENIED direction={s} limit={d} elapsed_ms={d}\n", .{ if (write) "write" else "read", limit, ms });
        try std.testing.expect(ms < 125);
        if (write) {
            try sockets[1].setRecvTimeout(25);
            try std.testing.expectError(error.Timeout, sockets[1].recv(&byte));
        } else {
            sockets[0].native_timeouts = true;
            try std.testing.expectEqual(@as(usize, 1), try sockets[0].recv(&byte));
            try std.testing.expectEqual(@as(u8, 'r'), byte[0]);
        }
    }
};

test "timed fallback healthy read never runs timer eagerly" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.healthy(false);
}
test "timed fallback healthy write never runs timer eagerly" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.healthy(true);
}
test "timed fallback stalled read remains bounded at zero async allowance" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.stalled(false);
}
test "timed fallback stalled write remains bounded at zero async allowance" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.stalled(true);
}
test "timed fallback denied read preserves bytes" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.denied(false, 0);
    try TimedFallbackTest.denied(false, 1);
}
test "timed fallback denied write sends no bytes" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try TimedFallbackTest.denied(true, 0);
    try TimedFallbackTest.denied(true, 1);
}

test "ChunkedBodyReader preserves truncation versus invalid framing at every phase" {
    const Case = struct { wire: []const u8, expected: anyerror };
    for ([_]Case{
        .{ .wire = "", .expected = error.UnexpectedEof },
        .{ .wire = "5\r", .expected = error.UnexpectedEof },
        .{ .wire = "5\r\nhel", .expected = error.UnexpectedEof },
        .{ .wire = "5\r\nhello\r", .expected = error.UnexpectedEof },
        .{ .wire = "5\r\nhello\r\n", .expected = error.UnexpectedEof },
        .{ .wire = "0\r\nTrailer: value\r\n", .expected = error.UnexpectedEof },
        .{ .wire = "z\r\n", .expected = error.InvalidResponse },
        .{ .wire = "1\r\nx!", .expected = error.InvalidResponse },
    }) |case| {
        var inner = Io.Reader.fixed(case.wire);
        var buffer: [32]u8 = undefined;
        var chunked = ChunkedBodyReader.init(&inner, &buffer);
        var output: [32]u8 = undefined;
        try std.testing.expectError(error.ReadFailed, chunked.reader_iface.readSliceShort(&output));
        try std.testing.expectError(case.expected, chunked.checkReadError());
    }
    var inner = Io.Reader.fixed("5\r\nhello\r\n0\r\nTrailer: value\r\n\r\n");
    var buffer: [32]u8 = undefined;
    var chunked = ChunkedBodyReader.init(&inner, &buffer);
    var output: [32]u8 = undefined;
    const n = try chunked.reader_iface.readSliceShort(&output);
    try std.testing.expectEqualStrings("hello", output[0..n]);
    try chunked.checkReadError();
}

const ControlledWriteTest = struct {
    io: Io,
    deadline_ms: ?i64 = null,
    canceled: std.atomic.Value(bool) = .init(false),
    fn check(raw: ?*anyopaque) WriteControl.Error!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.canceled.load(.acquire)) return error.Canceled;
        if (self.deadline_ms) |deadline| if (common.milliTimestamp(self.io) >= deadline) return error.Timeout;
    }
    fn remaining(raw: ?*anyopaque) WriteControl.Error!?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(raw);
        const deadline = self.deadline_ms orelse return null;
        return @intCast(@max(0, deadline - common.milliTimestamp(self.io)));
    }
    fn control(self: *@This()) WriteControl {
        return .{ .ptr = self, .check_fn = check, .remaining_ms_fn = remaining };
    }
    fn blocked(socket: *Socket, payload: []const u8, control_value: WriteControl) !void {
        // TCP can accept more bytes after the initial fill as ACKs arrive.
        // Keep one absolute control across every write until it actually stalls.
        for (0..1024) |_| try socket.sendAllControlled(payload, control_value);
        return error.FixtureBufferDidNotFill;
    }
    fn run(comptime fallback: bool, comptime cancel: bool) !void {
        if (is_windows) return error.SkipZigTest;
        if (fallback and builtin.os.tag != .linux) return error.SkipZigTest;
        var lane = Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(4) });
        defer lane.deinit();
        const io = lane.io();
        var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var sender = try Socket.connect(listener.getLocalAddress(), io);
        defer sender.close();
        var accepted = try listener.accept();
        defer accepted.socket.close();
        const buffer_size: u32 = 4096;
        try Socket.setSocketOption(sender.handle, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&buffer_size));
        try Socket.setSocketOption(accepted.socket.handle, posix.SOL.SOCKET, posix.SO.RCVBUF, std.mem.asBytes(&buffer_size));
        sender.enableNativeTimeouts();
        try sender.setSendTimeout(5);
        try TimedFallbackTest.fill(sender.handle);
        try sender.setSendTimeout(0);
        sender.native_timeouts = !fallback;
        sender.setRequestDeadline(common.milliTimestamp(io) + 10_000);
        const original_socket_deadline = sender.request_deadline_ms;
        var request = ControlledWriteTest{ .io = io, .deadline_ms = if (cancel) null else common.milliTimestamp(io) + 300 };
        if (!cancel) try io.sleep(.fromMilliseconds(50), .awake);
        var canceler: ?Io.Future(void) = if (cancel) try io.concurrent(struct {
            fn run(active_io: Io, flag: *std.atomic.Value(bool)) void {
                active_io.sleep(.fromMilliseconds(50), .awake) catch {};
                flag.store(true, .release);
            }
        }.run, .{ io, &request.canceled }) else null;
        defer if (canceler) |*task| task.cancel(io);
        var payload: [128 * 1024]u8 = @splat(0xa5);
        const start = common.milliTimestamp(io);
        try std.testing.expectError(if (cancel) error.Canceled else error.Timeout, blocked(&sender, &payload, request.control()));
        const elapsed = common.milliTimestamp(io) - start;
        std.debug.print("CONTROLLED_WRITE backend={s} cause={s} elapsed_ms={d}\n", .{ if (fallback) "fallback" else "native", if (cancel) "canceled" else "deadline", elapsed });
        try std.testing.expect(elapsed < 1000);
        try std.testing.expectEqual(original_socket_deadline, sender.request_deadline_ms);
        if (!cancel) try std.testing.expect(common.milliTimestamp(io) >= request.deadline_ms.?);
        try std.testing.expectEqual(@as(?u64, null), sender.send_timeout_ms);
        if (canceler) |*task| task.await(io);
    }
};

test "Socket controlled native write retains the original deadline under backpressure" {
    try ControlledWriteTest.run(false, false);
}
test "Socket controlled native write observes original cancellation under backpressure" {
    try ControlledWriteTest.run(false, true);
}
test "Socket controlled fallback write retains the original deadline with zero async allowance" {
    try ControlledWriteTest.run(true, false);
}
test "Socket controlled fallback write observes original cancellation with zero async allowance" {
    try ControlledWriteTest.run(true, true);
}

test "Socket controlled framing retires uncertainty and preserves a rejected next frame" {
    if (is_windows) return error.SkipZigTest;
    const Mode = enum { partial, complete, encoder };
    const Control = struct {
        canceled: bool = false,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.Canceled;
        }
    };
    const io = std.testing.io;
    for (std.enums.values(Mode)) |mode| {
        var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var sender = try Socket.connect(listener.getLocalAddress(), io);
        defer sender.close();
        sender.enableNativeTimeouts();
        var accepted = try listener.accept();
        defer accepted.socket.close();
        accepted.socket.enableNativeTimeouts();
        try accepted.socket.setRecvTimeout(25);
        var control = Control{};
        var writer = ControlledWriter{ .socket = &sender, .control = .{ .ptr = &control, .check_fn = Control.check } };
        if (mode == .encoder) {
            try writer.beginEncoding();
        } else {
            writer.beginFrame();
            try writer.writeAll("hdr");
            if (mode == .complete) {
                writer.finishFrame();
                writer.beginFrame();
            }
        }
        control.canceled = true;
        try std.testing.expectError(error.Canceled, writer.writeAll("later"));
        var bytes: [3]u8 = undefined;
        if (mode != .encoder) {
            var received: usize = 0;
            while (received < bytes.len) received += try accepted.socket.recv(bytes[received..]);
            try std.testing.expectEqualStrings("hdr", &bytes);
        }
        if (mode == .complete) {
            try std.testing.expectError(error.Timeout, accepted.socket.recv(&bytes));
        } else try std.testing.expectEqual(@as(usize, 0), try accepted.socket.recv(&bytes));
    }
}

test "Socket controlled uncertain output retires after actual fallback task cancellation" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var lane = Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(4) });
    defer lane.deinit();
    const io = lane.io();
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var sender = try Socket.connect(listener.getLocalAddress(), io);
    defer sender.close();
    var accepted = try listener.accept();
    defer accepted.socket.close();
    const buffer_size: u32 = 4096;
    try Socket.setSocketOption(sender.handle, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&buffer_size));
    try Socket.setSocketOption(accepted.socket.handle, posix.SOL.SOCKET, posix.SO.RCVBUF, std.mem.asBytes(&buffer_size));
    sender.enableNativeTimeouts();
    try sender.setSendTimeout(5);
    try TimedFallbackTest.fill(sender.handle);
    try sender.setSendTimeout(0);
    sender.native_timeouts = false;
    sender.setRequestDeadline(common.milliTimestamp(io) + 10_000);
    var original = ControlledWriteTest{ .io = io };
    var writer = ControlledWriter{ .socket = &sender, .control = original.control() };
    var started: Io.Event = .unset;
    var call = try io.concurrent(struct {
        fn run(sink: *ControlledWriter, ready: *Io.Event, active_io: Io) anyerror!void {
            sink.beginFrame();
            ready.set(active_io);
            var data: [128 * 1024]u8 = @splat(0x61);
            for (0..1024) |_| try sink.writeAll(&data);
            return error.FixtureBufferDidNotFill;
        }
    }.run, .{ &writer, &started, io });
    defer call.cancel(io) catch {};
    try started.wait(io);
    try io.sleep(.fromMilliseconds(50), .awake);
    const before = common.milliTimestamp(io);
    try std.testing.expectError(error.Canceled, call.cancel(io));
    try std.testing.expect(common.milliTimestamp(io) - before < 1000);
    // The original application control was never cancelled. This exercises
    // runtime task cancellation and protected retirement, not the monitor path.
    try std.testing.expect(!original.canceled.load(.acquire));
    accepted.socket.enableNativeTimeouts();
    try accepted.socket.setRecvTimeout(1000);
    var bytes: [64 * 1024]u8 = undefined;
    for (0..1024) |_| {
        if (try accepted.socket.recv(&bytes) == 0) return;
    }
    return error.FixtureRetirementNotObserved;
}

test "Socket output wait ownership restores CPU before formatting and rejects failed resumes" {
    const State = struct {
        mode: enum { success, io_failure, begin_failure, end_failure, canceled_after_park },
        parked: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        operations: usize = 0,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.mode == .canceled_after_park and self.parked) return error.Canceled;
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(!self.parked);
            self.begins += 1;
            if (self.mode == .begin_failure) return error.Denied;
            self.parked = true;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            self.ends += 1;
            if (self.mode == .end_failure) return error.Denied;
            self.parked = false;
        }
        fn operation(self: *@This()) !usize {
            try std.testing.expect(self.parked);
            self.operations += 1;
            if (self.mode == .io_failure) return error.EndOfStream;
            return 7;
        }
    };
    for ([_]@FieldType(State, "mode"){ .success, .io_failure, .begin_failure, .end_failure, .canceled_after_park }) |mode| {
        var state = State{ .mode = mode };
        const control = WriteControl{ .ptr = &state, .check_fn = State.check, .wait_begin_fn = State.begin, .wait_end_fn = State.end };
        const result = control.withWait(State.operation, .{&state});
        switch (mode) {
            .success => try std.testing.expectEqual(@as(usize, 7), try result),
            .io_failure => try std.testing.expectError(error.EndOfStream, result),
            .begin_failure, .end_failure => try std.testing.expectError(error.Denied, result),
            .canceled_after_park => try std.testing.expectError(error.Canceled, result),
        }
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_failure) 0 else 1), state.ends);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_failure or mode == .canceled_after_park) 0 else 1), state.operations);
        try std.testing.expectEqual(mode == .end_failure, state.parked);
    }
    var state = State{ .mode = .success };
    const incomplete = WriteControl{ .ptr = &state, .wait_begin_fn = State.begin };
    try std.testing.expectError(error.InvalidOutputWait, incomplete.withWait(State.operation, .{&state}));
    try std.testing.expectEqual(@as(usize, 0), state.begins);
}

test "Socket controlled receive fences physical input and rejects denied publication" {
    if (is_windows) return error.SkipZigTest;
    const State = struct {
        mode: enum { success, begin_failure, end_failure, canceled_after_read, deadline_after_read, io_timeout },
        buffer: *[1]u8,
        parked: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.buffer[0] != 'r') return;
            if (self.mode == .canceled_after_read) return error.Canceled;
            if (self.mode == .deadline_after_read) return error.Timeout;
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(!self.parked);
            self.begins += 1;
            if (self.mode == .begin_failure) return error.SchedulerClosed;
            self.parked = true;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            self.ends += 1;
            if (self.mode == .end_failure) return error.DeadlineExceeded;
            self.parked = false;
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |use_adapter| for (std.enums.values(@FieldType(State, "mode"))) |mode| {
        var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var sender = try Socket.connect(listener.getLocalAddress(), io);
        defer sender.close();
        var accepted = try listener.accept();
        defer accepted.socket.close();
        try accepted.socket.setRecvTimeout(50);
        var buffer: [1]u8 = .{0};
        var state: State = .{ .mode = mode, .buffer = &buffer };
        const control: WriteControl = .{ .ptr = &state, .check_fn = State.check, .wait_begin_fn = State.begin, .wait_end_fn = State.end };
        if (mode != .io_timeout) try sender.sendAll("r");
        var reader_buffer: [0]u8 = .{};
        var reader = SocketIoReader.initControlled(&accepted.socket, &reader_buffer, control);
        const result = if (use_adapter) blk: {
            const count = reader.reader_iface.readSliceShort(&buffer) catch |err| break :blk @as(anyerror!usize, reader.last_read_error orelse err);
            break :blk @as(anyerror!usize, count);
        } else accepted.socket.recvControlled(&buffer, control);
        switch (mode) {
            .success => {
                try std.testing.expectEqual(@as(usize, 1), try result);
                try std.testing.expect(!state.parked);
            },
            .begin_failure => {
                try std.testing.expectError(error.SchedulerClosed, result);
                try std.testing.expectEqual(@as(u8, 0), buffer[0]);
                // Denied entry performed no receive; the original byte remains.
                try std.testing.expectEqual(@as(usize, 1), try accepted.socket.recv(&buffer));
            },
            .end_failure => try std.testing.expectError(error.DeadlineExceeded, result),
            .canceled_after_read => try std.testing.expectError(error.Canceled, result),
            .deadline_after_read, .io_timeout => try std.testing.expectError(error.Timeout, result),
        }
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_failure) 0 else 1), state.ends);
        try std.testing.expectEqual(mode == .end_failure, state.parked);
        if (mode != .io_timeout) try std.testing.expectEqual(@as(u8, 'r'), buffer[0]);
        // No callback or CPU transition occurs for an empty receive.
        _ = try accepted.socket.recvControlled(buffer[0..0], control);
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(@as(usize, 0), reader.reader_iface.end);
    };
}

test "SocketIoWriter controlled drain preserves buffered vectors and splats" {
    const State = struct {
        parked: bool = false,
        waits: usize = 0,
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(!self.parked);
            self.parked = true;
            self.waits += 1;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            self.parked = false;
        }
    };
    const io = std.testing.io;
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var sender = try Socket.connect(listener.getLocalAddress(), io);
    defer sender.close();
    var accepted = try listener.accept();
    defer accepted.socket.close();
    var buffer: [32]u8 = undefined;
    var writer = SocketIoWriter.init(&sender, &buffer);
    var state: State = .{};
    writer.control = .{ .ptr = &state, .wait_begin_fn = State.begin, .wait_end_fn = State.end };
    try writer.writer_iface.writeAll("header");
    const sent = try SocketIoWriter.drain(&writer.writer_iface, &.{ "ab", "cd" }, 3);
    try std.testing.expectEqual(@as(usize, 8), sent);
    try std.testing.expectEqual(@as(usize, 0), writer.writer_iface.end);
    var received: [14]u8 = undefined;
    var count: usize = 0;
    while (count < received.len) count += try accepted.socket.recv(received[count..]);
    try std.testing.expectEqualStrings("headerabcdcdcd", &received);
    try std.testing.expect(!state.parked);
    try std.testing.expectEqual(@as(usize, 5), state.waits);
}

test "Socket owned connect wait retires rejected descriptors before returning" {
    if (is_windows) return error.SkipZigTest;
    const State = struct {
        mode: enum { success, begin_denied, end_denied, canceled_after_connect, unpaired },
        parked: bool = false,
        ended: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.mode == .canceled_after_connect and self.ended) return error.Canceled;
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.begins += 1;
            if (self.mode == .begin_denied) return error.ResumeDenied;
            self.parked = true;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            self.ends += 1;
            self.ended = true;
            if (self.mode == .end_denied) return error.ResumeDenied;
            self.parked = false;
        }
    };
    var lane = Io.Threaded.init(std.testing.allocator, .{});
    defer lane.deinit();
    const io = lane.io();
    for ([_]@FieldType(State, "mode"){ .success, .begin_denied, .end_denied, .canceled_after_connect, .unpaired }) |mode| {
        var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var state = State{ .mode = mode };
        const control = WriteControl{ .ptr = &state, .check_fn = State.check, .wait_begin_fn = State.begin, .wait_end_fn = if (mode == .unpaired) null else State.end };
        var rejected = false;
        const result = Socket.connectControlled(listener.getLocalAddress(), io, control, &rejected);
        switch (mode) {
            .success => {
                var socket = try result;
                socket.close();
            },
            .begin_denied, .end_denied => try std.testing.expectError(error.ResumeDenied, result),
            .canceled_after_connect => try std.testing.expectError(error.Canceled, result),
            .unpaired => try std.testing.expectError(error.InvalidOutputWait, result),
        }
        try std.testing.expectEqual(mode != .success, rejected);
        try std.testing.expectEqual(@as(usize, if (mode == .unpaired) 0 else 1), state.begins);
        try std.testing.expectEqual(@as(usize, if (mode == .begin_denied or mode == .unpaired) 0 else 1), state.ends);
        if (mode != .begin_denied and mode != .unpaired) {
            var accepted = try listener.accept();
            defer accepted.socket.close();
            accepted.socket.enableNativeTimeouts();
            try accepted.socket.setRecvTimeout(1000);
            var byte: [1]u8 = undefined;
            // Observe physical retirement through the actual peer, rather
            // than treating a callback count as proof of descriptor closure.
            try std.testing.expectEqual(@as(usize, 0), try accepted.socket.recv(&byte));
        }
    }
}

test "Socket owned wait discards successful resources without transition callbacks" {
    const State = struct {
        created: bool = false,
        retired: usize = 0,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.created) return error.Timeout;
        }
        fn create(self: *@This()) !*@This() {
            self.created = true;
            return self;
        }
        fn discard(self: *@This(), _: anytype) void {
            self.retired += 1;
        }
    };
    var state = State{};
    var rejected = false;
    const control = WriteControl{ .ptr = &state, .check_fn = State.check };
    try std.testing.expectError(error.Timeout, control.withOwnedWait(&rejected, State.create, .{&state}, State.discard));
    try std.testing.expect(rejected);
    try std.testing.expectEqual(@as(usize, 1), state.retired);
}

test "Socket controlled resolver opens localhost without retaining request controls" {
    if (is_windows) return error.SkipZigTest;
    var lane = Io.Threaded.init(std.testing.allocator, .{});
    defer lane.deinit();
    const io = lane.io();
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var rejected = false;
    var connection = try Socket.connectHostResolvedControlled("localhost", listener.getLocalAddress().getPort(), io, .{}, &rejected);
    defer connection.socket.close();
    try std.testing.expect(!rejected);
    var accepted = try listener.accept();
    defer accepted.socket.close();
    try connection.socket.sendAll("x");
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try accepted.socket.recv(&byte));
    try std.testing.expectEqualStrings("x", &byte);
}

test "Socket controlled resolver never retries an address after denied authority" {
    if (is_windows) return error.SkipZigTest;
    const State = struct {
        begins: usize = 0,
        ends: usize = 0,
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.begins += 1;
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.ends += 1;
            // DNS completes first. Reject the first concrete connect,
            // whether it succeeded or returned a retryable transport error.
            if (self.ends > 1) return error.ResumeDenied;
        }
    };
    var lane = Io.Threaded.init(std.testing.allocator, .{});
    defer lane.deinit();
    const io = lane.io();
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var state = State{};
    var rejected = false;
    try std.testing.expectError(error.ResumeDenied, Socket.connectHostResolvedControlled("localhost", listener.getLocalAddress().getPort(), io, .{ .ptr = &state, .wait_begin_fn = State.begin, .wait_end_fn = State.end }, &rejected));
    try std.testing.expect(rejected);
    try std.testing.expectEqual(@as(usize, 2), state.begins);
    try std.testing.expectEqual(@as(usize, 2), state.ends);
}

test "HTTP request lifetime composes intrinsic write deadlines with callback and phase limits" {
    const Callback = struct {
        fn remaining(_: ?*anyopaque) WriteControl.Error!?u64 {
            return 100;
        }
    };
    const io = std.testing.io;
    const now = @as(u64, @intCast(@max(0, Io.Clock.now(.awake, io).nanoseconds)));
    const original = (WriteControl{ .remaining_ms_fn = Callback.remaining }).withDeadline(io, now + 5 * std.time.ns_per_s);
    try std.testing.expect(original.active());
    try std.testing.expectEqual(@as(?u64, 100), try original.timeout(1000));
    try std.testing.expectEqual(@as(?u64, 10), try original.timeout(10));
    const unchanged = original.withDeadline(io, now + 10 * std.time.ns_per_s);
    try std.testing.expectEqual(original.deadline_ns, unchanged.deadline_ns);
    const expired = original.withDeadline(io, 0);
    try std.testing.expectError(error.Timeout, expired.check());
    var deferred: WriteControl.DeferredWait = .{ .control = expired };
    try std.testing.expectError(error.Timeout, deferred.view().timeout(null));
}

test "HTTP request lifetime native receive preserves queued bytes after both FINs" {
    if (builtin.os.tag != .macos) return;
    const io = std.testing.io;
    var listener = try TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var client = try Socket.connect(listener.getLocalAddress(), io);
    defer client.close();
    client.enableNativeTimeouts();
    try client.setRecvTimeout(1000);
    var peer = try listener.accept();
    defer peer.socket.close();
    try client.shutdownWrite();
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try peer.socket.recv(&byte));
    const payload = "queued response survives both FINs";
    try peer.socket.sendAll(payload);
    try peer.socket.shutdownWrite();
    try io.sleep(.fromMilliseconds(5), .awake);
    // Leave unread response bytes behind each receive, including after the
    // peer's FIN has arrived, without changing their order or dropping EOF.
    var actual: [payload.len]u8 = undefined;
    for (&actual) |*value| {
        try std.testing.expectEqual(@as(usize, 1), try client.recv(&byte));
        value.* = byte[0];
    }
    try std.testing.expectEqualStrings(payload, &actual);
    try std.testing.expectEqual(@as(usize, 0), try client.recv(&byte));
}
