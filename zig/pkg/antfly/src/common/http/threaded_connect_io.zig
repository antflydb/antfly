// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");
const builtin = @import("builtin");

const native_os = builtin.os.tag;
const posix = std.posix;

/// Installs Antfly's cancellation-safe POSIX connector over a Threaded I/O
/// runtime. The vtable is separately allocated so the returned I/O remains
/// stable when its owning executor moves.
pub fn createVTable(alloc: std.mem.Allocator, threaded: *std.Io.Threaded) !*std.Io.VTable {
    const vtable = try alloc.create(std.Io.VTable);
    vtable.* = threaded.io().vtable.*;
    vtable.netConnectIp = netConnectIp;
    return vtable;
}

pub fn io(threaded: *std.Io.Threaded, vtable: *const std.Io.VTable) std.Io {
    return .{
        .userdata = threaded,
        .vtable = vtable,
    };
}

const ConnectDisposition = enum {
    connected,
    pending,
};

fn classifyConnectErrno(err: posix.E) ?ConnectDisposition {
    return switch (err) {
        .SUCCESS, .ISCONN => .connected,
        .AGAIN, .INPROGRESS, .ALREADY => .pending,
        else => null,
    };
}

fn netConnectIp(
    userdata: ?*anyopaque,
    address: *const std.Io.net.IpAddress,
    options: std.Io.net.IpAddress.ConnectOptions,
) std.Io.net.IpAddress.ConnectError!std.Io.net.Socket {
    const threaded: *std.Io.Threaded = @ptrCast(@alignCast(userdata));
    const original_io = threaded.io();
    if (native_os == .windows or native_os == .wasi) {
        return original_io.vtable.netConnectIp(original_io.userdata, address, options);
    }
    return netConnectIpPosix(original_io, address, options);
}

fn netConnectIpPosix(
    original_io: std.Io,
    address: *const std.Io.net.IpAddress,
    options: std.Io.net.IpAddress.ConnectOptions,
) std.Io.net.IpAddress.ConnectError!std.Io.net.Socket {
    const family = std.Io.Threaded.posixAddressFamily(address);
    const mode, const protocol = std.Io.Threaded.posixSocketModeProtocol(
        family,
        options.mode,
        options.protocol,
    ) catch |err| return err;

    const socket_fd = try openSocket(original_io, family, mode, protocol);
    var open = true;
    errdefer if (open) {
        const socket: std.Io.net.Socket = .{ .handle = socket_fd, .address = undefined };
        original_io.vtable.netClose(original_io.userdata, (&socket)[0..1]);
    };

    try setDescriptorFlag(original_io, socket_fd, posix.F.SETFD, posix.FD_CLOEXEC);
    try setNonBlocking(original_io, socket_fd, true);

    var storage: std.Io.Threaded.PosixAddress = undefined;
    var addr_len = std.Io.Threaded.addressToPosix(address, &storage);
    const deadline = options.timeout.toTimestamp(original_io);
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        const connect_errno = posix.errno(posix.system.connect(
            socket_fd,
            &storage.any,
            addr_len,
        ));
        if (classifyConnectErrno(connect_errno)) |disposition| switch (disposition) {
            .connected => break,
            .pending => {
                try waitForConnect(original_io, socket_fd, deadline);
                break;
            },
        } else if (connect_errno == .INTR) {
            continue;
        } else {
            try connectError(connect_errno);
            unreachable;
        }
    }

    // Threaded's readers and writers expect blocking descriptors and provide
    // cancellation around their own syscalls.
    try setNonBlocking(original_io, socket_fd, false);
    try getSocketName(original_io, socket_fd, &storage, &addr_len);
    open = false;
    return .{
        .handle = socket_fd,
        .address = std.Io.Threaded.addressFromPosix(&storage),
    };
}

fn openSocket(
    original_io: std.Io,
    family: posix.sa_family_t,
    mode: u32,
    protocol: u32,
) std.Io.net.IpAddress.ConnectError!posix.socket_t {
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        const rc = posix.system.socket(family, mode, protocol);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .INVAL => return error.ProtocolUnsupportedBySystem,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
            .PROTOTYPE => return error.SocketModeUnsupported,
            else => return error.Unexpected,
        }
    }
}

fn setDescriptorFlag(
    original_io: std.Io,
    fd: posix.fd_t,
    command: @TypeOf(posix.F.SETFD),
    flag: usize,
) std.Io.net.IpAddress.ConnectError!void {
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        switch (posix.errno(posix.system.fcntl(fd, command, flag))) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn setNonBlocking(
    original_io: std.Io,
    fd: posix.fd_t,
    enabled: bool,
) std.Io.net.IpAddress.ConnectError!void {
    var flags: usize = while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        const rc = posix.system.fcntl(fd, posix.F.GETFL, @as(usize, 0));
        switch (posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    };
    const nonblocking_flag = @as(usize, 1 << @bitOffsetOf(posix.O, "NONBLOCK"));
    if (enabled) {
        flags |= nonblocking_flag;
    } else {
        flags &= ~nonblocking_flag;
    }
    return setDescriptorFlag(original_io, fd, posix.F.SETFL, flags);
}

fn waitForConnect(
    original_io: std.Io,
    fd: posix.fd_t,
    deadline: ?std.Io.Clock.Timestamp,
) std.Io.net.IpAddress.ConnectError!void {
    var poll_fds = [_]posix.pollfd{.{
        .fd = fd,
        .events = posix.POLL.OUT,
        .revents = 0,
    }};
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        const timeout_ms = try boundedPollTimeout(original_io, deadline);
        poll_fds[0].revents = 0;
        if (try posix.poll(&poll_fds, timeout_ms) == 0) continue;
        if (poll_fds[0].revents & posix.POLL.NVAL != 0) return error.Unexpected;
        return connectError(try socketPendingError(original_io, fd));
    }
}

fn socketPendingError(
    original_io: std.Io,
    fd: posix.fd_t,
) std.Io.net.IpAddress.ConnectError!posix.E {
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        var socket_error: c_int = 0;
        var value_len: posix.socklen_t = @sizeOf(c_int);
        const rc = std.c.getsockopt(
            fd,
            posix.SOL.SOCKET,
            posix.SO.ERROR,
            @ptrCast(&socket_error),
            &value_len,
        );
        switch (posix.errno(rc)) {
            .SUCCESS => {
                if (value_len != @sizeOf(c_int) or socket_error < 0) return error.Unexpected;
                return @fromBackingInt(@intCast(socket_error));
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn connectError(err: posix.E) std.Io.net.IpAddress.ConnectError!void {
    return switch (err) {
        .SUCCESS, .ISCONN => {},
        .ADDRNOTAVAIL => error.AddressUnavailable,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .TIMEDOUT => error.Timeout,
        .ACCES => error.AccessDenied,
        .NETDOWN => error.NetworkDown,
        else => error.Unexpected,
    };
}

fn boundedPollTimeout(
    original_io: std.Io,
    deadline: ?std.Io.Clock.Timestamp,
) std.Io.net.IpAddress.ConnectError!i32 {
    const cancellation_poll_ms: i64 = 25;
    const value = if (deadline) |timestamp| blk: {
        const remaining_ms = timestamp.durationFromNow(original_io).raw.toMilliseconds();
        if (remaining_ms <= 0) return error.Timeout;
        break :blk @min(remaining_ms, cancellation_poll_ms);
    } else cancellation_poll_ms;
    return @intCast(@max(value, 1));
}

fn getSocketName(
    original_io: std.Io,
    fd: posix.fd_t,
    storage: *std.Io.Threaded.PosixAddress,
    addr_len: *posix.socklen_t,
) std.Io.net.IpAddress.ConnectError!void {
    while (true) {
        try original_io.vtable.checkCancel(original_io.userdata);
        switch (posix.errno(posix.system.getsockname(fd, &storage.any, addr_len))) {
            .SUCCESS => return,
            .INTR => continue,
            .NOBUFS => return error.SystemResources,
            else => return error.Unexpected,
        }
    }
}

test "threaded connector accepts already-connected completion" {
    try std.testing.expectEqual(
        ConnectDisposition.connected,
        classifyConnectErrno(.ISCONN).?,
    );
    try std.testing.expectEqual(
        ConnectDisposition.pending,
        classifyConnectErrno(.INPROGRESS).?,
    );
    try std.testing.expect(classifyConnectErrno(.CONNREFUSED) == null);
    try connectError(.SUCCESS);
    try std.testing.expectError(error.ConnectionRefused, connectError(.CONNREFUSED));
}
