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

//! Windows native primitives for the platform API and its owning I/O backend.

const std = @import("std");
const c = std.c;
pub const timespec = extern struct { sec: i64, nsec: i64 };

const BOOL = c_int;

extern "kernel32" fn Sleep(milliseconds: u32) callconv(.winapi) void;
extern "kernel32" fn QueryPerformanceCounter(count: *i64) callconv(.winapi) BOOL;
extern "kernel32" fn QueryPerformanceFrequency(frequency: *i64) callconv(.winapi) BOOL;
extern "kernel32" fn GetSystemTimePreciseAsFileTime(file_time: *u64) callconv(.winapi) void;
extern "kernel32" fn AcquireSRWLockExclusive(lock: *?*anyopaque) callconv(.winapi) void;
extern "kernel32" fn ReleaseSRWLockExclusive(lock: *?*anyopaque) callconv(.winapi) void;
extern "kernel32" fn TryAcquireSRWLockExclusive(lock: *?*anyopaque) callconv(.winapi) u8;
extern "kernel32" fn SleepConditionVariableSRW(cond: *?*anyopaque, lock: *?*anyopaque, milliseconds: u32, flags: u32) callconv(.winapi) BOOL;
extern "kernel32" fn WakeConditionVariable(cond: *?*anyopaque) callconv(.winapi) void;
extern "kernel32" fn WakeAllConditionVariable(cond: *?*anyopaque) callconv(.winapi) void;
extern "kernel32" fn LoadLibraryW(name: [*:0]const u16) callconv(.winapi) ?*anyopaque;
extern "kernel32" fn GetModuleHandleW(name: [*:0]const u16) callconv(.winapi) ?*anyopaque;
extern "kernel32" fn GetProcAddress(module: *anyopaque, name: [*:0]const u8) callconv(.winapi) ?*anyopaque;
extern "kernel32" fn FreeLibrary(module: *anyopaque) callconv(.winapi) BOOL;
extern "kernel32" fn GetLastError() callconv(.winapi) u32;
extern "kernel32" fn UnmapViewOfFile(base: *const anyopaque) callconv(.winapi) BOOL;
extern "kernel32" fn ReadFile(file: std.os.windows.HANDLE, buffer: [*]u8, len: u32, read: ?*u32, overlapped: ?*Overlapped) callconv(.winapi) BOOL;
extern "kernel32" fn SetEvent(event: windows.HANDLE) callconv(.winapi) BOOL;
extern "kernel32" fn ResetEvent(event: windows.HANDLE) callconv(.winapi) BOOL;
extern "kernel32" fn WaitForMultipleObjectsEx(count: u32, handles: [*]const windows.HANDLE, wait_all: BOOL, milliseconds: u32, alertable: BOOL) callconv(.winapi) u32;
extern "kernel32" fn ReOpenFile(file: windows.HANDLE, access: u32, share: u32, flags: u32) callconv(.winapi) windows.HANDLE;
extern "kernel32" fn CreateEventW(attributes: ?*anyopaque, manual_reset: BOOL, initial_state: BOOL, name: ?[*:0]const u16) callconv(.winapi) ?windows.HANDLE;
extern "kernel32" fn GetOverlappedResult(file: windows.HANDLE, overlapped: *Overlapped, transferred: *u32, wait: BOOL) callconv(.winapi) BOOL;
extern "kernel32" fn CancelIoEx(file: windows.HANDLE, overlapped: *Overlapped) callconv(.winapi) BOOL;
extern "ntdll" fn RtlNtStatusToDosError(status: windows.NTSTATUS) callconv(.winapi) u32;
extern "ws2_32" fn WSAGetOverlappedResult(socket: usize, overlapped: *Overlapped, transferred: *u32, wait: BOOL, flags: *u32) callconv(.winapi) BOOL;
extern "bcrypt" fn BCryptGenRandom(algorithm: ?*anyopaque, buffer: [*]u8, len: u32, flags: u32) callconv(.winapi) windows.NTSTATUS;
extern "ws2_32" fn setsockopt(socket: windows.HANDLE, level: i32, option: i32, value: [*]const u8, len: i32) callconv(.winapi) c_int;
extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
extern "ws2_32" fn WSAStartup(version: u16, data: *anyopaque) callconv(.winapi) c_int;
extern "ws2_32" fn WSASocketW(family: i32, mode: i32, protocol: i32, info: ?*anyopaque, group: u32, flags: u32) callconv(.winapi) usize;
extern "ws2_32" fn closesocket(socket: usize) callconv(.winapi) c_int;
extern "ws2_32" fn connect(socket: usize, address: [*]const u8, len: i32) callconv(.winapi) c_int;
extern "ws2_32" fn accept(socket: usize, address: [*]u8, len: *i32) callconv(.winapi) usize;
extern "ws2_32" fn WSASend(socket: usize, buffers: *const anyopaque, count: u32, sent: *u32, flags: u32, overlapped: ?*anyopaque, completion: ?*anyopaque) callconv(.winapi) c_int;
extern "ws2_32" fn WSARecv(socket: usize, buffers: *const anyopaque, count: u32, received: *u32, flags: *u32, overlapped: ?*anyopaque, completion: ?*anyopaque) callconv(.winapi) c_int;
extern "ws2_32" fn shutdown(socket: usize, how: i32) callconv(.winapi) c_int;

const Overlapped = extern struct {
    internal: usize = 0,
    internal_high: usize = 0,
    offset: u32 = 0,
    offset_high: u32 = 0,
    event: ?std.os.windows.HANDLE = null,
};

const windows = std.os.windows;
const WineLockFn = *const fn (windows.HANDLE, ?windows.HANDLE, ?*align(2) const windows.IO_APC_ROUTINE, ?*anyopaque, ?*windows.IO_STATUS_BLOCK, *const windows.LARGE_INTEGER, *const windows.LARGE_INTEGER, ?*const windows.ULONG, windows.BOOLEAN, windows.BOOLEAN) callconv(.winapi) windows.NTSTATUS;
const WineUnlockFn = *const fn (windows.HANDLE, *windows.IO_STATUS_BLOCK, *const windows.LARGE_INTEGER, *const windows.LARGE_INTEGER, ?*const windows.ULONG) callconv(.winapi) windows.NTSTATUS;

var wine_status: std.atomic.Value(u8) = .init(0);

pub fn isWine() bool {
    const cached = wine_status.load(.monotonic);
    if (cached != 0) return cached == 2;
    const module = GetModuleHandleW(std.unicode.utf8ToUtf16LeStringLiteral("ntdll.dll"));
    const wine = if (module) |handle| GetProcAddress(handle, "wine_get_version") != null else false;
    // The result is immutable for the process. Concurrent first probes may
    // repeat the lookup, then publish the same value without a lock.
    wine_status.store(if (wine) 2 else 1, .monotonic);
    return wine;
}

fn wineNtdll() ?*anyopaque {
    if (!isWine()) return null;
    return GetModuleHandleW(std.unicode.utf8ToUtf16LeStringLiteral("ntdll.dll"));
}

/// Use the system cryptographic RNG when Wine lacks Zig's CNG device path.
/// Never substitute a predictable seed when the API fails.
pub fn randomSecure(buffer: []u8) std.Io.RandomSecureError!void {
    const system_preferred_rng = 0x00000002;
    var remaining = buffer;
    while (remaining.len != 0) {
        const len = std.math.lossyCast(u32, remaining.len);
        if (BCryptGenRandom(null, remaining.ptr, len, system_preferred_rng) != .SUCCESS) return error.EntropyUnavailable;
        remaining = remaining[len..];
    }
}

/// Wine's Winsock functions accept its AFD socket handles and translate
/// options to Wine-specific IOCTLs. Native Windows must keep Zig's AFD path.
pub fn setSocketOption(socket: windows.HANDLE, level: i32, option: u32, value: []const u8) !void {
    const ws2 = windows.ws2_32;
    // Wine has no REUSE_UNICASTPORT implementation. This is only a hint for
    // sharing ephemeral outbound ports, so retain ordinary port allocation.
    if (level == ws2.SOL.SOCKET and option == ws2.SO.REUSE_UNICASTPORT) return;
    var boolean: u32 = if (value.len == 1 and value[0] != 0) 1 else 0;
    const boolean_option = level == ws2.SOL.SOCKET and (option == ws2.SO.REUSEADDR or option == ws2.SO.BROADCAST);
    const bytes = if (boolean_option and value.len == 1) std.mem.asBytes(&boolean) else value;
    if (setsockopt(socket, level, @intCast(option), bytes.ptr, @intCast(bytes.len)) != 0) {
        return socketError();
    }
}

var winsock_ready: std.atomic.Value(bool) = .init(false);
var winsock_init_lock: ?*anyopaque = null;

fn socketError() error{ SystemResources, Unexpected } {
    const code = WSAGetLastError();
    if (code == 10055) return error.SystemResources;
    return windows.unexpectedError(@fromBackingInt(@intCast(@as(u32, @intCast(code)))));
}

pub fn openSocket(family: i32, mode: i32, protocol: i32) !windows.HANDLE {
    if (!winsock_ready.load(.acquire)) {
        AcquireSRWLockExclusive(&winsock_init_lock);
        defer ReleaseSRWLockExclusive(&winsock_init_lock);
        if (!winsock_ready.load(.monotonic)) {
            // WSADATA is at most 408 bytes on Win64 (400 on Win32). We only
            // need the output storage; its fields are not used here.
            var data: [512]u8 align(8) = undefined;
            if (WSAStartup(0x0202, &data) != 0) return error.NetworkDown;
            winsock_ready.store(true, .release);
        }
    }
    const socket = WSASocketW(family, mode, protocol, null, 0, 0x01 | 0x80);
    if (socket == std.math.maxInt(usize)) return socketError();
    return @ptrFromInt(socket);
}

pub fn closeSocket(socket: windows.HANDLE) void {
    if (isWine()) {
        if (closesocket(@intFromPtr(socket)) != 0) windows.CloseHandle(socket);
    } else windows.CloseHandle(socket);
}

pub fn connectSocket(socket: windows.HANDLE, address: []const u8) std.Io.net.IpAddress.ConnectError!void {
    if (connect(@intFromPtr(socket), address.ptr, @intCast(address.len)) != 0) {
        return switch (WSAGetLastError()) {
            10004 => error.Canceled,
            10061 => error.ConnectionRefused,
            else => socketError(),
        };
    }
}

pub fn acceptSocket(socket: windows.HANDLE, address: []u8) std.Io.net.Server.AcceptError!windows.HANDLE {
    var len: i32 = @intCast(address.len);
    const accepted = accept(@intFromPtr(socket), address.ptr, &len);
    if (accepted == std.math.maxInt(usize)) {
        return if (WSAGetLastError() == 10004) error.Canceled else socketError();
    }
    return @ptrFromInt(accepted);
}

/// Each operation owns its event and OVERLAPPED until completion, including
/// after cancellation. The owning executor supplies an alertable wait so a
/// cancellation wakes the worker without closing a socket shared by other tasks.
pub fn sendBuffers(socket: windows.HANDLE, buffers: []const windows.AFD.WSABUF(.@"const"), comptime wait: anytype) std.Io.net.Stream.Writer.Error!usize {
    var operation: Overlapped = .{ .event = try createIoEvent() };
    defer windows.CloseHandle(operation.event.?);
    var sent: u32 = 0;
    if (WSASend(@intFromPtr(socket), buffers.ptr, @intCast(buffers.len), &sent, 0, &operation, null) == 0) return sent;
    const code = WSAGetLastError();
    if (code != 997) return streamSocketError(code); // WSA_IO_PENDING
    return completeSocketOperation(socket, &operation, wait);
}

pub fn receiveBuffers(socket: windows.HANDLE, buffers: []const windows.AFD.WSABUF(.@"var"), comptime wait: anytype) std.Io.net.Stream.Reader.Error!usize {
    var operation: Overlapped = .{ .event = try createIoEvent() };
    defer windows.CloseHandle(operation.event.?);
    var received: u32 = 0;
    var flags: u32 = 0;
    if (WSARecv(@intFromPtr(socket), buffers.ptr, @intCast(buffers.len), &received, &flags, &operation, null) == 0) return received;
    const code = WSAGetLastError();
    if (code != 997) return streamSocketError(code);
    return completeSocketOperation(socket, &operation, wait);
}

pub fn createIoEvent() error{ SystemResources, Unexpected }!windows.HANDLE {
    return CreateEventW(null, 1, 0, null) orelse switch (GetLastError()) {
        8, 14, 1450 => error.SystemResources,
        else => |code| windows.unexpectedError(@fromBackingInt(code)),
    };
}

pub fn signalIoEvent(event: windows.HANDLE) void {
    std.debug.assert(SetEvent(event) != 0);
}

pub fn resetIoEvent(event: windows.HANDLE) void {
    std.debug.assert(ResetEvent(event) != 0);
}

/// True means the I/O completed; false means cancellation or an APC woke us.
/// The caller rechecks its executor cancellation state after a wakeup.
pub fn waitIoEvent(event: windows.HANDLE, cancellation: ?windows.HANDLE) error{Unexpected}!bool {
    const handles = [_]windows.HANDLE{ event, cancellation orelse event };
    return switch (WaitForMultipleObjectsEx(if (cancellation != null) 2 else 1, &handles, 0, 0xffffffff, 1)) {
        0 => true,
        1, 0xc0 => false, // cancellation event or WAIT_IO_COMPLETION
        else => windows.unexpectedError(@fromBackingInt(GetLastError())),
    };
}

fn completeSocketOperation(socket: windows.HANDLE, operation: *Overlapped, comptime wait: anytype) error{ Canceled, ConnectionResetByPeer, ConnectionTimedOut, SystemResources, Unexpected }!usize {
    var transferred: u32 = 0;
    var flags: u32 = 0;
    wait(operation.event.?) catch |err| {
        // Cancel only this request. A completion racing cancellation is valid;
        // either way, drain before the event, buffers or OVERLAPPED go away.
        _ = CancelIoEx(socket, operation);
        _ = WSAGetOverlappedResult(@intFromPtr(socket), operation, &transferred, 1, &flags);
        return err;
    };
    if (WSAGetOverlappedResult(@intFromPtr(socket), operation, &transferred, 0, &flags) == 0) return streamSocketError(WSAGetLastError());
    return transferred;
}

fn streamSocketError(code: c_int) error{ Canceled, ConnectionResetByPeer, ConnectionTimedOut, SystemResources, Unexpected } {
    return switch (code) {
        995, 10004 => error.Canceled,
        10053, 10054, 10058 => error.ConnectionResetByPeer,
        10060 => error.ConnectionTimedOut,
        10055 => error.SystemResources,
        else => windows.unexpectedError(@fromBackingInt(@intCast(@as(u32, @intCast(code))))),
    };
}

pub fn shutdownSocket(socket: windows.HANDLE, how: std.Io.net.ShutdownHow) std.Io.net.ShutdownError!void {
    const direction: i32 = switch (how) {
        .recv => 0,
        .send => 1,
        .both => 2,
    };
    if (shutdown(@intFromPtr(socket), direction) != 0) return socketError();
}

/// Wine implements synchronous byte-range locks with a null status block;
/// passing one returns NOT_IMPLEMENTED. Preserve native Windows arguments and
/// keep the real lock operation, including contention and shared-lock flags.
pub fn NtLockFile(file: windows.HANDLE, event: ?windows.HANDLE, apc: ?*align(2) const windows.IO_APC_ROUTINE, context: ?*anyopaque, status: *windows.IO_STATUS_BLOCK, offset: *const windows.LARGE_INTEGER, length: *const windows.LARGE_INTEGER, key: ?*const windows.ULONG, immediately: windows.BOOLEAN, exclusive: windows.BOOLEAN) windows.NTSTATUS {
    const synchronous = event == null and apc == null and context == null and key == null;
    if (synchronous) if (wineNtdll()) |module| {
        // Resolve separately so the optimizer cannot inherit non-null pointer
        // assumptions from Zig's native declaration of the same import.
        const lock: WineLockFn = @ptrCast(GetProcAddress(module, "NtLockFile") orelse return .NOT_IMPLEMENTED);
        const result = lock(file, event, apc, context, null, offset, length, key, immediately, exclusive);
        // Wine reports contention using FILE_LOCK_CONFLICT; Zig expects the
        // native NtLockFile status LOCK_NOT_GRANTED for nonblocking locks.
        return if (result == .FILE_LOCK_CONFLICT) .LOCK_NOT_GRANTED else result;
    };
    return windows.ntdll.NtLockFile(file, event, apc, context, status, offset, length, key, immediately, exclusive);
}

/// Wine's last parameter is a pointer, whereas the native Windows API takes
/// ULONG. Pass a full-width null for the zero key used by Zig under Wine.
pub fn NtUnlockFile(file: windows.HANDLE, status: *windows.IO_STATUS_BLOCK, offset: *const windows.LARGE_INTEGER, length: *const windows.LARGE_INTEGER, key: windows.ULONG) windows.NTSTATUS {
    if (key == 0) if (wineNtdll()) |module| {
        const unlock: WineUnlockFn = @ptrCast(GetProcAddress(module, "NtUnlockFile") orelse return .NOT_IMPLEMENTED);
        return unlock(file, status, offset, length, null);
    };
    return windows.ntdll.NtUnlockFile(file, status, offset, length, key);
}

pub const clockid_t = enum(u32) {
    REALTIME = 0,
    MONOTONIC = 1,
    PROCESS_CPUTIME_ID = 2,
    THREAD_CPUTIME_ID = 3,
    _,
};

fn setTimespec(tp: *timespec, ns: u128) void {
    tp.* = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
}

pub fn clock_gettime(clk_id: clockid_t, tp: *timespec) c_int {
    switch (clk_id) {
        .REALTIME => {
            // FILETIME counts 100ns intervals since 1601-01-01.
            const unix_epoch_in_filetime: u64 = 116_444_736_000_000_000;
            var file_time: u64 = 0;
            GetSystemTimePreciseAsFileTime(&file_time);
            const since_epoch = file_time -| unix_epoch_in_filetime;
            setTimespec(tp, @as(u128, since_epoch) * 100);
        },
        else => {
            // CPU-time clocks fall back to the monotonic clock; callers use
            // them only for diagnostics.
            var count: i64 = 0;
            var frequency: i64 = 0;
            if (QueryPerformanceCounter(&count) == 0 or QueryPerformanceFrequency(&frequency) == 0 or frequency <= 0) return -1;
            setTimespec(tp, @as(u128, @intCast(count)) * std.time.ns_per_s / @as(u128, @intCast(frequency)));
        },
    }
    return 0;
}

pub fn nanosleep(rqtp: *const timespec, rmtp: ?*timespec) c_int {
    const ns: u128 = @as(u128, @intCast(@max(rqtp.sec, 0))) * std.time.ns_per_s + @as(u128, @intCast(@max(rqtp.nsec, 0)));
    const ms = (ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
    Sleep(@intCast(@min(ms, std.math.maxInt(u32) - 1)));
    if (rmtp) |remaining| remaining.* = .{ .sec = 0, .nsec = 0 };
    return 0;
}

pub const MADV = struct {
    pub const NORMAL = 0;
    pub const RANDOM = 1;
    pub const SEQUENTIAL = 2;
    pub const WILLNEED = 3;
    pub const DONTNEED = 4;
};

/// Advice is a hint; Windows has no direct equivalent, so accept and ignore it.
pub fn madvise(addr: *align(std.heap.page_size_min) anyopaque, length: usize, advice: u32) c_int {
    _ = addr;
    _ = length;
    _ = advice;
    return 0;
}

pub const pthread_mutex_t = extern struct {
    lock: ?*anyopaque = null,
};

pub fn pthread_mutex_lock(mutex: *pthread_mutex_t) c.E {
    AcquireSRWLockExclusive(&mutex.lock);
    return .SUCCESS;
}

pub fn pthread_mutex_unlock(mutex: *pthread_mutex_t) c.E {
    ReleaseSRWLockExclusive(&mutex.lock);
    return .SUCCESS;
}

pub fn pthread_mutex_trylock(mutex: *pthread_mutex_t) c.E {
    return if (TryAcquireSRWLockExclusive(&mutex.lock) != 0) .SUCCESS else .BUSY;
}

pub fn pthread_mutex_destroy(mutex: *pthread_mutex_t) c.E {
    _ = mutex;
    return .SUCCESS;
}

/// mingw-w64 `struct dirent` (include/dirent.h); opendir/readdir/closedir
/// come from mingwex's misc/dirent.c.
pub const dirent = extern struct {
    ino: c_long,
    reclen: c_ushort,
    namlen: c_ushort,
    d_name: [260]u8,
};

pub const pthread_cond_t = extern struct {
    cond: ?*anyopaque = null,
};

const INFINITE: u32 = 0xFFFF_FFFF;

pub fn pthread_cond_wait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t) c.E {
    _ = SleepConditionVariableSRW(&cond.cond, &mutex.lock, INFINITE, 0);
    return .SUCCESS;
}

/// `abstime` is CLOCK_REALTIME, matching POSIX's default condattr.
pub fn pthread_cond_timedwait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t, noalias abstime: *const timespec) c.E {
    var now: timespec = undefined;
    _ = clock_gettime(.REALTIME, &now);
    const deadline_ns: i128 = @as(i128, abstime.sec) * std.time.ns_per_s + abstime.nsec;
    const now_ns: i128 = @as(i128, now.sec) * std.time.ns_per_s + now.nsec;
    if (deadline_ns <= now_ns) return .TIMEDOUT;
    const ms: u32 = @intCast(@min(@divFloor(deadline_ns - now_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms), INFINITE - 1));
    if (SleepConditionVariableSRW(&cond.cond, &mutex.lock, ms, 0) == 0) return .TIMEDOUT;
    return .SUCCESS;
}

pub fn pthread_cond_signal(cond: *pthread_cond_t) c.E {
    WakeConditionVariable(&cond.cond);
    return .SUCCESS;
}

pub fn pthread_cond_broadcast(cond: *pthread_cond_t) c.E {
    WakeAllConditionVariable(&cond.cond);
    return .SUCCESS;
}

pub fn pthread_cond_destroy(cond: *pthread_cond_t) c.E {
    _ = cond;
    return .SUCCESS;
}

/// std.DynLib backend for Windows (removed from upstream std in 0.16).
pub const WindowsDynLib = struct {
    module: *anyopaque,

    pub fn open(path: []const u8) !WindowsDynLib {
        var buf: [std.os.windows.PATH_MAX_WIDE:0]u16 = undefined;
        if (path.len > buf.len) return error.NameTooLong;
        const len = std.unicode.wtf8ToWtf16Le(&buf, path) catch return error.FileNotFound;
        buf[len] = 0;
        return .{ .module = LoadLibraryW(buf[0..len :0]) orelse return error.FileNotFound };
    }

    pub fn openZ(path_c: [*:0]const u8) !WindowsDynLib {
        return open(std.mem.span(path_c));
    }

    pub fn close(self: *WindowsDynLib) void {
        _ = FreeLibrary(self.module);
        self.* = undefined;
    }

    pub fn lookup(self: *WindowsDynLib, comptime T: type, name: [:0]const u8) ?T {
        const address = GetProcAddress(self.module, name.ptr) orelse return null;
        return @ptrCast(@alignCast(address));
    }
};

/// Only views created by MapViewOfFile reach this on Windows.
pub fn munmap(addr: *align(std.heap.page_size_min) const anyopaque, len: usize) c_int {
    _ = len;
    return if (UnmapViewOfFile(addr) != 0) 0 else -1;
}

/// POSIX-style positional read. Synchronous Windows file objects update their
/// shared position even with an explicit OVERLAPPED offset. Reopen that object
/// asynchronously (never by pathname), leaving the caller's position alone.
/// Already asynchronous handles need no reopen. There is deliberately no global
/// handle cache: handle reuse and concurrent close would make it unsafe.
pub fn pread(fd: c.fd_t, buf: [*]u8, nbyte: usize, offset: c.off_t) isize {
    if (offset < 0) return failErrno(.INVAL);
    var iosb: windows.IO_STATUS_BLOCK = undefined;
    var access: windows.FILE.ACCESS_INFORMATION = undefined;
    var status = windows.ntdll.NtQueryInformationFile(fd, &iosb, &access, @sizeOf(@TypeOf(access)), .Access);
    if (status != .SUCCESS) return failWindowsError(RtlNtStatusToDosError(status));
    // ReOpenFile must not grant a write-only caller new read access.
    if (!access.AccessFlags.SPECIFIC.FILE.READ_DATA) return failErrno(.BADF);
    if (nbyte == 0) return 0;
    var mode: windows.FILE.MODE.INFORMATION = undefined;
    status = windows.ntdll.NtQueryInformationFile(fd, &iosb, &mode, @sizeOf(@TypeOf(mode)), .Mode);
    if (status != .SUCCESS) return failWindowsError(RtlNtStatusToDosError(status));
    const reopened = mode.Mode.IO != .ASYNCHRONOUS;
    const handle = if (reopened) ReOpenFile(fd, 1, 1 | 2 | 4, 0x40000000) else fd;
    if (handle == windows.INVALID_HANDLE_VALUE) return failWindowsError(GetLastError());
    defer if (reopened) windows.CloseHandle(handle);
    const event = CreateEventW(null, 1, 0, null) orelse return failWindowsError(GetLastError());
    defer windows.CloseHandle(event);
    const position: u64 = @intCast(offset);
    var operation: Overlapped = .{ .offset = @truncate(position), .offset_high = @truncate(position >> 32), .event = event };
    var read: u32 = 0;
    const len: u32 = @intCast(@min(nbyte, std.math.maxInt(u32), std.math.maxInt(isize)));
    if (ReadFile(handle, buf, len, &read, &operation) == 0) {
        const code = GetLastError();
        if (code == 38) return 0; // ERROR_HANDLE_EOF
        if (code != 997) return failWindowsError(code); // ERROR_IO_PENDING
        // Keep all request storage alive until completion, also on error.
        if (GetOverlappedResult(handle, &operation, &read, 1) == 0) {
            const completed_code = GetLastError();
            if (completed_code == 38) return 0;
            return failWindowsError(completed_code);
        }
    }
    return @intCast(read);
}

fn failErrno(value: c.E) isize {
    c._errno().* = @backingInt(value);
    return -1;
}

fn failWindowsError(code: u32) isize {
    return failErrno(switch (code) {
        2, 3 => .NOENT,
        4 => .MFILE,
        5, 32, 33 => .ACCES,
        6 => .BADF,
        8, 14, 1450 => .NOMEM,
        19 => .ROFS,
        50 => .NOSYS,
        87, 131 => .INVAL,
        109 => .PIPE,
        112 => .NOSPC,
        995 => .INTR,
        else => .IO,
    });
}
