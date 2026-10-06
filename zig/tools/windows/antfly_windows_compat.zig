//! Win32-backed stand-ins for POSIX declarations that Zig 0.17's std.c leaves
//! as `void` on Windows. Installed into an experimental zig lib overlay by
//! make_zig_lib_overlay.py; not part of upstream std.

const std = @import("../std.zig");
const c = std.c;

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

const Overlapped = extern struct {
    internal: usize = 0,
    internal_high: usize = 0,
    offset: u32,
    offset_high: u32,
    event: ?std.os.windows.HANDLE = null,
};

const windows = std.os.windows;
const WineLockFn = *const fn (windows.HANDLE, ?windows.HANDLE, ?*align(2) const windows.IO_APC_ROUTINE, ?*anyopaque, ?*windows.IO_STATUS_BLOCK, *const windows.LARGE_INTEGER, *const windows.LARGE_INTEGER, ?*const windows.ULONG, windows.BOOLEAN, windows.BOOLEAN) callconv(.winapi) windows.NTSTATUS;
const WineUnlockFn = *const fn (windows.HANDLE, *windows.IO_STATUS_BLOCK, *const windows.LARGE_INTEGER, *const windows.LARGE_INTEGER, ?*const windows.ULONG) callconv(.winapi) windows.NTSTATUS;

fn wineNtdll() ?*anyopaque {
    const module = GetModuleHandleW(std.unicode.utf8ToUtf16LeStringLiteral("ntdll.dll")) orelse return null;
    return if (GetProcAddress(module, "wine_get_version") != null) module else null;
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

fn setTimespec(tp: *c.timespec, ns: u128) void {
    tp.* = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
}

pub fn clock_gettime(clk_id: clockid_t, tp: *c.timespec) c_int {
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

pub fn nanosleep(rqtp: *const c.timespec, rmtp: ?*c.timespec) c_int {
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
pub fn pthread_cond_timedwait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t, noalias abstime: *const c.timespec) c.E {
    var now: c.timespec = undefined;
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

/// Positional read on a Win32 file handle (std.posix.fd_t is HANDLE here).
pub fn pread(fd: c.fd_t, buf: [*]u8, nbyte: usize, offset: c.off_t) isize {
    const position: u64 = @bitCast(@as(i64, offset));
    var overlapped: Overlapped = .{ .offset = @truncate(position), .offset_high = @truncate(position >> 32) };
    var read: u32 = 0;
    const len: u32 = @intCast(@min(nbyte, std.math.maxInt(u32)));
    if (ReadFile(fd, buf, len, &read, &overlapped) == 0) {
        // ERROR_HANDLE_EOF (38) reports a read at or past end of file.
        return if (GetLastError() == 38) 0 else -1;
    }
    return read;
}
