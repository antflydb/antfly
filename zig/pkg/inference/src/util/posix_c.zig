// Copyright 2026 Antfly, Inc.
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

//! Hand-written libc POSIX shim for the file/dir helpers in `c_file.zig` and a
//! few backends. Zig 0.17 removed the `@cImport`/`@cInclude` builtins; rather
//! than wire a `addTranslateC` module into every consumer of `c_file` across
//! both the inference and antfly module graphs, this provides the exact
//! `extern "c"` surface those call sites used (`fcntl.h`, `unistd.h`,
//! `sys/stat.h`, `sys/mman.h`, `dirent.h`). Only valid when linking libc.
//!
//! Flags/types use the C ABI (`c_int` flags, `mode_t`) so existing call sites
//! such as `open(p, O_WRONLY | O_CREAT | O_TRUNC, @as(mode_t, 0o644))` and
//! `struct_stat{ .st_size }` / `dirent{ .d_name }` compile unchanged.

const std = @import("std");
const builtin = @import("builtin");

pub const mode_t = std.c.mode_t;
pub const off_t = std.c.off_t;

// --- fcntl.h open(2) flags ---
// Values are the POSIX/Linux and Darwin constants used by the call sites
// (O_RDONLY/O_WRONLY/O_CREAT/O_TRUNC).
pub const O_RDONLY: c_int = 0x0000;
pub const O_WRONLY: c_int = 0x0001;
pub const O_CREAT: c_int = if (builtin.os.tag == .macos) 0x0200 else 0o100;
pub const O_TRUNC: c_int = if (builtin.os.tag == .macos) 0x0400 else 0o1000;

// --- sys/mman.h madvise(2) advice (same values on Linux and Darwin) ---
pub const MADV_NORMAL: c_int = 0;
pub const MADV_RANDOM: c_int = 1;
pub const MADV_SEQUENTIAL: c_int = 2;

pub const struct_stat = std.c.Stat;

pub const DIR = std.c.DIR;

// `struct dirent` with the field named `d_name`, matching the C member the call
// sites read (`@ptrCast(&entry.*.d_name)`). std.c.dirent names the member `name`
// and is platform-specific; this mirrors the C ABI layout (the leading members
// only need to occupy the right space before `d_name`).
pub const dirent = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos => extern struct {
        d_ino: u64,
        d_seekoff: u64,
        d_reclen: u16,
        d_namlen: u16,
        d_type: u8,
        d_name: [1024]u8,
    },
    else => extern struct {
        d_ino: std.c.ino_t,
        d_off: std.c.off_t,
        d_reclen: c_ushort,
        d_type: u8,
        d_name: [256]u8,
    },
};

comptime {
    // Call sites read the entry name via `@ptrCast(&entry.*.d_name)` over the
    // pointer libc's `readdir` returns, so `d_name`'s offset must match the real
    // `struct dirent` ABI: 19 on glibc/musl Linux x86_64, 21 on Darwin. Turn any
    // future drift in `std.c.ino_t`/`off_t` widths into a build error rather than
    // a silent garbage read.
    const expected: usize = if (builtin.os.tag == .macos) 21 else 19;
    if (@offsetOf(dirent, "d_name") != expected)
        @compileError("posix_c.dirent.d_name offset does not match the platform struct dirent ABI");
}

// ABI-stable symbols (same name and layout on every platform we target).
pub extern "c" fn open(path: [*:0]const u8, oflag: c_int, ...) c_int;
pub extern "c" fn close(fd: c_int) c_int;
pub extern "c" fn read(fd: c_int, buf: [*]u8, nbyte: usize) isize;
pub extern "c" fn write(fd: c_int, buf: [*]const u8, nbyte: usize) isize;
pub extern "c" fn pread(fd: c_int, buf: [*]u8, nbyte: usize, offset: off_t) isize;
pub extern "c" fn madvise(addr: [*]u8, length: usize, advice: c_int) c_int;
pub extern "c" fn unlink(path: [*:0]const u8) c_int;
pub extern "c" fn link(oldpath: [*:0]const u8, newpath: [*:0]const u8) c_int;
pub extern "c" fn symlink(target: [*:0]const u8, linkpath: [*:0]const u8) c_int;
pub extern "c" fn mkdir(path: [*:0]const u8, mode: mode_t) c_int;
pub extern "c" fn getcwd(buf: [*]u8, size: usize) ?[*:0]u8;
pub extern "c" fn getpid() c_int;
pub extern "c" fn opendir(name: [*:0]const u8) ?*DIR;
pub extern "c" fn closedir(dir: ?*DIR) c_int;

// The stat-family and readdir return `struct stat`/`struct dirent`, whose layout
// (and thus the libc symbol) changed when Darwin widened ino_t to 64-bit: on
// macOS x86_64 the 64-bit-inode variants carry a `$INODE64` suffix, matching the
// layouts in `struct_stat`/`dirent` above. Linux and Apple-Silicon macOS have no
// suffix (the plain symbols are already 64-bit). This mirrors std.c exactly; a
// plain `extern fn fstat` would silently bind the legacy 32-bit-inode symbol on
// Intel Macs and corrupt every read. (`open`/`pread`/etc. above are unaffected —
// they don't traffic in stat/dirent structs.)
const darwin_x86_64 = builtin.os.tag.isDarwin() and builtin.cpu.arch == .x86_64;
pub const fstat = if (darwin_x86_64) inode64.@"fstat$INODE64" else plain.fstat;
pub const lstat = if (darwin_x86_64) inode64.@"lstat$INODE64" else plain.lstat;
pub const readdir = if (darwin_x86_64) inode64.@"readdir$INODE64" else plain.readdir;

const plain = struct {
    extern "c" fn fstat(fd: c_int, buf: *struct_stat) c_int;
    extern "c" fn lstat(path: [*:0]const u8, buf: *struct_stat) c_int;
    extern "c" fn readdir(dir: ?*DIR) ?*dirent;
};
const inode64 = struct {
    extern "c" fn @"fstat$INODE64"(fd: c_int, buf: *struct_stat) c_int;
    extern "c" fn @"lstat$INODE64"(path: [*:0]const u8, buf: *struct_stat) c_int;
    extern "c" fn @"readdir$INODE64"(dir: ?*DIR) ?*dirent;
};
