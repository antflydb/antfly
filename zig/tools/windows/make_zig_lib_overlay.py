#!/usr/bin/env python3
"""Create a patched Zig lib directory for experimental Windows builds.

Zig 0.17's std.c leaves several POSIX declarations (time, pthread mutex and
condition variable, memory advice, dirent, pread, munmap) as `void` or as
unresolved externs on Windows. Antfly calls them directly from dozens of
modules, so this overlay gives std.c small Win32-backed implementations instead
of patching every call site. Experimental only: the supported port should route
these calls through antfly_platform.

    python3 tools/windows/make_zig_lib_overlay.py --zig-lib ~/.local/zig-0.17.0/lib --out DIR
    ZIG_LIB_DIR=DIR zig build antfly -Dtarget=x86_64-windows-gnu ...

(`zig build --zig-lib-dir` is rejected after the step name; see README.md.)
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

COMPAT = Path(__file__).with_name("antfly_windows_compat.zig")

# (anchor, replacement) pairs applied once each to std/c.zig.
EDITS = [
    (
        "    .netbsd, .openbsd, .serenity, .wasi => c_longlong,\n    else => void,\n};\npub const suseconds_t",
        "    .netbsd, .openbsd, .serenity, .wasi => c_longlong,\n    .windows => i64,\n    else => void,\n};\npub const suseconds_t",
    ),
    (
        "pub const clockid_t = switch (native_os) {\n",
        "pub const clockid_t = switch (native_os) {\n    .windows => antfly_windows_compat.clockid_t,\n",
    ),
    (
        "pub const clock_gettime = switch (native_os) {\n",
        "pub const clock_gettime = switch (native_os) {\n    .windows => antfly_windows_compat.clock_gettime,\n",
    ),
    (
        "pub const nanosleep = switch (native_os) {\n",
        "pub const nanosleep = switch (native_os) {\n    .windows => antfly_windows_compat.nanosleep,\n",
    ),
    (
        "pub const MADV = switch (native_os) {\n",
        "pub const MADV = switch (native_os) {\n    .windows => antfly_windows_compat.MADV,\n",
    ),
    (
        'pub extern "c" fn madvise(\n    addr: *align(page_size) anyopaque,\n    length: usize,\n    advice: u32,\n) c_int;',
        "pub const madvise = if (native_os == .windows) antfly_windows_compat.madvise else private.madvise;",
    ),
    (
        "pub const pthread_mutex_t = switch (native_os) {\n",
        "pub const pthread_mutex_t = switch (native_os) {\n    .windows => antfly_windows_compat.pthread_mutex_t,\n",
    ),
    (
        (
            'pub extern "c" fn pthread_mutex_lock(mutex: *pthread_mutex_t) E;\n'
            'pub extern "c" fn pthread_mutex_unlock(mutex: *pthread_mutex_t) E;\n'
            'pub extern "c" fn pthread_mutex_trylock(mutex: *pthread_mutex_t) E;\n'
            'pub extern "c" fn pthread_mutex_destroy(mutex: *pthread_mutex_t) E;\n'
        ),
        (
            "pub const pthread_mutex_lock = if (native_os == .windows) antfly_windows_compat.pthread_mutex_lock else private.pthread_mutex_lock;\n"
            "pub const pthread_mutex_unlock = if (native_os == .windows) antfly_windows_compat.pthread_mutex_unlock else private.pthread_mutex_unlock;\n"
            "pub const pthread_mutex_trylock = if (native_os == .windows) antfly_windows_compat.pthread_mutex_trylock else private.pthread_mutex_trylock;\n"
            "pub const pthread_mutex_destroy = if (native_os == .windows) antfly_windows_compat.pthread_mutex_destroy else private.pthread_mutex_destroy;\n"
        ),
    ),
    (
        '    extern "c" fn clock_gettime(clk_id: clockid_t, tp: *timespec) c_int;\n',
        (
            '    extern "c" fn clock_gettime(clk_id: clockid_t, tp: *timespec) c_int;\n'
            '    extern "c" fn madvise(addr: *align(page_size) anyopaque, length: usize, advice: u32) c_int;\n'
            '    extern "c" fn pthread_mutex_lock(mutex: *pthread_mutex_t) E;\n'
            '    extern "c" fn pthread_mutex_unlock(mutex: *pthread_mutex_t) E;\n'
            '    extern "c" fn pthread_mutex_trylock(mutex: *pthread_mutex_t) E;\n'
            '    extern "c" fn pthread_mutex_destroy(mutex: *pthread_mutex_t) E;\n'
        ),
    ),
    (
        "pub const pthread_cond_t = switch (native_os) {\n",
        "pub const pthread_cond_t = switch (native_os) {\n    .windows => antfly_windows_compat.pthread_cond_t,\n",
    ),
    (
        (
            'pub extern "c" fn pthread_cond_wait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t) E;\n'
            'pub extern "c" fn pthread_cond_timedwait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t, noalias abstime: *const timespec) E;\n'
            'pub extern "c" fn pthread_cond_signal(cond: *pthread_cond_t) E;\n'
            'pub extern "c" fn pthread_cond_broadcast(cond: *pthread_cond_t) E;\n'
            'pub extern "c" fn pthread_cond_destroy(cond: *pthread_cond_t) E;\n'
        ),
        (
            "pub const pthread_cond_wait = if (native_os == .windows) antfly_windows_compat.pthread_cond_wait else private.pthread_cond_wait;\n"
            "pub const pthread_cond_timedwait = if (native_os == .windows) antfly_windows_compat.pthread_cond_timedwait else private.pthread_cond_timedwait;\n"
            "pub const pthread_cond_signal = if (native_os == .windows) antfly_windows_compat.pthread_cond_signal else private.pthread_cond_signal;\n"
            "pub const pthread_cond_broadcast = if (native_os == .windows) antfly_windows_compat.pthread_cond_broadcast else private.pthread_cond_broadcast;\n"
            "pub const pthread_cond_destroy = if (native_os == .windows) antfly_windows_compat.pthread_cond_destroy else private.pthread_cond_destroy;\n"
        ),
    ),
    (
        '    extern "c" fn madvise(addr: *align(page_size) anyopaque, length: usize, advice: u32) c_int;\n',
        (
            '    extern "c" fn madvise(addr: *align(page_size) anyopaque, length: usize, advice: u32) c_int;\n'
            '    extern "c" fn pthread_cond_wait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t) E;\n'
            '    extern "c" fn pthread_cond_timedwait(noalias cond: *pthread_cond_t, noalias mutex: *pthread_mutex_t, noalias abstime: *const timespec) E;\n'
            '    extern "c" fn pthread_cond_signal(cond: *pthread_cond_t) E;\n'
            '    extern "c" fn pthread_cond_broadcast(cond: *pthread_cond_t) E;\n'
            '    extern "c" fn pthread_cond_destroy(cond: *pthread_cond_t) E;\n'
        ),
    ),
    (
        "        else => private.readdir,\n    },\n    .windows => {},\n    else => private.readdir,\n};",
        "        else => private.readdir,\n    },\n    else => private.readdir,\n};",
    ),
    (
        'pub extern "c" fn pread(fd: fd_t, buf: [*]u8, nbyte: usize, offset: off_t) isize;\n',
        "pub const pread = if (native_os == .windows) antfly_windows_compat.pread else private.pread;\n",
    ),
    (
        'pub extern "c" fn munmap(addr: *align(page_size) const anyopaque, len: usize) c_int;\n',
        "pub const munmap = if (native_os == .windows) antfly_windows_compat.munmap else private.munmap;\n",
    ),
    (
        '    extern "c" fn nanosleep(rqtp: *const timespec, rmtp: ?*timespec) c_int;\n',
        (
            '    extern "c" fn nanosleep(rqtp: *const timespec, rmtp: ?*timespec) c_int;\n'
            '    extern "c" fn pread(fd: fd_t, buf: [*]u8, nbyte: usize, offset: off_t) isize;\n'
            '    extern "c" fn munmap(addr: *align(page_size) const anyopaque, len: usize) c_int;\n'
        ),
    ),
    (
        "pub const dirent = switch (native_os) {\n",
        "pub const dirent = switch (native_os) {\n    .windows => antfly_windows_compat.dirent,\n",
    ),
    (
        "const windows = std.os.windows;\n",
        'const windows = std.os.windows;\nconst antfly_windows_compat = @import("c/antfly_windows_compat.zig");\n',
    ),
]


DYNLIB_EDITS = [
    (
        "        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => DlDynLib,\n",
        (
            "        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => DlDynLib,\n"
            '        .windows => @import("c/antfly_windows_compat.zig").WindowsDynLib,\n'
        ),
    ),
]


# std locks one byte at offset 0. Windows byte-range locks are mandatory, so
# that blocks every other handle (even in-process) from reading or writing a
# file's first byte, where Lite keeps its header and writer-lock marker. A
# sentinel byte past any real data keeps lock-vs-lock semantics, like SQLite.
THREADED_EDITS = [
    (
        "const windows_lock_range_off: windows.LARGE_INTEGER = 0;\n",
        "const windows_lock_range_off: windows.LARGE_INTEGER = 1 << 62;\n",
    ),
]


def apply(path: Path, edits) -> None:
    source = path.read_text(encoding="utf-8")
    for anchor, replacement in edits:
        if source.count(anchor) != 1:
            raise SystemExit(f"anchor not found exactly once in {path}:\n{anchor}")
        source = source.replace(anchor, replacement)
    path.write_text(source, encoding="utf-8")


def create_overlay(zig_lib: Path, out: Path) -> None:
    zig_lib = zig_lib.resolve(strict=True)
    out = out.resolve()
    if zig_lib == out or zig_lib in out.parents or out in zig_lib.parents:
        raise ValueError("Zig library and overlay output must not overlap")
    # Patch a complete copy first. An incompatible Zig release must not
    # destroy the previously usable overlay.
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".antfly-zig-overlay-", dir=out.parent
    ) as temporary:
        staged = Path(temporary) / "lib"
        shutil.copytree(zig_lib, staged, symlinks=True)
        apply(staged / "std" / "c.zig", EDITS)
        apply(staged / "std" / "dynamic_library.zig", DYNLIB_EDITS)
        apply(staged / "std" / "Io" / "Threaded.zig", THREADED_EDITS)
        threaded = staged / "std" / "Io" / "Threaded.zig"
        source = threaded.read_text(encoding="utf-8")
        lock_call = "windows.ntdll.NtLockFile("
        if source.count(lock_call) != 4:
            raise SystemExit("expected four NtLockFile calls in Threaded.zig")
        source = source.replace(
            lock_call, '@import("../c/antfly_windows_compat.zig").NtLockFile('
        )
        unlock_call = "windows.ntdll.NtUnlockFile("
        if source.count(unlock_call) != 4:
            raise SystemExit("expected four NtUnlockFile calls in Threaded.zig")
        source = source.replace(
            unlock_call, '@import("../c/antfly_windows_compat.zig").NtUnlockFile('
        )
        threaded.write_text(source, encoding="utf-8")
        shutil.copyfile(COMPAT, staged / "std" / "c" / "antfly_windows_compat.zig")
        if out.exists():
            shutil.rmtree(out)
        staged.rename(out)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--zig-lib", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    create_overlay(args.zig_lib, args.out)
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
