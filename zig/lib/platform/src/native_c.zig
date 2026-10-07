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

//! Native C compatibility APIs. Prefer higher-level platform APIs in new code.
const std = @import("std");
const windows = @import("windows_native.zig");
const is_windows = @import("builtin").os.tag == .windows;
pub const socket_testing = if (is_windows and @import("builtin").is_test) windows.SocketTesting else void;

pub const CLOCK = if (is_windows) windows.clockid_t else std.posix.CLOCK;
pub const clockid_t = if (is_windows) windows.clockid_t else std.c.clockid_t;
pub const clock_gettime = if (is_windows) windows.clock_gettime else std.c.clock_gettime;
pub const nanosleep = if (is_windows) windows.nanosleep else std.c.nanosleep;
pub const MADV = if (is_windows) windows.MADV else std.c.MADV;
pub const madvise = if (is_windows) windows.madvise else std.c.madvise;
pub const pthread_mutex_t = if (is_windows) windows.pthread_mutex_t else std.c.pthread_mutex_t;
pub const pthread_mutex_lock = if (is_windows) windows.pthread_mutex_lock else std.c.pthread_mutex_lock;
pub const pthread_mutex_unlock = if (is_windows) windows.pthread_mutex_unlock else std.c.pthread_mutex_unlock;
pub const pthread_mutex_trylock = if (is_windows) windows.pthread_mutex_trylock else std.c.pthread_mutex_trylock;
pub const pthread_mutex_destroy = if (is_windows) windows.pthread_mutex_destroy else std.c.pthread_mutex_destroy;
pub const pthread_cond_t = if (is_windows) windows.pthread_cond_t else std.c.pthread_cond_t;
pub const pthread_cond_wait = if (is_windows) windows.pthread_cond_wait else std.c.pthread_cond_wait;
pub const pthread_cond_timedwait = if (is_windows) windows.pthread_cond_timedwait else std.c.pthread_cond_timedwait;
pub const pthread_cond_signal = if (is_windows) windows.pthread_cond_signal else std.c.pthread_cond_signal;
pub const pthread_cond_broadcast = if (is_windows) windows.pthread_cond_broadcast else std.c.pthread_cond_broadcast;
pub const pthread_cond_destroy = if (is_windows) windows.pthread_cond_destroy else std.c.pthread_cond_destroy;
pub const dirent = if (is_windows) windows.dirent else std.c.dirent;
pub const munmap = if (is_windows) windows.munmap else std.c.munmap;
pub const pread = if (is_windows) windows.pread else std.c.pread;
pub const timespec = if (is_windows) windows.timespec else std.c.timespec;
pub const PTHREAD_MUTEX_INITIALIZER: pthread_mutex_t = if (is_windows) .{} else std.c.PTHREAD_MUTEX_INITIALIZER;
pub const PTHREAD_COND_INITIALIZER: pthread_cond_t = if (is_windows) .{} else std.c.PTHREAD_COND_INITIALIZER;
pub const readdir = if (is_windows) mingw.readdir else std.c.readdir;
const mingw = struct {
    extern "c" fn readdir(dir: *std.c.DIR) ?*dirent;
};
