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

//! Win32 clock and sleep backends for `time.zig`, whose POSIX paths are
//! unavailable on Windows (experimental Windows support).

const std = @import("std");

const BOOL = c_int;

extern "kernel32" fn Sleep(milliseconds: u32) callconv(.winapi) void;
extern "kernel32" fn QueryPerformanceCounter(count: *i64) callconv(.winapi) BOOL;
extern "kernel32" fn QueryPerformanceFrequency(frequency: *i64) callconv(.winapi) BOOL;
extern "kernel32" fn GetSystemTimePreciseAsFileTime(file_time: *u64) callconv(.winapi) void;

pub fn sleepNs(ns: u64) void {
    const ms = std.math.divCeil(u64, ns, std.time.ns_per_ms) catch unreachable;
    Sleep(@intCast(@min(ms, std.math.maxInt(u32) - 1)));
}

pub fn monotonicNs() u64 {
    var count: i64 = 0;
    var frequency: i64 = 0;
    if (QueryPerformanceCounter(&count) == 0 or QueryPerformanceFrequency(&frequency) == 0 or frequency <= 0) return 0;
    const ticks: u128 = @intCast(count);
    return @intCast(ticks * std.time.ns_per_s / @as(u128, @intCast(frequency)));
}

/// Nanoseconds since the Unix epoch.
pub fn realtimeNs() u64 {
    // FILETIME counts 100ns intervals since 1601-01-01.
    const unix_epoch_in_filetime: u64 = 116_444_736_000_000_000;
    var file_time: u64 = 0;
    GetSystemTimePreciseAsFileTime(&file_time);
    if (file_time < unix_epoch_in_filetime) return 0;
    return (file_time - unix_epoch_in_filetime) * 100;
}
