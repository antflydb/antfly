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

const std = @import("std");
const builtin = @import("builtin");

var freestanding_counter: u64 = 0;
const is_hostless = builtin.os.tag == .freestanding or builtin.os.tag == .wasi;

pub fn sleepNs(ns: u64) void {
    if (comptime is_hostless) return;

    var req = std.posix.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

pub fn yieldBriefly() void {
    if (comptime is_hostless) return;
    sleepNs(100_000);
}

pub fn monotonicNs() u64 {
    if (comptime is_hostless) {
        freestanding_counter +%= 1;
        return freestanding_counter;
    }

    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

/// Suspend-inclusive authority clock for fail-closed lease deadlines. Linux
/// CLOCK_BOOTTIME advances while a VM/node is suspended; MONOTONIC does not.
/// Other supported platforms fall back to their monotonic clock.
pub fn authorityNs() u64 {
    if (comptime builtin.os.tag == .linux) {
        var ts: std.posix.timespec = undefined;
        switch (std.posix.errno(std.posix.system.clock_gettime(.BOOTTIME, &ts))) {
            .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
            // A suspend-blind fallback could reopen a stale writer after node
            // resume. Saturating time expires every outstanding authority.
            else => return std.math.maxInt(u64),
        }
    }
    return monotonicNs();
}

pub fn realtimeNs() u64 {
    if (comptime is_hostless) {
        freestanding_counter +%= 1;
        return freestanding_counter;
    }

    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

pub fn nowSeconds() u64 {
    return monotonicNs() / std.time.ns_per_s;
}

pub fn residentBytes() usize {
    if (comptime is_hostless) return 0;

    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}
