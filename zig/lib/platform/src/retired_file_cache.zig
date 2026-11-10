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

//! Bounded ownership of clean file-cache reclamation opportunities. A retired
//! descriptor is duplicated with CLOEXEC; neither paths nor owner pointers are
//! dereferenced later. Normal close preserves warm pages until pressure, owner
//! teardown, or the fixed descriptor bound requires an advisory discard.
const std = @import("std");
const builtin = @import("builtin");
const sync = @import("sync.zig");
const time = @import("time.zig");
pub const supported = builtin.os.tag == .linux;
const Entry = struct { fd: std.posix.fd_t, owner: usize, bytes: u64, retry_after_ns: u64 = 0 };
const capacity = 64;
var mutex: std.atomic.Mutex = .unlocked;
var entries: [capacity]?Entry = @splat(null);
var cursor: usize = 0;

/// Abstract/fault-injected I/O must never send virtual handles to the kernel.
/// Other runtimes may opt in when they share Threaded's OS file-close method.
fn osFileIo(io: std.Io) bool {
    if (comptime !supported) return false;
    if (io.vtable.fileClose == std.Io.Threaded.global_single_threaded.io().vtable.fileClose) return true;
    if (comptime std.Io.fiber.supported) return @import("io_uring_compat.zig").usesOsFileDescriptors(io);
    return false;
}

pub fn retire(owner: usize, io: std.Io, file: std.Io.File, bytes: u64) void {
    if (comptime !supported) return;
    if (bytes == 0 or !osFileIo(io)) return;
    const fd = std.os.linux.fcntl(file.handle, std.os.linux.F.DUPFD_CLOEXEC, 0);
    if (std.os.linux.errno(fd) != .SUCCESS) {
        // Descriptor pressure must not turn this optional cache into a leak.
        _ = std.os.linux.fadvise(file.handle, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
        return;
    }
    sync.lockYielding(&mutex);
    const previous = entries[cursor];
    entries[cursor] = .{ .fd = @intCast(fd), .owner = owner, .bytes = bytes };
    cursor = (cursor + 1) % capacity;
    mutex.unlock();
    if (previous) |entry| discard(entry);
}

pub fn discardUnowned(io: std.Io, file: std.Io.File) void {
    if (comptime !supported) return;
    if (!osFileIo(io)) return;
    _ = std.os.linux.fadvise(file.handle, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
}

fn discard(entry: Entry) void {
    if (comptime !supported) return;
    // DONTNEED never writes dirty pages, unmaps live readers, or changes data.
    // The kernel may decline the hint; callers must re-probe physical memory.
    _ = std.os.linux.fadvise(entry.fd, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
    _ = std.os.linux.close(entry.fd);
}

fn take(owner: ?usize) ?Entry {
    sync.lockYielding(&mutex);
    defer mutex.unlock();
    for (0..capacity) |offset| {
        const index = (cursor + offset) % capacity;
        const entry = entries[index] orelse continue;
        if (owner) |id| if (entry.owner != id) continue;
        entries[index] = null;
        return entry;
    }
    return null;
}

fn takeEligible(now: u64) ?Entry {
    if (comptime !supported) return null;
    sync.lockYielding(&mutex);
    defer mutex.unlock();
    for (0..capacity) |offset| {
        const index = (cursor + offset) % capacity;
        const entry = entries[index] orelse continue;
        if (entry.retry_after_ns > now) continue;
        // Keep the original opportunity for a later request: dirty/writeback
        // pages may become reclaimable after this hint. A duplicated descriptor
        // fences concurrent owner shutdown without holding a lock across I/O.
        const fd = std.os.linux.fcntl(entry.fd, std.os.linux.F.DUPFD_CLOEXEC, 0);
        if (std.os.linux.errno(fd) != .SUCCESS) {
            entries[index] = null;
            return entry;
        }
        entries[index].?.retry_after_ns = now +| std.time.ns_per_s;
        return .{ .fd = @intCast(fd), .owner = entry.owner, .bytes = entry.bytes };
    }
    return null;
}

/// Return attempted bytes, not freed bytes. Each opportunity is offered at most
/// once per second, so declined hints cannot spin within an admission attempt.
/// No allocator or owner callback is involved.
pub fn reclaim(target_bytes: u64) u64 {
    return reclaimAt(target_bytes, time.monotonicNs());
}

fn reclaimAt(target_bytes: u64, now: u64) u64 {
    var attempted: u64 = 0;
    for (0..capacity) |_| {
        if (attempted >= target_bytes) break;
        const entry = takeEligible(now) orelse break;
        discard(entry);
        attempted +|= entry.bytes;
    }
    return attempted;
}

pub fn releaseOwner(owner: usize) void {
    for (0..capacity) |_| {
        const entry = take(owner) orelse break;
        discard(entry);
    }
}

pub fn pendingForOwner(owner: usize) usize {
    sync.lockYielding(&mutex);
    defer mutex.unlock();
    var count: usize = 0;
    for (entries) |slot| if (slot) |entry| {
        if (entry.owner == owner) count += 1;
    };
    return count;
}

test "retired file cache owns duplicate descriptors and bounds reclamation" {
    if (comptime !supported) return error.SkipZigTest;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const io = std.testing.io;
    const file = try dir.dir.createFile(io, "retired", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "preserved data");
    try file.sync(io);
    defer releaseOwner(123);
    defer releaseOwner(456);
    for (0..capacity + 3) |_| retire(123, io, file, 14);
    releaseOwner(456);
    try std.testing.expectEqual(@as(u64, 14), reclaim(1));
    releaseOwner(123);
    try std.testing.expectEqual(@as(u64, 0), reclaim(1));
    var contents: [14]u8 = undefined;
    const count = try file.readPositionalAll(io, &contents, 0);
    try std.testing.expectEqualStrings("preserved data", contents[0..count]);
}

test "retired file cache retries declined hints without spinning or borrowing owners" {
    if (comptime !supported) return error.SkipZigTest;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const io = std.testing.io;
    const file = try dir.dir.createFile(io, "dirty", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "dirty data");
    retire(789, io, file, 10);
    defer releaseOwner(789);
    try std.testing.expectEqual(@as(u64, 10), reclaimAt(1, 123));
    try std.testing.expectEqual(@as(u64, 0), reclaimAt(1, 123));
    try std.testing.expectEqual(@as(u64, 10), reclaimAt(1, 123 + std.time.ns_per_s));
    var data: [10]u8 = undefined;
    _ = try file.readPositionalAll(io, &data, 0);
    try std.testing.expectEqualStrings("dirty data", &data);
}
