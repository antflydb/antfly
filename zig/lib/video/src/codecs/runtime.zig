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

const std = @import("std");
const media = @import("antfly_media");
const Budget = @import("../decode_budget.zig").Budget;
const Header = struct { size: usize, total: usize, owner: *Owner, previous: ?*Header, next: ?*Header };
comptime {
    std.debug.assert(@sizeOf(Header) <= 64);
}
threadlocal var current: ?*Owner = null;
/// Stable session owner; all core allocations, including partially constructed
/// state, are reclaimable without traversing decoder structures after failure.
pub const Owner = struct {
    budget: Budget,
    allocations: ?media.admission.TrackedAllocator = null,
    head: ?*Header = null,
    control: media.source.Control = .{},
    failure: ?anyerror = null,
    native_error: c_int = 0,
    polls: usize = 0,
    pub fn check(self: *Owner) bool {
        if (self.failure != null or self.native_error != 0) return false;
        self.control.check() catch |err| {
            self.failure = err;
            return false;
        };
        return true;
    }
    pub fn sweep(self: *Owner) void {
        while (self.head) |h| antfly_codec_afz_free(@as([*]u8, @ptrCast(h)) + 64);
        std.debug.assert(self.budget.live == 0);
        if (self.allocations) |*tracked| {
            std.debug.assert(tracked.live == 0);
            tracked.reservation.deinit();
        }
    }
    pub fn result(self: *Owner) !void {
        if (self.failure) |err| return err;
        if (self.native_error != 0) return error.InvalidVideoData;
    }
};
pub const Scope = struct {
    previous: ?*Owner,
    pub fn enter(owner: *Owner) Scope {
        const scope = Scope{ .previous = current };
        current = owner;
        return scope;
    }
    pub fn deinit(self: Scope) void {
        current = self.previous;
    }
};
pub export fn antfly_codec_poll() c_int {
    return if (current) |owner| @intFromBool(!owner.check()) else 1;
}
pub export fn antfly_codec_afz_malloc(n: usize) ?*anyopaque {
    const owner = current orelse return null;
    if (!owner.check()) return null;
    const total = std.math.add(usize, n, 64) catch {
        owner.failure = error.ResourceLimitExceeded;
        return null;
    };
    const data = owner.budget.allocator().alignedAlloc(u8, .@"64", total) catch |err| {
        owner.failure = if (owner.budget.denied) error.ResourceLimitExceeded else if (owner.allocations) |tracked| tracked.denied orelse err else err;
        return null;
    };
    const h: *Header = @ptrCast(data.ptr);
    h.* = .{ .size = n, .total = total, .owner = owner, .previous = null, .next = owner.head };
    if (owner.head) |next| next.previous = h;
    owner.head = h;
    return data.ptr + 64;
}
pub export fn antfly_codec_afz_free(p: ?*anyopaque) void {
    const data: [*]align(64) u8 = @ptrFromInt(@intFromPtr(p orelse return) - 64);
    const h: Header = @as(*Header, @ptrCast(data)).*;
    if (h.previous) |previous| previous.next = h.next else h.owner.head = h.next;
    if (h.next) |next| next.previous = h.previous;
    h.owner.budget.allocator().free(data[0..h.total]);
}
pub export fn antfly_codec_afz_calloc(n: usize, size: usize) ?*anyopaque {
    const bytes = std.math.mul(usize, n, size) catch return null;
    const p = antfly_codec_afz_malloc(bytes) orelse return null;
    @memset(@as([*]u8, @ptrCast(p))[0..bytes], 0);
    return p;
}
pub export fn antfly_codec_afz_realloc(p: ?*anyopaque, n: usize) ?*anyopaque {
    if (p == null) return antfly_codec_afz_malloc(n);
    const h: *Header = @ptrFromInt(@intFromPtr(p.?) - 64);
    const out = antfly_codec_afz_malloc(n) orelse return null;
    @memcpy(@as([*]u8, @ptrCast(out))[0..@min(n, h.size)], @as([*]u8, @ptrCast(p.?))[0..@min(n, h.size)]);
    antfly_codec_afz_free(p);
    return out;
}
pub export fn antfly_codec_afz_aligned_alloc(alignment: usize, n: usize) ?*anyopaque {
    if (alignment > 64) return null;
    return antfly_codec_afz_malloc(n);
}
pub export fn antfly_codec_afz_posix_memalign(out: *?*anyopaque, alignment: usize, n: usize) c_int {
    out.* = antfly_codec_afz_aligned_alloc(alignment, n);
    return if (out.* == null) 12 else 0;
}
pub export fn antfly_codec_afz_memcpy(out: ?*anyopaque, source: ?*const anyopaque, n: usize) ?*anyopaque {
    // Scalar reference code sometimes copies a restoration record to itself.
    // Treat identical ranges as a no-op; partial overlap remains invalid.
    if (n != 0 and out != source) @memcpy(@as([*]u8, @ptrCast(out.?))[0..n], @as([*]const u8, @ptrCast(source.?))[0..n]);
    return out;
}
pub export fn antfly_codec_afz_memmove(out: ?*anyopaque, source: ?*const anyopaque, n: usize) ?*anyopaque {
    if (n == 0) return out;
    const a = @as([*]u8, @ptrCast(out.?))[0..n];
    const b = @as([*]const u8, @ptrCast(source.?))[0..n];
    if (@intFromPtr(a.ptr) < @intFromPtr(b.ptr)) std.mem.copyForwards(u8, a, b) else std.mem.copyBackwards(u8, a, b);
    return out;
}
pub export fn antfly_codec_afz_memset(out: ?*anyopaque, v: c_int, n: usize) ?*anyopaque {
    if (n != 0) @memset(@as([*]u8, @ptrCast(out.?))[0..n], @truncate(@as(u32, @bitCast(v))));
    return out;
}
pub export fn antfly_codec_afz_memcmp(a: ?*const anyopaque, b: ?*const anyopaque, n: usize) c_int {
    if (n == 0) return 0;
    const aa = @as([*]const u8, @ptrCast(a.?));
    const bb = @as([*]const u8, @ptrCast(b.?));
    for (0..n) |i| {
        if (aa[i] != bb[i]) return @as(c_int, aa[i]) - @as(c_int, bb[i]);
    }
    return 0;
}
fn atomic(p: *anyopaque) *i32 {
    return @ptrCast(@alignCast(p));
}
pub export fn antfly_codec_afz_atomic_load(p: *const anyopaque) c_int {
    return @atomicLoad(i32, @as(*const i32, @ptrCast(@alignCast(p))), .seq_cst);
}
pub export fn antfly_codec_afz_atomic_store(p: *anyopaque, v: c_int) void {
    @atomicStore(i32, atomic(p), v, .seq_cst);
}
pub export fn antfly_codec_afz_atomic_add(p: *anyopaque, v: c_int) c_int {
    return @atomicRmw(i32, atomic(p), .Add, v, .seq_cst);
}
pub export fn antfly_codec_afz_atomic_exchange(p: *anyopaque, v: c_int) c_int {
    return @atomicRmw(i32, atomic(p), .Xchg, v, .seq_cst);
}
pub export fn antfly_codec_afz_atomic_or(p: *anyopaque, v: c_int) c_int {
    return @atomicRmw(i32, atomic(p), .Or, v, .seq_cst);
}
pub export fn antfly_codec_afz_atomic_compare(p: *anyopaque, expected: *i32, desired: c_int) c_int {
    if (@cmpxchgStrong(i32, atomic(p), expected.*, desired, .seq_cst, .seq_cst)) |old| {
        expected.* = old;
        return 0;
    }
    return 1;
}
pub export fn antfly_codec_afz_mutex_init(p: *i32, _: ?*const anyopaque) c_int {
    p.* = 0;
    return 0;
}
pub export fn antfly_codec_afz_mutex_destroy(_: *i32) c_int {
    return 0;
}
pub export fn antfly_codec_afz_mutex_lock(p: *i32) c_int {
    while (@atomicRmw(i32, p, .Xchg, 1, .acquire) != 0) std.atomic.spinLoopHint();
    return 0;
}
pub export fn antfly_codec_afz_mutex_unlock(p: *i32) c_int {
    @atomicStore(i32, p, 0, .release);
    return 0;
}
pub export fn antfly_codec_afz_cond_init(p: *i32, _: ?*const anyopaque) c_int {
    p.* = 0;
    return 0;
}
pub export fn antfly_codec_afz_cond_destroy(_: *i32) c_int {
    return 0;
}
pub export fn antfly_codec_afz_cond_signal(_: *i32) c_int {
    return 0;
}
pub export fn antfly_codec_afz_cond_broadcast(_: *i32) c_int {
    return 0;
}
pub export fn antfly_codec_afz_cond_wait(_: *i32, _: *i32) c_int {
    @panic("unexpected parallel AV1 path");
}
pub export fn antfly_codec_afz_pthread_once(p: *i32, callback: *const fn () callconv(.c) void) c_int {
    if (@cmpxchgStrong(i32, p, 0, 1, .acq_rel, .acquire) == null) {
        callback();
        @atomicStore(i32, p, 2, .release);
    } else {
        while (@atomicLoad(i32, p, .acquire) != 2) std.atomic.spinLoopHint();
    }
    return 0;
}
pub export fn antfly_codec_afz_pthread_create(_: *usize, _: ?*const i32, _: *const fn (?*anyopaque) callconv(.c) ?*anyopaque, _: ?*anyopaque) c_int {
    return 11;
}
pub export fn antfly_codec_afz_pthread_join(_: usize, _: ?*anyopaque) c_int {
    return 0;
}
pub export fn antfly_codec_afz_attr_init(p: *i32) c_int {
    p.* = 0;
    return 0;
}
pub export fn antfly_codec_afz_attr_destroy(_: *i32) c_int {
    return 0;
}
pub export fn antfly_codec_afz_attr_setstacksize(_: *i32, _: usize) c_int {
    return 0;
}
pub export var antfly_av1_dav1d_cpu_flags: c_uint = 0;
pub export var antfly_av1_dav1d_cpu_flags_mask: c_uint = 0;
pub export fn antfly_av1_dav1d_init_cpu() void {}
pub export fn antfly_av1_dav1d_set_cpu_flags_mask(_: c_uint) void {}
pub export fn antfly_av1_dav1d_num_logical_processors() c_int {
    return 1;
}
pub export fn antfly_codec_clzll(n: u64) c_int {
    return @clz(n);
}
pub export fn antfly_codec_abs(n: c_int) c_int {
    return if (n < 0) -n else n;
}
pub export fn antfly_codec_llabs(n: c_longlong) c_longlong {
    return if (n < 0) -n else n;
}
pub export fn antfly_codec_afz_trap() void {
    @panic("invalid decoder control state");
}

pub export fn antfly_codec_afz_vp9_failed() c_int {
    const owner = current orelse return 1;
    owner.polls +%= 1;
    if (owner.polls % 256 == 0) _ = owner.check();
    return @intFromBool(owner.failure != null or owner.native_error != 0);
}
pub export fn antfly_codec_afz_vp9_fail(info: ?*anyopaque, code: c_int) void {
    if (current) |owner| {
        if (owner.native_error == 0) owner.native_error = code;
    }
    if (info) |p| @as(*c_int, @ptrCast(@alignCast(p))).* = code;
}
const OnceEntry = struct { callback: ?*const fn () callconv(.c) void = null, state: i32 = 0 };
var once_lock: i32 = 0;
var once_entries: [32]OnceEntry = @splat(.{});
pub export fn antfly_codec_afz_vp9_once(callback: *const fn () callconv(.c) void) void {
    while (@cmpxchgWeak(i32, &once_lock, 0, 1, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    var selected: ?*OnceEntry = null;
    for (&once_entries) |*entry| {
        if (entry.callback == callback) {
            selected = entry;
            break;
        }
        if (entry.callback == null) {
            entry.callback = callback;
            selected = entry;
            break;
        }
    }
    @atomicStore(i32, &once_lock, 0, .release);
    const entry = selected orelse @panic("codec once registry full");
    if (@cmpxchgStrong(i32, &entry.state, 0, 1, .acquire, .monotonic) == null) {
        callback();
        @atomicStore(i32, &entry.state, 2, .release);
    } else while (@atomicLoad(i32, &entry.state, .acquire) != 2) std.atomic.spinLoopHint();
}

pub export fn antfly_codec_abort() noreturn {
    @panic("decoder invariant failed");
}
/// Portable in-place insertion sort for the bounded tile scheduling array.
pub export fn antfly_codec_sort(base: ?*anyopaque, count: usize, width: usize, compare: ?*const fn (?*const anyopaque, ?*const anyopaque) callconv(.c) c_int) void {
    if (count < 2 or width == 0) return;
    const bytes: [*]u8 = @ptrCast(base.?);
    const cmp = compare.?;
    for (1..count) |i| {
        var j = i;
        while (j > 0 and cmp(bytes + (j - 1) * width, bytes + j * width) > 0) : (j -= 1) {
            for (0..width) |k| std.mem.swap(u8, &bytes[(j - 1) * width + k], &bytes[j * width + k]);
        }
    }
}

/// libvpx inv_txfm.h specifies signed (8 + depth) intermediate coefficients
/// for valid streams. Reject violations through the normal owned error path,
/// rather than overflowing transform arithmetic or asserting on coded data.
pub fn vp9TransformRange(input: i64, depth: c_int) i64 {
    if (antfly_codec_afz_vp9_failed() != 0) return 0;
    if (depth != 8 and depth != 10 and depth != 12) {
        antfly_codec_afz_vp9_fail(null, 3);
        return 0;
    }
    const limit: i64 = @as(i64, 1) << @intCast(7 + depth);
    if (input < -limit or input >= limit) {
        antfly_codec_afz_vp9_fail(null, 3);
        return 0;
    }
    return input;
}

test "codec realloc shared denial preserves old allocation and accounts headers" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 300 } };
    var owner = Owner{ .budget = .{ .backing = std.testing.allocator, .limit = 4096 }, .allocations = .{ .backing = std.testing.allocator, .limit = 4096, .reservation = try pool.acquire(.{}) } };
    owner.budget.backing = owner.allocations.?.allocator();
    defer owner.sweep();
    const scope = Scope.enter(&owner);
    defer scope.deinit();
    const data = antfly_codec_afz_malloc(100) orelse return error.TestUnexpectedResult;
    const bytes = @as([*]u8, @ptrCast(data))[0..100];
    @memset(bytes, 0x5a);
    try std.testing.expectEqual(@as(u64, 164), pool.snapshot().host_bytes);
    try std.testing.expect(antfly_codec_afz_realloc(data, 150) == null);
    try std.testing.expectError(error.SharedAdmissionExceeded, owner.result());
    try std.testing.expectEqual(@as(u64, 164), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(usize, 164), owner.budget.live);
    for (bytes) |value| try std.testing.expectEqual(@as(u8, 0x5a), value);
}
