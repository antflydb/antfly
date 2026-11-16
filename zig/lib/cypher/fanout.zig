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
/// Every wave joins before its slots, allocator, or authority can be released.
/// Returns true when the task group was canceled. Already-started workers
/// always finish before return; subsequent waves are not dispatched.
pub fn run(io: ?std.Io, width: usize, count: usize, context: anytype, comptime callback: anytype) bool {
    return runImpl(io, width, count, context, callback, false);
}
/// Cleanup ignores cancellation as a dispatch stop: every retained capability
/// must receive its release callback even when native I/O has been canceled.
pub fn finish(io: ?std.Io, width: usize, count: usize, context: anytype, comptime callback: anytype) void {
    _ = runImpl(io, width, count, context, callback, true);
}
fn runImpl(io: ?std.Io, width: usize, count: usize, context: anytype, comptime callback: anytype, cleanup: bool) bool {
    const bound = @max(1, @min(width, 16));
    if (io == null or bound == 1) {
        for (0..count) |index| callback(context, index);
        return false;
    }
    var canceled = false;
    var start: usize = 0;
    while (start < count) : (start += bound) {
        var group: std.Io.Group = .init;
        for (start..@min(start + bound, count)) |index| group.async(io.?, callback, .{ context, index });
        group.await(io.?) catch {
            canceled = true;
        };
        if (canceled and !cleanup) break;
    }
    return canceled;
}
/// The lock covers allocations, never remote waits or query work. Hosts retain
/// this wrapper until every capability allocated through it has been closed.
pub const Allocator = struct {
    backing: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    quota_exhausted: std.atomic.Value(bool) = .init(false),
    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }
    fn allocate(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const result = self.backing.rawAlloc(len, alignment, ra);
        if (result == null and @import("budget.zig").allocatorExhausted(self.backing)) self.quota_exhausted.store(true, .release);
        return result;
    }
    fn resize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const result = self.backing.rawResize(memory, alignment, len, ra);
        if (!result and @import("budget.zig").allocatorExhausted(self.backing)) self.quota_exhausted.store(true, .release);
        return result;
    }
    fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const result = self.backing.rawRemap(memory, alignment, len, ra);
        if (result == null and @import("budget.zig").allocatorExhausted(self.backing)) self.quota_exhausted.store(true, .release);
        return result;
    }
    fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.backing.rawFree(memory, alignment, ra);
    }
};

/// Some retained capabilities have controller-thread affinity. Only eligible
/// entries move to workers; local callbacks keep the original calling thread.
pub fn selected(io: ?std.Io, width: usize, count: usize, context: anytype, comptime callback: anytype, comptime eligible: anytype, cleanup: bool) bool {
    if (io == null or width <= 1 or count > 1024) {
        for (0..count) |index| callback(context, index);
        return false;
    }
    var indices: [1024]usize = undefined;
    var length: usize = 0;
    for (0..count) |index| {
        if (eligible(context, index)) {
            indices[length] = index;
            length += 1;
        } else callback(context, index);
    }
    const Dispatch = struct {
        context: @TypeOf(context),
        indices: []const usize,
        fn invoke(self: *@This(), offset: usize) void {
            callback(self.context, self.indices[offset]);
        }
    };
    var dispatch: Dispatch = .{ .context = context, .indices = indices[0..length] };
    if (cleanup) {
        finish(io, width, length, &dispatch, Dispatch.invoke);
        return false;
    }
    return run(io, width, length, &dispatch, Dispatch.invoke);
}
