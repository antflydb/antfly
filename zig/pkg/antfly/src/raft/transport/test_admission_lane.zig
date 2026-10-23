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
const Self = @This();

/// Test-only admission fault injection backed by real std.Io tasks. The test's
/// owning task performs all admission and await calls, so counters need no
/// synchronization. Worker retirement never controls the injected refusal.
runtime: std.Io.Threaded,
vtable: std.Io.VTable,
refuse_after: ?usize,
admitted: usize = 0,
awaited: usize = 0,

pub fn init(alloc: std.mem.Allocator, refuse_after: ?usize) Self {
    var self: Self = .{
        .runtime = .init(alloc, .{ .async_limit = .nothing }),
        .vtable = undefined,
        .refuse_after = refuse_after,
    };
    self.vtable = self.runtime.io().vtable.*;
    self.vtable.concurrent = concurrent;
    self.vtable.await = awaitTask;
    return self;
}

pub fn deinit(self: *Self) void {
    self.runtime.deinit();
}

pub fn io(self: *Self) std.Io {
    // Preserve the runtime userdata for all unmodified I/O operations.
    return .{ .userdata = &self.runtime, .vtable = &self.vtable };
}

fn concurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: std.mem.Alignment,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (*const anyopaque, *anyopaque) void,
) std.Io.ConcurrentError!*std.Io.AnyFuture {
    const runtime: *std.Io.Threaded = @ptrCast(@alignCast(userdata.?));
    const self: *Self = @fieldParentPtr("runtime", runtime);
    if (self.refuse_after) |limit| if (self.admitted >= limit)
        return error.ConcurrencyUnavailable;
    const inner = runtime.io();
    const future = try inner.vtable.concurrent(inner.userdata, result_len, result_alignment, context, context_alignment, start);
    self.admitted += 1;
    return future;
}

fn awaitTask(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result: []u8, alignment: std.mem.Alignment) void {
    const runtime: *std.Io.Threaded = @ptrCast(@alignCast(userdata.?));
    const self: *Self = @fieldParentPtr("runtime", runtime);
    const inner = runtime.io();
    inner.vtable.await(inner.userdata, future, result, alignment);
    self.awaited += 1;
}
