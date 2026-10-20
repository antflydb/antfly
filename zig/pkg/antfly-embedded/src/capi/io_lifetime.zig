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

//! Pin Zig's process signal owner while foreign handles own executors.
//! Threaded restores handlers per instance, so independent close order needs
//! an outer lifetime whose handlers all per-handle executors can safely restore.
const std = @import("std");
const builtin = @import("builtin");
const supported = !builtin.single_threaded and switch (builtin.os.tag) {
    .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
    else => false,
};
var mutex: std.Io.Mutex = .init;
var references: usize = 0;
var owner: std.Io.Threaded = undefined;

pub fn acquire() void {
    if (comptime !supported) return;
    mutex.lockUncancelable(std.Io.Threaded.global_single_threaded.io());
    defer mutex.unlock(std.Io.Threaded.global_single_threaded.io());
    if (references == 0) owner = std.Io.Threaded.init(std.heap.page_allocator, .{ .async_limit = .nothing, .concurrent_limit = .nothing });
    references += 1;
}

pub fn release() void {
    if (comptime !supported) return;
    mutex.lockUncancelable(std.Io.Threaded.global_single_threaded.io());
    defer mutex.unlock(std.Io.Threaded.global_single_threaded.io());
    std.debug.assert(references != 0);
    references -= 1;
    // This owner never dispatches work. All real workers have joined before
    // their handle's lease ends, so the final restore cannot interrupt a peer.
    if (references == 0) owner.deinit();
}

test "capi foreign executor lifetime retains signal owner across out of order closes" {
    if (comptime !supported or builtin.os.tag == .windows) return error.SkipZigTest;
    const ignored: std.posix.Sigaction = .{ .handler = .{ .handler = std.posix.SIG.IGN }, .mask = std.posix.sigemptyset(), .flags = 0 };
    var original: std.posix.Sigaction = undefined;
    std.posix.sigaction(.IO, &ignored, &original);
    defer std.posix.sigaction(.IO, &original, null);
    acquire();
    var first = std.Io.Threaded.init(std.testing.allocator, .{});
    acquire();
    var second = std.Io.Threaded.init(std.testing.allocator, .{});
    first.deinit();
    release();
    var active: std.posix.Sigaction = undefined;
    std.posix.sigaction(.IO, null, &active);
    const retained = active.handler.handler != std.posix.SIG.IGN;
    second.deinit();
    release();
    try std.testing.expect(retained);
    std.posix.sigaction(.IO, null, &active);
    try std.testing.expectEqual(std.posix.SIG.IGN, active.handler.handler);
}
