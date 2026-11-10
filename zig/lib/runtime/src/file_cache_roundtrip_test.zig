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

//! This archive creates the executor; an independent consumer uses its native
//! file advice through the checked executor borrow, never a function identity.
const std = @import("std");
const builtin = @import("builtin");
const executor = @import("runtime_io_abi.zig");
fn provide(out: *executor.Borrow) callconv(.c) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    out.* = executor.Borrow.init(&io);
}
comptime {
    if (!builtin.is_test) @export(&provide, .{ .name = "antfly_test_file_cache_executor", .visibility = .hidden });
}
extern fn antfly_test_file_cache_executor(*executor.Borrow) callconv(.c) void;

test "file cache capability survives independently compiled I/O owners" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var borrow: executor.Borrow = undefined;
    antfly_test_file_cache_executor(&borrow);
    var received = try borrow.receive();
    const io = received.io();
    try std.testing.expect(borrow.file_cache.available());
    // A nested borrow must forward the original capability and dispatcher.
    const nested = executor.Borrow.init(&io);
    try std.testing.expect(nested.file_cache.available());
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const file = try dir.dir.createFile(io, "preserved", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "preserved data");
    try file.sync(io);
    nested.file_cache.discard(io, file, 14);
    var data: [14]u8 = undefined;
    _ = try file.readPositionalAll(io, &data, 0);
    try std.testing.expectEqualStrings("preserved data", &data);
}
