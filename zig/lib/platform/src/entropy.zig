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

extern "env" fn antfly_platform_random_secure(ptr: [*]u8, len: usize) u32;

/// Preserve the borrowed native I/O authority; browser hosts supply fresh
/// cryptographic entropy and report failure instead of a predictable fallback.
pub fn fill(io: std.Io, buffer: []u8) std.Io.RandomSecureError!void {
    if (builtin.os.tag == .freestanding and
        (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64))
    {
        if (antfly_platform_random_secure(buffer.ptr, buffer.len) != 0) return error.EntropyUnavailable;
        return;
    }
    return io.randomSecure(buffer);
}

test "secure entropy preserves the caller's native I/O authority and errors" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Host = struct {
        fn random(userdata: ?*anyopaque, bytes: []u8) std.Io.RandomSecureError!void {
            const calls: *usize = @ptrCast(@alignCast(userdata.?));
            calls.* += 1;
            @memset(bytes, 0x44);
        }
    };
    var calls: usize = 0;
    const failing: std.Io = .failing;
    var vtable = failing.vtable.*;
    vtable.randomSecure = Host.random;
    const io: std.Io = .{ .userdata = &calls, .vtable = &vtable };
    var bytes: [16]u8 = @splat(0);
    try fill(io, &bytes);
    try std.testing.expectEqual(@as(usize, 1), calls);
    for (bytes) |byte| try std.testing.expectEqual(@as(u8, 0x44), byte);
    try std.testing.expectError(error.EntropyUnavailable, fill(.failing, &bytes));
}
