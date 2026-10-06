// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Volatile generation handshake. Durable markers and structural/apply fences
//! belong to the caller; only successful passes acknowledge captured demand.
const std = @import("std");
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);
pub const Owner = struct {
    requested: AtomicU64 = .init(0),
    completed: AtomicU64 = .init(0),
    mutex: std.atomic.Mutex = .unlocked,
    pub const Port = struct { ptr: *anyopaque, pass: *const fn (*anyopaque, std.mem.Allocator) anyerror!void };
    pub fn request(self: *Owner) void {
        _ = self.requested.fetchAdd(1, .release);
    }
    pub fn pending(self: *const Owner) bool {
        return self.completed.load(.acquire) != self.requested.load(.acquire);
    }
    pub fn drain(self: *Owner, alloc: std.mem.Allocator, port: Port) !void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        defer self.mutex.unlock();
        while (true) {
            const target = self.requested.load(.acquire);
            if (self.completed.load(.acquire) == target) return;
            try port.pass(port.ptr, alloc);
            self.completed.store(target, .release);
        }
    }
};
test "managed admission retains raced demand and failed passes" {
    const F = struct {
        owner: Owner = .{},
        calls: usize = 0,
        fail: bool = true,
        fn pass(ptr: *anyopaque, _: std.mem.Allocator) !void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.calls += 1;
            if (f.fail) return error.InjectedFailure;
            if (f.calls == 2) f.owner.request();
        }
    };
    var f: F = .{};
    const port: Owner.Port = .{ .ptr = &f, .pass = F.pass };
    f.owner.request();
    try std.testing.expectError(error.InjectedFailure, f.owner.drain(std.testing.allocator, port));
    try std.testing.expect(f.owner.pending());
    f.fail = false;
    try f.owner.drain(std.testing.allocator, port);
    try std.testing.expectEqual(@as(usize, 3), f.calls);
    try std.testing.expect(!f.owner.pending());
    try f.owner.drain(std.testing.allocator, port);
    try std.testing.expectEqual(@as(usize, 3), f.calls);
}
