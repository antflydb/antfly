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

const native_platform = @import("antfly_platform");
const std = @import("std");

/// Local projection supervision. The borrowed round owns catalog pins and publication checks.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, io: std.Io, closing: *const std.atomic.Value(bool), round: *const fn (*anyopaque) anyerror!bool };
    enabled: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    wake: std.Io.Event = .unset,
    future: ?std.Io.Future(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),
    port: Port = undefined,
    pub fn schedule(self: *Owner, port: Port) void {
        if (!self.enabled or self.stopping.load(.acquire) or port.closing.load(.acquire)) return;
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or port.closing.load(.acquire)) return;
        if (self.future == null) {
            self.port = port;
            self.future = port.io.concurrent(run, .{self}) catch |err| {
                std.log.warn("native projection maintenance admission deferred err={s}", .{@errorName(err)});
                return;
            };
        }
        self.wake.set(port.io);
    }
    pub fn stop(self: *Owner, io: std.Io) void {
        self.stopping.store(true, .release);
        while (!self.mutex.tryLock()) native_platform.time.yieldNow();
        var future = self.future;
        self.future = null;
        self.mutex.unlock();
        self.wake.set(io);
        if (future) |*task| task.await(io);
    }
    fn closing(self: *Owner) bool {
        return self.stopping.load(.acquire) or self.port.closing.load(.acquire);
    }
    fn run(self: *Owner) void {
        const port = self.port;
        while (!self.closing()) {
            self.wake.waitUncancelable(port.io);
            self.wake.reset();
            while (!self.closing()) {
                const pending = port.round(port.ptr) catch |err| blk: {
                    std.log.warn("native projection maintenance deferred err={s}", .{@errorName(err)});
                    break :blk retryable(err);
                };
                if (!pending) break;
                port.io.sleep(.fromMilliseconds(50), .awake) catch return;
            }
        }
    }
    pub fn retryable(err: anyerror) bool {
        return switch (err) {
            error.ResourceBudgetExceeded, error.OutOfMemory, error.WriterLocked, error.PostingCheckpointCaptureActive, error.PostingCheckpointSequenceMismatch, error.VectorBlockSnapshotAdvancedWithoutWal, error.VectorBlockGenerationReservationLost => true,
            else => false,
        };
    }
};

test "native projection owner joins observation and rejects admission after shutdown" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const F = struct {
        closing: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn round(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.closing.store(true, .release);
            return false;
        }
    };
    var f: F = .{};
    var owner: Owner = .{ .enabled = true };
    defer owner.stop(native_platform.testing.io);
    const port: Owner.Port = .{ .ptr = &f, .io = native_platform.testing.io, .closing = &f.closing, .round = F.round };
    owner.schedule(port);
    const deadline = native_platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!f.closing.load(.acquire) and native_platform.time.monotonicNs() < deadline)
        native_platform.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
    try std.testing.expect(f.closing.load(.acquire));
    owner.stop(native_platform.testing.io);
    f.closing.store(false, .release);
    owner.schedule(port);
    try std.testing.expect(owner.future == null);
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expect(Owner.retryable(error.ResourceBudgetExceeded));
    try std.testing.expect(!Owner.retryable(error.ArtifactCatalogCorrupt));
}
