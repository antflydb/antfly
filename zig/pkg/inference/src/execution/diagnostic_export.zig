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

//! Numerical publication copies only owned values into a bounded process queue.
//! Export runs explicitly after the server handler has retired its numerical
//! lanes. Queue overflow is reported; it never blocks numerical publication on
//! external IO or turns another request's snapshot into the current request's.
const std = @import("std");
const Snapshot = @import("diagnostics.zig").Snapshot;
pub const Record = struct { owner: u64, snapshot: Snapshot };
const capacity = 8;
var mutex: std.atomic.Mutex = .unlocked;
var records: [capacity]Record = undefined;
var head: usize = 0;
var count: usize = 0;
var dropped: u64 = 0;
fn lock() void {
    @import("antfly_platform").sync.lockYielding(&mutex);
}
pub fn ownerIdentity() u64 {
    return @import("source_identity.zig").next();
}
pub fn enqueue(owner: u64, snapshot: Snapshot) void {
    lock();
    defer mutex.unlock();
    if (count == capacity) {
        dropped +|= 1;
        return;
    }
    records[(head + count) % capacity] = .{ .owner = owner, .snapshot = snapshot.owned() };
    count += 1;
}
/// Caller guarantees it holds no model/provider/registry/diagnostics scope.
/// Each callback receives a self-contained value after the queue lock releases.
pub fn drain(comptime consume: anytype, context: anytype) void {
    // Bounded even if other requests continue publishing during external IO.
    for (0..capacity) |_| {
        lock();
        if (count == 0) {
            mutex.unlock();
            break;
        }
        const value = records[head];
        head = (head + 1) % capacity;
        count -= 1;
        mutex.unlock();
        consume(context, value);
    }
}
fn logRecord(_: void, record: Record) void {
    const bytes = std.json.Stringify.valueAlloc(std.heap.page_allocator, record, .{}) catch {
        std.log.warn("execution_diagnostics_export unavailable=OutOfMemory", .{});
        return;
    };
    defer std.heap.page_allocator.free(bytes);
    std.log.info("execution_diagnostics {s}", .{bytes});
}
pub fn flush() void {
    drain(logRecord, {});
    lock();
    const lost = dropped;
    dropped = 0;
    mutex.unlock();
    if (lost != 0) std.log.warn("execution_diagnostics_export dropped={d}", .{lost});
}

test "diagnostic export copies failure values and releases its queue before sinks" {
    const Probe = struct {
        calls: usize = 0,
        fn accept(self: *@This(), value: Record) void {
            std.debug.assert(mutex.tryLock());
            mutex.unlock();
            std.debug.assert(value.owner == 71);
            std.debug.assert(value.snapshot.outcome == .failed);
            std.debug.assert(std.mem.eql(u8, value.snapshot.error_name.?.view(), "Cancelled"));
            self.calls += 1;
        }
    };
    var probe = Probe{};
    enqueue(71, .{ .outcome = .failed, .error_name = @import("diagnostics.zig").Text.init("Cancelled") });
    drain(Probe.accept, &probe);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    drain(Probe.accept, &probe);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
