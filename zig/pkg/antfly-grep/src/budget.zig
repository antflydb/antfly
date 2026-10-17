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
pub const Limits = struct {
    spill_bytes: std.atomic.Value(usize) = .init(0),
    pending_output: std.atomic.Value(usize) = .init(0),
    active: std.atomic.Value(usize) = .init(0),
    lanes: std.atomic.Value(usize) = .init(0),
    pause_until_ms: std.atomic.Value(i64) = .init(0),
    requests: std.atomic.Value(usize) = .init(0),
    regions: std.atomic.Value(usize) = .init(0),
    source_bytes: std.atomic.Value(usize) = .init(0),
    cancel: std.atomic.Value(bool) = .init(false),
    pub fn acquire(self: *Limits, io: std.Io, max: usize) !void {
        _ = self.lanes.cmpxchgStrong(0, @max(1, max), .monotonic, .monotonic);
        while (true) {
            if (self.cancel.load(.monotonic)) return error.Canceled;
            const now: i64 = @intCast(@divFloor(std.Io.Clock.awake.now(io).nanoseconds, 1_000_000));
            if (now >= self.pause_until_ms.load(.monotonic)) {
                const active = self.active.load(.monotonic);
                if (active < self.lanes.load(.monotonic) and self.active.cmpxchgWeak(active, active + 1, .monotonic, .monotonic) == null) return;
            }
            try std.Io.sleep(io, .fromMilliseconds(5), .awake);
        }
    }
    pub fn release(self: *Limits) void {
        _ = self.active.fetchSub(1, .monotonic);
    }
    pub fn throttle(self: *Limits, io: std.Io, delay_ms: u64) void {
        var old = self.lanes.load(.monotonic);
        while (old > 1) {
            if (self.lanes.cmpxchgWeak(old, @max(1, old / 2), .monotonic, .monotonic)) |actual| old = actual else break;
        }
        const now: i64 = @intCast(@divFloor(std.Io.Clock.awake.now(io).nanoseconds, 1_000_000));
        _ = self.pause_until_ms.fetchMax(now + @as(i64, @intCast(delay_ms)), .monotonic);
    }
    pub fn recover(self: *Limits, max: usize) void {
        var old = self.lanes.load(.monotonic);
        while (old < @max(1, max)) {
            if (self.lanes.cmpxchgWeak(old, old + 1, .monotonic, .monotonic)) |actual| old = actual else break;
        }
    }
    pub fn reserve(counter: *std.atomic.Value(usize), n: usize, max: usize) bool {
        var old = counter.load(.monotonic);
        while (true) {
            if (n > max - old) return false;
            if (counter.cmpxchgWeak(old, old + n, .monotonic, .monotonic)) |actual| old = actual else return true;
        }
    }
};
