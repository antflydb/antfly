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

/// Per-process, per-path closed-lease telemetry. Coordination and kernel wait
/// time precede acquisition; hold time includes registration cleanup and ends
/// when the safety flock closes. Snapshots are observational under concurrency.
pub const Stats = struct {
    const Counter = std.atomic.Value(u64);
    leases: Counter = .init(0),
    admission_ns: Counter = .init(0),
    admission_max_ns: Counter = .init(0),
    hold_ns: Counter = .init(0),
    hold_max_ns: Counter = .init(0),
    hold_log2_ns: [64]Counter = @splat(.init(0)),

    pub const Snapshot = struct {
        leases: u64 = 0,
        admission_ns: u64 = 0,
        admission_max_ns: u64 = 0,
        hold_ns: u64 = 0,
        hold_max_ns: u64 = 0,
        hold_log2_ns: [64]u64 = @splat(0),
    };

    pub fn record(self: *Stats, admission_ns: u64, hold_ns: u64) void {
        _ = self.admission_ns.fetchAdd(admission_ns, .monotonic);
        _ = self.admission_max_ns.fetchMax(admission_ns, .monotonic);
        _ = self.hold_ns.fetchAdd(hold_ns, .monotonic);
        _ = self.hold_max_ns.fetchMax(hold_ns, .monotonic);
        _ = self.hold_log2_ns[std.math.log2_int(u64, @max(1, hold_ns))].fetchAdd(1, .monotonic);
        _ = self.leases.fetchAdd(1, .release);
    }

    pub fn snapshot(self: *const Stats) Snapshot {
        var result: Snapshot = .{
            .leases = self.leases.load(.acquire),
            .admission_ns = self.admission_ns.load(.monotonic),
            .admission_max_ns = self.admission_max_ns.load(.monotonic),
            .hold_ns = self.hold_ns.load(.monotonic),
            .hold_max_ns = self.hold_max_ns.load(.monotonic),
        };
        for (&result.hold_log2_ns, 0..) |*count, index| count.* = self.hold_log2_ns[index].load(.monotonic);
        return result;
    }
};
