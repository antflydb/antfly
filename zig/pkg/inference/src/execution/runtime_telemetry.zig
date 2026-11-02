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

//! Optional backend measurements. Counters are context cumulative; memory is
//! a gauge with explicit ownership scope. Null means unmeasured, never zero.
const std = @import("std");
pub const Counters = struct {
    kernel_dispatches: ?u64 = null,
    graph_submissions: ?u64 = null,
    frame_submissions: ?u64 = null,
    synchronizations: ?u64 = null,
    physical_allocations: ?u64 = null,
    physical_frees: ?u64 = null,

    /// A reset/unsupported counter is unknown for this interval. Concurrent
    /// submissions to the same context remain part of the context interval.
    pub fn since(self: Counters, before: Counters) Counters {
        var result: Counters = .{};
        inline for (@typeInfo(Counters).@"struct".field_names) |field| {
            if (@field(self, field)) |after| {
                if (@field(before, field)) |start| {
                    if (after >= start) @field(result, field) = after - start;
                }
            }
        }
        return result;
    }
};
pub const Memory = struct {
    scope: enum { unavailable, backend_device_buffers, backend_runtime_buffers } = .unavailable,
    live_bytes: ?u64 = null,
    /// Lifetime high-water gauge; not an invocation's peak allocation.
    lifetime_peak_bytes: ?u64 = null,
};
pub const Snapshot = struct {
    counters: Counters = .{},
    memory: Memory = .{},
    pub fn since(self: Snapshot, before: Snapshot) Snapshot {
        return .{ .counters = self.counters.since(before.counters), .memory = self.memory };
    }
};

test "runtime telemetry distinguishes unavailable reset and measured zero counters" {
    const before = Snapshot{ .counters = .{ .kernel_dispatches = 10, .graph_submissions = 4, .synchronizations = 3 }, .memory = .{ .scope = .backend_device_buffers, .live_bytes = 128, .lifetime_peak_bytes = 1024 } };
    const after = Snapshot{ .counters = .{ .kernel_dispatches = 12, .graph_submissions = 4, .synchronizations = 1 }, .memory = .{ .scope = .backend_device_buffers, .live_bytes = 64, .lifetime_peak_bytes = 1024 } };
    const delta = after.since(before);
    try std.testing.expectEqual(@as(?u64, 2), delta.counters.kernel_dispatches);
    try std.testing.expectEqual(@as(?u64, 0), delta.counters.graph_submissions);
    try std.testing.expectEqual(@as(?u64, null), delta.counters.synchronizations);
    try std.testing.expectEqual(@as(?u64, null), delta.counters.physical_allocations);
    try std.testing.expectEqual(@as(?u64, 64), delta.memory.live_bytes);
    try std.testing.expectEqual(@as(?u64, 1024), delta.memory.lifetime_peak_bytes);
}
