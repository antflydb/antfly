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

//! Opt-in phase timings for expensive correctness fixtures. No latency assertions.
const std = @import("std");
const platform = @import("antfly_platform");

pub fn Profile(comptime Phase: type) type {
    return struct {
        const Self = @This();
        enabled: bool,
        last: u64,
        elapsed: [@typeInfo(Phase).@"enum".fields.len]u64 = @splat(0),

        pub fn init() Self {
            const enabled = platform.env.getenvBool("ANTFLY_TEST_WORK_PROFILE");
            return .{ .enabled = enabled, .last = if (enabled) platform.time.monotonicNs() else 0 };
        }

        pub fn mark(self: *Self, phase: Phase) void {
            if (!self.enabled) return;
            const now = platform.time.monotonicNs();
            self.elapsed[@intFromEnum(phase)] += now -| self.last;
            self.last = now;
        }

        pub fn report(self: *const Self, label: []const u8) void {
            if (!self.enabled) return;
            std.debug.print("\nWORK {s}", .{label});
            inline for (@typeInfo(Phase).@"enum".fields, 0..) |field, i|
                std.debug.print(" {s}_ms={d}", .{ field.name, self.elapsed[i] / std.time.ns_per_ms });
            std.debug.print("\n", .{});
        }
    };
}
