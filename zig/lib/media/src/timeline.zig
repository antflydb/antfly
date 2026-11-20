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
/// Signed integer media/source ticks. No float accumulation; negative preroll
/// timestamps remain representable. Division rounds toward negative infinity.
pub fn rescale(ticks: i64, from_scale: u32, to_scale: u32) !i64 {
    if (from_scale == 0 or to_scale == 0) return error.InvalidTimebase;
    const value = @divFloor(@as(i128, ticks) * to_scale, from_scale);
    return std.math.cast(i64, value) orelse error.TimestampOverflow;
}
pub const Edit = struct {
    media_start: i64 = 0,
    source_start: i64 = 0,
    source_duration: ?i64 = null,
    pub fn present(self: Edit, media_ticks: i64) !i64 {
        const value = @as(i128, media_ticks) - self.media_start + self.source_start;
        return std.math.cast(i64, value) orelse error.TimestampOverflow;
    }
};
/// Count timed samples in the presentation edit. PTS values are already in
/// presentation coordinates and may arrive in decode order.
pub const SampleClock = struct {
    scale: u32,
    edit: Edit = .{},
    count: usize = 0,
    first: i64 = std.math.maxInt(i64),
    last: i64 = std.math.minInt(i64),

    pub fn includeInterval(self: *SampleClock, pts: i64, duration: u64) void {
        if (self.edit.source_duration != null and pts < self.edit.source_start and @as(i128, pts) + duration > self.edit.source_start) {
            self.include(self.edit.source_start);
        } else self.include(pts);
    }
    pub fn include(self: *SampleClock, pts: i64) void {
        if (self.edit.source_duration) |duration| {
            const relative = @as(i128, pts) - self.edit.source_start;
            if (relative < 0 or relative >= duration) return;
        }
        self.count += 1;
        self.first = @min(self.first, pts);
        self.last = @max(self.last, pts);
    }

    pub fn origin(self: SampleClock) i64 {
        return if (self.edit.source_duration != null) self.edit.source_start else self.first;
    }
};

test "timeline signed rational scaling preserves negative ticks and overflow" {
    try std.testing.expectEqual(@as(i64, -334), try rescale(-1, 3, 1000));
    try std.testing.expectError(error.InvalidTimebase, rescale(1, 0, 1));
    try std.testing.expectError(error.TimestampOverflow, rescale(std.math.maxInt(i64), 1, 1000));
    try std.testing.expectEqual(@as(i64, 90), try (Edit{ .media_start = 20, .source_start = 100 }).present(10));
}
