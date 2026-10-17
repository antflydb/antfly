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

//! Stable DASH cursor across period-local segment-number resets and live windows.
const std = @import("std");
const hls = @import("hls.zig");
pub fn compatible(old: hls.DashPeriod, new: hls.DashPeriod) bool {
    return old.compatible(new);
}
pub fn sameTiming(left: hls.DashTiming, right: hls.DashTiming) bool {
    return compatible(left.period, right.period) and left.start_ticks == right.start_ticks and left.duration_ticks == right.duration_ticks and left.timescale == right.timescale and left.presentation_offset == right.presentation_offset;
}
pub fn order(left: hls.Segment, right: hls.Segment) std.math.Order {
    const period = std.math.order(left.dash.?.period.start_ms, right.dash.?.period.start_ms);
    return if (period == .eq) std.math.order(left.sequence, right.sequence) else period;
}
pub const Cursor = struct {
    period: hls.DashPeriod,
    sequence: u64,
    last: ?hls.DashTiming = null,
    pub fn initial(segment: hls.Segment) Cursor {
        return .{ .period = segment.dash.?.period, .sequence = segment.sequence };
    }
    pub fn consumed(self: *Cursor, segment: hls.Segment) void {
        self.period = segment.dash.?.period;
        self.sequence = segment.sequence + 1;
        self.last = segment.dash.?;
    }
    pub fn refresh(self: *Cursor, playlist: hls.Playlist) void {
        for (playlist.dash_periods) |period| if (std.meta.eql(period.identity, self.period.identity)) {
            self.period = period;
            if (self.last) |*last| last.period = period;
            return;
        };
    }
    /// Pure validation: failed reload/fetch never advances this cursor.
    pub fn select(self: Cursor, playlist: hls.Playlist) !?usize {
        var current: ?hls.DashPeriod = null;
        var following: ?hls.DashPeriod = null;
        for (playlist.dash_periods) |period| {
            if (std.meta.eql(period.identity, self.period.identity)) {
                if (!compatible(self.period, period)) return error.SourceVersionChanged;
                current = period;
            } else if (period.start_ms == self.period.start_ms) return error.SourceVersionChanged;
            if (period.start_ms > self.period.start_ms and following == null) following = period;
        }
        var last = self.last;
        if (current) |period| if (last) |*timing| {
            timing.period = period;
            if (period.duration_ms) |duration| {
                const start = timing.presentationStart();
                if (start.numerator >= (@as(i128, period.start_ms) + duration) * start.denominator) return error.SourceVersionChanged;
            }
        };
        var last_sequence: ?u64 = null;
        for (playlist.segments, 0..) |segment, index| {
            if (!std.meta.eql(segment.dash.?.period.identity, self.period.identity)) continue;
            if (segment.sequence == self.sequence) return index;
            if (segment.sequence > self.sequence) return error.MediaStreamGap;
            last_sequence = segment.sequence;
        }
        if (last_sequence) |sequence| if (self.sequence > sequence + 1) return error.SourceVersionChanged;
        if (last == null or !last.?.finishesPeriod()) {
            if (current == null) return error.MediaStreamGap;
            // A clock may expose no complete segment in a future period. An
            // already-consumed live window can also temporarily be empty.
            for (playlist.segments) |segment| if (segment.dash.?.period.start_ms > self.period.start_ms) return error.MediaStreamGap;
            if (playlist.ended) return error.MediaStreamGap;
            if (playlist.dash_window_start_ms) |cutoff| {
                if (last) |timing| {
                    const end = timing.presentationEnd();
                    if (end.numerator < cutoff * end.denominator) return error.MediaStreamGap;
                } else if (current.?.duration_ms) |duration| {
                    if (@as(i128, current.?.start_ms) + duration <= cutoff) return error.MediaStreamGap;
                }
            }
            return null;
        }
        const next = following orelse {
            if (current == null) return error.SourceVersionChanged;
            return null;
        };
        const duration = last.?.period.duration_ms orelse return error.MediaStreamGap;
        const end = std.math.add(u64, last.?.period.start_ms, duration) catch return error.SourceVersionChanged;
        if (last.?.period.next_start_ms) |announced| {
            if (next.start_ms != announced or !std.meta.eql(last.?.period.next_identity, @as(?[32]u8, next.identity))) return error.MediaStreamGap;
        } else if (next.start_ms != end) return error.MediaStreamGap;
        for (playlist.segments, 0..) |segment, index| if (std.meta.eql(segment.dash.?.period.identity, next.identity)) {
            if (!segment.dash.?.startsPeriod()) return error.MediaStreamGap;
            return index;
        };
        // Do not skip a period just because none of its segments are in the
        // window anymore. A genuinely future period waits for its first segment.
        for (playlist.segments) |segment| if (segment.dash.?.period.start_ms > next.start_ms) return error.MediaStreamGap;
        return null;
    }
};
