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

//! Presence-aware operator policy, independent of environment access and GPUs.
const std = @import("std");

pub const Policy = struct {
    cuda_enabled: bool,
    rows: usize,
    window: usize,

    pub const Defaults = struct {
        cuda_available: bool,
        cuda_enabled: bool,
        rows: usize,
        row_ceiling: usize,
        window: usize,
        window_ceiling: usize,
        window_tracks_rows: bool = false,
    };
    pub const Overrides = struct {
        cuda_enabled: ?bool = null,
        rows: ?usize = null,
        window: ?usize = null,
    };

    pub fn resolve(defaults: Defaults, overrides: Overrides) Policy {
        const rows = std.math.clamp(overrides.rows orelse defaults.rows, 1, @max(defaults.row_ceiling, 1));
        const window = overrides.window orelse if (defaults.window_tracks_rows) rows *| 2 else defaults.window;
        return .{
            .cuda_enabled = defaults.cuda_available and (overrides.cuda_enabled orelse defaults.cuda_enabled),
            .rows = rows,
            .window = std.math.clamp(window, 1, @max(defaults.window_ceiling, 1)),
        };
    }
};

test "reader batching explicit rollback overrides canonical settings" {
    const defaults = Policy.Defaults{ .cuda_available = true, .cuda_enabled = true, .rows = 4, .row_ceiling = 16, .window = 8, .window_ceiling = 128, .window_tracks_rows = true };
    const automatic = Policy.resolve(defaults, .{});
    try std.testing.expect(automatic.cuda_enabled);
    try std.testing.expectEqual(@as(usize, 4), automatic.rows);
    try std.testing.expectEqual(@as(usize, 8), automatic.window);
    const rollback = Policy.resolve(defaults, .{ .cuda_enabled = false, .rows = 1, .window = 1 });
    try std.testing.expect(!rollback.cuda_enabled);
    try std.testing.expectEqual(@as(usize, 1), rollback.rows);
    try std.testing.expectEqual(@as(usize, 1), rollback.window);
    try std.testing.expectEqual(@as(usize, 2), Policy.resolve(defaults, .{ .rows = 1 }).window);
    const bounded = Policy.resolve(defaults, .{ .rows = 10000, .window = 10000 });
    try std.testing.expectEqual(@as(usize, 16), bounded.rows);
    try std.testing.expectEqual(@as(usize, 128), bounded.window);
    try std.testing.expectEqual(@as(usize, 1), Policy.resolve(defaults, .{ .rows = 0, .window = 0 }).rows);
    var unavailable = defaults;
    unavailable.cuda_available = false;
    try std.testing.expect(!Policy.resolve(unavailable, .{ .cuda_enabled = true }).cuda_enabled);
}
