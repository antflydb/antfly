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

const std = @import("std");
const types = @import("types.zig");
/// Direct writes use bulk store admission while this session is active.
/// The legacy coalescing statistics remain wire-compatible; staging is unused.
pub const State = struct {
    active: bool = false,
    active_session: std.atomic.Value(u8) = .init(0),
    pub fn begin(self: *State) void {
        self.active = true;
        self.active_session.store(1, .monotonic);
    }
    pub fn finish(self: *State) void {
        self.active = false;
        self.active_session.store(0, .monotonic);
    }
    pub fn snapshot(self: *const State) types.BulkCoalescingStats {
        return .{ .active_session = self.active_session.load(.monotonic) != 0 };
    }
};
test "bulk session retains active admission and legacy statistics" {
    var state: State = .{};
    try std.testing.expect(!state.snapshot().active_session);
    state.begin();
    try std.testing.expect(state.active and state.snapshot().active_session);
    try std.testing.expectEqual(@as(u64, 0), state.snapshot().staged_keys);
    try std.testing.expectEqual(@as(u64, 0), state.snapshot().flush_calls);
    state.finish();
    try std.testing.expect(!state.active and !state.snapshot().active_session);
}
