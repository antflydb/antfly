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

//! Bound optional control work by useful work and time, rather than making
//! every empty service consume a separate native capture/publication turn.
const std = @import("std");

pub const service_count = 9;

/// `step` reports work or a continuation. Yield immediately in that case;
/// otherwise check the next service within this turn's time budget. The
/// persistent cursor advances only after a successful check. One complete
/// unchanged sweep is still required before declaring this lane idle.
pub fn run(state: anytype, context: anytype, comptime step: anytype, comptime now: anytype, budget_ns: u64) !bool {
    std.debug.assert(state.control_maintenance_slot < service_count);
    const started = now(context);
    state.control_maintenance_pending = true;
    for (0..service_count) |_| {
        const slot = state.control_maintenance_slot;
        const more = try step(context, slot);
        state.control_maintenance_changed = state.control_maintenance_changed or more;
        state.control_maintenance_slot = (slot + 1) % service_count;
        if (state.control_maintenance_slot == 0) {
            const changed = state.control_maintenance_changed;
            state.control_maintenance_changed = false;
            state.control_maintenance_pending = changed;
            return changed;
        }
        // Always check at least one service, even when setup consumed the
        // quantum. Empty checks can coalesce; real work keeps its old yield.
        if (more or now(context) -| started >= budget_ns) return true;
    }
    unreachable; // Every starting cursor reaches the boundary within 9 checks.
}

const State = struct {
    control_maintenance_slot: u4 = 0,
    control_maintenance_changed: bool = false,
    control_maintenance_pending: bool = false,
};

const Probe = struct {
    elapsed: u64 = 0,
    cost: u64 = 0,
    calls: usize = 0,
    visited: [service_count]usize = @splat(0),
    useful: ?u4 = null,
    interrupted: ?u4 = null,

    fn step(self: *Probe, slot: u4) !bool {
        self.calls += 1;
        self.visited[slot] += 1;
        if (self.interrupted == slot) return error.Interrupted;
        self.elapsed += self.cost;
        return self.useful == slot;
    }
    fn now(self: *Probe) u64 {
        return self.elapsed;
    }
};

test "empty control services finish in one bounded capture" {
    var state: State = .{};
    var probe: Probe = .{ .cost = 1 };
    try std.testing.expect(!try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(service_count, probe.calls);
    try std.testing.expectEqual(@as(u4, 0), state.control_maintenance_slot);
    try std.testing.expect(!state.control_maintenance_pending);
}

test "useful control work yields and requires another clean sweep" {
    var state: State = .{};
    var probe: Probe = .{ .useful = 2 };
    try std.testing.expect(try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(u4, 3), state.control_maintenance_slot);
    try std.testing.expectEqual(@as(usize, 0), probe.visited[3]);
    probe.useful = null;
    try std.testing.expect(try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(service_count, probe.calls);
    try std.testing.expect(state.control_maintenance_pending);
    try std.testing.expect(!try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(2 * service_count, probe.calls);
    try std.testing.expect(!state.control_maintenance_pending);
}

test "control quantum resumes without repeating successful checks" {
    var state: State = .{};
    var probe: Probe = .{ .cost = 10 };
    try std.testing.expect(try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(u4, 3), state.control_maintenance_slot);
    try std.testing.expect(try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(u4, 6), state.control_maintenance_slot);
    try std.testing.expect(!try run(&state, &probe, Probe.step, Probe.now, 25));
    for (probe.visited) |count| try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expect(!state.control_maintenance_pending);
}

test "interrupted control check cannot establish a clean sweep" {
    var state: State = .{};
    var probe: Probe = .{ .interrupted = 3 };
    try std.testing.expectError(error.Interrupted, run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(u4, 3), state.control_maintenance_slot);
    try std.testing.expect(state.control_maintenance_pending);
    try std.testing.expectEqual(@as(usize, 0), probe.visited[4]);
    probe.interrupted = null;
    try std.testing.expect(!try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(usize, 2), probe.visited[3]);
    try std.testing.expect(!state.control_maintenance_pending);
}

test "expired control quantum still checks one service" {
    var state: State = .{};
    var probe: Probe = .{};
    try std.testing.expect(try run(&state, &probe, Probe.step, Probe.now, 0));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(@as(u4, 1), state.control_maintenance_slot);
}

test "control tail ends at its existing sweep boundary" {
    var state: State = .{ .control_maintenance_slot = 8 };
    var probe: Probe = .{};
    try std.testing.expect(!try run(&state, &probe, Probe.step, Probe.now, 25));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(@as(u4, 0), state.control_maintenance_slot);
}
