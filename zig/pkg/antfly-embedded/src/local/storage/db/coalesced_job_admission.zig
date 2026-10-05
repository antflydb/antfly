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
pub const State = std.atomic.Value(u8);

/// 0 idle, 1 active, 2 active with a pending notification. A failed CAS must
/// reobserve idle: the previous worker may have retired during notification.
pub fn request(state: *State) bool {
    return requestObserved(state, state.load(.acquire));
}
fn requestObserved(state: *State, initial: u8) bool {
    var observed = initial;
    while (true) {
        if (observed == 2) return false;
        const desired: u8 = if (observed == 0) 1 else 2;
        if (state.cmpxchgWeak(observed, desired, .acq_rel, .acquire)) |raced| {
            observed = raced;
            continue;
        }
        return observed == 0;
    }
}
/// Returns true when this flight retires, false when it must service demand.
pub fn settled(state: *State) bool {
    if (state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return true;
    return state.cmpxchgStrong(2, 1, .acq_rel, .acquire) != null;
}

test "coalesced admission retains active demand and admits after final idle handoff" {
    var state: State = .init(0);
    try std.testing.expect(request(&state));
    try std.testing.expect(!request(&state));
    try std.testing.expect(!request(&state));
    try std.testing.expect(!settled(&state));
    try std.testing.expect(settled(&state));
    try std.testing.expect(request(&state));
    try std.testing.expect(settled(&state));
    // Force the exact stale observation from the broken two-CAS protocol.
    state.store(1, .release);
    const observed = state.load(.acquire);
    try std.testing.expect(settled(&state));
    try std.testing.expect(requestObserved(&state, observed));
    try std.testing.expectEqual(@as(u8, 1), state.load(.acquire));
}
