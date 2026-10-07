// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

const platform = @import("antfly_platform");
const std = @import("std");

/// Await mutation work without forwarding caller I/O cancellation to it.
/// The work still observes its borrowed request token at its own safe points.
pub fn wait(io: std.Io, future: *std.Io.Future(void)) void {
    const previous = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(previous);
    future.await(io);
}

test "caller cancellation does not interrupt protected child work" {
    var pool = platform.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer pool.deinit();
    const io = pool.io();
    const State = struct {
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        canceled: bool = false,
        finished: bool = false,

        fn child(i: std.Io, self: *@This()) void {
            self.started.set(i);
            self.release.wait(i) catch {
                self.canceled = true;
                return;
            };
            self.finished = true;
        }
        fn parent(i: std.Io, self: *@This()) void {
            var future = i.concurrent(child, .{ i, self }) catch @panic("test executor unavailable");
            wait(i, &future);
        }
        fn releaseChild(i: std.Io, self: *@This()) void {
            i.sleep(.fromMilliseconds(50), .awake) catch {};
            self.release.set(i);
        }
    };
    var state = State{};
    var parent = try io.concurrent(State.parent, .{ io, &state });
    try state.started.wait(io);
    var release = try io.concurrent(State.releaseChild, .{ io, &state });
    parent.cancel(io);
    release.await(io);
    try std.testing.expect(!state.canceled);
    try std.testing.expect(state.finished);
}
