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

//! Attention-output ownership across the decoder's fallible residual gather.
const std = @import("std");

pub fn add(cb: anytype, mixed: anytype, hidden: @TypeOf(mixed), prune: bool, gather: anytype) !@TypeOf(mixed) {
    defer cb.free(mixed);
    const residual = if (prune) try gather.call(hidden) else hidden;
    defer if (residual != hidden) cb.free(residual);
    return cb.add(mixed, residual);
}

const Probe = struct {
    const Tensor = enum { hidden, mixed, gathered, output };
    fail_gather: bool = false,
    fail_add: bool = false,
    freed: [4]usize = @splat(0),
    gathered: usize = 0,

    pub fn call(self: *Probe, input: Tensor) !Tensor {
        try std.testing.expectEqual(Tensor.hidden, input);
        self.gathered += 1;
        if (self.fail_gather) return error.InjectedGatherFailure;
        return .gathered;
    }

    pub fn free(self: *Probe, tensor: Tensor) void {
        self.freed[@backingInt(tensor)] += 1;
    }

    pub fn add(self: *Probe, mixed: Tensor, residual: Tensor) !Tensor {
        try std.testing.expectEqual(Tensor.mixed, mixed);
        try std.testing.expect(residual == .hidden or residual == .gathered);
        // Inputs must remain live until the addition finishes.
        try std.testing.expectEqualSlices(usize, &.{ 0, 0, 0, 0 }, &self.freed);
        if (self.fail_add) return error.InjectedAddFailure;
        return .output;
    }
};

test "GLiDE residual frees attention when the pruned gather fails" {
    var probe = Probe{ .fail_gather = true };
    try std.testing.expectError(error.InjectedGatherFailure, add(&probe, Probe.Tensor.mixed, .hidden, true, &probe));
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 0, 0 }, &probe.freed);
}

test "GLiDE residual releases inputs on addition failure and before feed-forward work" {
    for ([_]bool{ false, true }) |prune| {
        for ([_]bool{ false, true }) |fail| {
            var probe = Probe{ .fail_add = fail };
            if (fail) {
                try std.testing.expectError(error.InjectedAddFailure, add(&probe, Probe.Tensor.mixed, .hidden, prune, &probe));
            } else {
                try std.testing.expectEqual(Probe.Tensor.output, try add(&probe, Probe.Tensor.mixed, .hidden, prune, &probe));
            }
            try std.testing.expectEqual(@as(usize, @intFromBool(prune)), probe.gathered);
            try std.testing.expectEqualSlices(usize, &.{ 0, 1, @intFromBool(prune), 0 }, &probe.freed);
        }
    }
}
