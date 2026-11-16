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

//! Isolate deletion tombstones from live retirement count. Reports timings only;
//! correctness and probe bounds are covered by allocator_v4's churn regression.
//! From zig/: ZIG_GLOBAL_CACHE_DIR=$PWD/.zig-cache/global zig build-exe
//! -OReleaseFast --cache-dir .zig-cache --dep lite_allocator
//! -Mroot=bench/lite_retirement_churn.zig
//! -Mlite_allocator=pkg/antfly-embedded/src/storage/lite/allocator_v4.zig
const std = @import("std");
const State = @import("lite_allocator").State;
pub fn main(init: std.process.Init) !void {
    for ([_]u32{ 256, 4096 }) |reserve| {
        var state = State.init(init.gpa);
        defer state.deinit();
        try state.pending.ensureTotalCapacity(init.gpa, reserve);
        try state.retire(.{ .epoch = 1000, .page = 1, .kind = .page });
        const capacity = state.pending.capacity();
        const started = std.Io.Clock.awake.now(init.io).nanoseconds;
        for (0..@as(usize, capacity) * 16) |_| {
            try state.retire(.{ .epoch = 1, .page = 2, .kind = .page });
            try state.complete(state.eligible(1, 1).?);
            state.clean();
        }
        const churn_done = std.Io.Clock.awake.now(init.io).nanoseconds;
        var hits: usize = 0;
        for (0..20000) |i| hits += @intFromBool(state.pending.get(state.root.next_id + i) != null);
        const done = std.Io.Clock.awake.now(init.io).nanoseconds;
        std.debug.print("capacity={d} churn_ns={d} missing_20000_ns={d} hits={d}\n", .{ capacity, churn_done - started, done - churn_done, hits });
    }
}
