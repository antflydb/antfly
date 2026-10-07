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
const abi = @import("antfly_executor_abi");

/// Compile this in a separate archive from executor_bridge_host.zig.
export fn exerciseBorrowedExecutor(borrow: *const abi.Borrow) callconv(.c) bool {
    var receiver = borrow.receive() catch return false;
    const io = receiver.io();
    const Job = struct {
        fn run(count: *std.atomic.Value(usize)) void {
            _ = count.fetchAdd(1, .release);
        }
    };
    var count = std.atomic.Value(usize).init(0);
    for (0..64) |_| {
        // Let the owning archive's workers park before every dispatch. Busy
        // background tasks can conceal a foreign vtable's missed wakeups.
        io.sleep(.fromMilliseconds(100), .awake) catch return false;
        var future = io.concurrent(Job.run, .{&count}) catch return false;
        const previous = io.swapCancelProtection(.blocked);
        future.await(io);
        _ = io.swapCancelProtection(previous);
    }
    return count.load(.acquire) == 64;
}
