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

const platform = @import("antfly_platform");
const std = @import("std");

const abi = @import("antfly_executor_abi");

extern fn exerciseBorrowedExecutor(borrow: *const abi.Borrow) callconv(.c) bool;

fn warmWorker() void {}

test "borrowed executor wakes idle owning workers across archives" {
    var pool = platform.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer pool.deinit();
    const io = pool.io();
    var warm = try io.concurrent(warmWorker, .{});
    warm.await(io);
    const borrow = abi.Borrow.init(&io);
    try std.testing.expect(exerciseBorrowedExecutor(&borrow));
}
