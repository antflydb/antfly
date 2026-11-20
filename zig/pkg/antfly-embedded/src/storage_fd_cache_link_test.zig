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
const builtin = @import("builtin");
const storage = @import("storage/lsm_backend/storage_io.zig");

comptime {
    if (!builtin.is_test) @export(&observeWithPermit, .{
        .name = "antfly_test_observe_with_descriptor_permit",
        .visibility = .hidden,
    });
}

fn observeWithPermit(observe: *const fn () callconv(.c) usize) callconv(.c) bool {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    var permit = storage.ProcessNativeFdPermit.acquire(threaded.io()) catch return false;
    defer permit.release();
    return observe() == 1;
}

extern fn antfly_test_observe_with_descriptor_permit(observe: *const fn () callconv(.c) usize) bool;

fn admittedDescriptors() callconv(.c) usize {
    var pool = storage.NativeStoragePool.init(std.heap.page_allocator);
    defer pool.deinit();
    return pool.snapshotStats().fd_admitted_descriptors;
}

test "native fd admission is visible across separately compiled archives" {
    try std.testing.expectEqual(@as(usize, 0), admittedDescriptors());
    try std.testing.expect(antfly_test_observe_with_descriptor_permit(&admittedDescriptors));
    try std.testing.expectEqual(@as(usize, 0), admittedDescriptors());
}
