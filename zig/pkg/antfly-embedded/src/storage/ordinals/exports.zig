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

//! Private C ABI over fixed-width scalar buffers, with no allocator or IO ABI.
const operations = @import("operations.zig");
fn sort32(values: [*]u32, len: usize) callconv(.c) void {
    operations.sort(u32, values[0..len]);
}
fn sort64(values: [*]u64, len: usize) callconv(.c) void {
    operations.sort(u64, values[0..len]);
}
fn unique32(values: [*]u32, len: usize) callconv(.c) usize {
    return operations.unique(u32, values[0..len]);
}
fn union32(left: [*]const u32, left_len: usize, right: [*]const u32, right_len: usize, out: [*]u32, capacity: usize) callconv(.c) usize {
    return operations.merge(.union_set, left[0..left_len], right[0..right_len], out[0..capacity]);
}
fn intersection32(left: [*]const u32, left_len: usize, right: [*]const u32, right_len: usize, out: [*]u32, capacity: usize) callconv(.c) usize {
    return operations.merge(.intersection, left[0..left_len], right[0..right_len], out[0..capacity]);
}
fn difference32(left: [*]const u32, left_len: usize, right: [*]const u32, right_len: usize, out: [*]u32, capacity: usize) callconv(.c) usize {
    return operations.merge(.difference, left[0..left_len], right[0..right_len], out[0..capacity]);
}
comptime {
    @export(&sort32, .{ .name = "antfly_storage_sort_u32_v1", .visibility = .hidden });
    @export(&sort64, .{ .name = "antfly_storage_sort_u64_v1", .visibility = .hidden });
    @export(&unique32, .{ .name = "antfly_storage_unique_u32_v1", .visibility = .hidden });
    @export(&union32, .{ .name = "antfly_storage_union_u32_v1", .visibility = .hidden });
    @export(&intersection32, .{ .name = "antfly_storage_intersection_u32_v1", .visibility = .hidden });
    @export(&difference32, .{ .name = "antfly_storage_difference_u32_v1", .visibility = .hidden });
}
