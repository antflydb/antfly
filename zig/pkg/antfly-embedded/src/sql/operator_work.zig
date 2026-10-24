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

//! Borrowed operator work authority. Only its owner decides whether a poll may
//! yield; native scalar callers may supply a lifetime-only callback instead.
const std = @import("std");
pub const Control = struct {
    ptr: ?*anyopaque,
    checkpoint: *const fn (?*anyopaque) anyerror!void,
};
pub fn poll(control: ?Control) !void {
    if (control) |owner| try owner.checkpoint(owner.ptr);
}
pub fn hashBytes(control: ?Control, seed: u64, bytes: []const u8) !u64 {
    if (control == null) return std.hash.Wyhash.hash(seed, bytes);
    var hash = std.hash.Wyhash.init(seed);
    var offset: usize = 0;
    try poll(control);
    while (offset < bytes.len) {
        const end = offset + @min(@as(usize, 4096), bytes.len - offset);
        hash.update(bytes[offset..end]);
        offset = end;
        try poll(control);
    }
    return hash.final();
}
pub fn orderBytes(control: ?Control, left: []const u8, right: []const u8) !std.math.Order {
    if (control == null) return std.mem.order(u8, left, right);
    const count = @min(left.len, right.len);
    var offset: usize = 0;
    try poll(control);
    while (offset < count) {
        const end = offset + @min(@as(usize, 256), count - offset);
        const order = std.mem.order(u8, left[offset..end], right[offset..end]);
        try poll(control);
        if (order != .eq) return order;
        offset = end;
    }
    return std.math.order(left.len, right.len);
}
pub fn equalBytes(control: ?Control, left: []const u8, right: []const u8) !bool {
    if (control == null) return std.mem.eql(u8, left, right);
    try poll(control);
    if (left.len != right.len) return false;
    return (try orderBytes(control, left, right)) == .eq;
}
/// Copy into the caller's owned allocation region. Cancellation frees the
/// incomplete physical buffer; its region remains owned until ordinary cleanup.
pub fn dupe(comptime T: type, a: std.mem.Allocator, input: []const T, control: ?Control) ![]T {
    if (control == null) return a.dupe(T, input);
    try poll(control);
    const output = try a.alloc(T, input.len);
    errdefer a.free(output);
    var offset: usize = 0;
    const width = @max(@as(usize, 1), @as(usize, 4096) / @max(@as(usize, 1), @sizeOf(T)));
    while (offset < input.len) {
        const end = offset + @min(width, input.len - offset);
        @memcpy(output[offset..end], input[offset..end]);
        offset = end;
        try poll(control);
    }
    return output;
}
