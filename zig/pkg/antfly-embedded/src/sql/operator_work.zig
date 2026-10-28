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
    /// Borrowed only by synchronous, fixed-size metadata visitors. Byte work
    /// larger than 64 bytes always checks the original owner directly.
    metadata: ?struct {
        ptr: *anyopaque,
        consume: *const fn (*anyopaque, usize, bool) bool,
    } = null,
};

/// One caller-owned metadata quantum, shared by nested visitors. Its callback
/// transports no errors; all fallible checks still use the original owner.
pub const MetadataScope = struct {
    original: ?Control,
    remaining: usize = 64,
    pub fn view(self: *@This()) ?Control {
        var control = self.original orelse return null;
        if (control.metadata != null) return control;
        control.metadata = .{ .ptr = self, .consume = consume };
        return control;
    }
    fn consume(raw: *anyopaque, units: usize, reset: bool) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (reset) self.remaining = 64;
        if (units > self.remaining) {
            self.remaining = 0;
            return true;
        }
        self.remaining -= units;
        return false;
    }
};
fn metadataCharge(control: ?Control, units: usize) !void {
    std.debug.assert(units <= 64);
    const owner = control orelse return;
    const metadata = owner.metadata orelse return poll(control);
    if (metadata.consume(metadata.ptr, units, false)) {
        // Do not advance the meter after denial. Even a retry must first
        // recheck this same owner instead of receiving a fresh quantum.
        try poll(control);
        const exhausted = metadata.consume(metadata.ptr, units, true);
        std.debug.assert(!exhausted);
    }
}
pub fn metadataPoll(control: ?Control) !void {
    try metadataCharge(control, 1);
}
/// Descriptor-name lookup shares the metadata quantum too. Each comparison
/// chunk is charged before inspecting bytes; long names cannot hide a scan.
pub fn equalMetadataBytes(control: ?Control, left: []const u8, right: []const u8) anyerror!bool {
    if (control == null) return std.mem.eql(u8, left, right);
    if (control.?.metadata == null) return equalBytes(control, left, right);
    try metadataPoll(control);
    if (left.len != right.len) return false;
    var offset: usize = 0;
    while (offset < left.len) {
        const count = @min(@as(usize, 64), left.len - offset);
        try metadataCharge(control, count);
        if (!std.mem.eql(u8, left[offset..][0..count], right[offset..][0..count])) return false;
        offset += count;
    }
    return true;
}
pub fn poll(control: ?Control) !void {
    if (control) |owner| try owner.checkpoint(owner.ptr);
}
pub fn hashBytes(control: ?Control, seed: u64, bytes: []const u8) !u64 {
    if (control == null) return std.hash.Wyhash.hash(seed, bytes);
    if (control.?.metadata != null and bytes.len <= 64) {
        try metadataPoll(control);
        try metadataCharge(control, bytes.len);
        return std.hash.Wyhash.hash(seed, bytes);
    }
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
    if (control.?.metadata != null and left.len <= 64 and right.len <= 64)
        return equalMetadataBytes(control, left, right);
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

/// Retain the old buffer until its bounded copy completes. A denied growth
/// leaves the caller's list intact, including its capacity and logical length.
pub fn ensureTotalCapacity(comptime T: type, a: std.mem.Allocator, list: *std.ArrayList(T), minimum: usize, control: ?Control) !void {
    if (minimum <= list.capacity) return;
    if (control == null) return list.ensureTotalCapacity(a, minimum);
    list.pointer_stability.assertUnlocked();
    try poll(control);
    if (@sizeOf(T) == 0) {
        list.capacity = std.math.maxInt(usize);
        return;
    }
    const capacity = std.ArrayList(T).growCapacity(minimum);
    const bytes = std.math.mul(usize, @sizeOf(T), capacity) catch return error.OutOfMemory;
    // Convenience alloc/free poison the entire buffer in Debug. The raw
    // callbacks preserve allocator accounting without adding that hidden scan.
    const allocation = a.rawAlloc(bytes, .of(T), @returnAddress()) orelse return error.OutOfMemory;
    errdefer a.rawFree(allocation[0..bytes], .of(T), @returnAddress());
    const output: []T = @as([*]T, @ptrCast(@alignCast(allocation)))[0..capacity];
    const width = @max(@as(usize, 1), @as(usize, 4096) / @max(@as(usize, 1), @sizeOf(T)));
    var offset: usize = 0;
    while (offset < list.items.len) {
        try poll(control);
        const end = offset + @min(width, list.items.len - offset);
        @memcpy(output[offset..end], list.items[offset..end]);
        offset = end;
    }
    try poll(control);
    const length = list.items.len;
    const old = std.mem.sliceAsBytes(list.allocatedSlice());
    if (old.len != 0) a.rawFree(old, .of(T), @returnAddress());
    list.items = output[0..length];
    list.capacity = capacity;
}

/// Publish only initialized prefixes. Ordinary failed-store cleanup can free
/// a partially filled list without observing undefined tagged values.
pub fn resizeFill(comptime T: type, a: std.mem.Allocator, list: *std.ArrayList(T), length: usize, value: T, control: ?Control) !void {
    try ensureTotalCapacity(T, a, list, length, control);
    if (length <= list.items.len) {
        list.items.len = length;
        return;
    }
    const width = @max(@as(usize, 1), @as(usize, 4096) / @max(@as(usize, 1), @sizeOf(T)));
    while (list.items.len < length) {
        try poll(control);
        const begin = list.items.len;
        const end = begin + @min(width, length - begin);
        const storage = list.allocatedSlice();
        @memset(storage[begin..end], value);
        list.items.len = end;
    }
}
