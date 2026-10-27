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
const Budget = @This();
backing: std.mem.Allocator,
limit: usize,
live: usize = 0,
peak: usize = 0,
exhausted: bool = false,
pub fn allocator(self: *Budget) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
}
/// Inspect a recognized statement allocator while its caller holds allocation
/// admission. Cross-worker wrappers use this to retain failure provenance even
/// if another successful allocation resets the ordinary per-call flag.
pub fn allocatorExhausted(a: std.mem.Allocator) bool {
    var probe: Budget = undefined;
    if (a.vtable != probe.allocator().vtable) return false;
    const budget: *Budget = @ptrCast(@alignCast(a.ptr));
    return budget.exhausted;
}
fn admit(self: *Budget, growth: usize) bool {
    if (growth > self.limit -| self.live) {
        self.exhausted = true;
        return false;
    }
    return true;
}
fn account(self: *Budget, old: usize, new: usize) void {
    self.live = self.live - old + new;
    self.peak = @max(self.peak, self.live);
}
fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.exhausted = false;
    if (!self.admit(len)) return null;
    const memory = self.backing.rawAlloc(len, alignment, ra) orelse return null;
    self.account(0, len);
    return memory;
}
fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.exhausted = false;
    if (!self.admit(len -| bytes.len) or !self.backing.rawResize(bytes, alignment, len, ra)) return false;
    self.account(bytes.len, len);
    return true;
}
fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.exhausted = false;
    if (!self.admit(len -| bytes.len)) return null;
    const memory = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
    self.account(bytes.len, len);
    return memory;
}
fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.backing.rawFree(bytes, alignment, ra);
    self.account(bytes.len, 0);
}
