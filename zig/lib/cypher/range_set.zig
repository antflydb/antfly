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

//! Exact compressed integer membership for DISTINCT reducers. Complexity is
//! charged and bounded; a declined union leaves the existing set unchanged.
const std = @import("std");
const arithmetic = @import("range.zig");
pub const Range = arithmetic.Range;
pub const Part = struct { sequence: Range, reversed: bool = false };
pub const Node = union(enum) {
    leaf: Part,
    concat: struct { left: *const Node, right: *const Node },
    reverse: *const Node,
};
pub const Set = struct {
    ranges: std.ArrayList(Range) = .empty,
    encounters: std.ArrayList(Part) = .empty,
    measure: ?arithmetic.Measure = .{},
    pub fn contains(self: *const @This(), value: i64, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !bool {
        for (self.ranges.items) |range| {
            try charge(ptr, 1);
            if (range.contains(.{ .integer = value }).boolean) return true;
        }
        return false;
    }
    /// Measure an incoming range without publishing membership. Reducers whose
    /// order-sensitive summary can decline must certify it before insertion.
    pub fn novelMeasure(self: *const @This(), scratch: std.mem.Allocator, range: Range, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !?arithmetic.Measure {
        if (range.length == 0) return .{};
        try charge(ptr, 1);
        if (self.ranges.items.len == 0) return arithmetic.Measure.of(range);
        if (self.ranges.items.len == 1) if (arithmetic.mergeCompatible(self.ranges.items[0], range)) |merged| {
            return arithmetic.Measure.of(merged).difference(self.measure orelse arithmetic.Measure.of(self.ranges.items[0]));
        };
        const before = self.measure orelse (try arithmetic.unionMeasureWork(scratch, self.ranges.items, ptr, charge)) orelse return null;
        const combined = try scratch.alloc(Range, self.ranges.items.len + 1);
        @memcpy(combined[0..self.ranges.items.len], self.ranges.items);
        combined[self.ranges.items.len] = range;
        const after = (try arithmetic.unionMeasureWork(scratch, combined, ptr, charge)) orelse return null;
        return after.difference(before);
    }
    pub fn insert(self: *@This(), a: std.mem.Allocator, scratch: std.mem.Allocator, range: Range, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !?arithmetic.Measure {
        if (range.length == 0) return .{};
        try charge(ptr, 1);
        if (self.ranges.items.len == 0) {
            const measured = arithmetic.Measure.of(range);
            try self.ranges.append(a, range);
            self.measure = measured;
            return measured;
        }
        if (self.ranges.items.len == 1) if (arithmetic.mergeCompatible(self.ranges.items[0], range)) |merged| {
            const before = self.measure orelse arithmetic.Measure.of(self.ranges.items[0]);
            const after = arithmetic.Measure.of(merged);
            self.ranges.items[0] = merged;
            self.measure = after;
            return after.difference(before);
        };
        for (self.ranges.items) |prior| {
            try charge(ptr, 1);
            if (prior.contains(.{ .integer = range.start }).boolean and prior.contains(range.at(range.length - 1)).boolean and
                (range.length == 1 or @rem(@as(i128, range.step), prior.step) == 0)) return .{};
        }
        const before = self.measure orelse (try arithmetic.unionMeasureWork(scratch, self.ranges.items, ptr, charge)) orelse return null;
        const combined = try scratch.alloc(Range, self.ranges.items.len + 1);
        @memcpy(combined[0..self.ranges.items.len], self.ranges.items);
        combined[self.ranges.items.len] = range;
        const after = (try arithmetic.unionMeasureWork(scratch, combined, ptr, charge)) orelse return null;
        // Reserve before mutation so allocation failure cannot leave a partial set.
        try self.ranges.ensureUnusedCapacity(a, 1);
        var merged = range;
        var i: usize = 0;
        while (i < self.ranges.items.len) {
            try charge(ptr, 1);
            if (arithmetic.mergeCompatible(merged, self.ranges.items[i])) |joined| {
                merged = joined;
                _ = self.ranges.swapRemove(i);
            } else i += 1;
        }
        self.ranges.appendAssumeCapacity(merged);
        self.measure = after;
        return after.difference(before);
    }
};
