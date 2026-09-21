// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

/// Atomic-shaped counters for the explicitly single-threaded tokenizer build.
/// wasm32 cannot lower 64-bit atomics in Zig 0.16's single-threaded mode.
/// Native concurrent tokenizers continue to use std.atomic.Value(u64).
pub const Counter = struct {
    raw: u64,
    pub fn init(value: u64) Counter {
        return .{ .raw = value };
    }
    pub fn load(self: *const Counter, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        return self.raw;
    }
    pub fn store(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) void {
        _ = order;
        self.raw = value;
    }
    pub fn swap(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        const old = self.raw;
        self.raw = value;
        return old;
    }
    pub fn fetchAdd(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        const old = self.raw;
        self.raw +%= value;
        return old;
    }
    pub fn fetchSub(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        const old = self.raw;
        self.raw -%= value;
        return old;
    }
    pub fn fetchOr(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        const old = self.raw;
        self.raw |= value;
        return old;
    }
    pub fn fetchAnd(self: *Counter, value: u64, comptime order: std.builtin.AtomicOrder) u64 {
        _ = order;
        const old = self.raw;
        self.raw &= value;
        return old;
    }
};

test "single-threaded counters preserve atomic old-value and wrap semantics" {
    var counter = Counter.init(std.math.maxInt(u64));
    try std.testing.expectEqual(std.math.maxInt(u64), counter.fetchAdd(1, .monotonic));
    try std.testing.expectEqual(@as(u64, 0), counter.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), counter.fetchSub(1, .monotonic));
    try std.testing.expectEqual(std.math.maxInt(u64), counter.swap(3, .acq_rel));
    try std.testing.expectEqual(@as(u64, 3), counter.fetchAnd(2, .monotonic));
    try std.testing.expectEqual(@as(u64, 2), counter.fetchOr(4, .monotonic));
    counter.store(7, .release);
    try std.testing.expectEqual(@as(u64, 7), counter.load(.acquire));
}
