// Copyright 2026 Antfly, Inc.
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

fn usePlainValue(comptime T: type) bool {
    return builtin.single_threaded and builtin.cpu.arch == .wasm32 and @bitSizeOf(T) > 32;
}

pub fn Value(comptime T: type) type {
    if (comptime usePlainValue(T)) {
        return struct {
            raw: T,

            pub fn init(value: T) @This() {
                return .{ .raw = value };
            }

            pub inline fn load(self: *const @This(), order: anytype) T {
                _ = order;
                return self.raw;
            }

            pub inline fn store(self: *@This(), value: T, order: anytype) void {
                _ = order;
                self.raw = value;
            }

            pub inline fn swap(self: *@This(), operand: T, order: anytype) T {
                _ = order;
                const old = self.raw;
                self.raw = operand;
                return old;
            }

            pub inline fn fetchAdd(self: *@This(), operand: T, order: anytype) T {
                _ = order;
                const old = self.raw;
                self.raw +%= operand;
                return old;
            }

            pub inline fn fetchSub(self: *@This(), operand: T, order: anytype) T {
                _ = order;
                const old = self.raw;
                self.raw -%= operand;
                return old;
            }

            pub inline fn fetchOr(self: *@This(), operand: T, order: anytype) T {
                _ = order;
                const old = self.raw;
                self.raw |= operand;
                return old;
            }

            pub inline fn fetchAnd(self: *@This(), operand: T, order: anytype) T {
                _ = order;
                const old = self.raw;
                self.raw &= operand;
                return old;
            }

            pub inline fn cmpxchgWeak(
                self: *@This(),
                expected_value: T,
                new_value: T,
                success_order: anytype,
                fail_order: anytype,
            ) ?T {
                return self.compareExchange(expected_value, new_value, success_order, fail_order);
            }

            pub inline fn cmpxchgStrong(
                self: *@This(),
                expected_value: T,
                new_value: T,
                success_order: anytype,
                fail_order: anytype,
            ) ?T {
                return self.compareExchange(expected_value, new_value, success_order, fail_order);
            }

            inline fn compareExchange(
                self: *@This(),
                expected_value: T,
                new_value: T,
                success_order: anytype,
                fail_order: anytype,
            ) ?T {
                _ = success_order;
                _ = fail_order;
                if (self.raw == expected_value) {
                    self.raw = new_value;
                    return null;
                }
                return self.raw;
            }
        };
    }

    return std.atomic.Value(T);
}

/// Raw counters embedded in wire/diagnostic records keep their scalar layout.
/// Only the single-threaded wasm32 runtime may use plain wide accesses.
pub inline fn load(comptime T: type, ptr: *const T, comptime order: std.builtin.AtomicOrder) T {
    if (comptime usePlainValue(T)) return ptr.*;
    return @atomicLoad(T, ptr, order);
}

pub inline fn fetchAdd(comptime T: type, ptr: *T, operand: T, comptime order: std.builtin.AtomicOrder) T {
    if (comptime usePlainValue(T)) {
        const old = ptr.*;
        ptr.* +%= operand;
        return old;
    }
    return @atomicRmw(T, ptr, .Add, operand, order);
}

test "wide values preserve native atomic identity and compare-exchange semantics" {
    if (comptime !usePlainValue(u64)) try std.testing.expect(Value(u64) == std.atomic.Value(u64));
    const high: u64 = 0x1_0000_0000;
    var value = Value(u64).init(high);
    try std.testing.expectEqual(high, value.fetchAdd(9, .monotonic));
    try std.testing.expectEqual(high + 9, value.swap(high, .acq_rel));
    try std.testing.expectEqual(@as(?u64, high), value.cmpxchgStrong(0, 1, .acq_rel, .acquire));
    try std.testing.expectEqual(@as(?u64, null), value.cmpxchgStrong(high, high + 2, .acq_rel, .acquire));
    try std.testing.expectEqual(high + 2, value.load(.acquire));
}
