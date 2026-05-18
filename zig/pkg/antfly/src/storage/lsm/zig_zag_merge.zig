// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Direction = @import("k_way_merge.zig").Direction;
const Pending = @import("k_way_merge.zig").Pending;

pub fn IteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) Key,
    comptime streams_max: u32,
    comptime stream_peek: fn (*Context, u32) Pending!?Key,
    comptime stream_pop: fn (*Context, u32) Value,
    comptime stream_probe: fn (*Context, u32, Key) void,
) type {
    return struct {
        const Self = @This();
        const BitSet = std.bit_set.IntegerBitSet(streams_max);

        context: *Context,
        streams_count: u32,
        direction: Direction,
        probe_key_previous: ?Key = null,
        key_popped: ?Key = null,

        pub fn init(context: *Context, streams_count: u32, direction: Direction) Self {
            std.debug.assert(streams_count <= streams_max);
            std.debug.assert(streams_count > 1);
            return .{
                .context = context,
                .streams_count = streams_count,
                .direction = direction,
            };
        }

        pub fn reset(_: *Self) void {}

        pub fn pop(self: *Self) Pending!?Value {
            const key = try self.peekKey() orelse return null;
            if (self.key_popped) |previous| {
                std.debug.assert(cmp(self.direction, previous, .lt, key));
            }
            self.key_popped = key;

            const value = stream_pop(self.context, 0);
            std.debug.assert(key_from_value(&value) == key);
            for (1..self.streams_count) |stream_index| {
                const other = stream_pop(self.context, @intCast(stream_index));
                std.debug.assert(key_from_value(&other) == key);
            }
            return value;
        }

        fn peekKey(self: *Self) Pending!?Key {
            const key_min: Key = switch (self.direction) {
                .ascending => 0,
                .descending => std.math.maxInt(Key),
            };

            var pending = BitSet.initEmpty();
            var probe_key: Key = key_min;

            var probing = BitSet.initFull();
            while (probing.count() > 0) {
                probing = BitSet.initEmpty();
                for (0..self.streams_count) |stream_index| {
                    if (pending.isSet(stream_index)) continue;

                    const key = stream_peek(self.context, @intCast(stream_index)) catch |err| switch (err) {
                        error.Pending => {
                            pending.set(stream_index);
                            continue;
                        },
                    } orelse return null;

                    if (self.probe_key_previous) |previous| {
                        std.debug.assert(cmp(self.direction, previous, .lte, key));
                    }

                    if (probe_key == key) continue;

                    if (cmp(self.direction, probe_key, .lt, key)) {
                        probe_key = key;
                        probing.setRangeValue(.{ .start = 0, .end = stream_index }, true);
                        probing.setIntersection(pending.complement());
                        std.debug.assert(!probing.isSet(stream_index));
                    } else {
                        probing.set(stream_index);
                    }
                }

                var it = probing.iterator(.{ .kind = .set });
                while (it.next()) |stream_index| {
                    stream_probe(self.context, @intCast(stream_index), probe_key);
                    const key = stream_peek(self.context, @intCast(stream_index)) catch |err| switch (err) {
                        error.Pending => {
                            pending.set(stream_index);
                            probing.unset(stream_index);
                            continue;
                        },
                    } orelse return null;

                    if (key == probe_key) {
                        probing.unset(stream_index);
                    } else {
                        std.debug.assert(cmp(self.direction, probe_key, .lt, key));
                    }
                }
            }

            if (pending.count() == self.streams_count) return error.Pending;

            std.debug.assert(probe_key != key_min);
            for (0..self.streams_count) |stream_index| {
                if (pending.isSet(stream_index)) {
                    stream_probe(self.context, @intCast(stream_index), probe_key);
                    _ = stream_peek(self.context, @intCast(stream_index)) catch |err| switch (err) {
                        error.Pending => continue,
                    };
                    unreachable;
                } else {
                    std.debug.assert((stream_peek(self.context, @intCast(stream_index)) catch unreachable).? == probe_key);
                }
            }

            if (self.probe_key_previous) |previous| {
                std.debug.assert(cmp(self.direction, previous, .lte, probe_key));
            }
            self.probe_key_previous = probe_key;
            return if (pending.count() == 0) probe_key else error.Pending;
        }

        fn cmp(direction: Direction, lhs: Key, comptime op: std.math.CompareOperator, rhs: Key) bool {
            return switch (direction) {
                .ascending => std.math.compare(lhs, op, rhs),
                .descending => std.math.compare(lhs, invert(op), rhs),
            };
        }

        fn invert(comptime op: std.math.CompareOperator) std.math.CompareOperator {
            return switch (op) {
                .lt => .gt,
                .lte => .gte,
                .eq => .eq,
                .gte => .lte,
                .gt => .lt,
                .neq => .neq,
            };
        }
    };
}

fn TestContextType(comptime streams_max: u32) type {
    return struct {
        const Self = @This();
        const Key = u128;
        const Value = u128;

        streams: [streams_max][]const Value,
        direction: Direction,

        fn keyFromValue(value: *const Value) Key {
            return value.*;
        }

        fn streamPeek(context: *Self, stream_index: u32) Pending!?Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return switch (context.direction) {
                .ascending => keyFromValue(&stream[0]),
                .descending => keyFromValue(&stream[stream.len - 1]),
            };
        }

        fn streamPop(context: *Self, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            return switch (context.direction) {
                .ascending => blk: {
                    context.streams[stream_index] = stream[1..];
                    break :blk stream[0];
                },
                .descending => blk: {
                    context.streams[stream_index] = stream[0 .. stream.len - 1];
                    break :blk stream[stream.len - 1];
                },
            };
        }

        fn streamProbe(context: *Self, stream_index: u32, probe_key: Key) void {
            while (true) {
                const key = streamPeek(context, stream_index) catch unreachable orelse return;
                const matched = switch (context.direction) {
                    .ascending => key >= probe_key,
                    .descending => key <= probe_key,
                };
                if (matched) break;
                _ = streamPop(context, stream_index);
            }
        }

        fn merge(streams: []const []const Value, expect: []const Value) !void {
            const ZigZag = IteratorType(
                Self,
                Key,
                Value,
                keyFromValue,
                streams_max,
                streamPeek,
                streamPop,
                streamProbe,
            );

            for (std.enums.values(Direction)) |direction| {
                var actual = std.ArrayListUnmanaged(Value).empty;
                defer actual.deinit(std.testing.allocator);

                var context: Self = .{
                    .streams = undefined,
                    .direction = direction,
                };
                for (streams, 0..) |stream, i| context.streams[i] = stream;

                var it = ZigZag.init(&context, @intCast(streams.len), direction);
                while (try it.pop()) |value| {
                    try actual.append(std.testing.allocator, value);
                }

                if (direction == .descending) std.mem.reverse(Value, actual.items);
                try std.testing.expectEqualSlices(Value, expect, actual.items);
            }
        }
    };
}

test "zig-zag merge intersections" {
    const Context = TestContextType(10);

    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 2, 3, 4, 5 },
            &.{ 1, 2, 3, 4, 5 },
            &.{ 1, 2, 3, 4, 5 },
        },
        &.{ 1, 2, 3, 4, 5 },
    );

    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 3, 5, 7, 9 },
            &.{ 2, 4, 6, 8, 10 },
        },
        &.{},
    );

    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 2, 3, 4, 5 },
            &.{ 2, 3, 4, 5, 6 },
            &.{ 3, 4, 5, 6, 7 },
            &.{ 4, 5, 6, 7, 8 },
        },
        &.{ 4, 5 },
    );

    try Context.merge(
        &[_][]const Context.Value{
            &.{ 2, 4, 6, 8, 10 },
            &.{ 2, 4, 6, 8, 10 },
            &.{},
        },
        &.{},
    );
}
