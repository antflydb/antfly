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
const k_way_merge = @import("k_way_merge.zig");
const zig_zag_merge = @import("zig_zag_merge.zig");

pub fn UnionIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime streams_max: u32,
    comptime key_from_value: fn (*const Value) Key,
    comptime stream_peek: fn (*Context, u32) Pending!?Key,
    comptime stream_pop: fn (*Context, u32) Value,
) type {
    return k_way_merge.IteratorType(
        Context,
        Key,
        Value,
        .{
            .streams_max = streams_max,
            .deduplicate = true,
        },
        key_from_value,
        stream_peek,
        stream_pop,
    );
}

pub fn IntersectionIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime streams_max: u32,
    comptime key_from_value: fn (*const Value) Key,
    comptime stream_peek: fn (*Context, u32) Pending!?Key,
    comptime stream_pop: fn (*Context, u32) Value,
    comptime stream_probe: fn (*Context, u32, Key) void,
) type {
    return zig_zag_merge.IteratorType(
        Context,
        Key,
        Value,
        key_from_value,
        streams_max,
        stream_peek,
        stream_pop,
        stream_probe,
    );
}

fn UnionTestContextType(comptime streams_max: u32) type {
    return struct {
        const Self = @This();

        const Value = struct {
            key: u32,
            version: u32,

            fn keyFromValue(v: *const Value) u32 {
                return v.key;
            }
        };

        streams: [streams_max][]const Value,

        fn streamPeek(context: *Self, stream_index: u32) Pending!?u32 {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return stream[0].key;
        }

        fn streamPop(context: *Self, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }
    };
}

fn IntersectionTestContextType(comptime streams_max: u32) type {
    return struct {
        const Self = @This();

        const Value = u128;
        streams: [streams_max][]const Value,
        direction: Direction,

        fn keyFromValue(v: *const Value) u128 {
            return v.*;
        }

        fn streamPeek(context: *Self, stream_index: u32) Pending!?u128 {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return switch (context.direction) {
                .ascending => stream[0],
                .descending => stream[stream.len - 1],
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

        fn streamProbe(context: *Self, stream_index: u32, probe_key: u128) void {
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
    };
}

test "scan merge union wraps k-way merge" {
    const Context = UnionTestContextType(3);
    const Merge = UnionIteratorType(
        Context,
        u32,
        Context.Value,
        3,
        Context.Value.keyFromValue,
        Context.streamPeek,
        Context.streamPop,
    );

    const streams: [3][]const Context.Value = .{
        &.{ .{ .key = 1, .version = 0 }, .{ .key = 4, .version = 0 }, .{ .key = 8, .version = 0 } },
        &.{ .{ .key = 2, .version = 1 }, .{ .key = 4, .version = 1 }, .{ .key = 9, .version = 1 } },
        &.{ .{ .key = 3, .version = 2 }, .{ .key = 8, .version = 2 } },
    };
    var context: Context = .{ .streams = streams };
    var merge = Merge.init(&context, 3, .ascending);

    var actual = std.ArrayListUnmanaged(u32).empty;
    defer actual.deinit(std.testing.allocator);
    while (try merge.pop()) |value| {
        try actual.append(std.testing.allocator, value.key);
    }
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3, 4, 8, 9 }, actual.items);
}

test "scan merge intersection wraps zig-zag merge" {
    const Context = IntersectionTestContextType(4);
    const Merge = IntersectionIteratorType(
        Context,
        u128,
        Context.Value,
        4,
        Context.keyFromValue,
        Context.streamPeek,
        Context.streamPop,
        Context.streamProbe,
    );

    for (std.enums.values(Direction)) |direction| {
        const streams: [4][]const Context.Value = .{
            &.{ 1, 2, 3, 4, 5 },
            &.{ 2, 3, 4, 5, 6 },
            &.{ 3, 4, 5, 6, 7 },
            &.{ 4, 5, 6, 7, 8 },
        };
        var context: Context = .{
            .streams = streams,
            .direction = direction,
        };
        var merge = Merge.init(&context, 4, direction);

        var actual = std.ArrayListUnmanaged(u128).empty;
        defer actual.deinit(std.testing.allocator);
        while (try merge.pop()) |value| {
            try actual.append(std.testing.allocator, value);
        }

        if (direction == .descending) std.mem.reverse(u128, actual.items);
        try std.testing.expectEqualSlices(u128, &.{ 4, 5 }, actual.items);
    }
}
