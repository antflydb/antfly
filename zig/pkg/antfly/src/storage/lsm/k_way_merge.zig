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

pub const Direction = enum {
    ascending,
    descending,
};

pub const Pending = error{Pending};

pub const Options = struct {
    streams_max: comptime_int,
    deduplicate: bool = false,
};

pub fn TournamentTreeType(comptime Key: type, contestants_max: comptime_int) type {
    return struct {
        loser_keys: [node_count_max]Key,
        loser_ids: [node_count_max]u32,
        win_key: Key,
        win_id: u32,
        contestants_left: u16,
        height: u8,
        direction: Direction,

        pub const node_count_max: u32 = std.math.ceilPowerOfTwoAssert(u32, contestants_max);
        const height_max = std.math.log2_int(u32, node_count_max);
        const sentinel_key = std.math.maxInt(Key);

        pub const Node = struct {
            key: Key,
            id: u32,

            pub const id_sentinel = std.math.maxInt(u32);
            pub const sentinel: Node = .{
                .key = sentinel_key,
                .id = id_sentinel,
            };
        };

        const Self = @This();

        pub fn init(direction: Direction, contestants: *[node_count_max]Node, contestant_count: u16) Self {
            var contestants_left: u16 = 0;
            for (contestants[0..contestant_count]) |contestant| {
                if (contestant.id != Node.id_sentinel) contestants_left += 1;
            }

            var tree: Self = .{
                .loser_keys = @splat(sentinel_key),
                .loser_ids = @splat(Node.id_sentinel),
                .win_key = sentinel_key,
                .win_id = Node.id_sentinel,
                .contestants_left = contestants_left,
                .height = 0,
                .direction = direction,
            };
            if (contestants_left == 0) return tree;

            const node_count = std.math.ceilPowerOfTwoAssert(u32, contestant_count);
            tree.height = @intCast(std.math.log2_int(u32, node_count));

            for (0..tree.height) |level| {
                const shift_min: u5 = @intCast(level + 1);
                const shift_max: u5 = @intCast(level);
                const level_min: usize = (node_count >> shift_min) - 1;
                const level_max: usize = (node_count >> shift_max) - 1;

                for (level_min..level_max, 0..) |loser_index, competitor_index| {
                    const a = contestants[competitor_index * 2];
                    const b = contestants[competitor_index * 2 + 1];
                    if (beats(a.key, a.id, b.key, b.id, direction)) {
                        contestants[competitor_index] = a;
                        tree.loser_keys[loser_index] = b.key;
                        tree.loser_ids[loser_index] = b.id;
                    } else {
                        contestants[competitor_index] = b;
                        tree.loser_keys[loser_index] = a.key;
                        tree.loser_ids[loser_index] = a.id;
                    }
                }
            }

            tree.win_key = contestants[0].key;
            tree.win_id = contestants[0].id;
            return tree;
        }

        pub fn popWinner(tree: *Self, entrant: ?Key) void {
            switch (tree.direction) {
                inline else => |direction| switch (tree.height) {
                    inline 0...height_max => |height| popWinnerImpl(tree, entrant, direction, height),
                    else => unreachable,
                },
            }
        }

        inline fn popWinnerImpl(tree: *Self, entrant: ?Key, comptime direction: Direction, comptime height: u32) void {
            const node_count = @as(u32, 1) << @as(u5, @intCast(height));
            const winner_id = tree.win_id;

            if (entrant == null) tree.contestants_left -= 1;

            var new_key: Key = entrant orelse sentinel_key;
            var new_id: u32 = if (entrant != null) winner_id else Node.id_sentinel;
            var idx: usize = (node_count - 1) + winner_id;

            inline for (0..height) |_| {
                idx = (idx - 1) >> 1;
                const opp_key = tree.loser_keys[idx];
                const opp_id = tree.loser_ids[idx];
                if (beats(new_key, new_id, opp_key, opp_id, direction)) {
                    tree.loser_keys[idx] = opp_key;
                    tree.loser_ids[idx] = opp_id;
                } else {
                    tree.loser_keys[idx] = new_key;
                    tree.loser_ids[idx] = new_id;
                    new_key = opp_key;
                    new_id = opp_id;
                }
            }

            tree.win_key = new_key;
            tree.win_id = new_id;
        }

        inline fn beats(a_key: Key, a_id: u32, b_key: Key, b_id: u32, direction: Direction) bool {
            if (a_id == Node.id_sentinel) return false;
            if (b_id == Node.id_sentinel) return true;
            if (a_key == b_key) return a_id < b_id;
            return switch (direction) {
                .ascending => a_key < b_key,
                .descending => a_key > b_key,
            };
        }
    };
}

pub fn IteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime options: Options,
    comptime key_from_value: fn (*const Value) Key,
    comptime stream_peek: fn (*Context, u32) Pending!?Key,
    comptime stream_pop: fn (*Context, u32) Value,
) type {
    comptime std.debug.assert(options.streams_max >= 1);

    return struct {
        context: *Context,
        streams_count: u16,
        direction: Direction,
        key_popped: ?Key,
        tree: ?Tree,

        const Tree = TournamentTreeType(Key, options.streams_max);
        const Self = @This();

        pub fn init(context: *Context, streams_count: u16, direction: Direction) Self {
            return .{
                .context = context,
                .streams_count = streams_count,
                .direction = direction,
                .key_popped = null,
                .tree = null,
            };
        }

        pub fn reset(self: *Self) void {
            self.tree = null;
            self.key_popped = null;
        }

        fn load(self: *Self) Pending!void {
            if (self.tree != null) return;
            var contestants: [Tree.node_count_max]Tree.Node = @splat(.sentinel);
            for (0..self.streams_count) |id_usize| {
                const id: u32 = @intCast(id_usize);
                const key = try stream_peek(self.context, id) orelse continue;
                contestants[id_usize] = .{ .key = key, .id = id };
            }
            self.tree = Tree.init(self.direction, &contestants, self.streams_count);
        }

        pub fn pop(self: *Self) Pending!?Value {
            if (self.tree == null) try self.load();
            const tree = &self.tree.?;

            while (tree.contestants_left > 0) {
                const winner_id = tree.win_id;
                const value = stream_pop(self.context, winner_id);
                const next_key = try stream_peek(self.context, winner_id);
                tree.popWinner(next_key);
                if (options.deduplicate) {
                    const key = key_from_value(&value);
                    if (self.key_popped) |prev| {
                        if (prev == key) continue;
                    }
                    self.key_popped = key;
                }
                return value;
            }

            return null;
        }
    };
}

fn TestContextType(comptime streams_max: u32) type {
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

        fn lessThan(direction: Direction, a: Value, b: Value) bool {
            if (a.key == b.key) return a.version < b.version;
            return switch (direction) {
                .ascending => a.key < b.key,
                .descending => a.key > b.key,
            };
        }

        fn merge(direction: Direction, streams_keys: []const []const u32, expect: []const Value) !void {
            const Merge = IteratorType(
                Self,
                u32,
                Value,
                .{ .streams_max = streams_max, .deduplicate = true },
                Value.keyFromValue,
                streamPeek,
                streamPop,
            );

            const alloc = std.testing.allocator;
            var owned_streams: [streams_max][]Value = undefined;
            for (streams_keys, 0..) |stream_keys, i| {
                owned_streams[i] = try alloc.alloc(Value, stream_keys.len);
                for (stream_keys, 0..) |key, j| {
                    owned_streams[i][j] = .{ .key = key, .version = @intCast(i) };
                }
            }
            defer for (owned_streams[0..streams_keys.len]) |stream| alloc.free(stream);

            var context: Self = .{ .streams = owned_streams };
            var iter = Merge.init(&context, @intCast(streams_keys.len), direction);

            var actual = std.ArrayListUnmanaged(Value).empty;
            defer actual.deinit(alloc);
            while (try iter.pop()) |value| {
                try actual.append(alloc, value);
            }

            try std.testing.expectEqualSlices(Value, expect, actual.items);
        }
    };
}

test "k-way merge ascending and descending" {
    try TestContextType(3).merge(
        .ascending,
        &.{
            &.{ 0, 3, 4, 8, 11 },
            &.{ 2, 11, 12, 13, 15 },
            &.{ 1, 2, 11 },
        },
        &.{
            .{ .key = 0, .version = 0 },
            .{ .key = 1, .version = 2 },
            .{ .key = 2, .version = 1 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 11, .version = 0 },
            .{ .key = 12, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 15, .version = 1 },
        },
    );

    try TestContextType(3).merge(
        .descending,
        &.{
            &.{ 11, 8, 4, 3, 0 },
            &.{ 15, 13, 12, 11, 2 },
            &.{ 11, 2, 1 },
        },
        &.{
            .{ .key = 15, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 12, .version = 1 },
            .{ .key = 11, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 2, .version = 1 },
            .{ .key = 1, .version = 2 },
            .{ .key = 0, .version = 0 },
        },
    );
}
