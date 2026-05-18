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

pub const Config = struct {
    mode: enum { lower_bound, upper_bound } = .lower_bound,
};

pub const KeySearchResult = struct {
    index: u32,
    exact: bool,
};

pub fn valuesUpsertIndex(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) Key,
    items: []const Value,
    key: Key,
    comptime config: Config,
) u32 {
    if (items.len == 0) return 0;

    var offset: usize = 0;
    var length: usize = items.len;
    while (length > 1) {
        const half = length / 2;
        const mid = offset + half;
        const take_upper_half = switch (comptime config.mode) {
            .lower_bound => key_from_value(&items[mid]) < key,
            .upper_bound => key_from_value(&items[mid]) <= key,
        };
        if (take_upper_half) offset = mid;
        length -= half;
    }

    offset += @intFromBool(switch (comptime config.mode) {
        .lower_bound => key_from_value(&items[offset]) < key,
        .upper_bound => key_from_value(&items[offset]) <= key,
    });
    return @intCast(offset);
}

pub fn keysUpsertIndex(
    comptime Key: type,
    items: []const Key,
    key: Key,
    comptime config: Config,
) u32 {
    return valuesUpsertIndex(
        Key,
        Key,
        struct {
            fn fromKey(v: *const Key) Key {
                return v.*;
            }
        }.fromKey,
        items,
        key,
        config,
    );
}

pub fn searchValues(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) Key,
    items: []const Value,
    key: Key,
    comptime config: Config,
) ?*const Value {
    const index = valuesUpsertIndex(Key, Value, key_from_value, items, key, config);
    const exact = index < items.len and key_from_value(&items[index]) == key;
    if (!exact) return null;
    return &items[index];
}

pub fn searchKeys(
    comptime Key: type,
    items: []const Key,
    key: Key,
    comptime config: Config,
) KeySearchResult {
    const index = keysUpsertIndex(Key, items, key, config);
    return .{
        .index = index,
        .exact = index < items.len and items[index] == key,
    };
}

test "binary search keys lower and upper bounds" {
    const values_u32 = [_]u32{ 1, 3, 3, 7, 9 };

    try std.testing.expectEqual(@as(u32, 1), keysUpsertIndex(u32, &values_u32, 3, .{ .mode = .lower_bound }));
    try std.testing.expectEqual(@as(u32, 3), keysUpsertIndex(u32, &values_u32, 3, .{ .mode = .upper_bound }));
    try std.testing.expectEqual(@as(u32, 3), keysUpsertIndex(u32, &values_u32, 4, .{}));
    try std.testing.expectEqual(@as(u32, 0), keysUpsertIndex(u32, &values_u32, 0, .{}));
    try std.testing.expectEqual(@as(u32, 5), keysUpsertIndex(u32, &values_u32, 10, .{}));
}

test "binary search values exact matches" {
    const Item = struct {
        key: u32,
        value: u32,
    };

    const items = [_]Item{
        .{ .key = 2, .value = 20 },
        .{ .key = 4, .value = 40 },
        .{ .key = 6, .value = 60 },
    };

    const found = searchValues(u32, Item, struct {
        fn keyFromValue(item: *const Item) u32 {
            return item.key;
        }
    }.keyFromValue, &items, 4, .{}) orelse unreachable;
    try std.testing.expectEqual(@as(u32, 40), found.value);
    try std.testing.expect(searchValues(u32, Item, struct {
        fn keyFromValue(item: *const Item) u32 {
            return item.key;
        }
    }.keyFromValue, &items, 5, .{}) == null);
}
