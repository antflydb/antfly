// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const current = @import("bloom");
const original = @import("baseline.zig");

fn next(state: *u64) u64 {
    state.* = state.* *% 6364136223846793005 +% 1442695040888963407;
    var value = state.*;
    value = (value ^ (value >> 30)) *% 0xbf58476d1ce4e5b9;
    value = (value ^ (value >> 27)) *% 0x94d049bb133111eb;
    return value ^ (value >> 31);
}

test "review differential v1 bytes queries and cross-version decoding" {
    const alloc = std.testing.allocator;
    var state: u64 = 0x32b100ca1ab2026;
    for ([_]u32{ 0, 1, 2, 4, 7, 8, 16, 31, 63, 64, 127, 128, 511, 512, 1024, 65536 }) |bits| {
        for ([_]u8{ 0, 1, 4, 7, 10, 16, 255 }) |hash_count| {
            var old = original.Builder{
                .allocator = alloc,
                .bytes = try alloc.alloc(u8, (bits + 7) / 8),
                .bit_count = bits,
                .hash_count = hash_count,
            };
            defer old.deinit();
            var new = current.Builder{
                .allocator = alloc,
                .bytes = try alloc.alloc(u8, (bits + 7) / 8),
                .bit_count = bits,
                .hash_count = hash_count,
            };
            defer new.deinit();
            @memset(old.bytes, 0);
            @memset(new.bytes, 0);
            const before = current.BorrowedFilter{ .bytes = new.bytes, .bit_count = bits, .hash_count = hash_count };
            try std.testing.expect(!before.maybeContainsHashes(0, 0));
            var inserted: [32][2]u64 = undefined;
            for (&inserted, 0..) |*pair, i| {
                pair.* = switch (i) {
                    0 => .{ std.math.maxInt(u64), 0 },
                    1 => .{ std.math.maxInt(u64) - 3, 2 },
                    2 => .{ 0, std.math.maxInt(u64) },
                    else => .{ next(&state), next(&state) },
                };
                old.addHashes(pair[0], pair[1]);
                new.addHashes(pair[0], pair[1]);
                try std.testing.expectEqualSlices(u8, old.bytes, new.bytes);
            }
            const old_view = original.BorrowedFilter{ .bytes = old.bytes, .bit_count = bits, .hash_count = hash_count };
            const new_view = current.BorrowedFilter{ .bytes = new.bytes, .bit_count = bits, .hash_count = hash_count };
            for (inserted) |pair| {
                const expected = bits != 0 and hash_count != 0;
                try std.testing.expectEqual(expected, old_view.maybeContainsHashes(pair[0], pair[1]));
                try std.testing.expectEqual(expected, new_view.maybeContainsHashes(pair[0], pair[1]));
            }
            for (0..256) |_| {
                const h1 = next(&state);
                const h2 = next(&state);
                try std.testing.expectEqual(old_view.maybeContainsHashes(h1, h2), new_view.maybeContainsHashes(h1, h2));
            }
            // The owning wrappers only borrow builder storage for encoding.
            const old_owned = original.OwnedFilter{ .bytes = old.bytes, .bit_count = bits, .hash_count = hash_count };
            const new_owned = current.OwnedFilter{ .bytes = new.bytes, .bit_count = bits, .hash_count = hash_count };
            const old_encoded = try old_owned.encodeAlloc(alloc);
            defer alloc.free(old_encoded);
            const new_encoded = try new_owned.encodeAlloc(alloc);
            defer alloc.free(new_encoded);
            try std.testing.expectEqualSlices(u8, old_encoded, new_encoded);
            const new_reader = try current.BorrowedFilter.decode(old_encoded);
            const old_reader = try original.BorrowedFilter.decode(new_encoded);
            for (inserted) |pair| {
                try std.testing.expectEqual(old_view.maybeContainsHashes(pair[0], pair[1]), new_reader.maybeContainsHashes(pair[0], pair[1]));
                try std.testing.expectEqual(new_view.maybeContainsHashes(pair[0], pair[1]), old_reader.maybeContainsHashes(pair[0], pair[1]));
            }
        }
    }
}
