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

//! Reader helpers for decoded algebraic materialization artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("types.zig");
const codec = @import("codec.zig");

pub const Reader = struct {
    alloc: Allocator,
    segment: algebraic_segment.Segment,

    pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !Reader {
        return .{
            .alloc = alloc,
            .segment = try codec.decodeAlloc(alloc, bytes),
        };
    }

    pub fn deinit(self: *Reader) void {
        self.segment.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn groupCount(self: Reader) usize {
        return self.segment.aggregate.groups.len;
    }

    pub fn find(self: Reader, key: []const u8) ?algebraic_segment.AggregateValue {
        for (self.segment.aggregate.groups) |group| {
            if (std.mem.eql(u8, group.key, key)) return group.value;
        }
        return null;
    }
};

test "algebraic reader finds decoded group folds" {
    const alloc = std.testing.allocator;
    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{ .key = try alloc.dupe(u8, "t1"), .value = .{ .sum_i64 = 42 } };

    const encoded = try codec.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    var reader = try Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), reader.groupCount());
    try std.testing.expectEqual(@as(i64, 42), reader.find("t1").?.sum_i64);
    try std.testing.expect(reader.find("missing") == null);
}
