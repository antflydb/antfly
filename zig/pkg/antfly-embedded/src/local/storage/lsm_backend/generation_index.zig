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

//! Immutable L0 publication summaries. One entry per logical generation,
//! independent of the number of physical SSTs in that publication.
const std = @import("std");

pub const Generation = struct {
    sequence: u64,
    files: usize = 0,
    bytes: u64 = 0,
    pub const Summary = struct {
        files: usize = 0,
        bytes: u64 = 0,
        largest_bytes: u64 = 0,
        largest_sequence: u64 = 0,

        pub fn join(a: @This(), b: @This()) @This() {
            const take_b = b.largest_bytes > a.largest_bytes or
                (b.largest_bytes == a.largest_bytes and b.largest_sequence > a.largest_sequence);
            return .{ .files = a.files + b.files, .bytes = a.bytes +| b.bytes, .largest_bytes = if (take_b) b.largest_bytes else a.largest_bytes, .largest_sequence = if (take_b) b.largest_sequence else a.largest_sequence };
        }
    };
    pub fn summarize(entry: Generation, left: Summary, right: Summary) Summary {
        return left.join(.{ .files = entry.files, .bytes = entry.bytes, .largest_bytes = entry.bytes, .largest_sequence = entry.sequence }).join(right);
    }
    pub fn retainShared(self: Generation) Generation {
        return self;
    }
    pub fn deinit(_: Generation, _: std.mem.Allocator) void {}
    pub fn retainedBytes(_: Generation) usize {
        return 0;
    }
    pub fn compare(a: Generation, b: Generation) std.math.Order {
        return std.math.order(b.sequence, a.sequence);
    }
};
pub const Tree = @import("ordered_index.zig").Index(Generation, Generation.compare);

/// Aggregate a rank prefix in O(log generations), without visiting its files.
pub fn prefix(root: ?*const Tree.Node, count: usize) Generation.Summary {
    const node = root orelse return .{};
    if (count >= node.count) return node.summary;
    const left_count = if (node.left) |left| left.count else 0;
    if (count <= left_count) return prefix(node.left, count);
    return Generation.summarize(node.entry, if (node.left) |left| left.summary else .{}, prefix(node.right, count - left_count - 1));
}

pub fn sequence(run: anytype) u64 {
    return if (run.visibility_id == 0) run.id else run.visibility_id;
}
