// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Authenticated minimum logical positions for disjoint serving-pack ranges.
const std = @import("std");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const corpus = @import("lake_index_native_text.zig");
pub const Position = @import("lake_index_text_directory.zig").Position;
pub fn compare(l: Position, r: Position) std.math.Order {
    if (l.recent != r.recent) return if (l.recent) .gt else .lt;
    const file = std.mem.order(u8, &l.file, &r.file);
    return if (file == .eq) std.math.order(l.rank, r.rank) else file;
}
pub fn encode(value: Position) [36]u8 {
    std.debug.assert(!value.recent);
    var bytes: [36]u8 = undefined;
    bytes[0..32].* = value.file;
    std.mem.writeInt(u32, bytes[32..36], value.rank, .big);
    return bytes;
}
pub fn decode(bytes: []const u8) !Position {
    if (bytes.len != 36) return error.InvalidNativeLakeTextCorpus;
    return .{ .file = bytes[0..32].*, .rank = std.mem.readInt(u32, bytes[32..36], .big) };
}
pub fn reduce(a: std.mem.Allocator, entries: []const tree.Entry, height: u8) ![]u8 {
    var minimum: ?Position = null;
    for (entries) |entry| {
        const value = try decode(if (height == 0) entry.value else entry.aggregate);
        if (minimum == null or compare(value, minimum.?) == .lt) minimum = value;
    }
    return a.dupe(u8, &encode(minimum orelse return error.InvalidNativeLakeTextCorpus));
}
pub fn mutate(a: std.mem.Allocator, mutations: *corpus.text_directory.Mutations, group: corpus.FileGroup, deleting: bool) !void {
    if (group.segments.len != 1 or group.sources.len == 0) return error.InvalidNativeLakeTextCorpus;
    var minimum: ?Position = null;
    for (group.sources) |span| {
        const position: Position = .{ .file = corpus.term_directory.identity(span.file.id), .rank = span.file_lower };
        if (minimum == null or compare(position, minimum.?) == .lt) minimum = position;
    }
    _ = a;
    const bytes = encode(minimum.?);
    try mutations.addPrefix(&corpus.term_directory.identity(group.segments[0].artifact_id), if (deleting) null else &bytes, bytes.len);
}
pub fn range(a: std.mem.Allocator, storage: tree.Store, root: tree.Ref, first: [32]u8, last: [32]u8) !?Position {
    const Fold = struct {
        minimum: ?Position = null,
        fn add(self: *@This(), bytes: []const u8) !void {
            const value = try decode(bytes);
            if (self.minimum == null or compare(value, self.minimum.?) == .lt) self.minimum = value;
        }
        pub fn aggregate(self: *@This(), bytes: []const u8, _: u64) !void {
            try self.add(bytes);
        }
        pub fn record(self: *@This(), key: []const u8, bytes: []const u8) !void {
            if (key.len != 36 or std.mem.readInt(u32, key[32..36], .big) != 0) return error.InvalidNativeLakeTextCorpus;
            try self.add(bytes);
        }
    };
    var fold: Fold = .{};
    var upper: [33]u8 = undefined;
    upper[0..32].* = last;
    upper[32] = 1;
    try tree.reduceRange(a, storage, root, &first, &upper, &fold);
    return fold.minimum;
}
