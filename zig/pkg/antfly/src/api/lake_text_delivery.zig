// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Final-result locators survive ranking; native ordinals belong only to a
//! joined pack batch. Source coverage and key validation stay with the hydrator.
const std = @import("std");
const local = @import("antfly_local_sources");
const types = local.storage_db_types;
const Wave = @import("lake_text_pack_wave.zig");
const Pin = local.storage_db_query_search_exec.PinnedTextSource;
const A = std.mem.Allocator;
pub const Location = struct { segment: [32]u8, document: u32 };
pub const Locations = std.StringHashMapUnmanaged(Location);
pub const Selection = struct {
    hits: []types.SearchHit,
    indices: ?[]const usize = null,
    pub fn len(self: Selection) usize {
        return if (self.indices) |indices| indices.len else self.hits.len;
    }
    pub fn at(self: Selection, i: usize) *types.SearchHit {
        return &self.hits[if (self.indices) |indices| indices[i] else i];
    }
};
pub const Hydrate = struct { ptr: *anyopaque, apply: *const fn (*anyopaque, A, Selection, ?*const Pin) anyerror!void };
const Winner = struct {
    position: usize,
    location: Location,
    fn less(_: void, left: Winner, right: Winner) bool {
        const order = std.mem.order(u8, &left.location.segment, &right.location.segment);
        return if (order == .eq) left.location.document < right.location.document else order == .lt;
    }
};
/// A missing locator declines the entire optimization before any result changes.
/// Sorting a permutation preserves ranked result order and opens each pack once.
pub fn run(a: A, io: ?std.Io, hits: []types.SearchHit, locations: *const Locations, source: Wave.Source, hydrate: Hydrate) !bool {
    for (hits) |hit| if (!locations.contains(hit.id)) return false;
    const winners = try a.alloc(Winner, hits.len);
    defer a.free(winners);
    const indices = try a.alloc(usize, hits.len);
    defer a.free(indices);
    for (hits, winners, 0..) |hit, *winner, i| winner.* = .{ .position = i, .location = locations.get(hit.id).? };
    std.mem.sort(Winner, winners, {}, Winner.less);
    for (winners, indices) |winner, *index| index.* = winner.position;
    var begin: usize = 0;
    while (begin < winners.len) {
        var ids: [Wave.width][32]u8 = undefined;
        var bounds: [Wave.width + 1]usize = undefined;
        var count: usize = 0;
        var end = begin;
        while (end < winners.len and count < Wave.width) {
            bounds[count] = end;
            const id = winners[end].location.segment;
            ids[count] = id;
            while (end < winners.len and std.mem.eql(u8, &winners[end].location.segment, &id)) end += 1;
            count += 1;
        }
        bounds[count] = end;
        var wave = Wave.init(source, io, ids[0..count]);
        defer wave.deinit();
        wave.start();
        for (0..count) |i| {
            defer wave.release(i);
            var retry: ?Pin = null;
            defer if (retry) |*pin| pin.deinit();
            var pin = try wave.get(i);
            if (pin == null) {
                // Speculation is optional. Retry the authoritative open so
                // integrity/cancellation failures are not hidden by fallback.
                retry = source.open(source.ptr, ids[i]) catch |err| switch (err) {
                    error.OutOfMemory, error.NativeLakeTextCacheBusy, error.NativeLakeTextCorpusTooLarge, error.ArtifactReadBudgetExceeded => null,
                    else => return err,
                };
                pin = if (retry) |*value| value else null;
            }
            const selected: Selection = .{ .hits = hits, .indices = indices[bounds[i]..bounds[i + 1]] };
            // Never let an ordinal escape the pin that establishes its meaning.
            defer {
                for (0..selected.len()) |at| {
                    const hit = selected.at(at);
                    hit.native_text_doc_id = null;
                }
            }
            for (winners[bounds[i]..bounds[i + 1]]) |winner| hits[winner.position].native_text_doc_id = if (pin != null) winner.location.document else null;
            try hydrate.apply(hydrate.ptr, a, selected, pin);
        }
        begin = end;
    }
    return true;
}

test "external lake bounded window profiles final delivery groups winning packs preserves rank and releases failed batches" {
    const State = struct {
        live: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        created: std.atomic.Value(usize) = .init(0),
        released: std.atomic.Value(usize) = .init(0),
        callbacks: usize = 0,
        fail: bool = false,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.live.fetchSub(1, .acq_rel);
            _ = self.released.fetchAdd(1, .monotonic);
        }
        fn open(raw: *anyopaque, id: [32]u8) !?Pin {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (id[0] == 2) return null;
            const a = std.heap.page_allocator;
            const key = try std.fmt.allocPrint(a, "row{d}", .{id[0]});
            defer a.free(key);
            const value = try std.fmt.allocPrint(a, "{{\"body\":\"pack{d}\",\"counter\":9007199254740993}}", .{id[0]});
            defer a.free(value);
            const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = key, .value = value }}, .{}, null)).?;
            defer a.free(bytes);
            var writer = try local.index.IndexWriter.init(a);
            defer writer.deinit();
            try writer.addSegmentWithId(1, bytes);
            _ = self.created.fetchAdd(1, .monotonic);
            _ = self.peak.fetchMax(self.live.fetchAdd(1, .acq_rel) + 1, .monotonic);
            return .{ .snapshot = writer.acquireSnapshot(), .name = "pack", .text_analysis = .{}, .runtime_schema = null, .stored_projection_fields = &.{ "body", "counter" }, .owner = self, .release_owner = release };
        }
        fn hydrate(raw: *anyopaque, a: A, selected: Selection, pin: ?*const Pin) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            if (self.fail) return error.InjectedDeliveryFailure;
            var hydrator = @import("lake_index_source_projection.zig").Hydrator.init(a);
            defer hydrator.deinit();
            for (0..selected.len()) |i| {
                const hit = selected.at(i);
                var row: std.json.Value = .{ .object = .empty };
                errdefer types.deinitJsonValue(a, &row);
                if (pin) |source| {
                    try std.testing.expectEqual(@as(?u32, 0), hit.native_text_doc_id);
                    try hydrator.append(a, source.snapshot, hit.native_text_doc_id.?, hit.id, &.{ "body", "counter" }, &row);
                } else {
                    try std.testing.expect(hit.native_text_doc_id == null);
                    try row.object.put(a, try a.dupe(u8, "body"), .{ .string = try a.dupe(u8, "physical fallback") });
                }
                hit.source_value = row;
            }
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |fail| {
        var state: State = .{ .fail = fail };
        var ids = [_][4]u8{ "row6".*, "row0".*, "row3".*, "row1".*, "row5".*, "row2".*, "row4".* };
        var hits: [7]types.SearchHit = undefined;
        var locations: Locations = .empty;
        defer locations.deinit(a);
        for (&hits, &ids) |*hit, *id| {
            hit.* = .{ .id = id };
            try locations.put(a, id, .{ .segment = @splat(id[3] - '0'), .document = 0 });
        }
        defer for (&hits) |*hit| if (hit.source_value) |*value| types.deinitJsonValue(a, value);
        const result = run(a, std.testing.io, &hits, &locations, .{ .ptr = &state, .open = State.open }, .{ .ptr = &state, .apply = State.hydrate });
        if (fail) try std.testing.expectError(error.InjectedDeliveryFailure, result) else {
            try std.testing.expect(try result);
            try std.testing.expectEqual(@as(usize, 7), state.callbacks);
            for (hits, ids) |hit, id| {
                try std.testing.expectEqualStrings(&id, hit.id);
                try std.testing.expect(hit.native_text_doc_id == null);
                if (id[3] != '2') try std.testing.expectEqual(@as(i64, 9007199254740993), hit.source_value.?.object.get("counter").?.integer);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), state.live.load(.acquire));
        try std.testing.expectEqual(state.created.load(.acquire), state.released.load(.acquire));
        try std.testing.expect(state.peak.load(.acquire) <= Wave.width);
        _ = locations.remove("row0");
        try std.testing.expect(!try run(a, std.testing.io, &hits, &locations, .{ .ptr = &state, .open = State.open }, .{ .ptr = &state, .apply = State.hydrate }));
    }
}
