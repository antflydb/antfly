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

//! Row-location translation on an already-pinned read scope. Keep every native
//! vector/score fast path, batch location probes and fetch projected heap ranges
//! in physical order without materializing complete row payloads.
const std = @import("std");
const erased = @import("backend_erased.zig");
const types = @import("backend_types.zig");
const keys = @import("internal_keys.zig");
const heap = @import("row_heap.zig");
const A = std.mem.Allocator;
const Scope = erased.ReadScope;
const VTable = @typeInfo(@FieldType(Scope, "vtable")).pointer.child;
const Range = erased.ValueRange;
const Request = erased.RangeRequest;
const Plan = @import("prepared_score_plan.zig").PreparedScorePlan;
const Prepared = @import("prepared_score_request.zig").PreparedScoreRequest;

pub fn wrap(a: A, backing: Scope) !Scope {
    const box = try a.create(Box);
    box.* = .{ .a = a, .backing = backing, .vtable = .{
        .get = Box.get,
        .get_many_sorted = Box.many,
        .get_into = Box.into,
        .get_many_sorted_into = Box.manyInto,
        .get_ranges = Box.ranges,
        .get_ranges_many = Box.rangesMany,
        .get_range = if (backing.supportsRange()) Box.range else null,
        .close = Box.close,
        .reset = if (backing.vtable.reset != null) Box.reset else null,
        .begin_score_batch = if (backing.vtable.begin_score_batch != null) Box.beginScoreBatch else null,
        .reset_score_batch = if (backing.vtable.reset_score_batch != null) Box.resetScoreBatch else null,
        .working_allocator = Box.working,
        .prepared_score_allocator = if (backing.vtable.prepared_score_allocator != null) Box.preparedAllocator else null,
        .prepared_score_plan = if (backing.vtable.prepared_score_plan != null) Box.preparedPlan else null,
        .visit_prepared_request = if (backing.vtable.visit_prepared_request != null) Box.visitPreparedRequest else null,
        .visit_prepared_scores = if (backing.vtable.visit_prepared_scores != null) Box.visitPreparedScores else null,
        .get_many_sorted_scores = if (backing.vtable.get_many_sorted_scores != null) Box.scores else null,
        .visit_many_sorted_scores = if (backing.vtable.visit_many_sorted_scores != null) Box.visitScores else null,
        .read_sorted_score_column = if (backing.vtable.read_sorted_score_column != null) Box.scoreColumn else null,
    } };
    return .{ .allocator = a, .ptr = box, .vtable = &box.vtable };
}
const Box = struct {
    a: A,
    vtable: VTable,
    backing: Scope,
    fn cast(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn get(raw: *anyopaque, key: []const u8) anyerror![]const u8 {
        const self = cast(raw);
        return heap.resolve(&self.backing, key, try self.backing.get(key));
    }
    fn many(raw: *anyopaque, logical: []const []const u8, values: []?[]const u8) anyerror!void {
        const self = cast(raw);
        try self.backing.getManySorted(logical, values);
        try heap.resolveMany(self.a, &self.backing, logical, values);
    }
    fn into(raw: *anyopaque, key: []const u8, out: []u8) anyerror![]const u8 {
        const self = cast(raw);
        if (!keys.isStoredDocumentRowKey(key)) return self.backing.getInto(key, out);
        const value = try get(raw, key);
        if (value.len > out.len) return error.BufferTooSmall;
        @memcpy(out[0..value.len], value);
        return out[0..value.len];
    }
    fn manyInto(raw: *anyopaque, logical: []const []const u8, out: []const []u8, values: []?[]const u8) anyerror!void {
        const self = cast(raw);
        const contains_row = for (logical) |key| {
            if (keys.isStoredDocumentRowKey(key)) break true;
        } else false;
        if (!contains_row) return self.backing.getManySortedInto(logical, out, values);
        if (out.len != logical.len or values.len != logical.len) return error.InvalidBatch;
        try many(raw, logical, values);
        for (out, values) |buffer, *value| if (value.*) |body| {
            if (body.len > buffer.len) return error.BufferTooSmall;
            @memcpy(buffer[0..body.len], body);
            value.* = buffer[0..body.len];
        };
    }
    fn bounds(ref: heap.Reference, ranges_: []const Range) !void {
        for (ranges_) |requested| if (requested.offset > ref.value_len or requested.length > ref.value_len - requested.offset) return error.EndOfStream;
    }
    fn locate(self: *@This(), key: []const u8) !?heap.Reference {
        if (!keys.isStoredDocumentRowKey(key)) return null;
        if (self.backing.supportsRange()) {
            const prefix = self.backing.getRange(key, 0, heap.magic.len) catch |err| switch (err) {
                error.EndOfStream => return null,
                else => return err,
            };
            if (!heap.isReference(prefix)) return null;
        }
        return heap.decode(key, try self.backing.get(key));
    }
    fn ranges(raw: *anyopaque, key: []const u8, ranges_: []const Range, output: [][]const u8) anyerror!void {
        const self = cast(raw);
        if (try self.locate(key)) |ref| {
            try bounds(ref, ranges_);
            const physical = ref.physicalKey();
            return self.backing.getRanges(&physical, ranges_, output) catch |err| switch (err) {
                error.NotFound => error.DanglingRowHeapReference,
                else => err,
            };
        }
        return self.backing.getRanges(key, ranges_, output);
    }
    fn range(raw: *anyopaque, key: []const u8, offset: u64, length: usize) anyerror![]const u8 {
        const self = cast(raw);
        if (try self.locate(key)) |ref| {
            try bounds(ref, &.{.{ .offset = offset, .length = length }});
            const physical = ref.physicalKey();
            return self.backing.getRange(&physical, offset, length) catch |err| switch (err) {
                error.NotFound => error.DanglingRowHeapReference,
                else => err,
            };
        }
        return self.backing.getRange(key, offset, length);
    }
    fn less(_: void, a: Request, b: Request) bool {
        return std.mem.order(u8, a.key, b.key) == .lt;
    }
    fn rangesMany(raw: *anyopaque, requests: []const Request) anyerror!void {
        const self = cast(raw);
        var count: usize = 0;
        for (requests) |request| if (keys.isStoredDocumentRowKey(request.key)) {
            count += 1;
        };
        if (count == 0) return self.backing.getRangesMany(requests);
        var arena: std.heap.ArenaAllocator = .init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const translated = try a.dupe(Request, requests);
        const headers = try a.alloc(Request, count);
        const output = try a.alloc([1][]const u8, count);
        const indices = try a.alloc(usize, count);
        const prefix_range = [_]Range{.{ .offset = 0, .length = heap.magic.len }};
        var n: usize = 0;
        for (requests, 0..) |request, i| if (keys.isStoredDocumentRowKey(request.key)) {
            headers[n] = .{ .key = request.key, .ranges = &prefix_range, .output = &output[n] };
            indices[n] = i;
            n += 1;
        };
        // Short inline JSON can be smaller than the location prefix. Preserve
        // its ordinary range behavior and translate the rest individually.
        self.backing.getRangesMany(headers) catch |err| switch (err) {
            error.EndOfStream => {
                for (requests) |request| try ranges(raw, request.key, request.ranges, request.output);
                return;
            },
            else => return err,
        };
        const refs = try a.alloc(?heap.Reference, requests.len);
        @memset(refs, null);
        const reference_keys = try a.alloc([]const u8, count);
        const reference_indices = try a.alloc(usize, count);
        var reference_count: usize = 0;
        for (output, indices) |prefix, original| if (heap.isReference(prefix[0])) {
            reference_keys[reference_count] = requests[original].key;
            reference_indices[reference_count] = original;
            reference_count += 1;
        };
        const values = try a.alloc(?[]const u8, reference_count);
        try self.backing.getManySorted(reference_keys[0..reference_count], values);
        for (values, reference_indices[0..reference_count]) |value, original| {
            const bytes = value orelse return error.DanglingRowHeapReference;
            refs[original] = (try heap.decode(requests[original].key, bytes)) orelse return error.CorruptRowHeapReference;
        }
        const physical = try a.alloc([heap.key_len]u8, reference_count);
        var physical_count: usize = 0;
        for (refs, translated) |optional, *request| if (optional) |ref| {
            try bounds(ref, request.ranges);
            physical[physical_count] = ref.physicalKey();
            request.key = &physical[physical_count];
            physical_count += 1;
        };
        // Output slices still address each original caller slot after sorting.
        std.mem.sort(Request, translated, {}, less);
        self.backing.getRangesMany(translated) catch |err| switch (err) {
            error.NotFound => return error.DanglingRowHeapReference,
            else => return err,
        };
    }
    fn close(a: A, raw: *anyopaque) void {
        const self = cast(raw);
        self.backing.close();
        a.destroy(self);
    }
    fn reset(raw: *anyopaque) anyerror!void {
        return cast(raw).backing.reset();
    }
    fn beginScoreBatch(raw: *anyopaque) anyerror!void {
        return cast(raw).backing.beginScoreBatch();
    }
    fn resetScoreBatch(raw: *anyopaque) anyerror!void {
        return cast(raw).backing.resetScoreBatch();
    }
    fn working(raw: *anyopaque) A {
        return cast(raw).backing.workingAllocator();
    }
    fn preparedAllocator(raw: *anyopaque) A {
        return cast(raw).backing.preparedScoreAllocator();
    }
    fn preparedPlan(raw: *anyopaque) ?*Plan {
        return cast(raw).backing.preparedScorePlan();
    }
    fn visitPreparedRequest(raw: *anyopaque, request: *const Prepared, sink: types.CompleteScoreSink) anyerror!void {
        return cast(raw).backing.visitPreparedRequest(request, sink);
    }
    fn visitPreparedScores(raw: *anyopaque, plan: *Plan, sink: types.CompleteScoreSink) anyerror!void {
        return cast(raw).backing.visitPreparedScores(plan, sink);
    }
    fn scores(raw: *anyopaque, logical: []const []const u8, values: []?f64) anyerror!void {
        return cast(raw).backing.getManySortedScores(logical, values);
    }
    fn visitScores(raw: *anyopaque, logical: []const []const u8, sink: types.ScoreSink) anyerror!void {
        return cast(raw).backing.visitManySortedScores(logical, sink);
    }
    fn scoreColumn(raw: *anyopaque, prefix: []const u8, nodes: []const []const u8, values: []f64) anyerror!usize {
        return cast(raw).backing.readSortedScoreColumn(prefix, nodes, values);
    }
};

test "row heap projected scope batches physical ranges without hydrating rows and preserves native score fast paths" {
    const a = std.testing.allocator;
    const ka = try keys.documentKeyAlloc(a, "a");
    defer a.free(ka);
    const kb = try keys.documentKeyAlloc(a, "b");
    defer a.free(kb);
    const payload_a = try a.alloc(u8, 8192);
    defer a.free(payload_a);
    const payload_b = try a.alloc(u8, 8192);
    defer a.free(payload_b);
    @memset(payload_a, 'x');
    @memset(payload_b, 'y');
    @memcpy(payload_a[0..10], "ABCDEFGHIJ");
    @memcpy(payload_b[0..10], "0123456789");
    const ra = try heap.location(ka, payload_a, @splat(1), 2);
    const rb = try heap.location(kb, payload_b, @splat(1), 1);
    const Context = struct {
        ka: []const u8,
        kb: []const u8,
        ra: [heap.reference_len]u8,
        rb: [heap.reference_len]u8,
        pa: [heap.key_len]u8,
        pb: [heap.key_len]u8,
        va: []const u8,
        vb: []const u8,
        full_heap_reads: usize = 0,
        direct_into: usize = 0,
        direct_scores: usize = 0,
        resets: usize = 0,
        closed: bool = false,
        fn cast(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn value(self: *@This(), key: []const u8, full: bool) ![]const u8 {
            if (std.mem.eql(u8, key, self.ka)) return &self.ra;
            if (std.mem.eql(u8, key, self.kb)) return &self.rb;
            if (std.mem.eql(u8, key, &self.pa)) {
                if (full) self.full_heap_reads += 1;
                return self.va;
            }
            if (std.mem.eql(u8, key, &self.pb)) {
                if (full) self.full_heap_reads += 1;
                return self.vb;
            }
            if (std.mem.eql(u8, key, "vector")) return "nativefast";
            return error.NotFound;
        }
        fn get(raw: *anyopaque, key: []const u8) anyerror![]const u8 {
            return cast(raw).value(key, true);
        }
        fn range(raw: *anyopaque, key: []const u8, offset: u64, len: usize) anyerror![]const u8 {
            const bytes = try cast(raw).value(key, false);
            if (offset > bytes.len or len > bytes.len - offset) return error.EndOfStream;
            return bytes[@intCast(offset)..][0..len];
        }
        fn manyRanges(raw: *anyopaque, requests: []const Request) anyerror!void {
            for (requests) |request| for (request.ranges, request.output) |part, *out| {
                out.* = try range(raw, request.key, part.offset, part.length);
            };
        }
        fn into(raw: *anyopaque, logical: []const []const u8, buffers: []const []u8, values: []?[]const u8) anyerror!void {
            const self = cast(raw);
            self.direct_into += 1;
            for (logical, buffers, values) |key, buffer, *out| {
                const bytes = try self.value(key, true);
                if (bytes.len > buffer.len) return error.BufferTooSmall;
                @memcpy(buffer[0..bytes.len], bytes);
                out.* = buffer[0..bytes.len];
            }
        }
        fn scores(raw: *anyopaque, _: []const []const u8, values: []?f64) anyerror!void {
            cast(raw).direct_scores += 1;
            @memset(values, 42);
        }
        fn close(_: A, raw: *anyopaque) void {
            cast(raw).closed = true;
        }
        fn reset(raw: *anyopaque) anyerror!void {
            cast(raw).resets += 1;
        }
    };
    var context: Context = .{ .ka = ka, .kb = kb, .ra = ra.encode(), .rb = rb.encode(), .pa = ra.physicalKey(), .pb = rb.physicalKey(), .va = payload_a, .vb = payload_b };
    var scope = try wrap(a, .{ .allocator = a, .ptr = &context, .vtable = &.{ .get = Context.get, .get_range = Context.range, .get_ranges_many = Context.manyRanges, .get_many_sorted_into = Context.into, .get_many_sorted_scores = Context.scores, .close = Context.close, .reset = Context.reset } });
    var closed = false;
    defer if (!closed) scope.close();
    var out_a: [1][]const u8 = undefined;
    var out_b: [1][]const u8 = undefined;
    const selected = [_]Range{.{ .offset = 2, .length = 3 }};
    try scope.getRangesMany(&.{ .{ .key = ka, .ranges = &selected, .output = &out_a }, .{ .key = kb, .ranges = &selected, .output = &out_b } });
    try std.testing.expectEqualStrings("CDE", out_a[0]);
    try std.testing.expectEqualStrings("234", out_b[0]);
    try std.testing.expectEqual(@as(usize, 0), context.full_heap_reads);
    try std.testing.expectEqualStrings("GH", try scope.getRange(ka, 6, 2));
    try std.testing.expectError(error.EndOfStream, scope.getRange(ka, 8191, 2));
    try std.testing.expectEqual(@as(usize, 0), context.full_heap_reads);
    var values: [2]?[]const u8 = undefined;
    try scope.getManySorted(&.{ ka, kb }, &values);
    try std.testing.expectEqualSlices(u8, payload_a, values[0].?);
    try std.testing.expectEqualSlices(u8, payload_b, values[1].?);
    try std.testing.expectEqual(@as(usize, 2), context.full_heap_reads);
    var buffer: [16]u8 = undefined;
    var into: [1]?[]const u8 = undefined;
    try std.testing.expect(scope.supportsDirectBatchInto());
    try scope.getManySortedInto(&.{"vector"}, &.{&buffer}, &into);
    try std.testing.expectEqualStrings("nativefast", into[0].?);
    try std.testing.expectEqual(@as(usize, 1), context.direct_into);
    var scores: [1]?f64 = undefined;
    try std.testing.expect(scope.supportsDirectScores());
    try scope.getManySortedScores(&.{"vector"}, &scores);
    try std.testing.expectEqual(@as(?f64, 42), scores[0]);
    try std.testing.expectEqual(@as(usize, 1), context.direct_scores);
    try scope.reset();
    try std.testing.expectEqual(@as(usize, 1), context.resets);
    try std.testing.expectEqualStrings("nativefast", into[0].?);
    scope.close();
    closed = true;
    try std.testing.expect(context.closed);
}
