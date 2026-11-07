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

//! Bounded logical requests sharing one prepared primary/index publication.
//! Inputs and provenance are owned before writer admission. Conflicting key
//! sets are separate cohorts, so request-local transforms and predicates keep
//! their ordering without exposing an intermediate, unpublished overlay.
const std = @import("std");
const types = @import("types.zig");
const effects = @import("mutation_effects.zig");
const journal = @import("derived/change_journal.zig");
const ReplayAppend = @import("../docstore.zig").DocStore.ReplayAppend;

pub const maximum_requests = ReplayAppend.maximum_records;
pub const maximum_bytes = 8 * 1024 * 1024;
pub const maximum_operations = 8192;

pub const Group = struct {
    arena: std.heap.ArenaAllocator,
    request: types.BatchRequest,
    requests: []const types.BatchRequest,
    owners: std.StringHashMapUnmanaged(usize),

    pub fn init(allocator: std.mem.Allocator, requests: []const types.BatchRequest) !Group {
        if (requests.len == 0 or requests.len > maximum_requests) return error.UnsupportedPublicationCohort;
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        const owned_requests = try a.alloc(types.BatchRequest, requests.len);
        var owners: std.StringHashMapUnmanaged(usize) = .empty;
        var writes: std.ArrayList(types.BatchWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var transforms: std.ArrayList(types.DocumentTransform) = .empty;
        var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
        var bytes: usize = 0;
        var operations: usize = 0;
        for (requests, 0..) |request, ordinal| {
            try requireCompatible(request, requests[0]);
            const weight = try measure(request);
            operations +|= weight.operations;
            bytes +|= weight.bytes;
            if (bytes > maximum_bytes or operations > maximum_operations) return error.UnsupportedPublicationCohort;
            // The parent arena owns these copies. Child teardown is unnecessary
            // and would only defer their reclamation to that same parent.
            const owned = try effects.OwnedRequest.init(a, request.writes, request.deletes, request.transforms);
            var next = request;
            next.writes = owned.writes;
            next.deletes = owned.deletes;
            next.transforms = owned.transforms;
            const checks = try a.alloc(types.TransactionVersionPredicate, request.predicates.len);
            for (request.predicates, checks) |predicate, *copy| {
                copy.* = predicate;
                copy.key = try a.dupe(u8, predicate.key);
            }
            next.predicates = checks;
            for (next.writes) |write| try ownKey(a, &owners, write.key, ordinal);
            for (next.deletes) |key| try ownKey(a, &owners, key, ordinal);
            for (next.transforms) |transform| try ownKey(a, &owners, transform.key, ordinal);
            for (next.predicates) |predicate| try ownKey(a, &owners, predicate.key, ordinal);
            owned_requests[ordinal] = next;
            try writes.appendSlice(a, next.writes);
            try deletes.appendSlice(a, next.deletes);
            try transforms.appendSlice(a, next.transforms);
            try predicates.appendSlice(a, next.predicates);
        }
        var combined = requests[0];
        combined.writes = try writes.toOwnedSlice(a);
        combined.deletes = try deletes.toOwnedSlice(a);
        combined.transforms = try transforms.toOwnedSlice(a);
        combined.predicates = try predicates.toOwnedSlice(a);
        return .{ .arena = arena, .request = combined, .requests = owned_requests, .owners = owners };
    }

    pub const Weight = struct { bytes: usize, operations: usize };
    pub fn measure(request: types.BatchRequest) !Weight {
        try requireCompatible(request, request);
        var bytes: usize = 0;
        var operations = request.writes.len +| request.deletes.len +| request.predicates.len +| request.transforms.len;
        for (request.writes) |write| {
            if (write.json_null_fields.len != 0 or (write.key.len != 0 and write.key[0] == 0)) return error.UnsupportedPublicationCohort;
            bytes +|= write.key.len +| write.value.len;
        }
        for (request.deletes) |key| {
            if (key.len != 0 and key[0] == 0) return error.UnsupportedPublicationCohort;
            bytes +|= key.len;
        }
        for (request.transforms) |transform| {
            if (transform.key.len != 0 and transform.key[0] == 0) return error.UnsupportedPublicationCohort;
            operations +|= transform.operations.len;
            bytes +|= transform.key.len;
            for (transform.operations) |operation| bytes +|= operation.path.len +| if (operation.value_json) |value| value.len else 0;
        }
        for (request.predicates) |predicate| {
            if (predicate.key.len != 0 and predicate.key[0] == 0) return error.UnsupportedPublicationCohort;
            bytes +|= predicate.key.len;
        }
        if (bytes > maximum_bytes or operations > maximum_operations or operations == 0) return error.UnsupportedPublicationCohort;
        return .{ .bytes = bytes, .operations = operations };
    }

    fn ownKey(a: std.mem.Allocator, owners: *std.StringHashMapUnmanaged(usize), key: []const u8, ordinal: usize) !void {
        if (key.len != 0 and key[0] == 0) return error.UnsupportedPublicationCohort;
        const entry = try owners.getOrPut(a, key);
        if (entry.found_existing and entry.value_ptr.* != ordinal) return error.UnsupportedPublicationCohort;
        entry.value_ptr.* = ordinal;
    }

    fn requireCompatible(request: types.BatchRequest, first: types.BatchRequest) !void {
        if (request.timestamp_ns != first.timestamp_ns or request.sync_level != first.sync_level) return error.UnsupportedPublicationCohort;
        inline for (comptime std.meta.fieldNames(types.BatchRequest)) |name| {
            if (comptime std.mem.eql(u8, name, "writes") or std.mem.eql(u8, name, "deletes") or std.mem.eql(u8, name, "transforms") or std.mem.eql(u8, name, "predicates") or std.mem.eql(u8, name, "timestamp_ns") or std.mem.eql(u8, name, "sync_level")) continue;
            const value = @field(request, name);
            switch (@typeInfo(@TypeOf(value))) {
                .optional => if (value != null) return error.UnsupportedPublicationCohort,
                .pointer => if (value.len != 0) return error.UnsupportedPublicationCohort,
                .bool => if (value) return error.UnsupportedPublicationCohort,
                .int => if (value != 0) return error.UnsupportedPublicationCohort,
                else => @compileError("publication cohorts require an explicit policy for BatchRequest." ++ name),
            }
        }
    }

    pub fn deinit(self: *Group) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Retain every logical source record. A source with no net derived effect
    /// still gets its sequence, while source authority is stamped only for its
    /// own documents. Unexpected generated effects decline the cohort before
    /// primary publication rather than inventing their provenance.
    pub fn prepareJournal(self: *const Group, allocator: std.mem.Allocator, payload: []const u8, sequences: []const u64) !Journal {
        if (sequences.len != self.requests.len) return error.InvalidBatchRequest;
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        var decoded = try journal.decodeRecord(a, payload);
        defer decoded.deinit();
        if (decoded.record.changed_artifact_keys.len != 0) return error.UnsupportedPublicationCohort;
        const records = try a.alloc(ReplayAppend.Record, sequences.len);
        const changed = try self.distributeKeys(a, decoded.record.changed_doc_keys);
        const deleted = try self.distributeKeys(a, decoded.record.deleted_doc_keys);
        const overwritten = try self.distributeKeys(a, decoded.record.overwritten_doc_keys);
        var total_bytes: usize = 0;
        for (sequences, 0..) |sequence, ordinal| {
            var record = decoded.record;
            record.sequence = sequence;
            record.changed_doc_keys = changed[ordinal];
            record.deleted_doc_keys = deleted[ordinal];
            record.overwritten_doc_keys = overwritten[ordinal];
            const encoded = try journal.encodeRecord(a, record);
            total_bytes +|= encoded.len;
            records[ordinal] = .{ .sequence = sequence, .payload = encoded };
        }
        const result: Journal = .{ .arena = arena, .records = records, .bytes = total_bytes };
        try result.append().validate();
        return result;
    }

    fn distributeKeys(self: *const Group, a: std.mem.Allocator, keys: []const []const u8) ![]const []const []const u8 {
        const buckets = try a.alloc(std.ArrayList([]const u8), self.requests.len);
        for (buckets) |*bucket| bucket.* = .empty;
        // One ownership lookup per effect, independent of the number of
        // requests. Never rescan the whole combined record for every origin.
        for (keys) |key| {
            const owner = self.owners.get(key) orelse return error.UnsupportedPublicationCohort;
            try buckets[owner].append(a, key);
        }
        const result = try a.alloc([]const []const u8, buckets.len);
        for (buckets, result) |*bucket, *out| out.* = try bucket.toOwnedSlice(a);
        return result;
    }
};

pub const Journal = struct {
    arena: std.heap.ArenaAllocator,
    records: []const ReplayAppend.Record,
    bytes: usize,
    pub fn append(self: *const Journal) ReplayAppend {
        const last = self.records[self.records.len - 1];
        return .{ .sequence = last.sequence, .payload = last.payload, .preceding = self.records[0 .. self.records.len - 1] };
    }
    pub fn deinit(self: *Journal) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

test "publication cohort owns disjoint logical inputs and journal provenance" {
    const a = std.testing.allocator;
    var input = [_]u8{'a'};
    var group = try Group.init(a, &.{
        .{ .writes = &.{.{ .key = &input, .value = "{\"n\":1}" }} },
        .{ .deletes = &.{"日本語"}, .predicates = &.{.{ .key = "日本語", .expected_version = 7 }} },
    });
    defer group.deinit();
    input[0] = 'z';
    try std.testing.expectEqualStrings("a", group.request.writes[0].key);
    const payload = try journal.encodeRecord(a, .{ .sequence = 11, .changed_doc_keys = &.{"a"}, .deleted_doc_keys = &.{"日本語"}, .overwritten_doc_keys = &.{"a"}, .target_hints = &.{.full_text} });
    defer a.free(payload);
    var prepared = try group.prepareJournal(a, payload, &.{ 5, 11 });
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.records.len);
    var first = try journal.decodeRecord(a, prepared.records[0].payload);
    defer first.deinit();
    var second = try journal.decodeRecord(a, prepared.records[1].payload);
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 5), first.record.sequence);
    try std.testing.expectEqualStrings("a", first.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 0), first.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(u64, 11), second.record.sequence);
    try std.testing.expectEqualStrings("日本語", second.record.deleted_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 0), second.record.changed_doc_keys.len);
    try std.testing.expectError(error.InvalidBatchRequest, group.prepareJournal(a, payload, &.{ 11, 5 }));
}

test "publication cohort splits conflicts private controls and incompatible visibility" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedPublicationCohort, Group.init(a, &.{ .{ .writes = &.{.{ .key = "same", .value = "{}" }} }, .{ .deletes = &.{"same"} } }));
    try std.testing.expectError(error.UnsupportedPublicationCohort, Group.init(a, &.{ .{ .writes = &.{.{ .key = "a", .value = "{}" }} }, .{ .sync_level = .full_text } }));
    try std.testing.expectError(error.UnsupportedPublicationCohort, Group.init(a, &.{.{ .activate_range_tracking = true }}));
}

test "publication cohort bounds a full request group and partitions every logical effect" {
    const a = std.testing.allocator;
    var inputs = std.heap.ArenaAllocator.init(a);
    defer inputs.deinit();
    const scratch = inputs.allocator();
    var requests: [maximum_requests]types.BatchRequest = undefined;
    var keys: std.ArrayList([]const u8) = .empty;
    for (&requests, 0..) |*request, origin| {
        const writes = try scratch.alloc(types.BatchWrite, maximum_operations / maximum_requests);
        for (writes, 0..) |*write, row| {
            const key = try std.fmt.allocPrint(scratch, "{d}/{d}", .{ origin, row });
            write.* = .{ .key = key, .value = "{}" };
            try keys.append(scratch, key);
        }
        request.* = .{ .writes = writes };
    }
    var group = try Group.init(a, &requests);
    defer group.deinit();
    const payload = try journal.encodeRecord(a, .{ .sequence = maximum_requests, .changed_doc_keys = keys.items, .target_hints = &.{.full_text} });
    defer a.free(payload);
    var sequences: [maximum_requests]u64 = undefined;
    for (&sequences, 0..) |*sequence, ordinal| sequence.* = ordinal + 1;
    var prepared = try group.prepareJournal(a, payload, &sequences);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, maximum_requests), prepared.records.len);
    var documents: usize = 0;
    for (prepared.records, 0..) |record, origin| {
        var decoded = try journal.decodeRecord(a, record.payload);
        defer decoded.deinit();
        try std.testing.expectEqual(@as(usize, maximum_operations / maximum_requests), decoded.record.changed_doc_keys.len);
        for (decoded.record.changed_doc_keys) |key| try std.testing.expectEqual(origin, group.owners.get(key).?);
        documents += decoded.record.changed_doc_keys.len;
    }
    try std.testing.expectEqual(@as(usize, maximum_operations), documents);
    const oversized = try scratch.alloc(types.BatchRequest, maximum_requests + 1);
    @memset(oversized, requests[0]);
    try std.testing.expectError(error.UnsupportedPublicationCohort, Group.init(a, oversized));
}
