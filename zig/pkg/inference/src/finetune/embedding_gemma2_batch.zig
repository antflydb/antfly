// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical candidate pools and validity masks shared by loss and replay.
//! Tokens and identity strings are borrowed until the logical batch commits.
const std = @import("std");
const data = @import("embedding_gemma2_data.zig");

pub const Sequence = struct { ids: []const i32, task: data.Task };
pub const Candidate = struct { document: data.EncodedDocument, task: data.Task };
pub const Batch = struct {
    allocator: std.mem.Allocator,
    queries: []Sequence,
    candidates: []Candidate,
    positive_indices: []i32,
    /// Row-major query × unique candidate, positive remains allowed.
    validity: []u8,

    pub fn deinit(self: *Batch) void {
        self.allocator.free(self.queries);
        self.allocator.free(self.candidates);
        self.allocator.free(self.positive_indices);
        self.allocator.free(self.validity);
        self.* = undefined;
    }
};

fn findCandidate(candidates: []const Candidate, document: data.EncodedDocument) !?usize {
    for (candidates, 0..) |candidate, i| {
        const same_id = std.mem.eql(u8, candidate.document.id, document.id);
        const same_content = std.mem.eql(u8, candidate.document.content_sha256, document.content_sha256);
        if (same_id and !same_content) return error.CandidateIdentityMismatch;
        if (same_content) {
            if (!std.mem.eql(i32, candidate.document.input_ids, document.input_ids)) return error.CandidateTokenMismatch;
            return i;
        }
    }
    return null;
}

pub fn prepare(a: std.mem.Allocator, records: []const data.Record, max_negatives: usize) !Batch {
    if (records.len == 0 or records.len > 1024 or max_negatives > 16) return error.InvalidContrastiveBatch;
    var candidates = std.ArrayListUnmanaged(Candidate).empty;
    errdefer candidates.deinit(a);
    const queries = try a.alloc(Sequence, records.len);
    errdefer a.free(queries);
    const targets = try a.alloc(i32, records.len);
    errdefer a.free(targets);
    for (records, 0..) |record, q| {
        for (records[0..q]) |prior| {
            if (std.mem.eql(u8, prior.query_id, record.query_id) or std.mem.eql(u8, prior.query_content_sha256, record.query_content_sha256))
                return error.DuplicateContrastiveQuery;
        }
        queries[q] = .{ .ids = record.query_ids, .task = record.task };
        if (try findCandidate(candidates.items, record.positive)) |index| {
            targets[q] = @intCast(index);
        } else {
            targets[q] = @intCast(candidates.items.len);
            try candidates.append(a, .{ .document = record.positive, .task = record.task });
        }
    }
    for (records) |record| {
        if (record.negatives.len > max_negatives) return error.InvalidContrastiveBatch;
        for (record.negatives) |negative| {
            if (try findCandidate(candidates.items, negative) == null)
                try candidates.append(a, .{ .document = negative, .task = record.task });
        }
    }
    const validity = try a.alloc(u8, try std.math.mul(usize, records.len, candidates.items.len));
    errdefer a.free(validity);
    @memset(validity, 1);
    for (records, 0..) |record, q| {
        const positive: usize = @intCast(targets[q]);
        const row = validity[q * candidates.items.len ..][0..candidates.items.len];
        for (record.known_positive_ids) |known| {
            // Follow aliases that deduplication removed from the pool, too.
            for (records) |other| {
                if (std.mem.eql(u8, known, other.positive.id)) {
                    if (try findCandidate(candidates.items, other.positive)) |index| if (index != positive) {
                        row[index] = 0;
                    };
                }
                for (other.negatives) |n| if (std.mem.eql(u8, known, n.id)) {
                    if (try findCandidate(candidates.items, n)) |index| if (index != positive) {
                        row[index] = 0;
                    };
                };
            }
            for (candidates.items, 0..) |candidate, index|
                if (index != positive and std.mem.eql(u8, known, candidate.document.id)) {
                    row[index] = 0;
                };
        }
        row[positive] = 1;
        var count: usize = 0;
        for (row) |allowed| count += allowed;
        if (count < 2) return error.ContrastiveBatchHasNoNegatives;
    }
    return .{ .allocator = a, .queries = queries, .candidates = try candidates.toOwnedSlice(a), .positive_indices = targets, .validity = validity };
}

pub const Padded = struct {
    allocator: std.mem.Allocator,
    batch: u32,
    sequence: u32,
    ids: []i32,
    mask: []i32,
    token_count: u64,
    pub fn deinit(self: *Padded) void {
        self.allocator.free(self.ids);
        self.allocator.free(self.mask);
        self.* = undefined;
    }
};

pub fn pad(a: std.mem.Allocator, sequences: []const Sequence, maximum: u32) !Padded {
    if (sequences.len == 0 or sequences.len > 2 or maximum > 512) return error.InvalidEncoderMicrobatch;
    var length: usize = 0;
    for (sequences) |sequence| {
        if (sequence.ids.len < 2 or sequence.ids.len > maximum) return error.TrainingSequenceTooLong;
        length = @max(length, sequence.ids.len);
    }
    const ids = try a.alloc(i32, sequences.len * length);
    errdefer a.free(ids);
    const mask = try a.alloc(i32, ids.len);
    @memset(ids, 0);
    @memset(mask, 0);
    var tokens: u64 = 0;
    for (sequences, 0..) |sequence, row| {
        @memcpy(ids[row * length ..][0..sequence.ids.len], sequence.ids);
        @memset(mask[row * length ..][0..sequence.ids.len], 1);
        tokens += sequence.ids.len;
    }
    return .{ .allocator = a, .batch = @intCast(sequences.len), .sequence = @intCast(length), .ids = ids, .mask = mask, .token_count = tokens };
}

fn doc(id: []const u8, hash: []const u8) data.EncodedDocument {
    return .{ .id = id, .content_sha256 = hash, .input_ids = &.{ 2, 7, 1 } };
}
fn testRecord(id: []const u8, positive: data.EncodedDocument, negatives: []const data.EncodedDocument) data.Record {
    return .{ .query_id = id, .query_content_sha256 = id, .query_ids = &.{ 2, 8, 1 }, .positive = positive, .negatives = negatives, .known_positive_ids = &.{}, .task = .retrieval, .split = .train, .group_id = "" };
}
test "EG2 batch deduplicates shared positives and negative aliases" {
    const records = [_]data.Record{ testRecord("q1", doc("p1", "hash1"), &.{doc("p2", "hash2")}), testRecord("q2", doc("p2", "hash2"), &.{doc("p1", "hash1")}) };
    var batch = try prepare(std.testing.allocator, &records, 1);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 2), batch.candidates.len);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1 }, batch.positive_indices);
    try std.testing.expectEqualSlices(u8, &.{ 1, 1, 1, 1 }, batch.validity);
}
test "EG2 batch masks known alternate positives and rejects conflicting identities" {
    var first = testRecord("q1", doc("p1", "hash1"), &.{doc("n", "hash3")});
    first.known_positive_ids = &.{"p2"};
    const second = testRecord("q2", doc("p2", "hash2"), &.{});
    var batch = try prepare(std.testing.allocator, &.{ first, second }, 1);
    defer batch.deinit();
    try std.testing.expectEqual(@as(u8, 0), batch.validity[1]);
    try std.testing.expectEqual(@as(u8, 1), batch.validity[2]);
    try std.testing.expectError(error.CandidateIdentityMismatch, prepare(std.testing.allocator, &.{ first, testRecord("q2", doc("p1", "wrong"), &.{}) }, 1));
}
test "EG2 microbatch padding trims to active maximum and retains masks" {
    var padded = try pad(std.testing.allocator, &.{ .{ .ids = &.{ 2, 5, 1 }, .task = .retrieval }, .{ .ids = &.{ 2, 1 }, .task = .code_retrieval } }, 512);
    defer padded.deinit();
    try std.testing.expectEqual(@as(u32, 3), padded.sequence);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 1, 1, 1, 0 }, padded.mask);
    try std.testing.expectEqual(@as(u64, 5), padded.token_count);
}
