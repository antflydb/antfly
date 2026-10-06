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

//! Receiver verification for bounded chunk-vector stream census pages.
//! A sender's local checkpoint is not evidence. Recompute each proposed page
//! against accepted local provenance and a caller-supplied durable predecessor.
//! This read-only primitive neither closes a stream nor discharges obligations.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const checkpoints = @import("artifact_stream_checkpoint.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const vectors = @import("artifact_chunk_vector_publication.zig");
const candidates = @import("artifact_chunk_vector_cursor.zig");
const Request = @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest;
const Plan = @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot;

/// No physical-root identity or local checkpoint key crosses this boundary.
/// An ordered driver must bind this progress to its catalog producer identity.
pub const Progress = struct {
    observation: Observation,
    cursor: []const u8 = "",
    scan_cursor: []const u8 = "",
    logical_scan_cursor: []const u8 = "",
    members: u64 = 0,
    chain: publication.Digest = @splat(0),
    enumerated: bool = false,

    pub fn validate(self: Progress) !void {
        if ((self.members == 0) != (self.cursor.len == 0) or
            (self.members == 0 and !std.mem.allEqual(u8, &self.chain, 0)) or
            self.cursor.len > checkpoints.max_cursor_bytes or self.scan_cursor.len > checkpoints.max_cursor_bytes or
            (self.enumerated and (self.scan_cursor.len != 0 or self.logical_scan_cursor.len != 0))) return error.InvalidBatchRequest;
        if (self.logical_scan_cursor.len != 0) _ = try @import("artifact_chunk_scan_position.zig").Position.decode(self.logical_scan_cursor);
    }

    fn append(self: *Progress, key: []const u8, accepted_digest: publication.Digest) !void {
        return checkpoints.appendMember(self, key, accepted_digest);
    }
    fn clone(self: Progress, alloc: std.mem.Allocator) !Progress {
        var result = self;
        result.cursor = try alloc.dupe(u8, self.cursor);
        result.scan_cursor = try alloc.dupe(u8, self.scan_cursor);
        result.logical_scan_cursor = try alloc.dupe(u8, self.logical_scan_cursor);
        return result;
    }
    pub fn digest(self: Progress) !publication.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:verified-stream-census:v1:");
        hash.update(&self.observation.authority.namespace);
        number(&hash, self.observation.authority.epoch);
        hash.update(&self.observation.authority.catalog_digest);
        hash.update(&self.observation.document_digest);
        hash.update(&.{@intFromBool(self.observation.revision != null)});
        if (self.observation.revision) |revision| hash.update(&try revision.encode());
        hash.update(&.{@intFromBool(self.observation.foreign_inputs)});
        // Owner-local progress survives unrelated writes. Once a foreign
        // dependency participates, the pinned mutation epoch becomes causal.
        if (self.observation.foreign_inputs) number(&hash, self.observation.validation_epoch);
        for ([_][]const u8{ self.cursor, self.scan_cursor, self.logical_scan_cursor }) |key| {
            number(&hash, key.len);
            hash.update(key);
        }
        number(&hash, self.members);
        hash.update(&self.chain);
        hash.update(&.{@intFromBool(self.enumerated)});
        var result: publication.Digest = undefined;
        hash.final(&result);
        return result;
    }
};
fn number(hash: *std.crypto.hash.Blake3, value: u64) void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, value, .little);
    hash.update(&raw);
}

pub fn boundDigest(request: Request, plan: *const Plan, progress: Progress) !publication.Digest {
    const generation = plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift;
    if (generation == 0) return error.ArtifactCatalogDrift;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:stream-census-producer:v1:");
    for ([_][]const u8{ @tagName(request.kind), request.index_name, request.artifact_name, @import("enrichment/enrichment_types.zig").requestEmbeddingName(request) }) |name| {
        number(&hash, name.len);
        hash.update(name);
    }
    number(&hash, generation);
    hash.update(&try progress.digest());
    var digest: publication.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub const Limits = struct {
    visits: usize = 128,
    bytes: usize = 64 * 1024,
    pub fn validate(self: Limits) !void {
        if (self.visits == 0 or self.visits > 128 or self.bytes == 0 or self.bytes > 64 * 1024) return error.InvalidBatchRequest;
    }
};
pub const Claim = struct { before: publication.Digest, after: publication.Digest };
pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    progress: Progress,
    claim: Claim,
    visits: usize,
    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// The caller pins txn and the immutable plan for this call only. Provider
/// invocation is deliberately impossible here; every member needs a current
/// accepted receipt, including explicit absence/obsolete-output retirement.
/// Deterministic work limits let a receiver reproduce the exact page boundary.
pub fn requireStreamPlan(alloc: std.mem.Allocator, txn: anytype, request: Request, plan: *const Plan, authority: publication.Authority) !void {
    _ = try captureSession(alloc, txn, request, plan, authority);
}

fn captureSession(alloc: std.mem.Allocator, txn: anytype, request: Request, plan: *const Plan, authority: publication.Authority) !vectors.CaptureSession(@TypeOf(txn)) {
    if (request.doc_key.len == 0 or request.doc_key.len > checkpoints.max_cursor_bytes) return error.InvalidBatchRequest;
    if ((request.kind != .dense_embedding and request.kind != .sparse_embedding) or request.input_kind != .materialized_chunks or
        request.embedding_input != .text or request.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    // Validate even empty streams, then share authorization across this page.
    const bound = (try vectors.captureSession(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(bound.authority, authority)) return error.ArtifactCatalogDrift;
    return bound;
}

pub fn scan(alloc: std.mem.Allocator, txn: anytype, request: Request, plan: *const Plan, start: Progress, limits: Limits) !Page {
    try limits.validate();
    try start.validate();
    if (start.enumerated) return error.InvalidBatchRequest;
    try start.observation.requireCurrent(txn, request.doc_key);
    const captures = try captureSession(alloc, txn, request, plan, start.observation.authority);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var result = try start.clone(owned);
    const Txn = @typeInfo(@TypeOf(txn)).pointer.child;
    var cursor = try candidates.Cursor(Txn).open(alloc, txn, request.doc_key, request.artifact_name, @import("enrichment/enrichment_types.zig").requestEmbeddingName(request));
    defer cursor.close();
    if (start.cursor.len != 0) try cursor.seekAfter(start.cursor);
    if (start.scan_cursor.len != 0) try cursor.resumePhysical(start.scan_cursor);
    if (start.logical_scan_cursor.len != 0) try cursor.resumeLogical(start.logical_scan_cursor);
    var budget: candidates.Budget = .{ .max_visits = limits.visits, .max_bytes = limits.bytes };
    while (true) switch (try cursor.poll(&budget)) {
        .end => {
            // EOF alone cannot distinguish a finished empty producer from
            // one that has not published. The root manifest also carries
            // inherited inputs that no member can expose for an empty set.
            // Unit/DAG closure remains a separate activation requirement.
            var upstream = try @import("artifact_chunk_publication.zig").readAcceptedRoot(alloc, txn, request.doc_key, request.artifact_name);
            defer upstream.deinit();
            try result.observation.observeProof(request.doc_key, upstream.proof);
            result.enumerated = true;
            result.scan_cursor = "";
            result.logical_scan_cursor = "";
            break;
        },
        .logical_yielded => |position| {
            result.logical_scan_cursor = try owned.dupe(u8, position);
            break;
        },
        .yielded => |position| {
            result.scan_cursor = try owned.dupe(u8, position);
            break;
        },
        .member => |key| {
            var input = try captures.capture(alloc, key);
            defer input.deinit();
            var accepted = (try input.token.acceptedProof(alloc, txn)) orelse {
                // Keep a verified prefix, but never persist the cursor after
                // the unaccepted member. The next ordered page must revisit
                // that exact scope instead of repeatedly scanning the prefix.
                if (result.members != start.members) break;
                return error.ArtifactPublicationPending;
            };
            defer accepted.deinit();
            try result.observation.observeProof(request.doc_key, accepted.owned.proof);
            try result.append(try owned.dupe(u8, key), accepted.owned.proof.publication_digest);
            result.logical_scan_cursor = try cursor.checkpointLogicalAlloc(owned);
            if (budget.exhausted()) break;
        },
    };
    try result.observation.requireCurrent(txn, request.doc_key);
    return .{ .arena = arena, .progress = result, .claim = .{ .before = try boundDigest(request, plan, start), .after = try boundDigest(request, plan, result) }, .visits = budget.visits };
}

/// `durable_start` must come from the receiver's ordered progress registry,
/// never from the sender's claim or its receiver-local census checkpoint.
/// The future commit stage must recheck the input witness and predecessor CAS
/// in the writer; this owned result alone does not authorize completion.
pub fn verify(alloc: std.mem.Allocator, txn: anytype, request: Request, plan: *const Plan, durable_start: Progress, limits: Limits, claimed: Claim) !Page {
    if (!std.mem.eql(u8, &try boundDigest(request, plan, durable_start), &claimed.before)) return error.EnrichmentSourceChanged;
    var page = try scan(alloc, txn, request, plan, durable_start, limits);
    errdefer page.deinit();
    if (!std.mem.eql(u8, &page.claim.after, &claimed.after)) return error.InvalidBatchRequest;
    return page;
}

test "ordered artifact inventory census claims bind causal epochs and framed progress" {
    var progress: Progress = .{ .observation = .{
        .authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) },
        .document_digest = @splat(3),
        .revision = .{ .raft = .{ .term = 1, .index = 4 } },
        .validation_epoch = 7,
    } };
    const initial = try progress.digest();
    progress.observation.validation_epoch += 1;
    try std.testing.expectEqualDeep(initial, try progress.digest());
    progress.observation.foreign_inputs = true;
    const foreign = try progress.digest();
    progress.observation.validation_epoch += 1;
    try std.testing.expect(!std.mem.eql(u8, &foreign, &try progress.digest()));
    progress.observation.foreign_inputs = false;
    try progress.append("scope\x00", @splat(4));
    try progress.validate();
    const one = try progress.digest();
    progress.enumerated = true;
    try std.testing.expect(!std.mem.eql(u8, &one, &try progress.digest()));
    try std.testing.expectError(error.InvalidBatchRequest, progress.append("tail", @splat(5)));
    progress.scan_cursor = "ignored";
    try std.testing.expectError(error.InvalidBatchRequest, progress.validate());
    progress.enumerated = false;
    progress.cursor = "ab";
    progress.scan_cursor = "c";
    const framed = try progress.digest();
    progress.cursor = "a";
    progress.scan_cursor = "bc";
    try std.testing.expect(!std.mem.eql(u8, &framed, &try progress.digest()));
    try std.testing.expectError(error.InvalidBatchRequest, (Limits{ .visits = 0 }).validate());
    try std.testing.expectError(error.InvalidBatchRequest, (Limits{ .bytes = 64 * 1024 + 1 }).validate());
}
