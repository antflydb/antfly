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

//! Compiled producer input equality, separate from source observation clocks.
//! These are internal planning primitives, not wire proofs or authorization.
//! A writer must derive dependencies from its pinned producer definition and
//! validate the versioned causal proof before accepting any narrowed guard.
const std = @import("std");
const requests = @import("enrichment/enrichment_types.zig");
const logical = @import("../document_commitment.zig");
const Position = @import("receipt_position.zig").Position;
pub const Digest = [32]u8;
pub const Dependency = struct { field: []const u8 };

/// Literal root-field extraction has a closed read set: strings become text,
/// all other values produce no text. Canonical value equality is conservative
/// for those non-string values. Templates, units, graph/context and media keep
/// whole-source guards until their complete dependency sets are represented.
pub fn compile(request: requests.GeneratedEnrichmentRequest) ?Dependency {
    comptime {
        @setEvalBranchQuota(50_000);
        for (@typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_names) |name| {
            const reviewed = for (.{ "kind", "index_name", "artifact_name", "embedding_name", "embedding_input", "input_kind", "doc_key", "source_field", "source_template", "expected_dims", "chunk_size", "chunk_overlap", "chunker_json", "full_text_index", "persist_artifact", "independently_required", "content_type", "producer_json", "neighbor_context_json", "group_source_json", "group_artifact_name", "group_configuration", "prior_context_json", "requires_committed_graph", "execution_json", "upstream_artifact_name", "consumer_indexes", "sequence", "reprocess_generation" }) |known| {
                if (std.mem.eql(u8, name, known)) break true;
            } else false;
            if (!reviewed) @compileError("classify the new producer field before narrowing its input dependencies: " ++ name);
        }
    }
    if ((request.kind != .dense_embedding and request.kind != .sparse_embedding) or
        request.input_kind != .document or request.embedding_input != .text or
        request.source_field.len == 0 or request.source_template.len != 0 or
        request.chunk_size != 0 or request.upstream_artifact_name.len != 0 or
        request.neighbor_context_json.len != 0 or request.group_source_json.len != 0 or
        request.group_artifact_name.len != 0 or request.prior_context_json.len != 0 or
        request.requires_committed_graph) return null;
    return .{ .field = request.source_field };
}
pub const Context = struct {
    namespace: [24]u8,
    document_key: []const u8,
    canonical_document: u64,
    /// The live identity directory's created generation changes on resurrection
    /// even when the canonical ID, content and user timestamp are reused.
    created_generation: u64,
    definition: Digest,
    reprocess_generation: u64,
};
fn number(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn hashBytes(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    number(hash, value.len);
    hash.update(value);
}
/// Members come from the validated current logical commitment at the SAME
/// pinned read cut as namespace and incarnation. No observation sequence or
/// timestamp participates; no per-producer counter is written on a document edit.
pub fn revision(dependency: Dependency, context: Context, members: []const logical.Member) !Digest {
    if (dependency.field.len == 0 or context.document_key.len == 0 or context.canonical_document == 0 or
        context.created_generation == 0 or std.mem.allEqual(u8, &context.namespace, 0) or
        std.mem.allEqual(u8, &context.definition, 0)) return error.InvalidProducerInputRevision;
    // Require canonical order once. This also rejects ambiguous duplicate keys.
    _ = try logical.objectDigest(members);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.producer.input.fields/1\x00");
    hash.update(&context.namespace);
    hashBytes(&hash, context.document_key);
    number(&hash, context.canonical_document);
    number(&hash, context.created_generation);
    hash.update(&context.definition);
    number(&hash, context.reprocess_generation);
    hashBytes(&hash, dependency.field);
    var lower: usize = 0;
    var upper = members.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        switch (std.mem.order(u8, members[middle].name, dependency.field)) {
            .lt => lower = middle + 1,
            .gt => upper = middle,
            .eq => {
                hash.update(&.{1});
                hash.update(&members[middle].digest);
                return hash.finalResult();
            },
        }
    }
    hash.update(&.{0});
    return hash.finalResult();
}
pub const Observation = struct {
    input_revision: Digest,
    position: ?Position,
    timestamp: u64,
    pub fn sameInput(a: Observation, b: Observation) bool {
        return std.mem.eql(u8, &a.input_revision, &b.input_revision);
    }
    /// Publication identity still binds the real observation. Equality of
    /// inputs never fabricates or advances an ordered completion position.
    pub fn auditDigest(self: Observation) !Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.producer.observation/1\x00");
        hash.update(&self.input_revision);
        hash.update(&.{@intFromBool(self.position != null)});
        if (self.position) |position| hash.update(&try position.encode());
        number(&hash, self.timestamp);
        return hash.finalResult();
    }
};

test "producer input revision preserves unrelated edits and separates observation clocks" {
    const a = std.testing.allocator;
    const first = try logical.build(a, "{\"body\":\"hello\",\"metadata\":1}");
    defer first.deinit(a);
    const edited = try logical.build(a, "{\"metadata\":2,\"body\":\"h\\u0065llo\"}");
    defer edited.deinit(a);
    const different = try logical.build(a, "{\"body\":\"different\",\"metadata\":1}");
    defer different.deinit(a);
    const request: requests.GeneratedEnrichmentRequest = .{ .kind = .dense_embedding, .index_name = "idx", .doc_key = "doc", .source_field = "body" };
    const dependency = compile(request).?;
    const context: Context = .{ .namespace = @splat(1), .document_key = "doc", .canonical_document = 7, .created_generation = 10, .definition = @splat(2), .reprocess_generation = 0 };
    const original = try revision(dependency, context, first.members);
    try std.testing.expectEqualDeep(original, try revision(dependency, context, edited.members));
    try std.testing.expect(!std.mem.eql(u8, &original, &try revision(dependency, context, different.members)));
    inline for (.{ "document_key", "canonical_document", "created_generation", "definition", "reprocess_generation", "namespace" }) |name| {
        var changed = context;
        if (comptime std.mem.eql(u8, name, "document_key")) changed.document_key = "other" else if (comptime std.mem.eql(u8, name, "definition") or std.mem.eql(u8, name, "namespace")) @field(changed, name)[0] ^= 1 else @field(changed, name) += 1;
        try std.testing.expect(!std.mem.eql(u8, &original, &try revision(dependency, changed, first.members)));
    }
    const before: Observation = .{ .input_revision = original, .position = .{ .raft = .{ .term = 1, .index = 10 } }, .timestamp = 100 };
    const after: Observation = .{ .input_revision = original, .position = .{ .raft = .{ .term = 2, .index = 20 } }, .timestamp = 200 };
    try std.testing.expect(Observation.sameInput(before, after));
    try std.testing.expect(!std.mem.eql(u8, &try before.auditDigest(), &try after.auditDigest()));
}

test "producer input revision distinguishes absence null and literal dotted field names" {
    const a = std.testing.allocator;
    const missing = try logical.build(a, "{}");
    defer missing.deinit(a);
    const present = try logical.build(a, "{\"a.b\":null,\"a\":{\"b\":\"nested\"}}");
    defer present.deinit(a);
    const context: Context = .{ .namespace = @splat(1), .document_key = "doc", .canonical_document = 7, .created_generation = 10, .definition = @splat(2), .reprocess_generation = 0 };
    try std.testing.expect(!std.mem.eql(u8, &try revision(.{ .field = "a.b" }, context, missing.members), &try revision(.{ .field = "a.b" }, context, present.members)));
    try std.testing.expect(!std.mem.eql(u8, &try revision(.{ .field = "a" }, context, present.members), &try revision(.{ .field = "a.b" }, context, present.members)));
    const duplicate = [_]logical.Member{ present.members[0], present.members[0] };
    try std.testing.expectError(error.InvalidDocumentCommitment, revision(.{ .field = "a" }, context, &duplicate));
}

test "producer input revision compiler retains full source for open dependencies" {
    const request: requests.GeneratedEnrichmentRequest = .{ .kind = .dense_embedding, .index_name = "idx", .doc_key = "doc", .source_field = "body" };
    try std.testing.expect(compile(request) != null);
    inline for (.{ "source_template", "neighbor_context_json", "group_source_json", "group_artifact_name", "prior_context_json", "upstream_artifact_name" }) |name| {
        var changed = request;
        @field(changed, name) = "present";
        try std.testing.expect(compile(changed) == null);
    }
    var changed = request;
    changed.requires_committed_graph = true;
    try std.testing.expect(compile(changed) == null);
    changed = request;
    changed.input_kind = .materialized_chunks;
    try std.testing.expect(compile(changed) == null);
    changed = request;
    changed.embedding_input = .pdf_page_images;
    try std.testing.expect(compile(changed) == null);
}

pub const Term = struct { definition: Digest, input_revision: Digest };
pub const maximum_terms = 128;
fn validateTerms(terms: []const Term) !void {
    if (terms.len > maximum_terms) return error.ResourceLimitExceeded;
    for (terms, 0..) |term, index| {
        if (std.mem.allEqual(u8, &term.definition, 0) or std.mem.allEqual(u8, &term.input_revision, 0) or
            (index != 0 and std.mem.order(u8, &terms[index - 1].definition, &term.definition) != .lt)) return error.InvalidProducerInputRevision;
    }
}
/// Conjunction for ONE document's scoped dependencies. Equal observations of
/// one definition coalesce; a second producer's dependency is never discarded.
/// Legacy whole-source guards are separate predicates and must remain in the
/// caller's read set. A contradictory revision requires retry, not replacement.
pub fn conjoin(a: std.mem.Allocator, previous: []const Term, incoming: []const Term) ![]Term {
    try validateTerms(previous);
    try validateTerms(incoming);
    const out = try a.alloc(Term, @min(maximum_terms, previous.len + incoming.len));
    errdefer a.free(out);
    var left: usize = 0;
    var right: usize = 0;
    var size: usize = 0;
    while (left < previous.len or right < incoming.len) {
        const order: std.math.Order = if (left == previous.len) .gt else if (right == incoming.len) .lt else std.mem.order(u8, &previous[left].definition, &incoming[right].definition);
        const next = switch (order) {
            .lt => blk: {
                const value = previous[left];
                left += 1;
                break :blk value;
            },
            .gt => blk: {
                const value = incoming[right];
                right += 1;
                break :blk value;
            },
            .eq => blk: {
                if (!std.mem.eql(u8, &previous[left].input_revision, &incoming[right].input_revision)) return error.EnrichmentSourceChanged;
                const value = previous[left];
                left += 1;
                right += 1;
                break :blk value;
            },
        };
        if (size == out.len) return error.ResourceLimitExceeded;
        out[size] = next;
        size += 1;
    }
    return try a.realloc(out, size);
}

test "producer input revision inherited terms form a bounded conjunction" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const left = [_]Term{
                .{ .definition = @splat(1), .input_revision = @splat(2) },
                .{ .definition = @splat(5), .input_revision = @splat(6) },
            };
            const right = [_]Term{
                .{ .definition = @splat(1), .input_revision = @splat(2) },
                .{ .definition = @splat(3), .input_revision = @splat(4) },
            };
            const joined = try conjoin(a, &left, &right);
            defer a.free(joined);
            try std.testing.expectEqual(@as(usize, 3), joined.len);
            try std.testing.expectEqualDeep(left[0], joined[0]);
            try std.testing.expectEqualDeep(right[1], joined[1]);
            try std.testing.expectEqualDeep(left[1], joined[2]);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    const first = [_]Term{.{ .definition = @splat(1), .input_revision = @splat(2) }};
    const conflict = [_]Term{.{ .definition = @splat(1), .input_revision = @splat(3) }};
    try std.testing.expectError(error.EnrichmentSourceChanged, conjoin(std.testing.allocator, &first, &conflict));
}
