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

//! Exact-input fence for a multi-snapshot producer census. Owner-local streams
//! do not restart on unrelated writes. Cross-document proofs additionally pin
//! the mutation epoch until dependency validation can certify their closure.
//! An observation alone is not a durable stream-completion certificate.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const validation = @import("artifact_producer_validation.zig");

pub const Observation = struct {
    authority: publication.Authority,
    document_digest: publication.Digest,
    revision: ?publication.Position,
    validation_epoch: u64,
    foreign_inputs: bool = false,

    pub fn capture(txn: anytype, document: []const u8) !Observation {
        const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        const state = (try validation.load(txn)) orelse return error.ArtifactCoverageBaselinePending;
        if (state.authority_epoch != authority.epoch or !std.mem.eql(u8, &state.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
        var digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(document, &digest, .{});
        return .{ .authority = authority, .document_digest = digest, .revision = try publication.materializationRevision(txn, authority.namespace, document), .validation_epoch = state.mutation_epoch };
    }

    fn requireDocument(self: Observation, document: []const u8) !void {
        var digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(document, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.document_digest)) return error.InvalidBatchRequest;
    }

    /// The caller must resolve the current accepted proof in the SAME snapshot
    /// as requireCurrent. A hash-matching output without provenance is not work
    /// completion, and inheriting one provider's receipt does not close others.
    pub fn observeProof(self: *Observation, document: []const u8, proof: @import("artifact_producer_provenance.zig").Proof) !void {
        try self.requireDocument(document);
        if (proof.authority_epoch != self.authority.epoch or !std.mem.eql(u8, &proof.namespace, &self.authority.namespace) or
            !std.mem.eql(u8, &proof.catalog_digest, &self.authority.catalog_digest)) return error.ArtifactCatalogDrift;
        for (proof.sources) |source| if (!std.mem.eql(u8, source.document_key, document)) {
            self.foreign_inputs = true;
            break;
        };
    }

    pub fn requireCurrent(self: Observation, txn: anytype, document: []const u8) !void {
        try self.requireDocument(document);
        const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(current, self.authority)) return error.ArtifactCatalogDrift;
        if (!std.meta.eql(self.revision, try publication.materializationRevision(txn, current.namespace, document))) return error.EnrichmentSourceChanged;
        if (self.foreign_inputs) {
            const state = (try validation.load(txn)) orelse return error.ArtifactCoverageBaselinePending;
            if (state.authority_epoch != current.epoch or !std.mem.eql(u8, &state.namespace, &current.namespace)) return error.ArtifactCatalogDrift;
            if (state.mutation_epoch != self.validation_epoch) return error.EnrichmentSourceChanged;
        }
    }

    /// Join independently verified requirements from one materialization
    /// snapshot. Owner-local prefixes may survive unrelated mutations; once
    /// foreign inputs participate, every later page must retain that epoch.
    pub fn merge(self: *Observation, incoming: Observation) !void {
        if (!std.meta.eql(self.authority, incoming.authority) or
            !std.mem.eql(u8, &self.document_digest, &incoming.document_digest)) return error.ArtifactCatalogDrift;
        if (!std.meta.eql(self.revision, incoming.revision)) return error.EnrichmentSourceChanged;
        if (self.foreign_inputs and incoming.foreign_inputs) {
            if (self.validation_epoch != incoming.validation_epoch) return error.EnrichmentSourceChanged;
        } else if (incoming.foreign_inputs) {
            self.validation_epoch = incoming.validation_epoch;
            self.foreign_inputs = true;
        }
    }
};

test "ordered artifact inventory completion observation merges only a coherent causal cut" {
    const first: Observation = .{ .authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, .document_digest = @splat(3), .revision = .{ .raft = .{ .term = 1, .index = 7 } }, .validation_epoch = 8 };
    var joined = first;
    var next = first;
    next.validation_epoch = 9;
    try joined.merge(next);
    try std.testing.expect(!joined.foreign_inputs);
    next.foreign_inputs = true;
    try joined.merge(next);
    try std.testing.expect(joined.foreign_inputs);
    try std.testing.expectEqual(@as(u64, 9), joined.validation_epoch);
    var independent = first;
    independent.validation_epoch = 3;
    try joined.merge(independent);
    try std.testing.expectEqual(@as(u64, 9), joined.validation_epoch);
    next.validation_epoch = 10;
    try std.testing.expectError(error.EnrichmentSourceChanged, joined.merge(next));
    next = first;
    next.revision = .{ .raft = .{ .term = 1, .index = 8 } };
    try std.testing.expectError(error.EnrichmentSourceChanged, joined.merge(next));
    next = first;
    next.document_digest = @splat(4);
    try std.testing.expectError(error.ArtifactCatalogDrift, joined.merge(next));
    next = first;
    next.authority.epoch = 2;
    try std.testing.expectError(error.ArtifactCatalogDrift, joined.merge(next));
}
