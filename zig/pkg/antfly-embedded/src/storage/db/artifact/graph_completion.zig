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

//! Artifact-backed graph projection closure. Current producer provenance
//! authenticates the complete per-artifact graph state, including empty output
//! and retirement. Final apply checks fixed references and the input observation.
const std = @import("std");
const publication = @import("publication.zig");
const provenance = @import("producer_provenance.zig");
const keys = @import("../../internal_keys.zig");
const manager = @import("../catalog/index_manager.zig");
const requests = @import("../enrichment/enrichment_types.zig");
const Observation = @import("stream_observation.zig").Observation;
const A = std.mem.Allocator;
pub const Closure = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    document: []const u8,
    requirement: publication.Digest,
    observation: Observation,
    certificates: []const provenance.ArtifactCertificate,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn requireCurrent(self: @This(), txn: anytype, root: u128) !void {
        if (root != self.root) return error.EnrichmentSourceChanged;
        try self.observation.requireCurrent(txn, self.document);
        for (self.certificates) |certificate| try certificate.requireCurrent(txn);
    }
};
pub fn prepare(a: A, txn: anytype, root: u128, document: []const u8, node: *const @import("completion_plan.zig").Node, plan: *const manager.IndexManager.WritePlanSnapshot) !Closure {
    if (node.kind != .index_projection or node.index_kind != .graph) return error.ArtifactPublicationPending;
    try node.requireValidIdentity();
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const catalogs = plan.artifact_catalogs orelse return error.ArtifactCatalogDrift;
    const configs = try manager.deserializeCatalog(owned, catalogs.indexes);
    const config = for (configs) |config| {
        if (config.kind == .graph and std.mem.eql(u8, config.name, node.name) and config.coverage_generation == node.generation) break config;
    } else return error.ArtifactCatalogDrift;
    const graph = try manager.parseGraphConfig(owned, config.config_json);
    // Field-backed/native graph projections need their own census. They
    // cannot borrow an unrelated artifact producer's completion certificate.
    for (graph.edge_type_configs) |edge| if (edge.field_name != null) return error.ArtifactPublicationPending;
    if (graph.artifact_sources.len == 0 or graph.artifact_sources.len > 128) return error.ArtifactPublicationPending;
    var observation = try Observation.capture(txn, document);
    var certificates: std.ArrayList(provenance.ArtifactCertificate) = .empty;
    for (graph.artifact_sources) |source| {
        const template = for (plan.generated_templates) |template| {
            if (template.kind == .asset and std.mem.eql(u8, template.artifact_name, source.artifact_name)) break template;
        } else return error.ArtifactPublicationPending;
        if (!requests.appliesToDocument(template, document)) continue;
        // Producer-defined unit sets require their own scope census; a root
        // state alone must never claim that all children have been projected.
        if (template.input_kind != .document) return error.ArtifactPublicationPending;
        const state_name = try @import("../graph_state_name.zig").artifactAlloc(owned, .{ .document_id = @constCast(document), .name = source.artifact_name, .kind = .asset });
        var state_key: std.ArrayList(u8) = .empty;
        try state_key.appendSlice(owned, try keys.graphAssetStateIndexPrefixAlloc(owned, document, node.name));
        try keys.appendEncodedComponent(&state_key, owned, state_name);
        var accepted = (try provenance.readCurrentArtifactMetadata(owned, txn, state_key.items)) orelse return error.ArtifactPublicationPending;
        defer accepted.deinit();
        const proof = accepted.proof;
        if (proof.producer_kind != .graph or proof.producer_generation != node.generation or !std.mem.eql(u8, proof.producer_name, node.name) or !std.mem.eql(u8, proof.producer_artifact_name, source.artifact_name)) return error.ArtifactPublicationPending;
        const input = for (proof.sources) |input| {
            if (std.mem.eql(u8, input.document_key, document)) break input;
        } else return error.ArtifactCatalogCorrupt;
        // Validate the complete current graph effect set, including shared
        // contender/winner reconciliation, rather than just the state row.
        var current = (try provenance.readConvergedForSource(owned, txn, proof.inputCommand(), input)) orelse return error.ArtifactPublicationPending;
        defer current.deinit();
        if (!std.mem.eql(u8, &current.owned.proof.publication_digest, &proof.publication_digest)) return error.ArtifactPublicationPending;
        try observation.observeProof(document, current.owned.proof);
        try certificates.append(owned, try provenance.captureCurrentArtifactCertificate(txn, observation.authority, state_key.items, proof.publication_digest));
    }
    try observation.requireCurrent(txn, document);
    const owned_document = try owned.dupe(u8, document);
    const owned_certificates = try certificates.toOwnedSlice(owned);
    return .{ .arena = arena, .root = root, .document = owned_document, .requirement = node.id, .observation = observation, .certificates = owned_certificates };
}

test "grouped sources production graph closure fences replaced provenance and source revisions" {
    const a = std.testing.allocator;
    const ns: publication.Namespace = @splat(1);
    const certificate = provenance.ArtifactCertificate{ .reference = @splat(3), .value = @splat(4) };
    const Txn = struct {
        raw_authority: [100]u8,
        current: @TypeOf(certificate.value) = certificate.value,
        revision: ?[]const u8 = null,
        pub fn get(self: *@This(), selected: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, selected, publication.authority_key)) return &self.raw_authority;
            if (std.mem.eql(u8, selected, &certificate.reference)) return &self.current;
            if (std.mem.eql(u8, selected, &publication.materializationRevisionKey(ns, "unit"))) return self.revision orelse error.NotFound;
            return error.NotFound;
        }
    };
    var raw: [100]u8 = @splat(0);
    @memcpy(raw[0..4], "APA1");
    @memcpy(raw[4..28], &ns);
    std.mem.writeInt(u64, raw[28..36], 1, .little);
    @memset(raw[36..68], 2);
    std.crypto.hash.Blake3.hash(raw[0..68], raw[68..100], .{});
    var txn = Txn{ .raw_authority = raw };
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash("unit", &digest, .{});
    var closure = Closure{
        .arena = std.heap.ArenaAllocator.init(a),
        .root = 7,
        .document = "unit",
        .requirement = @splat(5),
        .observation = .{ .authority = (try publication.authority(&txn)).?, .document_digest = digest, .revision = null, .validation_epoch = 1 },
        .certificates = &.{certificate},
    };
    defer closure.deinit();
    try closure.requireCurrent(&txn, 7);
    txn.current[0] ^= 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&txn, 7));
    txn.current = certificate.value;
    try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&txn, 8));
    const materialization = try (publication.Materialization{ .position = .{ .raft = .{ .term = 1, .index = 9 } }, .replay_sequence = 3 }).encode();
    txn.revision = &materialization;
    try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&txn, 7));
}
