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

//! Persisted catalog of entity-resolution configs (see zig/RESOLUTION.md).
//!
//! A resolver consumes a source extraction artifact for a document, resolves
//! its mentions to canonical entity `DocRef`s, and writes a resolution
//! artifact. The durable config mirrors the enrichment catalog: it is stored
//! per shard and replayed at the recorded `config_generation` so resolution
//! decisions stay stable (the replay-stability invariant).
//!
//! `scorer_json` is the optional matcher scorer config (parsed by
//! `lib/matcher`); an empty `scorer_json` means a purely deterministic resolver
//! that always mints canonical keys from `key_template`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const matcher = @import("antfly_matcher");

/// Durable resolver-backfill cursors. They live with the resolver catalog
/// metadata so catalog mutations can mark backfill dirty in the same store
/// transaction that publishes the catalog change.
pub const reresolve_resume_key = "\x00\x00__metadata__:resolution_reresolve_resume";
pub const reresolve_repair_resume_key = "\x00\x00__metadata__:resolution_reresolve_repair_resume";

/// Map a `fusion_combine` config string to a matcher fusion strategy. `null`
/// means either "" (fusion disabled) or an unrecognized value; callers that
/// must distinguish (config validation) check `len == 0` first.
pub fn fusionStrategy(name: []const u8) ?matcher.FusionStrategy {
    if (std.mem.eql(u8, name, "noisy_or")) return .noisy_or;
    if (std.mem.eql(u8, name, "max")) return .max;
    if (std.mem.eql(u8, name, "mean")) return .mean;
    return null;
}

pub const ResolverSourceArtifactKind = enum {
    /// Root and unit-scoped asset artifacts. This is the legacy default.
    asset,
    /// Chunk-scoped artifacts produced under an asset or unit.
    chunk,
    /// Consume every artifact kind with the configured artifact name.
    any,

    pub fn matches(self: ResolverSourceArtifactKind, actual: ResolverSourceArtifactKind) bool {
        return self == .any or self == actual;
    }
};

pub const ResolverConfig = struct {
    /// Catalog name of the resolver.
    name: []const u8,
    /// Entity table that canonical entities live in.
    table: []const u8,
    /// Extraction artifact this resolver consumes.
    source_artifact: []const u8,
    /// Kind of extraction artifact this resolver consumes. Existing configs
    /// default to asset artifacts; chunk-level mention streams must opt in.
    source_artifact_kind: ResolverSourceArtifactKind = .asset,
    /// Resolution artifact this resolver writes.
    resolution_artifact: []const u8,
    /// Template that renders a canonical entity key from a mention.
    key_template: []const u8,
    /// Require the candidate entity label to match the mention label.
    type_must_match: bool = true,
    /// Optional matcher scorer config; empty means deterministic minting only.
    scorer_json: []const u8 = "",
    /// Candidate blocking strategy: "" (none, deterministic mint) or
    /// "exact_key" (look up the rendered canonical key as a candidate so the
    /// scorer can link to an existing entity). ANN/prefix blocking over the
    /// entity table is a phase-2 extension.
    candidate_search: []const u8 = "",
    /// Dense index to use for `candidate_search = "ann"`. Empty defaults to
    /// `name_embedding` for compatibility with early resolver configs.
    candidate_ann_index: []const u8 = "",
    /// Maximum ANN candidates to request. Zero uses the runtime default.
    candidate_limit: u32 = 0,
    /// Embedding model name used to backfill a mention's name embedding (for
    /// `cosine` scoring / `ann` blocking) when the extraction artifact carries
    /// none. Empty disables backfill. Must match the embedding the entity table
    /// indexes over its canonical name so the vectors are comparable.
    name_embedding: []const u8 = "",
    /// Dimensionality for `name_embedding` (0 lets the embedder decide).
    name_embedding_dims: u32 = 0,
    /// Confidence-fusion strategy for the provenance edge weight: "" (legacy
    /// fixed weight 1.0), "noisy_or", "max", or "mean". When set, the mention
    /// edge's weight is `matcher.fuse` of this extractor's `fusion_trust *`
    /// the mention's asserted confidence, folded with the config-pinned graph
    /// prior. This is the naive first fusion implementation (one source per
    /// resolver); cross-extractor combine over a live snapshot is a later step.
    fusion_combine: []const u8 = "",
    /// This extractor's trust in [0, 1]; scales its asserted confidence.
    fusion_trust: f64 = 1.0,
    /// Config-generation-pinned graph prior belief in [0, 1] folded into the
    /// fused weight (a fixed snapshot value avoids the streaming self-reinforce
    /// caveat: the prior never reads the edges currently being written).
    fusion_prior: f64 = 0.0,
    /// Weight of `fusion_prior` in the fusion; 0 ignores the prior.
    fusion_prior_weight: f64 = 0.0,
    /// Bumped to force a versioned re-resolution pass.
    config_generation: u64 = 0,

    pub fn clone(alloc: Allocator, cfg: ResolverConfig) !ResolverConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .table = try alloc.dupe(u8, cfg.table),
            .source_artifact = try alloc.dupe(u8, cfg.source_artifact),
            .source_artifact_kind = cfg.source_artifact_kind,
            .resolution_artifact = try alloc.dupe(u8, cfg.resolution_artifact),
            .key_template = try alloc.dupe(u8, cfg.key_template),
            .type_must_match = cfg.type_must_match,
            .scorer_json = if (cfg.scorer_json.len > 0) try alloc.dupe(u8, cfg.scorer_json) else "",
            .candidate_search = if (cfg.candidate_search.len > 0) try alloc.dupe(u8, cfg.candidate_search) else "",
            .candidate_ann_index = if (cfg.candidate_ann_index.len > 0) try alloc.dupe(u8, cfg.candidate_ann_index) else "",
            .candidate_limit = cfg.candidate_limit,
            .name_embedding = if (cfg.name_embedding.len > 0) try alloc.dupe(u8, cfg.name_embedding) else "",
            .name_embedding_dims = cfg.name_embedding_dims,
            .fusion_combine = if (cfg.fusion_combine.len > 0) try alloc.dupe(u8, cfg.fusion_combine) else "",
            .fusion_trust = cfg.fusion_trust,
            .fusion_prior = cfg.fusion_prior,
            .fusion_prior_weight = cfg.fusion_prior_weight,
            .config_generation = cfg.config_generation,
        };
    }

    pub fn deinit(self: *ResolverConfig, alloc: Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.table));
        alloc.free(@constCast(self.source_artifact));
        alloc.free(@constCast(self.resolution_artifact));
        alloc.free(@constCast(self.key_template));
        if (self.scorer_json.len > 0) alloc.free(@constCast(self.scorer_json));
        if (self.candidate_search.len > 0) alloc.free(@constCast(self.candidate_search));
        if (self.candidate_ann_index.len > 0) alloc.free(@constCast(self.candidate_ann_index));
        if (self.name_embedding.len > 0) alloc.free(@constCast(self.name_embedding));
        if (self.fusion_combine.len > 0) alloc.free(@constCast(self.fusion_combine));
        self.* = undefined;
    }

    /// Reject a config that can never behave as intended. With fusion enabled:
    /// an unknown `fusion_combine` would silently fall back to the legacy fixed
    /// weight (fusion configured but doing nothing); `fusion_trust` of 0 (or a
    /// prior outside [0, 1]) collapses every edge weight toward 0, which a
    /// weighted traversal with a positive `min_weight` would silently drop.
    pub fn validate(self: ResolverConfig) !void {
        if (self.name.len == 0 or
            self.table.len == 0 or
            self.source_artifact.len == 0 or
            self.resolution_artifact.len == 0 or
            self.key_template.len == 0)
        {
            return error.InvalidResolverConfig;
        }
        if (std.mem.eql(u8, self.source_artifact, self.resolution_artifact)) return error.InvalidResolverConfig;
        if (self.candidate_search.len > 0 and
            !std.mem.eql(u8, self.candidate_search, "exact_key") and
            !std.mem.eql(u8, self.candidate_search, "prefix") and
            !std.mem.eql(u8, self.candidate_search, "ann"))
        {
            return error.InvalidResolverConfig;
        }
        if (std.mem.eql(u8, self.candidate_search, "ann")) {
            if (self.candidate_ann_index.len == 0 and self.name_embedding.len == 0) return error.InvalidResolverConfig;
        }
        if (self.fusion_combine.len == 0) return;
        if (fusionStrategy(self.fusion_combine) == null) return error.InvalidResolverConfig;
        if (!(self.fusion_trust > 0.0 and self.fusion_trust <= 1.0)) return error.InvalidResolverConfig;
        if (self.fusion_prior < 0.0 or self.fusion_prior > 1.0) return error.InvalidResolverConfig;
        if (self.fusion_prior_weight < 0.0 or self.fusion_prior_weight > 1.0) return error.InvalidResolverConfig;
    }

    /// Provenance edge weight for one mention from this extractor. When the
    /// resolver declares a fusion strategy, the weight is `matcher.fuse` of this
    /// extractor's `fusion_trust * confidence` folded with the config-pinned
    /// prior; otherwise the legacy fixed 1.0. Single source of truth for both
    /// the sync and async mention-edge materializers.
    pub fn fusedMentionWeight(self: ResolverConfig, confidence: f64) f64 {
        const strategy = fusionStrategy(self.fusion_combine) orelse return 1.0;
        return matcher.fuse(
            strategy,
            &.{.{ .confidence = confidence, .trust = self.fusion_trust }},
            self.fusion_prior,
            self.fusion_prior_weight,
        );
    }
};

pub fn serializeCatalog(alloc: Allocator, resolvers: []const ResolverConfig) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, resolvers, .{});
}

pub fn deserializeCatalog(alloc: Allocator, data: []const u8) ![]ResolverConfig {
    const parsed = try std.json.parseFromSlice([]ResolverConfig, alloc, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const out = try alloc.alloc(ResolverConfig, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
        alloc.free(out);
    }
    for (parsed.value, 0..) |cfg, i| {
        out[i] = try ResolverConfig.clone(alloc, cfg);
        initialized += 1;
    }
    return out;
}

test "resolver catalog round trip" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .source_artifact_kind = .chunk,
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}",
            .scorer_json = "{\"comparisons\":[],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}",
            .config_generation = 7,
        },
        .{
            .name = "people_only",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "people_resolution_v1",
            .key_template = "{{ slug _entity.text }}",
            .type_must_match = false,
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("knowledge_graph", decoded[0].name);
    try std.testing.expectEqualStrings("entities", decoded[0].table);
    try std.testing.expectEqualStrings("relations_v1", decoded[0].source_artifact);
    try std.testing.expectEqual(ResolverSourceArtifactKind.chunk, decoded[0].source_artifact_kind);
    try std.testing.expectEqualStrings("resolution_v1", decoded[0].resolution_artifact);
    try std.testing.expectEqualStrings("{{ lower _entity.label }}/{{ slug _entity.canonical_text }}", decoded[0].key_template);
    try std.testing.expect(decoded[0].type_must_match);
    try std.testing.expectEqual(@as(u64, 7), decoded[0].config_generation);
    try std.testing.expect(decoded[0].scorer_json.len > 0);

    try std.testing.expectEqualStrings("people_only", decoded[1].name);
    try std.testing.expectEqual(ResolverSourceArtifactKind.asset, decoded[1].source_artifact_kind);
    try std.testing.expect(!decoded[1].type_must_match);
    try std.testing.expectEqual(@as(usize, 0), decoded[1].scorer_json.len);
    try std.testing.expectEqual(@as(u64, 0), decoded[1].config_generation);
}

test "resolver catalog round trip preserves order and empty list" {
    const alloc = std.testing.allocator;

    const empty = try serializeCatalog(alloc, &.{});
    defer alloc.free(empty);
    const decoded_empty = try deserializeCatalog(alloc, empty);
    defer alloc.free(decoded_empty);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.len);
}

test "resolver catalog defaults legacy source artifact kind to asset" {
    const alloc = std.testing.allocator;

    const decoded = try deserializeCatalog(alloc,
        \\[
        \\  {
        \\    "name": "legacy",
        \\    "table": "entities",
        \\    "source_artifact": "relations_v1",
        \\    "resolution_artifact": "resolution_v1",
        \\    "key_template": "{{ slug _entity.text }}"
        \\  }
        \\]
    );
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(ResolverSourceArtifactKind.asset, decoded[0].source_artifact_kind);
}

test "resolver config validates fusion strategy and folds confidence into the weight" {
    const base = ResolverConfig{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ _entity.text }}",
    };

    // No fusion: legacy fixed weight, always valid.
    try base.validate();
    try std.testing.expectEqual(@as(f64, 1.0), base.fusedMentionWeight(0.5));

    // Valid fusion: weight is the fused confidence (noisy_or, trust 0.9,
    // confidence 0.8, no prior -> 1 - (1 - 0.72) = 0.72).
    var fused = base;
    fused.fusion_combine = "noisy_or";
    fused.fusion_trust = 0.9;
    try fused.validate();
    try std.testing.expectApproxEqAbs(@as(f64, 0.72), fused.fusedMentionWeight(0.8), 1e-9);

    // Unknown strategy is rejected, not silently treated as "fusion off".
    var bad_strategy = base;
    bad_strategy.fusion_combine = "noisyor";
    try std.testing.expectError(error.InvalidResolverConfig, bad_strategy.validate());

    // Zero trust would collapse every edge weight toward 0; rejected.
    var zero_trust = base;
    zero_trust.fusion_combine = "mean";
    zero_trust.fusion_trust = 0.0;
    try std.testing.expectError(error.InvalidResolverConfig, zero_trust.validate());

    // Prior outside [0, 1] is rejected.
    var bad_prior = base;
    bad_prior.fusion_combine = "max";
    bad_prior.fusion_prior = 1.5;
    try std.testing.expectError(error.InvalidResolverConfig, bad_prior.validate());
}

test "resolver config validates required references and candidate mode" {
    const base = ResolverConfig{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ _entity.text }}",
    };

    try base.validate();

    var missing_name = base;
    missing_name.name = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_name.validate());

    var missing_table = base;
    missing_table.table = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_table.validate());

    var missing_source = base;
    missing_source.source_artifact = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_source.validate());

    var missing_resolution = base;
    missing_resolution.resolution_artifact = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_resolution.validate());

    var missing_key_template = base;
    missing_key_template.key_template = "";
    try std.testing.expectError(error.InvalidResolverConfig, missing_key_template.validate());

    var recursive_artifact = base;
    recursive_artifact.resolution_artifact = recursive_artifact.source_artifact;
    try std.testing.expectError(error.InvalidResolverConfig, recursive_artifact.validate());

    var bad_candidate_mode = base;
    bad_candidate_mode.candidate_search = "nearest";
    try std.testing.expectError(error.InvalidResolverConfig, bad_candidate_mode.validate());

    var exact_key = base;
    exact_key.candidate_search = "exact_key";
    try exact_key.validate();

    var prefix = base;
    prefix.candidate_search = "prefix";
    try prefix.validate();

    var ann_missing_reference = base;
    ann_missing_reference.candidate_search = "ann";
    try std.testing.expectError(error.InvalidResolverConfig, ann_missing_reference.validate());

    var ann_with_index = base;
    ann_with_index.candidate_search = "ann";
    ann_with_index.candidate_ann_index = "entity_name_embedding";
    try ann_with_index.validate();
}
