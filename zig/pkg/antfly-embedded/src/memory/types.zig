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

//! Versioned memory contracts. No server, model runtime, or extension imports.
const std = @import("std");

pub const provenance = @import("../common/provenance.zig");
pub const schema_version: u32 = 1;
pub const max_items = 256;
pub const max_text_bytes = 64 * 1024;
pub const Mode = enum { external, managed, hybrid };
pub const Role = enum { reported, observed, inferred, hypothesized };
pub const Modality = enum { actual, planned, desired, conditional, hypothetical };
pub const Lifecycle = enum { active, disputed, superseded, retracted };
pub const Stability = enum { provisional, settled };
pub const Kind = enum { semantic, episodic, procedural, decision, rationale, preference, lesson, constraint, open_question, alternative, summary };
pub const Visibility = enum { private, team };
pub const Reconciliation = enum { explicit, deterministic, model };
pub const Predicate = struct {
    name: []const u8,
    cardinality: enum { one, many } = .many,
    subject_type: ?[]const u8 = null,
    object_type: ?[]const u8 = null,
};
pub const Model = struct {
    provider: []const u8,
    model: []const u8,
    revision: []const u8,
    dimensions: ?u32 = null,
};
pub const Models = struct {
    embedder: ?Model = null,
    extractor: ?Model = null,
    generator: ?Model = null,
    decider: ?Model = null,
};
pub const Features = struct {
    semantic_recall: bool = false,
    entity_extraction: bool = false,
    assertion_distillation: bool = false,
    summaries: bool = false,
    reconciliation: Reconciliation = .explicit,
};
pub const SourceRule = struct { source_id: []const u8, principal: ?[]const u8 = null, producer: ?[]const u8 = null, authority: u16 = 0 };
pub const SemanticDiscovery = struct {
    enabled: bool = false,
    name_threshold: f64 = 0.80,
    view_threshold: f64 = 0.85,
    neighbors: u16 = 32,
};
pub const ProjectionKind = enum { names, view };
pub const EntityProjection = struct {
    entity_id: []const u8,
    entity_revision: u64,
    model: Model,
    generation: u64,
    config_generation: u64,
    owner: []const u8,
    visibility: Visibility,
    kind: ProjectionKind,
    fingerprint: []const u8,
    dependencies: []const DerivedDependency,
    text: []const u8,
    embedding: []const f32,
};
pub const semantic_index_version: u32 = 2;
pub const SemanticIndex = struct { version: u32 = 0, generation: u64 = 0, cursor: ?[]const u8 = null, work_cursor: ?[]const u8 = null, backfill_complete: bool = false, complete: bool = false };
pub const Config = struct {
    version: u32 = schema_version,
    generation: u64 = 1,
    embedding_generation: u64 = 1,
    processing: Mode = .external,
    ontology_version: []const u8 = "memory/v1",
    policy_version: []const u8 = "memory/v1",
    features: Features = .{},
    models: Models = .{},
    semantic_discovery: SemanticDiscovery = .{},
    ontology_pack: enum { none, software_team_v1 } = .none,
    predicates: []const Predicate = &.{},
    source_rules: []const SourceRule = &.{},
    max_input_bytes: u32 = 256 * 1024,
    max_output_items: u16 = 128,
    max_attempts: u8 = 3,
    max_recall_tokens: u32 = 8192,
    retention_ms: ?u64 = null,

    pub fn validate(self: Config) !void {
        if (self.version != schema_version or self.generation == 0 or self.ontology_version.len == 0 or self.policy_version.len == 0) return error.InvalidMemoryConfig;
        if (self.max_input_bytes == 0 or self.max_input_bytes > 4 * 1024 * 1024 or self.max_output_items == 0 or self.max_output_items > max_items or self.max_attempts == 0 or self.max_attempts > 16 or self.max_recall_tokens == 0 or self.max_recall_tokens > 65536) return error.InvalidMemoryConfig;
        if (self.semantic_discovery.enabled and (self.semantic_discovery.neighbors == 0 or self.semantic_discovery.neighbors > 64 or !std.math.isFinite(self.semantic_discovery.name_threshold) or !std.math.isFinite(self.semantic_discovery.view_threshold) or self.semantic_discovery.name_threshold <= 0 or self.semantic_discovery.view_threshold <= 0)) return error.InvalidMemoryConfig;
        if ((self.features.semantic_recall or self.semantic_discovery.enabled) and self.models.embedder == null) return error.MemoryModelRequired;
        if (self.features.entity_extraction and self.models.extractor == null and self.models.generator == null) return error.MemoryModelRequired;
        if ((self.features.assertion_distillation or self.features.summaries) and self.models.generator == null) return error.MemoryModelRequired;
        if (self.features.reconciliation == .model and self.models.decider == null and self.models.generator == null) return error.MemoryModelRequired;
        if (self.processing == .external and self.features.assertion_distillation) return error.InvalidMemoryConfig;
        if (self.processing == .managed and !(self.features.assertion_distillation or self.features.entity_extraction or self.features.summaries)) return error.InvalidMemoryConfig;
        inline for (@typeInfo(Models).@"struct".field_names) |field| {
            if (@field(self.models, field)) |model| {
                if (model.provider.len == 0 or model.model.len == 0 or model.revision.len == 0 or std.mem.indexOf(u8, model.model, "${secret:") != null) return error.InvalidMemoryConfig;
            }
        }
        if (self.models.embedder) |model| if (model.dimensions == null or model.dimensions.? == 0 or model.dimensions.? > 65536) return error.InvalidMemoryConfig;
        if (self.source_rules.len > max_items) return error.InvalidMemoryConfig;
        for (self.source_rules, 0..) |rule, i| {
            try validateId(rule.source_id);
            if (rule.authority > 0 and rule.principal == null) return error.InvalidMemoryConfig;
            if (rule.principal) |principal| try validateId(principal);
            for (self.source_rules[0..i]) |prior| if (std.mem.eql(u8, prior.source_id, rule.source_id)) return error.InvalidMemoryConfig;
        }
        if (self.predicates.len > max_items) return error.InvalidMemoryConfig;
        for (self.predicates, 0..) |predicate, i| {
            try validateId(predicate.name);
            if (self.ontology_pack == .software_team_v1 and (std.mem.eql(u8, predicate.name, "is_a") or std.mem.eql(u8, predicate.name, "part_of") or std.mem.eql(u8, predicate.name, "alternative_to"))) {
                if (predicate.subject_type) |name| if (!std.mem.eql(u8, name, "concept")) return error.InvalidMemoryConfig;
                if (predicate.object_type) |name| if (!std.mem.eql(u8, name, "concept")) return error.InvalidMemoryConfig;
            }
            for (self.predicates[0..i]) |other| if (std.mem.eql(u8, other.name, predicate.name)) return error.InvalidMemoryConfig;
        }
    }
};
pub const Span = struct { start: u32, end: u32 };
pub const Evidence = struct {
    id: []const u8,
    source_id: []const u8 = "",
    source_version: []const u8 = "",
    text: []const u8 = "",
    resource: ?provenance.Resource = null,
    resource_selector: ?provenance.Selector = null,
    source_url: ?[]const u8 = null,
    author: ?[]const u8 = null,
    event_time: ?i64 = null,
    timezone: ?[]const u8 = null,
    thread_id: ?[]const u8 = null,
    turn_id: ?[]const u8 = null,
    ordinal: ?i64 = null,
    producer: []const u8 = "external",
    managed: bool = false,
    visibility: Visibility = .private,
    expires_at: ?i64 = null,
};
pub const EvidenceRef = struct { id: []const u8, span: ?Span = null };
pub const Support = struct { evidence: []const EvidenceRef, depends_on: []const []const u8 = &.{} };
pub const Assertion = struct {
    id: []const u8,
    subject: []const u8,
    predicate: []const u8,
    value: []const u8,
    statement: []const u8,
    kind: Kind = .semantic,
    scope: []const u8 = "",
    task: ?[]const u8 = null,
    environment: ?[]const u8 = null,
    role: Role = .reported,
    modality: Modality = .actual,
    stability: Stability = .settled,
    valid_from: ?i64 = null,
    valid_to: ?i64 = null,
    evidence: []const EvidenceRef,
    depends_on: []const []const u8 = &.{},
    support_sets: []const Support = &.{},
    supersedes: []const []const u8 = &.{},
    motivates: []const []const u8 = &.{},
    rejected_in_favor_of: []const []const u8 = &.{},
    preconditions: []const []const u8 = &.{},
    steps: []const []const u8 = &.{},
    expected_outcome: ?[]const u8 = null,
    producer: []const u8 = "external",
    visibility: Visibility = .private,
    embedding: ?[]const f32 = null,
    embedding_model: ?Model = null,
    reconciliation_reason: ?[]const u8 = null,
};
pub const ObservationRequest = struct { origin: ?provenance.Origin = null, context_evidence_ids: []const []const u8 = &.{}, evidence: []const Evidence, assertions: []const Assertion = &.{}, entities: []const Entity = &.{}, mentions: []const EntityMention = &.{} };
pub const AssertionRequest = struct { assertions: []const Assertion };
pub const Correction = struct {
    id: []const u8,
    expected_revision: u64,
    lifecycle: Lifecycle,
    stability: ?Stability = null,
    reason: []const u8,
    evidence: []const EvidenceRef = &.{},
};
pub const Forget = struct {
    evidence_ids: []const []const u8 = &.{},
    assertion_ids: []const []const u8 = &.{},
    source_ids: []const []const u8 = &.{},
    all: bool = false,
};
pub const Outcome = struct {
    id: []const u8,
    task_id: []const u8,
    success: bool,
    used_memory_ids: []const []const u8,
    detail: []const u8 = "",
};
pub const Relationship = enum { depends_on, supersedes, motivates, rejected_in_favor_of };
pub const Recall = struct {
    graph: ?GraphRecall = null,
    query: []const u8 = "",
    relationship: ?Relationship = null,
    target_id: ?[]const u8 = null,
    missing_relationship: ?Relationship = null,
    subject: ?[]const u8 = null,
    predicate: ?[]const u8 = null,
    scope: ?[]const u8 = null,
    environment: ?[]const u8 = null,
    task: ?[]const u8 = null,
    kind: ?Kind = null,
    valid_at: ?i64 = null,
    known_at: ?i64 = null,
    include_history: bool = false,
    include_provisional: bool = true,
    limit: u16 = 20,
    token_budget: u32 = 2048,
    cursor: ?[]const u8 = null,
    embedding: ?[]const f32 = null,
    embedding_model: ?Model = null,
};
pub const State = struct {
    table_id: u64 = 0,
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    space: []const u8 = "",
    execution_owner: []const u8 = "",
    execution_principal: []const u8 = "",
    execution_signature: []const u8 = "",
    config: Config,
    pending_config: ?Config = null,
    next_expiry_at: ?i64 = null,
    next_resource_check_at: ?i64 = null,
    resource_cursor: ?[]const u8 = null,
    revision: u64 = 1,
    owner: []const u8,
    created_at: i64,
};
pub const StoredEvidence = struct { context_only: bool = false, evidence: Evidence, owner: []const u8, recorded_at: i64, revision: u64, forgotten: bool = false };
/// Explicit human/producer decisions are the baseline for identity-policy
/// projection. Rebuilding canonical groups never erases these decisions.
pub const CorrectionProvenance = struct {
    principal: []const u8,
    recorded_at: i64,
    evidence: []const EvidenceRef,
    forgotten: bool = false,
};
pub const ExplicitDisposition = struct {
    provenance: ?CorrectionProvenance = null,
    lifecycle: Lifecycle,
    valid_to: ?i64,
    superseded_by: ?[]const u8,
    superseded_valid_at: ?i64 = null,
    recorded_to: ?i64,
    conflicts_with: []const []const u8,
    reason: ?[]const u8,
};
pub const StoredAssertion = struct {
    assertion: Assertion,
    input_hash: []const u8 = "",
    asserted_valid_to: ?i64 = null,
    explicit_disposition: ?ExplicitDisposition = null,
    owner: []const u8,
    recorded_from: i64,
    recorded_at: i64 = 0,
    revision: u64,
    lifecycle: Lifecycle = .active,
    // Explicit semantic invalidation of a derived artifact. Time-qualified
    // dependency applicability is evaluated at read/reconciliation time.
    stale: bool = false,
    reason: ?[]const u8 = null,
    conflicts_with: []const []const u8 = &.{},
    recorded_to: ?i64 = null,
    superseded_by: ?[]const u8 = null,
    /// Accepted replacement boundary; null means business time is unknown.
    superseded_valid_at: ?i64 = null,
    config_generation: u64,
};
pub const Stage = enum { pending, running, succeeded, failed, cancelled };
pub const RequiredGrant = enum { write, curator, admin };
pub const Operation = struct {
    proposal_id: ?[]const u8 = null,
    origin: ?provenance.Origin = null,
    required_grant: ?RequiredGrant = null,
    execution_principal: []const u8 = "",
    execution_signature: []const u8 = "",
    id: []const u8,
    action: []const u8,
    request_hash: []const u8,
    owner: []const u8,
    revision: u64,
    config_generation: u64,
    created_at: i64,
    phase: Stage = .succeeded,
    evidence_accepted: bool = true,
    assertions_ready: bool = true,
    attempts: u8 = 0,
    error_code: ?[]const u8 = null,
    next_retry_at: ?i64 = null,
    inputs: []const []const u8 = &.{},
    context_inputs: []const []const u8 = &.{},
    cursor: ?[]const u8 = null,
};
pub const Entity = struct { visibility: Visibility = .private, evidence: []const EvidenceRef = &.{}, id: []const u8, name: []const u8, type: []const u8 = "concept", aliases: []const []const u8 = &.{}, distinct_from: []const []const u8 = &.{} };
pub const ReconciliationDecision = struct { supersedes: []const []const u8 = &.{}, reason: []const u8 };
pub const Generated = struct { assertions: []const Assertion = &.{}, entities: []const Entity = &.{}, mentions: []const EntityMention = &.{} };

pub fn validateId(value: []const u8) !void {
    if (value.len == 0 or value.len > 256) return error.InvalidMemoryId;
    for (value) |ch| if (ch < 0x20 or ch == 0x7f) return error.InvalidMemoryId;
    if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidMemoryId;
}

test "memory model-free defaults and feature admission" {
    try (Config{}).validate();
    try std.testing.expectError(error.MemoryModelRequired, (Config{ .features = .{ .semantic_recall = true } }).validate());
    const model: Model = .{ .provider = "antfly", .model = "qualified", .revision = "sha256:pinned" };
    try (Config{ .processing = .managed, .features = .{ .assertion_distillation = true, .entity_extraction = true, .summaries = true, .reconciliation = .model }, .models = .{ .generator = model } }).validate();
}

pub const EntityCorrection = struct {
    absorbed: enum { reject, release, suppress } = .reject,
    reason: []const u8 = "",
    action: enum { merge, unmerge, suppress, unsuppress, distinct },
    from: []const u8,
    to: ?[]const u8 = null,
    expected_revision: u64,
    replace: bool = false,
    pin_distinct: bool = false,
};

pub const EmbeddingBatch = struct {
    generation: u64,
    model: Model,
    items: []const struct { id: []const u8, embedding: []const f32 },
};

/// Public receipts contain no durable execution grants or worker cursors.
pub const OperationStatus = struct {
    proposal_id: ?[]const u8 = null,
    id: []const u8,
    action: []const u8,
    request_hash: []const u8,
    owner: []const u8,
    revision: u64,
    config_generation: u64,
    created_at: i64,
    phase: Stage,
    evidence_accepted: bool,
    assertions_ready: bool,
    attempts: u8,
    error_code: ?[]const u8,
    next_retry_at: ?i64,
};
pub const RecallHit = struct { memory: StoredAssertion, score: f64, historical: bool, citations: []const EvidenceRef };
pub const RecallResult = struct {
    items: []const RecallHit,
    next_cursor: ?[]const u8,
    complete: bool,
    truncated: bool,
    scanned: usize,
    omitted: usize,
    token_upper_bound: usize,
    revision: u64,
    config_generation: u64,
    retrieval: enum { lexical, lexical_vector, graph },
    entities: []const EntityView = &.{},
    episodes: []const Episode = &.{},
    graph: ?GraphRecallStatus = null,
};
pub const RecordPage = struct { items: []const StoredAssertion, next_cursor: ?[]const u8, complete: bool, revision: u64 };
pub const EntityRecord = struct { entity: Entity, owner: []const u8, revision: u64 = 0, recorded_at: ?i64 = null };
pub const EntityPage = struct { items: []const EntityRecord, next_cursor: ?[]const u8, complete: bool };
pub const SpaceInfo = struct {
    table_id: u64 = 0,
    database: []const u8,
    namespace: []const u8,
    space: []const u8,
    config: Config,
    pending_config: ?Config,
    revision: u64,
    owner: []const u8,
    created_at: i64,
};
pub const Inspection = struct {
    state: SpaceInfo,
    capabilities: struct { native: bool, model_free: bool, generator_available: bool, embedder_available: bool },
};

/// External judges carry attribution and evidence, never executable provider
/// configuration or credentials. The authenticated principal is server-owned.
pub const EntityCandidateRequest = struct { kind: enum { identity, relationship } = .identity, cursor: ?[]const u8 = null, limit: u16 = 16 };
pub const EntityCandidate = struct { entity: EntityRecord, view: EntityView, match_score: f64, description_score: f64, name_similarity: ?f64 = null, view_similarity: ?f64 = null, semantic_discovered: bool = false, predicates: []const []const u8 };
pub const EntityCandidatePage = struct {
    seed: EntityRecord,
    seed_view: EntityView,
    kind: @FieldType(EntityCandidateRequest, "kind"),
    relationship_predicates: []const []const u8,
    items: []const EntityCandidate,
    revision: u64,
    config_generation: u64,
    next_cursor: ?[]const u8,
    complete: bool,
    omitted: usize,
    retrieval: enum { indexed_lexical, bounded_audience_fallback, bounded_primary_fallback },
    semantic_discovery: enum { not_indexed, not_ready, indexed } = .not_indexed,
    basis: enum { name_matches_description_tiebreaker_not_identity_or_calibrated_confidence } = .name_matches_description_tiebreaker_not_identity_or_calibrated_confidence,
};
// The resolver owns queue mechanics and their wire shape. Memory adds the
// versioned causal inputs and read envelope, without a second queue model.
pub const EntityProposal = @import("antfly_resolver").curation.Proposal;
pub const EntityProposalInputs = struct {
    revision: u64 = 0,
    operation: ?provenance.Resource = null,
    owner: []const u8,
    actor: []const u8 = "",
    verdict_actor: []const u8 = "",
    config_generation: u64,
    entities: []const struct { id: []const u8, revision: u64 },
    evidence: []const EvidenceRef,
    verdict_revision: ?u64 = null,
    model: ?Model = null,
};
pub const EntityProposalPage = struct { items: []const EntityProposal, next_cursor: ?[]const u8, complete: bool, revision: u64 };
pub const EntityProposalDetail = struct { proposal: EntityProposal, inputs: EntityProposalInputs, reason: ?[]const u8, stale_configuration: bool, revision: u64 };
pub const EntityProposalRequest = struct {
    action: enum { merge, unmerge, suppress, unsuppress, distinct },
    from: []const u8,
    to: ?[]const u8 = null,
    expected_revision: u64,
    replace: bool = false,
    pin_distinct: bool = false,
    absorbed: enum { reject, release, suppress } = .reject,
    actor: []const u8 = "",
    evidence: []const EvidenceRef = &.{},
};
pub const EntityVerdict = struct {
    expected_revision: u64,
    /// null keeps the proposal open for another reviewer.
    accept: ?bool = null,
    pin_distinct: bool = false,
    actor: []const u8 = "",
    reason: []const u8 = "",
    evidence: []const EvidenceRef = &.{},
    model: ?Model = null,
};

/// Immutable repeated mentions, separate from immutable entity identity.
pub const EntityMention = struct { id: []const u8, entity_id: []const u8, evidence: []const EvidenceRef, visibility: Visibility = .private };
pub const StoredMention = struct { mention: EntityMention, owner: []const u8, revision: u64, recorded_at: i64 };
pub const DerivedDependency = struct { resource: provenance.Resource, evidence: []const EvidenceRef };
pub const EntityView = struct {
    entity: Entity,
    canonical_id: []const u8,
    description: []const u8,
    dependencies: []const DerivedDependency,
    fingerprint: []const u8,
    revision: u64,
    config_generation: u64,
    complete: bool,
    scanned: usize,
};
pub const EntityViewRequest = struct { limit: u16 = 16, known_at: ?i64 = null, valid_at: ?i64 = null };
pub const EpisodeTurn = struct { evidence: StoredEvidence, entities: []const []const u8 };
pub const Episode = struct { thread_id: []const u8, turns: []const EpisodeTurn, next_cursor: ?[]const u8, complete: bool, revision: u64 };
pub const EpisodeRequest = struct { cursor: ?[]const u8 = null, limit: u16 = 32, known_at: ?i64 = null };
pub const EpisodePage = struct { items: []const Episode, next_cursor: ?[]const u8, complete: bool, revision: u64 };
pub const GraphRecall = struct {
    entity_seeds: []const []const u8 = &.{},
    max_hops: u8 = 2,
    max_nodes: u16 = 256,
    max_edges: u16 = 1024,
    iterations: u8 = 24,
    restart_probability: f64 = 0.15,
    relation_weight: f64 = 0.5,
    provisional_weight: f64 = 0.5,
    include_episodes: bool = true,
};
pub const GraphRecallStatus = struct { nodes: usize, edges: usize, iterations: u8, complete: bool, omitted: usize, algorithm: []const u8 = "personalized_pagerank" };

pub const RelationshipProposalRequest = struct { id: []const u8, expected_revision: u64, assertion: Assertion, actor: []const u8 = "" };
pub const RelationshipProposal = struct {
    id: []const u8,
    assertion: Assertion,
    owner: []const u8,
    actor: []const u8,
    config_generation: u64,
    revision: u64,
    recorded_at: i64,
    state: enum { open, accepted, rejected } = .open,
    verdict_actor: []const u8 = "",
    verdict_principal: []const u8 = "",
    verdict_revision: ?u64 = null,
    model: ?Model = null,
    verdict_evidence: []const EvidenceRef = &.{},
    reason: []const u8 = "",
};
